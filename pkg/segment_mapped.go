package pkg

// The mapped snapshot keeps scalar columns and posting arrays in the file.
// Only dictionary tables and string/slice headers are allocated when opening a
// serving partition. The regular LoadIndex API still returns a mutable copy.
import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"unsafe"
)

const mappedMagic = "LXMM0002"

func isMappedMagic(magic string) bool { return magic == mappedMagic || magic == "LXMM0001" }

var mappedSchema = func() [32]byte {
	var b strings.Builder
	var describe func(reflect.Type)
	describe = func(t reflect.Type) {
		b.WriteString(t.String())
		b.WriteByte(':')
		switch t.Kind() {
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				b.WriteString(t.Field(i).Name)
				describe(t.Field(i).Type)
			}
		case reflect.Slice, reflect.Pointer:
			describe(t.Elem())
		case reflect.Map:
			describe(t.Key())
			describe(t.Elem())
		}
	}
	describe(reflect.TypeFor[persistentDump]())
	return sha256.Sum256([]byte(b.String()))
}()

func nativeLittleEndian() bool { n := uint16(1); return *(*byte)(unsafe.Pointer(&n)) == 1 }

func (ix *Index) encodeMapped(w io.Writer) error {
	if !nativeLittleEndian() {
		return ix.encodePersistent(w)
	}
	return ix.writePersistentView(func(d *persistentDump) error {
		bw := bufio.NewWriterSize(w, 256<<10)
		e := mappedEncoder{w: bw, strings: make(map[string]uint64)}
		e.write([]byte(mappedMagic))
		e.write(mappedSchema[:])
		e.value(reflect.ValueOf(d).Elem())
		if e.err != nil {
			return e.err
		}
		return bw.Flush()
	})
}

type mappedEncoder struct {
	strings map[string]uint64
	w       io.Writer
	offset  uint64
	err     error
}

func (e *mappedEncoder) write(b []byte) {
	if e.err != nil {
		return
	}
	n, err := e.w.Write(b)
	e.offset += uint64(n)
	e.err = err
	if err == nil && n != len(b) {
		e.err = io.ErrShortWrite
	}
}
func (e *mappedEncoder) word(n uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	e.write(b[:])
}
func (e *mappedEncoder) align() {
	if n := (8 - e.offset%8) % 8; n > 0 {
		e.write(make([]byte, n))
	}
}
func scalarSlice(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}
func (e *mappedEncoder) value(v reflect.Value) {
	if e.err != nil {
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			e.word(1)
		} else {
			e.word(0)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.word(uint64(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		e.word(v.Uint())
	case reflect.Float32, reflect.Float64:
		e.word(math.Float64bits(v.Float()))
	case reflect.String:
		s := v.String()
		if offset, ok := e.strings[s]; ok && len(s) > 0 {
			e.word(offset | (1 << 63))
		} else {
			e.word(uint64(len(s)))
			if len(s) > 0 {
				e.strings[s] = e.offset
			}
			e.write(unsafe.Slice(unsafe.StringData(s), len(s)))
		}
	case reflect.Pointer:
		if v.IsNil() {
			e.word(0)
		} else {
			e.word(1)
			e.value(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			e.value(v.Field(i))
		}
	case reflect.Slice:
		e.word(uint64(v.Len()))
		if scalarSlice(v.Type().Elem()) {
			e.align()
			if v.Len() > 0 {
				e.write(unsafe.Slice((*byte)(v.UnsafePointer()), v.Len()*int(v.Type().Elem().Size())))
			}
			return
		}
		for i := 0; i < v.Len(); i++ {
			e.value(v.Index(i))
		}
	case reflect.Map:
		e.word(uint64(v.Len()))
		iter := v.MapRange()
		for iter.Next() {
			e.value(iter.Key())
			e.value(iter.Value())
		}
	case reflect.Interface:
		if v.IsNil() {
			e.word(0)
			return
		}
		var b bytes.Buffer
		x := v.Interface()
		if err := gob.NewEncoder(&b).Encode(&x); err != nil {
			e.err = err
			return
		}
		e.word(uint64(b.Len()))
		e.write(b.Bytes())
	default:
		e.err = fmt.Errorf("unsupported mapped snapshot type %s", v.Type())
	}
}

type mappedDecoder struct {
	compactStrings map[string]mappedStringColumn
	compactIDs     *mappedStringColumn
	data           []byte
	offset         int
	mapped         bool
	err            error
}

func (d *mappedDecoder) take(n uint64) []byte {
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.data)-d.offset) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	b := d.data[d.offset : d.offset+int(n)]
	d.offset += int(n)
	return b
}
func (d *mappedDecoder) word() uint64 {
	b := d.take(8)
	if d.err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (d *mappedDecoder) count() int {
	n := d.word()
	if d.err != nil {
		return 0
	}
	// Every aggregate entry occupies at least one byte in this format. Bound
	// allocations before trusting lengths supplied by a corrupt/truncated file.
	if n > uint64(len(d.data)-d.offset) {
		d.err = errors.New("invalid mapped snapshot length")
		return 0
	}
	return int(n)
}
func (d *mappedDecoder) value(v reflect.Value) {
	if d.err != nil {
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(d.word() != 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(d.word()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(d.word())
	case reflect.Float32, reflect.Float64:
		v.SetFloat(math.Float64frombits(d.word()))
	case reflect.String:
		b := d.stringBytes()
		if len(b) > 0 {
			if d.mapped {
				v.SetString(unsafe.String(&b[0], len(b)))
			} else {
				v.SetString(string(b))
			}
		}

	case reflect.Pointer:
		if d.word() != 0 {
			v.Set(reflect.New(v.Type().Elem()))
			d.value(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if d.compactIDs != nil && uint64(len(d.data)) <= uint64(^uint32(0)) && v.Type().Field(i).Name == "DocToExt" && v.Field(i).Type() == reflect.TypeFor[[]string]() {
				*d.compactIDs = d.stringColumn()
				continue
			}
			d.value(v.Field(i))
		}
	case reflect.Slice:
		n := d.count()
		if d.err != nil {
			return
		}
		if scalarSlice(v.Type().Elem()) {
			d.take(uint64((8 - d.offset%8) % 8))
			size := uint64(v.Type().Elem().Size())
			b := d.take(uint64(n) * size)
			if d.err != nil || n == 0 {
				return
			}
			view := reflect.SliceAt(v.Type().Elem(), unsafe.Pointer(&b[0]), n)
			if d.mapped {
				v.Set(view)
			} else {
				out := reflect.MakeSlice(v.Type(), n, n)
				reflect.Copy(out, view)
				v.Set(out)
			}
			return
		}
		if n > (len(d.data)-d.offset)/8 {
			d.err = errors.New("invalid mapped slice count")
			return
		}
		v.Set(reflect.MakeSlice(v.Type(), n, n))
		for i := 0; i < n; i++ {
			d.value(v.Index(i))
			if d.err != nil {
				return
			}
		}
	case reflect.Map:
		if d.compactStrings != nil && uint64(len(d.data)) <= uint64(^uint32(0)) && v.Type() == reflect.TypeFor[map[string][]string]() {
			n := d.count()
			if d.err != nil || n > (len(d.data)-d.offset)/16 {
				if d.err == nil {
					d.err = errors.New("invalid compact string dictionary")
				}
				return
			}
			v.Set(reflect.MakeMapWithSize(v.Type(), n))
			for i := 0; i < n; i++ {
				key := reflect.New(v.Type().Key()).Elem()
				d.value(key)
				column := d.stringColumn()
				if d.err != nil {
					return
				}
				d.compactStrings[key.String()] = column
				v.SetMapIndex(key, reflect.Zero(v.Type().Elem()))
			}
			return
		}
		n := d.count()
		if d.err != nil {
			return
		}
		if n > (len(d.data)-d.offset)/16 {
			d.err = errors.New("invalid mapped dictionary count")
			return
		}
		v.Set(reflect.MakeMapWithSize(v.Type(), n))
		for i := 0; i < n; i++ {
			k := reflect.New(v.Type().Key()).Elem()
			x := reflect.New(v.Type().Elem()).Elem()
			d.value(k)
			d.value(x)
			if d.err != nil {
				return
			}
			v.SetMapIndex(k, x)
		}
	case reflect.Interface:
		b := d.take(d.word())
		if d.err != nil || len(b) == 0 {
			return
		}
		var x any
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&x); err != nil {
			d.err = err
			return
		}
		v.Set(reflect.ValueOf(x))
	default:
		d.err = fmt.Errorf("unsupported mapped snapshot type %s", v.Type())
	}
}

func decodeMapped(data []byte, dump *persistentDump, mapped bool) error {
	return decodeMappedColumns(data, dump, mapped, nil)
}
func decodeMappedColumns(data []byte, dump *persistentDump, mapped bool, columns map[string]mappedStringColumn, ids ...*mappedStringColumn) error {
	if !nativeLittleEndian() {
		return errors.New("mapped snapshots require a little-endian machine")
	}
	if len(data) < 40 || !isMappedMagic(string(data[:8])) || !bytes.Equal(data[8:40], mappedSchema[:]) {
		return errors.New("unsupported mapped snapshot schema; rebuild this index")
	}
	d := mappedDecoder{data: data, offset: 40, mapped: mapped, compactStrings: columns}
	if len(ids) > 0 {
		d.compactIDs = ids[0]
	}
	d.value(reflect.ValueOf(dump).Elem())
	if d.err != nil {
		return fmt.Errorf("decode mapped snapshot: %w", d.err)
	}
	if d.offset != len(data) {
		return errors.New("mapped snapshot has trailing data")
	}
	return nil
}

func (d *mappedDecoder) stringBytes() []byte {
	n := d.word()
	var b []byte
	if n&(1<<63) != 0 {
		offset := n &^ (1 << 63)
		if offset < 48 || offset > uint64(d.offset-8) {
			d.err = errors.New("invalid mapped string reference")
			return nil
		}
		length := binary.LittleEndian.Uint64(d.data[offset-8 : offset])
		if length > uint64(d.offset-8)-offset {
			d.err = errors.New("invalid mapped string length")
			return nil
		}
		b = d.data[offset : offset+length]
	} else {
		b = d.take(n)
	}
	return b
}

func (d *mappedDecoder) stringColumn() mappedStringColumn {
	count := d.count()
	if d.err != nil || count > (len(d.data)-d.offset)/8 {
		if d.err == nil {
			d.err = errors.New("invalid compact string column")
		}
		return mappedStringColumn{}
	}
	offsets := make([]uint32, count)
	for i := range offsets {
		offsets[i] = uint32(d.offset)
		d.stringBytes()
		if d.err != nil {
			return mappedStringColumn{}
		}
	}
	return mappedStringColumn{data: d.data, offsets: offsets}
}
