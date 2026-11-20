package pkg

import (
	"encoding/binary"
	"unsafe"
)

// Keep four-byte offsets instead of a sixteen-byte Go string header for every
// stored cell. Payloads remain mapped; strings are materialized only when read.
type mappedStringColumn struct {
	data    []byte
	offsets []uint32
}

func (c mappedStringColumn) at(id DocID) string {
	if uint64(id) >= uint64(len(c.offsets)) {
		return ""
	}
	pos := uint64(c.offsets[id])
	n := binary.LittleEndian.Uint64(c.data[pos : pos+8])
	pos += 8
	if n&(1<<63) != 0 {
		pos = n &^ (1 << 63)
		n = binary.LittleEndian.Uint64(c.data[pos-8 : pos])
	}
	if n == 0 {
		return ""
	}
	return unsafe.String(&c.data[pos], int(n))
}
func (ix *Index) stringValue(field string, id DocID) (string, bool) {
	if col, ok := ix.mappedStrings[field]; ok {
		s := col.at(id)
		return s, id > 0 && s != ""
	}
	return stringColumnValue(ix.strings[field], id)
}
func (ix *Index) stringColumnLen(field string) int {
	if col, ok := ix.mappedStrings[field]; ok {
		return len(col.offsets)
	}
	return len(ix.strings[field])
}
func (ix *Index) hasStringColumn(field string) bool {
	_, mapped := ix.mappedStrings[field]
	return mapped || ix.strings[field] != nil
}

func (ix *Index) externalID(id DocID) string {
	if ix.mappedIDs.offsets != nil {
		return ix.mappedIDs.at(id)
	}
	if uint64(id) < uint64(len(ix.docToExt)) {
		return ix.docToExt[id]
	}
	return ""
}
func (ix *Index) externalIDCount() int {
	if ix.mappedIDs.offsets != nil {
		return len(ix.mappedIDs.offsets)
	}
	return len(ix.docToExt)
}
