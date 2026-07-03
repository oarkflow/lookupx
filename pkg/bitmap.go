package pkg

import (
	"math/bits"
	"sort"
)

const sparseBitmapLimit = 4096

func shouldPromoteSparse(count int, max DocID) bool {
	// Dense bitmaps are only smaller when the number of set bits is close to the
	// number of uint64 words needed to cover max. A fixed 4096 threshold promoted
	// medium-frequency text terms too early in million-row indexes.
	words := int(max>>6) + 1
	if words < sparseBitmapLimit {
		words = sparseBitmapLimit
	}
	return count > words
}

// Bitmap is an adaptive posting container. Small/sparse postings stay as a
// sorted DocID slice; large/dense postings are promoted to a classic bitset.
// This avoids allocating a full max-doc bitmap for every repeated token in
// large text indexes.
type Bitmap struct {
	words  []uint64
	sparse []DocID
}

func NewBitmap() *Bitmap { return &Bitmap{} }
func NewBitmapCap(max DocID) *Bitmap {
	if max == 0 {
		return &Bitmap{}
	}
	return &Bitmap{words: make([]uint64, int((max+63)>>6))}
}
func (b *Bitmap) isDense() bool { return len(b.words) != 0 }
func (b *Bitmap) Reset() {
	for i := range b.words {
		b.words[i] = 0
	}
	b.sparse = b.sparse[:0]
}
func (b *Bitmap) ensure(id DocID) {
	idx := int(id >> 6)
	if idx >= len(b.words) {
		nw := make([]uint64, idx+1)
		copy(nw, b.words)
		b.words = nw
	}
}
func (b *Bitmap) promote(max DocID) {
	if b.isDense() {
		b.ensure(max)
		return
	}
	capTo := max
	if len(b.sparse) > 0 {
		last := b.sparse[len(b.sparse)-1]
		if last > capTo {
			capTo = last
		}
	}
	if capTo == 0 {
		b.words = []uint64{0}
	} else {
		b.words = make([]uint64, int(capTo>>6)+1)
	}
	for _, id := range b.sparse {
		b.words[id>>6] |= 1 << (id & 63)
	}
	b.sparse = nil
}
func (b *Bitmap) Add(id DocID) {
	if b.isDense() {
		b.ensure(id)
		b.words[id>>6] |= 1 << (id & 63)
		return
	}
	if n := len(b.sparse); n > 0 {
		last := b.sparse[n-1]
		if id == last {
			return
		}
		if id > last {
			b.sparse = append(b.sparse, id)
		} else {
			i := sort.Search(len(b.sparse), func(i int) bool { return b.sparse[i] >= id })
			if i < len(b.sparse) && b.sparse[i] == id {
				return
			}
			b.sparse = append(b.sparse, 0)
			copy(b.sparse[i+1:], b.sparse[i:])
			b.sparse[i] = id
		}
	} else {
		b.sparse = append(b.sparse, id)
	}
	if shouldPromoteSparse(len(b.sparse), id) {
		b.promote(id)
	}
}

// AddUnsafe sets a bit without bounds checks/growth. Use only on dense bitmaps
// pre-sized to include id. Sparse bitmaps fall back to Add for correctness.
func (b *Bitmap) AddUnsafe(id DocID) {
	if !b.isDense() || int(id>>6) >= len(b.words) {
		b.Add(id)
		return
	}
	b.words[id>>6] |= 1 << (id & 63)
}
func (b *Bitmap) Remove(id DocID) {
	if b.isDense() {
		idx := int(id >> 6)
		if idx < len(b.words) {
			b.words[idx] &^= 1 << (id & 63)
		}
		return
	}
	i := sort.Search(len(b.sparse), func(i int) bool { return b.sparse[i] >= id })
	if i < len(b.sparse) && b.sparse[i] == id {
		copy(b.sparse[i:], b.sparse[i+1:])
		b.sparse = b.sparse[:len(b.sparse)-1]
	}
}
func (b *Bitmap) Has(id DocID) bool {
	if b == nil {
		return false
	}
	if b.isDense() {
		idx := int(id >> 6)
		return idx < len(b.words) && (b.words[idx]&(1<<(id&63))) != 0
	}
	i := sort.Search(len(b.sparse), func(i int) bool { return b.sparse[i] >= id })
	return i < len(b.sparse) && b.sparse[i] == id
}
func (b *Bitmap) Clone() *Bitmap {
	if b == nil {
		return NewBitmap()
	}
	w := make([]uint64, len(b.words))
	copy(w, b.words)
	s := make([]DocID, len(b.sparse))
	copy(s, b.sparse)
	return &Bitmap{words: w, sparse: s}
}
func (b *Bitmap) Count() int {
	if b == nil {
		return 0
	}
	if !b.isDense() {
		return len(b.sparse)
	}
	n := 0
	for _, w := range b.words {
		n += bits.OnesCount64(w)
	}
	return n
}
func (b *Bitmap) Empty() bool           { return b == nil || b.Count() == 0 }
func (b *Bitmap) And(o *Bitmap) *Bitmap { return b.Clone().AndInPlace(o) }
func (b *Bitmap) Or(o *Bitmap) *Bitmap  { return b.Clone().OrInPlace(o) }
func (b *Bitmap) OrInPlace(o *Bitmap) *Bitmap {
	if o == nil || o.Empty() {
		if b == nil {
			return NewBitmap()
		}
		return b
	}
	if b == nil || b.Empty() {
		return o.Clone()
	}
	if !b.isDense() && !o.isDense() && len(b.sparse)+len(o.sparse) <= sparseBitmapLimit {
		merged := make([]DocID, 0, len(b.sparse)+len(o.sparse))
		i, j := 0, 0
		for i < len(b.sparse) && j < len(o.sparse) {
			if b.sparse[i] == o.sparse[j] {
				merged = append(merged, b.sparse[i])
				i++
				j++
			} else if b.sparse[i] < o.sparse[j] {
				merged = append(merged, b.sparse[i])
				i++
			} else {
				merged = append(merged, o.sparse[j])
				j++
			}
		}
		merged = append(merged, b.sparse[i:]...)
		merged = append(merged, o.sparse[j:]...)
		b.sparse = merged
		return b
	}
	if !b.isDense() {
		max := DocID(0)
		if len(b.sparse) > 0 {
			max = b.sparse[len(b.sparse)-1]
		}
		if o.isDense() && DocID(len(o.words)*64) > max {
			max = DocID(len(o.words) * 64)
		}
		b.promote(max)
	}
	if o.isDense() {
		if len(o.words) > len(b.words) {
			nw := make([]uint64, len(o.words))
			copy(nw, b.words)
			b.words = nw
		}
		for i := 0; i < len(o.words); i++ {
			b.words[i] |= o.words[i]
		}
		return b
	}
	for _, id := range o.sparse {
		b.Add(id)
	}
	return b
}
func (b *Bitmap) AndInPlace(o *Bitmap) *Bitmap {
	if b == nil || o == nil || b.Empty() || o.Empty() {
		return NewBitmap()
	}
	if !b.isDense() {
		out := b.sparse[:0]
		for _, id := range b.sparse {
			if o.Has(id) {
				out = append(out, id)
			}
		}
		b.sparse = out
		return b
	}
	if !o.isDense() {
		out := make([]DocID, 0, minInt(o.Count(), sparseBitmapLimit))
		for _, id := range o.sparse {
			if b.Has(id) {
				out = append(out, id)
			}
		}
		b.words = nil
		b.sparse = out
		return b
	}
	n := len(b.words)
	if len(o.words) < n {
		n = len(o.words)
	}
	for i := 0; i < n; i++ {
		b.words[i] &= o.words[i]
	}
	for i := n; i < len(b.words); i++ {
		b.words[i] = 0
	}
	return b
}
func (b *Bitmap) Not(max DocID) *Bitmap {
	r := NewBitmapCap(max)
	for i := range r.words {
		r.words[i] = ^uint64(0)
	}
	if b != nil {
		if b.isDense() {
			for i, w := range b.words {
				if i < len(r.words) {
					r.words[i] &^= w
				}
			}
		} else {
			for _, id := range b.sparse {
				if int(id>>6) < len(r.words) {
					r.words[id>>6] &^= 1 << (id & 63)
				}
			}
		}
	}
	if len(r.words) > 0 {
		if rem := uint(max & 63); rem != 0 {
			r.words[len(r.words)-1] &= (uint64(1) << rem) - 1
		}
	}
	return r
}
func (b *Bitmap) Each(fn func(DocID) bool) {
	if b == nil {
		return
	}
	if !b.isDense() {
		for _, id := range b.sparse {
			if !fn(id) {
				return
			}
		}
		return
	}
	for wi, w := range b.words {
		for w != 0 {
			tz := bits.TrailingZeros64(w)
			id := DocID(wi*64 + tz)
			if !fn(id) {
				return
			}
			w &= w - 1
		}
	}
}
func (b *Bitmap) Words(max DocID) []uint64 {
	if b == nil {
		return nil
	}
	if b.isDense() {
		return append([]uint64(nil), b.words...)
	}
	w := make([]uint64, int((max+63)>>6))
	for _, id := range b.sparse {
		idx := int(id >> 6)
		if idx >= len(w) {
			nw := make([]uint64, idx+1)
			copy(nw, w)
			w = nw
		}
		w[idx] |= 1 << (id & 63)
	}
	return w
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
