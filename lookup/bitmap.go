package lookup

import "math/bits"

type Bitmap struct{ words []uint64 }

func NewBitmap() *Bitmap { return &Bitmap{} }
func NewBitmapCap(max DocID) *Bitmap {
	if max == 0 {
		return &Bitmap{}
	}
	return &Bitmap{words: make([]uint64, int((max+63)>>6))}
}
func (b *Bitmap) Reset() {
	for i := range b.words {
		b.words[i] = 0
	}
}

func (b *Bitmap) ensure(id DocID) {
	idx := int(id >> 6)
	if idx >= len(b.words) {
		nw := make([]uint64, idx+1)
		copy(nw, b.words)
		b.words = nw
	}
}
func (b *Bitmap) Add(id DocID) { b.ensure(id); b.words[id>>6] |= 1 << (id & 63) }

// AddUnsafe sets a bit without bounds checks/growth. Use only when the bitmap
// was pre-sized to include id. It exists for compiled ingestion hot paths.
func (b *Bitmap) AddUnsafe(id DocID) { b.words[id>>6] |= 1 << (id & 63) }
func (b *Bitmap) Remove(id DocID) {
	idx := int(id >> 6)
	if idx < len(b.words) {
		b.words[idx] &^= 1 << (id & 63)
	}
}
func (b *Bitmap) Has(id DocID) bool {
	idx := int(id >> 6)
	return idx < len(b.words) && (b.words[idx]&(1<<(id&63))) != 0
}
func (b *Bitmap) Clone() *Bitmap {
	w := make([]uint64, len(b.words))
	copy(w, b.words)
	return &Bitmap{w}
}
func (b *Bitmap) Count() int {
	n := 0
	for _, w := range b.words {
		n += bits.OnesCount64(w)
	}
	return n
}
func (b *Bitmap) Empty() bool {
	for _, w := range b.words {
		if w != 0 {
			return false
		}
	}
	return true
}
func (b *Bitmap) And(o *Bitmap) *Bitmap {
	if b == nil || o == nil {
		return NewBitmap()
	}
	n := len(b.words)
	if len(o.words) < n {
		n = len(o.words)
	}
	r := &Bitmap{words: make([]uint64, n)}
	for i := 0; i < n; i++ {
		r.words[i] = b.words[i] & o.words[i]
	}
	return r
}
func (b *Bitmap) Or(o *Bitmap) *Bitmap {
	if b == nil {
		if o == nil {
			return NewBitmap()
		}
		return o.Clone()
	}
	if o == nil {
		return b.Clone()
	}
	n := len(b.words)
	if len(o.words) > n {
		n = len(o.words)
	}
	r := &Bitmap{words: make([]uint64, n)}
	copy(r.words, b.words)
	for i := 0; i < len(o.words); i++ {
		r.words[i] |= o.words[i]
	}
	return r
}
func (b *Bitmap) OrInPlace(o *Bitmap) *Bitmap {
	if o == nil {
		return b
	}
	if b == nil {
		return o.Clone()
	}
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
func (b *Bitmap) AndInPlace(o *Bitmap) *Bitmap {
	if b == nil || o == nil {
		return NewBitmap()
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
	r := &Bitmap{words: make([]uint64, int((max+63)>>6))}
	for i := range r.words {
		r.words[i] = ^uint64(0)
	}
	for i, w := range b.words {
		if i < len(r.words) {
			r.words[i] &^= w
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
