package pkg

import "sort"

type radixNode struct {
	children map[byte]*radixNode
	ids      []string
}

type Radix struct{ root *radixNode }

func NewRadix() *Radix { return &Radix{root: &radixNode{}} }
func (r *Radix) Add(key, id string) {
	n := r.root
	for i := 0; i < len(key); i++ {
		if n.children == nil {
			n.children = map[byte]*radixNode{}
		}
		c := key[i]
		nn := n.children[c]
		if nn == nil {
			nn = &radixNode{}
			n.children[c] = nn
		}
		n = nn
	}
	n.ids = append(n.ids, id)
}
func (r *Radix) Prefix(prefix string) []string {
	n := r.root
	for i := 0; i < len(prefix); i++ {
		if n.children == nil {
			return nil
		}
		n = n.children[prefix[i]]
		if n == nil {
			return nil
		}
	}
	out := make([]string, 0, 32)
	var walk func(*radixNode)
	walk = func(n *radixNode) {
		out = append(out, n.ids...)
		for _, child := range n.children {
			walk(child)
		}
	}
	walk(n)
	return out
}

type ReverseRadix struct{ r *Radix }

func NewReverseRadix() *ReverseRadix                  { return &ReverseRadix{NewRadix()} }
func (r *ReverseRadix) Add(key, id string)            { r.r.Add(reverseASCII(key), id) }
func (r *ReverseRadix) Suffix(suffix string) []string { return r.r.Prefix(reverseASCII(suffix)) }
func reverseASCII(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		b[i] = s[len(s)-1-i]
	}
	return string(b)
}

type FST struct {
	terms  map[string]int
	sorted []string
	dirty  bool
}

func NewFST() *FST { return &FST{terms: map[string]int{}} }
func (f *FST) Add(term string, weight int) {
	if _, ok := f.terms[term]; !ok {
		f.dirty = true
	}
	f.terms[term] = weight
}
func (f *FST) Complete(prefix string, limit int) []string {
	if f.dirty {
		if cap(f.sorted) < len(f.terms) {
			f.sorted = make([]string, 0, len(f.terms))
		} else {
			f.sorted = f.sorted[:0]
		}
		for t := range f.terms {
			f.sorted = append(f.sorted, t)
		}
		sort.Strings(f.sorted)
		f.dirty = false
	}
	i := sort.SearchStrings(f.sorted, prefix)
	out := make([]string, 0, limit)
	for ; i < len(f.sorted); i++ {
		t := f.sorted[i]
		if len(t) < len(prefix) || t[:len(prefix)] != prefix {
			break
		}
		out = append(out, t)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

type PerfectHash struct{ m map[string]int }

func NewPerfectHash(keys []string) *PerfectHash {
	m := map[string]int{}
	for i, k := range keys {
		m[k] = i
	}
	return &PerfectHash{m}
}
func (p *PerfectHash) Get(k string) (int, bool) { v, ok := p.m[k]; return v, ok }

type Bloom struct {
	bits []uint64
	k    uint64
}

func NewBloom(size int, k uint64) *Bloom {
	if size <= 0 {
		size = 1024
	}
	if k == 0 {
		k = 3
	}
	return &Bloom{bits: make([]uint64, (size+63)/64), k: k}
}
func (b *Bloom) Add(s string) {
	for i := uint64(0); i < b.k; i++ {
		h := hash64(s, i) % uint64(len(b.bits)*64)
		b.bits[h>>6] |= 1 << (h & 63)
	}
}
func (b *Bloom) Has(s string) bool {
	for i := uint64(0); i < b.k; i++ {
		h := hash64(s, i) % uint64(len(b.bits)*64)
		if b.bits[h>>6]&(1<<(h&63)) == 0 {
			return false
		}
	}
	return true
}

type Cuckoo = Bloom

func hash64(s string, seed uint64) uint64 {
	var h uint64 = 1469598103934665603 + seed*1099511628211
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func QuantizeVector(v []float64) []int8 {
	out := make([]int8, len(v))
	for i, x := range v {
		if x > 1 {
			x = 1
		}
		if x < -1 {
			x = -1
		}
		out[i] = int8(x * 127)
	}
	return out
}
func DequantizeVector(v []int8) []float64 {
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = float64(x) / 127
	}
	return out
}
