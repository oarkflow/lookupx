package main

import (
	"fmt"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	r := lookup.NewRadix()
	r.Add("search", "1")
	r.Add("segment", "2")
	rr := lookup.NewReverseRadix()
	rr.Add("hello@example.com", "1")
	f := lookup.NewFST()
	f.Add("search", 10)
	f.Add("segment", 3)
	ph := lookup.NewPerfectHash([]string{"a", "b", "c"})
	bl := lookup.NewBloom(1024, 3)
	bl.Add("present")
	h := lookup.NewHNSW(8, "cosine")
	h.Add("v1", []float64{1, 0})
	h.Add("v2", []float64{0, 1})
	fmt.Println("radix", r.Prefix("se"))
	fmt.Println("reverse", rr.Suffix("example.com"))
	fmt.Println("complete", f.Complete("se", 10))
	idx, ok := ph.Get("b")
	fmt.Println("hash", idx, ok)
	fmt.Println("bloom", bl.Has("present"), bl.Has("missing"))
	fmt.Println("hnsw", h.Search([]float64{1, 0}, 1)[0].ID)
}
