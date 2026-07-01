package main

import (
	"fmt"
	"log"
	"strconv"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	ix, err := lookup.New(lookup.Config{DisableSource: true, InitialCapacity: 5000, Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
		"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
		"vec":    {Kind: lookup.FieldVector, Dim: 4},
	}}})
	if err != nil {
		log.Fatal(err)
	}
	tenant := ix.KeywordField("tenant")
	vec := ix.VectorField("vec")
	for i := 0; i < 5000; i++ {
		w := ix.BeginFast("doc-" + strconv.Itoa(i))
		if i&1 == 0 {
			w.KeywordHNormalized(tenant, "orgware")
		} else {
			w.KeywordHNormalized(tenant, "acme")
		}
		w.VectorH(vec, []float64{float64(i&7) / 7, float64(i&3) / 3, float64(i&15) / 15, 1})
		if err := w.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	hits := make([]lookup.Hit, 0, 10)
	_, hits = ix.SearchInto(lookup.SearchRequest{Query: lookup.VectorQuery{Field: "vec", Vector: []float64{.7, .3, .2, 1}, K: 20, Metric: "dot", Filter: lookup.Term{Field: "tenant", Value: "orgware"}}, Limit: 10}, hits)
	fmt.Println("ann filtered vector hits", len(hits))
	for i, h := range hits[:min(len(hits), 3)] {
		fmt.Println(i, h.ID, h.Score)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
