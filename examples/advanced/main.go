package main

import (
	"fmt"
	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	lookup.RegisterSynonym("tv", "television")
	ix, _ := lookup.New(lookup.Config{Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
		"body":   {Kind: lookup.FieldText, Indexed: true, Lookup: true, Lowercase: true, Analyzer: "standard"},
		"ip":     {Kind: lookup.FieldKeyword, Lookup: true},
		"domain": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
		"vec":    {Kind: lookup.FieldVector, Dim: 3},
	}}})
	_ = ix.Upsert("1", lookup.Document{"body": "fast television lookup with phrase matching", "ip": "10.0.1.20", "domain": "api.example.com", "vec": []float64{1, 0, 0}})
	_ = ix.Upsert("2", lookup.Document{"body": "vector search and boolean filters", "ip": "10.0.2.20", "domain": "cdn.example.com", "vec": []float64{0, 1, 0}})

	fmt.Println("phrase", ix.Count(lookup.Phrase{Field: "body", Value: "phrase matching"}))
	fmt.Println("proximity", ix.Count(lookup.Proximity{Field: "body", Terms: []string{"fast", "phrase"}, Slop: 4, Ordered: true}))
	fmt.Println("synonym", ix.Count(lookup.Term{Field: "body", Value: "television"}))
	fmt.Println("cidr", ix.Count(lookup.CIDR{Field: "ip", Value: "10.0.1.0/24"}))
	fmt.Println("wildcard", ix.Count(lookup.DomainWildcard{Field: "domain", Pattern: "*.example.com"}))
	fmt.Println("vector", ix.Search(lookup.SearchRequest{Query: lookup.VectorQuery{Field: "vec", Vector: []float64{1, 0, 0}, K: 1}, Limit: 1}).Hits[0].ID)
	fmt.Println("highlight", ix.Highlight("1", "body", "phrase", 40)[0].Fragments[0])
}
