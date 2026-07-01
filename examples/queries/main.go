package main

import (
	"fmt"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	ix, _ := lookup.New(lookup.Config{Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
		"title":  {Kind: lookup.FieldText, Indexed: true, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 2, MaxGram: 4},
		"status": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
		"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
		"price":  {Kind: lookup.FieldFloat, Lookup: true, Sortable: true, Facetable: true},
		"sku":    {Kind: lookup.FieldKeyword, Lookup: true, Prefix: true, Lowercase: true, Unique: true},
	}}})
	_ = ix.BatchUpsert(map[string]lookup.Document{
		"1": {"title": "Fast Go lookup engine", "status": "active", "tenant": "orgware", "price": 10.5, "sku": "sku-1"},
		"2": {"title": "Robust boolean search platform", "status": "active", "tenant": "orgware", "price": 35, "sku": "sku-2"},
		"3": {"title": "Archived lookup document", "status": "archived", "tenant": "acme", "price": 99, "sku": "sku-3"},
	})
	queries := map[string]lookup.Query{
		"term":     lookup.Term{Field: "status", Value: "active"},
		"prefix":   lookup.Prefix{Field: "title", Value: "fa"},
		"suffix":   lookup.Suffix{Field: "title", Value: "engine"},
		"contains": lookup.Contains{Field: "title", Value: "look"},
		"fuzzy":    lookup.Fuzzy{Field: "title", Value: "lokup", Distance: 1},
		"range":    lookup.Range{Field: "price", GTE: 10, LT: 40},
		"bool":     lookup.Bool{Must: []lookup.Query{lookup.Term{Field: "tenant", Value: "orgware"}}, MustNot: []lookup.Query{lookup.Term{Field: "status", Value: "archived"}}},
		"simple":   lookup.Simple("title", "+go lookup -archived"),
	}
	for name, q := range queries {
		res := ix.Search(lookup.SearchRequest{Query: q, Limit: 10})
		fmt.Printf("%s=%d\n", name, res.Total)
	}
}
