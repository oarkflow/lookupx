package main

import (
	"fmt"
	"log"

	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	ix, err := lookup.New(lookup.Config{DisableSource: true, InitialCapacity: 4, Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
		"title":  {Kind: lookup.FieldText, Indexed: true, Lookup: true, Prefix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 3, MaxGram: 4, Phrase: true},
		"status": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
		"ip":     {Kind: lookup.FieldKeyword, Lookup: true},
	}}})
	if err != nil {
		log.Fatal(err)
	}
	_ = ix.Upsert("1", lookup.Document{"title": "fast phrase prefix fuzzy search", "status": "active", "ip": "10.1.1.20"})
	_ = ix.Upsert("2", lookup.Document{"title": "slow database lookup", "status": "archived", "ip": "192.168.1.1"})
	hits := make([]lookup.Hit, 0, 8)
	queries := []lookup.Query{
		lookup.Phrase{Field: "title", Value: "phrase prefix"},
		lookup.Contains{Field: "title", Value: "fuzz"},
		lookup.Fuzzy{Field: "title", Value: "serch", Distance: 1},
		lookup.CIDR{Field: "ip", Value: "10.1.1.0/24"},
	}
	for _, q := range queries {
		_, hits = ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 10}, hits)
		fmt.Printf("%T -> %d hits\n", q, len(hits))
	}
}
