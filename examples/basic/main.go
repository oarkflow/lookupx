package main

import (
	"fmt"
	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	ix, _ := lookup.New(lookup.Config{Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
		"name":   {Kind: lookup.FieldText, Indexed: true, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 2, MaxGram: 3},
		"email":  {Kind: lookup.FieldKeyword, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Lowercase: true, MinGram: 2, MaxGram: 4},
		"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
		"status": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
	}}})
	_ = ix.Upsert("1", lookup.Document{"name": "Sujit Shrestha", "email": "sujit@example.com", "tenant": "orgware", "status": "active"})
	_ = ix.Upsert("2", lookup.Document{"name": "John Smith", "email": "john@gmail.com", "tenant": "orgware", "status": "inactive"})
	_ = ix.Upsert("3", lookup.Document{"name": "Jane Doe", "email": "jane@example.com", "tenant": "acme", "status": "active"})

	res := ix.Search(lookup.SearchRequest{Query: lookup.Bool{Must: []lookup.Query{lookup.Prefix{Field: "name", Value: "su"}}, Filter: []lookup.Query{lookup.Term{Field: "tenant", Value: "orgware"}, lookup.Term{Field: "status", Value: "active"}}}, Limit: 10, WithDocs: true})
	fmt.Printf("prefix+filters total=%d hits=%v\n", res.Total, res.Hits)
	res = ix.Search(lookup.SearchRequest{Query: lookup.Fuzzy{Field: "name", Value: "sujjt", Distance: 1}, Limit: 10, WithDocs: true})
	fmt.Printf("fuzzy total=%d hits=%v\n", res.Total, res.Hits)
}
