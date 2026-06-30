package main

import (
	"github.com/oarkflow/lookupx/lookup"
	"log"
	"net/http"
)

func main() {
	ix, _ := lookup.New(lookup.Config{Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
		"title":  {Kind: lookup.FieldText, Indexed: true, Lookup: true, Prefix: true, Lowercase: true, MinGram: 2, MaxGram: 4},
		"status": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
		"price":  {Kind: lookup.FieldFloat, Lookup: true, Sortable: true, Facetable: true},
	}}, APIKeys: []string{"dev-key"}, RateLimitQPS: 1000})
	_ = ix.Upsert("1", lookup.Document{"title": "HTTP lookup server", "status": "active", "price": 10})
	log.Println("listening on :8090; use X-API-Key: dev-key")
	log.Fatal(http.ListenAndServe(":8090", &lookup.Server{Index: ix}))
}
