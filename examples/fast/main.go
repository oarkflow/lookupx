package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	ix, err := lookup.New(lookup.Config{
		DisableSource:   true,
		InitialCapacity: 1024,
		Clock:           lookup.StaticClock{T: time.Unix(1700000000, 0)},
		Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
			"sku":    {Kind: lookup.FieldKeyword, Lookup: true, Unique: true, Lowercase: true},
			"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"price":  {Kind: lookup.FieldFloat, Sortable: true},
		}},
	})
	if err != nil {
		panic(err)
	}

	sku := ix.KeywordField("sku")
	tenant := ix.KeywordField("tenant")
	price := ix.NumericField("price")

	w := ix.BeginFast("doc-1")
	w.KeywordHNormalized(sku, "sku-1")
	w.KeywordHNormalized(tenant, "orgware")
	w.FloatH(price, 99.50)
	if err := w.Commit(); err != nil {
		panic(err)
	}

	hits := ix.SearchTermFast(ix.FieldID("sku"), "sku-1", 1, nil)
	fmt.Println("hits", len(hits), hits[0].ID)
}
