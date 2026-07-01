package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	ix, err := lookup.New(lookup.Config{
		DisableSource:   true,
		CollectTook:     false,
		InitialCapacity: 10000,
		Clock:           lookup.StaticClock{T: time.Unix(1700000000, 0)},
		Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
			"sku":    {Kind: lookup.FieldKeyword, Lookup: true, Prefix: true, Lowercase: true, Unique: true},
			"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"price":  {Kind: lookup.FieldFloat, Sortable: true},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}

	sku := ix.KeywordField("sku")
	tenant := ix.KeywordField("tenant")
	price := ix.NumericField("price")

	ids := make([]string, 1000)
	skus := make([]string, 1000)
	prices := make([]float64, 1000)
	for i := range ids {
		ids[i] = "doc-" + strconv.Itoa(i)
		skus[i] = "sku-" + strconv.Itoa(i)
		prices[i] = float64(i % 100)
	}
	if err := ix.BatchUpsertKeywordNumericFast(ids, skus, sku, tenant, price, "orgware", prices); err != nil {
		log.Fatal(err)
	}

	hits := make([]lookup.Hit, 0, 16)
	_, hits = ix.SearchInto(lookup.SearchRequest{Query: lookup.Term{Field: "sku", Value: "sku-777"}, Limit: 10}, hits)
	fmt.Println("exact", len(hits), hits[0].ID)
	_, hits = ix.SearchInto(lookup.SearchRequest{Query: lookup.Prefix{Field: "sku", Value: "sku-77"}, Limit: 5}, hits)
	fmt.Println("prefix", len(hits))
	_, hits = ix.SearchInto(lookup.SearchRequest{Query: lookup.Bool{Must: []lookup.Query{lookup.Term{Field: "tenant", Value: "orgware"}}, Filter: []lookup.Query{lookup.Range{Field: "price", GTE: 10, LT: 20}}}, Limit: 5}, hits)
	fmt.Println("bool+range", len(hits))
	fmt.Println("count", ix.CountTerm("tenant", "orgware"))
}
