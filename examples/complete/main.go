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
		InitialCapacity: 20000,
		Clock:           lookup.StaticClock{T: time.Unix(1700000000, 0)},
		Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
			"sku":    {Kind: lookup.FieldKeyword, Lookup: true, Prefix: true, Lowercase: true, Unique: true},
			"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"status": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"title":  {Kind: lookup.FieldText, Indexed: true, Lookup: true, Prefix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 3, MaxGram: 4},
			"body":   {Kind: lookup.FieldText, Indexed: true, Lookup: true, Lowercase: true, Phrase: true},
			"price":  {Kind: lookup.FieldFloat, Sortable: true},
			"ip":     {Kind: lookup.FieldKeyword, Lookup: true},
			"domain": {Kind: lookup.FieldKeyword, Lookup: true, Suffix: true, Lowercase: true},
			"vec":    {Kind: lookup.FieldVector, Dim: 4},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}

	sku := ix.KeywordField("sku")
	tenant := ix.KeywordField("tenant")
	status := ix.KeywordField("status")
	title := ix.KeywordField("title")
	body := ix.KeywordField("body")
	price := ix.NumericField("price")
	ip := ix.KeywordField("ip")
	domain := ix.KeywordField("domain")
	vec := ix.VectorField("vec")

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

	for i := 1000; i < 1050; i++ {
		w := ix.BeginFast("doc-" + strconv.Itoa(i))
		w.KeywordHNormalized(sku, "sku-"+strconv.Itoa(i))
		w.KeywordHNormalized(tenant, []string{"orgware", "acme"}[i&1])
		w.KeywordHNormalized(status, []string{"active", "pending"}[i&1])
		w.TextHNormalized(title, "fast go lookup engine")
		w.TextHNormalized(body, "boolean phrase prefix suffix fuzzy contains vector search")
		w.FloatH(price, float64(i%100))
		w.KeywordHNormalized(ip, "10.1.1."+strconv.Itoa(i&255))
		w.KeywordHNormalized(domain, "api.example.com")
		w.VectorH(vec, []float64{float64(i&7) / 7, float64(i&3) / 3, float64(i&15) / 15, 1})
		if err := w.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	hits := make([]lookup.Hit, 0, 32)
	run := func(name string, q lookup.Query) {
		var res lookup.Result
		res, hits = ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 10}, hits)
		fmt.Printf("%-16s total=%d hits=%d", name, res.Total, len(hits))
		if len(hits) > 0 {
			fmt.Printf(" first=%s", hits[0].ID)
		}
		fmt.Println()
	}
	run("exact", lookup.Term{Field: "sku", Value: "sku-777"})
	run("prefix", lookup.Prefix{Field: "sku", Value: "sku-77"})
	run("terms", lookup.Terms{Field: "status", Values: []string{"active", "pending"}})
	run("range", lookup.Range{Field: "price", GTE: 10, LT: 20})
	run("bool", lookup.Bool{Must: []lookup.Query{lookup.Term{Field: "tenant", Value: "orgware"}}, Filter: []lookup.Query{lookup.Range{Field: "price", GTE: 10, LT: 20}}})
	run("contains", lookup.Contains{Field: "title", Value: "look"})
	run("fuzzy", lookup.Fuzzy{Field: "title", Value: "engin", Distance: 1})
	run("phrase", lookup.Phrase{Field: "body", Value: "phrase prefix suffix"})
	run("cidr", lookup.CIDR{Field: "ip", Value: "10.1.1.0/24"})
	run("domain", lookup.DomainWildcard{Field: "domain", Pattern: "*.example.com"})
	run("vector", lookup.VectorQuery{Field: "vec", Vector: []float64{.7, .3, .2, 1}, K: 20, Metric: "dot", Filter: lookup.Term{Field: "tenant", Value: "orgware"}})
	fmt.Println("fast count", ix.CountTerm("tenant", "orgware"))
}
