package pkg

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

func benchIndex(b *testing.B, n int) *Index {
	b.Helper()
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"title":      {Kind: FieldText, Indexed: true, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 2, MaxGram: 4},
		"body":       {Kind: FieldText, Indexed: true, Lookup: true, Prefix: true, Ngram: true, Lowercase: true, MinGram: 3, MaxGram: 5},
		"email":      {Kind: FieldKeyword, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Lowercase: true, MinGram: 2, MaxGram: 4},
		"phone":      {Kind: FieldKeyword, Lookup: true},
		"domain":     {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"ip":         {Kind: FieldKeyword, Lookup: true},
		"tenant":     {Kind: FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
		"status":     {Kind: FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
		"tags":       {Kind: FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
		"price":      {Kind: FieldFloat, Lookup: true, Sortable: true, Facetable: true},
		"rank":       {Kind: FieldInt, Lookup: true, Sortable: true},
		"created_at": {Kind: FieldTime, Lookup: true, Sortable: true},
		"sku":        {Kind: FieldKeyword, Lookup: true, Prefix: true, Lowercase: true, Unique: true},
		"vec":        {Kind: FieldVector, Dim: 4},
	}}})
	if err != nil {
		b.Fatal(err)
	}
	statuses := []string{"active", "pending", "archived"}
	tenants := []string{"orgware", "acme", "globex", "initech"}
	tags := []string{"go", "lookup", "search", "fast", "index"}
	for i := 0; i < n; i++ {
		doc := Document{
			"title":      fmt.Sprintf("Fast Go lookup engine document %d", i),
			"body":       fmt.Sprintf("LookupX boolean phrase prefix suffix fuzzy contains vector search body %d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"phone":      fmt.Sprintf("+977980000%04d", i%10000),
			"domain":     fmt.Sprintf("sub%d.example.com", i%100),
			"ip":         fmt.Sprintf("10.1.%d.%d", (i/255)%255, i%255),
			"tenant":     tenants[i%len(tenants)],
			"status":     statuses[i%len(statuses)],
			"tags":       []any{tags[i%len(tags)], tags[(i+1)%len(tags)]},
			"price":      float64(i%1000) + 0.99,
			"rank":       i % 100,
			"created_at": time.Unix(int64(1700000000+i), 0).Unix(),
			"sku":        fmt.Sprintf("sku-%06d", i),
			"vec":        []float64{float64(i%10) / 10, float64(i%7) / 7, float64(i%5) / 5, 1},
		}
		if err := ix.Upsert(fmt.Sprintf("doc-%d", i), doc); err != nil {
			b.Fatal(err)
		}
	}
	return ix
}

func disabledBenchmarkLookupOperations(b *testing.B) {
	ix := benchIndex(b, 10000)
	b.Run("ExactTermSearch", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: Term{"sku", "sku-000777"}, Limit: 10})
		}
	})
	b.Run("ExactTermIterator", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ix.EachTerm("sku", "sku-000777", func(string, DocID) bool { return true })
		}
	})
	b.Run("CollectTermHotPath", func(b *testing.B) {
		dst := make([]string, 0, 3000)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = ix.CollectTerm("tenant", "orgware", dst)
		}
	})
	b.Run("TermsIN", func(b *testing.B) {
		q := Terms{Field: "status", Values: []string{"active", "pending"}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Prefix", func(b *testing.B) {
		q := Prefix{"sku", "sku-000"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Suffix", func(b *testing.B) {
		q := Suffix{"email", "example.com"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("ContainsNgram", func(b *testing.B) {
		q := Contains{"email", "ser77"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Fuzzy", func(b *testing.B) {
		q := Fuzzy{Field: "title", Value: "engin", Distance: 1, LimitTerms: 256}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Range", func(b *testing.B) {
		q := Range{Field: "price", GTE: 100, LT: 200}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Exists", func(b *testing.B) {
		q := Exists{Field: "email"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Count(q)
		}
	})
	b.Run("Missing", func(b *testing.B) {
		q := Missing{Field: "email"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Count(q)
		}
	})
	b.Run("AndConjunction", func(b *testing.B) {
		q := And{Term{"tenant", "orgware"}, Term{"status", "active"}, Range{Field: "price", GTE: 50, LT: 500}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("OrDisjunction", func(b *testing.B) {
		q := Or{Term{"tenant", "orgware"}, Term{"tenant", "acme"}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Not", func(b *testing.B) {
		q := Not{Q: Term{"status", "archived"}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("BoolMustShouldFilterMustNot", func(b *testing.B) {
		q := Bool{Must: []Query{Term{"tenant", "orgware"}}, Should: []Query{Term{"tags", "go"}, Term{"tags", "fast"}}, Filter: []Query{Range{Field: "price", LTE: 500}}, MustNot: []Query{Term{"status", "archived"}}, MinShouldMatch: 1}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Phrase", func(b *testing.B) {
		q := Phrase{Field: "body", Value: "boolean phrase prefix", Slop: 0}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("ProximityOrdered", func(b *testing.B) {
		q := Proximity{Field: "body", Terms: []string{"boolean", "prefix"}, Slop: 2, Ordered: true}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("ProximityUnordered", func(b *testing.B) {
		q := Proximity{Field: "body", Terms: []string{"vector", "boolean"}, Slop: 8, Ordered: false}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("CIDR", func(b *testing.B) {
		q := CIDR{Field: "ip", Value: "10.1.1.0/24"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("DomainWildcard", func(b *testing.B) {
		q := DomainWildcard{Field: "domain", Pattern: "*.example.com"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("VectorCosine", func(b *testing.B) {
		q := VectorQuery{Field: "vec", Vector: []float64{.7, .4, .2, 1}, K: 20, Metric: "cosine"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("FilteredVector", func(b *testing.B) {
		q := VectorQuery{Field: "vec", Vector: []float64{.7, .4, .2, 1}, K: 20, Metric: "cosine", Filter: Term{"tenant", "orgware"}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20})
		}
	})
	b.Run("Sort", func(b *testing.B) {
		q := Term{"status", "active"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20, Sort: []SortField{{Field: "price", Desc: true}, {Field: "rank"}}})
		}
	})
	b.Run("Facets", func(b *testing.B) {
		q := MatchAll{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20, Facets: []string{"tenant", "status", "tags"}})
		}
	})
	b.Run("SearchWithDocs", func(b *testing.B) {
		q := Term{"tenant", "orgware"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Search(SearchRequest{Query: q, Limit: 20, WithDocs: true})
		}
	})
	b.Run("SearchInto", func(b *testing.B) {
		q := Term{"tenant", "orgware"}
		dst := make([]Hit, 0, 32)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, dst = ix.SearchInto(SearchRequest{Query: q, Limit: 20}, dst)
		}
	})
	b.Run("Analyze", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Analyze("title", "Fast lookup engine benchmark")
		}
	})
	b.Run("Highlight", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Highlight("doc-777", "body", "vector", 80)
		}
	})
	b.Run("BM25", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.BM25("body", "lookupx", DocID(2))
		}
	})
	b.Run("Profile", func(b *testing.B) {
		q := Term{"tenant", "orgware"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Profile(q)
		}
	})
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = ix.Get("doc-777")
		}
	})
	b.Run("Count", func(b *testing.B) {
		q := Term{"tenant", "orgware"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Count(q)
		}
	})
}

func disabledBenchmarkIndexingAndDurability(b *testing.B) {
	b.Run("Upsert", func(b *testing.B) {
		ix := benchIndex(b, 0)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.Upsert(fmt.Sprintf("d-%d", i), Document{"title": "fast lookup", "sku": fmt.Sprintf("sku-b-%d", i), "price": float64(i)})
		}
	})
	b.Run("BatchUpsert100", func(b *testing.B) {
		ix := benchIndex(b, 0)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			docs := map[string]Document{}
			for j := 0; j < 100; j++ {
				id := fmt.Sprintf("b-%d-%d", i, j)
				docs[id] = Document{"title": "batch lookup", "sku": "sku-" + id, "price": j}
			}
			_ = ix.BatchUpsert(docs)
		}
	})
	b.Run("Delete", func(b *testing.B) {
		ix := benchIndex(b, 20000)
		ids := make([]string, 20000)
		for i := range ids {
			ids[i] = fmt.Sprintf("doc-%d", i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ix.Delete(ids[i%len(ids)])
		}
	})
	b.Run("Snapshot", func(b *testing.B) {
		ix := benchIndex(b, 1000)
		p := filepath.Join(b.TempDir(), "snap.json")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.SaveSnapshot(p)
		}
	})
	b.Run("Segment", func(b *testing.B) {
		ix := benchIndex(b, 1000)
		p := filepath.Join(b.TempDir(), "seg.lxs")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.SaveSegment(p)
		}
	})
}

func disabledBenchmarkHelperStructures(b *testing.B) {
	words := make([]string, 10000)
	for i := range words {
		words[i] = fmt.Sprintf("term-%06d", i)
	}
	b.Run("RadixPrefix", func(b *testing.B) {
		r := NewRadix()
		for _, w := range words {
			r.Add(w, w)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = r.Prefix("term-000")
		}
	})
	b.Run("ReverseRadixSuffix", func(b *testing.B) {
		r := NewReverseRadix()
		for _, w := range words {
			r.Add(w, w)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = r.Suffix("777")
		}
	})
	b.Run("FSTComplete", func(b *testing.B) {
		f := NewFST()
		for i, w := range words {
			f.Add(w, i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = f.Complete("term-000", 10)
		}
	})
	b.Run("PerfectHash", func(b *testing.B) {
		p := NewPerfectHash(words)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = p.Get("term-000777")
		}
	})
	b.Run("BloomHas", func(b *testing.B) {
		bl := NewBloom(1<<20, 4)
		for _, w := range words {
			bl.Add(w)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = bl.Has("term-000777")
		}
	})
	b.Run("HNSWSearch", func(b *testing.B) {
		h := NewHNSW(16, "cosine")
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 5000; i++ {
			h.Add(fmt.Sprintf("v-%d", i), []float64{rng.Float64(), rng.Float64(), rng.Float64(), rng.Float64()})
		}
		q := []float64{.2, .4, .6, .8}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = h.Search(q, 10)
		}
	})
	b.Run("Quantize", func(b *testing.B) {
		v := []float64{.1, .2, .3, .4, .5, .6, .7, .8}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = QuantizeVector(v)
		}
	})
	b.Run("PostingKernelAND", func(b *testing.B) {
		k := DefaultPostingKernel{}
		a := make([]uint64, 1024)
		c := make([]uint64, 1024)
		dst := make([]uint64, 0, 1024)
		for i := range a {
			a[i] = uint64(i) * 1234567
			c[i] = uint64(i) * 9876543
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = k.And(dst, a, c)
		}
		_ = dst
	})
}
