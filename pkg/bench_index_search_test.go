package pkg

import (
	"strconv"
	"testing"
	"time"
)

func perfConfig() Config {
	return Config{DisableSource: true, InitialCapacity: 10000, Schema: Schema{Fields: map[string]FieldOptions{
		"title":      {Kind: FieldText, Indexed: true, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 3, MaxGram: 4},
		"body":       {Kind: FieldText, Indexed: true, Lookup: true, Lowercase: true, Phrase: true},
		"email":      {Kind: FieldKeyword, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Lowercase: true, MinGram: 3, MaxGram: 4},
		"tenant":     {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"status":     {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"price":      {Kind: FieldFloat, Sortable: true},
		"rank":       {Kind: FieldInt, Sortable: true},
		"sku":        {Kind: FieldKeyword, Lookup: true, Prefix: true, Lowercase: true, Unique: true},
		"domain":     {Kind: FieldKeyword, Lookup: true, Suffix: true, Lowercase: true},
		"ip":         {Kind: FieldKeyword, Lookup: true},
		"created_at": {Kind: FieldTime, Sortable: true},
		"vec":        {Kind: FieldVector, Dim: 4},
	}}}
}

func perfDoc(i int) Document {
	return Document{
		"title":      "fast go lookup engine",
		"body":       "boolean phrase prefix suffix fuzzy contains vector search",
		"email":      "user" + strconv.Itoa(i) + "@example.com",
		"tenant":     [...]string{"orgware", "acme", "globex", "initech"}[i&3],
		"status":     [...]string{"active", "pending", "archived", "active"}[i&3],
		"price":      float64(i % 1000),
		"rank":       i & 127,
		"sku":        "sku-" + strconv.Itoa(i),
		"domain":     "sub" + strconv.Itoa(i%256) + ".example.com",
		"ip":         "10.1." + strconv.Itoa((i>>8)&255) + "." + strconv.Itoa(i&255),
		"created_at": time.Unix(int64(1700000000+i), 0).Unix(),
		"vec":        []float64{float64(i&7) / 7, float64(i&3) / 3, float64(i&15) / 15, 1},
	}
}

func seedPerfIndex(b *testing.B, n int) *Index {
	b.Helper()
	ix, err := New(perfConfig())
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := ix.Upsert("doc-"+strconv.Itoa(i), perfDoc(i)); err != nil {
			b.Fatal(err)
		}
	}
	return ix
}

func BenchmarkIndexing(b *testing.B) {
	b.Run("FastUpsertKeywordNumeric", func(b *testing.B) {
		ix, err := New(Config{DisableSource: true, CollectTook: false, Clock: StaticClock{T: time.Unix(1700000000, 0)}, InitialCapacity: b.N, Schema: Schema{Fields: map[string]FieldOptions{
			"sku":    {Kind: FieldKeyword, Lookup: true, Lowercase: true, Unique: true},
			"tenant": {Kind: FieldKeyword, Lookup: true, Lowercase: true},
			"price":  {Kind: FieldFloat, Lookup: true, Sortable: true},
		}}})
		if err != nil {
			b.Fatal(err)
		}
		skuH := ix.KeywordField("sku")
		tenantH := ix.KeywordField("tenant")
		priceH := ix.NumericField("price")
		ids := make([]string, b.N)
		skus := make([]string, b.N)
		for i := 0; i < b.N; i++ {
			ids[i] = "doc-" + strconv.Itoa(i)
			skus[i] = "sku-" + strconv.Itoa(i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id, sk := ids[i], skus[i]
			if err := ix.UpsertKeywordNumericFast(id, skuH, tenantH, priceH, sk, "orgware", float64(i)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("GenericMapUpsertKeywordNumeric", func(b *testing.B) {
		ix, err := New(Config{DisableSource: true, CollectTook: false, Clock: StaticClock{T: time.Unix(1700000000, 0)}, InitialCapacity: b.N, Schema: Schema{Fields: map[string]FieldOptions{
			"sku":    {Kind: FieldKeyword, Lookup: true, Lowercase: true, Unique: true},
			"tenant": {Kind: FieldKeyword, Lookup: true, Lowercase: true},
			"price":  {Kind: FieldFloat, Lookup: true, Sortable: true},
		}}})
		if err != nil {
			b.Fatal(err)
		}
		ids := make([]string, b.N)
		docs := make([]Document, b.N)
		for i := 0; i < b.N; i++ {
			ids[i] = "doc-" + strconv.Itoa(i)
			docs[i] = Document{"sku": "sku-" + strconv.Itoa(i), "tenant": "orgware", "price": float64(i)}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := ix.Upsert(ids[i], docs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("FastUpsertTextLookup", func(b *testing.B) {
		ix, err := New(Config{DisableSource: true, CollectTook: false, Clock: StaticClock{T: time.Unix(1700000000, 0)}, InitialCapacity: b.N, Schema: Schema{Fields: map[string]FieldOptions{
			"title": {Kind: FieldText, Indexed: true, Lookup: true, Prefix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 3, MaxGram: 4},
			"sku":   {Kind: FieldKeyword, Lookup: true, Lowercase: true, Unique: true},
		}}})
		if err != nil {
			b.Fatal(err)
		}
		titleH := ix.KeywordField("title")
		skuH := ix.KeywordField("sku")
		ids := make([]string, b.N)
		skus := make([]string, b.N)
		for i := 0; i < b.N; i++ {
			ids[i] = "doc-" + strconv.Itoa(i)
			skus[i] = "sku-" + strconv.Itoa(i)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id, sk := ids[i], skus[i]
			w := ix.BeginFast(id)
			w.TextHNormalized(titleH, "fast go lookup engine")
			w.KeywordHNormalized(skuH, sk)
			if err := w.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("FastBatch100KeywordNumeric", func(b *testing.B) {
		ix, err := New(Config{DisableSource: true, CollectTook: false, Clock: StaticClock{T: time.Unix(1700000000, 0)}, InitialCapacity: b.N * 100, Schema: Schema{Fields: map[string]FieldOptions{
			"sku":    {Kind: FieldKeyword, Lookup: true, Lowercase: true, Unique: true},
			"tenant": {Kind: FieldKeyword, Lookup: true, Lowercase: true},
			"price":  {Kind: FieldFloat, Sortable: true},
		}}})
		if err != nil {
			b.Fatal(err)
		}
		skuH := ix.KeywordField("sku")
		tenantH := ix.KeywordField("tenant")
		priceH := ix.NumericField("price")
		batchesID := make([][]string, b.N)
		batchesSKU := make([][]string, b.N)
		base := 0
		for i := 0; i < b.N; i++ {
			ids := make([]string, 100)
			skus := make([]string, 100)
			for j := 0; j < 100; j++ {
				id := base + j
				ids[j] = "doc-" + strconv.Itoa(id)
				skus[j] = "sku-" + strconv.Itoa(id)
			}
			base += 100
			batchesID[i] = ids
			batchesSKU[i] = skus
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ids, skus := batchesID[i], batchesSKU[i]
			if err := ix.BatchUpsertKeywordNumericFast(ids, skus, skuH, tenantH, priceH, "orgware", nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSearch(b *testing.B) {
	ix := seedPerfIndex(b, 10000)
	hits := make([]Hit, 0, 64)
	ids := make([]string, 0, 4096)
	b.Run("ExactUnique", func(b *testing.B) {
		q := Term{Field: "sku", Value: "sku-777"}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 10}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("ExactHighCardinality", func(b *testing.B) {
		q := Term{Field: "tenant", Value: "orgware"}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("IteratorHighCardinality", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ix.EachTerm("tenant", "orgware", func(string, DocID) bool { return true })
		}
	})
	b.Run("CollectHighCardinality", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ids = ix.CollectTerm("tenant", "orgware", ids)
		}
	})
	b.Run("TermsIN", func(b *testing.B) {
		q := Terms{Field: "status", Values: []string{"active", "pending"}}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("Prefix", func(b *testing.B) {
		q := Prefix{Field: "sku", Value: "sku-77"}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("Contains", func(b *testing.B) {
		q := Contains{Field: "email", Value: "777"}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("Fuzzy", func(b *testing.B) {
		q := Fuzzy{Field: "title", Value: "engin", Distance: 1, LimitTerms: 64}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("GlobalTermBM25", func(b *testing.B) {
		q := GlobalTerm{Words: []string{"engine"}}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20, WithDocs: true}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("GlobalTermFuzzyBM25", func(b *testing.B) {
		q := GlobalTerm{Words: []string{"engin"}, Fuzzy: true}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20, WithDocs: true}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("Range", func(b *testing.B) {
		q := Range{Field: "price", GTE: 100, LT: 200}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("BoolFilter", func(b *testing.B) {
		q := Bool{Must: []Query{Term{Field: "tenant", Value: "orgware"}}, Filter: []Query{Range{Field: "price", GTE: 100, LT: 900}}, MustNot: []Query{Term{Field: "status", Value: "archived"}}}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("Phrase", func(b *testing.B) {
		q := Phrase{Field: "body", Value: "phrase prefix suffix", Slop: 0}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("CIDR", func(b *testing.B) {
		q := CIDR{Field: "ip", Value: "10.1.1.0/24"}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("VectorFiltered", func(b *testing.B) {
		q := VectorQuery{Field: "vec", Vector: []float64{.7, .3, .2, 1}, K: 20, Metric: "dot", Filter: Term{Field: "tenant", Value: "orgware"}}
		b.ReportAllocs()
		req := SearchRequest{Query: q, Limit: 20}
		for i := 0; i < b.N; i++ {
			_, hits = ix.SearchInto(req, hits)
		}
	})
	b.Run("Count", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ix.CountTerm("tenant", "orgware")
		}
	})
}
