package lookup

import (
	"path/filepath"
	"testing"
	"time"
)

func testIndex(t *testing.T) *Index {
	t.Helper()
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"name":       {Kind: FieldText, Indexed: true, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, Lowercase: true, MinGram: 2, MaxGram: 3},
		"email":      {Kind: FieldKeyword, Lookup: true, Prefix: true, Suffix: true, Ngram: true, Lowercase: true, MinGram: 2, MaxGram: 4},
		"tenant":     {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"status":     {Kind: FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
		"price":      {Kind: FieldFloat, Lookup: true, Sortable: true, Facetable: true},
		"rank":       {Kind: FieldInt, Lookup: true, Sortable: true},
		"sku":        {Kind: FieldKeyword, Lookup: true, Lowercase: true, Unique: true},
		"expires_at": {Kind: FieldTime, TTLField: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ix
}
func seed(t *testing.T, ix *Index) {
	t.Helper()
	docs := map[string]Document{"1": {"name": "Sujit Shrestha", "email": "sujit@example.com", "tenant": "orgware", "status": "active", "price": 10.5, "rank": 2, "sku": "sku-1"}, "2": {"name": "John Smith", "email": "john@gmail.com", "tenant": "orgware", "status": "inactive", "price": 99.0, "rank": 1, "sku": "sku-2"}, "3": {"name": "Jane Doe", "email": "jane@example.com", "tenant": "acme", "status": "active", "price": 50.0, "rank": 3, "sku": "sku-3"}}
	for id, d := range docs {
		if err := ix.Upsert(id, d); err != nil {
			t.Fatal(err)
		}
	}
}
func TestExactPrefixSuffixContainsFuzzy(t *testing.T) {
	ix := testIndex(t)
	seed(t, ix)
	cases := []struct {
		name string
		q    Query
		want int
	}{
		{"exact", Term{"email", "sujit@example.com"}, 1}, {"prefix", Prefix{"name", "su"}, 1}, {"suffix", Suffix{"email", "gmail.com"}, 1}, {"contains", Contains{"email", "exam"}, 2}, {"fuzzy", Fuzzy{"name", "sujjt", 1, 0}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ix.Search(SearchRequest{Query: tc.q, Limit: 10})
			if got.Total != tc.want {
				t.Fatalf("got %d want %d", got.Total, tc.want)
			}
		})
	}
}
func TestBoolAndFilters(t *testing.T) {
	ix := testIndex(t)
	seed(t, ix)
	res := ix.Search(SearchRequest{Query: Bool{Must: []Query{Prefix{"name", "j"}}, Filter: []Query{Term{"tenant", "orgware"}}, MustNot: []Query{Term{"status", "inactive"}}}, Limit: 10})
	if res.Total != 0 {
		t.Fatalf("got %d", res.Total)
	}
	res = ix.Search(SearchRequest{Query: Bool{Should: []Query{Term{"tenant", "orgware"}, Term{"tenant", "acme"}}, MinShouldMatch: 1}, Limit: 10})
	if res.Total != 3 {
		t.Fatalf("got %d", res.Total)
	}
}

func TestRangeTermsMissingSortFacetsUniqueBatchAnalyze(t *testing.T) {
	ix := testIndex(t)
	seed(t, ix)
	if got := ix.Search(SearchRequest{Query: Range{Field: "price", GTE: 10, LT: 60}, Limit: 10}); got.Total != 2 {
		t.Fatalf("range got %d", got.Total)
	}
	if got := ix.Search(SearchRequest{Query: Terms{Field: "tenant", Values: []string{"orgware", "missing"}}, Limit: 10}); got.Total != 2 {
		t.Fatalf("terms got %d", got.Total)
	}
	if got := ix.Search(SearchRequest{Query: Missing{Field: "email"}, Limit: 10}); got.Total != 0 {
		t.Fatalf("missing got %d", got.Total)
	}
	res := ix.Search(SearchRequest{Query: MatchAll{}, Limit: 3, Sort: []SortField{{Field: "price", Desc: true}}, Facets: []string{"status"}})
	if len(res.Hits) != 3 || res.Hits[0].ID != "2" {
		t.Fatalf("sort failed: %#v", res.Hits)
	}
	if len(res.Facets["status"]) != 2 || res.Facets["status"][0].Value != "active" || res.Facets["status"][0].Count != 2 {
		t.Fatalf("facets failed: %#v", res.Facets)
	}
	if err := ix.Upsert("4", Document{"name": "Duplicate", "sku": "sku-1"}); err == nil {
		t.Fatal("expected unique constraint violation")
	}
	if err := ix.BatchUpsert(map[string]Document{"4": {"name": "Batch One", "tenant": "acme", "status": "active", "sku": "sku-4"}}); err != nil {
		t.Fatal(err)
	}
	if ix.Count(Term{Field: "tenant", Value: "acme"}) != 2 {
		t.Fatalf("count failed")
	}
	if toks := ix.Analyze("name", "Fast Lookup Engine"); len(toks) != 3 || toks[0].Term != "fast" {
		t.Fatalf("analyze failed: %#v", toks)
	}
}

func TestDeleteTTLAndSnapshotWAL(t *testing.T) {
	ix := testIndex(t)
	seed(t, ix)
	_ = ix.Delete("2")
	if _, ok := ix.Get("2"); ok {
		t.Fatal("deleted doc returned")
	}
	past := time.Now().Add(-time.Hour).Unix()
	_ = ix.Upsert("4", Document{"name": "Expired", "expires_at": past})
	if res := ix.Search(SearchRequest{Query: Term{"name", "expired"}, Limit: 10}); res.Total != 0 {
		t.Fatalf("ttl got %d", res.Total)
	}
	p := filepath.Join(t.TempDir(), "snap.json")
	if err := ix.SaveSnapshot(p); err != nil {
		t.Fatal(err)
	}
	ix2 := testIndex(t)
	if err := ix2.LoadSnapshot(p); err != nil {
		t.Fatal(err)
	}
	if res := ix2.Search(SearchRequest{Query: MatchAll{}, Limit: 10}); res.Total != 2 {
		t.Fatalf("snapshot got %d", res.Total)
	}
}
func disabledBenchmarkExactLookup(b *testing.B) {
	ix := testIndex(&testing.T{})
	for i := 0; i < 10000; i++ {
		_ = ix.Upsert(string(rune(i+1)), Document{"email": "user" + string(rune(i+1)) + "@example.com", "tenant": "t", "status": "active"})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ix.Search(SearchRequest{Query: Term{"tenant", "t"}, Limit: 10})
	}
}

func disabledBenchmarkCollectTermHotPath(b *testing.B) {
	ix := testIndex(&testing.T{})
	for i := 0; i < 10000; i++ {
		_ = ix.Upsert(string(rune(i+1)), Document{"tenant": "t", "status": "active"})
	}
	dst := make([]string, 0, 10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = ix.CollectTerm("tenant", "t", dst)
	}
}
