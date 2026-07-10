package pkg

import (
	"testing"
	"time"
)

func advancedIndex(t *testing.T) *Index {
	t.Helper()
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"title":  {Kind: FieldText, Indexed: true, Lowercase: true, Analyzer: "stem", Prefix: true, Ngram: true, MinGram: 2, MaxGram: 3},
		"ip":     {Kind: FieldKeyword, Lookup: true},
		"domain": {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"email":  {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"phone":  {Kind: FieldKeyword, Lookup: true},
		"url":    {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"vec":    {Kind: FieldVector, Dim: 3},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("1", Document{"title": "fast running search engine", "ip": "10.0.0.10", "domain": "api.example.com", "email": "USER@Example.COM", "phone": "+977 980-123", "url": "HTTPS://Example.COM/A", "vec": []any{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("2", Document{"title": "slow database lookup", "ip": "192.168.1.1", "domain": "test.local", "vec": []any{0, 1, 0}}); err != nil {
		t.Fatal(err)
	}
	return ix
}

func TestAdvancedQueries(t *testing.T) {
	ix := advancedIndex(t)
	if got := ix.Count(Phrase{Field: "title", Value: "running search", Slop: 0}); got != 1 {
		t.Fatalf("phrase=%d", got)
	}
	if got := ix.Count(Proximity{Field: "title", Terms: []string{"fast", "engine"}, Slop: 3, Ordered: true}); got != 1 {
		t.Fatalf("prox=%d", got)
	}
	if got := ix.Count(CIDR{Field: "ip", Value: "10.0.0.0/24"}); got != 1 {
		t.Fatalf("cidr=%d", got)
	}
	if got := ix.Count(DomainWildcard{Field: "domain", Pattern: "*.example.com"}); got != 1 {
		t.Fatalf("domain=%d", got)
	}
	if got := ix.Count(Term{Field: "email", Value: "user@example.com"}); got != 1 {
		t.Fatalf("email normalize=%d", got)
	}
	if got := ix.Count(VectorQuery{Field: "vec", Vector: []float64{1, 0, 0}, K: 1}); got != 1 {
		t.Fatalf("vector=%d", got)
	}
	if h := ix.Highlight("1", "title", "search", 20); len(h) == 0 {
		t.Fatal("expected highlight")
	}
}

func TestAnalyzerRegistryAndSynonym(t *testing.T) {
	RegisterSynonym("quick", "fast")
	ix, _ := New(Config{Schema: Schema{Fields: map[string]FieldOptions{"body": {Kind: FieldText, Indexed: true, Lowercase: true}}}})
	_ = ix.Upsert("1", Document{"body": "quick lookup"})
	if got := ix.Count(Term{Field: "body", Value: "fast"}); got != 1 {
		t.Fatalf("synonym=%d", got)
	}
}

func TestVectorMetricsExactAndUpdate(t *testing.T) {
	ix, err := New(Config{DisableSource: true, InitialCapacity: 8, Schema: Schema{Fields: map[string]FieldOptions{
		"vec": {Kind: FieldVector, Dim: 2, VectorMetric: "cosine", VectorM: 8, VectorEFSearch: 32},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	v := ix.VectorField("vec")
	w := ix.BeginFast("a")
	w.VectorH(v, []float64{1, 0})
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	w = ix.BeginFast("b")
	w.VectorH(v, []float64{0, 1})
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	w = ix.BeginFast("c")
	w.VectorH(v, []float64{2, 0})
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}

	var hits []Hit
	_, hits = ix.SearchInto(SearchRequest{Query: VectorQuery{Field: "vec", Vector: []float64{1, 0}, K: 3, Metric: "cosine", Exact: true}, Limit: 3}, hits)
	if len(hits) != 3 || hits[0].ID != "a" && hits[0].ID != "c" {
		t.Fatalf("unexpected cosine hits: %+v", hits)
	}
	_, hits = ix.SearchInto(SearchRequest{Query: VectorQuery{Field: "vec", Vector: []float64{1, 0}, K: 3, Metric: "dot", Exact: true}, Limit: 3}, hits[:0])
	if len(hits) == 0 || hits[0].ID != "c" {
		t.Fatalf("dot metric override ignored: %+v", hits)
	}
	_, hits = ix.SearchInto(SearchRequest{Query: VectorQuery{Field: "vec", Vector: []float64{0, 1}, K: 1, Metric: "l2", Exact: true}, Limit: 1}, hits[:0])
	if len(hits) != 1 || hits[0].ID != "b" {
		t.Fatalf("unexpected l2 hit: %+v", hits)
	}
}

func TestVectorRangeFilterDoesNotDeadlock(t *testing.T) {
	ix, err := New(Config{InitialCapacity: 4, Schema: Schema{Fields: map[string]FieldOptions{
		"vec":   {Kind: FieldVector, Dim: 2, VectorMetric: "cosine", VectorEFSearch: 8},
		"price": {Kind: FieldFloat, Sortable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("cheap", Document{"vec": []float64{1, 0}, "price": 5}); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("target", Document{"vec": []float64{0, 1}, "price": 15}); err != nil {
		t.Fatal(err)
	}
	done := make(chan []Hit, 1)
	go func() {
		_, hits := ix.SearchInto(SearchRequest{Query: VectorQuery{Field: "vec", Vector: []float64{0, 1}, K: 2, Filter: Range{Field: "price", GTE: 10, LTE: 20}}, Limit: 1}, nil)
		done <- hits
	}()
	select {
	case hits := <-done:
		if len(hits) != 1 || hits[0].ID != "target" {
			t.Fatalf("unexpected filtered vector hits: %+v", hits)
		}
	case <-time.After(time.Second):
		t.Fatal("filtered vector search deadlocked")
	}
}

func TestWireVectorFilter(t *testing.T) {
	q := WireQuery{Type: "vector", Field: "vec", Vector: []float64{1, 0}, K: 10, Filter: []WireQuery{{Type: "term", Field: "tenant", Value: "orgware"}}}.ToQuery()
	vq, ok := q.(VectorQuery)
	if !ok {
		t.Fatalf("expected VectorQuery, got %T", q)
	}
	if vq.Filter == nil {
		t.Fatal("wire vector filter was not preserved")
	}
}
