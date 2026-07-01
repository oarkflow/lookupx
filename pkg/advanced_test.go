package pkg

import "testing"

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
