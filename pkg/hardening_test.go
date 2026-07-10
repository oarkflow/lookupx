package pkg

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestVectorDimensionValidationNoMutation(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"vec": {Kind: FieldVector, Dim: 3, VectorMetric: "cosine"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("a", Document{"vec": []float64{1, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("a", Document{"vec": []float64{1, 0}}); err == nil {
		t.Fatal("expected dimension mismatch")
	}
	_, hits := ix.SearchInto(SearchRequest{Query: VectorQuery{Field: "vec", Vector: []float64{1, 0, 0}, K: 1, Exact: true}, Limit: 1}, nil)
	if len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("existing vector was mutated after failed upsert: %#v", hits)
	}
}

func TestVectorCompactionDropsStaleNodes(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"vec": {Kind: FieldVector, Dim: 2, VectorMetric: "cosine"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := ix.Upsert("same", Document{"vec": []float64{float64(i + 1), 0}}); err != nil {
			t.Fatal(err)
		}
	}
	before := ix.Stats()
	if before.VectorTombstones == 0 {
		t.Fatalf("expected stale vector nodes before compaction: %#v", before)
	}
	if err := ix.Compact(); err != nil {
		t.Fatal(err)
	}
	after := ix.Stats()
	if after.Vectors != 1 || after.VectorNodes != 1 || after.VectorTombstones != 0 {
		t.Fatalf("unexpected vector stats after compaction: %#v", after)
	}
}

func TestStrictWALRecoveryDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	wal := filepath.Join(dir, "index.wal")
	ix, err := New(Config{EnableWAL: true, WALPath: wal, WALSyncEveryWrite: true, Schema: Schema{Fields: map[string]FieldOptions{"tenant": {Kind: FieldKeyword}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert("a", Document{"tenant": "orgware"}); err != nil {
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(wal, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("{bad json}\n")
	_ = f.Close()
	_, err = Open(Config{EnableWAL: true, WALPath: wal, StrictRecovery: true, Schema: Schema{Fields: map[string]FieldOptions{"tenant": {Kind: FieldKeyword}}}})
	if err == nil {
		t.Fatal("expected strict WAL recovery error")
	}
}

func TestHTTPVectorValidationAndCompactionEndpoint(t *testing.T) {
	ix, err := New(Config{MaxRequestBytes: 1024, APIKeys: []string{"secret"}, Schema: Schema{Fields: map[string]FieldOptions{
		"vec": {Kind: FieldVector, Dim: 2},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Index: ix}
	body, _ := json.Marshal(Document{"vec": []float64{1}})
	req := httptest.NewRequest(http.MethodPut, "/docs/a", bytes.NewReader(body))
	req.Header.Set("X-API-Key", "secret")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad vector dimension, got %d body=%s", rr.Code, rr.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/compact", nil)
	req.Header.Set("X-API-Key", "secret")
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("compact failed: %d %s", rr.Code, rr.Body.String())
	}
}
