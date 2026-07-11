package pkg

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const csvSample = `id,ld,cpt_code,charge_amt,is_active
1,"Office or other outpatient visit, established patient",99213,125.50,true
2,Comprehensive metabolic panel,80053,42.00,true
3,"Chest x-ray, two views",71046,88.25,false
`

func TestInferCSVColumns_NonSeekableReplaysFullStream(t *testing.T) {
	cols, r, err := InferCSVColumns(strings.NewReader(csvSample), "id", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 {
		t.Fatalf("expected 4 columns, got %d: %+v", len(cols), cols)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	if buf.String() != csvSample {
		t.Fatalf("rewound stream mismatch:\nwant: %q\n got: %q", csvSample, buf.String())
	}
}

func TestInferCSVColumns_SeekableFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte(csvSample), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cols, r, err := InferCSVColumns(f, "id", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 {
		t.Fatalf("expected 4 columns, got %d: %+v", len(cols), cols)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	if buf.String() != csvSample {
		t.Fatalf("rewound stream mismatch:\nwant: %q\n got: %q", csvSample, buf.String())
	}
}

func TestAutoCSVSource_EndToEnd(t *testing.T) {
	ix, src, err := AutoCSVSource(context.Background(), Config{InitialCapacity: 16}, strings.NewReader(csvSample), "id")
	if err != nil {
		t.Fatalf("AutoCSVSource: %v", err)
	}
	defer ix.Close()

	stats, err := ix.IndexFrom(context.Background(), src, BulkOptions{Name: "charge_master_csv", BatchSize: 10})
	if err != nil {
		t.Fatalf("IndexFrom: %v", err)
	}
	if stats.Indexed != 3 {
		t.Fatalf("expected 3 indexed rows, got %+v", stats)
	}

	_, hits := ix.SearchInto(SearchRequest{Query: Term{Field: "cpt_code", Value: "99213"}, Limit: 10}, nil)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit for cpt_code=99213, got %d", len(hits))
	}

	_, hits = ix.SearchInto(SearchRequest{Query: Simple("ld", "metabolic panel"), Limit: 10}, nil)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit for free-text search on ld, got %d", len(hits))
	}
}

const jsonlSample = `{"id":"1","ld":"Office or other outpatient visit, established patient","cpt_code":"99213","charge_amt":125.5,"is_active":true}
{"id":"2","ld":"Comprehensive metabolic panel","cpt_code":"80053","charge_amt":42,"is_active":true}
{"id":"3","ld":"Chest x-ray, two views","cpt_code":"71046","charge_amt":88.25,"is_active":false}
`

func TestInferJSONLColumns_NonSeekableReplaysFullStream(t *testing.T) {
	cols, r, err := InferJSONLColumns(strings.NewReader(jsonlSample), "id", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 {
		t.Fatalf("expected 4 columns, got %d: %+v", len(cols), cols)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	if buf.String() != jsonlSample {
		t.Fatalf("rewound stream mismatch:\nwant: %q\n got: %q", jsonlSample, buf.String())
	}
}

func TestAutoJSONLSource_EndToEnd(t *testing.T) {
	ix, src, err := AutoJSONLSource(context.Background(), Config{InitialCapacity: 16}, strings.NewReader(jsonlSample), "id")
	if err != nil {
		t.Fatalf("AutoJSONLSource: %v", err)
	}
	defer ix.Close()

	stats, err := ix.IndexFrom(context.Background(), src, BulkOptions{Name: "charge_master_jsonl", BatchSize: 10})
	if err != nil {
		t.Fatalf("IndexFrom: %v", err)
	}
	if stats.Indexed != 3 {
		t.Fatalf("expected 3 indexed rows, got %+v", stats)
	}

	_, hits := ix.SearchInto(SearchRequest{Query: Term{Field: "cpt_code", Value: "99213"}, Limit: 10}, nil)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit for cpt_code=99213, got %d", len(hits))
	}

	_, hits = ix.SearchInto(SearchRequest{Query: Simple("ld", "metabolic panel"), Limit: 10}, nil)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit for free-text search on ld, got %d", len(hits))
	}
}
