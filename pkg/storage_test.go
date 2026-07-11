package pkg

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

func TestSQLSourceTimeWithoutLayoutAcceptsRFC3339(t *testing.T) {
	db := openFakeDB(t, &fakeDataset{
		cols: []fakeColSpec{{name: "id", dbType: "BIGINT"}, {name: "effective_date", dbType: "TIMESTAMPTZ"}},
		rows: [][]driver.Value{{int64(1), "2018-01-01T00:00:00Z"}},
	})
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"effective_date": {Kind: FieldTime, Indexed: true, Stored: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	src := SQLSource{
		DB: db, Query: "SELECT id, effective_date FROM charge_master", IDColumn: "id",
		Columns: []SQLColumn{{Column: "effective_date", Field: ix.FieldID("effective_date"), Kind: ValueTimeUnix}},
	}
	if _, err := ix.IndexFrom(context.Background(), src, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	doc, ok := ix.Get("1")
	if !ok || doc["effective_date"] != "2018-01-01T00:00:00Z" {
		t.Fatalf("unexpected document: ok=%v doc=%#v", ok, doc)
	}
}

func TestIndexFromSliceTupleQuery(t *testing.T) {
	ix, err := New(Config{DisableSource: true, InitialCapacity: 8, Clock: StaticClock{T: time.Unix(1700000000, 0)}, Schema: TupleLookupSchema()})
	if err != nil {
		t.Fatal(err)
	}
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	recs := make([]SourceRecord, 0, 3)
	for i, row := range []struct{ id, code, work, date_key string }{{"1", "key-special", "4", "2026-01-01"}, {"2", "key-special", "5", "2026-01-01"}, {"3", "ab9", "4", "2026-01-02"}} {
		r := SourceRecord{ID: row.id, Seq: uint64(i + 1)}
		r.AddKeyword(term, row.code, true)
		r.AddKeyword(work, row.work, true)
		r.AddKeyword(date_key, row.date_key, true)
		recs = append(recs, r)
	}
	stats, err := ix.IndexFrom(context.Background(), SliceSource{Records: recs}, BulkOptions{Name: "records", BatchSize: 2, Checkpoint: NewMemoryCheckpoint(), Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 3 {
		t.Fatalf("indexed=%d", stats.Indexed)
	}
	_, hits := ix.SearchInto(SearchRequest{Query: TupleQuery("key-special", "4", "2026-01-01"), Limit: 10}, nil)
	if len(hits) != 1 || hits[0].ID != "1" {
		t.Fatalf("unexpected hits: %#v", hits)
	}
}

// TestIndexFromSlicePopulatesDocs guards against a regression where bulk
// ingestion (IndexFrom / SQL / CSV / JSONL sources) wrote indexed field
// values but never reconstructed a Document, so WithDocs search/lookup
// silently returned no doc for any bulk-loaded record even with
// DisableSource left false.
func TestIndexFromSlicePopulatesDocs(t *testing.T) {
	ix, err := New(Config{InitialCapacity: 8, Clock: StaticClock{T: time.Unix(1700000000, 0)}, Schema: TupleLookupSchema()})
	if err != nil {
		t.Fatal(err)
	}
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	dateKey := ix.FieldID("date_key")
	r := SourceRecord{ID: "1", Seq: 1}
	r.AddKeyword(term, "key-special", true)
	r.AddKeyword(work, "4", true)
	r.AddKeyword(dateKey, "2026-01-01", true)

	stats, err := ix.IndexFrom(context.Background(), SliceSource{Records: []SourceRecord{r}}, BulkOptions{Name: "records", BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 1 {
		t.Fatalf("indexed=%d", stats.Indexed)
	}

	_, hits := ix.SearchInto(SearchRequest{Query: TupleQuery("key-special", "4", "2026-01-01"), Limit: 10, WithDocs: true}, nil)
	if len(hits) != 1 {
		t.Fatalf("unexpected hits: %#v", hits)
	}
	if hits[0].Doc == nil {
		t.Fatal("expected WithDocs to return a populated document for a bulk-ingested record")
	}
	if hits[0].Doc["term"] != "key-special" || hits[0].Doc["group_id"] != "4" || hits[0].Doc["date_key"] != "2026-01-01" {
		t.Fatalf("unexpected doc contents: %#v", hits[0].Doc)
	}
	if _, ok := hits[0].Doc["partition_id"]; !ok {
		t.Fatalf("expected sparse inferred field to remain in result shape: %#v", hits[0].Doc)
	}

	doc, ok := ix.Get("1")
	if !ok || doc["term"] != "key-special" {
		t.Fatalf("Get did not return the bulk-ingested document: ok=%v doc=%#v", ok, doc)
	}
}

func TestParseLookupQuery(t *testing.T) {
	q := ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
	if q == nil {
		t.Fatal("nil query")
	}
}
