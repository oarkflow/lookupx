package lookup

import (
	"context"
	"testing"
	"time"
)

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

func TestParseLookupQuery(t *testing.T) {
	q := ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
	if q == nil {
		t.Fatal("nil query")
	}
}
