package lookup

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestTupleCompositeAndPersistentStore(t *testing.T) {
	ix, err := New(Config{Schema: TupleLookupSchema(), DisableSource: true, InitialCapacity: 128, Clock: StaticClock{T: time.Unix(0, 0)}})
	if err != nil {
		t.Fatal(err)
	}
	ix.EnableTupleComposite()
	term := ix.FieldID("term")
	wi := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	rows := make([]SourceRecord, 0, 10)
	for i := 0; i < 10; i++ {
		r := SourceRecord{ID: fmt.Sprintf("enc-%d", i), Seq: uint64(i + 1)}
		r.AddKeyword(term, "key-special", true)
		r.AddKeyword(wi, "4", true)
		r.AddKeyword(date_key, "2026-01-01", true)
		rows = append(rows, r)
	}
	_, err = ix.IndexFrom(context.Background(), SliceSource{Records: rows}, BulkOptions{Name: "test", BatchSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	_, hits := ix.SearchInto(SearchRequest{Query: ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01"), Limit: 3}, nil)
	if len(hits) != 3 {
		t.Fatalf("expected 3 limited hits, got %d", len(hits))
	}
	if err := ix.Freeze(); err != nil {
		t.Fatal(err)
	}
	store := FileSegmentStore{Root: filepath.Join(t.TempDir(), "data")}
	man, err := ix.SavePersistent(context.Background(), store, "dataset_a")
	if err != nil {
		t.Fatal(err)
	}
	if man.Docs != 10 {
		t.Fatalf("docs=%d", man.Docs)
	}
	got, _, err := OpenPersistent(context.Background(), store, "dataset_a", Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, hits = got.SearchInto(SearchRequest{Query: ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01"), Limit: 20}, nil)
	if len(hits) != 10 {
		t.Fatalf("after load expected 10 hits, got %d", len(hits))
	}
}

func TestTaskManagerCancelMissing(t *testing.T) {
	tm := NewTaskManager()
	if tm.Cancel("none") {
		t.Fatal("cancel of missing task returned true")
	}
	if len(tm.List()) != 0 {
		t.Fatal("expected no tasks")
	}
}
