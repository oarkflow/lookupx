package pkg

import (
	"context"
	"database/sql/driver"
	"path/filepath"
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

func TestTextAndStoredAliasesSurvivePersistentRoundTrip(t *testing.T) {
	cfg := Config{DisableSource: true, Schema: Schema{Fields: map[string]FieldOptions{
		"description":   {Kind: FieldText, Indexed: true, Stored: true, Lowercase: true, IndexAlias: "fulltext"},
		"fulltext":      {Kind: FieldText, Indexed: true, Stored: true, Lowercase: true},
		"work_item":     {Kind: FieldKeyword, Stored: true, StoredOnly: true, StoredAlias: "work_item_uid"},
		"work_item_uid": {Kind: FieldKeyword, Indexed: true, Stored: true, Lookup: true},
	}}}
	ix, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rec := SourceRecord{ID: "1", Seq: 1}
	rec.AddText(ix.FieldID("description"), "Office Visit", true)
	rec.AddText(ix.FieldID("fulltext"), "Office Visit", true)
	rec.AddKeyword(ix.FieldID("work_item"), "42", false)
	rec.AddKeyword(ix.FieldID("work_item_uid"), "42", true)
	if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: []SourceRecord{rec}}, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := ix.fields["fulltext"].totalTokens; got != 2 {
		t.Fatalf("text alias duplicated token statistics: totalTokens=%d", got)
	}
	if got := ix.Count(Prefix{Field: "fulltext", Value: "offic"}); got != 1 {
		t.Fatalf("text prefix query count=%d", got)
	}
	if got := ix.Count(Contains{Field: "fulltext", Value: "ffic"}); got != 1 {
		t.Fatalf("text infix query count=%d", got)
	}
	if got := ix.Count(Suffix{Field: "fulltext", Value: "fice"}); got != 1 {
		t.Fatalf("text suffix query count=%d", got)
	}
	store := FileSegmentStore{Root: filepath.Join(t.TempDir(), "index")}
	if _, err := ix.SavePersistent(context.Background(), store, "aliases"); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := OpenPersistent(context.Background(), store, "aliases", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Count(Term{Field: "description", Value: "office"}); got != 1 {
		t.Fatalf("alias text query count=%d", got)
	}
	if got := loaded.Count(Prefix{Field: "fulltext", Value: "offic"}); got != 1 {
		t.Fatalf("restored text prefix query count=%d", got)
	}
	if got := loaded.Count(Contains{Field: "fulltext", Value: "ffic"}); got != 1 {
		t.Fatalf("restored text infix query count=%d", got)
	}
	if got := loaded.Count(Suffix{Field: "fulltext", Value: "fice"}); got != 1 {
		t.Fatalf("restored text suffix query count=%d", got)
	}
	doc, ok := loaded.Get("1")
	if !ok || doc["description"] != "Office Visit" || doc["fulltext"] != "Office Visit" || doc["work_item"] != "42" || doc["work_item_uid"] != "42" {
		t.Fatalf("stored aliases were not restored: ok=%v doc=%#v", ok, doc)
	}
}

func TestTextPatternDictionaryRefreshesAfterWrites(t *testing.T) {
	ix, err := New(Config{DisableSource: true, Schema: Schema{Fields: map[string]FieldOptions{
		"body": {Kind: FieldText, Indexed: true, Stored: true, Lowercase: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	index := func(id, body string) {
		rec := SourceRecord{ID: id}
		rec.AddText(ix.FieldID("body"), body, true)
		if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: []SourceRecord{rec}}, BulkOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	index("1", "Office visit")
	if got := ix.Count(Prefix{Field: "body", Value: "offic"}); got != 1 {
		t.Fatalf("initial text prefix query count=%d", got)
	}
	index("2", "Official visit")
	if got := ix.Count(Prefix{Field: "body", Value: "offic"}); got != 2 {
		t.Fatalf("text prefix query after new term count=%d", got)
	}
	index("3", "Office visit")
	if got := ix.Count(Prefix{Field: "body", Value: "offic"}); got != 3 {
		t.Fatalf("cached text prefix query after repeated terms count=%d", got)
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

func TestStoredOnlyColumnsRemainInResultsWithoutSourceCopies(t *testing.T) {
	ix, err := New(Config{DisableSource: true, Schema: Schema{Fields: map[string]FieldOptions{
		"label": {Kind: FieldText, Stored: true, StoredOnly: true, Lowercase: true},
		"body":  {Kind: FieldText, Stored: true, Lowercase: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	label, body := ix.FieldID("label"), ix.FieldID("body")
	record := SourceRecord{ID: "doc-1", Seq: 1}
	record.AddText(label, "Office Visit", true)
	record.AddText(body, "Established patient", true)
	if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: []SourceRecord{record}}, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := ix.Count(Term{Field: "label", Value: "office"}); got != 0 {
		t.Fatalf("stored-only field unexpectedly indexed terms: count=%d", got)
	}
	_, hits := ix.SearchInto(SearchRequest{Query: Term{Field: "body", Value: "established"}, Limit: 1, WithDocs: true}, nil)
	if len(hits) != 1 || hits[0].Doc["label"] != "Office Visit" || hits[0].Doc["body"] != "Established patient" {
		t.Fatalf("stored fields were not reconstructed in results: %#v", hits)
	}
	got, ok := ix.Get("doc-1")
	if !ok || got["label"] != "Office Visit" || got["body"] != "Established patient" {
		t.Fatalf("stored fields were not reconstructed by Get: ok=%v doc=%#v", ok, got)
	}
}

func TestParseLookupQuery(t *testing.T) {
	q := ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
	if q == nil {
		t.Fatal("nil query")
	}
}
