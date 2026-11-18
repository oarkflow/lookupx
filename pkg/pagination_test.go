package pkg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSearchAfterDocIDPagesWithoutDuplicates(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{"kind": {Kind: FieldKeyword, Lookup: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 9; i++ {
		if err := ix.Upsert(string(rune('a'+i)), Document{"kind": "same"}); err != nil {
			t.Fatal(err)
		}
	}
	q := MatchAll{}
	var all []DocID
	var after DocID
	for {
		res := ix.Search(SearchRequest{Query: q, Limit: 3, AfterDocID: after})
		if len(res.Hits) == 0 {
			break
		}
		for _, h := range res.Hits {
			all = append(all, h.DocID)
		}
		after = res.Hits[len(res.Hits)-1].DocID
	}
	if len(all) != 9 {
		t.Fatalf("got %d paged hits, want 9", len(all))
	}
	for i, id := range all {
		if int(id) != i+1 {
			t.Fatalf("page %d got doc %d", i, id)
		}
	}
}

func TestBitmapEachAfterDenseAndSparse(t *testing.T) {
	for _, dense := range []bool{false, true} {
		b := NewBitmap()
		for i := DocID(1); i <= 10000; i++ {
			if dense || i%1000 == 0 {
				b.Add(i)
			}
		}
		var got []DocID
		b.EachAfter(4999, func(id DocID) bool { got = append(got, id); return true })
		if len(got) == 0 || got[0] <= 4999 {
			t.Fatalf("dense=%v first=%v", dense, got)
		}
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Fatalf("not ordered: %v", got)
			}
		}
	}
}

func TestIndexFromPublishesEachCompletedBatch(t *testing.T) {
	ix, err := New(Config{DisableSource: true, Schema: Schema{Fields: map[string]FieldOptions{"kind": {Kind: FieldKeyword, Lookup: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	fid := ix.FieldID("kind")
	records := make([]SourceRecord, 5)
	for i := range records {
		records[i].ID = string(rune('a' + i))
		records[i].Seq = uint64(i + 1)
		records[i].AddKeyword(fid, "visible", true)
	}
	var visible []int
	stats, err := ix.IndexFrom(context.Background(), SliceSource{Records: records}, BulkOptions{BatchSize: 2, Progress: func(p BulkProgress) {
		count := ix.Count(Term{Field: "kind", Value: "visible"})
		visible = append(visible, count)
		if uint64(count) != p.Indexed {
			t.Errorf("progress reports %d indexed, query sees %d", p.Indexed, count)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 5 || len(visible) != 3 || visible[0] != 2 || visible[1] != 4 || visible[2] != 5 {
		t.Fatalf("indexed=%d visible=%v", stats.Indexed, visible)
	}
}

func TestSearchAfterCanSkipExpensiveTotal(t *testing.T) {
	ix, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := ix.Upsert(string(rune('a'+i)), Document{}); err != nil {
			t.Fatal(err)
		}
	}
	res, hits := ix.SearchInto(SearchRequest{Query: MatchAll{}, Limit: 2, AfterDocID: 2, SkipTotal: true}, nil)
	if res.Total != -1 {
		t.Fatalf("total=%d, want -1 when skipped", res.Total)
	}
	if len(hits) != 2 || hits[0].DocID != 3 || hits[1].DocID != 4 {
		t.Fatalf("hits=%v", hits)
	}
}

func TestHTTPKeysetPaginationCanSkipTotal(t *testing.T) {
	ix, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := ix.Upsert(string(rune('a'+i)), Document{}); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/search", strings.NewReader(`{"type":"all","limit":2,"after_doc_id":2,"skip_total":true}`))
	rec := httptest.NewRecorder()
	serveFHTest(t, rec, &Server{Index: ix}, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var got Result
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Total != -1 || len(got.Hits) != 2 || got.Hits[0].DocID != 3 {
		t.Fatalf("result=%+v", got)
	}
}

func TestDateOnlyRangeBoundsMatchTimeField(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"event_dos": {Kind: FieldTime, Sortable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	fid := ix.FieldID("event_dos")
	var records []SourceRecord
	for i, value := range []string{"2024-12-31", "2025-01-01", "2025-01-15", "2025-02-01"} {
		day, err := time.Parse("2006-01-02", value)
		if err != nil {
			t.Fatal(err)
		}
		record := SourceRecord{ID: value}
		record.AddUnixTime(fid, day.Unix())
		record.Seq = uint64(i + 1)
		records = append(records, record)
	}
	if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: records}, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	res := ix.Search(SearchRequest{Query: Range{Field: "event_dos", GTE: "2025-01-01", LTE: "2025-01-31"}, Limit: 10})
	if res.Total != 2 {
		t.Fatalf("date range total=%d, want 2", res.Total)
	}
}
