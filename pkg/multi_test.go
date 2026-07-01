package pkg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMultiIndexReloadAndLookup(t *testing.T) {
	mgr := NewMultiIndexManager()
	if err := RegisterDemoIndexes(mgr, 1000); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
		_, err := mgr.ReloadFromFactory(context.Background(), id, func(indexID string) SourceFactory {
			return func(ix *Index) (Source, error) { return testDemoSource(ix, indexID, 1000), nil }
		}(id), BulkOptions{Name: id, BatchSize: 512})
		if err != nil {
			t.Fatalf("reload %s: %v", id, err)
		}
		ix, ok := mgr.Get(id)
		if !ok {
			t.Fatalf("missing %s", id)
		}
		if ix.Count(ParseLookupQuery("term="+testFirstTerm(id)+"&group_id=4&date_key=2026-01-01")) == 0 {
			t.Fatalf("expected hits for %s", id)
		}
	}
}

func testDemoSource(ix *Index, indexID string, rows int) Source {
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")
	records := make([]SourceRecord, rows)
	for i := 1; i <= rows; i++ {
		code := testFirstTerm(indexID)
		wi, day := strconv.Itoa((i%10)+1), fmt.Sprintf("2026-01-%02d", (i%28)+1)
		if i%100 == 23 {
			wi, day = "4", "2026-01-01"
		}
		records[i-1].ID = fmt.Sprintf("%s-%06d", indexID, i)
		records[i-1].Seq = uint64(i)
		records[i-1].Values = append(records[i-1].Values,
			SourceValue{Field: term, Kind: ValueKeyword, String: strings.ToLower(code), Normalized: true},
			SourceValue{Field: work, Kind: ValueKeyword, String: wi, Normalized: true},
			SourceValue{Field: date_key, Kind: ValueKeyword, String: day, Normalized: true},
			SourceValue{Field: partition, Kind: ValueKeyword, String: "1", Normalized: true},
		)
	}
	return SliceSource{Records: records}
}
func testFirstTerm(id string) string {
	if id == "dataset_b" {
		return "E11.9"
	}
	if id == "dataset_c" {
		return "A0428"
	}
	return "key-0013"
}

func TestLatencySummary(t *testing.T) {
	if LatencySummary(100, time.Millisecond) == "" {
		t.Fatal("empty")
	}
}
