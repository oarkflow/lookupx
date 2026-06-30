package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

const rowsPerIndex = 100_000

func main() {
	mgr := lookup.NewMultiIndexManager()
	if err := lookup.RegisterDemoIndexes(mgr, rowsPerIndex); err != nil {
		panic(err)
	}
	for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
		ix, _ := mgr.Get(id)
		factory := func(name string) lookup.SourceFactory {
			return func(ix *lookup.Index) (lookup.Source, error) { return buildSource(ix, name, rowsPerIndex), nil }
		}(id)
		started := time.Now()
		stats, err := mgr.ReloadFromFactory(context.Background(), id, factory, lookup.BulkOptions{Name: id, BatchSize: 65536})
		if err != nil {
			panic(err)
		}
		_ = ix
		fmt.Printf("loaded index=%s %s\n", id, lookup.LatencySummary(stats.Indexed, time.Since(started)))
	}
	queries := map[string]string{
		"dataset_a":   "term=key-0013&group_id=4&date_key=2026-01-01",
		"dataset_b": "term=E11.9&group_id=4&date_key=2026-01-01",
		"dataset_c": "term=A0428&group_id=4&date_key=2026-01-01",
	}
	for id, raw := range queries {
		ix, _ := mgr.Get(id)
		hits, _, avg := lookup.TimeSearch(ix, lookup.ParseLookupQuery(raw), 5, 1000)
		fmt.Printf("search index=%s query=%q hits=%d avg_query_ns=%d\n", id, raw, hits, avg)
	}
}

func buildSource(ix *lookup.Index, indexID string, rows int) lookup.Source {
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")
	records := make([]lookup.SourceRecord, rows)
	for i := 1; i <= rows; i++ {
		code := codeFor(indexID, i)
		wi := strconv.Itoa((i % 10) + 1)
		day := fmt.Sprintf("2026-01-%02d", (i%28)+1)
		if i%1000 == 123 {
			code, wi, day = firstTerm(indexID), "4", "2026-01-01"
		}
		records[i-1].ID = fmt.Sprintf("%s-%06d", indexID, i)
		records[i-1].Seq = uint64(i)
		records[i-1].Values = append(records[i-1].Values,
			lookup.SourceValue{Field: term, Kind: lookup.ValueKeyword, String: strings.ToLower(code), Normalized: true},
			lookup.SourceValue{Field: work, Kind: lookup.ValueKeyword, String: wi, Normalized: true},
			lookup.SourceValue{Field: date_key, Kind: lookup.ValueKeyword, String: day, Normalized: true},
			lookup.SourceValue{Field: partition, Kind: lookup.ValueKeyword, String: strconv.Itoa((i % 200) + 1), Normalized: true},
		)
	}
	return lookup.SliceSource{Records: records}
}
func codeFor(indexID string, i int) string {
	switch indexID {
	case "dataset_b":
		codes := [...]string{"A00.0", "E11.9", "I10", "J45.909", "M54.5", "R51", "S72.001A", "N39.0"}
		return codes[i%len(codes)]
	case "dataset_c":
		codes := [...]string{"A0428", "A0429", "E0114", "J1100", "J1885", "G0008", "Q9967", "L1830"}
		return codes[i%len(codes)]
	default:
		codes := [...]string{"key-0011", "key-0012", "key-0013", "key-0014", "key-0015", "key-3000", "key-0053", "key-5025", "key-6415", "key-1046"}
		return codes[i%len(codes)]
	}
}
func firstTerm(indexID string) string {
	if indexID == "dataset_b" {
		return "E11.9"
	}
	if indexID == "dataset_c" {
		return "A0428"
	}
	return "key-0013"
}
