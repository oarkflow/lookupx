package main

import (
	"context"
	"fmt"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	fields := map[string]lookup.FieldKind{
		"term":         lookup.FieldKeyword,
		"group_id": lookup.FieldKeyword,
		"date_key":          lookup.FieldKeyword,
		"partition_id":  lookup.FieldKeyword,
	}
	ix, err := lookup.New(lookup.Config{Schema: lookup.GenericLookupSchema(fields), DisableSource: true, InitialCapacity: 100000, Clock: lookup.StaticClock{T: time.Unix(0, 0)}})
	if err != nil {
		panic(err)
	}
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")
	ix.EnableComposite(lookup.CompositeDefinition{ID: "term_work_date_key", Fields: []lookup.CompositeField{{Name: "term", ID: term}, {Name: "group_id", ID: work}, {Name: "date_key", ID: date_key}}})

	src := lookup.StreamingRowsSource{
		Rows: 100000, Prefix: "generic",
		Fields: []lookup.SyntheticField{
			{Name: "term", Field: term, Kind: lookup.ValueKeyword, Values: []string{"alpha", "beta", "key-0013", "key-special"}, Normalized: true},
			{Name: "group_id", Field: work, Kind: lookup.ValueKeyword, Values: []string{"1", "2", "3", "4"}, Normalized: true},
			{Name: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Values: []string{"2026-01-02", "2026-01-03", "2026-01-04", "2026-01-01"}, Normalized: true},
			{Name: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Values: []string{"100", "200"}, Normalized: true},
		},
	}
	start := time.Now()
	stats, err := ix.IndexFrom(context.Background(), src, lookup.BulkOptions{Name: "generic", BatchSize: 65536})
	if err != nil {
		panic(err)
	}
	fmt.Printf("indexed=%d took=%s rows_per_sec=%.0f\n", stats.Indexed, time.Since(start), float64(stats.Indexed)/time.Since(start).Seconds())

	dst := make([]lookup.Hit, 0, 10)
	vals := []string{"key-special", "4", "2026-01-01"}
	loops := 1000
	qstart := time.Now()
	for i := 0; i < loops; i++ {
		dst = ix.CompositeLookup("term_work_date_key", vals, 5, dst)
	}
	fmt.Printf("query composite=%v hits=%d avg_ns=%d first=%s\n", vals, len(dst), time.Since(qstart).Nanoseconds()/int64(loops), first(dst))
}

func first(h []lookup.Hit) string {
	if len(h) == 0 {
		return ""
	}
	return h[0].ID
}
