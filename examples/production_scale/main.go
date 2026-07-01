package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	const rows = 100000
	ctx := context.Background()
	mgr := lookup.NewMultiIndexManager()
	if err := lookup.RegisterDemoIndexes(mgr, rows); err != nil {
		panic(err)
	}
	ix, _ := mgr.Get("dataset_a")
	ix.EnableTupleComposite()
	src := dataset_aSource(ix, rows)
	started := time.Now()
	stats, err := ix.IndexFrom(ctx, src, lookup.BulkOptions{Name: "dataset_a", BatchSize: 65536, Checkpoint: lookup.NewMemoryCheckpoint(), CheckpointEvery: 65536})
	if err != nil {
		panic(err)
	}
	if err := ix.Freeze(); err != nil {
		panic(err)
	}
	fmt.Printf("indexed %s frozen=%v\n", lookup.LatencySummary(stats.Indexed, time.Since(started)), ix.IsFrozen())

	q := lookup.ParseLookupQuery("term=key-0013&group_id=4&date_key=2026-01-01")
	hits, _, avg := lookup.TimeSearch(ix, q, 5, 1000)
	fmt.Printf("composite_query hits=%d avg_ns=%d\n", hits, avg)

	store := lookup.FileSegmentStore{Root: filepath.Join(".", "data", "production-scale")}
	man, err := ix.SavePersistent(ctx, store, "dataset_a")
	if err != nil {
		panic(err)
	}
	fmt.Printf("persisted generation=%d docs=%d path=%s\n", man.Generation, man.Docs, man.Path)

	loaded, man, err := lookup.OpenPersistent(ctx, store, "dataset_a", lookup.Config{})
	if err != nil {
		panic(err)
	}
	_, out := loaded.SearchInto(lookup.SearchRequest{Query: q, Limit: 5}, nil)
	fmt.Printf("loaded generation=%d hits=%d first=%s\n", man.Generation, len(out), out[0].ID)
}

func dataset_aSource(ix *lookup.Index, rows int) lookup.Source {
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")
	records := make([]lookup.SourceRecord, rows)
	codes := [...]string{"key-0011", "key-0012", "key-0013", "key-0014", "key-0015", "key-3000", "key-0053", "key-5025", "key-6415", "key-1046", "key-special"}
	for i := 1; i <= rows; i++ {
		code := strings.ToLower(codes[i%len(codes)])
		wi := strconv.Itoa((i % 10) + 1)
		day := fmt.Sprintf("2026-01-%02d", (i%28)+1)
		if i%1000 == 123 {
			code, wi, day = "key-0013", "4", "2026-01-01"
		}
		records[i-1].ID = fmt.Sprintf("enc-%09d", i)
		records[i-1].Seq = uint64(i)
		records[i-1].Values = append(records[i-1].Values,
			lookup.SourceValue{Field: term, Kind: lookup.ValueKeyword, String: code, Normalized: true},
			lookup.SourceValue{Field: work, Kind: lookup.ValueKeyword, String: wi, Normalized: true},
			lookup.SourceValue{Field: date_key, Kind: lookup.ValueKeyword, String: day, Normalized: true},
			lookup.SourceValue{Field: partition, Kind: lookup.ValueKeyword, String: strconv.Itoa((i % 200) + 1), Normalized: true},
		)
	}
	return lookup.SliceSource{Records: records}
}
