package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func envUint(name string, def uint64) uint64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envBool(name string) bool { return os.Getenv(name) == "1" || os.Getenv(name) == "true" }

func main() {
	ctx := context.Background()
	rows := envUint("ROWS", 1_000_000_000)
	sampleRows := envUint("SAMPLE_ROWS", 100_000)
	runFull := envBool("RUN_FULL")

	budget := lookup.LowMemoryBillionBudget()
	if v := os.Getenv("PARTITION_ROWS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			budget.TargetRowsPerPartition = n
		}
	}
	if v := os.Getenv("BATCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			budget.DefaultBatchSize = n
		}
	}
	plan := lookup.PlanBillionRowDeployment("dataset_a", int64(rows), budget)
	fmt.Println("plan_rows", plan["estimated_rows"], "plan_partitions", plan["recommended_partitions"], "rows_per_partition", plan["estimated_rows_per_partition"], "batch", plan["recommended_batch_size"])

	cfg := lookup.Config{Schema: lookup.TupleLookupSchema(), DisableSource: true, InitialCapacity: int(budget.TargetRowsPerPartition) + 1, Clock: lookup.StaticClock{T: time.Unix(0, 0)}}
	tmp := filepath.Join(os.TempDir(), "lookupx-billion-store")
	if v := os.Getenv("DATA_DIR"); v != "" {
		tmp = v
	}
	_ = os.RemoveAll(tmp)
	store := lookup.FileMMapSegmentStore{Root: tmp}

	rowsToProcess := rows
	if !runFull && rows > sampleRows {
		rowsToProcess = sampleRows
		fmt.Println("full_run", false, "processing_sample_rows", rowsToProcess, "set RUN_FULL=1 to process all rows")
	} else {
		fmt.Println("full_run", true, "processing_rows", rowsToProcess)
	}

	probe, err := lookup.New(cfg)
	if err != nil {
		panic(err)
	}
	term := probe.FieldID("term")
	work := probe.FieldID("group_id")
	date_key := probe.FieldID("date_key")
	partition := probe.FieldID("partition_id")
	_ = probe.Close()

	src := lookup.StreamingDatasetSource{Rows: rowsToProcess, Term: term, Group: work, DateKey: date_key, Partition: partition, SourceName: "dataset_a"}
	started := time.Now()
	stats, err := lookup.BuildPartitionedPersistent(ctx, src, lookup.PartitionedBuildOptions{
		IndexID:          "dataset_a",
		Store:            store,
		Config:           cfg,
		RowsPerPartition: uint64(budget.TargetRowsPerPartition),
		Bulk:             budget.BulkOptions("dataset_a", nil, false),
		EnableComposite:  true,
		Freeze:           true,
		MaxRows:          rowsToProcess,
		Progress: func(p lookup.PartitionedBuildProgress) {
			if p.Indexed > 0 && p.Indexed%500000 == 0 {
				fmt.Println("progress", "indexed", p.Indexed, "partitions", p.Partitions, "took", p.Took)
			}
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("partitioned_indexed", stats.Indexed, "partitions", stats.Partitions, "took", time.Since(started), "rows_per_sec", uint64(float64(stats.Indexed)/stats.Took.Seconds()))

	if len(stats.Manifests) == 0 {
		panic("no partitions persisted")
	}
	firstPartition := stats.Manifests[0].IndexID
	q := lookup.ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
	hits := make([]lookup.Hit, 0, 5)
	qStart := time.Now()
	loops := 100
	for i := 0; i < loops; i++ {
		hits, err = lookup.SearchPartitionedPersistent(ctx, store, stats.Manifests, cfg, lookup.SearchRequest{Query: q, Limit: 5}, hits[:0])
		if err != nil {
			panic(err)
		}
	}
	avg := time.Since(qStart).Nanoseconds() / int64(loops)
	fmt.Println("cold_partitioned_query_partitions", len(stats.Manifests), "hits", len(hits), "avg_query_ns", avg)

	ix, _, err := lookup.OpenPersistent(ctx, store, firstPartition, cfg)
	if err != nil {
		panic(err)
	}
	defer ix.Close()
	warmHits := make([]lookup.Hit, 0, 5)
	warmStart := time.Now()
	for i := 0; i < 1000; i++ {
		_, warmHits = ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 5}, warmHits[:0])
	}
	fmt.Println("warm_loaded_partition_query", firstPartition, "hits", len(warmHits), "avg_query_ns", time.Since(warmStart).Nanoseconds()/1000)

	rep, err := lookup.ValidatePersistentIndex(ctx, store, firstPartition, cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println("validated", rep.OK, "live_docs", rep.Stats.LiveDocs, "store", tmp)
}
