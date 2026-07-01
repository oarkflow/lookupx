package pkg

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestBillionPlanningAndPartition(t *testing.T) {
	budget := DefaultBillionRowBudget()
	if n := budget.RecommendedPartitions(1_000_000_000); n <= 1 {
		t.Fatalf("expected many partitions, got %d", n)
	}
	sch := PartitionScheme{IndexID: "dataset_a", GroupModulo: 32, MonthPartitions: true, PartitionModulo: 16}
	id := sch.TuplePartition("key-0013", 4, "2026-01-01", 201)
	if id == "" || id == "dataset_a" {
		t.Fatalf("bad partition id %q", id)
	}
}

func TestFileMMapSegmentStoreValidateRepair(t *testing.T) {
	ctx := context.Background()
	ix, err := New(Config{Schema: TupleLookupSchema(), DisableSource: true, InitialCapacity: 128, Clock: StaticClock{T: time.Unix(0, 0)}})
	if err != nil {
		t.Fatal(err)
	}
	ix.EnableTupleComposite()
	term, work, date_key := ix.FieldID("term"), ix.FieldID("group_id"), ix.FieldID("date_key")
	rec := SourceRecord{ID: "enc-1", Seq: 1}
	rec.AddKeyword(term, "key-special", true)
	rec.AddKeyword(work, "4", true)
	rec.AddKeyword(date_key, "2026-01-01", true)
	if _, err := ix.IndexFrom(ctx, SliceSource{Records: []SourceRecord{rec}}, BulkOptions{Name: "t", BatchSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := ix.Freeze(); err != nil {
		t.Fatal(err)
	}
	store := FileMMapSegmentStore{Root: filepath.Join(t.TempDir(), "store")}
	if _, err := ix.SavePersistent(ctx, store, "dataset_a"); err != nil {
		t.Fatal(err)
	}
	rep, err := ValidatePersistentIndex(ctx, store, "dataset_a", Config{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("validation not ok: %+v", rep.Issues)
	}
	opened, _, err := OpenPersistent(ctx, store, "dataset_a", Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, hits := opened.SearchInto(SearchRequest{Query: ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01"), Limit: 5}, nil)
	if len(hits) != 1 {
		t.Fatalf("hits=%d", len(hits))
	}
	if _, _, err := RepairPersistentIndex(ctx, store, "dataset_a", Config{}); err != nil {
		t.Fatal(err)
	}
	gens, err := ListIndexGenerations(ctx, store, "dataset_a")
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) < 2 {
		t.Fatalf("expected repaired generation, got %d", len(gens))
	}
	if _, err := CompactPersistentGenerations(ctx, FileSegmentStore{Root: store.Root}, "dataset_a", GenerationPolicy{KeepLast: 1}); err == nil {
		// Different store layout; FileSegmentStore compaction should not be used for mmap layout.
	}
}

func TestBuildPartitionedPersistentStreamingDatasetLowMemory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := Config{Schema: TupleLookupSchema(), DisableSource: true, InitialCapacity: 1001, Clock: StaticClock{T: time.Unix(0, 0)}}
	probe, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	term, work, date_key, partition := probe.FieldID("term"), probe.FieldID("group_id"), probe.FieldID("date_key"), probe.FieldID("partition_id")
	_ = probe.Close()
	src := StreamingDatasetSource{Rows: 3000, Term: term, Group: work, DateKey: date_key, Partition: partition}
	stats, err := BuildPartitionedPersistent(ctx, src, PartitionedBuildOptions{IndexID: "dataset_a", Store: FileMMapSegmentStore{Root: dir}, Config: cfg, RowsPerPartition: 1000, Bulk: BulkOptions{Name: "dataset_a", BatchSize: 128}, EnableComposite: true, Freeze: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 3000 || stats.Partitions != 3 {
		t.Fatalf("indexed=%d partitions=%d", stats.Indexed, stats.Partitions)
	}
	ix, _, err := OpenPersistent(ctx, FileMMapSegmentStore{Root: dir}, stats.Manifests[0].IndexID, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	_, hits := ix.SearchInto(SearchRequest{Query: ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01"), Limit: 5}, nil)
	if len(hits) == 0 {
		t.Fatal("expected composite lookup hits from streamed partition")
	}
}
