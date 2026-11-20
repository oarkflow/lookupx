package pkg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"unsafe"
)

func partitionFixture(tb testing.TB, n int) (FileMMapSegmentStore, Config, []PersistentManifest) {
	tb.Helper()
	cfg := Config{DisableSource: true, Schema: Schema{Fields: map[string]FieldOptions{
		"code":        {Kind: FieldKeyword, Indexed: true, Stored: true, Lowercase: true, TermSearch: true},
		"description": {Kind: FieldText, Indexed: true, Stored: true, Lowercase: true, TermSearch: true},
		"date":        {Kind: FieldTime, TimeFormat: "date", Indexed: true, Stored: true},
	}}}
	store := FileMMapSegmentStore{Root: tb.TempDir()}
	var manifests []PersistentManifest
	for part := 0; part < 3; part++ {
		ix, err := New(cfg)
		if err != nil {
			tb.Fatal(err)
		}
		for i := 0; i < n; i++ {
			if err := ix.Upsert(fmt.Sprintf("%d-%d", part, i), Document{"code": fmt.Sprintf("code-%d", part), "description": "office visit treatment", "date": fmt.Sprintf("202%d-01-01", part)}); err != nil {
				tb.Fatal(err)
			}
		}
		man, err := store.SaveIndex(context.Background(), fmt.Sprintf("part-%d", part), ix)
		_ = ix.Close()
		if err != nil {
			tb.Fatal(err)
		}
		manifests = append(manifests, man)
	}
	tb.Cleanup(func() { _ = ConfigurePartitionCache(0); _ = ConfigurePartitionCache(256 << 20) })
	return store, cfg, manifests
}
func TestClosedIndexReleasesExtras(t *testing.T) {
	ix, err := New(Config{Schema: TupleLookupSchema()})
	if err != nil {
		t.Fatal(err)
	}
	_ = ix.Freeze()
	if _, ok := extraIndexes.Load(ix); !ok {
		t.Fatal("fixture has no extras")
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := extraIndexes.Load(ix); ok {
		t.Fatal("closed index remains globally reachable")
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestMappedColumnsUseFileBackingAndMutableLoadCopies(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 20)
	data, release, err := mapSegmentFile(filepath.Join(mans[0].Path, "segment.dat"))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var dump persistentDump
	if err = decodeMapped(data, &dump, true); err != nil {
		t.Fatal(err)
	}
	base := uintptr(unsafe.Pointer(&data[0]))
	end := base + uintptr(len(data))
	col := dump.NumericDense["date"]
	ptr := uintptr(unsafe.Pointer(&col[0]))
	if ptr < base || ptr >= end {
		t.Fatal("numeric column was copied instead of mapped")
	}
	if col[1] != 1577836800 {
		t.Fatalf("date numeric=%v", col[1])
	}
	ix, _, err := store.LoadIndex(context.Background(), mans[0].IndexID, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	if err := ix.Upsert("0-0", Document{"code": "changed", "date": "2025-01-01"}); err != nil {
		t.Fatal(err)
	}
	if ix.Count(Term{Field: "code", Value: "changed"}) != 1 {
		t.Fatal("mutable restore cannot update")
	}
}
func TestMappedPartitionSearchConcurrentEvictionAndPruning(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 40)
	_ = ConfigurePartitionCache(0)
	req := SearchRequest{Query: And{Term{Field: "code", Value: "CODE-1"}, Term{Field: "description", Value: "office"}}, Limit: 7, WithDocs: true}
	before := servingPartitions.misses.Load()
	res, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, req, nil)
	if err != nil || len(res.Hits) != 7 || res.Total != 40 {
		t.Fatalf("result=%+v err=%v", res, err)
	}
	if got := servingPartitions.misses.Load() - before; got != 1 {
		t.Fatalf("opened %d partitions; wanted pruning to one", got)
	}
	// All results must survive the lease's immediate unmap with retention off.
	for _, h := range res.Hits {
		if h.Doc["code"] != "code-1" || h.Doc["date"] != "2021-01-01" {
			t.Fatalf("detached document=%v", h.Doc)
		}
	}
	_ = ConfigurePartitionCache(16 << 20)
	var wg sync.WaitGroup
	for j := 0; j < 12; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, req, nil)
			if err != nil || len(r.Hits) != 7 || r.Total != 40 {
				t.Errorf("concurrent total=%d err=%v", r.Total, err)
			}
		}()
	}
	wg.Wait()
	servingPartitions.mu.Lock()
	size, budget := servingPartitions.bytes, servingPartitions.maxBytes
	servingPartitions.mu.Unlock()
	if size > budget {
		t.Fatalf("cache bytes=%d exceed budget=%d", size, budget)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := SearchPartitionedPersistentResult(ctx, store, mans, cfg, req, nil); err == nil {
		t.Fatal("canceled search succeeded")
	}
}
func TestPartitionLatestSelectsGlobalMaximum(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 3)
	// Put the same group into three independent generations with different dates.
	for part, man := range mans {
		ix, _, err := store.LoadIndex(context.Background(), man.IndexID, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1; i++ {
			if err = ix.Upsert(fmt.Sprintf("%s-%d", man.IndexID, i), Document{"code": "same", "description": "visit", "date": fmt.Sprintf("202%d-01-01", part)}); err != nil {
				t.Fatal(err)
			}
		}
		newer, err := store.SaveIndex(context.Background(), man.IndexID, ix)
		_ = ix.Close()
		if err != nil {
			t.Fatal(err)
		}
		for i := range mans {
			if mans[i].IndexID == man.IndexID {
				mans[i] = newer
			}
		}
	}
	req := SearchRequest{Query: latestPerGroupQuery{Candidate: Term{Field: "code", Value: "same"}, DateField: "date", GroupBy: []string{"code"}}, Limit: 25, WithDocs: true}
	res, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 1 || len(res.Hits) != 1 {
		t.Fatalf("global latest total/hits=%d/%d", res.Total, len(res.Hits))
	}
	for _, h := range res.Hits {
		if h.Doc["date"] != "2022-01-01" {
			t.Fatalf("older date returned: %v", h.Doc)
		}
	}
}
func TestMappedSnapshotRejectsTruncationAndSchemaMismatch(t *testing.T) {
	_, _, mans := partitionFixture(t, 2)
	data, err := os.ReadFile(filepath.Join(mans[0].Path, "segment.dat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 8, 39, 40, len(data) / 2, len(data) - 1} {
		var d persistentDump
		if err := decodeMapped(data[:n], &d, false); err == nil {
			t.Fatalf("accepted truncated length %d", n)
		}
	}
	data[8] ^= 1
	var d persistentDump
	if err := decodeMapped(data, &d, false); err == nil {
		t.Fatal("accepted schema mismatch")
	}
}
func TestPartitionSummaryHasNoFalseNegatives(t *testing.T) {
	_, cfg, mans := partitionFixture(t, 3)
	for _, q := range []Query{Term{"code", "CODE-0"}, Prefix{"code", "co"}, GlobalTerm{Words: []string{"office"}}, Bool{Should: []Query{Term{"code", "absent"}, Term{"code", "code-0"}}}} {
		if !mans[0].Summary.mayMatch(q, cfg) {
			t.Fatalf("incorrectly pruned %#v", q)
		}
	}
	if mans[0].Summary.mayMatch(And{Term{"code", "code-2"}, Term{"description", "office"}}, cfg) {
		t.Fatal("failed to prune incompatible partition")
	}
	changed := cfg
	changed.Schema = Schema{Fields: map[string]FieldOptions{"code": {Kind: FieldKeyword}}}
	if !mans[0].Summary.mayMatch(Term{"code", "missing"}, changed) {
		t.Fatal("used summary for different schema")
	}
}
func BenchmarkPartitionServing(b *testing.B) {
	store, cfg, mans := partitionFixture(b, 5000)
	req := SearchRequest{Query: Term{"description", "office"}, Limit: 25, WithDocs: true, SkipTotal: true}
	for _, budget := range []int64{0, 64 << 20} {
		b.Run(fmt.Sprintf("cache_%d", budget), func(b *testing.B) {
			_ = ConfigurePartitionCache(0)
			_ = ConfigurePartitionCache(budget)
			if _, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, req, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, req, nil)
				if err != nil || len(res.Hits) != 25 {
					b.Fatalf("search err=%v", err)
				}
			}
		})
	}
}

func TestServingCatalogDetectsAtomicReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partitions.json")
	first := PartitionDataset{BuildID: "first", Complete: true, Generations: []PersistentManifest{{IndexID: "first", Generation: 1}}}
	if err := first.Save(path); err != nil {
		t.Fatal(err)
	}
	a, err := loadServingCatalog(path)
	if err != nil || a.BuildID != "first" {
		t.Fatalf("first=%v err=%v", a.BuildID, err)
	}
	next := PartitionDataset{BuildID: "other", Complete: true, Generations: []PersistentManifest{{IndexID: "other", Generation: 2}}}
	if err := next.Save(path); err != nil {
		t.Fatal(err)
	}
	b, err := loadServingCatalog(path)
	if err != nil || b.BuildID != "other" {
		t.Fatalf("stale catalog=%v err=%v", b.BuildID, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := loadServingCatalog(path); !os.IsNotExist(err) {
		t.Fatalf("deleted catalog served from cache: %v", err)
	}
}

func TestLegacyCompressedPartitionServing(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 3)
	man := mans[0]
	ix, _, err := store.LoadIndex(context.Background(), man.IndexID, cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(man.Path, "segment.dat")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = ix.encodePersistent(f)
	_ = f.Close()
	_ = ix.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Missing summaries emulate catalogs published by earlier versions.
	man.Summary = nil
	res, err := SearchPartitionedPersistentResult(context.Background(), store, []PersistentManifest{man}, cfg, SearchRequest{Query: Term{"code", "code-0"}, Limit: 10, WithDocs: true}, nil)
	if err != nil || res.Total != 3 || len(res.Hits) != 3 {
		t.Fatalf("legacy total/hits=%d/%d err=%v", res.Total, len(res.Hits), err)
	}
}

func TestBCLRoutingIndexDoesNotReserveSourceCapacity(t *testing.T) {
	cfg := &LookupXConfig{Indexes: []IndexDef{{ID: "large", Schema: "record", InitialCapacity: 1_000_000}}}
	mgr, err := BuildFromBCLConfig(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	managed, ok := mgr.Managed("large")
	if !ok {
		t.Fatal("missing routing index")
	}
	defer managed.Index.Close()
	if managed.Config.InitialCapacity != 1_000_000 {
		t.Fatal("lost configured build capacity")
	}
	ix := managed.Index
	if cap(ix.docToExt) > 1 || cap(ix.docs) > 1 {
		t.Fatalf("empty router reserves rows: ext=%d docs=%d", cap(ix.docToExt), cap(ix.docs))
	}
	for name, col := range ix.strings {
		if cap(col) > 1 {
			t.Fatalf("empty router reserves column %s: %d", name, cap(col))
		}
	}
}

func TestPartitionLeasePinsMappingAcrossBudgetReduction(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 10)
	_ = ConfigurePartitionCache(16 << 20)
	ix, release, err := servingPartitions.acquire(context.Background(), store, mans[0], cfg, partitionConfigSignature(cfg))
	if err != nil {
		t.Fatal(err)
	}
	_ = ConfigurePartitionCache(0)
	if ix.mappedRelease == nil {
		t.Fatal("budget reduction unmapped an active lease")
	}
	result := ix.Search(SearchRequest{Query: Term{"code", "code-0"}, Limit: 1, WithDocs: true})
	detachPartitionHits(result.Hits)
	release()
	if ix.mappedRelease != nil {
		t.Fatal("idle mapping survived eviction with zero budget")
	}
	if len(result.Hits) != 1 || result.Hits[0].Doc["description"] != "office visit treatment" {
		t.Fatal("response invalid after eviction")
	}
	if _, ok := extraIndexes.Load(ix); ok {
		t.Fatal("evicted partition retained in global extras")
	}
}

func TestPartitionBrowseReadsOnlyPageAndUsesPreview(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 4)
	before := servingPartitions.misses.Load()
	result, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, SearchRequest{Query: MatchAll{}, Limit: 5, Offset: 3, WithDocs: true}, nil)
	if err != nil || result.Total != 12 || len(result.Hits) != 5 {
		t.Fatalf("browse total/hits=%d/%d err=%v", result.Total, len(result.Hits), err)
	}
	want := []string{"0-3", "1-0", "1-1", "1-2", "1-3"}
	for i, h := range result.Hits {
		if h.ID != want[i] {
			t.Fatalf("page row %d=%s want=%s", i, h.ID, want[i])
		}
	}
	if got := servingPartitions.misses.Load() - before; got != 0 {
		t.Fatalf("browse decoded %d search partitions", got)
	}
	result.Hits[0].Doc["code"] = "modified by caller"
	again, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, SearchRequest{Limit: 1, Offset: 3, WithDocs: true, SkipTotal: true}, nil)
	if err != nil || again.Hits[0].Doc["code"] != "code-0" {
		t.Fatalf("preview mutated: %+v err=%v", again, err)
	}
	empty, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, SearchRequest{Query: MatchAll{}, Offset: 12, Limit: 25}, nil)
	if err != nil || len(empty.Hits) != 0 || empty.Total != 12 {
		t.Fatalf("past end=%+v err=%v", empty, err)
	}
}
func TestPartitionBrowseLegacyOpensOnlyNeededPartition(t *testing.T) {
	store, cfg, mans := partitionFixture(t, 8)
	for _, man := range mans {
		if err := os.Remove(filepath.Join(man.Path, "browse.gob")); err != nil {
			t.Fatal(err)
		}
	}
	_ = ConfigurePartitionCache(0)
	before := servingPartitions.misses.Load()
	for i := 0; i < 3; i++ {
		result, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, SearchRequest{Query: MatchAll{}, Limit: 2, Offset: 9, WithDocs: true}, nil)
		if err != nil || result.Total != 24 || len(result.Hits) != 2 || result.Hits[0].ID != "1-1" {
			t.Fatalf("legacy browse=%+v err=%v", result, err)
		}
	}
	if got := servingPartitions.misses.Load() - before; got != 3 {
		t.Fatalf("opened %d partitions for three pages, want 3", got)
	}
}
func TestPartitionBrowsePreviewFallsBackForDeeperPage(t *testing.T) {
	store, cfg, mans := partitionFixture(t, partitionPreviewRows+10)
	before := servingPartitions.misses.Load()
	result, err := SearchPartitionedPersistentResult(context.Background(), store, mans, cfg, SearchRequest{Query: MatchAll{}, Limit: 3, Offset: partitionPreviewRows - 1, WithDocs: true}, nil)
	if err != nil || len(result.Hits) != 3 || result.Hits[2].ID != fmt.Sprintf("0-%d", partitionPreviewRows+1) {
		t.Fatalf("deep browse=%+v err=%v", result, err)
	}
	if got := servingPartitions.misses.Load() - before; got != 1 {
		t.Fatalf("deep page opened %d partitions", got)
	}
}

func TestMappedCompactColumnsPreserveAliasesAndStringFilters(t *testing.T) {
	cfg := Config{DisableSource: true, Schema: Schema{Fields: map[string]FieldOptions{
		"description":  {Kind: FieldText, Indexed: true, Stored: true, IndexAlias: "fulltext", Lowercase: true},
		"fulltext":     {Kind: FieldText, Indexed: true, Stored: true, Lowercase: true},
		"tenant":       {Kind: FieldKeyword, Indexed: true, Stored: true},
		"tenant_alias": {Kind: FieldKeyword, Stored: true, StoredOnly: true, StoredAlias: "tenant"},
	}}}
	ix, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = ix.Upsert("one", Document{"description": "office visit", "fulltext": "office visit", "tenant": "37", "tenant_alias": "37"}); err != nil {
		t.Fatal(err)
	}
	store := FileMMapSegmentStore{Root: t.TempDir()}
	man, err := store.SaveIndex(context.Background(), "aliases", ix)
	_ = ix.Close()
	if err != nil {
		t.Fatal(err)
	}
	mapped, _, err := openServingPartition(context.Background(), store, man, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mapped.Close()
	if len(mapped.docToExt) != 0 || mapped.externalIDCount() != 2 || mapped.externalID(1) != "one" {
		t.Fatal("external IDs still allocate Go string headers or fail to resolve")
	}
	if len(mapped.strings["fulltext"]) != 0 || len(mapped.mappedStrings["fulltext"].offsets) != 2 {
		t.Fatal("stored column still allocates per-row Go string headers")
	}
	if mapped.Count(sourceStringFilter{Field: "description", Value: "office visit", Operator: "contains"}) != 1 {
		t.Fatal("mapped raw string filter failed")
	}
	r := mapped.Search(SearchRequest{Query: Term{"description", "office"}, Limit: 1, WithDocs: true})
	if len(r.Hits) != 1 || r.Hits[0].Doc["description"] != "office visit" || r.Hits[0].Doc["tenant_alias"] != "37" {
		t.Fatalf("mapped alias document=%+v", r.Hits)
	}
}
