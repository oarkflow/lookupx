package pkg

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamingDatasetSource generates deterministic Dataset/Dataset/DatasetC-like record rows
// without materializing the dataset. It is intended for examples, benchmarks and
// smoke tests of billion-row ingestion plans. Only the active SourceRecord is
// allocated by the caller and reused for every row.
type StreamingDatasetSource struct {
	Rows          uint64
	Term          FieldID
	Group         FieldID
	DateKey       FieldID
	Partition     FieldID
	Source        FieldID
	SourceName    string
	IncludeSource bool
	StartSeq      uint64
}

func (s StreamingDatasetSource) Open(ctx context.Context) (Cursor, error) {
	if s.Rows == 0 {
		return &streamingDatasetCursor{}, nil
	}
	return &streamingDatasetCursor{s: s, i: s.StartSeq}, nil
}

type streamingDatasetCursor struct {
	s   StreamingDatasetSource
	i   uint64
	err error
}

func (c *streamingDatasetCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	if c.err != nil || c.i >= c.s.Rows {
		return false
	}
	select {
	case <-ctx.Done():
		c.err = ctx.Err()
		return false
	default:
	}
	n := c.i + 1
	c.i++
	code := "key-0013"
	switch n & 3 {
	case 0:
		code = "key-0014"
	case 1:
		code = "key-special"
	case 2:
		code = "key-0053"
	}
	wiNum := (n % 10) + 1
	dayNum := (n % 28) + 1
	if n%1000 == 123 {
		code, wiNum, dayNum = "key-special", 4, 1
	}
	dst.Reset()
	dst.ID = "enc-" + strconv.FormatUint(n, 10)
	dst.Seq = n
	dst.AddKeyword(c.s.Term, strings.ToLower(code), true)
	dst.AddKeyword(c.s.Group, strconv.FormatUint(wiNum, 10), true)
	dst.AddKeyword(c.s.DateKey, fmt.Sprintf("2026-01-%02d", dayNum), true)
	if c.s.Partition != InvalidFieldID {
		dst.AddKeyword(c.s.Partition, strconv.FormatUint((n%200)+1, 10), true)
	}
	if c.s.IncludeSource && c.s.Source != InvalidFieldID && c.s.SourceName != "" {
		dst.AddKeyword(c.s.Source, strings.ToLower(c.s.SourceName), true)
	}
	return true
}
func (c *streamingDatasetCursor) Err() error   { return c.err }
func (c *streamingDatasetCursor) Close() error { return nil }

// PartitionedBuildOptions streams a source into many small persistent indexes.
// RowsPerPartition, MaxPartitionBytes, BatchSize, and MaxBatchBytes bound index
// size and pending input independently of total source size. Byte limits are
// estimates of source payload size, not an exact Go heap limit.
type PartitionedBuildOptions struct {
	IndexID           string
	Store             PersistentStore
	CatalogPath       string
	ProgressPath      string
	DatasetRoot       string
	PublishProgress   bool
	Config            Config
	RowsPerPartition  uint64
	MaxBatchBytes     uint64
	MaxPartitionBytes uint64
	Bulk              BulkOptions
	EnableComposite   bool
	Freeze            bool
	MaxRows           uint64
	Progress          func(PartitionedBuildProgress)
}

type PartitionedBuildProgress struct {
	IndexID     string        `json:"index_id"`
	PartitionID string        `json:"partition_id"`
	PartitionNo int           `json:"partition_no"`
	Seen        uint64        `json:"seen"`
	Indexed     uint64        `json:"indexed"`
	Skipped     uint64        `json:"skipped"`
	CurrentRows uint64        `json:"current_rows"`
	Partitions  int           `json:"partitions"`
	LastSeq     uint64        `json:"last_seq"`
	Took        time.Duration `json:"took"`
}

type PartitionedBuildStats struct {
	IndexID    string               `json:"index_id"`
	BuildID    string               `json:"build_id"`
	Seen       uint64               `json:"seen"`
	Indexed    uint64               `json:"indexed"`
	Skipped    uint64               `json:"skipped"`
	LastSeq    uint64               `json:"last_seq"`
	Partitions int                  `json:"partitions"`
	Took       time.Duration        `json:"took"`
	Manifests  []PersistentManifest `json:"manifests,omitempty"`
}

func estimateSourceRecordBytes(record *SourceRecord) uint64 {
	if record == nil {
		return 0
	}
	// Include fixed per-record/value overhead plus payload lengths. This is an
	// input-size estimate, not an exact prediction of the index's heap use.
	bytes := uint64(128 + len(record.ID))
	for i := range record.Values {
		value := &record.Values[i]
		bytes = saturatingAdd(bytes, 48)
		bytes = saturatingAdd(bytes, uint64(len(value.String)))
		if n := uint64(len(value.Vector)); n > 0 {
			bytes = saturatingAdd(bytes, n*8)
		}
		if text, ok := value.Source.(string); ok {
			bytes = saturatingAdd(bytes, uint64(len(text)))
		}
	}
	return bytes
}

func saturatingAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

func estimatedLimitExceeded(limit, current, add uint64) bool {
	return add > limit || current > limit-add
}

// BuildPartitionedPersistent indexes a stream into independent persistent
// partitions. It never loads all source rows and never keeps old partitions in
// memory after they are saved.
func BuildPartitionedPersistent(ctx context.Context, src Source, opt PartitionedBuildOptions) (PartitionedBuildStats, error) {
	if src == nil {
		return PartitionedBuildStats{}, errors.New("source required")
	}
	if opt.Store == nil {
		return PartitionedBuildStats{}, errors.New("persistent store required")
	}
	if opt.IndexID == "" {
		opt.IndexID = "index"
	}
	if opt.RowsPerPartition == 0 {
		opt.RowsPerPartition = 250_000
	}
	if opt.Bulk.BatchSize <= 0 {
		opt.Bulk.BatchSize = 16_384
	}
	if opt.MaxBatchBytes == 0 {
		if opt.Bulk.MaxBatchBytes > 0 {
			opt.MaxBatchBytes = uint64(opt.Bulk.MaxBatchBytes)
		} else {
			opt.MaxBatchBytes = 16 << 20
		}
	}
	if opt.MaxPartitionBytes == 0 {
		if opt.Bulk.MaxPartitionBytes > 0 {
			opt.MaxPartitionBytes = uint64(opt.Bulk.MaxPartitionBytes)
		} else {
			opt.MaxPartitionBytes = 128 << 20
		}
	}
	if opt.Bulk.BatchSize > int(opt.RowsPerPartition) && opt.RowsPerPartition < uint64(^uint(0)>>1) {
		opt.Bulk.BatchSize = int(opt.RowsPerPartition)
	}
	started := time.Now()

	mkIndex := func() (*Index, error) {
		cfg := opt.Config
		if cfg.Schema.Fields == nil {
			cfg.Schema = TupleLookupSchema()
		}
		if cfg.InitialCapacity <= 0 || uint64(cfg.InitialCapacity) > opt.RowsPerPartition {
			capHint := opt.RowsPerPartition
			if capHint > 1_000_000 {
				capHint = 1_000_000
			}
			cfg.InitialCapacity = int(capHint) + 1
		}
		cfg.DisableSource = true
		ix, err := New(cfg)
		if err != nil {
			return nil, err
		}
		if opt.EnableComposite {
			ix.EnableTupleComposite()
		}
		return ix, nil
	}

	ix, err := mkIndex()
	if err != nil {
		return PartitionedBuildStats{}, err
	}
	closeIndex := func() { _ = ix.Close() }
	defer closeIndex()

	batch := make([]SourceRecord, 0, opt.Bulk.BatchSize)
	batchBytes := uint64(0)
	for i := 0; i < opt.Bulk.BatchSize; i++ {
		batch = append(batch, SourceRecord{Values: make([]SourceValue, 0, len(ix.fieldList))})
	}
	batch = batch[:0]
	rec := SourceRecord{Values: make([]SourceValue, 0, len(ix.fieldList))}
	stats := PartitionedBuildStats{IndexID: cleanIndexID(opt.IndexID)}
	progressPath := opt.ProgressPath
	if progressPath == "" && opt.CatalogPath != "" {
		progressPath = opt.CatalogPath + ".progress.json"
	}
	fingerprint, canResume := partitionBuildFingerprint(src, opt)
	partitionNo := 0
	resumeAfter := uint64(0)
	if opt.Bulk.Resume && progressPath != "" {
		checkpoint, loadErr := LoadPartitionDataset(progressPath)
		if loadErr == nil && !checkpoint.Complete && checkpoint.BuildID != "" && len(checkpoint.Generations) > 0 {
			if !canResume {
				return stats, errors.New("cannot resume partition build: source does not provide a stable resume identity")
			}
			if _, err := hex.DecodeString(checkpoint.BuildID); err != nil {
				return stats, errors.New("cannot resume partition build: invalid saved build ID")
			}
			partitionPrefix := fmt.Sprintf("%s-b%s-p", stats.IndexID, checkpoint.BuildID)
			for _, manifest := range checkpoint.Generations {
				if !strings.HasPrefix(cleanIndexID(manifest.IndexID), partitionPrefix) {
					return stats, errors.New("cannot resume partition build: saved partition does not belong to this build")
				}
				if err := validateSavedPartition(manifest); err != nil {
					return stats, fmt.Errorf("cannot resume partition build: %w", err)
				}
			}
			if checkpoint.Fingerprint != fingerprint {
				return stats, errors.New("cannot resume partition build: source or index configuration differs from the saved build")
			}
			_, ok := src.(ResumableSource)
			if !ok || checkpoint.LastSeq == 0 {
				return stats, errors.New("cannot resume partition build: saved build has no resumable source sequence")
			}
			stats.BuildID = checkpoint.BuildID
			stats.Seen = checkpoint.Seen
			stats.Indexed = checkpoint.Indexed
			stats.Skipped = checkpoint.Skipped
			stats.LastSeq = checkpoint.LastSeq
			stats.Manifests = append([]PersistentManifest(nil), checkpoint.Generations...)
			stats.Partitions = len(stats.Manifests)
			partitionNo = stats.Partitions
			resumeAfter = stats.LastSeq
		} else if loadErr != nil && !errors.Is(loadErr, os.ErrNotExist) {
			return stats, fmt.Errorf("load partition build progress: %w", loadErr)
		}
	}
	if stats.BuildID == "" {
		var buildID [12]byte
		if _, err := rand.Read(buildID[:]); err != nil {
			return stats, fmt.Errorf("create partition build ID: %w", err)
		}
		stats.BuildID = hex.EncodeToString(buildID[:])
	}
	var cur Cursor
	if resumeAfter > 0 {
		cur, err = src.(ResumableSource).OpenAfter(ctx, resumeAfter)
	} else {
		cur, err = src.Open(ctx)
	}
	if err != nil {
		return stats, err
	}
	defer cur.Close()
	partitionRows := uint64(0)
	partitionBytes := uint64(0)
	currentPartitionID := func() string { return fmt.Sprintf("%s-b%s-p%06d", stats.IndexID, stats.BuildID, partitionNo) }

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		indexed, skipped, lastSeq, err := ix.indexSourceBatch(batch, opt.Bulk.SkipBadRecords)
		for i := range batch {
			partitionBytes = saturatingAdd(partitionBytes, estimateSourceRecordBytes(&batch[i]))
		}
		stats.Indexed += indexed
		stats.Skipped += skipped
		partitionRows += indexed
		if lastSeq > 0 {
			stats.LastSeq = lastSeq
		}
		for i := range batch {
			clear(batch[i].Values)
			batch[i].Values = batch[i].Values[:0]
			batch[i].ID = ""
			batch[i].Seq = 0
		}
		batch = batch[:0]
		batchBytes = 0
		if err != nil {
			return err
		}
		if opt.Progress != nil {
			opt.Progress(PartitionedBuildProgress{IndexID: stats.IndexID, PartitionID: currentPartitionID(), PartitionNo: partitionNo, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, CurrentRows: partitionRows, Partitions: stats.Partitions, LastSeq: stats.LastSeq, Took: time.Since(started)})
		}
		return nil
	}
	persistPartition := func(force bool) error {
		if !force && partitionRows < opt.RowsPerPartition {
			return nil
		}
		if partitionRows == 0 {
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		if opt.Freeze {
			if err := ix.Freeze(); err != nil {
				return err
			}
		}
		man, err := ix.SavePersistent(ctx, opt.Store, currentPartitionID())
		if err != nil {
			return err
		}
		stats.Manifests = append(stats.Manifests, man)
		stats.Partitions++
		checkpoint := PartitionDataset{Root: opt.DatasetRoot, BuildID: stats.BuildID, Fingerprint: fingerprint, LastSeq: stats.LastSeq, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, Generations: append([]PersistentManifest(nil), stats.Manifests...)}
		if progressPath != "" {
			if err := checkpoint.Save(progressPath); err != nil {
				return fmt.Errorf("save completed partition checkpoint: %w", err)
			}
		}
		if opt.Bulk.Checkpoint != nil && stats.LastSeq > 0 {
			name := opt.Bulk.Name
			if name == "" {
				name = stats.IndexID
			}
			if err := opt.Bulk.Checkpoint.Save(ctx, name, stats.LastSeq); err != nil {
				return err
			}
		}
		if opt.CatalogPath != "" && opt.PublishProgress {
			if err := checkpoint.Save(opt.CatalogPath); err != nil {
				return fmt.Errorf("publish completed partition catalog: %w", err)
			}
		}
		_ = ix.Close()
		partitionNo++
		partitionRows = 0
		partitionBytes = 0
		ix, err = mkIndex()
		return err
	}
	for cur.Next(ctx, &rec) {
		if opt.MaxRows > 0 && stats.Seen >= opt.MaxRows {
			break
		}
		recordBytes := estimateSourceRecordBytes(&rec)
		if len(batch) > 0 && estimatedLimitExceeded(opt.MaxBatchBytes, batchBytes, recordBytes) {
			if err := flush(); err != nil {
				return stats, err
			}
		}
		currentPartitionBytes := saturatingAdd(partitionBytes, batchBytes)
		if partitionRows+uint64(len(batch)) > 0 && estimatedLimitExceeded(opt.MaxPartitionBytes, currentPartitionBytes, recordBytes) {
			if len(batch) > 0 {
				if err := flush(); err != nil {
					return stats, err
				}
			}
			if partitionRows > 0 {
				if err := persistPartition(true); err != nil {
					return stats, err
				}
			}
		}
		stats.Seen++
		n := len(batch)
		if n < cap(batch) {
			batch = batch[:n+1]
		} else {
			batch = append(batch, SourceRecord{Values: make([]SourceValue, 0, len(ix.fieldList))})
		}
		slot := &batch[n]
		slot.ID = rec.ID
		slot.Seq = rec.Seq
		slot.Values = append(slot.Values[:0], rec.Values...)
		batchBytes += recordBytes
		if len(batch) >= opt.Bulk.BatchSize {
			if err := flush(); err != nil {
				return stats, err
			}
		}
		if partitionRows+uint64(len(batch)) >= opt.RowsPerPartition {
			if err := persistPartition(true); err != nil {
				return stats, err
			}
		}
		rec.Reset()
	}
	if err := cur.Err(); err != nil {
		return stats, err
	}
	if err := flush(); err != nil {
		return stats, err
	}
	if err := persistPartition(true); err != nil {
		return stats, err
	}
	stats.Took = time.Since(started)
	if opt.CatalogPath != "" {
		dataset := PartitionDataset{Root: opt.DatasetRoot, BuildID: stats.BuildID, Complete: true, Fingerprint: fingerprint, LastSeq: stats.LastSeq, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, Generations: append([]PersistentManifest(nil), stats.Manifests...)}
		if err := dataset.Save(opt.CatalogPath); err != nil {
			return stats, fmt.Errorf("publish partition catalog: %w", err)
		}
	}
	if progressPath != "" {
		if err := os.Remove(progressPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return stats, fmt.Errorf("remove completed partition checkpoint: %w", err)
		}
	}
	return stats, nil
}

// validateSavedPartition performs a metadata-only check before resuming. It
// verifies that the catalog still points at the saved generation and that the
// generation's payload is present, without decoding the full partition.
func validateSavedPartition(man PersistentManifest) error {
	if man.IndexID == "" || man.Generation == 0 || man.Path == "" {
		return errors.New("saved partition manifest is incomplete")
	}
	base := filepath.Dir(man.Path)
	if filepath.Base(base) != cleanIndexID(man.IndexID) || filepath.Base(man.Path) != fmt.Sprintf("generation-%020d", man.Generation) {
		return fmt.Errorf("saved partition %q has an unexpected generation path", man.IndexID)
	}
	current, err := os.ReadFile(filepath.Join(base, "CURRENT"))
	if err != nil {
		return fmt.Errorf("read saved partition %q CURRENT: %w", man.IndexID, err)
	}
	if strings.TrimSpace(string(current)) != filepath.Base(man.Path) {
		return fmt.Errorf("saved partition %q no longer points to generation %d", man.IndexID, man.Generation)
	}
	manifestPath := filepath.Join(man.Path, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("saved partition %q manifest is unavailable: %w", man.IndexID, err)
	}
	var persisted PersistentManifest
	if err := json.Unmarshal(manifestBytes, &persisted); err != nil {
		return fmt.Errorf("decode saved partition %q manifest: %w", man.IndexID, err)
	}
	if persisted.IndexID != man.IndexID || persisted.Generation != man.Generation || persisted.Docs != man.Docs {
		return fmt.Errorf("saved partition %q manifest does not match its checkpoint", man.IndexID)
	}
	dataPath := "index.gob"
	if strings.EqualFold(man.Format, string(LayoutMMapParts)) {
		dataPath = "segment.dat"
	}
	if _, err := os.Stat(filepath.Join(man.Path, dataPath)); err != nil {
		return fmt.Errorf("saved partition %q data is unavailable: %w", man.IndexID, err)
	}
	return nil
}

func partitionBuildFingerprint(src Source, opt PartitionedBuildOptions) (string, bool) {
	identity, ok := src.(SourceIdentity)
	if !ok {
		return "", false
	}
	sourceID := identity.ResumeIdentity()
	if sourceID == "" {
		return "", false
	}
	value := struct {
		IndexID         string `json:"index_id"`
		Source          string `json:"source"`
		Config          Config `json:"config"`
		RowsPerPart     uint64 `json:"rows_per_partition"`
		MaxBatchBytes   uint64 `json:"max_batch_bytes"`
		MaxPartBytes    uint64 `json:"max_partition_bytes"`
		MaxRows         uint64 `json:"max_rows"`
		SkipBadRecords  bool   `json:"skip_bad_records"`
		EnableComposite bool   `json:"enable_composite"`
		Freeze          bool   `json:"freeze"`
	}{cleanIndexID(opt.IndexID), sourceID, opt.Config, opt.RowsPerPartition, opt.MaxBatchBytes, opt.MaxPartitionBytes, opt.MaxRows, opt.Bulk.SkipBadRecords, opt.EnableComposite, opt.Freeze}
	data, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), true
}

// LowMemoryBillionBudget is a laptop-safe profile. It favors more partitions so
// a 16GB machine never tries to hold millions of rows in one heap index.
func LowMemoryBillionBudget() MemoryBudget {
	return MemoryBudget{MaxHeapBytes: 2 << 30, MaxActiveSegmentBytes: 128 << 20, TargetRowsPerPartition: 250_000, DefaultBatchSize: 16_384}
}

// SearchPartitionedPersistent searches persisted partitions with bounded
// parallelism. The worker limit keeps memory proportional to active partitions
// instead of the total dataset.
var ErrPartitionedSearchUnsupported = errors.New("partitioned persistent search does not support requested feature")

// Partition decoding materializes an Index in heap memory. Share a process-wide
// cap across requests so concurrent HTTP searches cannot multiply the per-call
// worker count into an unbounded number of decoded indexes.
var partitionSearchSlots = make(chan struct{}, 2)

func SearchPartitionedPersistent(ctx context.Context, store PersistentStore, manifests []PersistentManifest, cfg Config, req SearchRequest, dst []Hit) ([]Hit, error) {
	result, err := SearchPartitionedPersistentResult(ctx, store, manifests, cfg, req, dst)
	return result.Hits, err
}

// SearchPartitionedPersistentResult prunes impossible partitions, leases mapped
// or legacy indexes through a bounded cache, and merges local candidates by score. BM25 scores
// calculated against separate partition corpora are not globally calibrated.
// Broad queries still visit every possible partition; the function never returns
// early merely because one partition fills the requested page.
func SearchPartitionedPersistentResult(ctx context.Context, store PersistentStore, manifests []PersistentManifest, cfg Config, req SearchRequest, dst []Hit) (Result, error) {
	started := time.Now()
	if store == nil {
		return Result{}, errors.New("persistent store required")
	}
	if len(req.Sort) > 0 {
		return Result{}, fmt.Errorf("%w: field sorting", ErrPartitionedSearchUnsupported)
	}
	if len(req.Facets) > 0 {
		return Result{}, fmt.Errorf("%w: facets", ErrPartitionedSearchUnsupported)
	}
	if req.AfterDocID > 0 {
		return Result{}, fmt.Errorf("%w: after_doc_id pagination", ErrPartitionedSearchUnsupported)
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	if req.Offset < 0 || limit > math.MaxInt-req.Offset {
		return Result{}, errors.New("invalid partitioned search limit or offset")
	}
	req.Limit = limit
	signature := partitionConfigSignature(cfg)
	if unfilteredPartitionQuery(req.Query) {
		return browsePartitionedPersistent(ctx, store, manifests, cfg, req, dst, signature)
	}
	schema := summarySchema(cfg)
	candidates := make([]PersistentManifest, 0, len(manifests))
	for _, man := range manifests {
		if man.Summary == nil || man.Summary.Schema != schema || man.Summary.match(req.Query, cfg) {
			candidates = append(candidates, man)
		}
	}
	manifests = candidates
	if rule, ok := req.Query.(latestPerGroupQuery); ok {
		query, err := preparePartitionLatest(ctx, store, manifests, cfg, rule, signature)
		if err != nil {
			return Result{}, err
		}
		req.Query = query
	}
	pageSize := limit + req.Offset
	dst = dst[:0]
	total := 0
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > cap(partitionSearchSlots) {
		workers = cap(partitionSearchSlots)
	}
	if workers > len(manifests) {
		workers = len(manifests)
	}
	if workers == 0 {
		workers = 1
	}
	type partitionResult struct {
		hits  []Hit
		total int
		err   error
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan PersistentManifest)
	results := make(chan partitionResult, workers)
	var wg sync.WaitGroup
	localReq := req
	localReq.Limit = pageSize
	localReq.Offset = 0
	localReq.AfterDocID = 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for man := range jobs {
				select {
				case partitionSearchSlots <- struct{}{}:
				case <-workCtx.Done():
					return
				}
				ix, release, err := servingPartitions.acquire(workCtx, store, man, cfg, signature)
				if err != nil {
					<-partitionSearchSlots
					results <- partitionResult{err: err}
					cancel()
					continue
				}
				local := make([]Hit, 0, pageSize)
				localResult, local := ix.SearchInto(localReq, local)
				detachPartitionHits(local)
				release()
				<-partitionSearchSlots

				results <- partitionResult{hits: local, total: localResult.Total}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, man := range manifests {
			select {
			case jobs <- man:
			case <-workCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	var searchErr error
	for part := range results {
		if part.err != nil {
			if searchErr == nil {
				searchErr = part.err
			}
			continue
		}
		if !req.SkipTotal {
			if part.total > math.MaxInt-total {
				searchErr = errors.New("partitioned search total exceeds platform integer range")
				cancel()
				continue
			}
			total += part.total
		}
		dst = append(dst, part.hits...)
		sort.SliceStable(dst, func(i, j int) bool {
			if dst[i].Score != dst[j].Score {
				return dst[i].Score > dst[j].Score
			}
			if dst[i].ID != dst[j].ID {
				return dst[i].ID < dst[j].ID
			}
			return dst[i].DocID < dst[j].DocID
		})
		if len(dst) > pageSize {
			dst = dst[:pageSize]
		}
	}
	if searchErr != nil {
		return Result{Hits: dst, Total: total, Took: time.Since(started).Nanoseconds()}, searchErr
	}
	if err := ctx.Err(); err != nil {
		return Result{Hits: dst, Total: total, Took: time.Since(started).Nanoseconds()}, err
	}
	if req.Offset >= len(dst) {
		dst = dst[:0]
	} else if req.Offset > 0 {
		dst = dst[req.Offset:]
	}
	if len(dst) > limit {
		dst = dst[:limit]
	}
	if req.SkipTotal {
		total = 0
	}
	return Result{Hits: dst, Total: total, Took: time.Since(started).Nanoseconds()}, nil
}
