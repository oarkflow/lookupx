package lookup

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StreamingDatasetSource generates deterministic Dataset/Dataset/DatasetC-like record rows
// without materializing the dataset. It is intended for examples, benchmarks and
// smoke tests of billion-row ingestion plans. Only the active SourceRecord is
// allocated by the caller and reused for every row.
type StreamingDatasetSource struct {
	Rows          uint64
	Term          FieldID
	Group      FieldID
	DateKey           FieldID
	Partition      FieldID
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
// It is the low-memory path for 100M/1B row loads: memory is bounded by
// RowsPerPartition plus BatchSize rather than total source size.
type PartitionedBuildOptions struct {
	IndexID          string
	Store            PersistentStore
	Config           Config
	RowsPerPartition uint64
	Bulk             BulkOptions
	EnableComposite  bool
	Freeze           bool
	MaxRows          uint64
	Progress         func(PartitionedBuildProgress)
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
	Seen       uint64               `json:"seen"`
	Indexed    uint64               `json:"indexed"`
	Skipped    uint64               `json:"skipped"`
	LastSeq    uint64               `json:"last_seq"`
	Partitions int                  `json:"partitions"`
	Took       time.Duration        `json:"took"`
	Manifests  []PersistentManifest `json:"manifests,omitempty"`
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
	if opt.Bulk.BatchSize > int(opt.RowsPerPartition) && opt.RowsPerPartition < uint64(^uint(0)>>1) {
		opt.Bulk.BatchSize = int(opt.RowsPerPartition)
	}
	started := time.Now()
	cur, err := src.Open(ctx)
	if err != nil {
		return PartitionedBuildStats{}, err
	}
	defer cur.Close()

	mkIndex := func() (*Index, error) {
		cfg := opt.Config
		if cfg.Schema.Fields == nil {
			cfg.Schema = TupleLookupSchema()
		}
		if cfg.InitialCapacity <= 0 {
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
	for i := 0; i < opt.Bulk.BatchSize; i++ {
		batch = append(batch, SourceRecord{Values: make([]SourceValue, 0, len(ix.fieldList))})
	}
	batch = batch[:0]
	rec := SourceRecord{Values: make([]SourceValue, 0, len(ix.fieldList))}
	var stats PartitionedBuildStats
	stats.IndexID = cleanIndexID(opt.IndexID)
	partitionNo := 0
	partitionRows := uint64(0)
	currentPartitionID := func() string { return fmt.Sprintf("%s-p%06d", stats.IndexID, partitionNo) }

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		indexed, skipped, lastSeq, err := ix.indexSourceBatch(batch, opt.Bulk.SkipBadRecords)
		stats.Indexed += indexed
		stats.Skipped += skipped
		partitionRows += indexed
		if lastSeq > 0 {
			stats.LastSeq = lastSeq
		}
		for i := range batch {
			batch[i].Values = batch[i].Values[:0]
		}
		batch = batch[:0]
		if err != nil {
			return err
		}
		if opt.Bulk.Checkpoint != nil && stats.LastSeq > 0 && opt.Bulk.CheckpointEvery > 0 && stats.Indexed%uint64(opt.Bulk.CheckpointEvery) == 0 {
			if err := opt.Bulk.Checkpoint.Save(ctx, opt.Bulk.Name, stats.LastSeq); err != nil {
				return err
			}
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
		_ = ix.Close()
		partitionNo++
		partitionRows = 0
		ix, err = mkIndex()
		return err
	}

	for cur.Next(ctx, &rec) {
		if opt.MaxRows > 0 && stats.Seen >= opt.MaxRows {
			break
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
	if opt.Bulk.Checkpoint != nil && stats.LastSeq > 0 {
		if err := opt.Bulk.Checkpoint.Save(ctx, opt.Bulk.Name, stats.LastSeq); err != nil {
			return stats, err
		}
	}
	stats.Took = time.Since(started)
	return stats, nil
}

// LowMemoryBillionBudget is a laptop-safe profile. It favors more partitions so
// a 16GB machine never tries to hold millions of rows in one heap index.
func LowMemoryBillionBudget() MemoryBudget {
	return MemoryBudget{MaxHeapBytes: 2 << 30, MaxActiveSegmentBytes: 128 << 20, TargetRowsPerPartition: 250_000, DefaultBatchSize: 16_384}
}

// SearchPartitionedPersistent searches persisted partitions one at a time. It is
// intentionally low-memory: at most one partition index is decoded/resident while
// scanning. For production, pair this with partition pruning so only relevant
// manifests are passed for a given query.
func SearchPartitionedPersistent(ctx context.Context, store PersistentStore, manifests []PersistentManifest, cfg Config, req SearchRequest, dst []Hit) ([]Hit, error) {
	if store == nil {
		return dst[:0], errors.New("persistent store required")
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	dst = dst[:0]
	for _, man := range manifests {
		select {
		case <-ctx.Done():
			return dst, ctx.Err()
		default:
		}
		ix, _, err := store.LoadIndex(ctx, man.IndexID, cfg)
		if err != nil {
			return dst, err
		}
		local := make([]Hit, 0, limit-len(dst))
		_, local = ix.SearchInto(SearchRequest{Query: req.Query, Limit: limit - len(dst)}, local)
		_ = ix.Close()
		dst = append(dst, local...)
		if len(dst) >= limit {
			return dst, nil
		}
	}
	return dst, nil
}
