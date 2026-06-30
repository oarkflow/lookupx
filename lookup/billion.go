package lookup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------
// Billion-row deployment layer.
//
// This file adds the operational pieces required for very large deployments:
// partition manifests, generation management, validation/repair, compaction,
// memory-budgeted bulk options, incremental SQL sync contracts and HTTP helpers.
// The in-memory Index remains the hot query engine; production deployments should
// use many partitions/segments rather than a single 1B-row heap object.
// -----------------------------------------------------------------------------

type StorageLayout string

const (
	LayoutGenerationGob StorageLayout = "generation-gob"
	LayoutMMapParts     StorageLayout = "mmap-parts-v1"
)

type IndexGeneration struct {
	IndexID     string        `json:"index_id"`
	Generation  uint64        `json:"generation"`
	CreatedAt   time.Time     `json:"created_at"`
	Docs        uint64        `json:"docs"`
	LiveDocs    uint64        `json:"live_docs"`
	DeletedDocs uint64        `json:"deleted_docs"`
	Layout      StorageLayout `json:"layout"`
	Path        string        `json:"path"`
	Checksum    string        `json:"checksum,omitempty"`
	Frozen      bool          `json:"frozen"`
}

type GenerationPolicy struct {
	KeepLast       int           `json:"keep_last"`
	MaxAge         time.Duration `json:"max_age"`
	DeleteDangling bool          `json:"delete_dangling"`
}

type ValidationIssue struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type ValidationReport struct {
	IndexID    string             `json:"index_id"`
	OK         bool               `json:"ok"`
	Generation uint64             `json:"generation,omitempty"`
	Stats      Stats              `json:"stats"`
	Manifest   PersistentManifest `json:"manifest"`
	Issues     []ValidationIssue  `json:"issues,omitempty"`
	Took       time.Duration      `json:"took"`
}

func (r *ValidationReport) add(sev, code, msg string) {
	r.OK = false
	r.Issues = append(r.Issues, ValidationIssue{Severity: sev, Code: code, Message: msg})
}

// ListIndexGenerations returns generations newest first.
func ListIndexGenerations(ctx context.Context, store PersistentStore, indexID string) ([]PersistentManifest, error) {
	gens, err := store.ListGenerations(ctx, indexID)
	if err != nil {
		return nil, err
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i].Generation > gens[j].Generation })
	return gens, nil
}

// ValidatePersistentIndex loads the current generation and verifies critical invariants.
func ValidatePersistentIndex(ctx context.Context, store PersistentStore, indexID string, cfg Config) (ValidationReport, error) {
	started := time.Now()
	ix, man, err := store.LoadIndex(ctx, indexID, cfg)
	rep := ValidationReport{IndexID: cleanIndexID(indexID), OK: true, Manifest: man, Generation: man.Generation}
	if err != nil {
		rep.add("error", "load_failed", err.Error())
		rep.Took = time.Since(started)
		return rep, err
	}
	defer ix.Close()
	ix.mu.RLock()
	rep.Stats = ix.Stats()
	if ix.nextDocID == 0 {
		rep.add("error", "bad_next_doc_id", "nextDocID is zero")
	}
	if len(ix.docToExt) == 0 {
		rep.add("error", "missing_doc_to_ext", "docToExt is empty")
	}
	if len(ix.docToExt) > 0 && ix.docToExt[0] != "" {
		rep.add("warning", "doc_zero_reserved", "docToExt[0] should be reserved")
	}
	for ext, did := range ix.extToDoc {
		if did == 0 || int(did) >= len(ix.docToExt) {
			rep.add("error", "ext_to_doc_out_of_range", fmt.Sprintf("%s -> %d out of range", ext, did))
			continue
		}
		if ix.docToExt[did] != ext {
			rep.add("error", "ext_doc_mismatch", fmt.Sprintf("%s -> %d but docToExt=%q", ext, did, ix.docToExt[did]))
		}
	}
	for name, fi := range ix.fields {
		if fi.exists == nil {
			rep.add("error", "field_missing_exists", name+" has no exists bitmap")
		}
	}
	ix.mu.RUnlock()
	if ex := extras(ix); ex != nil {
		ex.mu.RLock()
		hasComposite := ex.composite != nil
		ex.mu.RUnlock()
		if !hasComposite && ix.fields["term"] != nil && ix.fields["group_id"] != nil && ix.fields["date_key"] != nil {
			rep.add("warning", "missing_composite", "record fields exist but composite accelerator is not built")
		}
	}
	rep.Took = time.Since(started)
	return rep, nil
}

// RepairPersistentIndex rebuilds derived/frozen structures and writes a new generation.
func RepairPersistentIndex(ctx context.Context, store PersistentStore, indexID string, cfg Config) (PersistentManifest, ValidationReport, error) {
	ix, _, err := store.LoadIndex(ctx, indexID, cfg)
	if err != nil {
		return PersistentManifest{}, ValidationReport{}, err
	}
	defer ix.Close()
	ix.EnableTupleComposite()
	if err := ix.Freeze(); err != nil {
		return PersistentManifest{}, ValidationReport{}, err
	}
	man, err := store.SaveIndex(ctx, indexID, ix)
	if err != nil {
		return man, ValidationReport{}, err
	}
	rep, err := ValidatePersistentIndex(ctx, store, indexID, cfg)
	return man, rep, err
}

// CompactPersistentGenerations removes old generations after a successful validation.
func CompactPersistentGenerations(ctx context.Context, store FileSegmentStore, indexID string, policy GenerationPolicy) ([]string, error) {
	if policy.KeepLast <= 0 {
		policy.KeepLast = 2
	}
	gens, err := ListIndexGenerations(ctx, store, indexID)
	if err != nil {
		return nil, err
	}
	if len(gens) <= policy.KeepLast {
		return nil, nil
	}
	curBytes, _ := os.ReadFile(filepath.Join(store.dir(indexID), "CURRENT"))
	currentName := strings.TrimSpace(string(curBytes))
	now := time.Now()
	removed := []string{}
	for i, g := range gens {
		base := filepath.Base(g.Path)
		if base == currentName {
			continue
		}
		if i < policy.KeepLast {
			continue
		}
		if policy.MaxAge > 0 && now.Sub(g.CreatedAt) < policy.MaxAge {
			continue
		}
		if err := os.RemoveAll(g.Path); err != nil {
			return removed, err
		}
		removed = append(removed, g.Path)
	}
	return removed, nil
}

// ChecksumFile returns a SHA-256 checksum for validation/backup manifests.
func ChecksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// MemoryBudget controls bulk loading and flush/partition recommendations. 1B-row
// deployments should keep individual partitions small enough to fit this budget.
type MemoryBudget struct {
	MaxHeapBytes           int64 `json:"max_heap_bytes"`
	MaxActiveSegmentBytes  int64 `json:"max_active_segment_bytes"`
	TargetRowsPerPartition int64 `json:"target_rows_per_partition"`
	DefaultBatchSize       int   `json:"default_batch_size"`
}

func DefaultBillionRowBudget() MemoryBudget {
	return MemoryBudget{MaxHeapBytes: 8 << 30, MaxActiveSegmentBytes: 512 << 20, TargetRowsPerPartition: 5_000_000, DefaultBatchSize: 131072}
}

func (b MemoryBudget) BulkOptions(name string, checkpoint CheckpointStore, resume bool) BulkOptions {
	bs := b.DefaultBatchSize
	if bs <= 0 {
		bs = 131072
	}
	return BulkOptions{Name: name, BatchSize: bs, CheckpointEvery: bs, Checkpoint: checkpoint, Resume: resume}
}

func (b MemoryBudget) RecommendedPartitions(rows int64) int {
	target := b.TargetRowsPerPartition
	if target <= 0 {
		target = 5_000_000
	}
	if rows <= target {
		return 1
	}
	n := int((rows + target - 1) / target)
	if n < 1 {
		n = 1
	}
	return n
}

type PartitionScheme struct {
	IndexID         string `json:"index_id"`
	GroupModulo  int    `json:"group_modulo,omitempty"`
	MonthPartitions bool   `json:"month_partitions"`
	PartitionModulo  int    `json:"partition_modulo,omitempty"`
	MaxRowsHint     int64  `json:"max_rows_hint,omitempty"`
}

type PartitionManifest struct {
	IndexID     string    `json:"index_id"`
	PartitionID string    `json:"partition_id"`
	GroupID  uint32    `json:"group_id,omitempty"`
	DateKeyMonth    string    `json:"date_key_month,omitempty"`
	PartitionValue uint32    `json:"partition_value,omitempty"`
	Rows        uint64    `json:"rows"`
	Generation  uint64    `json:"generation,omitempty"`
	Path        string    `json:"path,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

func (s PartitionScheme) TuplePartition(term string, groupID uint32, date_key string, partitionID uint32) string {
	base := cleanIndexID(s.IndexID)
	if base == "" {
		base = "lookup"
	}
	parts := []string{base}
	if s.GroupModulo > 0 {
		parts = append(parts, fmt.Sprintf("wi%02d", int(groupID)%s.GroupModulo))
	} else if groupID > 0 {
		parts = append(parts, fmt.Sprintf("wi%d", groupID))
	}
	if s.MonthPartitions && len(date_key) >= 7 {
		parts = append(parts, strings.ReplaceAll(date_key[:7], "-", ""))
	}
	if s.PartitionModulo > 0 {
		parts = append(parts, fmt.Sprintf("fac%02d", int(partitionID)%s.PartitionModulo))
	}
	return cleanIndexID(strings.Join(parts, "-"))
}

type PartitionDataset struct {
	Root       string              `json:"root"`
	Partitions []PartitionManifest `json:"partitions"`
}

func (c *PartitionDataset) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}
func LoadPartitionDataset(path string) (PartitionDataset, error) {
	var c PartitionDataset
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}

func PlanBillionRowDeployment(indexID string, estimatedRows int64, budget MemoryBudget) map[string]any {
	parts := budget.RecommendedPartitions(estimatedRows)
	rowsPer := estimatedRows
	if parts > 0 {
		rowsPer = (estimatedRows + int64(parts) - 1) / int64(parts)
	}
	return map[string]any{
		"index_id":                     cleanIndexID(indexID),
		"estimated_rows":               estimatedRows,
		"recommended_partitions":       parts,
		"estimated_rows_per_partition": rowsPer,
		"recommended_batch_size":       budget.BulkOptions(indexID, nil, false).BatchSize,
		"layout":                       "multi-partition persistent generations with frozen composite indexes",
	}
}

// -----------------------------------------------------------------------------
// Incremental SQL / CDC-style sync contracts.
// -----------------------------------------------------------------------------

type IncrementalCheckpoint struct {
	LastSeq       uint64    `json:"last_seq"`
	LastUpdatedAt time.Time `json:"last_updated_at"`
	LastID        string    `json:"last_id,omitempty"`
}

type IncrementalCheckpointStore interface {
	LoadIncremental(ctx context.Context, name string) (IncrementalCheckpoint, error)
	SaveIncremental(ctx context.Context, name string, cp IncrementalCheckpoint) error
}

type FileIncrementalCheckpoint struct{ Path string }

func (f FileIncrementalCheckpoint) LoadIncremental(ctx context.Context, name string) (IncrementalCheckpoint, error) {
	b, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return IncrementalCheckpoint{}, nil
	}
	if err != nil {
		return IncrementalCheckpoint{}, err
	}
	all := map[string]IncrementalCheckpoint{}
	if err := json.Unmarshal(b, &all); err != nil {
		return IncrementalCheckpoint{}, err
	}
	return all[name], nil
}
func (f FileIncrementalCheckpoint) SaveIncremental(ctx context.Context, name string, cp IncrementalCheckpoint) error {
	all := map[string]IncrementalCheckpoint{}
	if b, err := os.ReadFile(f.Path); err == nil {
		_ = json.Unmarshal(b, &all)
	}
	all[name] = cp
	if err := os.MkdirAll(filepath.Dir(f.Path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}

// IncrementalSQLQuery creates a keyset query based on (updated_at, id). It is a
// query builder, so callers can use it with SQLQuerySource/PagedSQLQuerySource.
type IncrementalSQLQuery struct {
	BaseSelect    string
	UpdatedColumn string
	IDColumn      string
	Dialect       SQLDialect
	PageSize      int
}

func (q IncrementalSQLQuery) Page(cp IncrementalCheckpoint) (string, []any) {
	if q.UpdatedColumn == "" {
		q.UpdatedColumn = "updated_at"
	}
	if q.IDColumn == "" {
		q.IDColumn = "id"
	}
	if q.PageSize <= 0 {
		q.PageSize = 100000
	}
	ph := q.Dialect.Placeholder
	// BaseSelect must be a SELECT without ORDER/LIMIT. It may already contain WHERE.
	sep := " WHERE "
	if strings.Contains(strings.ToLower(q.BaseSelect), " where ") {
		sep = " AND "
	}
	sql := q.BaseSelect + sep + fmt.Sprintf("(%s > %s OR (%s = %s AND %s > %s)) ORDER BY %s ASC, %s ASC LIMIT %s", q.UpdatedColumn, ph(1), q.UpdatedColumn, ph(2), q.IDColumn, ph(3), q.UpdatedColumn, q.IDColumn, ph(4))
	return sql, []any{cp.LastUpdatedAt, cp.LastUpdatedAt, cp.LastID, q.PageSize}
}

// MutationKind supports CDC/deletion streams.
type MutationKind uint8

const (
	MutationUpsert MutationKind = iota
	MutationDelete
)

type MutationRecord struct {
	Kind   MutationKind
	Record SourceRecord
}

type MutationCursor interface {
	NextMutation(ctx context.Context, dst *MutationRecord) bool
	Err() error
	Close() error
}
type MutationSource interface {
	OpenMutations(ctx context.Context) (MutationCursor, error)
}

type MutationStats struct {
	Seen, Upserted, Deleted, Skipped uint64
	Took                             time.Duration
}

func (ix *Index) ApplyMutations(ctx context.Context, src MutationSource, opt BulkOptions) (MutationStats, error) {
	cur, err := src.OpenMutations(ctx)
	if err != nil {
		return MutationStats{}, err
	}
	defer cur.Close()
	started := time.Now()
	var st MutationStats
	var mr MutationRecord
	for cur.NextMutation(ctx, &mr) {
		st.Seen++
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		default:
		}
		switch mr.Kind {
		case MutationDelete:
			if mr.Record.ID != "" {
				_ = ix.Delete(mr.Record.ID)
				st.Deleted++
			} else {
				st.Skipped++
			}
		default:
			if mr.Record.ID == "" {
				st.Skipped++
				continue
			}
			if err := ix.indexSourceRecord(&mr.Record); err != nil {
				if opt.SkipBadRecords {
					st.Skipped++
					continue
				}
				return st, err
			}
			st.Upserted++
		}
	}
	st.Took = time.Since(started)
	return st, cur.Err()
}

// -----------------------------------------------------------------------------
// HTTP extension endpoints for production-scale operations.
// -----------------------------------------------------------------------------

func (s *MultiServer) serveBillionHTTP(w http.ResponseWriter, r *http.Request, store PersistentStore) bool {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if path == "v1/plan" && r.Method == http.MethodGet {
		rows, _ := strconv.ParseInt(r.URL.Query().Get("rows"), 10, 64)
		if rows <= 0 {
			rows = 1_000_000_000
		}
		idx := r.URL.Query().Get("index")
		if idx == "" {
			idx = "dataset_a"
		}
		writeJSON(w, PlanBillionRowDeployment(idx, rows, DefaultBillionRowBudget()))
		return true
	}
	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "indexes" {
		id, action := parts[2], parts[3]
		switch action {
		case "generations":
			if r.Method == http.MethodGet {
				gens, err := store.ListGenerations(r.Context(), id)
				if err != nil {
					http.Error(w, err.Error(), 500)
				} else {
					writeJSON(w, map[string]any{"generations": gens})
				}
				return true
			}
		case "validate":
			if r.Method == http.MethodPost || r.Method == http.MethodGet {
				rep, err := ValidatePersistentIndex(r.Context(), store, id, Config{})
				if err != nil && len(rep.Issues) == 0 {
					http.Error(w, err.Error(), 500)
				} else {
					writeJSON(w, rep)
				}
				return true
			}
		case "repair":
			if r.Method == http.MethodPost {
				man, rep, err := RepairPersistentIndex(r.Context(), store, id, Config{})
				if err != nil {
					http.Error(w, err.Error(), 500)
				} else {
					writeJSON(w, map[string]any{"manifest": man, "validation": rep})
				}
				return true
			}
		case "compact":
			if r.Method == http.MethodPost {
				fs, ok := store.(FileSegmentStore)
				if !ok {
					http.Error(w, "file segment store required", 500)
					return true
				}
				var req GenerationPolicy
				_ = json.NewDecoder(r.Body).Decode(&req)
				removed, err := CompactPersistentGenerations(r.Context(), fs, id, req)
				if err != nil {
					http.Error(w, err.Error(), 500)
				} else {
					writeJSON(w, map[string]any{"removed": removed})
				}
				return true
			}
		case "plan":
			if r.Method == http.MethodGet {
				rows, _ := strconv.ParseInt(r.URL.Query().Get("rows"), 10, 64)
				if rows <= 0 {
					rows = 1_000_000_000
				}
				writeJSON(w, PlanBillionRowDeployment(id, rows, DefaultBillionRowBudget()))
				return true
			}
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// Source helpers for parallel/partition-oriented database ingestion.
// -----------------------------------------------------------------------------

type ParallelSQLPartition struct {
	Name  string
	Query string
	Args  []any
}

type ParallelSQLSourceSet struct {
	DB         *sql.DB
	Partitions []ParallelSQLPartition
	IDColumn   string
	SeqColumn  string
	Columns    []SQLColumn
}

func (p ParallelSQLSourceSet) SourceFor(i int) SQLQuerySource {
	part := p.Partitions[i]
	return SQLQuerySource{DB: p.DB, Query: part.Query, Args: part.Args, IDColumn: p.IDColumn, SeqColumn: p.SeqColumn, Columns: p.Columns}
}

func IndexParallelSQL(ctx context.Context, mgr *MultiIndexManager, baseIndexID string, set ParallelSQLSourceSet, cfg Config, opt BulkOptions, workers int) ([]BulkStats, error) {
	if workers <= 0 {
		workers = 4
	}
	stats := make([]BulkStats, len(set.Partitions))
	errCh := make(chan error, len(set.Partitions))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range set.Partitions {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			id := cleanIndexID(baseIndexID + "-" + set.Partitions[i].Name)
			if _, err := mgr.Register(IndexDefinition{ID: id, Config: cfg}); err != nil {
				errCh <- err
				return
			}
			st, err := mgr.ReloadFromSource(ctx, id, set.SourceFor(i), opt)
			stats[i] = st
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return stats, err
	}
	return stats, nil
}
