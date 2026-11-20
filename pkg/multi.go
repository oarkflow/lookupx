package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fhmetrics "github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/squealx"
)

// SourceFactory builds a streaming source for a concrete index. The factory gets
// the freshly-created index so it can resolve FieldID values from the compiled schema.
type SourceFactory func(ix *Index) (Source, error)

// IndexDefinition describes one independently reloadable index such as dataset_a,
// dataset_b, dataset_c, claims, members, or any tenant/domain-specific lookup index.
type IndexDefinition struct {
	ID          string
	Config      Config
	Source      SourceFactory
	BulkOptions BulkOptions
}

// IndexLatency captures the latest load/reload timing for an index.
type IndexLatency struct {
	LastIndexed       uint64        `json:"last_indexed"`
	LastSkipped       uint64        `json:"last_skipped"`
	LastReloadTook    time.Duration `json:"last_reload_took"`
	LastReloadNS      int64         `json:"last_reload_ns"`
	LastRowsPerSecond float64       `json:"last_rows_per_second"`
	LastError         string        `json:"last_error,omitempty"`
	LastReloadAt      time.Time     `json:"last_reload_at,omitempty"`
	LastResources     ResourceUsage `json:"last_resources"`
}

type ManagedIndex struct {
	ID         string        `json:"id"`
	Index      *Index        `json:"-"`
	Config     Config        `json:"-"`
	Source     SourceFactory `json:"-"`
	Bulk       BulkOptions   `json:"-"`
	Latency    IndexLatency  `json:"latency"`
	Progress   BulkProgress  `json:"progress,omitempty"`
	Reloading  bool          `json:"reloading"`
	Generation uint64        `json:"generation"`
}

// MultiIndexManager owns multiple live indexes and can atomically reload any
// single index without stopping searches on other indexes.
type MultiIndexManager struct {
	mu      sync.RWMutex
	indexes map[string]*ManagedIndex
}

// RestorePersistent discovers every index directory with a CURRENT generation
// and atomically adds the restored indexes to the manager.
func (m *MultiIndexManager) RestorePersistent(ctx context.Context, store FileSegmentStore) error {
	entries, err := os.ReadDir(store.Root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := cleanIndexID(entry.Name())
		if id == "" {
			continue
		}
		if err := m.RestorePersistentIndex(ctx, store, id, Config{}); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("restore index %q: %w", id, err)
		}
	}
	return nil
}

// RestorePersistentIndex replaces one configured index with its saved
// generation while retaining the BCL datasource factory and reload settings.
func (m *MultiIndexManager) RestorePersistentIndex(ctx context.Context, store FileSegmentStore, id string, cfg Config) error {
	id = cleanIndexID(id)
	m.mu.Lock()
	old := m.indexes[id]
	if old != nil && old.Reloading {
		m.mu.Unlock()
		return fmt.Errorf("index %q is already reloading", id)
	}
	if old != nil {
		old.Reloading = true
	}
	m.mu.Unlock()
	ix, man, err := OpenPersistent(ctx, store, id, cfg)
	if errors.Is(err, os.ErrNotExist) {
		m.setReloading(id, false, "")
		return err
	}
	if err != nil {
		m.setReloading(id, false, err.Error())
		return err
	}
	m.mu.Lock()
	if current := m.indexes[id]; current != old {
		m.mu.Unlock()
		_ = ix.Close()
		m.setReloading(id, false, "")
		return fmt.Errorf("index %q changed while restore was being prepared", id)
	}
	mi := &ManagedIndex{ID: cleanIndexID(id), Index: ix, Config: ix.cfg, Generation: man.Generation}
	if old != nil {
		mi.Source, mi.Bulk, mi.Latency = old.Source, old.Bulk, old.Latency
	}
	m.indexes[mi.ID] = mi
	m.mu.Unlock()
	if old != nil && old.Index != nil {
		_ = old.Index.Close()
	}
	return nil
}

func NewMultiIndexManager() *MultiIndexManager {
	return &MultiIndexManager{indexes: make(map[string]*ManagedIndex)}
}

func (m *MultiIndexManager) Register(def IndexDefinition) (*ManagedIndex, error) {
	id := cleanIndexID(def.ID)
	if id == "" {
		return nil, errors.New("index id required")
	}
	if def.Config.Schema.Fields == nil {
		return nil, errors.New("index schema required")
	}
	ix, err := New(def.Config)
	if err != nil {
		return nil, err
	}
	mi := &ManagedIndex{ID: id, Index: ix, Config: def.Config, Source: def.Source, Bulk: def.BulkOptions, Generation: 1}
	m.mu.Lock()
	old := m.indexes[id]
	m.indexes[id] = mi
	m.mu.Unlock()
	if old != nil && old.Index != nil {
		_ = old.Index.Close()
	}
	return mi, nil
}

func (m *MultiIndexManager) AddIndex(id string, ix *Index) error {
	id = cleanIndexID(id)
	if id == "" || ix == nil {
		return errors.New("index id and index required")
	}
	mi := &ManagedIndex{ID: id, Index: ix, Config: ix.cfg, Generation: 1}
	m.mu.Lock()
	old := m.indexes[id]
	m.indexes[id] = mi
	m.mu.Unlock()
	if old != nil && old.Index != nil && old.Index != ix {
		_ = old.Index.Close()
	}
	return nil
}

// EnsureIndex creates an empty index only when the name is unused. It is used
// by integrations that infer a schema while loading a new index.
func (m *MultiIndexManager) EnsureIndex(id string, cfg Config) (*ManagedIndex, bool, error) {
	id = cleanIndexID(id)
	if id == "" {
		return nil, false, errors.New("index id required")
	}
	m.mu.RLock()
	current := m.indexes[id]
	m.mu.RUnlock()
	if current != nil {
		return current, false, nil
	}
	ix, err := New(cfg)
	if err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	if current = m.indexes[id]; current != nil {
		m.mu.Unlock()
		_ = ix.Close()
		return current, false, nil
	}
	current = &ManagedIndex{ID: id, Index: ix, Config: cfg, Generation: 1}
	m.indexes[id] = current
	m.mu.Unlock()
	return current, true, nil
}

func (m *MultiIndexManager) Get(id string) (*Index, bool) {
	m.mu.RLock()
	mi := m.indexes[cleanIndexID(id)]
	m.mu.RUnlock()
	if mi == nil || mi.Index == nil {
		return nil, false
	}
	return mi.Index, true
}

func (m *MultiIndexManager) Remove(id string) bool {
	id = cleanIndexID(id)
	m.mu.Lock()
	mi, ok := m.indexes[id]
	if ok {
		delete(m.indexes, id)
	}
	m.mu.Unlock()
	if ok && mi != nil && mi.Index != nil {
		_ = mi.Index.Close()
	}
	return ok
}

func (m *MultiIndexManager) Managed(id string) (*ManagedIndex, bool) {
	m.mu.RLock()
	mi := m.indexes[cleanIndexID(id)]
	m.mu.RUnlock()
	return mi, mi != nil
}

// SetBuildProgress updates a background builder's progress without replacing
// the currently registered query index.
func (m *MultiIndexManager) SetBuildProgress(id string, progress BulkProgress, reloading bool, lastError string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mi := m.indexes[cleanIndexID(id)]; mi != nil {
		mi.Progress = progress
		mi.Reloading = reloading
		mi.Latency.LastError = lastError
	}
}

func (m *MultiIndexManager) List() []ManagedIndexInfo {
	m.mu.RLock()
	out := make([]ManagedIndexInfo, 0, len(m.indexes))
	for id, mi := range m.indexes {
		info := ManagedIndexInfo{ID: id, Generation: mi.Generation, Reloading: mi.Reloading, Latency: mi.Latency, Progress: mi.Progress}
		if mi.Index != nil {
			info.Stats = mi.Index.Stats()
		}
		out = append(out, info)
	}
	m.mu.RUnlock()
	// Go randomizes map iteration order per process; without this sort, the
	// startup build order (and therefore its logs) would appear to change on
	// every restart even though nothing about the data changes.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

type ManagedIndexInfo struct {
	ID          string       `json:"id"`
	Generation  uint64       `json:"generation"`
	Reloading   bool         `json:"reloading"`
	Stats       Stats        `json:"stats"`
	TermsKnown  bool         `json:"terms_known"`
	Partitioned bool         `json:"partitioned"`
	Latency     IndexLatency `json:"latency"`
	Progress    BulkProgress `json:"progress,omitempty"`
}

func (m *MultiIndexManager) Reload(ctx context.Context, id string) (BulkStats, error) {
	m.mu.RLock()
	mi := m.indexes[cleanIndexID(id)]
	m.mu.RUnlock()
	if mi == nil {
		return BulkStats{}, fmt.Errorf("index %q not found", id)
	}
	if mi.Source == nil {
		return BulkStats{}, fmt.Errorf("index %q has no registered source", id)
	}
	return m.ReloadFromFactory(ctx, id, mi.Source, mi.Bulk)
}

func (m *MultiIndexManager) ReloadFromFactory(ctx context.Context, id string, factory SourceFactory, opt BulkOptions) (BulkStats, error) {
	m.mu.RLock()
	old := m.indexes[cleanIndexID(id)]
	m.mu.RUnlock()
	if old == nil {
		return BulkStats{}, fmt.Errorf("index %q not found", id)
	}
	return m.ReloadWithConfig(ctx, id, old.Config, factory, opt)
}

// ReloadWithConfig builds a replacement using a new schema. A populated index
// remains active until the replacement is complete. If this is an initial
// build with no documents to serve, the new index is published immediately and
// each successfully flushed batch becomes queryable while ingestion continues.
// The source factory receives the replacement index so column names resolve
// against its compiled schema.
func (m *MultiIndexManager) ReloadWithConfig(ctx context.Context, id string, cfg Config, factory SourceFactory, opt BulkOptions) (BulkStats, error) {
	id = cleanIndexID(id)
	m.mu.RLock()
	old := m.indexes[id]
	m.mu.RUnlock()
	if old == nil {
		return BulkStats{}, fmt.Errorf("index %q not found", id)
	}
	if factory == nil {
		factory = old.Source
	}
	if factory == nil {
		return BulkStats{}, fmt.Errorf("index %q has no source", id)
	}
	m.mu.Lock()
	if m.indexes[id] != old {
		m.mu.Unlock()
		return BulkStats{}, fmt.Errorf("index %q changed while reload was being prepared", id)
	}
	if old.Reloading {
		m.mu.Unlock()
		return BulkStats{}, fmt.Errorf("index %q is already reloading", id)
	}
	old.Reloading = true
	old.Progress = BulkProgress{SourceName: opt.Name, StartedAt: time.Now()}
	m.mu.Unlock()
	if opt.Resume && opt.Checkpoint != nil {
		checkpointName := opt.Name
		if checkpointName == "" {
			checkpointName = id
		}
		seq, err := opt.Checkpoint.Load(ctx, checkpointName)
		if err != nil {
			m.setReloading(id, false, err.Error())
			return BulkStats{}, fmt.Errorf("load reload checkpoint: %w", err)
		}
		if seq > 0 {
			message := fmt.Sprintf("cannot resume replacement index %q from source checkpoint %d: the replacement index is empty; use a durable partition build that restores its matching index generation, or restart with resume=false", id, seq)
			m.setReloading(id, false, message)
			return BulkStats{}, errors.New(message)
		}
	}
	usageBefore := readResourceUsage()
	started := time.Now()
	ix, err := New(cfg)
	if err != nil {
		m.setReloading(id, false, err.Error())
		return BulkStats{}, err
	}
	previousIndex := old.Index
	exposePartial := previousIndex == nil || previousIndex.Stats().Docs == 0
	active := old
	if exposePartial {
		// A first build has no complete generation to serve. Publish its empty
		// index before ingest starts so each successfully flushed batch becomes
		// queryable while the build remains pending.
		active = &ManagedIndex{ID: id, Index: ix, Config: cfg, Source: factory, Bulk: old.Bulk, Latency: old.Latency, Progress: old.Progress, Reloading: true, Generation: old.Generation}
		m.mu.Lock()
		if m.indexes[id] != old {
			m.mu.Unlock()
			_ = ix.Close()
			return BulkStats{}, fmt.Errorf("index %q changed while reload was being prepared", id)
		}
		m.indexes[id] = active
		m.mu.Unlock()
	}
	src, err := factory(ix)
	if err != nil {
		if !exposePartial {
			_ = ix.Close()
		}
		m.mu.Lock()
		if m.indexes[id] == active {
			if exposePartial {
				active.Reloading = false
				active.Latency.LastError = err.Error()
			} else {
				old.Reloading = false
				old.Latency.LastError = err.Error()
			}
		}
		m.mu.Unlock()
		return BulkStats{}, err
	}
	if opt.Name == "" {
		opt.Name = id
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 65536
	}
	progressCallback := opt.Progress
	opt.Progress = func(progress BulkProgress) {
		m.mu.Lock()
		if current := m.indexes[id]; current == active {
			current.Progress = progress
		}
		m.mu.Unlock()
		if progressCallback != nil {
			progressCallback(progress)
		}
	}
	stats, err := ix.IndexFrom(ctx, src, opt)
	took := time.Since(started)
	lat := latencyFromStats(stats, took, err)
	lat.LastResources = usageDelta(usageBefore, readResourceUsage())
	m.mu.Lock()
	cur := m.indexes[id]
	if cur != active {
		m.mu.Unlock()
		_ = ix.Close()
		return stats, fmt.Errorf("index %q changed while reload was running", id)
	}
	if err == nil {
		ix.cfg = cfg
		m.indexes[id] = &ManagedIndex{ID: id, Index: ix, Config: cfg, Source: factory, Bulk: opt, Latency: lat, Progress: BulkProgress{SourceName: opt.Name, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, StartedAt: old.Progress.StartedAt, Took: took}, Generation: old.Generation + 1}
	} else if exposePartial {
		active.Reloading = false
		active.Latency = lat
		active.Progress = BulkProgress{SourceName: opt.Name, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, StartedAt: old.Progress.StartedAt, Took: took}
	} else {
		cur.Reloading = false
		cur.Latency = lat
	}
	m.mu.Unlock()
	if err != nil {
		if !exposePartial {
			_ = ix.Close()
		}
		return stats, err
	}
	if previousIndex != nil && previousIndex != ix {
		_ = previousIndex.Close()
	}
	return stats, nil
}

func (m *MultiIndexManager) ReloadFromSource(ctx context.Context, id string, src Source, opt BulkOptions) (BulkStats, error) {
	if src == nil {
		return BulkStats{}, errors.New("source required")
	}
	return m.ReloadFromFactory(ctx, id, func(ix *Index) (Source, error) { return src, nil }, opt)
}

func (m *MultiIndexManager) setReloading(id string, reloading bool, lastErr string) {
	m.mu.Lock()
	if mi := m.indexes[cleanIndexID(id)]; mi != nil {
		mi.Reloading = reloading
		if lastErr != "" {
			mi.Latency.LastError = lastErr
		}
	}
	m.mu.Unlock()
}

func latencyFromStats(stats BulkStats, took time.Duration, err error) IndexLatency {
	lat := IndexLatency{LastIndexed: stats.Indexed, LastSkipped: stats.Skipped, LastReloadTook: took, LastReloadNS: took.Nanoseconds(), LastReloadAt: time.Now()}
	if took > 0 {
		lat.LastRowsPerSecond = float64(stats.Indexed) / took.Seconds()
	}
	if err != nil {
		lat.LastError = err.Error()
	}
	return lat
}

func cleanIndexID(id string) string {
	id = strings.TrimSpace(strings.ToLower(id))
	id = strings.Trim(id, "/")
	return id
}

// MultiServer exposes operational APIs for many indexes.
type MultiServer struct {
	Manager               *MultiIndexManager
	APIKeys               []string
	AdminUser             string
	AdminPassword         string
	SessionSecret         string
	SessionSecure         bool
	SessionTrustedProxies []string
	ConfigPath            string
	auth                  *adminAuth
	configMu              sync.Mutex
	WebDir                string // optional: directory to serve static frontend files
	DataDir               string // persistent index root; defaults to LOOKUPX_DATA_DIR or data/indexes
	Storage               string // "memory" (default) or "disk"; disk persists after reload
	Requests              uint64
	metrics               *fhmetrics.Metrics
}

// wireQueryShape returns query structure without values, which can contain
// sensitive search text. It keeps routine query logs useful without logging
// the search payload.
func wireQueryShape(q WireQuery) string {
	types, fields := map[string]struct{}{}, map[string]struct{}{}
	var visit func(WireQuery)
	visit = func(node WireQuery) {
		if node.Type != "" {
			types[strings.ToLower(node.Type)] = struct{}{}
		}
		if node.Field != "" {
			fields[node.Field] = struct{}{}
		}
		for _, children := range [][]WireQuery{node.Must, node.Should, node.Filter, node.MustNot} {
			for _, child := range children {
				visit(child)
			}
		}
	}
	visit(q)
	typeList, fieldList := make([]string, 0, len(types)), make([]string, 0, len(fields))
	for value := range types {
		typeList = append(typeList, value)
	}
	for value := range fields {
		fieldList = append(fieldList, value)
	}
	sort.Strings(typeList)
	sort.Strings(fieldList)
	return fmt.Sprintf("types=%s fields=%s", strings.Join(typeList, ","), strings.Join(fieldList, ","))
}

func (s *MultiServer) diskStorageEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(s.Storage), "disk")
}

func (s *MultiServer) persistentStore() FileSegmentStore {
	root := s.DataDir
	if root == "" {
		root = os.Getenv("LOOKUPX_DATA_DIR")
	}
	if root == "" {
		root = filepath.Join("data", "indexes")
	}
	return FileSegmentStore{Root: root}
}

func (s *MultiServer) persistManagedIndex(ctx context.Context, id string) (PersistentManifest, error) {
	ix, ok := s.Manager.Get(id)
	if !ok {
		return PersistentManifest{}, errors.New("index not found")
	}
	store := s.persistentStore()
	man, err := store.SaveIndex(ctx, id, ix)
	if err != nil {
		return man, err
	}
	_, _ = CompactPersistentGenerations(ctx, store, id, GenerationPolicy{KeepLast: 2})
	return man, nil
}

type CreateIndexRequest struct {
	ID          string      `json:"id"`
	Schema      string      `json:"schema,omitempty"`
	Config      Config      `json:"config"`
	BulkOptions BulkOptions `json:"bulk_options"`
}

type SQLReloadRequest struct {
	Driver         string          `json:"driver"`
	DSN            string          `json:"dsn"`
	Query          string          `json:"query"`
	Args           []any           `json:"args"`
	IDColumn       string          `json:"id_column"`
	SeqColumn      string          `json:"seq_column"`
	PageSize       int             `json:"page_size,omitempty"`
	Columns        []SQLColumnSpec `json:"columns"`
	BatchSize      int             `json:"batch_size"`
	CheckpointPath string          `json:"checkpoint_path"`
	Resume         bool            `json:"resume"`
}

type SQLTableReloadRequest struct {
	Driver         string          `json:"driver"`
	DSN            string          `json:"dsn"`
	Table          string          `json:"table"`
	SelectColumns  []string        `json:"select_columns"`
	Where          string          `json:"where"`
	OrderColumn    string          `json:"order_column"`
	IDColumn       string          `json:"id_column"`
	SeqColumn      string          `json:"seq_column"`
	PageSize       int             `json:"page_size"`
	Columns        []SQLColumnSpec `json:"columns"`
	BatchSize      int             `json:"batch_size"`
	CheckpointPath string          `json:"checkpoint_path"`
	Resume         bool            `json:"resume"`
}

type SQLColumnSpec struct {
	Column     string `json:"column"`
	Field      string `json:"field"`
	Kind       string `json:"kind"`
	Normalized bool   `json:"normalized"`
	Layout     string `json:"layout,omitempty"`
}

func (s *MultiServer) reloadSQLRequest(ctx context.Context, id string, req SQLReloadRequest) (BulkStats, error) {
	if req.PageSize < 0 {
		return BulkStats{}, errors.New("page_size cannot be negative")
	}
	if req.PageSize > 0 && req.SeqColumn == "" {
		return BulkStats{}, errors.New("page_size requires a stable numeric seq_column")
	}
	if req.PageSize > 0 && req.IDColumn == "" {
		return BulkStats{}, errors.New("keyset paging requires an id_column for stable document IDs")
	}
	if req.PageSize > 0 && !validSQLIdentifier(req.SeqColumn) {
		return BulkStats{}, errors.New("seq_column must be a simple SELECT output identifier for keyset paging")
	}
	baseQuery := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(req.Query), ";"))
	if req.PageSize > 0 && hasSQLPageTokens(baseQuery) {
		if err := validateSQLPageTemplate(baseQuery, req.SeqColumn); err != nil {
			return BulkStats{}, fmt.Errorf("invalid paged SQL template: %w", err)
		}
	}
	mi, ok := s.Manager.Managed(id)
	dsn, err := secureSQLiteDSN(req.Driver, req.DSN, s.DataDir)
	if err != nil {
		return BulkStats{}, err
	}
	db, err := squealx.Connect(squealxDriver(req.Driver), dsn, "")
	if err != nil {
		return BulkStats{}, err
	}
	defer db.Close()
	configureBulkSQLPool(db.DB())
	inferred, err := InferSQLColumns(ctx, db.DB(), req.Query, req.Args, req.IDColumn, req.SeqColumn, 200)
	if err != nil {
		return BulkStats{}, err
	}
	inferred = applySQLColumnSpecs(inferred, req.Columns)
	cfg := Config{}
	if ok {
		cfg = mi.Config
	}
	cfg.Schema = AutoSchema(inferred)
	cfg.DisableSource = false
	if _, _, err := s.Manager.EnsureIndex(id, cfg); err != nil {
		return BulkStats{}, err
	}
	opt := bulkFromSQLReq(id, req.BatchSize, req.CheckpointPath, req.Resume)
	return s.Manager.ReloadWithConfig(ctx, id, cfg, func(ix *Index) (Source, error) {
		cols := BindSQLColumns(ix, inferred)
		if req.PageSize > 0 {
			page := func(last uint64, limit int) (string, []any) {
				return buildSQLQueryPageForSource(baseQuery, req.Args, req.SeqColumn, req.Driver, last, limit)
			}
			return PagedSQLQuerySource{DB: db.DB(), Page: page, IDColumn: req.IDColumn, SeqColumn: req.SeqColumn, Columns: cols, PageSize: req.PageSize}, nil
		}
		return SQLQuerySource{DB: db.DB(), Query: req.Query, Args: req.Args, IDColumn: req.IDColumn, SeqColumn: req.SeqColumn, Columns: cols}, nil
	}, opt)
}

func (s *MultiServer) reloadTableRequest(ctx context.Context, id string, req SQLTableReloadRequest) (BulkStats, error) {
	if req.OrderColumn == "" && req.IDColumn != "" {
		req.OrderColumn = req.IDColumn
	}
	mi, ok := s.Manager.Managed(id)
	db, inferred, err := connectAndSample(ctx, sqlSampleRequest{
		Driver: req.Driver, DSN: req.DSN, Source: "sql_table", Table: req.Table,
		Where: req.Where, IDColumn: req.IDColumn, SeqColumn: req.SeqColumn,
		OrderColumn: req.OrderColumn, SampleSize: 200, DataDir: s.DataDir,
	})
	if err != nil {
		return BulkStats{}, err
	}
	defer db.Close()
	inferred = applySQLColumnSpecs(inferred, req.Columns)
	cfg := Config{}
	if ok {
		cfg = mi.Config
	}
	cfg.Schema = AutoSchema(inferred)
	cfg.DisableSource = false
	if _, _, err := s.Manager.EnsureIndex(id, cfg); err != nil {
		return BulkStats{}, err
	}
	selectColumns := []string{}
	if req.IDColumn != "" {
		selectColumns = append(selectColumns, req.IDColumn)
	}
	if req.SeqColumn != "" && !strings.EqualFold(req.SeqColumn, req.IDColumn) {
		selectColumns = append(selectColumns, req.SeqColumn)
	}
	for _, col := range inferred {
		selectColumns = append(selectColumns, col.Column)
	}
	opt := bulkFromSQLReq(id, req.BatchSize, req.CheckpointPath, req.Resume)
	return s.Manager.ReloadWithConfig(ctx, id, cfg, func(ix *Index) (Source, error) {
		return PagedSQLSource{DB: db.DB(), Table: req.Table, Columns: selectColumns, Where: req.Where, OrderColumn: req.OrderColumn, PageSize: req.PageSize, IDColumn: req.IDColumn, SeqColumn: req.SeqColumn, ColumnBindings: BindSQLColumns(ix, inferred)}, nil
	}, opt)
}

func bulkFromSQLReq(id string, batch int, checkpointPath string, resume bool) BulkOptions {
	opt := BulkOptions{Name: id, BatchSize: batch, Resume: resume, CheckpointEvery: batch}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 65536
		opt.CheckpointEvery = opt.BatchSize
	}
	if checkpointPath != "" {
		opt.Checkpoint = FileCheckpoint{Path: checkpointPath}
	}
	return opt
}

func applySQLColumnSpecs(inferred []AutoColumn, specs []SQLColumnSpec) []AutoColumn {
	byColumn := make(map[string]SQLColumnSpec, len(specs))
	for _, spec := range specs {
		byColumn[strings.ToLower(spec.Column)] = spec
	}
	for i := range inferred {
		spec, ok := byColumn[strings.ToLower(inferred[i].Column)]
		if !ok {
			continue
		}
		if spec.Field != "" {
			inferred[i].Field = spec.Field
		}
		if spec.Kind != "" {
			inferred[i].Kind = parseValueKind(spec.Kind)
			inferred[i].Options = fieldOptionsForValueKind(inferred[i].Kind)
		}
		if spec.Layout != "" {
			inferred[i].Layout = spec.Layout
		}
	}
	return inferred
}

func fieldOptionsForValueKind(kind ValueKind) FieldOptions {
	switch kind {
	case ValueText:
		return FieldOptions{Kind: FieldText, Indexed: true, Stored: true, Lowercase: true}
	case ValueNumber:
		return FieldOptions{Kind: FieldFloat, Indexed: true, Stored: true, Sortable: true}
	case ValueTimeUnix:
		return FieldOptions{Kind: FieldTime, Indexed: true, Stored: true, Sortable: true}
	case ValueVector:
		return FieldOptions{Kind: FieldVector, Indexed: true, Stored: true}
	default:
		return FieldOptions{Kind: FieldKeyword, Indexed: true, Stored: true, Lookup: true, Lowercase: true}
	}
}

func parseValueKind(kind string) ValueKind {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "text":
		return ValueText
	case "number", "numeric", "float", "int":
		return ValueNumber
	case "time", "unix_time", "time_unix":
		return ValueTimeUnix
	case "vector":
		return ValueVector
	default:
		return ValueKeyword
	}
}

// RegisterDemoIndexes creates empty Dataset, DatasetB, and DatasetC indexes using the
// same record lookup schema. Sources can be attached later and each index can
// be reloaded independently.
func RegisterDemoIndexes(m *MultiIndexManager, initialCapacity int) error {
	for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
		mi, err := m.Register(IndexDefinition{ID: id, Config: Config{Schema: TupleLookupSchema(), DisableSource: true, InitialCapacity: initialCapacity, Clock: StaticClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}}})
		if err != nil {
			return err
		}
		mi.Index.EnableTupleComposite()
	}
	return nil
}

// SaveManagerConfig writes a minimal index list that external apps can extend.
func SaveManagerConfig(path string, infos []ManagedIndexInfo) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(infos, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

// LatencySummary formats indexing/search latency for examples and CLI output.
func LatencySummary(rows uint64, took time.Duration) string {
	if rows == 0 || took <= 0 {
		return "rows=0"
	}
	nsPerRow := float64(took.Nanoseconds()) / float64(rows)
	rps := float64(rows) / took.Seconds()
	return fmt.Sprintf("rows=%d took=%s ns_per_row=%.1f rows_per_sec=%.0f", rows, took.Round(time.Microsecond), nsPerRow, rps)
}

// TimeSearch runs the same query repeatedly and returns average latency.
func TimeSearch(ix *Index, q Query, limit, loops int) (hits int, totalNS int64, avgNS int64) {
	if loops <= 0 {
		loops = 1
	}
	var out []Hit
	started := time.Now()
	for i := 0; i < loops; i++ {
		_, out = ix.SearchInto(SearchRequest{Query: q, Limit: limit}, out[:0])
		hits = len(out)
	}
	totalNS = time.Since(started).Nanoseconds()
	avgNS = totalNS / int64(loops)
	return hits, totalNS, avgNS
}

func ParseIntDefault(s string, def int) int {
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	return def
}
