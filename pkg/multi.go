package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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
		ix, man, err := OpenPersistent(ctx, store, id, Config{})
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("restore index %q: %w", id, err)
		}
		mi := &ManagedIndex{ID: id, Index: ix, Config: ix.cfg, Generation: man.Generation}
		m.mu.Lock()
		old := m.indexes[id]
		m.indexes[id] = mi
		m.mu.Unlock()
		if old != nil && old.Index != nil {
			_ = old.Index.Close()
		}
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

func (m *MultiIndexManager) List() []ManagedIndexInfo {
	m.mu.RLock()
	out := make([]ManagedIndexInfo, 0, len(m.indexes))
	for id, mi := range m.indexes {
		info := ManagedIndexInfo{ID: id, Generation: mi.Generation, Reloading: mi.Reloading, Latency: mi.Latency}
		if mi.Index != nil {
			info.Stats = mi.Index.Stats()
		}
		out = append(out, info)
	}
	m.mu.RUnlock()
	return out
}

type ManagedIndexInfo struct {
	ID         string       `json:"id"`
	Generation uint64       `json:"generation"`
	Reloading  bool         `json:"reloading"`
	Stats      Stats        `json:"stats"`
	Latency    IndexLatency `json:"latency"`
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

// ReloadWithConfig atomically replaces an index using a new schema. The source
// factory receives the new index so column names are resolved against the
// replacement schema, not the index being retired.
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
	m.setReloading(id, true, "")
	usageBefore := readResourceUsage()
	started := time.Now()
	ix, err := New(cfg)
	if err != nil {
		m.setReloading(id, false, err.Error())
		return BulkStats{}, err
	}
	src, err := factory(ix)
	if err != nil {
		_ = ix.Close()
		m.setReloading(id, false, err.Error())
		return BulkStats{}, err
	}
	if opt.Name == "" {
		opt.Name = id
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 65536
	}
	stats, err := ix.IndexFrom(ctx, src, opt)
	took := time.Since(started)
	lat := latencyFromStats(stats, took, err)
	lat.LastResources = usageDelta(usageBefore, readResourceUsage())
	m.mu.Lock()
	cur := m.indexes[id]
	if err == nil {
		ix.cfg = cfg
		m.indexes[id] = &ManagedIndex{ID: id, Index: ix, Config: cfg, Source: factory, Bulk: opt, Latency: lat, Generation: old.Generation + 1}
	} else if cur != nil {
		cur.Reloading = false
		cur.Latency.LastError = err.Error()
	}
	m.mu.Unlock()
	if err != nil {
		_ = ix.Close()
		return stats, err
	}
	if old.Index != nil {
		_ = old.Index.Close()
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
	Manager  *MultiIndexManager
	APIKeys  []string
	WebDir   string // optional: directory to serve static frontend files
	DataDir  string // persistent index root; defaults to LOOKUPX_DATA_DIR or data/indexes
	Storage  string // "memory" (default) or "disk"; disk persists after reload
	Requests uint64
}

func (s *MultiServer) diskStorageEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(s.Storage), "disk")
}

func (s *MultiServer) writeReloadResult(w http.ResponseWriter, ctx context.Context, id string, stats BulkStats) {
	result := map[string]any{"ok": true, "stats": stats, "storage": "memory"}
	if s.diskStorageEnabled() {
		man, err := s.persistManagedIndex(ctx, id)
		if err != nil {
			http.Error(w, "indexed but persistence failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		result["storage"] = "disk"
		result["manifest"] = man
	}
	writeJSON(w, result)
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

func (s *MultiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Manager == nil {
		http.Error(w, "manager required", http.StatusInternalServerError)
		return
	}
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if s.ServeProductionHTTP(w, r, s.persistentStore()) {
		return
	}
	path := strings.Trim(r.URL.Path, "/")
	// Non-API paths: serve static web files if WebDir is set
	if !strings.HasPrefix(path, "v1/") && path != "health" && path != "metrics" {
		if s.WebDir != "" {
			s.serveWebOrNotFound(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if path == "health" || path == "v1/health" {
		writeJSON(w, map[string]any{"ok": true, "indexes": len(s.Manager.List())})
		return
	}
	if path == "metrics" || path == "v1/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, info := range s.Manager.List() {
			fmt.Fprintf(w, "lookupx_index_live_docs{index=%q} %d\n", info.ID, info.Stats.LiveDocs)
			fmt.Fprintf(w, "lookupx_index_terms{index=%q} %d\n", info.ID, info.Stats.Terms)
			fmt.Fprintf(w, "lookupx_index_last_reload_ns{index=%q} %d\n", info.ID, info.Latency.LastReloadNS)
			fmt.Fprintf(w, "lookupx_index_last_indexed{index=%q} %d\n", info.ID, info.Latency.LastIndexed)
		}
		return
	}
	if path == "v1/indexes" && r.Method == http.MethodGet {
		writeJSON(w, s.Manager.List())
		return
	}
	if path == "v1/indexes" && r.Method == http.MethodPost {
		var req CreateIndexRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		cfg := req.Config
		if cfg.Schema.Fields == nil {
			switch strings.ToLower(req.Schema) {
			case "record", "tuple", "lookup", "":
				cfg.Schema = TupleLookupSchema()
			default:
				http.Error(w, "schema required (use schema: record or provide config.schema.fields)", 400)
				return
			}
		}
		if _, err := s.Manager.Register(IndexDefinition{ID: req.ID, Config: cfg, BulkOptions: req.BulkOptions}); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "id": cleanIndexID(req.ID)})
		return
	}
	if path == "v1/indexes" && r.Method == http.MethodDelete {
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		s.Manager.Remove(cleanIndexID(req.ID))
		writeJSON(w, map[string]any{"ok": true})
		return
	}
	if path == "v1/auto-index" && r.Method == http.MethodPost {
		resp, err := s.autoCreateIndex(r.Context(), r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, resp)
		return
	}
	if path == "v1/infer-columns" && r.Method == http.MethodPost {
		resp, err := s.inferColumns(r.Context(), r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, resp)
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != "indexes" {
		http.NotFound(w, r)
		return
	}
	id := parts[2]
	ix, ok := s.Manager.Get(id)
	if !ok {
		http.Error(w, "index not found", 404)
		return
	}
	action := ""
	if len(parts) > 3 {
		action = parts[3]
	}
	switch {
	case r.Method == http.MethodGet && action == "stats":
		mi, _ := s.Manager.Managed(id)
		writeJSON(w, map[string]any{"id": id, "stats": ix.Stats(), "latency": mi.Latency, "resources": readResourceUsage(), "generation": mi.Generation, "reloading": mi.Reloading})
	case r.Method == http.MethodGet && action == "schema":
		writeJSON(w, map[string]any{"id": id, "fields": schemaFieldsWire(ix.SchemaFields())})
	case r.Method == http.MethodPost && action == "search":
		var q WireQuery
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&q); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		limit := q.Limit
		if limit <= 0 {
			limit = 20
		}
		started := time.Now()
		// Search APIs are record-oriented by default. Clients can opt out with
		// ?with_docs=false for the lowest-allocation ID-only hot path.
		withDocs := true
		if raw := r.URL.Query().Get("with_docs"); raw != "" {
			withDocs, _ = strconv.ParseBool(raw)
		}
		res, hits := ix.SearchInto(SearchRequest{Query: q.ToQuery(), Limit: limit, Offset: q.Offset, WithDocs: withDocs, Sort: q.Sort, Facets: q.Facets}, nil)
		res.Hits = hits
		writeJSON(w, map[string]any{"result": res, "latency_ns": time.Since(started).Nanoseconds()})
	case r.Method == http.MethodGet && action == "lookup":
		profile, _ := strconv.ParseBool(r.URL.Query().Get("profile"))
		var usageBefore ResourceUsage
		if profile {
			usageBefore = readResourceUsage()
		}
		raw := r.URL.RawQuery
		limit := IntParam(r, "limit", 20)
		query, err := ix.CompileDatasourceLookup(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		started := time.Now()
		result, hits := ix.SearchInto(SearchRequest{Query: query, Limit: limit, WithDocs: true}, nil)
		response := map[string]any{"hits": hits, "total": result.Total, "latency_ns": time.Since(started).Nanoseconds()}
		if profile {
			response["resources"] = usageDelta(usageBefore, readResourceUsage())
		}
		writeJSON(w, response)
	case r.Method == http.MethodPost && action == "count":
		var q WireQuery
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&q); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		started := time.Now()
		c := ix.Count(q.ToQuery())
		writeJSON(w, map[string]any{"count": c, "latency_ns": time.Since(started).Nanoseconds()})
	case r.Method == http.MethodPost && action == "reload":
		stats, err := s.Manager.Reload(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.writeReloadResult(w, r.Context(), id, stats)
	case r.Method == http.MethodPost && action == "reload-sql":
		stats, err := s.reloadSQL(r.Context(), id, r)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.writeReloadResult(w, r.Context(), id, stats)
	case r.Method == http.MethodPost && action == "reload-table":
		stats, err := s.reloadTable(r.Context(), id, r)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.writeReloadResult(w, r.Context(), id, stats)
	case r.Method == http.MethodPost && action == "snapshot":
		var body struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Path == "" {
			body.Path = filepath.Join("data", id+".snapshot.json")
		}
		if err := ix.SaveSnapshot(body.Path); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "path": body.Path})
	case len(parts) > 4 && action == "docs" && r.Method == http.MethodPut:
		docID := strings.Join(parts[4:], "/")
		var d Document
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&d); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := ix.Upsert(docID, d); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "id": docID})
	case action == "docs" && len(parts) == 5 && r.Method == http.MethodDelete:
		_ = ix.Delete(parts[4])
		writeJSON(w, map[string]any{"ok": true})
	default:
		s.serveWebOrNotFound(w, r)
	}
}

func (s *MultiServer) serveWebOrNotFound(w http.ResponseWriter, r *http.Request) {
	if s.WebDir == "" {
		http.NotFound(w, r)
		return
	}
	// Force revalidation on every request. Without this, browsers may serve a
	// stale index.html/app.js/styles.css straight from disk cache (no request
	// to the server at all) even on a normal reload, since Go's static file
	// server sends no cache-control hints of its own — only Last-Modified/ETag,
	// which no-cache still lets the browser use for a cheap 304.
	w.Header().Set("Cache-Control", "no-cache")
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" || path == "index.html" {
		http.ServeFile(w, r, filepath.Join(s.WebDir, "index.html"))
		return
	}
	http.FileServer(http.Dir(s.WebDir)).ServeHTTP(w, r)
}

func (s *MultiServer) authorized(r *http.Request) bool {
	if len(s.APIKeys) == 0 {
		return true
	}
	key := r.Header.Get("X-API-Key")
	if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		key = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	for _, k := range s.APIKeys {
		if k == key {
			return true
		}
	}
	return false
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

func (s *MultiServer) reloadSQL(ctx context.Context, id string, r *http.Request) (BulkStats, error) {
	var req SQLReloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return BulkStats{}, err
	}
	if req.IDColumn == "" {
		req.IDColumn = "id"
	}
	mi, ok := s.Manager.Managed(id)
	if !ok {
		return BulkStats{}, errors.New("index not found")
	}
	db, err := squealx.Connect(squealxDriver(req.Driver), req.DSN, "")
	if err != nil {
		return BulkStats{}, err
	}
	defer db.Close()
	inferred, err := InferSQLColumns(ctx, db.DB(), req.Query, req.Args, req.IDColumn, req.SeqColumn, 200)
	if err != nil {
		return BulkStats{}, err
	}
	inferred = applySQLColumnSpecs(inferred, req.Columns)
	cfg := mi.Config
	cfg.Schema = AutoSchema(inferred)
	cfg.DisableSource = false
	opt := bulkFromSQLReq(id, req.BatchSize, req.CheckpointPath, req.Resume)
	return s.Manager.ReloadWithConfig(ctx, id, cfg, func(ix *Index) (Source, error) {
		return SQLQuerySource{DB: db.DB(), Query: req.Query, Args: req.Args, IDColumn: req.IDColumn, SeqColumn: req.SeqColumn, Columns: BindSQLColumns(ix, inferred)}, nil
	}, opt)
}

func (s *MultiServer) reloadTable(ctx context.Context, id string, r *http.Request) (BulkStats, error) {
	var req SQLTableReloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return BulkStats{}, err
	}
	if req.IDColumn == "" {
		req.IDColumn = "id"
	}
	if req.OrderColumn == "" {
		req.OrderColumn = req.IDColumn
	}
	mi, ok := s.Manager.Managed(id)
	if !ok {
		return BulkStats{}, errors.New("index not found")
	}
	db, inferred, err := connectAndSample(ctx, sqlSampleRequest{
		Driver: req.Driver, DSN: req.DSN, Source: "sql_table", Table: req.Table,
		Where: req.Where, IDColumn: req.IDColumn, SeqColumn: req.SeqColumn,
		OrderColumn: req.OrderColumn, SampleSize: 200,
	})
	if err != nil {
		return BulkStats{}, err
	}
	defer db.Close()
	inferred = applySQLColumnSpecs(inferred, req.Columns)
	cfg := mi.Config
	cfg.Schema = AutoSchema(inferred)
	cfg.DisableSource = false
	selectColumns := []string{req.IDColumn}
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

func sqlColumnSpecs(ix *Index, specs []SQLColumnSpec) []SQLColumn {
	cols := make([]SQLColumn, 0, len(specs))
	for _, sp := range specs {
		field := sp.Field
		if field == "" {
			field = sp.Column
		}
		cols = append(cols, SQLColumn{Column: sp.Column, Field: ix.FieldID(field), Kind: parseValueKind(sp.Kind), Normalized: sp.Normalized, Layout: sp.Layout})
	}
	return cols
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

func writeJSON(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }

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
