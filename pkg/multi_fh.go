package pkg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

// registerMultiServerRoutes binds each API operation directly to an fh route.
func registerMultiServerRoutes(app *fh.App, s *MultiServer) {
	app.Use(s.requireManagerFH)
	app.Get("/health", s.healthFH)
	app.Get("/metrics", s.metricsFH)
	api := app.Group("/v1")
	api.Get("/health", s.healthFH)
	api.Get("/metrics", s.metricsFH)
	api.Get("/indexes", s.listIndexesFH)
	api.Post("/indexes", s.createIndexFH)
	api.Delete("/indexes", s.deleteIndexFH)
	api.Post("/auto-index", s.autoIndexFH)
	api.Post("/infer-columns", s.inferColumnsRouteFH)
	api.Get("/tasks", s.listTasksFH)
	api.Get("/tasks/:task", s.getTaskFH)
	api.Post("/tasks/:task/cancel", s.cancelTaskFH)
	api.Get("/plan", s.planFH)

	indexes := api.Group("/indexes")
	indexes.Get("/:id/stats", s.indexStatsFH)
	indexes.Get("/:id/plan", s.indexPlanFH)
	indexes.Get("/:id/schema", s.indexSchemaFH)
	indexes.Post("/:id/search", s.indexSearchFH)
	indexes.Post("/:id/count", s.indexCountFH)
	indexes.Get("/:id/lookup", s.indexLookupFH)
	indexes.Post("/:id/reload", s.indexReloadFH)
	indexes.Post("/:id/reload-async", s.indexReloadAsyncFH)
	indexes.Post("/:id/reload-file", s.reloadFileRouteFH)
	indexes.Post("/:id/reload-sql", s.indexReloadSQLFH)
	indexes.Post("/:id/reload-table", s.indexReloadTableFH)
	indexes.Post("/:id/snapshot", s.indexSnapshotFH)
	indexes.Put("/:id/docs/*docid", s.indexUpsertDocumentFH)
	indexes.Delete("/:id/docs/:docid", s.indexDeleteDocumentFH)
	indexes.Post("/:id/freeze", s.indexFreezeFH)
	indexes.Post("/:id/persist", s.indexPersistFH)
	indexes.Get("/:id/lookup-composite", s.indexCompositeLookupFH)
	indexes.Get("/:id/generations", s.indexGenerationsFH)
	indexes.Get("/:id/validate", s.indexValidateFH)
	indexes.Post("/:id/validate", s.indexValidateFH)
	indexes.Post("/:id/repair", s.indexRepairFH)
	indexes.Post("/:id/compact", s.indexCompactFH)
	indexes.Post("/:id/rebuild", s.indexRebuildFH)
}

// indexRebuildFH triggers a full partitioned rebuild of a disk-storage index
// from its configured datasource, in the background. Disk-storage indexes
// are served from partition catalogs rather than a live in-memory Index, so
// the older Reload/Freeze/Persist endpoints (which operate on mi.Index) do
// not apply to them; this is their equivalent. Progress is reported through
// the same Reloading/Progress fields GET /v1/indexes already exposes.
func (s *MultiServer) indexRebuildFH(c fh.Ctx) error {
	if !s.diskStorageEnabled() {
		return fhError(c, 400, errRebuildRequiresDiskStorage.Error())
	}
	id := strings.Clone(c.Param("id"))
	managed, ok := s.Manager.Managed(id)
	if !ok {
		return fhError(c, 404, "index not found")
	}
	if managed.Reloading {
		return fhError(c, 409, fmt.Sprintf("index %q is already rebuilding", id))
	}
	go func() {
		if _, err := RebuildPartitionedIndex(context.Background(), s.Manager, s.DataDir, id); err != nil {
			LogKV("index_rebuild_failed", "index", id, "error", err)
		}
	}()
	return c.JSON(map[string]any{"ok": true, "index": id, "state": "rebuilding"})
}

// requireMemoryStorageFH rejects operations that act on a managed index's
// in-memory shadow Index. Every registered index gets one (Manager.Register
// always calls New()), but an index built through the partitioned disk-mode
// path (cmd/lookupx's BCL loader, via RebuildPartitionedIndex) never adds a
// single document to it — the real served data lives only in its partition
// catalog on disk. Letting reload/freeze/persist/etc. through for such an
// index would silently act on zero documents while reporting success.
// Storage=="disk" alone isn't a reliable signal: it is also used by plain
// in-memory indexes that persist a generation for restart recovery (see
// TestHTTPReloadPersistsForServerRestart), where mi.Index is the real,
// populated, served index and these operations are exactly correct. The
// precise signal is whether a partition catalog actually exists for this
// specific index; Rebuild is the equivalent action when it does.
func (s *MultiServer) requireMemoryStorageFH(c fh.Ctx, action string) error {
	if !s.diskStorageEnabled() {
		return nil
	}
	id := strings.Clone(c.Param("id"))
	if _, _, ok := s.partitionCatalogStats(id, Config{}); ok {
		return fhError(c, 400, fmt.Sprintf("%s applies only to memory-resident indexes; %q is served from a partition catalog, use rebuild instead", action, id))
	}
	return nil
}

func (s *MultiServer) requireManagerFH(c fh.Ctx) error {
	if s.Manager == nil {
		return fhError(c, 500, "manager required")
	}
	return c.Next()
}

func (s *MultiServer) healthFH(c fh.Ctx) error {
	if s.Manager == nil {
		return fhError(c, 500, "manager required")
	}
	return c.JSON(map[string]any{"ok": true, "indexes": len(s.Manager.List()), "storage": s.Storage})
}

func (s *MultiServer) metricsFH(c fh.Ctx) error {
	if s.Manager == nil {
		return fhError(c, 500, "manager required")
	}
	c.Type("text/plain; version=0.0.4")
	var b strings.Builder
	usage := readResourceUsage()
	fmt.Fprintf(&b, "lookupx_heap_bytes %d\nlookupx_heap_in_use_bytes %d\nlookupx_runtime_system_bytes %d\nlookupx_goroutines %d\n", usage.HeapBytes, usage.HeapInUseBytes, usage.SystemBytes, usage.Goroutines)
	cache := &servingPartitions
	cache.mu.Lock()
	fmt.Fprintf(&b, "lookupx_partition_cache_bytes %d\nlookupx_partition_cache_budget_bytes %d\nlookupx_partition_cache_entries %d\n", cache.bytes, cache.maxBytes, len(cache.entries))
	cache.mu.Unlock()
	fmt.Fprintf(&b, "lookupx_partition_cache_hits_total %d\nlookupx_partition_cache_misses_total %d\n", cache.hits.Load(), cache.misses.Load())
	for _, info := range s.indexListWithCatalogStats() {
		fmt.Fprintf(&b, "lookupx_index_live_docs{index=%q} %d\n", info.ID, info.Stats.LiveDocs)
		fmt.Fprintf(&b, "lookupx_index_terms{index=%q} %d\n", info.ID, info.Stats.Terms)
		fmt.Fprintf(&b, "lookupx_index_last_reload_ns{index=%q} %d\n", info.ID, info.Latency.LastReloadNS)
		fmt.Fprintf(&b, "lookupx_index_last_indexed{index=%q} %d\n", info.ID, info.Latency.LastIndexed)
	}
	// Request-rate/error/duration metrics come from fh's own RED middleware
	// (wired in NewFHApp), which already tracks per-route histograms; this
	// handler only adds lookupx-specific index/cache gauges above.
	if s.metrics != nil {
		b.WriteString(s.metrics.Prometheus())
	}
	return c.SendString(b.String())
}

func (s *MultiServer) listIndexesFH(c fh.Ctx) error { return c.JSON(s.indexListWithCatalogStats()) }

// Disk-backed indexing keeps only an empty schema/router Index in memory. Its
// counts come from the published catalog; reading them must not open partitions.
func (s *MultiServer) indexListWithCatalogStats() []ManagedIndexInfo {
	infos := s.Manager.List()
	if !s.diskStorageEnabled() {
		for i := range infos {
			infos[i].TermsKnown = true
		}
		return infos
	}
	for i := range infos {
		mi, ok := s.Manager.Managed(infos[i].ID)
		if !ok || mi.Index == nil {
			continue
		}
		stats, termsKnown, ok := s.partitionCatalogStats(infos[i].ID, mi.Config)
		if !ok {
			infos[i].TermsKnown = true
			continue
		}
		infos[i].Stats.Docs = stats.Docs
		infos[i].Stats.LiveDocs = stats.LiveDocs
		infos[i].Stats.DeletedDocs = stats.DeletedDocs
		infos[i].Stats.Terms = stats.Terms
		infos[i].TermsKnown = termsKnown
	}
	return infos
}

func (s *MultiServer) partitionCatalogStats(id string, cfg Config) (Stats, bool, bool) {
	path := filepath.Join(s.persistentStore().Root, cleanIndexID(id), "partitions.json")
	dataset, err := loadServingCatalog(path)
	if err != nil || len(dataset.Generations) == 0 {
		return Stats{}, false, false
	}
	stats := Stats{Fields: len(cfg.Schema.Fields), Segments: len(dataset.Generations), Shards: len(dataset.Generations)}
	termsKnown := true
	for _, man := range dataset.Generations {
		if man.Docs > uint64(^uint(0)>>1)-uint64(stats.Docs) {
			return Stats{}, false, false
		}
		stats.Docs += int(man.Docs)
		if man.Summary == nil || !man.Summary.TermsKnown {
			termsKnown = false
		} else {
			stats.Terms += man.Summary.Terms
		}
	}
	stats.LiveDocs = stats.Docs
	return stats, termsKnown, true
}

func (s *MultiServer) createIndexFH(c fh.Ctx) error {
	var req CreateIndexRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, err.Error())
	}
	cfg := req.Config
	if cfg.Schema.Fields == nil {
		switch strings.ToLower(req.Schema) {
		case "record", "tuple", "lookup", "":
			cfg.Schema = TupleLookupSchema()
		default:
			return fhError(c, 400, "schema required (use schema: record or provide config.schema.fields)")
		}
	}
	if _, err := s.Manager.Register(IndexDefinition{ID: req.ID, Config: cfg, BulkOptions: req.BulkOptions}); err != nil {
		return fhError(c, 400, err.Error())
	}
	return c.JSON(map[string]any{"ok": true, "id": cleanIndexID(req.ID)})
}

func (s *MultiServer) deleteIndexFH(c fh.Ctx) error {
	var req struct {
		ID string `json:"id"`
	}
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, err.Error())
	}
	s.Manager.Remove(cleanIndexID(req.ID))
	return c.JSON(map[string]any{"ok": true})
}

func (s *MultiServer) autoIndexFH(c fh.Ctx) error {
	var req AutoIndexRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, err.Error())
	}
	resp, err := s.autoCreateIndexFH(c.Context(), req)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	return c.JSON(resp)
}

func (s *MultiServer) inferColumnsRouteFH(c fh.Ctx) error {
	var req sqlSampleRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, err.Error())
	}
	resp, err := s.inferColumnsFH(c.Context(), req)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	return c.JSON(resp)
}

func (s *MultiServer) listTasksFH(c fh.Ctx) error {
	return c.JSON(map[string]any{"tasks": s.Manager.Tasks().List()})
}
func (s *MultiServer) getTaskFH(c fh.Ctx) error {
	task, ok := s.Manager.Tasks().Get(c.Param("task"))
	if !ok {
		return fhError(c, 404, "task not found")
	}
	return c.JSON(task)
}
func (s *MultiServer) cancelTaskFH(c fh.Ctx) error {
	return c.JSON(map[string]any{"ok": s.Manager.Tasks().Cancel(c.Param("task"))})
}
func (s *MultiServer) planFH(c fh.Ctx) error {
	rows := int64(c.QueryInt("rows"))
	if rows <= 0 {
		rows = 1_000_000_000
	}
	id := c.Query("index")
	if id == "" {
		id = "dataset_a"
	}
	return c.JSON(PlanBillionRowDeployment(id, rows, DefaultBillionRowBudget()))
}

type indexFHHandler func(fh.Ctx, *Index, string) error

func (s *MultiServer) withIndexFH(c fh.Ctx, handler indexFHHandler) error {
	id := strings.Clone(c.Param("id"))
	ix, ok := s.Manager.Get(id)
	if !ok {
		return fhError(c, 404, "index not found")
	}
	return handler(c, ix, id)
}

func (s *MultiServer) indexStatsFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, id string) error {
		mi, _ := s.Manager.Managed(id)
		stats := ix.Stats()
		termsKnown := true
		if s.diskStorageEnabled() {
			if catalogStats, known, ok := s.partitionCatalogStats(id, mi.Config); ok {
				stats, termsKnown = catalogStats, known
			}
		}
		return c.JSON(map[string]any{"id": id, "stats": stats, "terms_known": termsKnown, "latency": mi.Latency, "progress": mi.Progress, "resources": readResourceUsage(), "generation": mi.Generation, "reloading": mi.Reloading})
	})
}
func (s *MultiServer) indexSchemaFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, id string) error {
		return c.JSON(map[string]any{"id": id, "fields": schemaFieldsWire(ix.SchemaFields()), "lookup_rules": ix.cfg.LookupRules})
	})
}
func (s *MultiServer) indexPlanFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		return c.JSON(PlanBillionRowDeployment(id, 1_000_000_000, DefaultBillionRowBudget()))
	})
}
func (s *MultiServer) indexSearchFH(c fh.Ctx) error { return s.withIndexFH(c, s.searchIndexFH) }
func (s *MultiServer) indexCountFH(c fh.Ctx) error  { return s.withIndexFH(c, s.countIndexFH) }
func (s *MultiServer) indexLookupFH(c fh.Ctx) error { return s.withIndexFH(c, s.lookupIndexFH) }
func (s *MultiServer) indexReloadFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "reload"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		stats, err := s.Manager.Reload(c.Context(), id)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return s.writeReloadResultFH(c, c.Context(), id, stats)
	})
}
func (s *MultiServer) indexReloadAsyncFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "reload-async"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		return c.JSON(map[string]any{"task_id": s.Manager.Tasks().StartReload(c.Context(), s.Manager, id)})
	})
}
func (s *MultiServer) reloadFileRouteFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "reload-file"); err != nil {
		return err
	}
	return s.reloadFileFH(c, strings.Clone(c.Param("id")))
}
func (s *MultiServer) indexReloadSQLFH(c fh.Ctx) error {
	id := strings.Clone(c.Param("id"))
	if err := s.requireMemoryStorageFH(c, "reload-sql"); err != nil {
		return err
	}
	var req SQLReloadRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, err.Error())
	}
	stats, err := s.reloadSQLRequest(c.Context(), id, req)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	return s.writeReloadResultFH(c, c.Context(), id, stats)
}
func (s *MultiServer) indexReloadTableFH(c fh.Ctx) error {
	id := strings.Clone(c.Param("id"))
	if err := s.requireMemoryStorageFH(c, "reload-table"); err != nil {
		return err
	}
	var req SQLTableReloadRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, err.Error())
	}
	stats, err := s.reloadTableRequest(c.Context(), id, req)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	return s.writeReloadResultFH(c, c.Context(), id, stats)
}
func (s *MultiServer) indexSnapshotFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "snapshot"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, id string) error {
		var body struct {
			Path string `json:"path"`
		}
		_ = c.BodyParser(&body)
		if body.Path == "" {
			body.Path = filepath.Join("data", id+".snapshot.json")
		}
		if err := ix.SaveSnapshot(body.Path); err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"ok": true, "path": body.Path})
	})
}
func (s *MultiServer) indexUpsertDocumentFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "document upsert"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		id := strings.Clone(c.Param("docid"))
		var doc Document
		if err := c.BodyParser(&doc); err != nil {
			return fhError(c, 400, err.Error())
		}
		if err := ix.Upsert(id, doc); err != nil {
			return fhError(c, 400, err.Error())
		}
		return c.JSON(map[string]any{"ok": true, "id": id})
	})
}
func (s *MultiServer) indexDeleteDocumentFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "document delete"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		_ = ix.Delete(strings.Clone(c.Param("docid")))
		return c.JSON(map[string]any{"ok": true})
	})
}
func (s *MultiServer) indexFreezeFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "freeze"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		if err := ix.Freeze(); err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"ok": true, "frozen": ix.IsFrozen()})
	})
}
func (s *MultiServer) indexPersistFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "persist"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, id string) error {
		manifest, err := s.persistentStore().SaveIndex(c.Context(), id, ix)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(manifest)
	})
}
func (s *MultiServer) indexCompositeLookupFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "composite lookup"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		groupID, _ := strconv.ParseUint(c.Query("group_id"), 10, 32)
		limit := (&Server{Index: ix}).sanitizeLimit(ParseIntDefault(c.Query("limit"), 20))
		_, hits := ix.SearchInto(SearchRequest{Query: TupleCompositeQuery{Term: c.Query("term"), GroupID: uint32(groupID), DateKey: c.Query("date_key")}, Limit: limit}, nil)
		return c.JSON(map[string]any{"hits": hits, "total": len(hits)})
	})
}
func (s *MultiServer) indexGenerationsFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "generations"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		generations, err := s.persistentStore().ListGenerations(c.Context(), id)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"generations": generations})
	})
}
func (s *MultiServer) indexValidateFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "validate"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		report, err := ValidatePersistentIndex(c.Context(), s.persistentStore(), id, Config{})
		if err != nil && len(report.Issues) == 0 {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(report)
	})
}
func (s *MultiServer) indexRepairFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "repair"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		manifest, report, err := RepairPersistentIndex(c.Context(), s.persistentStore(), id, Config{})
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"manifest": manifest, "validation": report})
	})
}
// indexCompactFH both prunes old persisted generations on disk and rebuilds
// this index's in-memory vector ANN graphs, dropping nodes for deleted or
// expired documents. Without the latter, vector deletes never shrink the
// graph (Add has no corresponding Remove), so churn on a long-running vector
// index leaks memory and keeps biasing neighbor search forever.
func (s *MultiServer) indexCompactFH(c fh.Ctx) error {
	if err := s.requireMemoryStorageFH(c, "compact"); err != nil {
		return err
	}
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, id string) error {
		var policy GenerationPolicy
		if len(c.Body()) > 0 {
			if err := c.BodyParser(&policy); err != nil {
				return fhError(c, 400, err.Error())
			}
		}
		if err := ix.Compact(); err != nil {
			return fhError(c, 500, err.Error())
		}
		removed, err := CompactPersistentGenerations(c.Context(), s.persistentStore(), id, policy)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"removed": removed, "vectors_rebuilt": true})
	})
}

func (s *MultiServer) searchIndexFH(c fh.Ctx, ix *Index, id string) error {
	var q WireQuery
	if err := c.BodyParser(&q); err != nil {
		return fhError(c, 400, err.Error())
	}
	limit := (&Server{Index: ix}).sanitizeLimit(q.Limit)
	if q.AfterDocID > 0 && len(q.Sort) > 0 {
		return fhError(c, 400, "after_doc_id pagination cannot be combined with sort")
	}
	if q.AfterDocID > 0 && q.Offset > 0 {
		return fhError(c, 400, "after_doc_id pagination cannot be combined with offset")
	}
	verbose := q.Verbose
	if raw := c.Query("verbose"); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			verbose = verbose || parsed
		}
	}
	withDocs := true
	if raw := c.Query("with_docs"); raw != "" {
		withDocs, _ = strconv.ParseBool(raw)
	}
	started := time.Now()
	searchReq := SearchRequest{Query: q.ToQuery(), Limit: limit, Offset: q.Offset, AfterDocID: q.AfterDocID, SkipTotal: q.SkipTotal || !verbose, WithDocs: withDocs, Sort: q.Sort, Facets: q.Facets}
	res, partitioned, err := s.searchPartitionCatalog(c.Context(), ix, id, searchReq)
	if err != nil {
		return partitionSearchError(c, err)
	}
	var hits []Hit
	if partitioned {
		hits = res.Hits
	} else {
		res, hits = ix.SearchInto(searchReq, nil)
	}
	latency := time.Since(started)
	LogKV("query_search", "index", id, "query", wireQueryShape(q), "limit", limit, "offset", q.Offset, "hits", len(hits), "total", res.Total, "latency", latency)
	response := hitResponse(hits, res.Total, latency, verbose)
	if len(res.Facets) > 0 {
		response["facets"] = res.Facets
	}
	return c.JSON(response)
}
func (s *MultiServer) countIndexFH(c fh.Ctx, ix *Index, id string) error {
	var q WireQuery
	if err := c.BodyParser(&q); err != nil {
		return fhError(c, 400, err.Error())
	}
	started := time.Now()
	partitionResult, partitioned, err := s.searchPartitionCatalog(c.Context(), ix, id, SearchRequest{Query: q.ToQuery(), Limit: 1})
	if err != nil {
		return partitionSearchError(c, err)
	}
	count := 0
	if partitioned {
		count = partitionResult.Total
	} else {
		count = ix.Count(q.ToQuery())
	}
	LogKV("query_count", "index", id, "query", wireQueryShape(q), "count", count, "latency", time.Since(started))
	return c.JSON(map[string]any{"count": count, "latency_ns": time.Since(started).Nanoseconds()})
}
func (s *MultiServer) lookupIndexFH(c fh.Ctx, ix *Index, id string) error {
	return s.lookupFH(c, ix, id)
}

func (s *MultiServer) lookupFH(c fh.Ctx, ix *Index, id string) error {
	verbose, _ := strconv.ParseBool(c.Query("verbose"))
	profile, _ := strconv.ParseBool(c.Query("profile"))
	var before ResourceUsage
	if profile {
		before = readResourceUsage()
	}
	rawURL, err := url.ParseRequestURI(c.OriginalURL())
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	query, err := ix.CompileDatasourceLookup(rawURL.RawQuery)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	limit := ParseIntDefault(c.Query("limit"), 20)
	started := time.Now()
	searchReq := SearchRequest{Query: query, Limit: limit, Offset: ParseIntDefault(c.Query("offset"), 0), WithDocs: true, SkipTotal: !verbose}
	result, partitioned, err := s.searchPartitionCatalog(c.Context(), ix, id, searchReq)
	if err != nil {
		return partitionSearchError(c, err)
	}
	var hits []Hit
	if partitioned {
		hits = result.Hits
	} else {
		result, hits = ix.SearchInto(searchReq, nil)
	}
	latency := time.Since(started)
	fields := make([]string, 0, len(rawURL.Query()))
	for key := range rawURL.Query() {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	LogKV("query_lookup", "index", id, "filters", strings.Join(fields, ","), "limit", limit, "hits", len(hits), "total", result.Total, "latency", time.Since(started))
	response := hitResponse(hits, result.Total, latency, verbose)
	if profile {
		response["resources"] = usageDelta(before, readResourceUsage())
	}
	return c.JSON(response)
}

// searchPartitionCatalog serves a completed low-memory partition build when
// the manager has no complete in-memory generation.
// The cold path bounds decoded partitions to the process-wide search worker cap.
func (s *MultiServer) searchPartitionCatalog(ctx context.Context, ix *Index, id string, req SearchRequest) (Result, bool, error) {
	if s == nil || ix == nil {
		return Result{}, false, nil
	}
	managed, hasManaged := s.Manager.Managed(id)
	if hasManaged {
		stats := ix.Stats()
		if stats.LiveDocs > 0 && (!managed.Reloading || managed.Generation > 1) {
			return Result{}, false, nil
		}
	}
	catalogPath := filepath.Join(s.persistentStore().Root, cleanIndexID(id), "partitions.json")
	dataset, err := loadServingCatalog(catalogPath)
	if errors.Is(err, os.ErrNotExist) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, true, fmt.Errorf("load partition catalog: %w", err)
	}
	if len(dataset.Generations) == 0 {
		return Result{Hits: []Hit{}}, true, nil
	}
	root := strings.TrimSpace(dataset.Root)
	if root == "" {
		root = s.persistentStore().Root
	}
	var store PersistentStore = FileSegmentStore{Root: root}
	if strings.EqualFold(dataset.Generations[0].Format, string(LayoutMMapParts)) {
		store = FileMMapSegmentStore{Root: root}
	}
	result, err := SearchPartitionedPersistentResult(ctx, store, dataset.Generations, ix.cfg, req, nil)
	if errors.Is(err, os.ErrNotExist) && ctx.Err() == nil {
		// A replacement may be published and its old directories cleaned after
		// this request read the previous catalog but before a cold open. Retry
		// once against the new complete catalog; already-mapped leases stay valid.
		if fresh, loadErr := loadServingCatalog(catalogPath); loadErr == nil && fresh.BuildID != dataset.BuildID {
			freshRoot := strings.TrimSpace(fresh.Root)
			if freshRoot == "" {
				freshRoot = s.persistentStore().Root
			}
			var freshStore PersistentStore = FileSegmentStore{Root: freshRoot}
			if len(fresh.Generations) > 0 && strings.EqualFold(fresh.Generations[0].Format, string(LayoutMMapParts)) {
				freshStore = FileMMapSegmentStore{Root: freshRoot}
			}
			result, err = SearchPartitionedPersistentResult(ctx, freshStore, fresh.Generations, ix.cfg, req, nil)
		}
	}
	if err != nil {
		return Result{}, true, err
	}
	return result, true, nil
}

func partitionSearchError(c fh.Ctx, err error) error {
	status := 500
	if errors.Is(err, ErrPartitionedSearchUnsupported) {
		status = 400
	}
	return fhError(c, status, err.Error())
}

// hitResponse keeps the common lookup payload compact: hits are document
// records directly. Verbose mode adds Hit metadata and aggregate diagnostics.
func hitResponse(hits []Hit, total int, latency time.Duration, verbose bool) map[string]any {
	if verbose {
		return map[string]any{"hits": hits, "total": total, "latency_ns": latency.Nanoseconds()}
	}
	records := make([]Document, 0, len(hits))
	for _, hit := range hits {
		records = append(records, hit.Doc)
	}
	return map[string]any{"hits": records}
}

func (s *MultiServer) reloadFileFH(c fh.Ctx, id string) error {
	fields := make(map[string]string)
	var uploadPath, filename string
	err := fh.StreamMultipart(c, fh.MultipartStreamOptions{
		MaxParts: 32, MaxFieldBytes: 1 << 20,
		MaxFileBytes: maxIndexUploadBytes, MaxTotalBytes: maxIndexUploadBytes + (64 << 20),
	}, func(part fh.MultipartStreamPart) error {
		if part.FieldName != "file" || part.FileName == "" {
			value, err := io.ReadAll(io.LimitReader(part.Reader, (1<<20)+1))
			if err != nil {
				return err
			}
			if len(value) > 1<<20 {
				return fmt.Errorf("multipart field exceeds 1 MiB")
			}
			fields[part.FieldName] = string(value)
			return nil
		}
		if uploadPath != "" {
			return fmt.Errorf("only one file is allowed")
		}
		file, err := os.CreateTemp("", "lookupx-upload-*")
		if err != nil {
			return err
		}
		uploadPath = file.Name()
		filename = filepath.Base(part.FileName)
		written, copyErr := io.Copy(file, io.LimitReader(part.Reader, maxIndexUploadBytes+1))
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if written > maxIndexUploadBytes {
			return fmt.Errorf("file exceeds the %d MiB upload limit", maxIndexUploadBytes>>20)
		}
		return nil
	})
	if err != nil {
		if uploadPath != "" {
			_ = os.Remove(uploadPath)
		}
		return fhError(c, 400, fmt.Sprintf("read upload form: %v", err))
	}
	if uploadPath == "" {
		return fhError(c, 400, "file is required")
	}
	defer os.Remove(uploadPath)
	format := strings.ToLower(strings.TrimSpace(fields["format"]))
	if format == "" || format == "auto" {
		format = strings.TrimPrefix(strings.ToLower(filepath.Ext(filename)), ".")
	}
	batchSize, _ := strconv.Atoi(strings.TrimSpace(fields["batch_size"]))
	stats, err := s.reloadFilePath(c.Context(), id, uploadPath, format, strings.TrimSpace(fields["id_field"]), strings.TrimSpace(fields["sheet"]), batchSize)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	return s.writeReloadResultFH(c, c.Context(), id, stats)
}

func (s *MultiServer) writeReloadResultFH(c fh.Ctx, ctx context.Context, id string, stats BulkStats) error {
	result := map[string]any{"ok": true, "stats": stats, "storage": "memory"}
	if s.diskStorageEnabled() {
		manifest, err := s.persistManagedIndex(ctx, id)
		if err != nil {
			return fhError(c, 500, "indexed but persistence failed: "+err.Error())
		}
		result["storage"] = "disk"
		result["manifest"] = manifest
	}
	return c.JSON(result)
}

func (s *MultiServer) inferColumnsFH(ctx context.Context, req sqlSampleRequest) (InferColumnsResponse, error) {
	req.DataDir = s.DataDir
	db, cols, err := connectAndSample(ctx, req)
	if err != nil {
		return InferColumnsResponse{}, err
	}
	_ = db.Close()
	return InferColumnsResponse{Columns: autoColumnsWire(cols)}, nil
}

func (s *MultiServer) autoCreateIndexFH(ctx context.Context, req AutoIndexRequest) (AutoIndexResponse, error) {
	id := cleanIndexID(req.ID)
	if id == "" {
		return AutoIndexResponse{}, fmt.Errorf("id required")
	}
	req.sqlSampleRequest.DataDir = s.DataDir
	db, cols, err := connectAndSample(ctx, req.sqlSampleRequest)
	if err != nil {
		return AutoIndexResponse{}, err
	}
	defer db.Close()
	ix, err := New(Config{Schema: AutoSchema(cols), InitialCapacity: 1024})
	if err != nil {
		return AutoIndexResponse{}, err
	}
	if err := s.Manager.AddIndex(id, ix); err != nil {
		_ = ix.Close()
		return AutoIndexResponse{}, err
	}
	return AutoIndexResponse{ID: id, Fields: schemaFieldsWire(ix.SchemaFields()), Columns: autoColumnsWire(cols)}, nil
}
