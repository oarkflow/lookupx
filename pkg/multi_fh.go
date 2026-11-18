package pkg

import (
	"context"
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
	return c.JSON(map[string]any{"ok": true, "indexes": len(s.Manager.List())})
}

func (s *MultiServer) metricsFH(c fh.Ctx) error {
	if s.Manager == nil {
		return fhError(c, 500, "manager required")
	}
	c.Type("text/plain; version=0.0.4")
	var b strings.Builder
	for _, info := range s.Manager.List() {
		fmt.Fprintf(&b, "lookupx_index_live_docs{index=%q} %d\n", info.ID, info.Stats.LiveDocs)
		fmt.Fprintf(&b, "lookupx_index_terms{index=%q} %d\n", info.ID, info.Stats.Terms)
		fmt.Fprintf(&b, "lookupx_index_last_reload_ns{index=%q} %d\n", info.ID, info.Latency.LastReloadNS)
		fmt.Fprintf(&b, "lookupx_index_last_indexed{index=%q} %d\n", info.ID, info.Latency.LastIndexed)
	}
	return c.SendString(b.String())
}

func (s *MultiServer) listIndexesFH(c fh.Ctx) error { return c.JSON(s.Manager.List()) }

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
		return c.JSON(map[string]any{"id": id, "stats": ix.Stats(), "latency": mi.Latency, "resources": readResourceUsage(), "generation": mi.Generation, "reloading": mi.Reloading})
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
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		stats, err := s.Manager.Reload(c.Context(), id)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return s.writeReloadResultFH(c, c.Context(), id, stats)
	})
}
func (s *MultiServer) indexReloadAsyncFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		return c.JSON(map[string]any{"task_id": s.Manager.Tasks().StartReload(c.Context(), s.Manager, id)})
	})
}
func (s *MultiServer) reloadFileRouteFH(c fh.Ctx) error {
	return s.reloadFileFH(c, strings.Clone(c.Param("id")))
}
func (s *MultiServer) indexReloadSQLFH(c fh.Ctx) error {
	id := strings.Clone(c.Param("id"))
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
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		_ = ix.Delete(strings.Clone(c.Param("docid")))
		return c.JSON(map[string]any{"ok": true})
	})
}
func (s *MultiServer) indexFreezeFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		if err := ix.Freeze(); err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"ok": true, "frozen": ix.IsFrozen()})
	})
}
func (s *MultiServer) indexPersistFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, id string) error {
		manifest, err := s.persistentStore().SaveIndex(c.Context(), id, ix)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(manifest)
	})
}
func (s *MultiServer) indexCompositeLookupFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, ix *Index, _ string) error {
		groupID, _ := strconv.ParseUint(c.Query("group_id"), 10, 32)
		limit := ParseIntDefault(c.Query("limit"), 20)
		_, hits := ix.SearchInto(SearchRequest{Query: TupleCompositeQuery{Term: c.Query("term"), GroupID: uint32(groupID), DateKey: c.Query("date_key")}, Limit: limit}, nil)
		return c.JSON(map[string]any{"hits": hits, "total": len(hits)})
	})
}
func (s *MultiServer) indexGenerationsFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		generations, err := s.persistentStore().ListGenerations(c.Context(), id)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"generations": generations})
	})
}
func (s *MultiServer) indexValidateFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		report, err := ValidatePersistentIndex(c.Context(), s.persistentStore(), id, Config{})
		if err != nil && len(report.Issues) == 0 {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(report)
	})
}
func (s *MultiServer) indexRepairFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		manifest, report, err := RepairPersistentIndex(c.Context(), s.persistentStore(), id, Config{})
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"manifest": manifest, "validation": report})
	})
}
func (s *MultiServer) indexCompactFH(c fh.Ctx) error {
	return s.withIndexFH(c, func(c fh.Ctx, _ *Index, id string) error {
		var policy GenerationPolicy
		if len(c.Body()) > 0 {
			if err := c.BodyParser(&policy); err != nil {
				return fhError(c, 400, err.Error())
			}
		}
		removed, err := CompactPersistentGenerations(c.Context(), s.persistentStore(), id, policy)
		if err != nil {
			return fhError(c, 500, err.Error())
		}
		return c.JSON(map[string]any{"removed": removed})
	})
}

func (s *MultiServer) searchIndexFH(c fh.Ctx, ix *Index, id string) error {
	var q WireQuery
	if err := c.BodyParser(&q); err != nil {
		return fhError(c, 400, err.Error())
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 20
	}
	if q.AfterDocID > 0 && len(q.Sort) > 0 {
		return fhError(c, 400, "after_doc_id pagination cannot be combined with sort")
	}
	if q.AfterDocID > 0 && q.Offset > 0 {
		return fhError(c, 400, "after_doc_id pagination cannot be combined with offset")
	}
	withDocs := true
	if raw := c.Query("with_docs"); raw != "" {
		withDocs, _ = strconv.ParseBool(raw)
	}
	started := time.Now()
	res, hits := ix.SearchInto(SearchRequest{Query: q.ToQuery(), Limit: limit, Offset: q.Offset, AfterDocID: q.AfterDocID, SkipTotal: q.SkipTotal, WithDocs: withDocs, Sort: q.Sort, Facets: q.Facets}, nil)
	res.Hits = hits
	LogKV("query_search", "index", id, "query", wireQueryShape(q), "limit", limit, "offset", q.Offset, "hits", len(hits), "total", res.Total, "latency", time.Since(started))
	return c.JSON(map[string]any{"result": res, "latency_ns": time.Since(started).Nanoseconds()})
}
func (s *MultiServer) countIndexFH(c fh.Ctx, ix *Index, id string) error {
	var q WireQuery
	if err := c.BodyParser(&q); err != nil {
		return fhError(c, 400, err.Error())
	}
	started := time.Now()
	count := ix.Count(q.ToQuery())
	LogKV("query_count", "index", id, "query", wireQueryShape(q), "count", count, "latency", time.Since(started))
	return c.JSON(map[string]any{"count": count, "latency_ns": time.Since(started).Nanoseconds()})
}
func (s *MultiServer) lookupIndexFH(c fh.Ctx, ix *Index, id string) error {
	return s.lookupFH(c, ix, id)
}

func (s *MultiServer) lookupFH(c fh.Ctx, ix *Index, id string) error {
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
	result, hits := ix.SearchInto(SearchRequest{Query: query, Limit: limit, WithDocs: true}, nil)
	fields := make([]string, 0, len(rawURL.Query()))
	for key := range rawURL.Query() {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	LogKV("query_lookup", "index", id, "filters", strings.Join(fields, ","), "limit", limit, "hits", len(hits), "total", result.Total, "latency", time.Since(started))
	response := map[string]any{"hits": hits, "total": result.Total, "latency_ns": time.Since(started).Nanoseconds()}
	if profile {
		response["resources"] = usageDelta(before, readResourceUsage())
	}
	return c.JSON(response)
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
