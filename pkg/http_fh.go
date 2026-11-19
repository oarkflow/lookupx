package pkg

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

// registerServerRoutes binds each single-index operation to its own fh route.
func registerServerRoutes(app *fh.App, s *Server) {
	app.Get("/health", s.healthFH)
	app.Get("/stats", s.statsFH)
	app.Get("/metrics", s.metricsFH)
	app.Get("/health/index", s.indexHealthFH)
	app.Post("/analyze", s.analyzeFH)
	app.Put("/docs/*id", s.upsertFH)
	app.Post("/docs/batch", s.batchUpsertFH)
	app.Get("/docs/*id", s.getDocumentFH)
	app.Delete("/docs/*id", s.deleteDocumentFH)
	app.Post("/search", s.searchFH)
	app.Post("/count", s.countFH)
	app.Post("/truncate-wal", s.truncateWALFH)
	app.Post("/snapshot", s.snapshotFH)
	app.Post("/compact", s.compactFH)
}

func (s *Server) requestGateFH(c fh.Ctx) error {
	atomic.AddUint64(&s.Requests, 1)
	started := time.Now()
	defer func() {
		dur := time.Since(started)
		s.observeLatency(dur)
		if s.Index != nil && s.Index.cfg.SlowQueryNanos > 0 && dur.Nanoseconds() > s.Index.cfg.SlowQueryNanos {
			atomic.AddUint64(&s.SlowQueries, 1)
		}
		s.auditFH(c, "ok")
	}()
	if s.Index != nil && s.Index.cfg.RateLimitQPS > 0 {
		now := time.Now().Unix()
		if atomic.LoadInt64(&s.lastSecond) != now {
			atomic.StoreInt64(&s.lastSecond, now)
			atomic.StoreInt64(&s.secondCount, 0)
		}
		if atomic.AddInt64(&s.secondCount, 1) > int64(s.Index.cfg.RateLimitQPS) {
			return fhError(c, 429, "rate limit exceeded")
		}
	}
	return c.Next()
}

func (s *Server) healthFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	return c.JSON(map[string]any{"ok": true})
}

func (s *Server) statsFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	return c.JSON(s.Index.Stats())
}

func (s *Server) metricsFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	c.Type("text/plain; version=0.0.4")
	stats := s.Index.Stats()
	var b strings.Builder
	fmt.Fprintf(&b, "lookupx_requests_total %d\nlookupx_errors_total %d\nlookupx_slow_queries_total %d\nlookupx_live_docs %d\nlookupx_vector_nodes %d\nlookupx_vector_tombstones %d\n", atomic.LoadUint64(&s.Requests), atomic.LoadUint64(&s.Errors), atomic.LoadUint64(&s.SlowQueries), stats.LiveDocs, stats.VectorNodes, stats.VectorTombstones)
	for i, le := range []string{"1000000", "5000000", "10000000", "50000000", "100000000", "+Inf"} {
		fmt.Fprintf(&b, "lookupx_request_duration_ns_bucket{le=\"%s\"} %d\n", le, atomic.LoadUint64(&s.LatencyBuckets[i]))
	}
	return c.SendString(b.String())
}

func (s *Server) indexHealthFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	return c.JSON(map[string]any{"ok": true, "stats": s.Index.Stats()})
}

func (s *Server) analyzeFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	var body struct{ Field, Text string }
	if err := c.BodyParser(&body); err != nil {
		return s.badRequestFH(c, err)
	}
	return c.JSON(map[string]any{"tokens": s.Index.Analyze(body.Field, body.Text)})
}

func (s *Server) upsertFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	id := strings.Clone(c.Param("id"))
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return s.badRequestFH(c, err)
	}
	if err := s.Index.Upsert(id, doc); err != nil {
		return s.badRequestFH(c, err)
	}
	return c.JSON(map[string]any{"ok": true, "id": id})
}

func (s *Server) batchUpsertFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	var docs map[string]Document
	if err := c.BodyParser(&docs); err != nil {
		return s.badRequestFH(c, err)
	}
	if err := s.Index.BatchUpsert(docs); err != nil {
		return s.badRequestFH(c, err)
	}
	return c.JSON(map[string]any{"ok": true, "count": len(docs)})
}

func (s *Server) getDocumentFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	doc, ok := s.Index.Get(strings.Clone(c.Param("id")))
	if !ok {
		return c.Status(404).SendString("404 page not found\n")
	}
	return c.JSON(doc)
}

func (s *Server) deleteDocumentFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	_ = s.Index.Delete(strings.Clone(c.Param("id")))
	return c.JSON(map[string]any{"ok": true})
}

func (s *Server) searchFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	var q WireQuery
	if err := c.BodyParser(&q); err != nil {
		return s.badRequestFH(c, err)
	}
	if q.AfterDocID > 0 && len(q.Sort) > 0 {
		return fhError(c, 400, "after_doc_id pagination cannot be combined with sort")
	}
	if q.AfterDocID > 0 && q.Offset > 0 {
		return fhError(c, 400, "after_doc_id pagination cannot be combined with offset")
	}
	started := time.Now()
	res, hits := s.Index.SearchInto(SearchRequest{Query: q.ToQuery(), Limit: s.sanitizeLimit(q.Limit), Offset: q.Offset, AfterDocID: q.AfterDocID, SkipTotal: q.SkipTotal, WithDocs: true, Sort: q.Sort, Facets: q.Facets}, nil)
	verbose := q.Verbose
	if raw := c.Query("verbose"); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			verbose = verbose || parsed
		}
	}
	response := hitResponse(hits, res.Total, time.Since(started), verbose)
	if len(res.Facets) > 0 {
		response["facets"] = res.Facets
	}
	return c.JSON(response)
}

func (s *Server) countFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	var q WireQuery
	if err := c.BodyParser(&q); err != nil {
		return s.badRequestFH(c, err)
	}
	return c.JSON(map[string]any{"count": s.Index.Count(q.ToQuery())})
}

func (s *Server) truncateWALFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	if err := s.Index.TruncateWAL(); err != nil {
		atomic.AddUint64(&s.Errors, 1)
		return fhError(c, 500, err.Error())
	}
	return c.JSON(map[string]any{"ok": true})
}

func (s *Server) snapshotFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	var body struct {
		Path string `json:"path"`
	}
	_ = c.BodyParser(&body)
	if body.Path == "" {
		body.Path = s.Index.cfg.SnapshotPath
	}
	if err := s.Index.SaveSnapshot(body.Path); err != nil {
		atomic.AddUint64(&s.Errors, 1)
		return fhError(c, 500, err.Error())
	}
	return c.JSON(map[string]any{"ok": true, "path": body.Path})
}

func (s *Server) compactFH(c fh.Ctx) error {
	if err := s.requireIndexFH(c); err != nil {
		return err
	}
	if err := s.Index.Compact(); err != nil {
		atomic.AddUint64(&s.Errors, 1)
		return fhError(c, 500, err.Error())
	}
	return c.JSON(map[string]any{"ok": true, "stats": s.Index.Stats()})
}

func (s *Server) requireIndexFH(c fh.Ctx) error {
	if s == nil || s.Index == nil {
		return fhError(c, 503, "index not configured")
	}
	return nil
}

func (s *Server) auditFH(c fh.Ctx, status string) {
	if s.Index != nil && s.Index.cfg.AuditPath != "" {
		_ = appendAudit(s.Index.cfg.AuditPath, map[string]any{"ts": time.Now().Format(time.RFC3339Nano), "method": c.Method(), "path": c.Path(), "status": status, "trace_id": c.Get("X-Trace-ID")})
	}
}

func (s *Server) badRequestFH(c fh.Ctx, err error) error {
	atomic.AddUint64(&s.Errors, 1)
	return fhError(c, 400, err.Error())
}

func fhError(c fh.Ctx, status int, message string) error {
	return c.Status(status).JSON(map[string]any{"ok": false, "error": message, "status": status})
}
