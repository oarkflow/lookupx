package pkg

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Server struct {
	Index          *Index
	Requests       uint64
	SlowQueries    uint64
	Errors         uint64
	LatencyBuckets [6]uint64
	lastSecond     int64
	secondCount    int64
}

func (s *Server) authorized(w http.ResponseWriter, r *http.Request) bool {
	atomic.AddUint64(&s.Requests, 1)
	if s.Index != nil && s.Index.cfg.RateLimitQPS > 0 {
		now := time.Now().Unix()
		if atomic.LoadInt64(&s.lastSecond) != now {
			atomic.StoreInt64(&s.lastSecond, now)
			atomic.StoreInt64(&s.secondCount, 0)
		}
		if atomic.AddInt64(&s.secondCount, 1) > int64(s.Index.cfg.RateLimitQPS) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return false
		}
	}
	if s.Index == nil || len(s.Index.cfg.APIKeys) == 0 {
		return true
	}
	key := r.Header.Get("X-API-Key")
	if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		key = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	for _, k := range s.Index.cfg.APIKeys {
		if constantTimeStringEqual(k, key) {
			return true
		}
	}
	writeError(w, http.StatusUnauthorized, "unauthorized")
	return false
}

func (s *Server) audit(r *http.Request, status string) {
	if s.Index == nil || s.Index.cfg.AuditPath == "" {
		return
	}
	_ = appendAudit(s.Index.cfg.AuditPath, map[string]any{"ts": time.Now().Format(time.RFC3339Nano), "method": r.Method, "path": r.URL.Path, "status": status, "trace_id": r.Header.Get("X-Trace-ID")})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.Index == nil {
		writeError(w, http.StatusServiceUnavailable, "index not configured")
		return
	}
	if !s.authorized(w, r) {
		return
	}
	started := time.Now()
	r.Body = http.MaxBytesReader(w, r.Body, s.maxRequestBytes())
	defer func() {
		dur := time.Since(started)
		s.observeLatency(dur)
		if s.Index != nil && s.Index.cfg.SlowQueryNanos > 0 && dur.Nanoseconds() > s.Index.cfg.SlowQueryNanos {
			atomic.AddUint64(&s.SlowQueries, 1)
		}
		s.audit(r, "ok")
	}()
	switch {
	case r.Method == "GET" && r.URL.Path == "/health":
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	case r.Method == "GET" && r.URL.Path == "/stats":
		json.NewEncoder(w).Encode(s.Index.Stats())
	case r.Method == "GET" && r.URL.Path == "/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		stats := s.Index.Stats()
		fmt.Fprintf(w, "lookupx_requests_total %d\nlookupx_errors_total %d\nlookupx_slow_queries_total %d\nlookupx_live_docs %d\nlookupx_vector_nodes %d\nlookupx_vector_tombstones %d\n", atomic.LoadUint64(&s.Requests), atomic.LoadUint64(&s.Errors), atomic.LoadUint64(&s.SlowQueries), stats.LiveDocs, stats.VectorNodes, stats.VectorTombstones)
		for i, le := range []string{"1000000", "5000000", "10000000", "50000000", "100000000", "+Inf"} {
			fmt.Fprintf(w, "lookupx_request_duration_ns_bucket{le=\"%s\"} %d\n", le, atomic.LoadUint64(&s.LatencyBuckets[i]))
		}
	case r.Method == "GET" && r.URL.Path == "/health/index":
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "stats": s.Index.Stats()})
	case r.Method == "POST" && r.URL.Path == "/analyze":
		var body struct{ Field, Text string }
		if err := decodeJSON(r.Body, &body); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"tokens": s.Index.Analyze(body.Field, body.Text)})
	case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/docs/"):
		id := strings.TrimPrefix(r.URL.Path, "/docs/")
		var d Document
		if err := decodeJSON(r.Body, &d); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.Index.Upsert(id, d); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": id})
	case r.Method == "POST" && r.URL.Path == "/docs/batch":
		var body map[string]Document
		if err := decodeJSON(r.Body, &body); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.Index.BatchUpsert(body); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "count": len(body)})
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/docs/"):
		id := strings.TrimPrefix(r.URL.Path, "/docs/")
		if d, ok := s.Index.Get(id); ok {
			json.NewEncoder(w).Encode(d)
		} else {
			http.NotFound(w, r)
		}
	case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/docs/"):
		id := strings.TrimPrefix(r.URL.Path, "/docs/")
		_ = s.Index.Delete(id)
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	case r.Method == "POST" && r.URL.Path == "/search":
		var q WireQuery
		if err := decodeJSON(r.Body, &q); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		limit := s.sanitizeLimit(q.Limit)
		res := s.Index.Search(SearchRequest{Query: q.ToQuery(), Limit: limit, Offset: q.Offset, WithDocs: q.WithDocs, Sort: q.Sort, Facets: q.Facets})
		json.NewEncoder(w).Encode(res)
	case r.Method == "POST" && r.URL.Path == "/count":
		var q WireQuery
		if err := decodeJSON(r.Body, &q); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"count": s.Index.Count(q.ToQuery())})
	case r.Method == "POST" && r.URL.Path == "/truncate-wal":
		if err := s.Index.TruncateWAL(); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	case r.Method == "POST" && r.URL.Path == "/snapshot":
		var body struct {
			Path string `json:"path"`
		}
		_ = decodeJSON(r.Body, &body)
		if body.Path == "" {
			body.Path = s.Index.cfg.SnapshotPath
		}
		if err := s.Index.SaveSnapshot(body.Path); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "path": body.Path})
	case r.Method == "POST" && r.URL.Path == "/compact":
		if err := s.Index.Compact(); err != nil {
			atomic.AddUint64(&s.Errors, 1)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "stats": s.Index.Stats()})
	default:
		http.NotFound(w, r)
	}
}

type WireQuery struct {
	Type           string      `json:"type"`
	Field          string      `json:"field,omitempty"`
	Value          string      `json:"value,omitempty"`
	Values         []string    `json:"values,omitempty"`
	GTE            any         `json:"gte,omitempty"`
	GT             any         `json:"gt,omitempty"`
	LTE            any         `json:"lte,omitempty"`
	LT             any         `json:"lt,omitempty"`
	Distance       int         `json:"distance,omitempty"`
	LimitTerms     int         `json:"limit_terms,omitempty"`
	Slop           int         `json:"slop,omitempty"`
	Ordered        bool        `json:"ordered,omitempty"`
	Metric         string      `json:"metric,omitempty"`
	Vector         []float64   `json:"vector,omitempty"`
	K              int         `json:"k,omitempty"`
	EFSearch       int         `json:"ef_search,omitempty"`
	Oversample     int         `json:"oversample,omitempty"`
	Exact          bool        `json:"exact,omitempty"`
	Fields         []string    `json:"fields,omitempty"`
	Must           []WireQuery `json:"must,omitempty"`
	Should         []WireQuery `json:"should,omitempty"`
	Filter         []WireQuery `json:"filter,omitempty"`
	MustNot        []WireQuery `json:"must_not,omitempty"`
	MinShouldMatch int         `json:"min_should_match,omitempty"`
	Limit          int         `json:"limit,omitempty"`
	Offset         int         `json:"offset,omitempty"`
	WithDocs       bool        `json:"with_docs,omitempty"`
	Sort           []SortField `json:"sort,omitempty"`
	Facets         []string    `json:"facets,omitempty"`
}

func (w WireQuery) ToQuery() Query {
	switch strings.ToLower(w.Type) {
	case "term":
		return Term{w.Field, w.Value}
	case "terms", "in":
		return Terms{Field: w.Field, Values: w.Values}
	case "prefix":
		return Prefix{w.Field, w.Value}
	case "suffix":
		return Suffix{w.Field, w.Value}
	case "contains":
		return Contains{w.Field, w.Value}
	case "fuzzy":
		return Fuzzy{w.Field, w.Value, w.Distance, w.LimitTerms}
	case "exists":
		return Exists{w.Field}
	case "missing":
		return Missing{w.Field}
	case "range":
		return Range{Field: w.Field, GTE: w.GTE, GT: w.GT, LTE: w.LTE, LT: w.LT}
	case "phrase":
		return Phrase{Field: w.Field, Value: w.Value, Slop: w.Slop}
	case "proximity":
		return Proximity{Field: w.Field, Terms: w.Values, Slop: w.Slop, Ordered: w.Ordered}
	case "cidr":
		return CIDR{Field: w.Field, Value: w.Value}
	case "domain_wildcard":
		return DomainWildcard{Field: w.Field, Pattern: w.Value}
	case "compound":
		return Compound{Fields: w.Fields, Values: w.Values}
	case "vector":
		return VectorQuery{Field: w.Field, Vector: w.Vector, K: w.K, Metric: w.Metric, Filter: w.vectorFilter(), EFSearch: w.EFSearch, Oversample: w.Oversample, Exact: w.Exact}
	case "and":
		return And(toQueries(w.Must))
	case "or":
		return Or(toQueries(w.Should))
	case "bool":
		return Bool{Must: toQueries(w.Must), Should: toQueries(w.Should), Filter: toQueries(w.Filter), MustNot: toQueries(w.MustNot), MinShouldMatch: w.MinShouldMatch}
	case "simple":
		return Simple(w.Field, w.Value)
	default:
		return MatchAll{}
	}
}
func (w WireQuery) vectorFilter() Query {
	if len(w.Filter) == 0 {
		return nil
	}
	return Bool{Filter: toQueries(w.Filter)}
}

func toQueries(ws []WireQuery) []Query {
	qs := make([]Query, 0, len(ws))
	for _, w := range ws {
		qs = append(qs, w.ToQuery())
	}
	return qs
}
func IntParam(r *http.Request, name string, def int) int {
	if v := r.URL.Query().Get(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func constantTimeStringEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func decodeJSON(r io.Reader, dst any) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("request body must contain a single JSON value")
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": message, "status": status})
}

func (s *Server) maxRequestBytes() int64 {
	if s != nil && s.Index != nil && s.Index.cfg.MaxRequestBytes > 0 {
		return s.Index.cfg.MaxRequestBytes
	}
	return 32 << 20
}

func (s *Server) sanitizeLimit(limit int) int {
	if limit <= 0 {
		limit = 20
	}
	max := 1000
	if s != nil && s.Index != nil && s.Index.cfg.MaxSearchLimit > 0 {
		max = s.Index.cfg.MaxSearchLimit
	}
	if limit > max {
		limit = max
	}
	return limit
}

func (s *Server) observeLatency(d time.Duration) {
	ns := d.Nanoseconds()
	idx := 5
	switch {
	case ns <= int64(time.Millisecond):
		idx = 0
	case ns <= int64(5*time.Millisecond):
		idx = 1
	case ns <= int64(10*time.Millisecond):
		idx = 2
	case ns <= int64(50*time.Millisecond):
		idx = 3
	case ns <= int64(100*time.Millisecond):
		idx = 4
	}
	atomic.AddUint64(&s.LatencyBuckets[idx], 1)
}
