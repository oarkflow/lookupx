package lookup

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Server struct {
	Index       *Index
	Requests    uint64
	SlowQueries uint64
	lastSecond  int64
	secondCount int64
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
		if k == key {
			return true
		}
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
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
	if !s.authorized(w, r) {
		return
	}
	started := time.Now()
	defer func() {
		if s.Index != nil && s.Index.cfg.SlowQueryNanos > 0 && time.Since(started).Nanoseconds() > s.Index.cfg.SlowQueryNanos {
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
		fmt.Fprintf(w, "lookupx_requests_total %d\nlookupx_slow_queries_total %d\nlookupx_live_docs %d\n", atomic.LoadUint64(&s.Requests), atomic.LoadUint64(&s.SlowQueries), s.Index.Stats().LiveDocs)
	case r.Method == "GET" && r.URL.Path == "/health/index":
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "stats": s.Index.Stats()})
	case r.Method == "POST" && r.URL.Path == "/analyze":
		var body struct{ Field, Text string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"tokens": s.Index.Analyze(body.Field, body.Text)})
	case r.Method == "PUT" && strings.HasPrefix(r.URL.Path, "/docs/"):
		id := strings.TrimPrefix(r.URL.Path, "/docs/")
		var d Document
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&d); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := s.Index.Upsert(id, d); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": id})
	case r.Method == "POST" && r.URL.Path == "/docs/batch":
		var body map[string]Document
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := s.Index.BatchUpsert(body); err != nil {
			http.Error(w, err.Error(), 400)
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
		res := s.Index.Search(SearchRequest{Query: q.ToQuery(), Limit: limit, Offset: q.Offset, WithDocs: q.WithDocs, Sort: q.Sort, Facets: q.Facets})
		json.NewEncoder(w).Encode(res)
	case r.Method == "POST" && r.URL.Path == "/count":
		var q WireQuery
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&q); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"count": s.Index.Count(q.ToQuery())})
	case r.Method == "POST" && r.URL.Path == "/truncate-wal":
		if err := s.Index.TruncateWAL(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	case r.Method == "POST" && r.URL.Path == "/snapshot":
		var body struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Path == "" {
			body.Path = s.Index.cfg.SnapshotPath
		}
		if err := s.Index.SaveSnapshot(body.Path); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "path": body.Path})
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
		return VectorQuery{Field: w.Field, Vector: w.Vector, K: w.K, Metric: w.Metric}
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
