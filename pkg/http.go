package pkg

import (
	"crypto/subtle"
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
	AfterDocID     DocID       `json:"after_doc_id,omitempty"`
	SkipTotal      bool        `json:"skip_total,omitempty"`
	WithDocs       bool        `json:"with_docs,omitempty"`
	Verbose        bool        `json:"verbose,omitempty"`
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
func constantTimeStringEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
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
