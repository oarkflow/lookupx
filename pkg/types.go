package pkg

import "time"

type DocID uint64

type FieldKind uint8

const (
	FieldKeyword FieldKind = iota
	FieldText
	FieldInt
	FieldFloat
	FieldBool
	FieldTime
	FieldVector
)

type FieldOptions struct {
	Kind      FieldKind `json:"kind"`
	Indexed   bool      `json:"indexed"`
	Stored    bool      `json:"stored"`
	Sortable  bool      `json:"sortable"`
	Facetable bool      `json:"facetable"`
	Lookup    bool      `json:"lookup"`
	Prefix    bool      `json:"prefix"`
	Suffix    bool      `json:"suffix"`
	Ngram     bool      `json:"ngram"`
	Fuzzy     bool      `json:"fuzzy"`
	Phrase    bool      `json:"phrase"`
	Lowercase bool      `json:"lowercase"`
	Unique    bool      `json:"unique"`
	MinGram   int       `json:"min_gram"`
	MaxGram   int       `json:"max_gram"`
	// MinPrefix/MaxPrefix bound prefix-index expansion. For large keyword
	// fields, use MinPrefix=3 to avoid huge low-selectivity prefixes like
	// "9" and "99". MaxPrefix<=0 means full length.
	MinPrefix int       `json:"min_prefix,omitempty"`
	MaxPrefix int       `json:"max_prefix,omitempty"`
	TTLField  bool      `json:"ttl_field"`
	Analyzer  string    `json:"analyzer,omitempty"`
	Boost     float64   `json:"boost,omitempty"`
	Dim       int       `json:"dim,omitempty"`
}

type Schema struct {
	Fields map[string]FieldOptions `json:"fields"`
}

type Document map[string]any

type Clock interface {
	Now() time.Time
	Unix() int64
	UnixNano() int64
}

type SystemClock struct{}

func (SystemClock) Now() time.Time  { return time.Now() }
func (SystemClock) Unix() int64     { return time.Now().Unix() }
func (SystemClock) UnixNano() int64 { return time.Now().UnixNano() }

type StaticClock struct{ T time.Time }

func (c StaticClock) Now() time.Time {
	if c.T.IsZero() {
		return time.Unix(0, 0)
	}
	return c.T
}
func (c StaticClock) Unix() int64     { return c.Now().Unix() }
func (c StaticClock) UnixNano() int64 { return c.Now().UnixNano() }

type Config struct {
	Schema         Schema        `json:"schema"`
	WALPath        string        `json:"wal_path"`
	SnapshotPath   string        `json:"snapshot_path"`
	AutoFlushEvery time.Duration `json:"auto_flush_every"`
	EnableWAL      bool          `json:"enable_wal"`
	APIKeys        []string      `json:"api_keys,omitempty"`
	TenantField    string        `json:"tenant_field,omitempty"`
	AuditPath      string        `json:"audit_path,omitempty"`
	SlowQueryNanos int64         `json:"slow_query_nanos,omitempty"`
	RateLimitQPS   int           `json:"rate_limit_qps,omitempty"`
	// DisableSource skips storing/cloning original documents. It is the fastest/lowest-memory mode
	// for pure lookup/search indexes where callers only need IDs and indexed columns.
	DisableSource bool `json:"disable_source,omitempty"`
	// AppendOnly skips the external-id-to-doc-id map and duplicate/update checks.
	// Use it for immutable bulk builds where each source row ID is unique. Search
	// results still include IDs through the doc-id-to-external-id column, but Get,
	// Delete, and repeated Upsert-by-ID are not available in this mode.
	AppendOnly      bool  `json:"append_only,omitempty"`
	InitialCapacity int   `json:"initial_capacity,omitempty"`
	Clock           Clock `json:"-"`
	CollectTook     bool  `json:"collect_took,omitempty"`
}

type Stats struct {
	Docs        int   `json:"docs"`
	LiveDocs    int   `json:"live_docs"`
	DeletedDocs int   `json:"deleted_docs"`
	Fields      int   `json:"fields"`
	Terms       int   `json:"terms"`
	Prefixes    int   `json:"prefixes"`
	Suffixes    int   `json:"suffixes"`
	Ngrams      int   `json:"ngrams"`
	NumericCols int   `json:"numeric_cols"`
	StringCols  int   `json:"string_cols"`
	WALBytes    int64 `json:"wal_bytes"`
	Snapshots   int   `json:"snapshots"`
	Vectors     int   `json:"vectors"`
	Segments    int   `json:"segments"`
	Shards      int   `json:"shards"`
}

type Hit struct {
	ID          string   `json:"id"`
	DocID       DocID    `json:"doc_id"`
	Score       float64  `json:"score"`
	Doc         Document `json:"doc,omitempty"`
	Fields      []string `json:"fields,omitempty"`
	Explanation string   `json:"explanation,omitempty"`
}

type FacetBucket struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type SortField struct {
	Field   string `json:"field"`
	Desc    bool   `json:"desc,omitempty"`
	Missing string `json:"missing,omitempty"` // first or last
}

type Result struct {
	Total   int                      `json:"total"`
	Hits    []Hit                    `json:"hits"`
	Facets  map[string][]FacetBucket `json:"facets,omitempty"`
	Took    int64                    `json:"took_ns"`
	TraceID string                   `json:"trace_id,omitempty"`
	Profile []ProfileEvent           `json:"profile,omitempty"`
}

type AnalyzeToken struct {
	Term     string `json:"term"`
	Position int    `json:"position"`
}

type ProfileEvent struct {
	Clause string `json:"clause"`
	TookNS int64  `json:"took_ns"`
	Hits   int    `json:"hits"`
}

type Highlight struct {
	Field     string   `json:"field"`
	Fragments []string `json:"fragments"`
}
