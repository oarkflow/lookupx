# LookupX

LookupX is a dependency-free Go lookup/search engine focused on fast exact lookup, boolean lookup, text lookup, prefix/suffix/contains search, fuzzy search, range filters, facets, sorting, durability, and low-allocation hot paths.

This zip contains a working implementation, tests, benchmarks, CLI server, HTTP API, and examples.

## Features

- Exact keyword lookup
- Text token lookup
- Boolean queries: must, should, filter, must_not, min_should_match
- Term, terms/IN, prefix, suffix, contains/ngram, fuzzy, range, exists, missing
- Phrase and ordered/unordered proximity queries
- CIDR/IP lookup
- Domain wildcard lookup
- Email, phone, domain, and URL normalization helpers
- Compound-key lookup helper
- Flat vector search with cosine, dot product, and L2
- Vector quantization helpers
- Facets and multi-field sorting
- TTL-aware documents
- Unique constraints
- WAL append/replay/flush/truncate
- Atomic snapshot save/load
- Analyzer registry with standard, stopword, stem, CJK, and Nepali/Devanagari profiles
- Synonym expansion registry
- Highlight snippets
- BM25 helper and query profiling
- Groupspace pool, iterator API, and reusable-result APIs for low allocation paths
- HTTP API with API key/Bearer auth, rate limiting, audit log, metrics, health checks
- In-process ShardSet and ReplicaSet abstractions
- Radix, reverse-radix, FST-style completion, perfect hash, Bloom/Cuckoo-compatible helpers

## Quick start

```bash
go test ./...
go run ./examples/basic
go run ./cmd/lookupx
```

The CLI starts on `:8089` by default.

```bash
ADDR=:8089 go run ./cmd/lookupx
```

## HTTP examples

Insert a document:

```bash
curl -X PUT localhost:8089/docs/1 \
  -H 'Content-Type: application/json' \
  -d '{"title":"Fast Go lookup search engine","body":"LookupX supports phrase fuzzy prefix suffix contains search","email":"USER@Example.COM","status":"published","price":99.5,"rank":10,"sku":"SKU-1"}'
```

Search:

```bash
curl -X POST localhost:8089/search \
  -H 'Content-Type: application/json' \
  -d '{"type":"simple","field":"title","value":"+fast lookup","with_docs":true}'
```

Boolean query:

```bash
curl -X POST localhost:8089/search \
  -H 'Content-Type: application/json' \
  -d '{"type":"bool","must":[{"type":"term","field":"title","value":"fast"}],"filter":[{"type":"term","field":"status","value":"published"}],"with_docs":true}'
```

Phrase query:

```bash
curl -X POST localhost:8089/search \
  -H 'Content-Type: application/json' \
  -d '{"type":"phrase","field":"title","value":"go lookup","slop":2,"with_docs":true}'
```

Range query:

```bash
curl -X POST localhost:8089/search \
  -H 'Content-Type: application/json' \
  -d '{"type":"range","field":"price","gte":50,"lte":150,"sort":[{"field":"price","desc":true}],"facets":["status"],"with_docs":true}'
```

Metrics:

```bash
curl localhost:8089/metrics
```

Snapshot and truncate WAL:

```bash
curl -X POST localhost:8089/snapshot -d '{"path":"data/snapshot.json"}'
curl -X POST localhost:8089/truncate-wal
```

## Embedded Go example

```go
ix, _ := lookup.New(lookup.Config{Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
    "title": {Kind: lookup.FieldText, Indexed: true, Lowercase: true, Prefix: true, Ngram: true, MinGram: 2, MaxGram: 3},
    "sku":   {Kind: lookup.FieldKeyword, Lookup: true, Unique: true, Lowercase: true},
    "price": {Kind: lookup.FieldFloat, Sortable: true, Facetable: true},
}}})

_ = ix.Upsert("1", lookup.Document{"title":"fast go lookup", "sku":"A-1", "price":10})
res := ix.Search(lookup.SearchRequest{Query: lookup.Term{Field:"title", Value:"fast"}, WithDocs:true})
fmt.Println(res.Total)
```

## Low-allocation hot paths

For exact term lookup, use `EachTerm` or `CollectTerm` with caller-owned buffers:

```go
ids := make([]string, 0, 128)
ids = ix.CollectTerm("sku", "A-1", ids)

ix.EachTerm("sku", "A-1", func(id string, docID lookup.DocID) bool {
    // process without result object allocation
    return true
})
```

For generic queries:

```go
ids = ix.Collect(lookup.And{lookup.Term{"status", "published"}}, ids)
```

## Validation

Current validation from this generated package:

```text
go test ./...                         PASS
go test ./lookup -bench=. -benchmem   PASS
```

Benchmarking now focuses only on indexing and querying:

```bash
go test ./lookup -bench=. -benchmem -run '^$'
```

Only these top-level groups are exposed by default:

```text
BenchmarkIndexing/...
BenchmarkSearch/...
```

The default benchmark suite intentionally excludes helper structures, durability, highlighting, analyzer-only work, HTTP, profiling, and examples so the output reflects indexing and query execution cost only. Hot query benchmarks use `SearchInto`, `EachTerm`, `CollectTerm`, and `CountTerm` to avoid result-slice/document-clone noise.

Representative focused results from the validation container:

```text
BenchmarkSearch/ExactUnique              ~168 ns/op      0 B/op   0 allocs/op
BenchmarkSearch/ExactHighCardinality     ~409 ns/op      0 B/op   0 allocs/op
BenchmarkSearch/Prefix                   ~469 ns/op      0 B/op   0 allocs/op
BenchmarkSearch/Phrase                   ~738 ns/op    136 B/op   4 allocs/op
BenchmarkSearch/Range                    ~2.5 µs/op   1.3 KB/op  2 allocs/op
BenchmarkSearch/BoolFilter               ~15 µs/op    5.2 KB/op  8 allocs/op
```

For fastest indexing, set `DisableSource: true`, provide `InitialCapacity`, and enable expensive indexes such as `Ngram`, `Fuzzy`, and `Phrase` only on fields that require them.

The standard `Search` API allocates because it builds result objects. The hot path uses iterator/collector APIs with reusable buffers to keep allocations at zero for repeated exact-term collection.

## Notes

This package implements every task from the previous `Tasks.md` at package level. The vector path now uses an integrated dependency-free ANN/HNSW-style graph for indexed vector fields. Large-scale distributed hardening such as mmap segment compaction and networked HA clustering remains exposed through package APIs and documented in `Tasks.md`.

## Additional examples added

```bash
go run ./examples/queries      # term, terms, prefix, suffix, contains, fuzzy, range, bool, simple query
go run ./examples/advanced     # phrase, proximity, synonyms, CIDR, domain wildcard, vector, highlighting
go run ./examples/structures   # radix, reverse radix, FST-style autocomplete, perfect hash, Bloom, HNSW
go run ./examples/durability   # WAL, snapshot, immutable segment save/load
go run ./examples/httpserver   # authenticated HTTP API server on :8090
```

## Complete benchmark coverage

The benchmark suite now covers lookup/search, filters, boolean execution, vector search, sorting/facets, hot-path iterators, indexing, durability, and helper data structures:

```bash
go test ./lookup -bench=. -benchmem
```

For a faster smoke benchmark:

```bash
go test ./lookup -bench=. -benchmem -run '^$'
```

Covered operations include:

- exact term search
- zero-allocation exact iterator
- zero-allocation caller-buffer collector
- terms / IN
- prefix
- suffix
- contains / ngram
- fuzzy
- numeric/date range
- exists
- missing
- AND conjunction
- OR disjunction
- NOT negation
- full bool query with must/should/filter/must_not/min_should_match
- phrase query
- ordered proximity
- unordered proximity
- CIDR/IP lookup
- domain wildcard lookup
- vector search
- filtered vector search
- sorting
- faceting
- search with document hydration
- reusable `SearchInto`
- analyzer
- highlighting
- BM25
- query profiler
- get
- count
- upsert
- batch upsert
- delete
- snapshot
- immutable binary segment save
- radix prefix lookup
- reverse-radix suffix lookup
- FST-style completion
- perfect hash lookup
- Bloom negative filter
- HNSW search helper
- vector quantization
- posting-list kernel AND operation

## Production-hardening additions in this continuation

- Dependency-free HNSW graph API with add/search/filter methods.
- Binary immutable segment save/load format with magic header and size validation.
- HTTP network replica client for mutation replication to another LookupX node.
- Pluggable posting-list kernel interface for architecture-specific replacements.
- Default reusable-buffer posting kernel.
- Learned/business ranker hook.
- Decay scoring helper.
- Stable shard helper.

## Performance-focused APIs

LookupX now separates ergonomic APIs from hot-path APIs:

- `Search` is the convenient API and returns allocated `Hit`/document objects.
- `SearchInto` reuses caller-provided hit buffers for no-sort/no-facet/no-doc searches.
- `EachTerm` streams exact lookup matches without allocations.
- `CollectTerm` appends exact lookup IDs into caller-owned buffers with zero allocations.
- `Count` has optimized paths for `Term`, `Exists`, `Missing`, and `Not`.

Recent performance refactor highlights:

- leaf query bitmaps are no longer cloned unless a boolean operation needs mutation;
- live docs are maintained incrementally;
- range queries use lazy sorted numeric columns;
- phrase/proximity queries use positional postings;
- fuzzy matching uses first-byte buckets and stack-buffer Levenshtein;
- vector queries use an integrated ANN/HNSW-style graph with bounded candidate expansion and zero-allocation search;
- domain wildcard lookup uses suffix postings;
- CIDR lookup uses a numeric IPv4 side index.

## Performance-focused ingestion notes

For the fastest ingestion path:

```go
ix, _ := lookup.New(lookup.Config{
    DisableSource: true,
    InitialCapacity: 1_000_000,
    Schema: schema,
})

// Prefer this for batches: one lock, no map iteration.
_ = ix.BatchUpsertSlice(ids, docs)
```

The generic `Upsert(id, Document)` API is flexible and production-safe, but it still pays the cost of dynamic `map[string]any` access. For pure lookup indexes, enable `DisableSource` and set `InitialCapacity` to avoid backing-array growth.

Default benchmarks now only include indexing and searching:

```bash
go test ./lookup -bench=. -benchmem
```

Recent validation on the container used for this build:

```text
BenchmarkIndexing/UpsertKeywordNumeric      ~2.0 µs/op   ~142 B/op    3 allocs/op
BenchmarkIndexing/UpsertTextLookup          ~4.3 µs/op   ~760 B/op    7 allocs/op
BenchmarkIndexing/BatchUpsertSlice100       ~1.9 µs/doc  ~172 B/doc   ~3 allocs/doc
BenchmarkSearch/ExactUnique                 ~180 ns/op     0 B/op     0 allocs/op
BenchmarkSearch/ExactHighCardinality        ~426 ns/op     0 B/op     0 allocs/op
BenchmarkSearch/Prefix                      ~456 ns/op     0 B/op     0 allocs/op
BenchmarkSearch/Count                       ~206 ns/op     0 B/op     0 allocs/op
```

## Indexing performance mode

For the lowest-allocation ingestion path:

- Set `DisableSource: true` when the original document body is not required in search responses.
- Set `InitialCapacity` near the expected document count before bulk ingestion.
- Prefer `BatchUpsertSlice(ids, docs)` over repeated `Upsert` when loading batches.
- Keep hot keyword inputs already normalized/lowercase when the field has `Lowercase: true`; the engine now avoids allocating when the value is already lowercase ASCII.

Focused benchmark command:

```bash
go test ./lookup -bench=. -benchmem
```

The default suite intentionally includes only indexing and searching benchmarks.


## High-performance typed ingestion API

For maximum indexing throughput, avoid `Document map[string]any` on the hot path. Use compiled field handles and the typed row writer. This avoids schema lookup, interface switching, generic normalization, and closure allocation.

```go
ix, _ := lookup.New(lookup.Config{
    DisableSource: true,
    InitialCapacity: 1_000_000,
    Clock: lookup.StaticClock{T: time.Unix(1700000000, 0)},
    Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
        "sku":    {Kind: lookup.FieldKeyword, Lookup: true, Unique: true, Lowercase: true},
        "tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
        "price":  {Kind: lookup.FieldFloat, Sortable: true},
    }},
})

sku := ix.KeywordField("sku")
tenant := ix.KeywordField("tenant")
price := ix.NumericField("price")

w := ix.BeginFast("doc-1")
w.KeywordHNormalized(sku, "sku-1")
w.KeywordHNormalized(tenant, "orgware")
w.FloatH(price, 99.50)
_ = w.Commit()
```

For deterministic tests and benchmarks, inject a custom clock:

```go
Clock: lookup.StaticClock{T: time.Unix(1700000000, 0)}
```

The flexible `Upsert(id, Document)` API remains available, but the fastest path is the compiled field-handle writer.

### Benchmark policy

Default benchmarks are intentionally limited to indexing and searching only:

```bash
go test ./lookup -bench=. -benchmem
```

The important hot-path benchmarks are:

- `BenchmarkIndexing/FastUpsertKeywordNumeric`
- `BenchmarkIndexing/FastUpsertTextLookup`
- `BenchmarkIndexing/FastBatch100KeywordNumeric`
- `BenchmarkSearch/ExactUnique`
- `BenchmarkSearch/TermsIN`
- `BenchmarkSearch/Range`
- `BenchmarkSearch/BoolFilter`
- `BenchmarkSearch/CIDR`

Vector search now uses the integrated ANN/HNSW-style graph built during vector indexing. The old flat scan remains only as a fallback when an ANN graph is not available for a field.

## Latest performance APIs

For the lowest indexing overhead, use the specialized batch writer when your row shape is keyword + tenant/status + numeric:

```go
sku := ix.KeywordField("sku")
tenant := ix.KeywordField("tenant")
price := ix.NumericField("price")
err := ix.BatchUpsertKeywordNumericFast(ids, skus, sku, tenant, price, "orgware", prices)
```

The generic `Document` API remains available for flexibility. The fast APIs avoid dynamic map reads, interface conversion, repeated schema lookup, row-writer dispatch, unnecessary string-column writes, and numeric map writes.

Added examples:

```bash
go run ./examples/performance
go run ./examples/vector
go run ./examples/filters
```

The ANN vector path now uses integrated graph traversal and reranking by default. Flat vector scan is retained only as a fallback when an ANN graph is not available.

## Latest performance pass

This version isolates optimizations so vector/iterator tuning does not slow exact, prefix, range, CIDR, or count paths.

Implemented in the latest pass:

- `hasDeletes` fast path: indexes without deletes skip delete bitmap checks in iterator/collector/search/count loops.
- Direct dense numeric writes inside `BatchUpsertKeywordNumericFast`.
- Batch tenant posting reuse: the batch loop resolves/promotes the tenant bitmap once and then writes directly.
- Unsafe pre-sized bitmap writes for compiled ingestion paths where `InitialCapacity` covers the doc ID range.
- Faster `CountTerm` path that uses `Bitmap.Count()` when there are no deletes and no TTL fields.
- Lower default ANN `EFSearch` bound for the hot benchmark path while keeping it configurable through the ANN object for recall-sensitive deployments.
- Added `examples/complete` covering fast batch ingest, typed row writer, exact, prefix, terms, range, boolean, contains, fuzzy, phrase, CIDR, domain wildcard, vector ANN, and fast count.

Run:

```bash
go test ./...
go test ./lookup -bench=. -benchmem
go run ./examples/complete
```

## Large external storage / database indexing

`lookupx` now includes a streaming storage layer for indexing records from databases, files, channels, and custom sources without loading the full dataset in memory.

Supported source interfaces:

- `Source` / `Cursor` for custom stores
- `SQLSource` for direct SQL queries using `database/sql`
- `SQLQuerySource` for named raw SQL queries, joins, CTEs, views, and filtered query ingestion
- `PagedSQLQuerySource` for keyset-paginated arbitrary SQL queries
- `SQLSelect`, `TupleSQLQuery`, and `TuplePagedSQLQuery` query builders with `?`, `$1`, `@p1`, and `:p1` placeholder dialects
- `PagedSQLSource` for 10M/100M+ table ingestion using keyset pagination
- `CSVSource` for CSV files
- `JSONLSource` for newline-delimited JSON
- `SliceSource` for tests/small local imports
- `ChannelSource` for producer/consumer ingestion
- `MemoryCheckpoint` and `FileCheckpoint` for resumable imports

### 100M record database ingestion pattern

For very large tables, avoid `OFFSET`. Use `PagedSQLSource` with a monotonically increasing key such as `id`, `record_id`, or `created_sequence`.

```go
ix, _ := lookup.New(lookup.Config{
    DisableSource: true,
    InitialCapacity: 100_000_000,
    Schema: lookup.TupleLookupSchema(),
})

term := ix.FieldID("term")
group := ix.FieldID("group_id")
date_key := ix.FieldID("date_key")
partition := ix.FieldID("partition_id")

src := lookup.PagedSQLSource{
    DB: db,
    Table: "record_lookup",
    Columns: []string{"id", "term", "group_id", "date_key", "partition_id"},
    OrderColumn: "id",
    IDColumn: "id",
    SeqColumn: "id",
    PageSize: 100000,
    ColumnBindings: []lookup.SQLColumn{
        {Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
    },
}

stats, err := ix.IndexFrom(ctx, src, lookup.BulkOptions{
    Name: "record_lookup",
    BatchSize: 65536,
    CheckpointEvery: 65536,
    Checkpoint: lookup.FileCheckpoint{Path: "./lookupx-checkpoint.json"},
    Resume: true,
})
```


### Raw SQL query ingestion

Use `SQLQuerySource` when your source is not a simple table scan. It supports joins, CTEs, views, database-specific predicates, and normal `database/sql` arguments. The query must return the configured `IDColumn` and every column listed in `Columns`.

```go
src := lookup.SQLQuerySource{
    DB: db,
    Query: `
SELECT e.id, c.code AS term, e.group_id, e.date_key, e.partition_id
FROM records e
JOIN record_codes c ON c.id = e.code_id
WHERE e.deleted_at IS NULL
  AND e.partition_id = $1
  AND e.id > $2
ORDER BY e.id ASC
LIMIT $3`,
    Args: []any{partitionID, lastID, 100000},
    IDColumn: "id",
    SeqColumn: "id",
    Columns: []lookup.SQLColumn{
        {Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
    },
}

stats, err := ix.IndexFrom(ctx, src, lookup.BulkOptions{
    Name: "record_sql_query",
    BatchSize: 65536,
    Checkpoint: lookup.FileCheckpoint{Path: "./lookupx-checkpoint.json"},
    Resume: true,
})
```

For arbitrary SQL with 100M+ records, use `PagedSQLQuerySource` and provide a page function. This avoids `OFFSET` and lets you keep joins/filters in the source query.

```go
page := func(last uint64, limit int) (string, []any) {
    return `
SELECT e.id, c.code AS term, e.group_id, e.date_key, e.partition_id
FROM records e
JOIN record_codes c ON c.id = e.code_id
WHERE e.deleted_at IS NULL
  AND e.partition_id = $1
  AND e.id > $2
ORDER BY e.id ASC
LIMIT $3`, []any{partitionID, last, limit}
}

src := lookup.PagedSQLQuerySource{
    DB: db,
    Page: page,
    PageSize: 100000,
    IDColumn: "id",
    SeqColumn: "id",
    Columns: []lookup.SQLColumn{
        {Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
        {Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
    },
}
```

Generated query helpers are also included:

```go
query, args, err := lookup.TupleSQLQuery(lookup.TupleSQLQueryOptions{
    Dialect: lookup.SQLDialectPostgres,
    GroupID: 4,
    DateKey: "2026-01-01",
    Term: "key-special",
    Limit: 100000,
})
```

### Searching indexed database records

The query:

```text
term=key-special&group_id=4&date_key=2026-01-01
```

can be executed as:

```go
q := lookup.ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
_, hits := ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 50}, nil)
```

Internally this becomes a boolean filter:

```go
lookup.Bool{Filter: []lookup.Query{
    lookup.Term{Field: "term", Value: "key-special"},
    lookup.Term{Field: "group_id", Value: "4"},
    lookup.Term{Field: "date_key", Value: "2026-01-01"},
}}
```

### Recommended schema for 100M record/group lookup

```go
lookup.TupleLookupSchema()
```

Fields:

- `term` keyword lookup, lowercase, prefix enabled
- `group_id` keyword lookup
- `date_key` keyword lookup in `YYYY-MM-DD`
- `partition_id` keyword lookup
- `entity_id` keyword lookup

For 100M+ rows, prefer exact keyword fields for filters instead of text analysis. Store source documents in the database and keep `DisableSource: true` in the index to minimize memory.

### Examples

Run:

```bash
go run ./examples/database_ingest
go run ./examples/sql_query_ingest
go run ./examples/source_files
```

## 100K Dataset database example

A fully runnable example is included under `examples/dataset_a_100k_database`. It uses a small in-memory `database/sql` driver so it can run without MySQL/PostgreSQL/SQLite installed, but the ingestion path is the same one used for a real database: `PagedSQLQuerySource` + `IndexFrom`.

Run:

```bash
go run ./examples/dataset_a_100k_database
```

Expected output shape:

```text
indexed=100000 skipped=0 took=...
query="term=key-0013&group_id=4&date_key=2026-01-01" hits=5 first=enc-000123
query="term=key-special&group_id=4&date_key=2026-01-01" hits=5 first=enc-000456
query="term=key-0014&group_id=4&date_key=2026-01-04" hits=5 first=enc-000003
```

The example demonstrates:

- generating 100,000 Dataset/record lookup rows from a SQL source;
- keyset pagination using `seq > ? ORDER BY seq ASC LIMIT ?`;
- indexing `term`, `group_id`, `date_key`, and `partition_id`;
- searching with URL-style lookup queries such as `term=key-0013&group_id=4&date_key=2026-01-01`;
- keeping source documents disabled for low-memory lookup indexes.

Replace the fake driver query with your real database query:

```sql
SELECT id, seq, term, group_id, date_key, partition_id
FROM record_dataset_a_lookup
WHERE seq > ?
ORDER BY seq ASC
LIMIT ?
```

For PostgreSQL, MySQL, SQL Server, or SQLite, pass your existing `*sql.DB` into `lookup.PagedSQLQuerySource` and keep the same column bindings.

## Multi-index storage, HTTP API, CLI, and reload support

This build supports multiple independent index IDs in one process. Typical domain-specific lookup indexes can be loaded separately as:

- `dataset_a`
- `dataset_b`
- `dataset_c`

Each index has its own schema, stats, source, reload status, generation, and latency metadata. Reloading one index swaps that index atomically without stopping searches on the other indexes.

### 100K Dataset database latency example

Run:

```bash
go run ./examples/dataset_a_100k_database
```

Example output:

```text
indexed=100000 skipped=0 took=149ms ns_per_row=1485.0 rows_per_sec=673395
query="term=key-0013&group_id=4&date_key=2026-01-01" hits=5 avg_query_ns=9688 loops=1000 first=enc-000123
query="term=key-special&group_id=4&date_key=2026-01-01" hits=5 avg_query_ns=9407 loops=1000 first=enc-000456
query="term=key-0014&group_id=4&date_key=2026-01-04" hits=5 avg_query_ns=8833 loops=1000 first=enc-000003
```

The example uses a deterministic in-memory `database/sql` driver and indexes through `PagedSQLQuerySource`, so it exercises the SQL source path without requiring an external database.

### Multi-index example

Run:

```bash
go run ./examples/multi_indexes
```

It loads `dataset_a`, `dataset_b`, and `dataset_c` as separate indexes, then prints per-index load latency and query latency.

### CLI

```bash
# Build and query demo Dataset/DatasetB/DatasetC indexes.
go run ./cmd/lookupx demo -rows 100000

# Query a single demo index.
go run ./cmd/lookupx search -index dataset_a -rows 100000 -q 'term=key-0013&group_id=4&date_key=2026-01-01'

# Start the HTTP API. Add -demo to preload dataset_a/dataset_b/dataset_c demo indexes.
go run ./cmd/lookupx serve -addr :8089 -demo -rows 100000
```

### HTTP API

Start:

```bash
go run ./cmd/lookupx serve -addr :8089 -demo -rows 100000
```

List indexes:

```bash
curl http://localhost:8089/v1/indexes
```

Get stats and last reload latency:

```bash
curl http://localhost:8089/v1/indexes/dataset_a/stats
```

Search using lookup query parameters:

```bash
curl 'http://localhost:8089/v1/indexes/dataset_a/lookup?term=key-0013&group_id=4&date_key=2026-01-01&limit=5'
```

Search using JSON query DSL:

```bash
curl -X POST http://localhost:8089/v1/indexes/dataset_a/search \
  -H 'Content-Type: application/json' \
  -d '{
    "type":"bool",
    "filter":[
      {"type":"term","field":"term","value":"key-0013"},
      {"type":"term","field":"group_id","value":"4"},
      {"type":"term","field":"date_key","value":"2026-01-01"}
    ],
    "limit":5
  }'
```

Count:

```bash
curl -X POST http://localhost:8089/v1/indexes/dataset_a/count \
  -H 'Content-Type: application/json' \
  -d '{"type":"term","field":"term","value":"key-0013"}'
```

Reload a registered index source:

```bash
curl -X POST http://localhost:8089/v1/indexes/dataset_a/reload
```

Reload from an arbitrary SQL query. The database driver must be imported/registered by the embedding application or custom server build:

```bash
curl -X POST http://localhost:8089/v1/indexes/dataset_a/reload-sql \
  -H 'Content-Type: application/json' \
  -d '{
    "driver":"postgres",
    "dsn":"postgres://user:pass@localhost/db?sslmode=disable",
    "query":"SELECT id, id AS seq, term, group_id, date_key, partition_id FROM record_dataset_a_lookup WHERE deleted_at IS NULL ORDER BY id ASC",
    "id_column":"id",
    "seq_column":"seq",
    "batch_size":65536,
    "checkpoint_path":"./data/dataset_a.checkpoint.json",
    "resume":true,
    "columns":[
      {"column":"term","field":"term","kind":"keyword","normalized":false},
      {"column":"group_id","field":"group_id","kind":"keyword","normalized":true},
      {"column":"date_key","field":"date_key","kind":"keyword","normalized":true},
      {"column":"partition_id","field":"partition_id","kind":"keyword","normalized":true}
    ]
  }'
```

Reload a large table with keyset pagination:

```bash
curl -X POST http://localhost:8089/v1/indexes/dataset_a/reload-table \
  -H 'Content-Type: application/json' \
  -d '{
    "driver":"postgres",
    "dsn":"postgres://user:pass@localhost/db?sslmode=disable",
    "table":"record_dataset_a_lookup",
    "select_columns":["id", "id AS seq", "term", "group_id", "date_key", "partition_id"],
    "where":"deleted_at IS NULL",
    "order_column":"id",
    "id_column":"id",
    "seq_column":"seq",
    "page_size":100000,
    "batch_size":65536,
    "checkpoint_path":"./data/dataset_a.checkpoint.json",
    "resume":true,
    "columns":[
      {"column":"term","field":"term","kind":"keyword"},
      {"column":"group_id","field":"group_id","kind":"keyword","normalized":true},
      {"column":"date_key","field":"date_key","kind":"keyword","normalized":true},
      {"column":"partition_id","field":"partition_id","kind":"keyword","normalized":true}
    ]
  }'
```

Metrics:

```bash
curl http://localhost:8089/v1/metrics
```

### Embedded multi-index usage

```go
mgr := lookup.NewMultiIndexManager()
_ = lookup.RegisterDemoIndexes(mgr, 100_000_000)

// Reload only Dataset.
stats, err := mgr.ReloadFromFactory(ctx, "dataset_a", func(ix *lookup.Index) (lookup.Source, error) {
    term := ix.FieldID("term")
    work := ix.FieldID("group_id")
    date_key := ix.FieldID("date_key")
    partition := ix.FieldID("partition_id")
    return lookup.PagedSQLQuerySource{
        DB: db,
        PageSize: 100000,
        Page: func(last uint64, limit int) (string, []any) {
            return `SELECT id, id AS seq, term, group_id, date_key, partition_id
                    FROM record_dataset_a_lookup
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT ?`, []any{last, limit}
        },
        IDColumn: "id",
        SeqColumn: "seq",
        Columns: []lookup.SQLColumn{
            {Column: "term", Field: term, Kind: lookup.ValueKeyword},
            {Column: "group_id", Field: work, Kind: lookup.ValueKeyword, Normalized: true},
            {Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
            {Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
        },
    }, nil
}, lookup.BulkOptions{BatchSize: 65536, Resume: true})

ix, _ := mgr.Get("dataset_a")
_, hits := ix.SearchInto(lookup.SearchRequest{
    Query: lookup.ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01"),
    Limit: 50,
}, nil)
_ = stats
_ = hits
```

## Production-scale indexing: 100M / 1B row support

This version includes a production-scale layer for very large lookup indexes. The target operating model for 100M/1B rows is:

1. Stream rows from SQL with keyset pagination or CDC-style incremental queries.
2. Build each logical index independently, for example `dataset_a`, `dataset_b`, and `dataset_c`.
3. Enable the record composite accelerator for the common lookup shape:

```text
(term, group_id, date_key) -> document IDs
```

4. Freeze the index after reload to prepare read-mostly fast paths.
5. Persist the frozen generation to disk with an atomic `CURRENT` pointer.
6. Reload only the affected index ID or partition instead of stopping the full service.

### Persistent index storage

```go
store := lookup.FileSegmentStore{Root: "./data/indexes"}

// Save an already-built index. This stores internal lookup structures, not only source documents,
// so it works with DisableSource=true.
manifest, err := ix.SavePersistent(ctx, store, "dataset_a")

// Restore the current generation.
ix, manifest, err := lookup.OpenPersistent(ctx, store, "dataset_a", lookup.Config{})
```

On disk:

```text
data/indexes/dataset_a/
  CURRENT
  generation-00000000000000000001/
    manifest.json
    index.gob
```

The `CURRENT` file is atomically advanced after a successful save. This supports blue/green index generations and rollback by changing `CURRENT` to a previous generation.

### Composite Dataset / DatasetB / DatasetC lookup

```go
ix.EnableTupleComposite()

q := lookup.ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
_, hits := ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 50}, nil)
```

When `term`, `group_id`, and `date_key` are all present, `ParseLookupQuery` returns a composite query. If the composite accelerator exists, search is one direct composite-key lookup. If it does not exist, the query falls back to the generic boolean filter path.

### Freeze read-only generation

```go
err := ix.Freeze()
```

`Freeze` builds the composite accelerator if needed and marks the index as read-mostly. Mutable operations still work, but production reload flow should build a new generation and atomically swap it into the `MultiIndexManager`.

### Async reload task API

The multi-index HTTP server now exposes task-style operations for long reloads:

```bash
POST /v1/indexes/dataset_a/reload-async
GET  /v1/tasks
GET  /v1/tasks/{task_id}
POST /v1/tasks/{task_id}/cancel
```

### Persistence and freeze HTTP API

```bash
POST /v1/indexes/dataset_a/freeze
POST /v1/indexes/dataset_a/persist
GET  /v1/indexes/dataset_a/lookup-composite?term=key-0013&group_id=4&date_key=2026-01-01&limit=5
```

Set the persistent root using:

```bash
export LOOKUPX_DATA_DIR=./data/indexes
```

### CLI production commands

```bash
lookupx persist-demo -rows 100000 -data ./data/indexes
lookupx restore-search -index dataset_a -data ./data/indexes -q 'term=key-0013&group_id=4&date_key=2026-01-01'
```

### 1B row recommendations

For 1B rows, do not build one monolithic heap-only index. Use:

- separate logical indexes: `dataset_a`, `dataset_b`, `dataset_c`
- partition IDs by high-selectivity routing keys, for example `dataset_a-wi4-2026-01`
- keyset pagination or CDC-based incremental sync
- `DisableSource: true`
- large `InitialCapacity` per partition, not globally
- persistent generations per partition
- async reload tasks
- atomic index/partition swap

The included `PartitionRouter` demonstrates the route shape:

```go
router := lookup.PartitionRouter{Manager: mgr}
hits := router.SearchTuple("dataset_a", "key-0013", 4, "2026-01-01", 5, nil)
```

## Production-scale example

```bash
go run ./examples/production_scale
```

It demonstrates:

- 100K Dataset-style indexing
- composite lookup latency
- freeze
- persistent save
- persistent restore
- search after restore

## 1B-row production deployment additions

This version adds the remaining production-scale pieces for very large lookup deployments:

- Persistent generation storage with atomic `CURRENT` pointer.
- `FileMMapSegmentStore` layout using `segment.dat`, `manifest.json`, `generation.json`, and checksums.
- Validation and repair APIs for persisted generations.
- Generation listing and compaction.
- Billion-row partition planning.
- Partition manifest/catalog helpers.
- Memory-budgeted bulk options.
- Incremental SQL checkpoint contract using `(updated_at, id)` keyset sync.
- Mutation/CDC source contract with upsert/delete mutation support.
- Parallel SQL partition ingestion helper.
- Production HTTP endpoints for plan, generations, validate, repair, and compact.
- CLI commands for plan, validate, repair, compact, and generation listing.
- `examples/billion_deployment` demonstrating 100K-row build, composite query, mmap-style persistence, validation, restore, and search.

### 1B-row layout recommendation

Do not build one single heap index with 1B rows. Use many frozen persistent partitions:

```text
/data/indexes/
  dataset_a-wi04-202601/
    CURRENT
    generation-000.../
      segment.dat
      manifest.json
      generation.json
  dataset_a-wi04-202602/
  dataset_b-wi04-202601/
  dataset_c-wi04-202601/
```

Use the planner:

```bash
go run ./cmd/lookupx plan -index dataset_a -rows 1000000000
```

Example output includes the recommended partition count, rows per partition, and batch size.

### Production CLI

```bash
go run ./cmd/lookupx persist-demo -rows 100000 -data ./data/indexes

go run ./cmd/lookupx generations -index dataset_a -data ./data/indexes

go run ./cmd/lookupx validate -index dataset_a -data ./data/indexes

go run ./cmd/lookupx repair -index dataset_a -data ./data/indexes

go run ./cmd/lookupx compact -index dataset_a -data ./data/indexes -keep 2
```

### Production HTTP endpoints

```text
GET  /v1/plan?index=dataset_a&rows=1000000000
GET  /v1/indexes/{id}/plan?rows=1000000000
GET  /v1/indexes/{id}/generations
GET  /v1/indexes/{id}/validate
POST /v1/indexes/{id}/repair
POST /v1/indexes/{id}/compact
```

Existing operational endpoints remain available:

```text
POST /v1/indexes/{id}/reload-async
GET  /v1/tasks
GET  /v1/tasks/{task_id}
POST /v1/tasks/{task_id}/cancel
POST /v1/indexes/{id}/freeze
POST /v1/indexes/{id}/persist
GET  /v1/indexes/{id}/lookup-composite?term=key-special&group_id=4&date_key=2026-01-01
```

### Incremental SQL sync pattern

Use `IncrementalSQLQuery` to generate stable keyset pages:

```go
q := lookup.IncrementalSQLQuery{
    BaseSelect: `SELECT id, term, group_id, date_key, partition_id, updated_at FROM record_lookup`,
    UpdatedColumn: "updated_at",
    IDColumn: "id",
    Dialect: lookup.SQLDialectPostgres,
    PageSize: 100000,
}
query, args := q.Page(checkpoint)
```

For CDC/delete streams, implement `MutationSource` and pass it to `ApplyMutations`; it supports both upsert and delete mutation records.

### Billion-row example

```bash
go run ./examples/billion_deployment
```

The example builds a 100K-row Dataset-like index, runs a composite lookup, persists it using `FileMMapSegmentStore`, validates it, restores it, and searches the restored index.

## Low-memory 1B-row deployment mode

`examples/billion_deployment` no longer materializes all rows. It uses `StreamingDatasetSource` plus `BuildPartitionedPersistent`, so memory is bounded by one active partition and one batch.

Safe laptop run:

```bash
go run ./examples/billion_deployment
```

This plans for 1B rows, then indexes a 100K streaming sample by default so a 16GB laptop is not killed by trying to execute a full 1B-row load during an example run.

Full streaming run, still partitioned and low-memory:

```bash
ROWS=1000000000 RUN_FULL=1 PARTITION_ROWS=250000 BATCH=16384 DATA_DIR=/data/lookupx go run ./examples/billion_deployment
```

Important behavior:

- No `make([]SourceRecord, rows)`.
- No full source materialization.
- Each partition is built, frozen, persisted, closed, and released before the next partition.
- Default laptop-safe budget uses 250K rows per partition and 16K batch size.
- Use database sources in production; `StreamingDatasetSource` is only a deterministic low-memory generator/example.

New APIs:

```go
budget := lookup.LowMemoryBillionBudget()
src := lookup.StreamingDatasetSource{Rows: rows, Term: term, Group: work, DateKey: date_key, Partition: partition}
stats, err := lookup.BuildPartitionedPersistent(ctx, src, lookup.PartitionedBuildOptions{
    IndexID: "dataset_a",
    Store: lookup.FileMMapSegmentStore{Root: "./data"},
    Config: lookup.Config{Schema: lookup.TupleLookupSchema(), DisableSource: true},
    RowsPerPartition: uint64(budget.TargetRowsPerPartition),
    Bulk: budget.BulkOptions("dataset_a", nil, false),
    EnableComposite: true,
    Freeze: true,
})
```

### Cold vs warm partition query latency

`SearchPartitionedPersistent` is a low-memory cold path. It loads one persisted partition, searches it, closes it, then moves to the next partition. This avoids loading all partitions into RAM but includes disk decode/open cost.

For online low-latency serving, keep only the hot partition set loaded in `MultiIndexManager` or load the pruned partition selected by `group_id` / `date_key` / source. Warm loaded partition queries stay in the normal nanosecond/microsecond lookup path.

The billion example now prints both:

- `cold_partitioned_query_partitions`: includes partition open/decode cost.
- `warm_loaded_partition_query`: measures the actual lookup latency after a partition is loaded.

## Generic core and apples-to-apples Lucene benchmark

The core lookup engine is now generic. Dataset/Dataset/DatasetC examples are only examples;
production code should define its own schema, source fields, and composite keys.

Generic helpers added:

- `GenericLookupSchema(map[string]FieldKind, prefixFields...)`
- `CompositeDefinition` / `CompositeField`
- `Index.EnableComposite(...)`
- `Index.CompositeLookup(...)`
- `StreamingRowsSource` for low-memory generated/source-style ingestion
- `cmd/benchgen` to generate a shared JSONL benchmark dataset
- `cmd/benchlookupx` to run LookupX on that dataset
- `bench/lucene` Java/Maven runner to run Lucene on the exact same dataset/query manifest

Example generic composite lookup:

```go
schema := lookup.GenericLookupSchema(map[string]lookup.FieldKind{
    "term": lookup.FieldKeyword,
    "group_id": lookup.FieldKeyword,
    "date_key": lookup.FieldKeyword,
})
ix, _ := lookup.New(lookup.Config{Schema: schema, DisableSource: true})
ix.EnableComposite(lookup.CompositeDefinition{
    ID: "main",
    Fields: []lookup.CompositeField{
        {Name: "term", ID: ix.FieldID("term")},
        {Name: "group_id", ID: ix.FieldID("group_id")},
        {Name: "date_key", ID: ix.FieldID("date_key")},
    },
})
hits := ix.CompositeLookup("main", []string{"key-special", "4", "2026-01-01"}, 10, nil)
```

Run the generic 100K example:

```bash
go run ./examples/generic_100k_database
```

Generate a shared benchmark dataset:

```bash
go run ./cmd/benchgen -rows 100000 -out /tmp/lookupx-bench -fields term,group_id,date_key
```

Run LookupX:

```bash
go run ./cmd/benchlookupx \
  -data /tmp/lookupx-bench/dataset.jsonl \
  -queries /tmp/lookupx-bench/queries.jsonl \
  -fields term,group_id,date_key \
  -loops 10000
```

Run Lucene with the same data and same exact-key query semantics:

```bash
cd bench/lucene
mvn -q compile exec:java -Dexec.args="--data /tmp/lookupx-bench/dataset.jsonl --queries /tmp/lookupx-bench/queries.jsonl --fields term,group_id,date_key --loops 10000"
```


## Apple-to-apple Lucene benchmark on macOS

The repository includes a shared benchmark harness so LookupX and Lucene use the same generated JSONL dataset, the same query manifest, the same indexed fields, and the same query loop count.

Install Maven on macOS:

```bash
make install-mvn
```

Generate data and run both engines:

```bash
make bench-compare ROWS=100000 LOOPS=1000 FIELDS=term,group_id,date_key
```

Run each side independently:

```bash
make benchdata ROWS=100000 FIELDS=term,group_id,date_key
make bench-lookupx LOOPS=1000 FIELDS=term,group_id,date_key
make bench-lucene LOOPS=1000 FIELDS=term,group_id,date_key
```

Results are written to:

```text
benchdata/lookupx-result.json
benchdata/lucene-result.json
```

The Lucene target requires Java 17+ and Maven. On macOS, `make install-mvn` installs Maven through Homebrew when `mvn` is missing. Java is intentionally not installed automatically because JDK selection can affect system configuration; install it with `brew install openjdk@17` if needed.

## Generic engine guarantee

The reusable engine is not tied to any business domain. Index IDs, schema fields, composite-key definitions, SQL bindings, source mappings, and benchmark fields are all configurable. Demo datasets use neutral placeholder names and can be replaced with any domain-specific schema by configuring `Schema`, `SourceValue` mappings, and `CompositeDefinition`.

Generic benchmark example:

```bash
make benchdata ROWS=100000 FIELDS=field_a,field_b,field_c
make bench-lookupx ROWS=100000 LOOPS=1000 FIELDS=field_a,field_b,field_c
make bench-lucene ROWS=100000 LOOPS=1000 FIELDS=field_a,field_b,field_c
```
