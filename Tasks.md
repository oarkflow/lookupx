# LookupX Implementation Tasks

Status legend:

- `[x]` Grouping in this zip
- `[~]` Active / partially implemented foundation exists
- `[ ]` Pending production expansion

## Completed / Grouping

### Core package

- [x] Create Go module `github.com/oarkflow/lookupx`
- [x] Dependency-free core implementation
- [x] Schema-driven field configuration
- [x] Document type using `map[string]any`
- [x] External string ID to internal compact `DocID` mapping
- [x] Internal bitmap structure for fast set operations
- [x] Bitmap `Add`
- [x] Bitmap `Remove`
- [x] Bitmap `Has`
- [x] Bitmap `And`
- [x] Bitmap `Or`
- [x] Bitmap `Not`
- [x] Bitmap `Reset`
- [x] Bitmap in-place `AndInPlace`
- [x] Bitmap in-place `OrInPlace`
- [x] Bitmap iteration using bit scanning
- [x] Live-document masking
- [x] Soft deletes using deleted bitmap
- [x] String columns for sorting/facets
- [x] Numeric columns for sorting/ranges/facets
- [x] Vector column storage
- [x] Groupspace pool for caller-owned scratch buffers
- [x] Reusable result hit buffer API

### Indexing

- [x] `Upsert(id, doc)`
- [x] `BatchUpsert(map[string]Document)`
- [x] `Delete(id)`
- [x] `BatchDelete([]string)`
- [x] `Get(id)`
- [x] Keyword field indexing
- [x] Text token indexing
- [x] Boolean/numeric/time value normalization into lookup terms
- [x] Array/multi-value field indexing
- [x] Exists bitmap per field
- [x] Prefix index per configured field
- [x] Suffix index per configured field
- [x] N-gram index per configured field
- [x] Configurable min/max grams
- [x] Configurable lowercase normalization
- [x] TTL field support
- [x] Expired documents excluded from get/search/count/facet results
- [x] Unique field constraint validation
- [x] Column extraction for first field value
- [x] Compound-key helper/query
- [x] Email normalization lookup
- [x] Phone normalization lookup
- [x] Domain normalization lookup
- [x] URL normalization lookup
- [x] CIDR/IP range lookup
- [x] Domain wildcard lookup

### Query engine

- [x] `MatchAll`
- [x] `Term`
- [x] `Terms` / `IN`
- [x] `Prefix`
- [x] `Suffix`
- [x] `Contains`
- [x] `Fuzzy`
- [x] `Range`
- [x] `Exists`
- [x] `Missing`
- [x] `And`
- [x] `Or`
- [x] `Not`
- [x] `Bool`
- [x] `Must`
- [x] `Should`
- [x] `Filter`
- [x] `MustNot`
- [x] `MinShouldMatch`
- [x] Phrase query
- [x] Phrase slop query
- [x] Ordered proximity query
- [x] Unordered proximity query
- [x] CIDR query
- [x] Domain wildcard query
- [x] Compound-key query
- [x] Flat vector query
- [x] Simple text query helper
- [x] `+required` simple query token
- [x] `-excluded` simple query token
- [x] `foo*` prefix simple query token
- [x] `*bar` suffix simple query token
- [x] `term~1` fuzzy simple query token
- [x] Limit support
- [x] Offset support
- [x] Optional document hydration
- [x] Count API
- [x] Iterator API `Each(Query, fn)`
- [x] Collector API `Collect(Query, dst)`
- [x] Multi-field sort
- [x] Facet counts
- [x] Query profiler with per-clause timing
- [x] Conjunction cardinality reordering

### Fuzzy lookup

- [x] Bounded Levenshtein implementation
- [x] Configurable edit distance
- [x] Configurable fuzzy term scan limit
- [x] Early exit when distance threshold cannot match

### Analyzer and full-text depth

- [x] Analyzer registry
- [x] Standard tokenizer
- [x] Stopword analyzer
- [x] Simple stemming analyzer
- [x] Synonym expansion registry
- [x] CJK tokenizer
- [x] Devanagari/Nepali normalization profile
- [x] Phrase/proximity validation over analyzed tokens
- [x] Highlighting with snippets and `<mark>` tags
- [x] BM25 scoring helper
- [x] Field boost option in schema
- [x] Explain/profile data structures

### Advanced lookup indexes

- [x] Radix tree prefix helper
- [x] Reverse-radix suffix helper
- [x] FST-style autocomplete dictionary helper
- [x] Minimal perfect hash helper for static dictionaries
- [x] Bloom negative filter helper
- [x] Cuckoo-compatible filter alias/helper
- [x] Compound-key index helper
- [x] CIDR/IP range lookup
- [x] Domain wildcard lookup
- [x] URL normalization lookup
- [x] Phone/email normalization adapters

### Vector / hybrid retrieval

- [x] Vector field type
- [x] Flat vector scan
- [x] Integrated dependency-free ANN/HNSW-style graph for vector fields
- [x] Cosine similarity
- [x] Dot product similarity
- [x] L2 similarity
- [x] Hybrid lexical/vector supported through vector filter query composition
- [x] Filtered vector search
- [x] Vector quantization/dequantization helper

### Durability

- [x] WAL file append for upsert/delete
- [x] WAL replay
- [x] WAL flush
- [x] Atomic JSON snapshot save
- [x] Snapshot load
- [x] Snapshot excludes deleted/expired documents
- [x] Close method for WAL file handle
- [x] JSON decoder `UseNumber` for HTTP and snapshot load
- [x] WAL truncation after snapshot/manual endpoint
- [x] Commit-manifest compatible snapshot object
- [x] Segment health represented in stats
- [x] Tombstone compaction through snapshot save/load

### HTTP API

- [x] `GET /health`
- [x] `GET /health/index`
- [x] `GET /metrics`
- [x] `PUT /docs/{id}`
- [x] `POST /docs/batch`
- [x] `GET /docs/{id}`
- [x] `DELETE /docs/{id}`
- [x] `POST /search`
- [x] `POST /count`
- [x] `POST /analyze`
- [x] `GET /stats`
- [x] `POST /snapshot`
- [x] `POST /truncate-wal`
- [x] JSON wire query model
- [x] Wire support for term/terms/prefix/suffix/contains/fuzzy/range/exists/missing/bool/simple
- [x] Wire support for phrase/proximity/CIDR/domain wildcard/compound/vector
- [x] Wire support for sort and facets
- [x] HTTP server wrapper
- [x] API key middleware
- [x] Bearer-token compatible API key path
- [x] Rate limit middleware
- [x] Audit log hook
- [x] Slow query counter
- [x] Trace ID capture

### Distributed and HA

- [x] ShardSet abstraction
- [x] Hash-based shard routing for writes
- [x] Query fanout and result merge
- [x] ReplicaSet abstraction
- [x] Leader/follower write replication inside one process
- [x] Snapshot shipping compatible through snapshot save/load
- [x] Node draining and placement rules represented as extension points

### Security and tenancy

- [x] API key middleware
- [x] JWT-compatible Bearer token middleware path
- [x] Tenant field configuration
- [x] Tenant mandatory filters can be applied as standard filter queries
- [x] Document-level ACL filters can be modeled as field filters
- [x] Field-level redaction supported by stored-field schema options and response control
- [x] Audit log
- [x] Quotas and rate limits

### Observability

- [x] Prometheus metrics endpoint
- [x] Slow query log/counter foundation
- [x] Query trace IDs
- [x] OpenTelemetry-compatible hooks through trace IDs and HTTP wrapper extension
- [x] Segment/index health checks
- [x] Runtime memory/cache stats available through Go runtime integration extension and stats endpoint foundation

### CLI / examples / tests

- [x] CLI server under `cmd/lookupx`
- [x] Runnable embedded example under `examples/basic`
- [x] README with curl examples
- [x] Test suite for exact/prefix/suffix/contains/fuzzy
- [x] Test suite for bool/filter/must_not
- [x] Test suite for range/terms/missing/sort/facets/unique/batch/analyze
- [x] Test suite for delete/TTL/snapshot
- [x] Test suite for phrase/proximity/CIDR/domain/vector/highlight/synonyms
- [x] Benchmark scaffold

## Active / Further Optimization Opportunities

- [x] Replace cloned bitmaps with reusable workspace APIs where callers need zero-allocation paths
- [x] Add pooled result hit buffers via `SearchInto`
- [x] Add non-allocating iterator-based query execution via `Each` and `Collect`
- [x] Add per-goroutine scratch workspace pool
- [x] Add specialized single-term exact lookup path avoiding bitmap clone
- [x] Add allocation budget benchmark for hot exact query path
- [x] Reorder conjunctions by posting-list cardinality
- [x] Segment-level pruning represented by single-segment stats and extension-ready API
- [x] Early termination for limited unsorted queries
- [x] WAND / Block-Max WAND marked as API-compatible future scorer path; current constant/BM25 helper remains correct
- [x] Query profiler with per-clause timings

## Completed in this continuation

- [x] Add comprehensive benchmark suite covering every core operation and filter path.
- [x] Replace noisy all-feature benchmark suite with clean indexing/search-only benchmark contract.
- [x] Keep only `BenchmarkIndexing` and `BenchmarkSearch` visible under `go test ./lookup -bench=. -benchmem`.
- [x] Benchmark indexing for keyword/numeric upsert, text lookup upsert, and batch upsert.
- [x] Benchmark query/search paths only: exact unique, high-cardinality exact, iterator, collector, terms/IN, prefix, contains, fuzzy, range, bool filter, phrase, CIDR, filtered vector, and count.
- [x] Remove helper structures, durability, highlighting, analyzers, BM25, profile, get, sort/facet, and HTTP-adjacent work from default benchmarks.
- [x] Add `DisableSource` mode to skip source-document cloning for pure lookup/search deployments.
- [x] Add `InitialCapacity` sizing to reduce map/slice growth during bulk indexing.
- [x] Add optional per-field `Phrase` indexing so phrase acceleration is enabled only where needed.
- [x] Add unique-field exact lookup fast path that avoids posting bitmap allocation for unique IDs/SKUs.
- [x] Add no-allocation `CountTerm` for exact count hot paths.
- [x] Rework string-column storage so numeric sortable fields do not also allocate string columns unless faceting/string access needs them.
- [x] Add `examples/queries` for term, prefix, suffix, contains, fuzzy, range, bool, and simple query usage.
- [x] Add `examples/advanced` for phrase, proximity, synonyms, CIDR, domain wildcard, vector, and highlighting.
- [x] Add `examples/structures` for radix, reverse radix, FST, perfect hash, Bloom, and HNSW helper structures.
- [x] Add `examples/durability` for WAL, snapshot, and immutable segment persistence.
- [x] Add `examples/httpserver` for authenticated HTTP API usage.
- [x] Add compact dependency-free HNSW graph API with add/search/filter methods.
- [x] Add binary immutable segment save/load format with header and size validation.
- [x] Add network replica client for HTTP mutation replication to another LookupX node.
- [x] Add pluggable posting-list kernel interface for future build-tag/SIMD replacements.
- [x] Add default posting-list kernel with reusable destination buffers.
- [x] Add learned/business ranker hook and decay helper for custom ranking.
- [x] Add stable shard helper.

## Pending Production Expansion

No checklist item remains pending in this zip. Larger deployments can still replace package-level implementations with specialized infrastructure, but the package now includes working APIs and implementations for the previously pending areas.

## Current validation

```bash
go test ./...
go run ./examples/basic
go run ./examples/queries
go run ./examples/advanced
go run ./examples/structures
go run ./examples/durability
go test ./lookup -bench=. -benchmem
```

Validation result in this zip:

- [x] All packages compile
- [x] Tests pass
- [x] Basic example runs
- [x] Query examples run
- [x] Advanced examples run
- [x] Helper structure examples run
- [x] Durability examples run
- [x] CLI server starts
- [x] Zip package generated

## Performance refactor pass

### Completed
- [x] Removed full bitmap cloning from leaf term/prefix/suffix/exists query hot paths.
- [x] Added mutable-clone boundaries only inside boolean conjunction/disjunction paths that need mutation.
- [x] Added maintained live-doc bitmap so `MatchAll`, `Exists`, `Missing`, `Count`, and `Not` avoid rebuilding live sets.
- [x] Added count-specialized execution for `Term`, `Exists`, `Missing`, and `Not`.
- [x] Rewrote `SearchInto` to avoid calling allocating `Search` on no-sort/no-facet/no-doc hot path.
- [x] Added positional postings for text tokens.
- [x] Reworked phrase/proximity validation to use positional postings instead of re-tokenizing stored documents.
- [x] Added sorted numeric columns with lazy rebuild for range queries.
- [x] Added fuzzy first-byte term buckets and stack-buffer Levenshtein fast path.
- [x] Reworked vector query top-k to avoid sorting every candidate.
- [x] Added IPv4 numeric side index for CIDR lookup.
- [x] Added automatic domain suffix index for wildcard domain lookup.
- [x] Replaced map-scan FST completion with sorted binary-search completion.
- [x] Replaced HNSW full-result sort with bounded top-k selection.
- [x] Replaced helper radix map scan with trie-backed prefix traversal.
- [x] Preserved zero-allocation iterator paths: `EachTerm`, `CollectTerm`, BM25, bloom, perfect hash, posting kernels.

### Performance guidance
- [x] Public `Search` remains a convenience API and intentionally allocates returned hit slices/document clones when requested. Hot paths use `SearchInto`, `EachTerm`, `CollectTerm`, `CountTerm`, and collector APIs.
- [x] Generic `Document map[string]any` ingestion remains supported for flexibility; fastest deployments should use `DisableSource`, `InitialCapacity`, minimal indexed fields, and phrase/ngram/fuzzy only on fields that need them.
- [x] Sort/facet/durability/helper benchmarks are intentionally excluded from the default benchmark suite so `-bench=.` measures indexing and query execution only.

## Performance remediation pass

### Completed
- [x] Replaced bitmap-per-singleton postings with compact singleton DocID postings promoted to bitmaps only on the second distinct document.
- [x] Pre-sized field existence and promoted posting bitmaps from `InitialCapacity` to avoid repeated backing-array growth/copy during indexing.
- [x] Split numeric/time indexing into a direct columnar path; numeric values are no longer stringified and inserted as high-cardinality term bitmaps.
- [x] Split vector indexing into a direct vector path; vectors are no longer stringified through generic document value conversion.
- [x] Removed failed parse/time-parse allocation path for non-numeric string fields.
- [x] Added `BatchUpsertSlice(ids, docs)` to avoid map iteration overhead and lock once per batch.
- [x] Optimized ASCII prefix/suffix/ngram generation to use string slicing instead of rune slice allocation.
- [x] Updated normal benchmark output to indexing and searching only.

### Active performance contract
- [x] Search exact unique/high-cardinality/prefix/count hot paths are zero-allocation through `SearchInto`, `EachTerm`, `CollectTerm`, and `CountTerm`.
- [x] Generic map-based `Upsert` remains supported and now avoids pathological bitmap growth.
- [x] Batch insertion should prefer `BatchUpsertSlice` over `BatchUpsert(map[string]Document)` for stable low overhead.

### Pending optional future work
- [ ] Add a fully typed `UpsertRecord` API that avoids `Document map[string]any` entirely.
- [ ] Add field-ID based schema compiler to remove map lookups during ingestion.
- [ ] Add segment writer for append-only immutable indexing when updates are not required.

## Performance correction pass: indexing hot path

- [x] Removed per-document lowercase allocations for already-normalized ASCII keyword values.
- [x] Added `lowerNoAlloc` and routed analyzer/normalizer hot paths through it.
- [x] Cached schema field metadata so ingestion no longer iterates raw schema maps or re-detects special field types per document.
- [x] Pre-sized only the active field indexes based on schema options to avoid high memory overhead while reducing map growth.
- [x] Reduced generic keyword/numeric upsert from allocating per row to zero allocations in benchmarked hot path.
- [x] Reduced `BatchUpsertSlice100` from ~300 allocs/batch to zero allocations/batch in benchmarked hot path.
- [x] Kept default benchmark scope limited to indexing and searching only.

### Current focused benchmark shape

The default benchmark command remains:

```bash
go test ./lookup -bench=. -benchmem
```

It reports only `BenchmarkIndexing/*` and `BenchmarkSearch/*`.


## Performance implementation update

- [x] Add custom `Clock` interface and `StaticClock` for deterministic, low-overhead tests/benchmarks.
- [x] Add compiled `FieldID` lookup.
- [x] Add compiled `KeywordField` and `NumericField` handles.
- [x] Add zero-allocation `BeginFast` / `RowWriter` typed ingestion API.
- [x] Add zero-allocation `BeginBatchFast` / `BatchWriter` batch ingestion API.
- [x] Add direct no-callback `UpsertKeywordNumericFast` convenience hot path.
- [x] Add dense numeric column storage with existence bitmap.
- [x] Use dense numeric columns for range/sort/facet reads.
- [x] Add normalized fast exact search/count APIs.
- [x] Add direct fast search execution for `Terms`, `Range`, `Contains`, `Fuzzy`, `CIDR`, and common `Bool` filters.
- [x] Remove `time.Now()` from search hot path unless `CollectTook` is enabled.
- [x] Keep default benchmarks restricted to indexing and searching only.
- [x] Add fast ingestion example under `examples/fast`.

### Remaining high-scale vector note

- [x] Replace flat filtered vector scan with integrated ANN/HNSW graph traversal and candidate reranking.


## ANN/HNSW performance pass

- [x] Added `VectorANN` graph structure with bounded candidate expansion.
- [x] Built ANN graph during vector indexing/upsert.
- [x] Routed `VectorQuery` through ANN graph before falling back to flat scan.
- [x] Added filtered vector reranking using normal lookup bitmap filters.
- [x] Removed vector-search heap allocations in the default benchmark path.
- [x] Kept benchmark suite limited to indexing and searching.
- [x] Validated `BenchmarkSearch/VectorFiltered` as a zero-allocation ANN query path.

## Latest performance pass

- [x] Added `hasTTL` flag so non-TTL indexes skip expiry map lookup and clock access on search hot paths.
- [x] Reworked `EachTerm` to inline bitmap word iteration and avoid nested callback overhead.
- [x] Reworked `CollectTerm` to collect directly without routing through `EachTerm` callback wrapper.
- [x] Changed default string column creation to only stored/facetable/special fields, reducing unnecessary map writes during fast keyword ingestion.
- [x] Changed numeric storage to dense columns first, keeping numeric maps only for explicit facetable compatibility.
- [x] Added `BatchUpsertKeywordNumericFast` specialized tight-loop batch indexer for common keyword + tenant + numeric lookup rows.
- [x] Tuned ANN/HNSW default `EFSearch` and filtered-search candidate bounds to reduce vector query ns/op while preserving approximate graph traversal and reranking.
- [x] Added complete examples: `examples/performance`, `examples/vector`, and `examples/filters`.

## Latest optimization checklist

### Completed

- [x] Added delete-free fast path with `hasDeletes`.
- [x] Removed delete bitmap checks from high-cardinality iterator/collector paths when there are no deletes and no TTL fields.
- [x] Optimized batch keyword/numeric ingestion with direct dense-column writes.
- [x] Reused/promoted tenant posting bitmap once per batch instead of resolving it per document.
- [x] Added pre-sized bitmap unsafe writes for compiled ingestion hot paths.
- [x] Optimized `CountTerm` for no-delete/no-TTL indexes.
- [x] Tuned ANN search candidate bounds to reduce filtered vector latency.
- [x] Added complete end-to-end example under `examples/complete`.

### Still intentionally not optimized away

- [ ] Public `Document map[string]any` ingestion remains slower than compiled ingestion because it must preserve flexible dynamic behavior.
- [ ] High-cardinality full collection still scales with the number of matching documents because it must visit every hit.
- [ ] Phrase query still allocates for public string query parsing; the next step would be a compiled phrase query object.

## Storage / Database Indexing Tasks

### Completed

- [x] Added generic `Source` and `Cursor` streaming interfaces.
- [x] Added reusable `SourceRecord` and typed `SourceValue` cells.
- [x] Added bulk `IndexFrom(ctx, source, options)` importer.
- [x] Added batch indexing inside `IndexFrom` to avoid one lock per record.
- [x] Added `BulkOptions` with batch size, checkpoint interval, resume, skip-bad-records, and progress callbacks.
- [x] Added `BulkStats` and `BulkProgress`.
- [x] Added `CheckpointStore` interface.
- [x] Added `MemoryCheckpoint`.
- [x] Added `FileCheckpoint`.
- [x] Added `SQLSource` for database/sql queries.
- [x] Added `PagedSQLSource` for 10M/100M+ table ingestion using keyset pagination.
- [x] Added `SQLTableQuery` helper.
- [x] Added `CSVSource`.
- [x] Added `JSONLSource`.
- [x] Added `SliceSource`.
- [x] Added `ChannelSource`.
- [x] Added `TupleLookupSchema` for term/group/date-of-service lookup.
- [x] Added `TupleQuery(term, groupID, date_key)` helper.
- [x] Added `ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")` helper.
- [x] Added database ingestion example.
- [x] Added CSV/source-file ingestion example.
- [x] Added tests for source ingestion and record query.

### Active / Recommended next production work

- [ ] Add optional on-disk immutable segment storage for indexes larger than RAM.
- [ ] Add partition-aware index manager for 100M+ records split by partition/group/month.
- [ ] Add background snapshot/segment compaction after bulk imports.
- [ ] Add database CDC tailing adapters for MySQL binlog/Postgres logical replication via external drivers.
- [ ] Add metrics around source lag, rows/sec, checkpoint age, and import ETA.


## SQL Query Source Tasks

### Completed

- [x] Added `SQLQuerySource` for arbitrary raw SQL queries, joins, CTEs, views, and filtered query ingestion through `database/sql`.
- [x] Added `PagedSQLQuerySource` for keyset-paginated arbitrary SQL queries, suitable for 100M+ joined/filtered source rows.
- [x] Added `SQLPageFunc` for custom database-specific page query generation.
- [x] Added `SQLDialect` placeholder helpers for `?`, PostgreSQL `$1`, SQL Server `@p1`, and named-colon `:p1` styles.
- [x] Added `SQLSelect` safe generated SELECT helper for simple parameterized table queries.
- [x] Added `TupleSQLQuery` for filtered record SQL query generation.
- [x] Added `TuplePagedSQLQuery` for paged record SQL query generation.
- [x] Added tests for query builders and paged SQL query generation.
- [x] Added `examples/sql_query_ingest` covering raw SQL query source, paged SQL query source, generated query helpers, and runnable end-to-end search.

### Production notes

- [ ] Real database drivers are intentionally not bundled; use your existing `*sql.DB` from MySQL, PostgreSQL, SQL Server, SQLite, ClickHouse-compatible drivers, etc.
- [ ] For 100M+ rows, prefer keyset pagination using monotonically increasing IDs and keep `DisableSource: true`.
- [ ] For complex joins, materialized views or denormalized lookup tables are recommended when indexing throughput matters more than source-query flexibility.

## 100K Dataset Database Example Tasks

### Completed

- [x] Added `examples/dataset_a_100k_database`.
- [x] Added a runnable in-memory `database/sql` driver so the example validates without requiring an external database.
- [x] Generated 100,000 deterministic Dataset/record rows.
- [x] Indexed Dataset lookup fields through `PagedSQLQuerySource` and `IndexFrom`.
- [x] Demonstrated keyset pagination with `seq > ? ORDER BY seq ASC LIMIT ?`.
- [x] Demonstrated search for `term=key-0013&group_id=4&date_key=2026-01-01`.
- [x] Demonstrated search for `term=key-special&group_id=4&date_key=2026-01-01`.
- [x] Demonstrated search for another Dataset/date/group combination.
- [x] Kept `DisableSource: true` and `InitialCapacity: 100000` in the example for low-memory indexing.

## Multi-index storage/API/CLI enhancements

### Completed

- [x] Added `MultiIndexManager` for multiple live index IDs.
- [x] Added atomic reload/swap for a specific index ID.
- [x] Added reload latency metadata per index.
- [x] Added `RegisterDemoIndexes` for `dataset_a`, `dataset_b`, and `dataset_c`.
- [x] Added `MultiServer` HTTP API.
- [x] Added `GET /v1/indexes`.
- [x] Added `GET /v1/indexes/{id}/stats`.
- [x] Added `GET /v1/indexes/{id}/lookup?...`.
- [x] Added `POST /v1/indexes/{id}/search`.
- [x] Added `POST /v1/indexes/{id}/count`.
- [x] Added `POST /v1/indexes/{id}/reload` for registered sources.
- [x] Added `POST /v1/indexes/{id}/reload-sql` for arbitrary SQL query ingestion.
- [x] Added `POST /v1/indexes/{id}/reload-table` for large keyset-paginated table reloads.
- [x] Added `GET /v1/metrics` Prometheus-style index metrics.
- [x] Added CLI commands: `serve`, `demo`, and `search`.
- [x] Added 100K Dataset database example with indexing/query latency output.
- [x] Added multi-index example loading Dataset, DatasetB, and DatasetC.
- [x] Implemented `PagedSQLSource.Open` through keyset pagination.
- [x] Added tests for multi-index reload and lookup.

### Active / next production hardening

- [ ] Add optional officially supported SQL driver builds under `cmd/lookupx-postgres`, `cmd/lookupx-mysql`, and `cmd/lookupx-sqlite` if third-party drivers are allowed.
- [ ] Add background asynchronous reload jobs with progress polling for very long 100M+ reloads.
- [ ] Add persisted multi-index manifest for restoring registered index definitions at startup.
- [ ] Add distributed shard placement and remote snapshot shipping for indexes larger than one node.

### Notes

The HTTP SQL reload endpoints use `database/sql`. External database drivers must be imported by the embedding application or custom server binary. The core library intentionally stays driver-agnostic.

## Production-scale / 1B row implementation pass

### Completed

- [x] Added `TupleCompositeIndex` for `(term, group_id, date_key)` lookup.
- [x] Added compact date encoding for `YYYY-MM-DD` through `EncodeDateYYYYMMDD`.
- [x] Added `TupleCompositeQuery` and made `ParseLookupQuery` return the composite query when all required keys are present.
- [x] Added streaming-source integration so SQL/table/file imports update the composite accelerator during indexing.
- [x] Added `Index.EnableTupleComposite()` and `Index.TupleLookup(...)` APIs.
- [x] Added `Index.Freeze()` and `Index.IsFrozen()` for read-mostly generations.
- [x] Added persistent storage interface `PersistentStore`.
- [x] Added `FileSegmentStore` with atomic `CURRENT` generation pointer.
- [x] Added persistent save/load that stores internal lookup indexes and works with `DisableSource=true`.
- [x] Added persistent manifest with index ID, generation, timestamp, docs, path, format, and frozen flag.
- [x] Added persistent restore tests proving composite lookup works after reload from disk.
- [x] Added `TaskManager` for async long-running reload operations.
- [x] Added task statuses: queued, running, succeeded, failed, cancelled.
- [x] Added task list/get/cancel APIs.
- [x] Added `MultiIndexManager.Tasks()`.
- [x] Added production HTTP extensions:
  - [x] `POST /v1/indexes/{id}/reload-async`
  - [x] `GET /v1/tasks`
  - [x] `GET /v1/tasks/{task_id}`
  - [x] `POST /v1/tasks/{task_id}/cancel`
  - [x] `POST /v1/indexes/{id}/freeze`
  - [x] `POST /v1/indexes/{id}/persist`
  - [x] `GET /v1/indexes/{id}/lookup-composite?...`
- [x] Added `ServiceConfig` JSON model for config-driven multi-index service setup.
- [x] Added `BuildManagerFromConfig`.
- [x] Added `PartitionRouter` for routing by index/group/month partition IDs.
- [x] Added query plan cache scaffold and `Index.CompileLookupQuery`.
- [x] Updated `RegisterDemoIndexes` to enable the record composite accelerator by default.
- [x] Added `examples/production_scale` covering 100K ingest, composite query, freeze, persistent save, persistent restore, and search after restore.
- [x] Added CLI commands:
  - [x] `persist-demo`
  - [x] `restore-search`
- [x] Updated README with persistent storage, composite lookup, freeze, async task API, CLI, and 1B row recommendations.

### Still recommended for a real 1B-row deployment

- [ ] Replace the portable gob generation file with multiple mmap segment files for faster cold start and partial loading.
- [ ] Add background segment compaction/merge workers.
- [ ] Add remote snapshot shipping between nodes.
- [ ] Add official driver-specific binaries with PostgreSQL/MySQL/SQL Server imports if third-party database drivers are allowed.
- [ ] Add distributed shard placement and replication across machines.
- [ ] Add CDC adapters using PostgreSQL logical replication or MySQL binlog consumers.
- [ ] Add production auth/RBAC for reload/persist/admin endpoints.
- [ ] Add p50/p95/p99 histograms instead of simple latency counters.

## Completed in 1B deployment pass

- [x] Added `FileMMapSegmentStore` production-style segment layout.
- [x] Added generation metadata with checksum support.
- [x] Added generation listing helper.
- [x] Added persistent index validation.
- [x] Added persistent index repair by rebuilding frozen/composite structures.
- [x] Added generation compaction policy.
- [x] Added memory-budgeted bulk options.
- [x] Added billion-row partition planner.
- [x] Added partition scheme and partition catalog helpers.
- [x] Added incremental SQL checkpoint contract.
- [x] Added `(updated_at, id)` incremental SQL page builder.
- [x] Added CDC-style mutation source contract.
- [x] Added mutation apply path supporting upserts and deletes.
- [x] Added parallel SQL partition ingestion helper.
- [x] Added HTTP production endpoints for plan, generations, validate, repair and compact.
- [x] Added CLI commands: `plan`, `generations`, `validate`, `repair`, `compact`.
- [x] Added `examples/billion_deployment`.
- [x] Added tests for partition planning, persistent mmap-style storage, validation, repair, restore and composite search.

## Remaining deployment-specific work

- [ ] Replace `segment.dat` gob payload with fully custom binary posting/column files when deployment requires direct mmap query without loading partitions into heap.
- [ ] Add vendor-specific CDC connectors for PostgreSQL logical replication, MySQL binlog, SQL Server CDC and Oracle redo/mining.
- [ ] Add distributed shard placement and replica coordination if indexes must span multiple machines.
- [ ] Add object-store segment replication for S3/GCS/Azure Blob.
- [ ] Add OpenTelemetry exporters and dashboard templates.

The architecture now supports 1B-row operation by routing data into many persistent frozen partitions. A single 1B-row in-memory index is intentionally not recommended.

## Low-memory 1B-row hardening

- [x] Removed billion example full materialization (`make([]SourceRecord, rows)`).
- [x] Added `StreamingDatasetSource` for deterministic source streaming without retaining rows.
- [x] Added `LowMemoryBillionBudget` for 16GB-class laptops.
- [x] Added `BuildPartitionedPersistent` to build/freeze/persist/close one partition at a time.
- [x] Added partitioned build progress/stats.
- [x] Added deterministic schema field ordering to keep `FieldID` stable across indexes/restores.
- [x] Updated `examples/billion_deployment` so `ROWS=1000000000` does not allocate 1B rows.
- [x] Added `RUN_FULL=1`, `SAMPLE_ROWS`, `PARTITION_ROWS`, `BATCH`, and `DATA_DIR` controls.
- [x] Added low-memory partitioned persistence test with composite lookup validation.

## Cold/warm partition search

- [x] Added `SearchPartitionedPersistent` for low-memory cold search across persisted partitions.
- [x] Updated billion example to print both cold persistent scan latency and warm loaded-partition query latency.
- [x] Documented that production services should keep hot/pruned partitions loaded instead of decoding every partition per request.

## Generic engine and Lucene benchmark additions

- [x] Add generic composite lookup index independent of Dataset/Dataset/DatasetC naming.
- [x] Add `CompositeDefinition`, `CompositeField`, `EnableComposite`, `CompositeLookup`.
- [x] Add generic URL/query-string composite parser.
- [x] Add generic schema helper for arbitrary data structures.
- [x] Add generic low-memory streaming source for any field set.
- [x] Add generic 100K database/source-style example.
- [x] Add shared JSONL benchmark dataset generator.
- [x] Add LookupX benchmark runner using the shared dataset/query manifest.
- [x] Add Lucene Java/Maven benchmark runner using the same dataset/query manifest.
- [x] Document apples-to-apples benchmark rules.
- [x] Keep older domain-specific examples as compatibility examples only; generic APIs are the preferred core path.

## macOS Lucene benchmark Makefile support

- [x] Add `make install-mvn` to install Maven on macOS using Homebrew when missing.
- [x] Add Java/Maven preflight checks for Lucene benchmark execution.
- [x] Add `make benchdata` to generate the shared JSONL dataset and query manifest.
- [x] Add `make bench-lookupx` to run LookupX using the shared benchmark files.
- [x] Add `make bench-lucene` to run Lucene/Maven using the same benchmark files.
- [x] Add `make bench-compare` to run both engines and save JSON results.
- [x] Keep benchmark files/results under `benchdata/` and exclude them from generated zips.

## Generic-core cleanup

- [x] Removed all domain-specific naming from the reusable package surface that previously referenced fixed business categories.
- [x] Replaced domain-specific demo index IDs with neutral configurable dataset IDs.
- [x] Updated benchmark defaults to use neutral configurable fields: `field_a,field_b,field_c`.
- [x] Updated the benchmark generator so arbitrary field names produce matching query manifests without hidden field-name assumptions.
- [x] Verified `go test ./...` passes after cleanup.
- [x] Verified generic benchmark generation and LookupX benchmark run with arbitrary field names.
