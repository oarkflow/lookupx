# Clear FullText vs LookupX

Run date: 2026-11-17. Both services indexed the local Clear `cleardb` data. The
Scala service was run with `sbt -batch run`. LookupX used
`config/lookupx.bcl` in disk mode, with `MYSQL_DSN` unset so the BCL's local
Clear DSN fallback was exercised. LookupX saved nine persistent generations,
then restored all nine with the DSN still unset.

## Overall results

| Measure | Scala Lucene FullText | LookupX |
| --- | ---: | ---: |
| Source rows read | 2,162,852 | 2,162,852 |
| Index build wall time | 14.26 s (service startup log) | 11.23 s (sum of per-index reload durations; excludes save time) |
| Index build CPU | 22.62 s (service JVM) | 8.99 s (sum of per-index process CPU) |
| Indexing process peak RSS | 873 MiB (service JVM) | 3,407 MiB (LookupX process, `/proc` VmHWM) |
| LookupX peak RSS with `GOGC=50 GOMEMLIMIT=2GiB` | n/a | 2,035 MiB |
| Process peak RSS after disk restore | n/a | 2,313 MiB |
| Persisted index data | 106,590,483 bytes | 234,072,101 bytes (`index.gob`, gzip) |
| CPT search, same 10 hits | 1.499 ms avg, direct DAO | 0.174 ms avg, 0.151 ms median, 0.315 ms p95, handler timer |

The earlier raw LookupX disk generation was 305,688,089 bytes. The v3 format
removes the serialized external-ID map and rebuilds it from DocID-ordered IDs
on restore; this reduced saved index data by 23.4%. The saved LookupX corpus is
still about 2.2 times the Lucene index size. LookupX's restore RSS is about 2.6
times the Scala service JVM peak, and its indexing peak is about 3.8 times that
peak. Memory and storage parity are not achieved.

LookupX's query timer surrounds search and document reconstruction but stops
before JSON encoding. The Scala figure calls the actual `FullTextDao.searchCpt`
method directly. The timings therefore compare the search paths, not full HTTP
round trips. These are one representative query and do not establish broad
query-suite performance or relevance parity.

With `GOGC=50 GOMEMLIMIT=2GiB`, LookupX indexed the same 2,162,852 rows in
11.99 seconds summed across index reloads, with 15.50 seconds summed CPU and
2,035 MiB process peak RSS. The resulting compressed index data was
234,126,941 bytes. This trades more CPU for a lower, bounded memory target;
the wall time remained close to the default run.

## Per-index results

This compares each index's source-to-index duration from the Scala startup logs
with LookupX's per-index `Reload` duration, and the saved bytes for that index.
`Speedup` is Scala duration divided by LookupX duration; above 1.0 means
LookupX was faster in that point. `Size` is LookupX bytes divided by Lucene
bytes; above 1.0 means LookupX stored more. The runs are same-day runs on the
same Clear sources, but not simultaneous, so small timing differences are
subject to machine load.

| Index | Rows | Scala index (s) | LookupX index (s) | Speedup | Lucene bytes | LookupX bytes | Size |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CPT | 1,317,251 | 5.879 | 4.007 | 1.47x | 65,024,678 | 147,856,810 | 2.27x |
| CPT-HM | 9,923 | 0.117 | 0.052 | 2.25x | 710,892 | 1,533,205 | 2.16x |
| HCPCS | 134,726 | 0.643 | 0.682 | 0.94x | 6,476,809 | 12,707,363 | 1.96x |
| DX Facility 1 | 14,516 | 0.102 | 0.130 | 0.78x | 768,942 | 1,639,932 | 2.13x |
| DX Facility 3 | 3,882 | 0.032 | 0.016 | 2.00x | 197,002 | 368,278 | 1.87x |
| ICD-10-CM | 404,500 | 1.666 | 1.306 | 1.28x | 20,582,971 | 43,998,196 | 2.14x |
| ICD-10-PCS | 143,849 | 0.726 | 0.463 | 1.57x | 6,860,261 | 16,001,973 | 2.33x |
| Special Facility | 22,367 | 1.424 | 1.668 | 0.85x | 1,059,549 | 1,634,642 | 1.54x |
| Special Professional | 111,838 | 3.325 | 2.904 | 1.14x | 4,909,274 | 8,331,702 | 1.70x |
| **Total** | **2,162,852** | **14.214** | **11.228** | **1.27x** | **106,590,483** | **234,072,101** | **2.20x** |

LookupX live-document counts are lower than source-row counts where content IDs
collide and replacement marks earlier documents deleted. The source counts
match the Scala run, but there is not yet a duplicate-sensitive, full-corpus
comparison of every query and ranking.

## Search accuracy sample

| Search point | Scala | LookupX | Latency ratio | Result comparison |
| --- | ---: | ---: | ---: | --- |
| CPT exact code `99213`, work item 42 | 1.499 ms avg | 0.174 ms avg | 8.6x faster | same 10 identities |
| CPT `fulltext` prefix `offic`, work item 42, limit 100 | 2.988 ms avg | 0.698 ms warm avg | 4.3x faster | same 100 identities, same order |

These are point-by-point search comparisons, not a statistical multi-query
benchmark. Both use 500 measured calls after warm-up. The Scala DAO is called
directly; LookupX's reported handler timer ends before JSON serialization.

Both engines were queried for CPT keyword `99213`, constrained to work item
`42`, with a 100-result limit. LookupX returned ten IDs; after removing its
content hash suffix, the identities matched all ten values returned by
`FullTextDao.searchCpt`:

```text
42:ED_PROFEE:2015-06-23:99213
42:ED_PROFEE:2015-06-23:99213P
42:ED_PROFEE:2015-06-23:99213T
42:ED_PROFEE:2015-06-23:99213G
42:ED_PROFEE:2017-01-01:99213
42:ED_PROFEE:2017-01-01:99213P
42:ED_PROFEE:2017-01-01:99213T
42:ED_PROFEE:2017-01-01:99213G
42:ED_PROFEE:2018-01-01:99213
42:ED_PROFEE:2019-01-01:99213
```

## Text pattern search sample

The text query `offic` was run as a prefix pattern against CPT `fulltext`,
restricted to work item 42, with a 100-result limit. LookupX's warm latency was
0.630 ms median and 1.205 ms p95. All 100 result identities matched in the same order.
The first LookupX query also pays to build its in-memory sorted token list;
that cold initialization was about 20 ms in this run. Prefix lookup then scans
only the matching range of sorted unique terms and unions their postings.

For text fields without configured prefix, suffix, or n-gram postings, LookupX
supports `Prefix`, `Suffix`, and `Contains` by checking a cached token
dictionary and unioning matching term postings. It does not scan stored source
documents or persist every possible token fragment. Prefix uses binary search
to find its contiguous dictionary range. Suffix and infix check the dictionary
terms, so their cost grows with the number of distinct analyzed terms in the
field. These are token-level patterns: a pattern does not match text spanning
token boundaries. Configuring derived postings can make those operations
faster at the cost of a larger index and more indexing work.

## Query scaling check

To check whether larger result sets or concurrent clients cause LookupX latency
to climb, I ran the reusable HTTP load script against the restored 1.3 million
row CPT index. Each run had 20 warm-up requests followed by 50 requests per
worker. The `engine` columns are LookupX's handler timer (search and document
reconstruction); HTTP percentiles include request/response handling and JSON.
Every request succeeded. The comparison with Scala below is suggestive only:
Scala is the direct DAO path and was measured in a separate run.

The direct DAO comparison at the same concurrency levels is:

| Search point | Concurrency | Scala avg / p95 | LookupX engine avg / p95 | Mean latency ratio |
| --- | ---: | ---: | ---: | ---: |
| Exact `99213`, work item 42 | 1 | 1.618 / 1.844 ms | 0.756 / 1.440 ms | LookupX 2.1x faster |
| Exact `99213`, work item 42 | 8 | 3.899 / 6.131 ms | 1.835 / 11.275 ms | LookupX 2.1x faster |
| `offic` prefix, work item 42 | 1 | 3.487 / 4.500 ms | 0.834 / 1.499 ms | LookupX 4.2x faster |
| `offic` prefix, work item 42 | 8 | 6.089 / 10.057 ms | 1.040 / 1.572 ms | LookupX 5.9x faster |
| Broad `o` prefix, work item 42 | 1 | 3.816 / 3.950 ms | 1.603 / 2.720 ms | LookupX 2.4x faster |
| Broad `o` prefix, work item 42 | 8 | 9.893 / 23.949 ms | 1.641 / 3.213 ms | LookupX 6.0x faster |

These are separate benchmark runs, not paired samples. The exact-query p95 at
concurrency eight is a caution: LookupX's mean stays lower, but its tail was
higher than Scala in that run. Do not infer tail-latency superiority from the
mean ratios.

| Search point | Concurrency | LookupX engine p50 | LookupX engine p95 | HTTP p95 | Throughput | Results returned / exact total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Exact `99213`, work item 42 | 1 | 0.666 ms | 1.440 ms | 2.655 ms | 518 req/s | 10 / 10 |
| Exact `99213`, work item 42 | 8 | 0.912 ms | 11.275 ms | 15.601 ms | 1,072 req/s | 10 / 10 |
| `offic` prefix, work item 42 | 1 | 0.730 ms | 1.499 ms | 4.044 ms | 292 req/s | 100 / 124 |
| `offic` prefix, work item 42 | 8 | 0.758 ms | 1.572 ms | 20.433 ms | 662 req/s | 100 / 124 |
| Broad `o` prefix, work item 42 | 1 | 1.396 ms | 2.720 ms | 5.278 ms | 238 req/s | 100 / 10,324 |
| Broad `o` prefix, work item 42 | 8 | 1.411 ms | 3.213 ms | 19.863 ms | 861 req/s | 100 / 10,324 |

The concurrency-eight exact-query p95 had a large outlier; the other two
handler p95 values stayed close to their serial values. HTTP p95 rose more,
which includes contention in the server and response path. Throughput increased
under concurrency for all three points. This small run is a scaling signal, not
a capacity limit: it has one host, one index size, and one query mix.

The broad-prefix case exposed and helped fix a correctness issue: the old fast
path could report a capped total of 101 when the page limit was 100. The current
path reports the exact total (10,324) while returning only the requested page.
Prefix bitmap caching is bounded to eight entries and 1 MiB per text field, and
updates invalidate cached entries. A cold uncached prefix still has to build its
bitmap; the load script warms the query first, so these numbers describe the
steady state.

Adding unrelated configured indexes should not make an individual search scan
those indexes: the server resolves the requested index and searches that index's
own structures. Increasing the selected index's documents and distinct terms
can increase posting and dictionary work, especially for broad suffix or
infix patterns. This run does not vary corpus size, so it cannot show how that
growth behaves or establish that LookupX stays faster at larger scales. The
next meaningful comparison is a multi-size corpus sweep with identical query
sets, measuring cold and warm latency, p50/p95, QPS, RSS, and exact ordered
result identities for both engines.

## Term-search field scope microbenchmark

The `GlobalTermBM25` benchmark on a synthetic 10,000-document index compares
the configured two-field search (`title`, `body`) with an explicit eight-field
search. The query and matching result set are the same. Medians below are across
three 3-second Go benchmark runs on the same host:

| Scope | Median latency | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Configured 2 of 8 fields | 0.801 ms | 182,119 | 109 |
| All 8 searchable fields | 0.864 ms | 182,298 | 115 |

Restricting the fields reduced median latency by about 7% and removed six
allocations in this synthetic case. The default Clear BCL selects four fields;
the description/fulltext alias is visited once for default searches. This is a
microbenchmark result, not a replacement for measuring the full Clear corpus.

## Configuration, ingestion, and persistence

- `config/lookupx.bcl` defines the nine SQL queries and their schemas. The local
  fallback DSN now targets the same Clear database/account configuration as
  the Scala service; `MYSQL_DSN` remains available for deployment overrides.
- SQL ingestion is pull-based: Squealx supplies `*sql.DB`, and `SQLSource` uses
  `QueryContext`, `Rows.Next`, and reusable `Rows.Scan` destinations. Indexing
  retains one current row plus a bounded 65,536-row bulk batch. It does not
  buffer the complete SQL result. `FetchSize` is not used by this cursor path.
- Disk mode saves compressed generations and restores them at startup. New
  generations omit duplicate alias fields and the duplicate external-ID map;
  older raw and gzip generations remain readable. Restored indexes are decoded
  into Go memory; this is durable on-disk storage, not memory mapping.
- `go test ./...` passed after the v3 persistence format change. A live disk
  indexing run with no `MYSQL_DSN` override indexed all nine sources, and a
  second process restored all nine indexes without connecting to MySQL. The
  restored CPT query returned the same ten identities.

## Remaining work

LookupX's default indexing used less CPU and wall time than the Scala run, and
its sampled exact and text-prefix searches were faster with matching results.
The 2 GiB runtime target lowers its indexing peak to about 2.0 GiB, with higher
CPU use. LookupX's RAM footprint and on-disk size remain larger than Lucene's.
Accuracy has been checked for one exact CPT query and one text-prefix query;
broader ranking, suffix/infix semantics, duplicate IDs, and result equality
across all nine indexes still need a query suite before claiming full parity.
