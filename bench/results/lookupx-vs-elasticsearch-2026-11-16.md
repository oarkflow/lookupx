# LookupX vs Elasticsearch benchmark

Run date: 2026-11-16. These are 1M-row samples on macOS 25.5 arm64
(Go 1.27.3) with Elasticsearch 8.17.0 running as a single local Docker node
with a 1 GB JVM heap. The generated JSONL dataset was 88,175,000 bytes.

Both indexes had the same three keyword fields and a precomputed exact
composite keyword. Elasticsearch used one primary shard, zero replicas,
refresh disabled during loading, and `_source` disabled. Each query returned
up to 10 hits and ran 1,000 times. The manifest included a composite key with
about 1,800 matches and a popular term matching about 200,000 rows.

Initial sample (before warmup was added):

| Measure | LookupX | Elasticsearch |
| --- | ---: | ---: |
| Index time | 1.305 s | 12.760 s |
| Index throughput | 766K rows/s | 78.4K rows/s |
| Retained/index storage observation | 108.1 MB Go heap delta | 54.8 MB primary store |
| JVM heap after indexing | n/a | 808 MB of 1 GB |
| Composite key query | 218 ns/call | 12 μs average server `took`; 576 μs client round trip |
| Popular term query | 151 ns/call | 5 μs average server `took`; 448 μs client round trip |

## Warmed rerun

This rerun used the same dataset and settings, with 100 excluded warmup
iterations followed by 1,000 timed calls for each query.

| Measure | LookupX | Elasticsearch |
| --- | ---: | ---: |
| Index time | 1.343 s | 13.230 s |
| Index throughput | 745K rows/s | 75.6K rows/s |
| Retained/index storage observation | 108.1 MB Go heap delta | 54.7 MB primary store |
| JVM heap after indexing | n/a | 753 MB of 1 GB |
| Composite key query | 181 ns/call | 11 μs average server `took`; 627 μs client round trip |
| Popular term query | 148 ns/call | 2 μs average server `took`; 567 μs client round trip |

## Three-engine rerun

This rerun used a newly generated 100K-row dataset and the same query manifest
for LookupX, Elasticsearch 8.17.0, and Lucene 9.11.1. Each query returned 10
hits over 1,000 timed calls after 100 warmups for LookupX and Elasticsearch.
Lucene's runner does not currently exclude warmup calls.

| Measure | LookupX | Elasticsearch | Lucene |
| --- | ---: | ---: | ---: |
| Index time | 144 ms | 2.326 s | 483 ms |
| Index throughput | 694K rows/s | 43.0K rows/s | 207K rows/s |
| Retained/index storage observation | 9.9 MB Go heap delta | 5.5 MB primary store | not reported |
| JVM heap after indexing | n/a | 547 MB | n/a |
| Composite key query | 181 ns/call | 1.159 ms client / 0.09 ms server `took` | 74.8 μs/call |
| Popular term query | 148 ns/call | 681 μs client / 0.01 ms server `took` | 100.3 μs/call |

This 100K sample is a separate run from the 1M warmed comparison above. Lucene
timings include cold and warm calls because its runner lacks warmup exclusion;
compare them as directional results rather than warmed-only latency.

The indexing comparison measures LookupX's embedded JSONL stream against
Elasticsearch's local HTTP bulk API, so Elasticsearch includes bulk transport
and request handling. Query comparison uses LookupX's in-process API against
Elasticsearch's server-side `took`, which has millisecond resolution and
under-reports sub-millisecond work; Elasticsearch client time also includes
HTTP and JSON handling. The memory columns measure different things: LookupX's
retained Go heap after GC versus Elasticsearch's on-disk primary store and
whole-node JVM heap. They are useful sizing signals, not a direct RAM-to-RAM
comparison. The samples show Elasticsearch throughput and query timing varying
with JVM/segment state. The host model and CPU count were not captured, so these
numbers should be treated as a local baseline, not a universal ratio.

Reproduce with:

```bash
make benchdata ROWS=1000000 FIELDS=term,group_id,date_key BENCH_DIR=/tmp/lookupx-bench-1m
make bench-lookupx ROWS=1000000 LOOPS=1000 FIELDS=term,group_id,date_key BENCH_DIR=/tmp/lookupx-bench-1m
make bench-elasticsearch ROWS=1000000 LOOPS=1000 FIELDS=term,group_id,date_key BENCH_DIR=/tmp/lookupx-bench-1m ELASTIC_URL=http://localhost:9200
```
