# Partition serving verification — 2026-11-20

Measured locally with `go run ./cmd/lookupx serve -config config/lookupx.bcl`
on Linux amd64, an Intel i9-13900K, and the configured Clear SQL sources.
The final test server is listening on port 8089. Logs are at
`/tmp/lookupx-partition-server.log`. These are local measurements, not a latency
SLA or a billion-row benchmark.

## Changes

- Closing an index removes its global extras registration, allowing its postings
  to be collected. Previously every closed build/query partition stayed reachable.
- Empty routing indexes reserve no rows. Build reservations cannot exceed a
  partition's row limit; temporary schema indexes do not reserve source capacity.
- New snapshots memory map postings, numeric arrays, and string payloads. Repeated
  strings share one stored payload per partition. Old gob/gzip files still load.
- A shared 256 MiB metadata cache uses leases to protect active mappings. Results
  own their strings before leases are released. Cold opens share notifications
  and perform disk I/O outside the cache lock.
- Exact-term Bloom summaries conservatively prune impossible partitions. Parsed
  catalogs are cached and atomic replacements become visible on the next request.
- Schema fingerprints are calculated once per request. Selective fulltext terms
  can be planned before broad tenant/date filters. At most two partition search
  slots are active across concurrent requests.
- Latest-date rules compare groups across candidate partitions before paging.
  A publication/cleanup race retries a missing old generation against the new
  catalog. Cleanup preserves published and resumable generations.
- After startup builds finish, one collection returns temporary build heap pages
  to the OS. There is no forced collection on each query or partition.
- `/v1/metrics` includes heap usage and cache budget, bytes, entries, hits/misses.
  `LOOKUPX_PARTITION_CACHE_BYTES` controls retained metadata, not total RSS.

## Live results

All nine indexes completed without skipped rows or source retries. Summed source
build times were **29.58 seconds** for **3,614,829 indexed rows**:

| Index | Rows | Partitions |
| --- | ---: | ---: |
| cpt | 1,451,977 | 9 |
| hcpcs | 1,451,977 | 9 |
| cpt-hm | 9,923 | 1 |
| dx-fac1 | 14,516 | 1 |
| dx-fac3 | 3,882 | 1 |
| dx-icd10 | 404,500 | 3 |
| dx-icd10-pcs | 143,849 | 1 |
| special-fac | 22,367 | 1 |
| special-pro | 111,838 | 1 |

The requested curl used:

```bash
curl -H "X-API-Key: $LOOKUPX_API_KEY" \
  'localhost:8089/v1/indexes/cpt/lookup?term=100&work_item_uid=37&dos=2016-01-01&charge_type=ED_FACILITY&limit=25'
```

It returns one record: code `15004`, description `ER PREP OTHER 1ST 100/1%`,
effective date `2019-01-01`. `term` searches tokens across configured searchable
fields; `100` matches the description. Changing `term` to `10060` returns the
expected code `10060` with effective date `2019-01-01`.

Checks passed for compact GET responses, exact code and field filters, no-match
filters, POST boolean search, POST count, and a POST date range starting at
`2018-01-01`. The ungrouped code search/count returns three dated records; the
latest-date GET rule returns only the newest record. Queries also succeeded
while background indexing was active.

After all indexing completed, the final warm sample was:

| Measurement | Result |
| --- | ---: |
| Server execution, median of 30 requests | 910 µs |
| Server execution, minimum / maximum | 761 µs / 1.58 ms |
| HTTP time, median of 30 requests | 2.36 ms |
| 200 mixed lookups, concurrency 4 | 778 requests/s |
| Concurrent HTTP p50 / p95 | 4.82 ms / 7.62 ms |
| Process RSS after warming CPT | 740 MiB |
| Sampled idle CPU over five seconds | 0% of one core |
| Final sampled Go heap after the load | 280 MiB |
| Retained metadata accounting / budget | 232 MiB / 256 MiB |

A previous final-build sample before warming any queries settled at 39 MiB RSS
and 3.3 MiB Go heap. The observed RSS peak during a sampled window with indexing
and live queries together was 987 MiB. Those are observations, not hard memory
limits. File-backed pages and active build/query workspaces are additional to
the cache budget. RSS can remain above live heap until the runtime releases pages.

Cold opens in the final-server runs took approximately 0.33–0.48 seconds.
The first request after publication may load new generation metadata again.
Warm measurements varied between runs on this shared machine; they do not imply
that every request completes below one millisecond. Bulk indexing takes seconds.
Mapped files occupy about 1.4 GiB for these sources and are larger than the older
compressed snapshots. The new format trades compression for direct reads.

## Checks

Build and `git diff --check` passed. Regression tests passed with the race
detector for mapped columns, mutable restore, truncation/schema rejection,
legacy snapshots, concurrent searches, cancellation, pruning, global latest-date
selection, atomic catalog replacement, leases across eviction, routing capacity,
and published/resumable partition cleanup:

```bash
go test -race ./cmd/lookupx ./pkg \
  -run 'Test(ClosedIndex|Mapped|PartitionLatest|PartitionSummary|ServingCatalog|LegacyCompressed|BCLRouting|BuildPartitioned|BuildFromBCL|PartitionLease|CleanupPreserves)' -count=1
```

A synthetic three-partition benchmark with 15,000 records measured about
30.0 ms/query without retention versus 303 µs/query with retention. It is a small
mechanism benchmark, not a substitute for the real-source results above.

The full `go test ./...` suite is not green: existing HTTP fixtures make requests
without credentials and receive 401 responses; one XLSX HTTP fixture times out.
The storage tests and authenticated live GET/POST checks above passed. The
production authentication policy was not weakened to accommodate those fixtures.

Field sorting, facets, and `after_doc_id` remain explicitly unsupported on the
partition route. Broad queries can visit every partition; Bloom filters do not
route common values to one partition. A working set exceeding the cache can incur
repeated cold opens. Partition-local BM25 scores are not globally calibrated.
These measurements do not establish performance for a billion-row dataset.

## Follow-up: term `120` and unfiltered browsing

The broad-query regression was reproduced against the local configured sources:
CPT `term=120` took 375 ms median and opened 84 partitions over ten requests;
HCPCS took 313 ms and opened 83. The working set repeatedly evicted its own
partitions. Unfiltered browsing also opened every search partition unnecessarily.

Serving now keeps four-byte offsets for stored string columns and external IDs,
materializing string views only when accessed. Retention accounting reflects
the compact columns and dictionary metadata. A direct retained-heap probe of all
nine CPT partitions measured approximately 121 MB; their final cache charge is
131,172,896 bytes. The budget remains 268,435,456 bytes (256 MiB).

The updated server was run with the provided BCL and authenticated requests on
2026-11-20. After all nine indexes completed (3,614,829 input rows; approximately
32 seconds summed build time), the measurements were:

| Request / measurement | Observed result |
| --- | ---: |
| CPT `term=120`, first server execution | 165 ms |
| HCPCS `term=120`, first server execution | 176 ms |
| CPT warm median, two samples of nine requests | 322 / 431 µs |
| HCPCS warm median, two samples of nine requests | 328 µs / 1.55 ms |
| CPT revisit after HCPCS, server execution | 444 µs / 1.42 ms |
| Partition loads after warming both indexes, 20 requests | 0 |
| Retained entries / cache charge | 18 / 250.2 MiB |
| Unfiltered CPT / HCPCS warm execution median | 83 / 127 µs |
| Unfiltered first-page search partition loads | 0 |

Both term searches returned 25 records with a verbose total of 84. Warm samples
varied with scheduling on this shared machine. Cold dictionary loads remain much
slower; these results do not promise microsecond latency for every request.
Twenty curl requests to the compact CPT `term=120&limit=25` endpoint returned
25 plain records each, with 2.35 ms median HTTP time (3.79 ms maximum).

Unfiltered pages use bounded previews from new builds, with a fallback that opens
only partitions needed for the page. Live checks passed for totals, GET/POST
offset pagination, compact responses, exact field filters, missing values,
latest-date selection, POST boolean search, date ranges, and count. The filtered
request from the original report measured 979 µs median server execution over
30 requests. A 200-request run at concurrency four measured 665 requests/s and
8.83 ms HTTP p95, with no further partition loads.

After warming both full indexes and running those checks, process RSS was
approximately 1.14 GiB: 300 MiB anonymous memory and 853 MiB file-backed pages.
Reported Go heap was 292 MiB; idle sampled CPU was 0% over five seconds. These
are observations, not resource limits. Mapped file pages contribute to RSS and
can be reclaimed by the OS. Broader working sets can still exceed the budget.

Focused mapped/partition tests passed with the race detector; `go build ./...`
and `git diff --check` passed. The full-suite limitations described above remain.
