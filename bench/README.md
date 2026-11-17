# Apple-to-Apple LookupX, Lucene, and Elasticsearch Benchmark

This harness compares LookupX, Lucene, and Elasticsearch using the same generated JSONL dataset,
the same query manifest, the same field list, and the same limit/loop count.

It intentionally avoids Dataset/Dataset/DatasetC hardcoding. Field names are provided by
flags. The default fields only mirror the user's example shape:
`term,group_id,date_key`.

## Generate dataset

```bash
go run ./cmd/benchgen -rows 100000 -out /tmp/lookupx-bench -fields term,group_id,date_key
```

## Run LookupX

```bash
go run ./cmd/benchlookupx \
  -data /tmp/lookupx-bench/dataset.jsonl \
  -queries /tmp/lookupx-bench/queries.jsonl \
  -fields term,group_id,date_key \
  -loops 10000
```

## Run Lucene

Requires Java 17+ and Maven.

```bash
cd bench/lucene
mvn -q compile exec:java -Dexec.args="--data /tmp/lookupx-bench/dataset.jsonl --queries /tmp/lookupx-bench/queries.jsonl --fields term,group_id,date_key --loops 10000"
```

## Run Elasticsearch

Run against an Elasticsearch 8.x/9.x endpoint with permission to replace the
benchmark index. The runner disables replicas and automatic refresh during
bulk loading, refreshes once, then measures the same exact composite-key and
popular single-field queries.

```bash
make bench-elasticsearch ELASTIC_URL=http://localhost:9200 ROWS=100000 LOOPS=1000
```

Run LookupX and Elasticsearch sequentially on the same generated dataset with:

```bash
make bench-compare-elasticsearch ELASTIC_URL=http://localhost:9200 ROWS=1000000 LOOPS=1000
```

For API key authentication, set `ELASTIC_API_KEY`; results are written to
`benchdata/elasticsearch-result.json`. The runner uses the same generated data
and query manifest and reports load time, store/heap use, and query timings.

## Apples-to-apples rules

- Same dataset file.
- Same query file.
- Same fields.
- Same query manifest and result page size.
- Same loop count.
- Same machine/JDK/Go environment.
- Report indexing and query latency separately.

Lucene uses `StringField` and `KeywordAnalyzer` to match LookupX exact lookup
semantics. Do not compare LookupX exact lookup against Lucene full-text analyzed
queries unless you also switch LookupX to text analysis mode.

Elasticsearch is also queried through a precomputed exact composite keyword
field, with `_source` disabled to align with LookupX's `DisableSource` mode.
The manifest exercises an exact composite key and a popular term. Elasticsearch
`took` has millisecond precision and can round sub-millisecond work to zero;
`avg_client_nanos` also includes local HTTP and JSON overhead. LookupX's
`avg_nanos` is in-process. Index timing includes Elasticsearch bulk HTTP
transport, while LookupX reads the JSONL stream in-process. LookupX reports
retained Go heap after GC; Elasticsearch reports primary store bytes and JVM
heap. Those memory values measure different layers and are not directly equal.
Both runners exclude 100 warmup queries before timing the configured loop count.

A recorded 1M-row local baseline and its hardware/configuration limitations are
in [`results/lookupx-vs-elasticsearch-2026-11-16.md`](results/lookupx-vs-elasticsearch-2026-11-16.md).

## Charge master query-type benchmark

`python3 bench/cpt_charge_master.py --out /tmp/lookupx-cpt-bench` projects the synthetic 10K fixture into the shared runner format and creates a common manifest for exact, prefix, substring, phrase, full-text, numeric range, date range, and offset-page searches. The three runners accept these query types and use the same input/manifest. A three-run report, raw results, setup, and caveats are in [`results/charge-master-10k-2026-11-16`](results/charge-master-10k-2026-11-16/README.md).
