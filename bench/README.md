# Apple-to-Apple LookupX vs Lucene Benchmark

This harness compares LookupX and Lucene using the same generated JSONL dataset,
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

## Apples-to-apples rules

- Same dataset file.
- Same query file.
- Same fields.
- Same exact composite key query.
- Same loop count.
- Same machine/JDK/Go environment.
- Report indexing and query latency separately.

Lucene uses `StringField` and `KeywordAnalyzer` to match LookupX exact lookup
semantics. Do not compare LookupX exact lookup against Lucene full-text analyzed
queries unless you also switch LookupX to text analysis mode.
