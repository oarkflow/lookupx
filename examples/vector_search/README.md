# Complete Vector Search Example

This is the production-style vector search example for `lookupx`. It is intentionally larger than the minimal `examples/vector` demo and shows how to run vector retrieval in a real product-search workload.

It demonstrates:

- high-throughput batch indexing
- cosine ANN search
- exact exhaustive search for deterministic recall checks
- per-query `ef_search` and oversampling
- tenant/status/category filters during ANN traversal
- hybrid text + numeric + vector filtering
- metric switching: cosine, dot product, and L2
- live insert, vector update, and delete
- snapshot save and reload
- concurrent reader latency reporting
- HTTP `/search`, `/stats`, `/metrics`, `/snapshot`, and `/docs/:id`

## Run

```bash
go run ./examples/vector_search
```

Use a larger generated dataset:

```bash
go run ./examples/vector_search -n 100000
```

Start the HTTP API:

```bash
go run ./examples/vector_search -n 100000 -serve -addr :8090
```

Static sample HTTP query files are included in this folder. The example also writes refreshed ready-to-use HTTP queries to the data directory, by default:

```text
/tmp/lookupx-vector-example/query_ann.json
/tmp/lookupx-vector-example/query_exact.json
/tmp/lookupx-vector-example/query_hybrid.json
```

Call the API:

```bash
curl -s http://127.0.0.1:8090/stats
curl -s -X POST http://127.0.0.1:8090/search \
  -H 'Content-Type: application/json' \
  --data @/tmp/lookupx-vector-example/query_ann.json
curl -s -X POST http://127.0.0.1:8090/search \
  -H 'Content-Type: application/json' \
  --data @/tmp/lookupx-vector-example/query_exact.json
curl -s -X POST http://127.0.0.1:8090/search \
  -H 'Content-Type: application/json' \
  --data @/tmp/lookupx-vector-example/query_hybrid.json
```

## Vector field configuration

The example uses this vector field:

```go
"embedding": {
    Kind: lookup.FieldVector,
    Dim: 16,
    VectorMetric: "cosine",
    VectorM: 32,
    VectorEFConstruction: 256,
    VectorEFSearch: 128,
}
```

Recommended starting points:

| Workload | M | EFConstruction | EFSearch |
|---|---:|---:|---:|
| low latency | 16 | 128 | 64 |
| balanced | 32 | 256 | 128 |
| higher recall | 48 | 384 | 256 |

Use `Exact: true` in tests and quality checks. Use ANN in production queries and compare recall against exact on sampled traffic.

## HTTP vector query with filters

```json
{
  "type": "vector",
  "field": "embedding",
  "vector": [0.1, 0.2, 0.3],
  "k": 80,
  "limit": 10,
  "metric": "cosine",
  "ef_search": 128,
  "oversample": 8,
  "filter": [
    {"type": "term", "field": "tenant", "value": "orgware"},
    {"type": "term", "field": "status", "value": "active"}
  ]
}
```

The vector query filter is evaluated as an AND filter and applied during vector candidate collection, which is the path you normally want for multi-tenant production search.
