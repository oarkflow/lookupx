#!/usr/bin/env python3
"""Concurrent point-query benchmark for the live LookupX HTTP service."""

import argparse
import concurrent.futures
import http.client
import json
import statistics
import time
from urllib.parse import urlsplit


def query_for(case):
    if case == "exact-99213":
        value = "99213"
        filters = [{"type": "term", "field": "work_item_uid", "value": "42"}]
    elif case == "prefix-offic":
        value = "offic"
        filters = [{"type": "term", "field": "work_item_uid", "value": "42"}]
    elif case == "broad-prefix-o-workitem":
        value = "o"
        filters = [{"type": "term", "field": "work_item_uid", "value": "42"}]
    else:
        value = "o"
        filters = []
    should = [
        {"type": "term", "field": "code", "value": value.upper()},
        {"type": "prefix", "field": "code", "value": value},
        {"type": "prefix", "field": "code_no_period", "value": value},
        {"type": "term", "field": "fulltext", "value": value},
        {"type": "prefix", "field": "fulltext", "value": value},
    ]
    return {
        "type": "bool",
        "should": should,
        "min_should_match": 1,
        "filter": filters,
        "limit": 100,
        "with_docs": True,
    }


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8089")
    parser.add_argument("--index", default="cpt")
    parser.add_argument("--case", choices=["exact-99213", "prefix-offic", "broad-prefix-o", "broad-prefix-o-workitem"], required=True)
    parser.add_argument("--loops", type=int, default=100, help="requests per worker")
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()
    target = urlsplit(args.base)
    path = f"/v1/indexes/{args.index}/search"
    payload = json.dumps(query_for(args.case)).encode()
    internal_ns = []
    http_ns = []
    result_counts = []
    result_totals = []
    errors = []

    def worker():
        connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=120)
        samples = []
        for _ in range(args.loops):
            started = time.perf_counter_ns()
            try:
                connection.request("POST", path, payload, {"Content-Type": "application/json"})
                response = connection.getresponse()
                body = response.read()
                ended = time.perf_counter_ns()
                if response.status != 200:
                    errors.append(f"HTTP {response.status}: {body[:300]!r}")
                    continue
                result = json.loads(body)
                samples.append((result["latency_ns"], ended - started, len(result["result"]["hits"]), result["result"]["total"]))
            except Exception as exc:  # include connection resets in the benchmark result
                errors.append(repr(exc))
                connection.close()
                connection = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=120)
        connection.close()
        return samples

    # Warm the route, query dictionary and connection pool before measurement.
    args.loops = max(args.loops, 1)
    for _ in range(20):
        conn = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=120)
        conn.request("POST", path, payload, {"Content-Type": "application/json"})
        response = conn.getresponse()
        body = response.read()
        conn.close()
        if response.status != 200:
            raise SystemExit(f"warm-up failed: HTTP {response.status}: {body[:300]!r}")

    start = time.perf_counter_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for samples in pool.map(lambda _: worker(), range(args.concurrency)):
            for engine_ns, http_request_ns, hits, total in samples:
                internal_ns.append(engine_ns)
                http_ns.append(http_request_ns)
                result_counts.append(hits)
                result_totals.append(total)
    elapsed = time.perf_counter_ns() - start
    if not internal_ns:
        raise SystemExit(f"no successful requests; errors={errors[:3]}")
    print(json.dumps({
        "engine": "lookupx-http",
        "index": args.index,
        "case": args.case,
        "concurrency": args.concurrency,
        "requests": len(internal_ns),
        "errors": len(errors),
        "hits_per_response": statistics.mode(result_counts),
        "total_per_response": statistics.mode(result_totals),
        "engine_avg_ms": statistics.mean(internal_ns) / 1e6,
        "engine_p50_ms": percentile(internal_ns, .50) / 1e6,
        "engine_p95_ms": percentile(internal_ns, .95) / 1e6,
        "http_p50_ms": percentile(http_ns, .50) / 1e6,
        "http_p95_ms": percentile(http_ns, .95) / 1e6,
        "throughput_qps": len(internal_ns) * 1e9 / elapsed,
        "elapsed_s": elapsed / 1e9,
        "first_errors": errors[:3],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
