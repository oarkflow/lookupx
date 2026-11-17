#!/usr/bin/env python3
"""Generate shared CPT-like data and FullTextDao-compatible query cases."""
import argparse
import json
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/lookupx-fulltext-bench")
    ap.add_argument("--rows", type=int, default=10000)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with (out / "dataset.jsonl").open("w") as stream:
        for i in range(args.rows):
            code = f"992{i:03d}" if i < 1000 else f"{10000 + i}"
            wid = 100100 + (i % 16)
            desc = ("Established patient office visit level four" if i % 4 == 0 else
                    "New patient office visit level three" if i % 4 == 1 else
                    "Diagnostic laboratory procedure panel" if i % 4 == 2 else
                    "Therapeutic service follow up")
            row = {
                "id": f"{code}:{wid}",
                "fields": {
                    "cpt_code": code,
                    "patient_status": f"status-{i % 3}",
                    "event_dos": f"2025-{(i % 12) + 1:02d}-01",
                    "work_item_id": str(wid),
                    "client_proc_desc": desc,
                },
            }
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")
    queries = [
        {"type": "prefix", "name": "cpt-code-exact-and-prefix", "field": "cpt_code", "value": "992013", "work_item_id": "100100", "limit": 100},
        {"type": "prefix", "name": "cpt-code-prefix", "field": "cpt_code", "value": "992", "work_item_id": "100100", "limit": 100},
        {"type": "prefix", "name": "description-token-prefix", "field": "client_proc_desc", "value": "establ", "work_item_id": "100100", "limit": 100},
        {"type": "prefix", "name": "description-office-prefix", "field": "client_proc_desc", "value": "office", "work_item_id": "100100", "limit": 100},
    ]
    with (out / "queries.jsonl").open("w") as stream:
        for query in queries:
            stream.write(json.dumps(query, separators=(",", ":")) + "\n")
    print(f"wrote {args.rows} rows and {len(queries)} queries to {out}")


if __name__ == "__main__":
    main()
