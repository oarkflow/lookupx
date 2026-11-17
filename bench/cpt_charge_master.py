#!/usr/bin/env python3
"""Create shared exact/prefix/text/range/page inputs from the synthetic CPT fixture."""
import json
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "examples/charge_master_ingest/data/charge_master_cpt_10k.json"
FIELDS = ["cpt_code", "patient_status", "event_dos", "work_item_id", "client_proc_desc"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="/private/tmp/lookupx-cpt-bench", help="directory for shared JSONL data and query manifest")
    out = Path(parser.parse_args().out)
    rows = json.loads(FIXTURE.read_text())
    out.mkdir(parents=True, exist_ok=True)
    with (out / "dataset.jsonl").open("w") as stream:
        for row in rows:
            fields = {name: str(row[name]).lower() for name in FIELDS}
            stream.write(json.dumps({"id": row["charge_master_uid"], "fields": fields}, separators=(",", ":")) + "\n")
    queries = [
        {"type": "term", "name": "exact-cpt-99213", "field": "cpt_code", "value": "99213", "limit": 10},
        {"type": "prefix", "name": "cpt-prefix-992", "field": "cpt_code", "value": "992", "limit": 10},
        {"type": "contains", "name": "cpt-contains-213", "field": "cpt_code", "value": "213", "limit": 10},
        {"type": "phrase", "name": "description-phrase-office-visit", "field": "client_proc_desc", "value": "office visit", "limit": 10},
        {"type": "text", "name": "description-text-established-patient", "field": "client_proc_desc", "value": "established patient", "limit": 10},
        {"type": "range", "name": "work-item-range", "field": "work_item_id", "gte": "100100", "lte": "100199", "limit": 10},
        {"type": "range", "name": "event-dos-range", "field": "event_dos", "gte": "2025-01-01", "lte": "2025-03-31", "limit": 10},
        {"type": "page", "name": "offset-page-cpt-99213", "field": "cpt_code", "value": "99213", "offset": 100, "limit": 10},
    ]
    with (out / "queries.jsonl").open("w") as stream:
        for query in queries:
            stream.write(json.dumps(query, separators=(",", ":")) + "\n")
    print(f"wrote {len(rows)} rows and {len(queries)} queries to {out}")


if __name__ == "__main__":
    main()
