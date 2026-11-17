# Synthetic 10K charge master fixture

`charge_master_cpt_10k.json` contains 10,000 deterministic synthetic charge master rows for LookupX indexing and UI demonstrations. It includes `event_dos` as ISO `YYYY-MM-DD` dates and `work_item_id` as JSON integers, plus CPT-style codes, descriptions, billing attributes, and amounts.

Regenerate it with:

```sh
python3 examples/charge_master_ingest/generate_10k.py
```

Start LookupX and import it into an index named `charge_master_10k`:

```sh
go run ./cmd/lookupx serve -addr :8089 -web ./web -storage memory
curl -X POST http://localhost:8089/v1/indexes/charge_master_10k/reload-file \
  -F file=@examples/charge_master_ingest/data/charge_master_cpt_10k.json \
  -F format=json -F id_field=charge_master_uid -F batch_size=512
```

Example exact CPT search:

```sh
curl -X POST http://localhost:8089/v1/indexes/charge_master_10k/search \
  -H 'Content-Type: application/json' \
  -d '{"type":"term","field":"cpt_code","value":"99213","limit":20}'
```

CPT-style codes and descriptions are illustrative demo values, not an official CPT catalog or billing guidance.
