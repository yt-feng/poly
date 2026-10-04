# poly data-capture experiments

Use this directory for reproducible capture and data-quality work only.

- `experiments/` — immutable JSON registrations before a run starts.
- `runs/` — one machine-readable manifest per completed run.
- `private/` — local-only encrypted archives; plaintext is ignored.
- `schema/` — schemas and validation notes.

The canonical sources are the capture scripts and public Polymarket/Binance
feeds described in `README.md` and `CAPTURE_V2.md`. A run must identify exact
source URLs or release tags, commit SHA, UTC dates, window IDs, sampling gaps,
and output SHA-256 values. Do not mix this BTC capture data with `equity_daily`.

## v3 observation output contract

New capture-to-research transforms must emit JSONL records conforming to
`schema/v3_observation.schema.json`. Validate locally with:

```bash
python schema/v3_contract.py \
  --input observations-v3.jsonl \
  --output /tmp/v3-contract-report.json
```

Rows without source event time, receive time, market/condition/token identity,
book level, fees, tick/minimum-order rules, or provenance are quarantined. The
validator also reports latency percentiles and rejects future-label fields.
Historical CSV archives are not upgraded in place.
