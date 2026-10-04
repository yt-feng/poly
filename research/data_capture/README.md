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
