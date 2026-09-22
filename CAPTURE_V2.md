# Capture v2 — explicit provenance and auditable coverage

## Deployment and scope

The legacy `polymarket_quotes.py` and its CSV files are retained unchanged for
compatibility and side-by-side validation. The new **capture-v2** workflow is an
independent production data path. A merge affecting its files starts it, and an
hourly scheduled trigger keeps a pending successor behind the four-hour run.
Concurrency prevents overlapping v2 production runs. GitHub scheduling and runner
handoffs still create possible gaps; this is not an exchange-grade zero-loss SLA.

Default LIVE scope: **Polymarket BTC five-minute Up/Down + Binance BTCUSDT spot
+ BTC/USD Chainlink through Polymarket RTDS**. Set repository variable
`CAPTURE_ASSETS=btc,eth,sol` to opt into the supported additional live assets.
Default HISTORICAL scope: **BTCUSDT spot, 2026-04-21 through yesterday UTC**.
Set `BINANCE_BACKFILL_START` and `BINANCE_BACKFILL_SYMBOLS` to expand the explicit
historical scope, including earlier dates. This deployment does not silently
subscribe to every Binance symbol, all futures streams, or all Polymarket markets.

## What is captured

* Polymarket: unmodified public WebSocket frames (including book, price_change,
  last_trade_price and lifecycle events), metadata, and parallel REST full-book
  responses for both outcomes. Current/next windows are discovered in advance;
  the previous window stays subscribed briefly for resolution notifications.
* Binance spot: independent `trade`, `aggTrade`, `bookTicker`, `depth@100ms`,
  `kline_1s`, `kline_1m` streams. REST depth snapshots (up to 5,000 levels per side)
  are saved every minute. Original IDs and connection boundaries are preserved.
  Depth sequence gaps within each connection are recorded as `depth_gap` events.
* Chainlink: the official Polymarket RTDS `crypto_prices_chainlink` topic,
  separately labelled. This is not an authenticated direct Chainlink subscription.
* Snapshots: a monotonic, fixed-phase one-second scheduler reads independent
  caches. Network latency is not added as an extra one-second sleep. Missed
  deadlines are logged and skipped, never synthesized or burst-filled.

A REST poll may finish later than its target. Cached observations therefore keep
**received_ms, event_ms and validity flags**; a sampled row is not proof of a new
exchange event. Binance `bookTicker` does not always supply an event timestamp;
that field remains null. Sources never overwrite each other's prices.

## Price and execution semantics

`official_price_to_beat` and `official_resolution_price` remain null unless a
future verified resolution importer populates them. Binance prices, rounded
opens, and the first observed Chainlink value are **not** substituted into these
fields. Raw Gamma/lifecycle responses are retained as evidence for future joins.
Use each snapshot's actual `sample_ms` and source receipt timestamps for causal
joins; do not turn a later observation into a window-start price. Empty books
have null best prices. Five-level depth is sorted from the best price outwards.

Raw trade notification counts are not asserted to be a complete trade tape.
A local depth replay must bridge REST `lastUpdateId` to WebSocket `U/u`, reject
sequence gaps, and respect reconnect boundaries. No reconstructed full-depth
order book is asserted by this version. Previously unrecorded Polymarket depth
cannot be recovered from candlesticks. A previous-window subscription is not
a guarantee of receiving delayed final resolution; a historical resolution
reconciliation importer remains a separate extension.

## Storage and retrieval

High-volume data is **not committed to main**. Closed gzip JSONL segments,
SHA-256 sidecars, manifest, health, and quality reports are uploaded to immutable
run-specific GitHub Release locations:

* `capture-v2-<run_id>-<attempt>`: `raw-*.jsonl.gz`, `snapshots-*.jsonl.gz`,
  their `.sha256` files, `manifest.json`, `health.json`, `quality.json/.md`.
* `binance-v2-spot-YYYY-MM`: original verified Binance daily ZIPs and checksums.
* `binance-v2-backfill-state`: durable `backfill-state.json` and backlog summary.
* `quality-v2-YYYY-MM-DD`: daily UTC completeness report.

Segments rotate every 15 minutes or 64 MiB uncompressed, and at UTC midnight.
Publication runs separately from the sampling loop. A hard runner termination
can lose the unclosed tail; failed jobs also upload a seven-day recovery artifact.
Artifacts are emergency diagnostics, not the permanent archive. Release storage
is a bootstrap destination, **not** an unlimited all-symbol data lake. Before
large-scale expansion, add object storage and resource budgets. Neither existing
CSV history nor existing releases are deleted by these workflows.

## Historical backfill

Every hour the bounded job resumes original `trades`, `aggTrades`, `1s` and `1m`
kline archives, prioritizing the latest completed day. A run downloads at most
24 files and 1 GiB, with at most 48 attempts. Checkpoints advance only after SHA-256
verification and, when publishing, successful archive upload. A 404 is explicitly
unavailable, retried later; over-budget files remain gaps. Other HTTP failures,
including permission/rate/region restrictions, fail instead of switching hosts.

Original timestamp units are preserved. Spot archives from 2025-01-01 use
microseconds; real-time frames are normalized separately. The backlog summary
contains the requested file count, completed/pending/unavailable counts, and an
`all_requested_files_complete` flag. Deployment does not mean the backlog has
already finished downloading. Use explicit symbols; delisted-symbol discovery
and a full Binance exchange-wide historical universe are not implemented.

CLI examples (run from repository root):

```bash
pip install -r requirements-v2.txt
python capture_v2.py --assets btc --seconds 90 --output smoke --require-core
python binance_backfill_v2.py --start 2026-04-21 --symbols BTCUSDT --publish
# Optional historical futures scope; 1s futures klines are deliberately rejected.
python binance_backfill_v2.py --market futures/um --datasets trades,aggTrades,klines_1m --publish
python quality_v2.py --root smoke --output smoke-report.json
python -m unittest discover -s tests -p 'test_capture_v2.py' -v
```

## Daily quality and operational interpretation

At 05:37 UTC (13:37 Beijing/Singapore) the daily job downloads completed snapshot
assets for the prior UTC date, verifies checksums, and reports observed seconds,
24-hour coverage, the longest gap including day boundaries, exact duplicate rows,
valid Poly/Binance/Chainlink counts, and maximum scheduling lag. Core Poly or
Binance coverage below 95% fails the quality job **after** publishing its report.
A partial deployment day is expected to show partial coverage. Chainlink validity
is separately visible; core coverage does not establish Chainlink completeness.
The report audits sampled snapshots, not every exchange event or every sequence.

`health.json` also exposes per-source/per-asset last-valid timestamps, errors,
connection status, and raw message counts. GitHub workflow success alone must not
be treated as proof of complete data. Monitor freshness and report coverage.

## Official protocol references

* https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
* https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
* https://github.com/binance/binance-public-data
* https://docs.polymarket.com/market-data/realtime-data
