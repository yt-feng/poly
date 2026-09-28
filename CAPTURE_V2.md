# Capture v2 — explicit provenance and auditable coverage

## Deployment and scope

The legacy `polymarket_quotes.py` and its CSV files are retained unchanged for
compatibility and side-by-side validation. The new **capture-v2** workflow is an
independent production data path. A merge affecting its files starts it, and an
completion event starts a successor through `capture-v2-watchdog`. An independent
hourly watchdog schedule recovers interrupted chains. The watchdog does not
dispatch while a capture is queued or running and stops fast restart loops
(three starts in 15 minutes). Disable both workflows to intentionally stop capture.
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

Shutdown preserves `pre_shutdown_live_health` before clearing connections,
retries cancelled feed tasks with bounded waits and records stalled task
names/stacks in `shutdown.json`. Checkpoint upload threads are drained before
final publication so two uploaders cannot overwrite each other's progress. Each
capture upload command has a 60-second timeout; all final publication calls share
one 10-minute deadline, and the workflow has a 255-minute process deadline with a
30-second interrupt grace period. Failed or cancelled jobs retain a recovery
artifact. Missing historical Polymarket observations remain gaps.

Both v2 and v3 CLIs also bound interpreter cleanup to 30 seconds after the
collector finishes. Remaining tasks and asynchronous generators are diagnosed
in `process_cleanup.json`; their cancellation cannot trigger an unlimited
`asyncio.run` cleanup gather. The owned default executor is joined within the
same deadline. Any unfinished cleanup fails the CLI. If a worker never returns,
the CLI writes diagnostics, reports the failure to stderr, and exits with status
1 without entering CPython's unlimited executor join at interpreter exit.
Completed tail segments, health and quality remain available for recovery.

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

The September 26 incident illustrates why a passing short smoke test is not
enough: a four-hour sampler stopped, but task shutdown held the concurrency slot
for another hour until the job timeout. Its final snapshot segment stayed open.
Other completed runs had no prompt successor when scheduled events were delayed.
The September 26 quality report correctly failed at 75,212/86,400 observed seconds
(87.05%, longest gap 4,919 seconds); changing code cannot repair those observations.
Regression coverage now includes delayed cancellation, overlapping publisher
shutdown, repeated lifecycles, duplicate continuation events, restart loops, and
two-day midnight/gap accounting. Production acceptance still requires a finished
four-hour run, its durable final segment, the next run, and a subsequent full UTC
day passing the unchanged 95% gate.

`health.json` also exposes per-source/per-asset last-valid timestamps, errors,
connection status, and raw message counts. GitHub workflow success alone must not
be treated as proof of complete data. Monitor freshness and report coverage.

## Official protocol references

* https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
* https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
* https://github.com/binance/binance-public-data
* https://docs.polymarket.com/market-data/realtime-data


### Continuous successor handoff

The September 27 repair produced two verified four-hour archives, but capture
stopped after the second at 02:10 UTC on September 28. The completion-triggered
watchdog was absent and no hourly watchdog ran between 00:52 and the manual
06:48 recovery. A first successful successor was therefore insufficient proof
of indefinite continuity. GitHub documents a three-level `workflow_run` chain
limit and delayed/dropped scheduled events; the API history does not expose the
platform's exact reason for omitting this particular event.

The capture workflow now has a final, bounded `handoff` job which directly
uses `workflow_dispatch`. It only excludes its own workflow from the active-run
inventory after verifying the repository/branch/workflow and completed capture
job. The handoff and independent watchdog share `capture-v2-continuation`
concurrency, while `capture-v2-production` still prevents overlapping collectors.
Requested, waiting, pending, queued and running successors all prevent another
dispatch. The current run still counts towards the unchanged three starts in
15 minutes restart limit. A dispatch returns an actual run ID, and its identity
and active state are verified; ambiguous POST failures are never retried blindly.
The watchdog remains an independent recovery path for cancellation or failure
which prevents the final handoff from executing. Neither scheduling path makes
GitHub runner availability a wall-clock guarantee.

`capture-continuity-probe.yml` verifies six real direct-dispatch handoffs on an
isolated branch without running collectors, accessing market data, or publishing
archives. Its fixed six-hop/session cap uses an isolated eight-start fixture
budget because the synthetic jobs finish in seconds rather than four hours;
the production three-start budget is unchanged and separately regression-tested.
The probe checks the same identity, worker-completion, active-run and dispatch
acknowledgement logic as production. History gaps remain gaps and the daily 95%
coverage gate is unchanged.

Platform references:
- https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run
- https://docs.github.com/en/actions/how-tos/troubleshoot-workflows#scheduled-workflows-running-at-unexpected-times
- https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
