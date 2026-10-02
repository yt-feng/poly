# Equity / index / commodity daily raw-data capture

Independent acquisition module in **yt-feng/poly**. It does not import, change,
start or stop the BTC five-minute collectors, `capture_v2`/`capture_v3`, their
release tags, their quality jobs, or anything in `yt-feng/poly_trade`.
It does not access trading accounts, place orders or evaluate strategies.

## Acquisition scope

| Stream | What is retained | Resolution / limitations |
|---|---|---|
| Polymarket market WebSocket | Original text/binary payload of every received frame; both outcome tokens; full books, price/size changes, best bid/ask, last-trade messages, tick changes, new-market and resolution messages, including unknown fields | Native received messages, no sampling or rounding. Not a guarantee of all exchange events; outages and missing tokens are reported. |
| Polymarket REST books | Full book responses and hashes for both outcomes | Every 60 seconds, plus resnapshot requests on WS subscription/reconnection. Does not turn a missing WS interval into complete tick history. |
| Gamma catalog and rules | Original listing responses, versioned accepted event/market objects, IDs, descriptions, resolution source, dates, outcome-token pairs, liquidity/volume/fee information returned by Gamma | Active discovery every 300 seconds; recent closed discovery every sixth cycle; default end-date window is previous 7 / next 8 days. Unknown or malformed candidates remain auditable. |
| Polymarket trades / analytics | Paginated public trade responses, overlap retained, open interest, resolution progress, CLOB details including constraints/fees when returned | V2 API with documented legacy fallback for 404/405 where applicable. Pagination truncation is explicit, never labeled all-time complete. |
| Polymarket price history | Original sampled history response | Request 60-second buckets; legacy fidelity=1 minute. This is a BACKFILL, not historic full-depth/tick replay. |
| Yahoo WebSocket | Original base64/protobuf wrapper plus separately decoded available fields: price, native time, currency/exchange, quote type, market-hours flag, daily volume/high/low/change, previous close/open, bid/ask/sizes, last size and other supplied fields | **Unofficial provider quote updates, not exchange executions or NBBO.** Missing fields stay missing. Delay and throttling are not assumed away. |
| Yahoo intraday chart | Entire unmodified response: timestamps, OHLCV arrays, metadata, trading periods, timezone, corporate-action and adjusted-close fields when supplied | 1-minute bars, include pre/post-market. First request 5d, then 1d every 60s. Revisions are preserved rather than overwriting old observations. Availability is provider-dependent. |
| Yahoo daily reference | Entire 3-month daily chart response including available dividends and splits | Initially and hourly; raw OHLC are not silently replaced by adjusted close. |
| Optional Alpaca | Native entitled-feed trades/quotes/bars/updatedBars/dailyBars, automatic corrections and cancel/error messages, SIP status/LULD where entitled; raw snapshots | Requires credentials. Default IEX means IEX-only, **not full US market**. SIP requires entitlement; delayed_sip is separate. No credentials means this stream is not running. |

All requested finance observations use `family=equity_daily`. The broad discovery
transport may naturally contain rejected/non-financial event metadata; it is not
a BTC price feed and must never be treated as an accepted research universe.
`catalog` records define the accepted universe. Only their two outcome tokens are
subscribed to. Newly listed ticker-bearing daily events do not require editing a
fixed stock list. Unmapped financial names are retained but flagged; unfamiliar
title formats can still require a classifier update. `pagination_complete` means
only that the requested listing was exhausted, not that parser coverage is proven.

## Identity and time alignment

Use the tuple `(family, market_id, condition_id, token_id)` plus catalog version.
Ticker alone is not a market key: one stock can have several dates and strikes.
Outcome labels and token IDs are correlated by their original array positions;
YES is not assumed to be index zero. End dates are kept in UTC, while exact
exchange trading dates, closing-price definitions, comparison periods, tie rules,
holidays and corporate actions must be read from the preserved rules/metadata.

NDX is mapped to the index reference, **not QQQ**. HSI is an index, not an equity.
GC/SI fallback symbols are explicitly labeled continuous-futures proxies; they
are not silently equated to a specific futures contract or spot settlement.
A symbol explicitly linked in market rules takes precedence, with provenance.
No underlying series is automatically declared the official settlement oracle.
Indices may have no meaningful transaction volume; missing values are not zero.

Every record contains a UTC receipt timestamp in nanoseconds, a monotonic clock,
a run ID and local sequence. WS records also have a connection ID and sequence;
these sequences are collector counters, not exchange sequence numbers. Native
source timestamps and precision remain in the untouched payload. HTTP rows include
request start time, status, params and a small safe response-header allowlist;
request authorization headers and authentication messages are not archived.

For later point-in-time research, require `received_at_ns <= decision_time` and
respect each provider's source time/delay/bar completion. A historical response
retrieved today was not necessarily knowable at its bar timestamp. Keep later
bar revisions and trade corrections separate. Daily cumulative volume changes
are not individual trades. Bid, ask, last price and midpoint are different data.
Deduplicate overlapping trade pages downstream using stable provider identifiers
where available; do not discard raw receipts or double-count maker/taker views.

## Storage and recovery

Code/tests/docs live under `equity_daily/`. Workflow:
`.github/workflows/equity-daily.yml`. Concurrency group:
`equity-daily-raw-production`. Default output: `equity_daily_output/`.
The collector refuses a non-equity-named or nonempty output directory.

Release namespace: `equity-daily-v1-<run_id>-<run_attempt>`.
No raw data is committed to code history. Gzip JSONL segments are closed at
checkpoints / ~32 MiB uncompressed / UTC day changes. Each segment has a SHA256
sidecar, a source name and receipt-time boundaries in the manifest. Underlying
segments can additionally be encrypted. A rolling bundle is a tar containing
closed segments, sidecars and immutable manifest snapshots; bundles are capped
around 128 MiB of member data. Each bundle gets its own SHA256. Bundling limits
GitHub asset-count growth. Partial files are excluded. Publication retries use
the same immutable bytes, verify asset sizes and GitHub digests when supplied,
and retain local recovery copies. Manifests include per-token/symbol coverage,
HTTP statuses, disconnects, missing streams and provider limitations.

Scheduled captures run every four hours with a four-hour acquisition duration.
A main-branch module change also starts a capture. One equity capture runs at a
time; it does not share concurrency with BTC. GitHub scheduling, runner startup,
queues, network failures and provider throttles can leave gaps. This is **not a
zero-gap 24/7 market-data service**. For an independently supervised long-running
host use `--seconds 0`. Raw tick history before deployment or during disconnections
cannot be reconstructed by interpolating bars or polling history endpoints.
No automatic alerts/agent task or live trading is created by this module.

Health/catalog/manifest artifacts have seven-day retention as convenience copies;
Releases are the intended durable store. GitHub storage/service limits still
apply; this is not a promise of unlimited archival capacity. Review growth and
mirror immutable bundles to object storage when scale requires it.

## Run and test

```bash
python -m pip install -r equity_daily/requirements.txt
python -m unittest discover -s equity_daily/tests -p 'test_*.py' -v
python -m equity_daily.collector --seconds 180 --output equity_daily_smoke
# Production publishing (gh CLI authenticated; GH_REPO=yt-feng/poly):
python -m equity_daily.collector --seconds 14400 --output equity_daily_run_001 \
  --checkpoint-seconds 300 --release equity-daily-v1-manual-001
```

Decode a verified, unencrypted raw segment:

```python
import base64, gzip, hashlib, json
with gzip.open('equity_daily-polymarket_ws-....jsonl.gz', 'rt') as f:
    for line in f:
        envelope = json.loads(line)
        raw = base64.b64decode(envelope['payload_b64'])
        assert hashlib.sha256(raw).hexdigest() == envelope['payload_sha256']
        # raw is the original UTF-8 WebSocket text or binary/HTTP entity bytes.
```

Before extracting a bundle, verify its sidecar. Use safe tar extraction (for
example Python `tarfile.extractall(filter='data')` on a supporting version), then
verify segment SHA256 against the manifest. Retrieve earlier bundles too when a
later cumulative manifest references files from earlier checkpoints.

## Optional credentials and publication rights

No API key is required for the default Polymarket/Yahoo paths. Yahoo is unofficial
and may reject cloud IPs or delay/throttle/stop a feed. A stream request is not
proof of receipt; inspect `health.json`. The Yahoo transport/protobuf implementation
is based on the published yfinance protocol described in the references below.

Repository secrets (only this workflow uses these names):
`EQUITY_ALPACA_KEY_ID`, `EQUITY_ALPACA_SECRET_KEY`, and optional
`EQUITY_ARCHIVE_KEY` (a generated Fernet key, not a short PIN).
Variables: `EQUITY_ALPACA_FEED` (`iex`, `sip`, `delayed_sip`) and optional
`EQUITY_ALPACA_SYMBOLS` (comma-separated explicit subset). Keep a private backup
of the encryption key; do not commit it. To generate one locally:

```bash
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

The repository and its Releases are public. Without `EQUITY_ARCHIVE_KEY`, default
public-source data bundles are plaintext. Confirm provider terms and your rights
to collect/store/redistribute each source; public accessibility is not a data
redistribution license. Set the encryption secret before enabling a licensed feed.
The Alpaca publisher is blocked without encryption unless the operator explicitly
sets `EQUITY_ALPACA_PUBLIC_REDISTRIBUTION=true` after confirming redistribution
rights. Encryption does not itself grant collection/use rights. With encryption,
all `underlying_*` segments are encrypted before entering published bundles;
Polymarket, audit and non-price health metadata remain readable. Secrets are not
included in workflow logs, manifests or archive authentication records.

## Primary references checked for this implementation

- Polymarket discovery: https://docs.polymarket.com/market-data/discover-markets
- Market identity/rules/outcome pairing: https://docs.polymarket.com/market-data/market-details
- WS frames/heartbeats: https://docs.polymarket.com/market-data/realtime-data
- Books and history: https://docs.polymarket.com/market-data/prices-order-books
- Trades/OI/resolution: https://docs.polymarket.com/market-data/public-analytics
- Yahoo transport: https://github.com/ranaroussi/yfinance/blob/main/yfinance/live.py
- Yahoo wire schema: https://github.com/ranaroussi/yfinance/blob/main/yfinance/pricing.proto
- Alpaca stock data: https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data
- GitHub Releases: https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases

Tests are offline protocol/storage/classification checks. Passing them alone is
not a claim that any live feed connected or a GitHub archive was published.

### Live smoke gate

Each PR and scheduled/push run first performs a 150-second public, read-only network smoke test after the 33 deterministic tests. Its health and raw recovery files are retained as a 7-day Actions artifact. The production capture starts only after at least one matching market and a Polymarket market-channel observation are verified. This gate does not certify Yahoo availability, quote completeness, or a full day of capture: inspect the printed per-source coverage. No Alpaca credentials or archive publication token are supplied to the smoke job.
