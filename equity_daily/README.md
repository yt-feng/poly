# Daily finance raw-data capture

Independent, read-only acquisition in **yt-feng/poly**. Code, workflow, output,
family keys and Release namespace are separate from BTC five-minute data.
No existing BTC file/workflow or `yt-feng/poly_trade` is changed or imported.
No trading account, order, strategy evaluation or live canary is enabled.

## What is collected

| Source | Retained raw data | Granularity / limit |
|---|---|---|
| Polymarket market WS | Every observed native text/binary frame, both outcome tokens: books, price/size changes, best bid/ask, last-trade, tick-size, new-market/resolution messages and unknown fields | Native observed messages; no intentional sampling. Not a guarantee of every exchange event. |
| Polymarket REST books | Full two-sided book responses including returned hashes | Every 60 seconds and resubscription/reconnection requests; not a repair for missing historical deltas. |
| Gamma | Original listing/tag responses, versioned accepted market/event rules, dates, condition IDs, outcomes/token pairing, resolution sources, liquidity/volume and flags returned by the provider | Finance tags resolved dynamically; newest IDs first, 25-event cursor pages with overflow reduction; open scans every 300s, recent closed scans every sixth cycle. Default end-date window: prior 7 / next 8 days. |
| Public trades | Original paginated Data API v2 trade responses; overlap intentionally retained | Windowed backfill with explicit completion/truncation. Legacy fallback for 404/405, bounded offset. Not labeled all-time complete. |
| History / analytics | Original sampled price history, OI, resolution state, CLOB market details such as fee/tick/minimum-size/negative-risk fields when supplied | History requests 1d / 60-second buckets and follows cursors; never described as historical tick/L2 reconstruction. OI/resolution every 600s, details/history every 1800s. |
| Yahoo WS | Original wrapper/base64/protobuf plus separate decoding of available price/time/currency/exchange/day-volume/high/low/change/open/previous-close/bid/ask/sizes/last-size and unknown fields | **Unofficial provider quote updates, not exchange executions or consolidated NBBO.** Delay is unknown unless the provider reports it. Missing fields stay missing. |
| Yahoo intraday | Entire response with timestamps, OHLCV, metadata, pre/post trading periods, timezone, dividends/splits/adjustments when supplied | 1m; first 5d then 1d every 60s. Revisions are retained, not overwritten. A metadata-only HTTP 200 is explicitly marked `metadata_only_no_bars`. |
| Yahoo daily reference | Entire 3mo daily chart including supplied corporate actions and adjusted close | Initial and hourly. Unadjusted OHLC and adjusted close remain separate. |
| Optional Alpaca | Native entitled-feed trades, quotes, bars, updated/daily bars, corrections/cancels/errors, snapshots; SIP statuses/LULD where entitled | Credentials required. Default IEX is **IEX only**, not all US trading; SIP requires entitlement, delayed_sip is separate. Not running without credentials. |

The accepted universe is defined by `catalog`, not every discovery response or
provider-wide new-market notification. Broad discovery transport can contain
rejected metadata; only accepted daily non-crypto outcome tokens are subscribed.
New ticker-bearing events require no fixed stock-list update. Unfamiliar title
formats can still need parser changes; unknown financial underlyings are flagged.
`pagination_complete` describes the API scan/window, not proven parser coverage.

## Identity, settlement and point-in-time use

Use `(family, market_id, condition_id, token_id)` and catalog version; ticker alone
is not unique across dates/strikes. Original outcome array position determines
token pairing; YES/UP is not assumed to be index zero. Opening-direction events
are `daily_open_direction`, separate from `daily_direction` and `close_threshold`.

NDX/SPX/HSI/DAX/FTSE/Nikkei map to index references, not same-named ETFs. WTI crude
maps to a labeled continuous-futures reference, not the W&T Offshore equity.
Gold/silver spot events are explicitly labeled `continuous_futures_proxy_not_spot`
when a futures series is used; it is NOT an exact contract or spot settlement.
An explicit Yahoo symbol URL in market rules takes precedence with provenance.
Daily FX pairs discovered in finance are retained as `foreign_exchange`; a quote
reference is not an official central-bank fixing or equity transaction volume.
No underlying is declared the official settlement oracle. Pyth rule references
are retained in `oracle_reference_symbols`, while `oracle_prices_collected=false`:
this module has no Pyth Pro credentials and does not substitute Yahoo for it.

Keep endDate UTC and native rules. Session date, exchange calendar, timezone,
comparison price, averaging window, tie rule and corporate actions must be
interpreted from those rules; do not derive an exchange session from UTC date.
Indices/FX can lack meaningful traded volume: missing is not zero.

Every envelope has receipt UTC nanoseconds, monotonic time, run ID and local
sequence. WS also has connection ID/sequence (collector counters, not exchange
sequence numbers). Native timestamps/precision remain in untouched payloads.
HTTP records include request start, status, params and safe response headers,
never request credentials or outbound authentication frames. Reconnects, errors,
missing tokens, per-symbol availability, partial pages and run boundaries are
recorded in audit/health/manifest. Sequence does not establish gap-free delivery.

For future backtests require `received_at_ns <= decision_time` and respect source
time, delay and bar completion. A historical response received today need not
have been knowable at its bar timestamp. Keep corrections/revisions separate.
Cumulative daily-volume changes are not individual trades. Bid, ask, last and
midpoint are different observations. Deduplicate overlapping REST trades using
provider identifiers downstream; retain originals and avoid maker/taker doubles.

## GitHub storage and running

Package: `equity_daily/`; workflow: `.github/workflows/equity-daily.yml`;
concurrency: `equity-daily-raw-production`; family: `equity_daily`;
output: `equity_daily_output/`; Release: `equity-daily-v1-<run_id>-<attempt>`.
Non-equity-named or nonempty output directories are refused.

Raw WS/HTTP bytes are base64 encoded with per-payload SHA256 in append-only gzip
JSONL. Segments close around 32 MiB uncompressed, on UTC day changes/checkpoints.
Each has a SHA256 sidecar and receipt range in immutable cumulative manifests.
Only closed segments referenced by a completed checksummed manifest enter tar
bundles (~128 MiB), which have their own SHA256 sidecars. Partial files are never
published. Release publication retries identical immutable bytes, confirms remote
sizes/digests when available, and retains local recovery copies. No raw data is
committed to the git code history. Later manifests can reference earlier bundles.

Production runs 4h, scheduled every 4h, with first checkpoint at about 45s and
rolling publication every 300s. Test/smoke and runner queues create gaps; GitHub
Actions is **not zero-gap 24/7 market infrastructure**. All 47 deterministic tests
and a 150s live read-only smoke must pass before production starts. Smoke requires
at least one discovered market and PM market observation; it does not certify
all underlyings. Smoke/recovery/health Actions artifacts expire after 7 days;
Releases are the intended durable archive. GitHub limits still apply; review
storage growth and mirror to object storage if needed, not unlimited retention.

```bash
python -m pip install -r equity_daily/requirements.txt
python -m unittest discover -s equity_daily/tests -p 'test_*.py' -v
python -m equity_daily.collector --seconds 180 --output equity_daily_local
# gh CLI authenticated; GH_REPO=yt-feng/poly
python -m equity_daily.collector --seconds 14400 --output equity_daily_run_001 \
  --checkpoint-seconds 300 --release equity-daily-v1-manual-001
# --seconds 0 supports a separately supervised long-running host.
```

Verify bundle sidecars before safe tar extraction (e.g. Python extractall with
`filter='data'` on a supporting version), then verify member hashes. Decode:

```python
import base64, gzip, hashlib, json
with gzip.open('equity_daily-polymarket_ws-....jsonl.gz', 'rt') as f:
    for line in f:
        row = json.loads(line)
        raw = base64.b64decode(row['payload_b64'])
        assert hashlib.sha256(raw).hexdigest() == row['payload_sha256']
```

### Auditing a published release

`equity_daily/release_audit.py` is an offline, read-only checker for bundles that
have already been downloaded. It validates the outer release sidecar, every
inner member sidecar and retained payload digest; it also reports observed
Polymarket token IDs, receipt/source timestamp ranges, Yahoo decoded rows and
chart bar counts. It rejects unsafe tar members and never downloads, decrypts or
fills missing rows. Keep the tars and extracted payloads outside git:

```bash
python -m equity_daily.release_audit \
  --input-dir /tmp/equity-release-audit-37228795593-1 \
  --metadata equity_daily/reports/equity-daily-v1-37228795593-1.metadata.json \
  --output /tmp/equity-daily-release-audit.json
```

The checked-in report for release
[`equity-daily-v1-37228795593-1`](reports/equity-daily-v1-37228795593-1.audit.md)
is deliberately limited to release metadata and aggregate counts. It samples
bundles 000001, 000025 and 000049 rather than claiming all 49 bundles were
downloaded. A cumulative manifest may reference files not present in its
newly-closed tar; the audit reports that difference. Its 202/202 token statement
is a manifest health claim, not proof of continuous receipt, and public Yahoo
quotes/chart bars are not exchange fills or settlement evidence.

## Optional credentials and publication rights

Default Polymarket/Yahoo paths need no API key. Yahoo is unofficial and can reject
cloud IPs, throttle, delay or stop feeds. Requests are not evidence of receipt:
inspect health. The repository and Releases are public; default source data is
plaintext unless `EQUITY_ARCHIVE_KEY` is configured. Public accessibility is not
a redistribution license: confirm source collection/use/storage/publication rights.

Secrets used only by this workflow: `EQUITY_ALPACA_KEY_ID`,
`EQUITY_ALPACA_SECRET_KEY`, optional `EQUITY_ARCHIVE_KEY` (generated Fernet key,
not a short PIN). Variables: `EQUITY_ALPACA_FEED=iex|sip|delayed_sip`, optional
`EQUITY_ALPACA_SYMBOLS` comma-separated subset. Licensed-feed publication is
blocked without encryption, unless the operator explicitly asserts rights via
`EQUITY_ALPACA_PUBLIC_REDISTRIBUTION=true`. Encryption grants no data license.
With an archive key, all `underlying_*` segments are encrypted before bundling;
Polymarket/audit/non-price health remain readable. Back up the key privately.

```bash
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

## Verified trial and known limits

The 150s PR smoke at `7e4bddb66205df970c7711e4ba2f323b1cec89d9` passed. Downloaded
artifact `11226138735` (run `37007622709`) contains 706 accepted recent markets,
202 unclosed markets, 404/404 outcome tokens with observations, 84,780 PM WS frames
(including control frames), 507 Yahoo quote messages, 808 REST books, and 58
successful chart responses. Nineteen Yahoo symbols produced quote messages; some
chart responses had metadata but no prices. These are trial counts, not complete
session coverage. No Alpaca credentials were configured. Trial underlying mapping
errors (WTI/DAX and some indices/metals) were corrected in the next commit before
production: do not treat the earlier trial mappings as validated research data.

Pre-deployment and disconnected tick history cannot be recovered by interpolation.
Finite REST backfills are not all-time complete. Optional licensed/SIP and exact
oracle feeds remain unavailable until the relevant credentials/rights exist.
Tests passing alone do not prove live feeds or Release publication.

## Primary implementation references

- https://docs.polymarket.com/market-data/discover-markets
- https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination
- https://docs.polymarket.com/market-data/market-details
- https://docs.polymarket.com/market-data/realtime-data
- https://docs.polymarket.com/market-data/prices-order-books
- https://docs.polymarket.com/api-reference/markets/get-a-tokens-price-history
- https://docs.polymarket.com/market-data/public-analytics
- https://github.com/ranaroussi/yfinance/blob/main/yfinance/live.py
- https://github.com/ranaroussi/yfinance/blob/main/yfinance/pricing.proto
- https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data
- https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases
- https://finance.yahoo.com/quote/%5EGDAXI/
- https://finance.yahoo.com/quote/%5EN225/
- https://finance.yahoo.com/quote/%5EFTSE/
- https://finance.yahoo.com/quote/DX-Y.NYB/
- https://finance.yahoo.com/quote/WTI/
