# Polymarket 5-minute market capture

This repo captures second-level snapshots for Polymarket 5-minute Up/Down markets such as BTC 5m Up/Down.

The current implementation focuses on **reliable order-book snapshots** plus a **reference price** feed. Some fields are very stable, while a few experimental fields are best-effort only.

## What this repo does

The main script is:

- `polymarket_quotes.py`

It can:

- lock the **current 5-minute window** and sample once per second until the window ends
- sample a **specified Beijing time range**
- sample the **next N hours** continuously across multiple 5-minute windows

The script derives the series prefix from a seed market URL and automatically switches to the correct slug for each 5-minute window.

## Main workflows

### 1. `polymarket-current-window`

One-click workflow for the **current active 5-minute window**.

Behavior:

- uses the fixed seed URL
- locks the current window
- samples once per second
- stops automatically when the current 5-minute window ends

### 2. `polymarket-current-window-volume-test`

Same as current-window, but kept as a separate test workflow while experimenting with trade-related fields.

### 3. `polymarket-plus2h-to-plus8h`

One-click workflow for capturing the period from **2 hours later through 8 hours later**.

Behavior:

- waits 2 hours
- captures 3 hours
- then captures another 3 hours
- total coverage: 6 hours

### 4. `polymarket-monthly-rolling-24h`

Long-running workflow for monthly data collection.

Behavior:

- captures the next 24 hours in five sequential jobs
- commits and pushes data every 30 minutes
- starts the next 24-hour run automatically when the current run finishes
- is restarted by `polymarket-monthly-watchdog` if no monthly run is active

Manual start:

```bash
gh workflow run polymarket-monthly-rolling-24h.yml -f auto_continue=true
```

Manual stop:

- disable `polymarket-monthly-watchdog` in GitHub Actions
- cancel any active or queued `polymarket-monthly-rolling-24h` runs

## Seed URL currently used in workflows

```text
https://polymarket.com/zh/event/btc-updown-5m-1776752100
```

## Manual usage examples

### Capture the current active window

```bash
python polymarket_quotes.py \
  --market-url 'https://polymarket.com/zh/event/btc-updown-5m-1776752100' \
  --mode loop-current-window \
  --sample-seconds 1
```

### Capture a specified Beijing time range

```bash
python polymarket_quotes.py \
  --market-url 'https://polymarket.com/zh/event/btc-updown-5m-1776752100' \
  --mode loop-range \
  --date-bj 2026-04-21 \
  --range-start-hm 16:15 \
  --range-end-hm 16:30 \
  --sample-seconds 1
```

### Capture the next 2 hours

```bash
python polymarket_quotes.py \
  --market-url 'https://polymarket.com/zh/event/btc-updown-5m-1776752100' \
  --mode loop-next-hours \
  --duration-hours 2 \
  --sample-seconds 1
```

### Snapshot a specific window once

```bash
python polymarket_quotes.py \
  --market-url 'https://polymarket.com/zh/event/btc-updown-5m-1776752100' \
  --mode once-current \
  --target-window-end-hm 16:45
```

## Output location

Captured CSV files are written under:

```text
data/YYYY-MM-DD/
```

Current default file name pattern:

```text
btc-updown-5m_quotes.csv
```

Debug artifacts, when generated, are written under:

```text
debug/YYYY-MM-DD/
```

## CSV schema

Current CSV fields are:

- `ts_iso`
- `slug`
- `market_url`
- `window_text`
- `buy_up_cents`
- `buy_down_cents`
- `sell_up_cents`
- `sell_down_cents`
- `buy_up_size`
- `buy_down_size`
- `sell_up_size`
- `sell_down_size`
- `mid_up_cents`
- `mid_down_cents`
- `spread_up_cents`
- `spread_down_cents`
- `bid_depth_up_5`
- `ask_depth_up_5`
- `bid_depth_down_5`
- `ask_depth_down_5`
- `level_count_bid_up`
- `level_count_ask_up`
- `level_count_bid_down`
- `level_count_ask_down`
- `target_price`
- `final_price`
- `trade_count_1s`
- `trade_volume_1s`

## Field definitions

### Identity / time

- `ts_iso`: snapshot timestamp in ISO-8601 format, Beijing timezone
- `slug`: exact Polymarket slug for the captured 5-minute market window
- `market_url`: Polymarket URL for that specific slug
- `window_text`: window label in Beijing time, for example `16:55-17:00`

### Top of book prices

- `buy_up_cents`: best ask for the `Up` outcome, in cents
- `buy_down_cents`: best ask for the `Down` outcome, in cents
- `sell_up_cents`: best bid for the `Up` outcome, in cents
- `sell_down_cents`: best bid for the `Down` outcome, in cents

Interpretation:

- `buy_*_cents` means the price you would pay to immediately buy that outcome
- `sell_*_cents` means the price you would receive to immediately sell that outcome

### Top of book size

- `buy_up_size`: size resting at the best ask for `Up`
- `buy_down_size`: size resting at the best ask for `Down`
- `sell_up_size`: size resting at the best bid for `Up`
- `sell_down_size`: size resting at the best bid for `Down`

These are level-1 sizes only, not total book depth.

### Mid / spread

- `mid_up_cents`: midpoint between best bid and best ask for `Up`
- `mid_down_cents`: midpoint between best bid and best ask for `Down`
- `spread_up_cents`: best ask minus best bid for `Up`
- `spread_down_cents`: best ask minus best bid for `Down`

### Depth / book shape

- `bid_depth_up_5`: summed size of the first 5 bid levels for `Up`
- `ask_depth_up_5`: summed size of the first 5 ask levels for `Up`
- `bid_depth_down_5`: summed size of the first 5 bid levels for `Down`
- `ask_depth_down_5`: summed size of the first 5 ask levels for `Down`
- `level_count_bid_up`: number of bid levels currently returned for `Up`
- `level_count_ask_up`: number of ask levels currently returned for `Up`
- `level_count_bid_down`: number of bid levels currently returned for `Down`
- `level_count_ask_down`: number of ask levels currently returned for `Down`

### Reference / target price

- `target_price`: target threshold price for the 5-minute market window
- `final_price`: reference price sampled at snapshot time

Current logic:

- `final_price` is sampled once per second from a reference price source, with Chainlink page parsing attempted first and exchange fallback used if needed
- `target_price` is derived from the window-start reference price when available
- if the script starts exactly near the start of a new 5-minute window, `target_price` may fall back to the initial `final_price`
- if no reliable source is found, the value can be blank

### Trade-related fields

- `trade_count_1s`: best-effort count of newly observed trades in the last sampling interval
- `trade_volume_1s`: best-effort summed trade size in the last sampling interval

Important note:

These two fields are currently **experimental**. If no reliable trade payload is detected, they may be blank. Blank means **not confidently captured**, not necessarily zero trades.

## Window mapping logic

This repo uses the convention:

- the slug timestamp corresponds to the **window start time**
- each window lasts 5 minutes

Example:

- slug timestamp at Beijing `16:55` means window `16:55-17:00`

## Reliability notes

### Usually reliable

These fields are directly based on the live book or deterministic window mapping and are the most reliable:

- `slug`
- `market_url`
- `window_text`
- `buy_up_cents`
- `buy_down_cents`
- `sell_up_cents`
- `sell_down_cents`
- `buy_up_size`
- `buy_down_size`
- `sell_up_size`
- `sell_down_size`
- `mid_up_cents`
- `mid_down_cents`
- `spread_up_cents`
- `spread_down_cents`
- `bid_depth_up_5`
- `ask_depth_up_5`
- `bid_depth_down_5`
- `ask_depth_down_5`
- `level_count_bid_up`
- `level_count_ask_up`
- `level_count_bid_down`
- `level_count_ask_down`

### Best-effort / may be blank

- `target_price`
- `final_price`
- `trade_count_1s`
- `trade_volume_1s`

## Known limitations

- GitHub Actions is not second-perfect at **start time scheduling**
- One workflow can sample once per second, but workflow startup itself may be delayed
- Web page parsing can break if page structure changes
- Trade-related fields are still under validation and should not yet be treated as guaranteed-correct production metrics

## Recommended usage

For most current work:

- use `polymarket-current-window` for a quick current-window snapshot run
- use `polymarket-plus2h-to-plus8h` when you want a delayed long capture window
- treat order-book fields as primary data
- treat trade fields as experimental until further validation
