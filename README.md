# Polymarket 5-minute quote capture

This repo contains a small capture script for Polymarket 5-minute Up/Down markets.

## What it records

The CSV schema is:

- `ts_iso`
- `market_url`
- `window_text`
- `buy_up_cents`
- `buy_down_cents`
- `sell_up_cents`
- `sell_down_cents`

Interpretation:

- `buy_*_cents` = best ask on that outcome book
- `sell_*_cents` = best bid on that outcome book

## Example manual test

```bash
python polymarket_quotes.py \
  --market-url 'https://polymarket.com/zh/event/btc-updown-5m-1776752100' \
  --mode once-current
```

## Example 1-second capture for Beijing 16:15-16:30

```bash
python polymarket_quotes.py \
  --market-url 'https://polymarket.com/zh/event/btc-updown-5m-1776752100' \
  --mode loop-range \
  --date-bj 2026-04-21 \
  --range-start-hm 16:15 \
  --range-end-hm 16:30 \
  --sample-seconds 1
```

The script derives the 5-minute series prefix from the seed URL and automatically switches slugs every 5 minutes.

## GitHub Actions note

GitHub scheduled workflows are **not** precise to the second and can be delayed. For second-level collection around a tight time window, prefer manually triggering the workflow shortly before the window or running the script on a continuously running machine.
