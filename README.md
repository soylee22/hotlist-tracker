# Hotlist Tracker

Daily scrape of the Trading 212 Hotlist Leaderboard, computing the **Hotlist 10** portfolio rule and publishing a dashboard via GitHub Pages.

## The rule

1. **Universe.** Top 10 most-owned single stocks on the T212 Hotlist (ETFs and trackers excluded).
2. **Initial weights.** Ownership-weighted at inception.
3. **Drift policy.** Hold and let drift. No periodic rebalance. No upper cap.
4. **Trigger.** A held stock outside the Top 10 single-stocks for 5 consecutive scrapes (hysteresis).
5. **Action.** Sell the exited stock, buy the highest-ranked confirmed entrant with the proceeds.

Rule v1.0. Stored in `data/portfolio_state.json`.

## What runs

- `scripts/scrape.py` — Playwright headless scrape of the leaderboard, top 30, appends to `data/hotlist_history.csv`.
- `scripts/compute.py` — applies the rule, writes `data/portfolio_state.json` and `data/trade_log.csv`.
- `scripts/perf.py` — yfinance prices, simulated £10,000 portfolio, benchmarked against `VUAG.L`. Output: `data/performance.csv`.
- `scripts/render.py` — Jinja2 + inline SVG, generates `dashboard/index.html`.
- `.github/workflows/daily.yml` — cron 21:00 UTC, runs the chain and publishes Pages.

## Local development

```bash
pip install -e .
python -m playwright install chromium

# Run end-to-end
python scripts/scrape.py
python scripts/compute.py
python scripts/perf.py
python scripts/render.py
open dashboard/index.html
```

## Data files

| File | Purpose |
|------|---------|
| `data/hotlist_history.csv` | Master timeseries: `date,rank,ticker,name,users,is_excluded`. Top 30 captured each day. |
| `data/portfolio_state.json` | Current basket, weights, hysteresis state, candidate queue. |
| `data/trade_log.csv` | Every rotation event. Append-only. |
| `data/performance.csv` | Daily portfolio + benchmark mark-to-market. |
| `data/exclusions.json` | ETF / tracker exclusion list. Edit as new ETFs appear. |
| `data/benchmarks/vuag.csv` | (Optional) raw VUAG.L closes if you want to inspect. |

## Why this exists

Replaces the old top-20-by-market-cap accumulation scanner. The thesis is different now: track what the crowd actually *holds* on Trading 212, not what is biggest by market value. See `outputs/2026-04-27-top-10-hotlist-pie-defence/anatomy.html` in SecondBrain for the full rationale.

## Caveats

- **Free-share contamination.** T212 promos can inflate user counts artificially. The daily report flags >20% week-on-week jumps as suspicious.
- **Cold-start.** Change windows beyond "since inception" show "—" until enough history exists.
- **Currency.** RR.L is GBP; the others are USD. Portfolio is reported in GBP using GBP/USD FX from yfinance.
- **Not investment advice.** A rule I follow.
