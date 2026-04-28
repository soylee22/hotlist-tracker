"""Simulated portfolio performance vs VUAG.L benchmark.

- Pulls daily close prices via yfinance for each basket ticker plus VUAG.L.
- Simulates a £10,000 notional portfolio bought at inception at the
  ownership weights stored in portfolio_state.json.
- Replays trade_log.csv for any rotation events; new entrant bought with
  full proceeds of the sold position.
- Buy-and-hold £10,000 of VUAG.L from same inception as the benchmark.
- Output: data/performance.csv with daily mark-to-market for both lines.

Currency: prices fetched in their native currency. RR.L is GBP, the rest
USD. Portfolio is valued in GBP using daily GBP/USD FX (yfinance ticker
GBPUSD=X). VUAG.L is GBP-denominated already.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = ROOT / "data" / "portfolio_state.json"
TRADE_LOG = ROOT / "data" / "trade_log.csv"
PERF_PATH = ROOT / "data" / "performance.csv"
BENCH_PATH = ROOT / "data" / "benchmarks" / "vuag.csv"

NOTIONAL_GBP = 10_000.0
BENCHMARK = "VUAG.L"
GBP_TICKERS = {"RR.L"}  # already GBP, no FX conversion needed
USD_FX_TICKER = "GBPUSD=X"


def load_state() -> dict:
    return json.loads(STATE_PATH.read_text())


def fetch_prices(tickers: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    """Fetch daily Close prices. Returns wide DataFrame indexed by date."""
    if not tickers:
        return pd.DataFrame()
    data = yf.download(
        tickers=" ".join(tickers),
        start=start.isoformat(),
        end=(end + dt.timedelta(days=1)).isoformat(),
        progress=False,
        auto_adjust=True,
        group_by="ticker",
        threads=True,
    )
    if data.empty:
        return pd.DataFrame()
    # Normalise: produce {ticker: Series of Close}
    out = {}
    if isinstance(data.columns, pd.MultiIndex):
        for t in tickers:
            try:
                out[t] = data[t]["Close"]
            except KeyError:
                continue
    else:
        # single ticker case
        out[tickers[0]] = data["Close"]
    return pd.DataFrame(out).ffill()


def to_gbp(prices: pd.DataFrame, fx_gbpusd: pd.Series) -> pd.DataFrame:
    """Convert USD-priced columns to GBP. fx_gbpusd is GBP per USD? No,
    GBPUSD=X gives USD per GBP. So GBP_value = USD_value / GBPUSD."""
    if prices.empty:
        return prices
    out = prices.copy()
    aligned_fx = fx_gbpusd.reindex(out.index).ffill()
    for col in out.columns:
        if col in GBP_TICKERS:
            continue
        out[col] = out[col] / aligned_fx
    return out


def simulate(state: dict) -> pd.DataFrame:
    inception = state.get("inception_date")
    if inception is None:
        print("No inception yet, skipping perf.", file=sys.stderr)
        return pd.DataFrame()
    inception_d = pd.to_datetime(inception).date()
    today = dt.date.today()

    basket = state.get("basket", [])
    if not basket:
        return pd.DataFrame()

    # Tickers we ever held (current + any in trade log) — needed for replay.
    historic_tickers: set[str] = {b["ticker"] for b in basket}
    if TRADE_LOG.exists():
        with TRADE_LOG.open() as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("sold_ticker"):
                    historic_tickers.add(row["sold_ticker"])
                if row.get("bought_ticker"):
                    historic_tickers.add(row["bought_ticker"])

    fetch_list = sorted({_yf(t) for t in historic_tickers}) + [BENCHMARK, USD_FX_TICKER]
    prices = fetch_prices(fetch_list, inception_d, today)
    if prices.empty:
        return pd.DataFrame()

    fx = prices[USD_FX_TICKER]
    prices_gbp = to_gbp(prices.drop(columns=[USD_FX_TICKER], errors="ignore"), fx)

    # Initial position = ownership weights at inception
    weights_init = {b["ticker"]: float(b.get("weight_at_entry", 1.0 / len(basket))) for b in basket}
    # Normalise (in case of float drift)
    s = sum(weights_init.values())
    if s > 0:
        weights_init = {k: v / s for k, v in weights_init.items()}

    # Initial shares: position_gbp / price_gbp_at_inception
    inception_price_row = prices_gbp.loc[prices_gbp.index >= pd.Timestamp(inception_d)].head(1)
    if inception_price_row.empty:
        return pd.DataFrame()
    p0 = inception_price_row.iloc[0]
    shares: dict[str, float] = {}
    for t, w in weights_init.items():
        col = _yf(t)
        if col not in p0 or pd.isna(p0[col]) or p0[col] <= 0:
            continue
        shares[t] = (NOTIONAL_GBP * w) / float(p0[col])

    # Replay rotations chronologically
    trade_events = []
    if TRADE_LOG.exists():
        with TRADE_LOG.open() as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("event") == "rotate" and row.get("date"):
                    trade_events.append({
                        "date": pd.to_datetime(row["date"]).date(),
                        "sold": row["sold_ticker"],
                        "bought": row["bought_ticker"],
                    })

    # Build daily timeline in GBP
    out_rows = []
    bench_inception_price = None
    for ts, row in prices_gbp.iterrows():
        d = ts.date()
        if d < inception_d:
            continue
        # Apply any rotation events on or before this date that haven't been applied.
        while trade_events and trade_events[0]["date"] <= d:
            ev = trade_events.pop(0)
            sold_t, bought_t = ev["sold"], ev["bought"]
            sold_col = _yf(sold_t)
            bought_col = _yf(bought_t)
            if sold_col in row.index and bought_col in row.index and not pd.isna(row[sold_col]) and not pd.isna(row[bought_col]):
                proceeds = shares.pop(sold_t, 0.0) * float(row[sold_col])
                shares[bought_t] = proceeds / float(row[bought_col]) if row[bought_col] > 0 else 0.0

        port_value = 0.0
        for t, sh in shares.items():
            col = _yf(t)
            if col in row.index and not pd.isna(row[col]):
                port_value += sh * float(row[col])

        bench_price = row.get(BENCHMARK)
        if pd.isna(bench_price):
            continue
        if bench_inception_price is None:
            bench_inception_price = float(bench_price)
        bench_value = NOTIONAL_GBP * (float(bench_price) / bench_inception_price)

        out_rows.append({
            "date": d,
            "portfolio_gbp": round(port_value, 2),
            "benchmark_gbp": round(bench_value, 2),
            "portfolio_return_pct": round((port_value / NOTIONAL_GBP - 1) * 100, 3),
            "benchmark_return_pct": round((bench_value / NOTIONAL_GBP - 1) * 100, 3),
        })

    df = pd.DataFrame(out_rows)
    return df


def _yf(ticker: str) -> str:
    """Map our internal ticker to a yfinance ticker. Lee's basket already
    uses yfinance-friendly tickers (e.g. RR.L) so this is mostly identity."""
    return ticker


def main() -> int:
    state = load_state()
    df = simulate(state)
    if df.empty:
        print("No performance to record yet.")
        return 0
    PERF_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PERF_PATH, index=False)
    print(f"Wrote {len(df)} rows of perf to {PERF_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
