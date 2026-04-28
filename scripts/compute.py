"""Compute rule state from hotlist_history.csv.

- Filters ETFs/excluded names from each daily snapshot.
- Identifies the current "Top 10 single stocks".
- Maintains hysteresis (5 consecutive days outside top 10 -> exit).
- Detects rotation events (a confirmed exit + a confirmed new entrant).
- On rotation events: FULL rebalance of the basket to current ownership weights.
- Writes data/portfolio_state.json with current basket + watch list.
- Weights stored as integer percentages (no spurious decimals).

Rule (v1.1):
    - Universe: top 10 most-owned single stocks (excluding ETFs).
    - Initial weights: ownership-weight at inception, rounded to int %.
    - Drift policy: hold and drift between rotations.
    - Trigger: held stock outside Top 10 single-stocks for 5 consecutive
      daily scrapes.
    - Action on trigger: full rebalance of all 10 positions to today's
      ownership weights (sell exited stock; reallocate the entire basket
      to fresh ownership weights using today's user counts).
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "data" / "hotlist_history.csv"
STATE = ROOT / "data" / "portfolio_state.json"
TRADES = ROOT / "data" / "trade_log.csv"
HYSTERESIS_DAYS = 5
TOP_N = 10


def load_history() -> pd.DataFrame:
    if not HISTORY.exists() or HISTORY.stat().st_size <= len("date,rank,ticker,name,users,is_excluded\n"):
        return pd.DataFrame(columns=["date", "rank", "ticker", "name", "users", "is_excluded"])
    df = pd.read_csv(HISTORY)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["users"] = df["users"].astype(int)
    df["is_excluded"] = df["is_excluded"].astype(int).astype(bool)
    return df


def load_state() -> dict:
    return json.loads(STATE.read_text())


def save_state(state: dict) -> None:
    STATE.write_text(json.dumps(state, indent=2, default=str))


def _top10_singles(day_df: pd.DataFrame) -> pd.DataFrame:
    """Return the day's top-10 single stocks (excluded names removed),
    re-ranked 1..10 within the filtered universe."""
    singles = day_df[~day_df["is_excluded"]].sort_values("users", ascending=False).head(TOP_N).copy()
    singles["filtered_rank"] = range(1, len(singles) + 1)
    return singles


def _consecutive_days(history: pd.DataFrame, ticker: str, predicate) -> int:
    """Count consecutive most-recent dates where `predicate(day_df)` is True
    for the given ticker. predicate takes the day's DataFrame and returns bool."""
    dates = sorted(history["date"].unique(), reverse=True)
    n = 0
    for d in dates:
        day = history[history["date"] == d]
        if predicate(day, ticker):
            n += 1
        else:
            break
    return n


def _ticker_in_top10(day_df: pd.DataFrame, ticker: str) -> bool:
    top = _top10_singles(day_df)
    return ticker in top["ticker"].values


def _ticker_outside_top10(day_df: pd.DataFrame, ticker: str) -> bool:
    return not _ticker_in_top10(day_df, ticker)


def round_to_int_pct(raw_weights: list[float]) -> list[int]:
    """Round each weight (0..1) to integer % using the largest-remainder
    method. Guarantees the result sums to exactly 100."""
    if not raw_weights:
        return []
    pct = [w * 100 for w in raw_weights]
    floored = [int(x) for x in pct]
    remainder = 100 - sum(floored)
    fracs = sorted(
        enumerate(pct), key=lambda kv: kv[1] - int(kv[1]), reverse=True
    )
    for idx, _ in fracs[:max(0, remainder)]:
        floored[idx] += 1
    return floored


def _basket_from_top10(top: pd.DataFrame, as_of_date: str) -> list[dict]:
    total = int(top["users"].sum())
    raw = [int(u) / total for u in top["users"]]
    pct = round_to_int_pct(raw)
    out = []
    for (_, r), w in zip(top.iterrows(), pct):
        out.append({
            "ticker": r["ticker"],
            "name": r["name"],
            "rank_at_entry": int(r["filtered_rank"]),
            "users_at_entry": int(r["users"]),
            "weight_pct": int(w),
            "entry_date": str(as_of_date),
        })
    return out


def initial_basket(history: pd.DataFrame, inception_date) -> list[dict]:
    """Set initial basket = ownership-weighted top 10 singles on inception."""
    day = history[history["date"] == inception_date]
    if day.empty:
        return []
    return _basket_from_top10(_top10_singles(day), str(inception_date))


def compute_state(history: pd.DataFrame, state: dict) -> dict:
    """Update state given the latest history. Detects rotation events
    using the hysteresis rule and updates basket / candidate queue / trade log."""
    if history.empty:
        return state
    today = max(history["date"].unique())
    state["last_updated"] = str(today)

    # Bootstrap on first run if no inception yet.
    if state.get("inception_date") is None:
        state["inception_date"] = str(today)
        state["basket"] = initial_basket(history, today)
        return state

    held_tickers = [b["ticker"] for b in state.get("basket", [])]
    today_df = history[history["date"] == today]
    today_top = _top10_singles(today_df)
    today_top_tickers = list(today_top["ticker"].values)

    # Confirmed exits: held tickers that have been outside top10 for >=N consecutive days.
    exit_watch = {}
    confirmed_exits: list[str] = []
    for t in held_tickers:
        days_out = _consecutive_days(history, t, _ticker_outside_top10)
        exit_watch[t] = days_out
        if days_out >= HYSTERESIS_DAYS:
            confirmed_exits.append(t)
    state["exit_watch"] = exit_watch

    # Confirmed entrants: top10 names not currently held that have been in
    # top10 for >=N consecutive days. Ranked by today's filtered_rank.
    entry_watch = {}
    confirmed_entrants: list[str] = []
    for t in today_top_tickers:
        if t in held_tickers:
            continue
        days_in = _consecutive_days(history, t, _ticker_in_top10)
        entry_watch[t] = days_in
        if days_in >= HYSTERESIS_DAYS:
            confirmed_entrants.append(t)
    state["candidate_queue"] = [
        {"ticker": t, "days_in_top10": entry_watch[t],
         "users": int(today_top.loc[today_top["ticker"] == t, "users"].iloc[0]),
         "name": str(today_top.loc[today_top["ticker"] == t, "name"].iloc[0])}
        for t in confirmed_entrants
    ]

    # Process rotations one per day to keep churn bounded.
    if confirmed_exits and confirmed_entrants:
        exit_t = max(confirmed_exits, key=lambda x: exit_watch[x])
        ent_t = sorted(
            confirmed_entrants,
            key=lambda x: int(today_top.loc[today_top["ticker"] == x, "filtered_rank"].iloc[0]),
        )[0]

        last_seen_for_exit = history[history["ticker"] == exit_t].sort_values("date").tail(1)
        sold_users = int(last_seen_for_exit["users"].iloc[0]) if not last_seen_for_exit.empty else 0
        bought_users = int(today_top.loc[today_top["ticker"] == ent_t, "users"].iloc[0])

        # FULL REBALANCE: reset the entire basket to today's ownership weights.
        new_basket = _basket_from_top10(today_top, str(today))
        state["basket"] = new_basket

        # Trade log: headline is the rotation; note captures the full new weights.
        weights_summary = ", ".join(f"{b['ticker']}:{b['weight_pct']}" for b in new_basket)
        with TRADES.open("a", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                today, "rotate+rebalance", exit_t, sold_users, ent_t, bought_users,
                f"Full rebalance to current ownership weights. New: {weights_summary}",
            ])
        state["exit_watch"].pop(exit_t, None)

    return state


def deltas_for_basket(history: pd.DataFrame, state: dict) -> list[dict]:
    """For each basket ticker, compute deltas: 1d, 7d, 30d, YTD, since-inception."""
    if history.empty:
        return []
    today = max(history["date"].unique())
    inception = pd.to_datetime(state.get("inception_date") or today).date()
    out = []
    for b in state.get("basket", []):
        t = b["ticker"]
        sub = history[history["ticker"] == t].sort_values("date")
        if sub.empty:
            continue
        latest = int(sub.iloc[-1]["users"])
        latest_rank = int(sub.iloc[-1]["rank"])

        def lookup(target_date):
            row = sub[sub["date"] <= target_date]
            return int(row.iloc[-1]["users"]) if not row.empty else None

        d1 = lookup(today - dt.timedelta(days=1))
        d7 = lookup(today - dt.timedelta(days=7))
        d30 = lookup(today - dt.timedelta(days=30))
        d_ytd = lookup(dt.date(today.year, 1, 1))
        d_inc = lookup(inception)

        def pct(curr, base):
            if base is None or base == 0:
                return None
            return round((curr - base) / base * 100, 2)

        out.append({
            "ticker": t,
            "name": b["name"],
            "users": latest,
            "rank": latest_rank,
            "weight_pct": int(b.get("weight_pct", 0)),
            "delta_1d_pct": pct(latest, d1),
            "delta_7d_pct": pct(latest, d7),
            "delta_30d_pct": pct(latest, d30),
            "delta_ytd_pct": pct(latest, d_ytd),
            "delta_inception_pct": pct(latest, d_inc),
        })
    return out


def watch_list(history: pd.DataFrame, state: dict, n: int = 20) -> list[dict]:
    """Top 11..n single-stocks not currently held — the early warning list."""
    if history.empty:
        return []
    today = max(history["date"].unique())
    today_df = history[(history["date"] == today) & (~history["is_excluded"])]
    held = {b["ticker"] for b in state.get("basket", [])}
    sorted_df = today_df.sort_values("users", ascending=False).reset_index(drop=True)
    out = []
    for i, row in sorted_df.iterrows():
        if row["ticker"] in held:
            continue
        days_in = _consecutive_days(history, row["ticker"], _ticker_in_top10)
        out.append({
            "ticker": row["ticker"],
            "name": row["name"],
            "users": int(row["users"]),
            "filtered_rank": int(i + 1),  # 0-based to 1-based; this counts in the ALL-not-held list
            "days_in_top10": days_in,
        })
        if len(out) >= n:
            break
    return out


def main() -> int:
    history = load_history()
    state = load_state()
    state = compute_state(history, state)
    save_state(state)
    print(f"State updated. Inception: {state.get('inception_date')}, basket size: {len(state.get('basket', []))}")
    if state.get("candidate_queue"):
        names = ", ".join(c["ticker"] for c in state["candidate_queue"])
        print(f"Confirmed entrants in candidate queue: {names}")
    if state.get("exit_watch"):
        watching = {k: v for k, v in state["exit_watch"].items() if v > 0}
        if watching:
            print(f"Held names outside top 10: {watching}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
