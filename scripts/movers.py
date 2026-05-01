"""Compute top-100 ownership movers from data/hotlist_history.csv.

Produces data/movers.json with structured 1d / 7d / 30d windows. Each
window contains:
  - climbers_users / fallers_users: top-N by absolute delta in user count
  - rank_climbers / rank_fallers:   top-N by absolute rank change
  - new_entrants:                   names in today's top 100 absent from
                                    the prior window's top 100 (only
                                    computed when both windows scraped
                                    >=90 rows, so we know the universe
                                    is comparable)

Windows whose prior date has no history are omitted. The dashboard and
email digest can render whatever windows are available.

Usage:
    python scripts/movers.py [--top-n 10] [--out PATH]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
HISTORY_PATH = ROOT / "data" / "hotlist_history.csv"
OUT_PATH = ROOT / "data" / "movers.json"

WINDOWS = [("1d", 1), ("7d", 7), ("30d", 30)]
DEEP_UNIVERSE_THRESHOLD = 90  # rows/day at which we trust new-entrant logic


def _row(r: pd.Series) -> dict:
    """Format one merged row into a small JSON-friendly record."""
    def _int_or_none(v):
        if v is None or pd.isna(v):
            return None
        return int(v)

    def _round_or_none(v, nd=1):
        if v is None or pd.isna(v):
            return None
        return round(float(v), nd)

    return {
        "ticker": r["ticker"],
        "name": r["name"],
        "rank_today": _int_or_none(r.get("rank")),
        "rank_then": _int_or_none(r.get("rank_then")),
        "rank_change": _int_or_none(r.get("rank_change")),
        "users_today": _int_or_none(r.get("users")),
        "users_then": _int_or_none(r.get("users_then")),
        "delta_users": _int_or_none(r.get("delta_users")),
        "delta_pct": _round_or_none(r.get("delta_pct"), 1),
        "is_excluded": int(r.get("is_excluded", 0)),
    }


def _window(today_df: pd.DataFrame, prior_df: pd.DataFrame, top_n: int,
            today_deep: bool, prior_deep: bool) -> dict:
    merged = today_df.merge(
        prior_df[["ticker", "rank", "users"]].rename(
            columns={"rank": "rank_then", "users": "users_then"}
        ),
        on="ticker", how="left",
    )
    merged["delta_users"] = merged["users"] - merged["users_then"]
    safe_then = merged["users_then"].where(merged["users_then"] > 0)
    merged["delta_pct"] = (merged["users"] / safe_then - 1) * 100
    merged["rank_change"] = merged["rank_then"] - merged["rank"]  # +ve = climbed

    matched = merged.dropna(subset=["users_then"])

    climbers_u = matched.nlargest(top_n, "delta_users")
    fallers_u = matched.nsmallest(top_n, "delta_users")
    rank_up = matched.dropna(subset=["rank_then"]).nlargest(top_n, "rank_change")
    rank_down = matched.dropna(subset=["rank_then"]).nsmallest(top_n, "rank_change")

    out = {
        "climbers_users": [_row(r) for _, r in climbers_u.iterrows()],
        "fallers_users":  [_row(r) for _, r in fallers_u.iterrows()],
        "rank_climbers":  [_row(r) for _, r in rank_up.iterrows()],
        "rank_fallers":   [_row(r) for _, r in rank_down.iterrows()],
    }

    # New entrants only computed when the universe is comparable on both
    # days. Otherwise rank-31-100 today would falsely appear as new
    # entrants relative to a top-30-only prior day.
    if today_deep and prior_deep:
        ne = merged[merged["users_then"].isna()].sort_values("rank").head(top_n * 2)
        out["new_entrants"] = [_row(r) for _, r in ne.iterrows()]
    else:
        out["new_entrants"] = []
        out["new_entrants_unavailable_reason"] = (
            "prior day captured fewer than 90 rows, can't reliably "
            "distinguish new entrants from previously-uncaptured names"
        )
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--out", type=Path, default=OUT_PATH)
    args = p.parse_args()

    if not HISTORY_PATH.exists():
        print("No history file yet, skipping movers.", file=sys.stderr)
        args.out.write_text(json.dumps({"as_of": None, "windows": {}}, indent=2))
        return 0

    df = pd.read_csv(HISTORY_PATH)
    if df.empty:
        args.out.write_text(json.dumps({"as_of": None, "windows": {}}, indent=2))
        return 0

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["rank"] = df["rank"].astype(int)
    df["users"] = df["users"].astype(int)

    today = max(df["date"])
    today_df = df[df["date"] == today].copy()
    today_deep = len(today_df) >= DEEP_UNIVERSE_THRESHOLD

    rows_per_day = df.groupby("date").size().to_dict()
    available_dates = sorted(rows_per_day.keys())

    windows: dict[str, dict] = {}
    for label, days in WINDOWS:
        target = today - dt.timedelta(days=days)
        prior_candidates = [d for d in available_dates if d <= target]
        if not prior_candidates:
            continue
        prior = max(prior_candidates)
        prior_df = df[df["date"] == prior].copy()
        prior_deep = len(prior_df) >= DEEP_UNIVERSE_THRESHOLD
        w = _window(today_df, prior_df, args.top_n, today_deep, prior_deep)
        w["from_date"] = prior.isoformat()
        w["from_rows"] = int(len(prior_df))
        w["to_date"] = today.isoformat()
        w["to_rows"] = int(len(today_df))
        w["label"] = label
        windows[label] = w

    out = {
        "as_of": today.isoformat(),
        "today_rows": int(len(today_df)),
        "deep_universe": today_deep,
        "available_windows": list(windows.keys()),
        "windows": windows,
        "history_days": len(available_dates),
    }
    args.out.write_text(json.dumps(out, indent=2, default=str))
    print(f"Wrote movers ({len(windows)} windows) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
