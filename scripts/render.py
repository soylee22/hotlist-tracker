"""Render the dashboard HTML from current state and history."""
from __future__ import annotations

import csv
import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = ROOT / "data" / "portfolio_state.json"
HISTORY_PATH = ROOT / "data" / "hotlist_history.csv"
TRADE_LOG = ROOT / "data" / "trade_log.csv"
PERF_PATH = ROOT / "data" / "performance.csv"
MOVERS_PATH = ROOT / "data" / "movers.json"
DASHBOARD_DIR = ROOT / "dashboard"
TEMPLATE = "template.html"
OUTPUT = DASHBOARD_DIR / "index.html"


# Reuse compute helpers
sys.path.insert(0, str(ROOT / "scripts"))
from compute import deltas_for_basket, watch_list, load_history, load_state, HYSTERESIS_DAYS  # noqa: E402


def perf_svg(perf_df: pd.DataFrame, w: int = 980, h: int = 320) -> str:
    if perf_df.empty or len(perf_df) < 2:
        return ""
    pf = perf_df["portfolio_gbp"].astype(float).tolist()
    bm = perf_df["benchmark_gbp"].astype(float).tolist()
    n = len(perf_df)
    lo = min(min(pf), min(bm)) * 0.99
    hi = max(max(pf), max(bm)) * 1.01
    if hi <= lo:
        hi = lo + 1
    pad_l, pad_r, pad_t, pad_b = 56, 16, 16, 32

    def x(i): return pad_l + (w - pad_l - pad_r) * (i / (n - 1))
    def y(v): return pad_t + (h - pad_t - pad_b) * (1 - (v - lo) / (hi - lo))

    def path(values):
        d = []
        for i, v in enumerate(values):
            d.append(("M" if i == 0 else "L") + f"{x(i):.1f},{y(v):.1f}")
        return " ".join(d)

    # Y axis ticks
    ticks = []
    for i in range(5):
        v = lo + (hi - lo) * i / 4
        ticks.append((v, y(v)))

    grid = "".join(
        f'<line x1="{pad_l}" x2="{w - pad_r}" y1="{ty:.1f}" y2="{ty:.1f}" '
        f'stroke="#D9CFBE" stroke-width="0.5" />'
        for _, ty in ticks
    )
    labels = "".join(
        f'<text x="{pad_l - 8}" y="{ty + 4:.1f}" text-anchor="end" '
        f'font-family="Inter, sans-serif" font-size="10" fill="#76695E">£{tv:,.0f}</text>'
        for tv, ty in ticks
    )
    # Date labels: first, middle, last
    date_labels = []
    for idx in (0, n // 2, n - 1):
        if 0 <= idx < n:
            date_labels.append(
                f'<text x="{x(idx):.1f}" y="{h - pad_b + 18:.1f}" text-anchor="middle" '
                f'font-family="Inter, sans-serif" font-size="10" fill="#76695E">'
                f'{perf_df.iloc[idx]["date"]}</text>'
            )

    return (
        f'<svg viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">'
        f"{grid}{labels}"
        f'<path d="{path(bm)}" stroke="#B8862F" stroke-width="2" fill="none" stroke-linejoin="round" stroke-linecap="round" />'
        f'<path d="{path(pf)}" stroke="#6E1A22" stroke-width="2.5" fill="none" stroke-linejoin="round" stroke-linecap="round" />'
        f'{"".join(date_labels)}'
        f"</svg>"
    )


def read_trades() -> list[dict]:
    if not TRADE_LOG.exists():
        return []
    out = []
    with TRADE_LOG.open() as f:
        r = csv.DictReader(f)
        for row in r:
            if not row.get("date"):
                continue
            try:
                row["sold_users"] = int(row.get("sold_users") or 0)
                row["bought_users"] = int(row.get("bought_users") or 0)
            except ValueError:
                row["sold_users"] = 0
                row["bought_users"] = 0
            out.append(row)
    out.reverse()  # newest first
    return out


def delta_class(v):
    if v is None:
        return "delta-zero"
    if v > 0:
        return "delta-pos"
    if v < 0:
        return "delta-neg"
    return "delta-zero"


def pct_or_dash(v):
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}%"


def main() -> int:
    history = load_history()
    state = load_state()
    basket_rows = deltas_for_basket(history, state)
    watch = watch_list(history, state)
    history_days = len(set(history["date"].tolist())) if not history.empty else 0

    perf_df = pd.DataFrame()
    if PERF_PATH.exists():
        try:
            perf_df = pd.read_csv(PERF_PATH)
        except Exception:
            perf_df = pd.DataFrame()

    movers = {}
    movers_window = None
    if MOVERS_PATH.exists():
        try:
            movers = json.loads(MOVERS_PATH.read_text())
            available = movers.get("available_windows") or []
            # Prefer 7d if available (more meaningful than 1d), else 30d, else 1d
            for pref in ("7d", "30d", "1d"):
                if pref in available:
                    movers_window = movers["windows"][pref]
                    movers_window["window_label"] = pref
                    break
        except Exception:
            movers = {}
            movers_window = None

    inception = state.get("inception_date")
    last_updated = state.get("last_updated") or "—"
    if inception:
        days_tracked = (pd.to_datetime(state.get("last_updated") or dt.date.today().isoformat()).date()
                        - pd.to_datetime(inception).date()).days
    else:
        days_tracked = 0

    # Determine which delta columns have any signal (suppress all-empty ones)
    show_1d  = any(r.get("delta_1d_pct")  is not None for r in basket_rows)
    show_7d  = any(r.get("delta_7d_pct")  is not None for r in basket_rows)
    show_30d = any(r.get("delta_30d_pct") is not None for r in basket_rows)
    show_ytd = any(r.get("delta_ytd_pct") is not None for r in basket_rows)
    show_inc = any(r.get("delta_inception_pct") is not None for r in basket_rows)
    is_day_one = history_days <= 1

    # Headline performance numbers (only meaningful with >=2 days of perf data)
    has_perf = (not perf_df.empty) and (len(perf_df) >= 2)
    if has_perf:
        last = perf_df.iloc[-1]
        perf_pct_v = float(last["portfolio_return_pct"])
        bm_pct_v = float(last["benchmark_return_pct"])
        diff = perf_pct_v - bm_pct_v
        perf_pct = ("+" if perf_pct_v >= 0 else "") + f"{perf_pct_v:.2f}%"
        vs_bench = ("+" if diff >= 0 else "") + f"{diff:.2f}pp"
        perf_class = "green" if perf_pct_v >= 0 else "red"
        vs_bench_class = "green" if diff >= 0 else "red"
    else:
        perf_pct = "—"
        vs_bench = "—"
        perf_class = ""
        vs_bench_class = ""

    exit_watch_rows = [
        {"ticker": k, "days": v}
        for k, v in (state.get("exit_watch") or {}).items()
    ]
    exit_watch_rows.sort(key=lambda r: r["days"], reverse=True)

    env = Environment(
        loader=FileSystemLoader(str(DASHBOARD_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    env.globals["delta_class"] = delta_class
    env.globals["pct_or_dash"] = pct_or_dash
    tmpl = env.get_template(TEMPLATE)

    html = tmpl.render(
        rule_version=state.get("rule_version", "1.0"),
        today=dt.date.today().isoformat(),
        basket=basket_rows,
        basket_size=len(basket_rows),
        watch=watch,
        exit_watch_rows=exit_watch_rows,
        hysteresis_days=HYSTERESIS_DAYS,
        days_tracked=days_tracked,
        perf_pct=perf_pct,
        vs_bench=vs_bench,
        perf_class=perf_class,
        vs_bench_class=vs_bench_class,
        has_perf=has_perf,
        perf_svg=perf_svg(perf_df) if has_perf else "",
        trades=read_trades(),
        inception_date=inception or "—",
        last_updated=last_updated,
        is_day_one=is_day_one,
        history_days=history_days,
        show_1d=show_1d, show_7d=show_7d, show_30d=show_30d,
        show_ytd=show_ytd, show_inc=show_inc,
        movers=movers,
        movers_window=movers_window,
    )
    OUTPUT.write_text(html)
    print(f"Wrote dashboard to {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
