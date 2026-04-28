"""Daily digest email for the Hotlist 10.

Composes an editorial HTML email summarising today's basket, watchlist,
exit watch and performance vs VUAG.L. Sends via Gmail SMTP using an
app password.

Required env vars:
    GMAIL_APP_PASSWORD — Gmail app password (16 chars, no spaces)

Optional:
    DIGEST_TO — recipient (default: leeslater1992@gmail.com)
    DIGEST_FROM — sender (default: leeslater1992@gmail.com)
    DIGEST_DRY_RUN — if "1", print to stdout instead of sending
    DASHBOARD_URL — link to live dashboard (default: GH Pages URL)
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = ROOT / "data" / "portfolio_state.json"
HISTORY_PATH = ROOT / "data" / "hotlist_history.csv"
PERF_PATH = ROOT / "data" / "performance.csv"
TRADE_LOG = ROOT / "data" / "trade_log.csv"
SENT_MARKER = ROOT / "data" / ".last_email_date"

sys.path.insert(0, str(ROOT / "scripts"))
from compute import deltas_for_basket, watch_list, load_history, load_state, HYSTERESIS_DAYS  # noqa: E402

DEFAULT_TO = "leeslater1992@gmail.com"
DEFAULT_FROM = "leeslater1992@gmail.com"
DEFAULT_DASH = "https://soylee22.github.io/hotlist-tracker/"


def fmt_pct(v):
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}%"


def fmt_user_delta(v):
    """Format absolute user-count delta with sign + thousands separator."""
    if v is None:
        return ""
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,}"


def fmt_rank_arrow(change, is_new=False):
    """Up/down arrow for rank change. Positive = moved UP (lower rank number)."""
    if is_new:
        return "NEW"
    if change is None:
        return ""
    if change == 0:
        return "→"
    if change > 0:
        return f"↑{change}"
    return f"↓{abs(change)}"


def rank_color(change, is_new=False):
    if is_new:
        return "#B8862F"  # gold for new entries
    if change is None or change == 0:
        return "#76695E"
    return "#2F5C39" if change > 0 else "#B7372E"


def delta_color(v):
    if v is None or v == 0:
        return "#76695E"
    return "#2F5C39" if v > 0 else "#B7372E"


def compose_subject(state: dict, basket_rows: list[dict], exit_watch: dict) -> str:
    today = dt.date.today().strftime("%-d %b")
    confirmed_exit = any(d >= HYSTERESIS_DAYS for d in exit_watch.values())
    if confirmed_exit:
        return f"Hotlist 10 — {today} · ROTATION TRIGGERED"
    watching = [t for t, d in exit_watch.items() if 0 < d < HYSTERESIS_DAYS]
    if watching:
        return f"Hotlist 10 — {today} · watching {', '.join(watching)}"
    # Movement headline: largest absolute 1-day delta
    movers = [(r["ticker"], r["delta_1d_pct"]) for r in basket_rows if r["delta_1d_pct"] is not None]
    movers.sort(key=lambda kv: abs(kv[1]) if kv[1] is not None else 0, reverse=True)
    if movers and movers[0][1] is not None and abs(movers[0][1]) >= 0.3:
        t, d = movers[0]
        return f"Hotlist 10 — {today} · {t} {fmt_pct(d)}"
    return f"Hotlist 10 — {today} · steady"


def render_html(state: dict, basket_rows: list[dict], watch: list[dict],
                exit_watch_rows: list[dict], perf_df: pd.DataFrame,
                dashboard_url: str, history_days: int = 0) -> str:
    today = dt.date.today().isoformat()
    inception = state.get("inception_date") or "—"
    days_tracked = 0
    if state.get("inception_date") and state.get("last_updated"):
        days_tracked = (pd.to_datetime(state["last_updated"]).date()
                        - pd.to_datetime(state["inception_date"]).date()).days

    # Determine which delta columns have any data to show. If a column is
    # all None across the basket, hide it entirely. Avoids the "—" sea
    # that makes day-1 emails look broken.
    show_1d  = any(r.get("delta_1d_pct")  is not None for r in basket_rows)
    show_7d  = any(r.get("delta_7d_pct")  is not None for r in basket_rows)
    show_30d = any(r.get("delta_30d_pct") is not None for r in basket_rows)
    is_day_one = history_days <= 1

    # Headline perf — only meaningful with >=2 days of history
    perf_pct = "—"
    vs_bench = "—"
    perf_color = "#76695E"
    has_perf = (not perf_df.empty) and (len(perf_df) >= 2)
    if has_perf:
        last = perf_df.iloc[-1]
        p = float(last["portfolio_return_pct"])
        b = float(last["benchmark_return_pct"])
        diff = p - b
        perf_pct = ("+" if p >= 0 else "") + f"{p:.2f}%"
        vs_bench = ("+" if diff >= 0 else "") + f"{diff:.2f}pp"
        perf_color = "#2F5C39" if p >= 0 else "#B7372E"

    # Day-one banner replaces the "all dashes" experience
    day_one_banner = ""
    if is_day_one:
        day_one_banner = """
        <table role="presentation" style="width:100%; margin:24px 0 8px;">
          <tr>
            <td style="background:#FAF1DC; border-left:4px solid #B8862F; padding:14px 18px;
                       font-family:'Source Serif 4', Georgia, serif; font-style:italic;
                       font-size:14px; color:#4A413A; line-height:1.5;">
              <strong style="font-style:normal; color:#B8862F; text-transform:uppercase;
                              letter-spacing:0.15em; font-size:10px; font-family:Inter,Arial,sans-serif;">
                Day 1 of tracking
              </strong><br>
              Tonight's snapshot is the inception. Day-over-day, week-over-week and
              since-inception comparisons populate from tomorrow's run onwards.
              The performance series will become meaningful over the next 5–7 trading days.
            </td>
          </tr>
        </table>"""

    # Basket rows. Conditionally include delta columns only when at least
    # one row has a non-None value for that timeframe.
    def _delta_cell(v):
        return (
            f'<td style="padding:9px 8px; border-bottom:1px solid #E8DFCC; text-align:right; '
            f'font-family:Inter,Arial,sans-serif; font-variant-numeric:tabular-nums; '
            f'color:{delta_color(v)}; font-weight:600;">{fmt_pct(v)}</td>'
        )

    basket_html = ""
    for r in basket_rows:
        wpct = r.get("weight_pct") or 0
        d1u = r.get("delta_1d_users")
        rk_change = r.get("rank_change")
        rk_filt = r.get("rank_filtered")

        # Users cell: count, then on second line a Δ users number when available
        d1u_html = ""
        if d1u is not None:
            d1u_html = f'<div style="font-size:10px; color:{delta_color(d1u)}; font-weight:600; margin-top:1px;">{fmt_user_delta(d1u)}</div>'

        # Rank cell: filtered rank within top-10 single stocks, with arrow when available
        rank_display = f"#{rk_filt}" if rk_filt is not None else f"#{r['rank']}"
        arrow_html = ""
        if rk_change is not None:
            arrow_html = f'<span style="color:{rank_color(rk_change)}; font-weight:700; margin-left:4px; font-size:11px;">{fmt_rank_arrow(rk_change)}</span>'

        delta_cells = ""
        if show_1d:
            delta_cells += _delta_cell(r.get("delta_1d_pct"))
        if show_7d:
            delta_cells += _delta_cell(r.get("delta_7d_pct"))
        if show_30d:
            delta_cells += _delta_cell(r.get("delta_30d_pct"))

        basket_html += f"""
            <tr>
              <td style="padding:9px 8px; border-bottom:1px solid #E8DFCC;">
                <div style="font-family:'JetBrains Mono', Menlo, monospace; font-size:11px; color:#6E1A22; font-weight:600;">{r['ticker']}</div>
                <div style="font-size:13px; color:#1A1715; font-weight:600;">{r['name']}</div>
              </td>
              <td style="padding:9px 8px; border-bottom:1px solid #E8DFCC; text-align:right; font-family:Inter,Arial,sans-serif; font-variant-numeric:tabular-nums;">
                <div style="font-size:13px; color:#1A1715;">{r['users']:,}</div>
                {d1u_html}
              </td>
              <td style="padding:9px 8px; border-bottom:1px solid #E8DFCC; text-align:right; font-family:Inter,Arial,sans-serif; font-variant-numeric:tabular-nums;">
                <span style="font-size:13px; color:#1A1715; font-weight:600;">{rank_display}</span>{arrow_html}
              </td>
              <td style="padding:9px 8px; border-bottom:1px solid #E8DFCC; text-align:right; font-family:Inter,Arial,sans-serif; font-weight:700; color:#6E1A22;">{wpct}%</td>
              {delta_cells}
            </tr>"""

    # Exit watch
    exit_html = ""
    if exit_watch_rows:
        rows_h = ""
        for r in exit_watch_rows:
            badge_bg, badge_color, badge_text = "#E4EDE5", "#2F5C39", "Inside top 10"
            if r["days"] >= HYSTERESIS_DAYS:
                badge_bg, badge_color, badge_text = "#FCE9E7", "#B7372E", "Sale triggered"
            elif r["days"] > 0:
                badge_bg, badge_color, badge_text = "#FAF1DC", "#B8862F", f"Watching {r['days']}/{HYSTERESIS_DAYS}d"
            else:
                continue  # don't show rows with 0 days outside
            rows_h += f"""
                <tr>
                  <td style="padding:8px 10px; font-family:'JetBrains Mono', Menlo, monospace; font-size:12px; color:#6E1A22; font-weight:600;">{r['ticker']}</td>
                  <td style="padding:8px 10px; text-align:right; font-family:Inter,Arial,sans-serif; font-size:12px; color:#1A1715;">{r['days']}/{HYSTERESIS_DAYS} days</td>
                  <td style="padding:8px 10px; text-align:right;">
                    <span style="display:inline-block; padding:3px 10px; border-radius:100px; background:{badge_bg}; color:{badge_color}; font-family:Inter,Arial,sans-serif; font-size:10px; text-transform:uppercase; letter-spacing:0.1em; font-weight:700;">{badge_text}</span>
                  </td>
                </tr>"""
        if rows_h:
            exit_html = f"""
            <h2 style="font-family:'Playfair Display', Georgia, serif; font-size:22px; font-weight:700; color:#1A1715; margin:36px 0 8px;">Exit watch</h2>
            <p style="margin:0 0 12px; color:#4A413A; font-style:italic; font-size:14px;">Held names that have dropped outside the Top 10. {HYSTERESIS_DAYS} consecutive days triggers a rotation.</p>
            <table style="width:100%; border-collapse:collapse; background:#FBF6EE; border:1px solid #D9CFBE;">
              <tbody>{rows_h}</tbody>
            </table>"""

    # Watch list (top 5 climbing names)
    watch_top = watch[:5]
    watch_html = ""
    if watch_top:
        rows_h = ""
        for w in watch_top:
            days = w.get("days_in_top10", 0)
            d1u = w.get("delta_1d_users")
            rk_change = w.get("rank_change")
            is_new = w.get("is_new", False)

            # Rank cell with arrow (or NEW badge)
            arrow_html = ""
            if is_new:
                arrow_html = '<span style="color:#B8862F; font-weight:700; margin-left:4px; font-size:10px;">NEW</span>'
            elif rk_change is not None:
                arrow_html = f'<span style="color:{rank_color(rk_change)}; font-weight:700; margin-left:4px; font-size:10px;">{fmt_rank_arrow(rk_change)}</span>'

            # Users cell with optional Δ
            d1u_html = ""
            if d1u is not None:
                d1u_html = f'<div style="font-size:10px; color:{delta_color(d1u)}; font-weight:600; margin-top:1px;">{fmt_user_delta(d1u)}</div>'

            badge = ""
            if days >= HYSTERESIS_DAYS:
                badge = '<span style="display:inline-block; padding:2px 8px; border-radius:100px; background:#E4EDE5; color:#2F5C39; font-family:Inter,Arial,sans-serif; font-size:10px; text-transform:uppercase; letter-spacing:0.1em; font-weight:700;">Confirmed</span>'
            elif days > 0:
                badge = f'<span style="color:#B8862F; font-family:Inter,Arial,sans-serif; font-size:11px;">In top 10 for {days}/{HYSTERESIS_DAYS}d</span>'

            rows_h += f"""
                <tr>
                  <td style="padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:11px; color:#76695E; vertical-align:top;">#{w['filtered_rank']}{arrow_html}</td>
                  <td style="padding:8px 10px; vertical-align:top;">
                    <span style="font-family:'JetBrains Mono', Menlo, monospace; font-size:12px; color:#6E1A22; font-weight:600;">{w['ticker']}</span>
                    <span style="font-size:13px; color:#1A1715; margin-left:6px;">{w['name']}</span>
                  </td>
                  <td style="padding:8px 10px; text-align:right; font-family:Inter,Arial,sans-serif; font-variant-numeric:tabular-nums; vertical-align:top;">
                    <div style="font-size:13px; color:#1A1715;">{w['users']:,}</div>
                    {d1u_html}
                  </td>
                  <td style="padding:8px 10px; text-align:right; vertical-align:top;">{badge}</td>
                </tr>"""
        watch_html = f"""
            <h2 style="font-family:'Playfair Display', Georgia, serif; font-size:22px; font-weight:700; color:#1A1715; margin:36px 0 8px;">Climbing the ranks</h2>
            <p style="margin:0 0 12px; color:#4A413A; font-style:italic; font-size:14px;">Top single stocks not currently in the basket — early warning for what could rotate in next.</p>
            <table style="width:100%; border-collapse:collapse; background:#FBF6EE; border:1px solid #D9CFBE;">
              <tbody>{rows_h}</tbody>
            </table>"""

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Hotlist 10 daily digest</title></head>
<body style="margin:0; padding:0; background:#F4ECDD;">
<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%; background:#F4ECDD;">
  <tr><td align="center" style="padding:32px 16px;">
    <table role="presentation" cellpadding="0" cellspacing="0" style="width:100%; max-width:640px; background:#FBF6EE; border-top:6px solid #6E1A22;">
      <tr><td style="padding:32px 32px 8px;">

        <!-- Masthead -->
        <table role="presentation" style="width:100%; border-bottom:1px solid #6E1A22; padding-bottom:10px; margin-bottom:24px;">
          <tr>
            <td style="font-family:Inter,Arial,sans-serif; font-size:10px; text-transform:uppercase; letter-spacing:0.2em; color:#6E1A22; font-weight:700;">
              <span style="color:#1A1715;">THE HOTLIST 10</span> · DAILY DIGEST
            </td>
            <td style="text-align:right; font-family:Inter,Arial,sans-serif; font-size:10px; text-transform:uppercase; letter-spacing:0.18em; color:#76695E;">
              {today}
            </td>
          </tr>
        </table>

        <!-- Hero -->
        <h1 style="font-family:'Playfair Display', Georgia, serif; font-size:36px; line-height:1.05; font-weight:900; color:#1A1715; margin:0 0 8px; letter-spacing:-0.02em;">
          The Hotlist 10 <em style="font-style:italic; font-weight:400; color:#6E1A22;">— today.</em>
        </h1>
        <p style="font-family:'Source Serif 4', Georgia, serif; font-style:italic; font-size:15px; color:#4A413A; margin:0 0 28px; line-height:1.5;">
          Top-10 most-owned single stocks on Trading 212. Ownership-weighted, drift-held, rotated on confirmed exit.
        </p>

        {day_one_banner}

        <!-- Stat grid -->
        <table role="presentation" style="width:100%; border-collapse:separate; border-spacing:6px;">
          <tr>
            <td style="background:#F4ECDD; border-top:3px solid #6E1A22; padding:14px 16px; text-align:center; width:25%;">
              <div style="font-family:'Playfair Display', Georgia, serif; font-size:24px; font-weight:700; color:#6E1A22; line-height:1;">{len(basket_rows)}</div>
              <div style="font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#76695E; font-weight:600; margin-top:5px;">Stocks held</div>
            </td>
            <td style="background:#F4ECDD; border-top:3px solid #6E1A22; padding:14px 16px; text-align:center; width:25%;">
              <div style="font-family:'Playfair Display', Georgia, serif; font-size:24px; font-weight:700; color:#6E1A22; line-height:1;">{days_tracked}</div>
              <div style="font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#76695E; font-weight:600; margin-top:5px;">Days tracked</div>
            </td>
            <td style="background:#F4ECDD; border-top:3px solid #6E1A22; padding:14px 16px; text-align:center; width:25%;">
              <div style="font-family:'Playfair Display', Georgia, serif; font-size:24px; font-weight:700; color:{perf_color}; line-height:1;">{perf_pct}</div>
              <div style="font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#76695E; font-weight:600; margin-top:5px;">Portfolio</div>
            </td>
            <td style="background:#F4ECDD; border-top:3px solid #6E1A22; padding:14px 16px; text-align:center; width:25%;">
              <div style="font-family:'Playfair Display', Georgia, serif; font-size:24px; font-weight:700; color:{perf_color}; line-height:1;">{vs_bench}</div>
              <div style="font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#76695E; font-weight:600; margin-top:5px;">vs VUAG.L</div>
            </td>
          </tr>
        </table>

        <!-- Basket -->
        <h2 style="font-family:'Playfair Display', Georgia, serif; font-size:22px; font-weight:700; color:#1A1715; margin:36px 0 12px;">Today's basket</h2>
        <table role="presentation" style="width:100%; border-collapse:collapse; background:#FBF6EE; border:1px solid #D9CFBE;">
          <thead>
            <tr style="background:#F4ECDD;">
              <th style="text-align:left; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">Stock</th>
              <th style="text-align:right; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">Users</th>
              <th style="text-align:right; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">Rank</th>
              <th style="text-align:right; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">Wt</th>
              {('<th style="text-align:right; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">1d</th>') if show_1d else ''}
              {('<th style="text-align:right; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">7d</th>') if show_7d else ''}
              {('<th style="text-align:right; padding:8px 10px; font-family:Inter,Arial,sans-serif; font-size:9px; text-transform:uppercase; letter-spacing:0.16em; color:#4A413A; border-bottom:2px solid #1A1715;">30d</th>') if show_30d else ''}
            </tr>
          </thead>
          <tbody>{basket_html}</tbody>
        </table>

        {exit_html}
        {watch_html}

        <!-- CTA -->
        <table role="presentation" style="width:100%; margin-top:36px;">
          <tr>
            <td style="background:#6E1A22; padding:20px 24px; border-left:6px solid #B8862F;">
              <div style="font-family:Inter,Arial,sans-serif; font-size:10px; text-transform:uppercase; letter-spacing:0.2em; color:rgba(255,255,255,0.7); margin-bottom:8px; font-weight:600;">Live dashboard</div>
              <a href="{dashboard_url}" style="font-family:'Source Serif 4', Georgia, serif; font-size:18px; color:white; text-decoration:none; font-style:italic;">{dashboard_url}</a>
            </td>
          </tr>
        </table>

        <!-- Footer -->
        <p style="margin-top:32px; padding-top:18px; border-top:1px solid #D9CFBE; font-family:'Source Serif 4', Georgia, serif; font-style:italic; font-size:12px; color:#76695E; text-align:center;">
          Inception {inception} · Hysteresis {HYSTERESIS_DAYS}d · Rule v{state.get('rule_version', '1.1')}<br>
          A rule I follow, not investment advice. Past performance does not predict future returns.
        </p>

      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""


def main() -> int:
    # Idempotency guard: skip if today's email has already been sent.
    # Marker file gets committed with the rest of data/, so subsequent
    # runs on the same UTC date see today's marker and exit silently.
    today = dt.date.today().isoformat()
    skip_idempotency = os.environ.get("DIGEST_FORCE") == "1"
    if not skip_idempotency and SENT_MARKER.exists():
        last_sent = SENT_MARKER.read_text().strip()
        if last_sent == today:
            print(f"Email already sent today ({today}). Skipping. (set DIGEST_FORCE=1 to override)")
            return 0

    state = load_state()
    history = load_history()
    basket_rows = deltas_for_basket(history, state)
    watch = watch_list(history, state)
    history_days = len(set(history["date"].tolist())) if not history.empty else 0
    exit_watch_rows = [
        {"ticker": k, "days": v}
        for k, v in (state.get("exit_watch") or {}).items()
    ]
    exit_watch_rows.sort(key=lambda r: r["days"], reverse=True)
    perf_df = pd.read_csv(PERF_PATH) if PERF_PATH.exists() else pd.DataFrame()

    dashboard_url = os.environ.get("DASHBOARD_URL", DEFAULT_DASH)
    subject = compose_subject(state, basket_rows, dict(state.get("exit_watch") or {}))
    if history_days <= 1:
        subject = f"Hotlist 10 — {dt.date.today().strftime('%-d %b')} · Day 1 of tracking"
    html = render_html(state, basket_rows, watch, exit_watch_rows, perf_df, dashboard_url, history_days=history_days)

    sender = os.environ.get("DIGEST_FROM", DEFAULT_FROM)
    recipient = os.environ.get("DIGEST_TO", DEFAULT_TO)

    if os.environ.get("DIGEST_DRY_RUN") == "1":
        print(f"To: {recipient}\nFrom: {sender}\nSubject: {subject}\n")
        out = ROOT / "dashboard" / "_email_preview.html"
        out.write_text(html)
        print(f"Preview saved to {out}")
        return 0

    pwd = os.environ.get("GMAIL_APP_PASSWORD")
    if not pwd:
        print("ERROR: GMAIL_APP_PASSWORD env var not set", file=sys.stderr)
        return 1

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(sender, pwd)
        s.sendmail(sender, [recipient], msg.as_string())
    print(f"Sent: {subject}")

    # Update marker file so subsequent runs on the same date skip.
    SENT_MARKER.parent.mkdir(parents=True, exist_ok=True)
    SENT_MARKER.write_text(today)
    return 0


if __name__ == "__main__":
    sys.exit(main())
