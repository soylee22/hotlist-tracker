"""Scrape Trading 212 Hotlist Leaderboard.

Captures the top 30 instruments by user-ownership count, appends to
data/hotlist_history.csv as one row per (date, rank, ticker).

Usage:
    python scripts/scrape.py [--top N] [--out PATH]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
HOTLIST_URL = "https://www.trading212.com/hotlist"
HISTORY_PATH = ROOT / "data" / "hotlist_history.csv"
EXCLUSIONS_PATH = ROOT / "data" / "exclusions.json"


def load_exclusions() -> tuple[set[str], list[str]]:
    raw = json.loads(EXCLUSIONS_PATH.read_text())
    tickers = {t.upper() for t in raw.get("tickers", [])}
    patterns = [p.lower() for p in raw.get("name_patterns", [])]
    return tickers, patterns


def is_excluded(ticker: str, name: str, ex_tickers: set[str], ex_patterns: list[str]) -> bool:
    if ticker.upper() in ex_tickers:
        return True
    name_l = name.lower()
    return any(p in name_l for p in ex_patterns)


def scrape_top(n: int = 30) -> list[dict]:
    """Scrape the top-N rows of the Hotlist Leaderboard.

    Returns list of {rank, name, ticker, users}. Ticker may be empty for
    instruments where T212 does not show one prominently; we extract it
    from the underlying data attributes when possible.
    """
    rows: list[dict] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 1800},
        )
        page = ctx.new_page()
        page.goto(HOTLIST_URL, wait_until="domcontentloaded", timeout=60_000)
        # Wait for the leaderboard list to render. The page is a SPA.
        page.wait_for_load_state("networkidle", timeout=60_000)
        page.wait_for_timeout(2500)

        # T212 virtualises the leaderboard. Scroll the inner scroll area
        # to render lower-ranked rows. Try several scroll strategies to
        # accommodate different DOM structures.
        try:
            for y in (1000, 2500, 4000, 6000, 8000):
                page.evaluate(f"() => window.scrollTo(0, {y})")
                page.wait_for_timeout(350)
            # Try scrolling any internal scrollable container too.
            page.evaluate(
                "() => { document.querySelectorAll('*').forEach(el => {"
                " if (el.scrollHeight > el.clientHeight + 50) el.scrollTop = el.scrollHeight; }); }"
            )
            page.wait_for_timeout(800)
            page.evaluate("() => window.scrollTo(0, 0)")
            page.wait_for_timeout(500)
        except Exception:
            pass

        # Strategy: walk up from each user-count text node to a row container,
        # and capture the row's structured textContent. Then parse with regex
        # on the Python side, which is far more robust than ad-hoc DOM walks.
        raw = page.evaluate(
            """
            () => {
              const numRe = /^[\\d]{1,3}(,[\\d]{3})+$/;
              const isLeaf = el => el.children.length === 0;
              const candidates = Array.from(document.querySelectorAll('*'))
                .filter(el => isLeaf(el) && numRe.test((el.textContent || '').trim()));

              const rows = [];
              const seen = new Set();
              for (const el of candidates) {
                const users = parseInt(el.textContent.replace(/,/g, ''), 10);
                if (!Number.isFinite(users) || users < 5000) continue;

                // Walk up to find the smallest parent that contains BOTH
                // the user-count text AND at least 3 chars of other content
                // (i.e., a name or ticker).
                const userTxt = el.textContent.trim();
                let parent = el.parentElement;
                let chosen = null;
                let chosenLen = Infinity;
                for (let i = 0; i < 14 && parent; i++, parent = parent.parentElement) {
                  const txt = (parent.textContent || '').replace(/\\s+/g, ' ').trim();
                  if (txt.length === 0) continue;
                  if (!txt.includes(userTxt)) continue;
                  // Must contain other content beyond just the user count.
                  const stripped = txt.replace(userTxt, '').trim();
                  if (stripped.length < 3) continue;
                  // First match wins (smallest containing parent).
                  if (txt.length < chosenLen && txt.length <= 200) {
                    chosen = txt;
                    chosenLen = txt.length;
                    if (chosenLen < 60) break;  // good enough, single row
                  }
                  if (txt.length > 200) break;  // walked into multi-row
                }
                if (!chosen) continue;
                // Dedup keyed on user count alone (one row per unique count).
                if (seen.has(users)) continue;
                seen.add(users);
                rows.push({ raw: chosen, users, len: chosenLen });
              }
              return rows;
            }
            """
        )
        rows = _parse_raw_rows(raw, n)
        browser.close()
    return rows


_ROW_RE = re.compile(
    r"^\s*(?P<rank>\d{1,3})\s*"           # leading rank number
    r"(?P<rest>.+?)\s*"                    # ticker + name
    r"(?P<users>\d{1,3}(?:,\d{3})+)\s*$"   # users with commas
)


def _split_ticker_name(rest: str) -> tuple[str, str]:
    """Given a string like 'NVDANvidia' or 'JPMJPMorgan Chase & Co',
    split into (ticker, name).

    Strategy:
    1. Try a known-tricky-ticker prefix match (handles JPM, AGNC, SGLN where
       the lowercase-letter heuristic fails).
    2. Fall back to: ticker is the leading run of uppercase chars, name
       starts at the first lowercase letter (with the preceding uppercase
       letter belonging to the name as its initial capital).
    """
    if not rest:
        return "", ""

    # Strategy 1: tricky known prefixes
    for px in TRICKY_PREFIXES:
        if rest.startswith(px) and len(rest) > len(px):
            return px, rest[len(px):]

    # Strategy 2: lowercase-letter heuristic
    for i in range(len(rest)):
        ch = rest[i]
        if ch.islower():
            split = max(0, i - 1)
            return rest[:split], rest[split:]
        if i > 0 and ch.isspace():
            return rest[:i], rest[i + 1:]
    return rest, ""


def _parse_raw_rows(raw_rows: list[dict], n: int) -> list[dict]:
    parsed: list[dict] = []
    for r in raw_rows:
        m = _ROW_RE.match(r["raw"])
        if not m:
            continue
        rank = int(m.group("rank"))
        rest = m.group("rest")
        users = int(m.group("users").replace(",", ""))
        ticker, name = _split_ticker_name(rest)
        # Clean up name; drop any stray leading punctuation.
        name = name.strip().strip(",")
        if not name:
            continue
        parsed.append({"page_rank": rank, "ticker": ticker, "name": name, "users": users})
    # Deduplicate by ticker (keeping first occurrence, which is highest on the list).
    seen_t: set[str] = set()
    dedup: list[dict] = []
    for r in parsed:
        key = r["ticker"] or r["name"]
        if key in seen_t:
            continue
        seen_t.add(key)
        dedup.append(r)
    # Sort by users desc and take top n
    dedup.sort(key=lambda r: r["users"], reverse=True)
    out = []
    for i, r in enumerate(dedup[:n], start=1):
        out.append({
            "rank": i,
            "ticker": _normalise_ticker(r["ticker"], r["name"]),
            "name": r["name"],
            "users": r["users"],
        })
    return out


KNOWN_TICKERS = {
    "Nvidia": "NVDA",
    "Microsoft": "MSFT",
    "Apple": "AAPL",
    "Amazon": "AMZN",
    "Tesla": "TSLA",
    "Meta Platforms": "META",
    "Alphabet (Class A)": "GOOGL",
    "Alphabet (Class C)": "GOOG",
    "Coca-Cola": "KO",
    "Palantir": "PLTR",
    "Palantir Technologies": "PLTR",
    "Rolls-Royce": "RR.L",
    "Vanguard S&P 500 (Acc)": "VUAG",
    "Vanguard S&P 500 (Dist)": "VUSA",
    "Vanguard FTSE All-World (Acc)": "VWRP",
    "Vanguard FTSE All-World (Dist)": "VWRL",
    "iShares Physical Gold": "SGLN",
    "IBM": "IBM",
    "Berkshire Hathaway": "BRK-B",
    "Johnson & Johnson": "JNJ",
    "JPMorgan Chase & Co": "JPM",
    "JPMorgan Chase": "JPM",
    "Procter & Gamble": "PG",
    "Visa": "V",
    "Mastercard": "MA",
    "Walt Disney": "DIS",
    "Costco": "COST",
    "Walmart": "WMT",
    "AMD": "AMD",
    "Advanced Micro Devices": "AMD",
    "Broadcom": "AVGO",
    "Eli Lilly": "LLY",
    "Novo Nordisk": "NVO",
    "Realty Income": "O",
    "McDonald's": "MCD",
    "Cisco Systems": "CSCO",
    "Chevron": "CVX",
    "PepsiCo": "PEP",
    "Main Street Capital": "MAIN",
    "AGNC Investment": "AGNC",
    "BlackRock": "BLK",
    "General Dynamics": "GD",
    "BP": "BP.L",
}

# Tickers whose name starts with the same letters as the ticker (e.g. JPM
# preceding "JPMorgan", AGNC preceding "AGNC Investment"). The simple
# lowercase-letter splitter fails on these, so we try a longest-known-
# ticker-prefix match first.
TRICKY_PREFIXES = ["JPM", "AGNC", "SGLN"]


def _normalise_ticker(scraped: str, name: str) -> str:
    # Prefer our known mapping; fall back to scraped ticker.
    for k, v in KNOWN_TICKERS.items():
        if k.lower() == name.lower():
            return v
    return scraped or ""


def append_history(rows: list[dict], date: str | None = None) -> None:
    """Upsert today's rows into hotlist_history.csv. If rows already exist
    for `date`, they are replaced with the new scrape (idempotent re-runs)."""
    date = date or dt.date.today().isoformat()
    ex_t, ex_p = load_exclusions()
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

    header = ["date", "rank", "ticker", "name", "users", "is_excluded"]
    existing: list[list[str]] = []
    if HISTORY_PATH.exists() and HISTORY_PATH.stat().st_size > 0:
        with HISTORY_PATH.open(newline="") as f:
            r = csv.reader(f)
            try:
                first = next(r)
                if first != header:
                    existing.append(first)  # unexpected; preserve as a row
            except StopIteration:
                pass
            for row in r:
                if row and row[0] != date:
                    existing.append(row)

    with HISTORY_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in existing:
            w.writerow(row)
        for r in rows:
            excluded = is_excluded(r["ticker"], r["name"], ex_t, ex_p)
            w.writerow([date, r["rank"], r["ticker"], r["name"], r["users"], int(excluded)])


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=30)
    p.add_argument("--date", type=str, default=None, help="Override date (YYYY-MM-DD)")
    p.add_argument("--dry-run", action="store_true", help="Print to stdout, do not append")
    args = p.parse_args()

    rows = scrape_top(args.top)
    if not rows:
        print("ERROR: no rows scraped", file=sys.stderr)
        return 1

    if args.dry_run:
        for r in rows:
            print(f"{r['rank']:2d}  {r['ticker']:<8} {r['name']:<40} {r['users']:>10,}")
        return 0

    append_history(rows, date=args.date)
    print(f"Wrote {len(rows)} rows to {HISTORY_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
