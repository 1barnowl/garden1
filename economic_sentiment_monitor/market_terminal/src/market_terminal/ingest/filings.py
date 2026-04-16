"""
market_terminal/ingest/filings.py

SEC EDGAR filing ingestion.  Two complementary sources:
  1. EDGAR RSS  — latest filings across all companies (fast, free)
  2. EDGAR EFTS — full-text search API for ticker-specific filing lookup

Forms ingested: 8-K, 10-K, 10-Q, DEF 14A (proxy), SC 13G/13D

Each filing is:
  - Validated via Pydantic FilingDoc model
  - Stored in the filings table
  - Indexed in filings_fts (FTS5) for keyword search
  - Scored with VADER sentiment on the description
  - Linked to the instrument via ticker
"""

import json
import time
from datetime import datetime, timezone, timedelta

import requests
import feedparser

from market_terminal.config.settings import log, WATCHLIST, ENTITY_MAP
from market_terminal.storage.database import get_db
from market_terminal.enrich.scorer import score_text
from market_terminal.enrich.entities import extract_keywords
from market_terminal.core.utils import ts_now, dedup_id, truncate, safe_float

# ── constants ─────────────────────────────────────────────────────────────────

EDGAR_RSS_BASE  = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_EFTS_BASE = "https://efts.sec.gov/LATEST/search-index"
EDGAR_COMPANY   = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_ATOM_URL  = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form}&dateb=&owner=include&count=40&search_text=&output=atom"

# Forms we care about, in priority order
TARGET_FORMS = ["8-K", "10-K", "10-Q", "DEF 14A", "SC 13G"]

# Map of ticker → CIK (populated lazily via EDGAR company lookup)
_CIK_CACHE: dict[str, str] = {}

# User-agent required by SEC robots.txt
HEADERS = {
    "User-Agent": "MarketTerminal research@example.com",
    "Accept-Encoding": "gzip, deflate",
}


# ── CIK lookup ────────────────────────────────────────────────────────────────

def get_cik(ticker: str) -> str | None:
    """Resolve ticker → CIK via EDGAR company search.  Cached."""
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]
    try:
        r = requests.get(
            "https://efts.sec.gov/LATEST/search-index?q=%22"
            + ticker + "%22&dateRange=custom&startdt=2020-01-01"
            "&forms=10-K&hits.hits._source=period_of_report,"
            "entity_name,file_num,period_of_report",
            headers=HEADERS, timeout=8
        )
        # Try the company tickers JSON (much faster)
        r2 = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=HEADERS, timeout=8
        )
        if r2.ok:
            data = r2.json()
            for entry in data.values():
                if entry.get("ticker", "").upper() == ticker.upper():
                    cik = str(entry["cik_str"]).zfill(10)
                    _CIK_CACHE[ticker] = cik
                    return cik
    except Exception as e:
        log.debug("CIK lookup error [%s]: %s", ticker, e)
    return None


# ── EDGAR RSS (latest filings, any company) ───────────────────────────────────

def fetch_edgar_rss(form_type: str = "8-K", limit: int = 40) -> list[dict]:
    """
    Pull the latest EDGAR filings of a given form type via Atom RSS.
    Returns raw entry dicts; caller maps to tickers.
    """
    url = EDGAR_ATOM_URL.format(form=form_type)
    try:
        feed = feedparser.parse(url)
        results = []
        for entry in feed.entries[:limit]:
            title   = getattr(entry, "title", "")
            link    = getattr(entry, "link",  "")
            updated = getattr(entry, "updated", ts_now())
            summary = getattr(entry, "summary", "")
            results.append({
                "form_type":   form_type,
                "title":       title,
                "url":         link,
                "filed_at":    updated[:10] if updated else "",
                "description": summary[:500],
                "company":     _extract_company(title),
            })
        return results
    except Exception as e:
        log.debug("EDGAR RSS error [%s]: %s", form_type, e)
        return []


def _extract_company(title: str) -> str:
    """Best-effort: EDGAR titles look like '10-K for ACME CORP (0001234567)'."""
    import re
    m = re.search(r" for (.+?) \(", title)
    return m.group(1).strip() if m else title[:60]


# ── EDGAR EFTS ticker-specific search ────────────────────────────────────────

def fetch_filings_for_ticker(ticker: str, forms: list[str] | None = None,
                              days_back: int = 90) -> list[dict]:
    """
    Use EDGAR's full-text search API to find recent filings for a specific ticker.
    Falls back gracefully if the API is unavailable.
    """
    forms     = forms or TARGET_FORMS
    cutoff    = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    results   = []

    for form in forms:
        try:
            r = requests.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q":        f'"{ticker}"',
                    "forms":    form,
                    "dateRange":"custom",
                    "startdt":  cutoff,
                    "hits.hits.total.value": 1,
                },
                headers=HEADERS, timeout=10,
            )
            if not r.ok:
                continue
            data = r.json()
            for hit in data.get("hits", {}).get("hits", [])[:5]:
                src = hit.get("_source", {})
                results.append({
                    "form_type":   form,
                    "ticker":      ticker,
                    "company":     src.get("entity_name", ""),
                    "filed_at":    src.get("file_date", "")[:10],
                    "period":      src.get("period_of_report", ""),
                    "url":         "https://www.sec.gov/Archives/edgar/data/"
                                   + src.get("file_num", ""),
                    "description": src.get("display_names", [ticker])[0] if
                                   src.get("display_names") else ticker,
                })
            time.sleep(0.15)   # SEC rate limit: ~10 req/s
        except Exception as e:
            log.debug("EDGAR EFTS [%s/%s]: %s", ticker, form, e)

    return results


# ── Save filings to DB ────────────────────────────────────────────────────────

def _enrich_filing(raw: dict) -> dict:
    """Add sentiment + keywords to a raw filing dict."""
    text = (raw.get("description") or raw.get("title") or "")
    sent = score_text(text)
    kws  = extract_keywords(text)
    raw["sentiment"] = round(safe_float(sent.get("score"), 0.0), 4)
    raw["keywords"]  = json.dumps(kws)
    raw["ingested"]  = ts_now()
    # generate stable id
    raw["id"] = dedup_id(
        raw.get("ticker", ""),
        raw.get("form_type", ""),
        raw.get("filed_at", ""),
        raw.get("url", ""),
    )
    return raw


def save_filings(rows: list[dict]) -> int:
    """Persist filings; return count of new rows inserted."""
    if not rows:
        return 0
    enriched = [_enrich_filing(r) for r in rows]
    n_new = 0
    with get_db() as db:
        for f in enriched:
            try:
                cur = db.execute("""
                    INSERT OR IGNORE INTO filings
                    (id, ticker, company, form_type, filed_at, period,
                     url, description, summary, sentiment, keywords, ingested)
                    VALUES
                    (:id, :ticker, :company, :form_type, :filed_at, :period,
                     :url, :description, :summary, :sentiment, :keywords, :ingested)
                """, {
                    "id":          f.get("id", ""),
                    "ticker":      f.get("ticker", ""),
                    "company":     f.get("company", ""),
                    "form_type":   f.get("form_type", ""),
                    "filed_at":    f.get("filed_at", ""),
                    "period":      f.get("period", ""),
                    "url":         f.get("url", ""),
                    "description": truncate(f.get("description", ""), 800),
                    "summary":     f.get("summary", ""),
                    "sentiment":   f.get("sentiment", 0.0),
                    "keywords":    f.get("keywords", "[]"),
                    "ingested":    f.get("ingested", ts_now()),
                })
                if cur.rowcount:
                    n_new += 1
            except Exception as e:
                log.debug("save_filings row error: %s", e)
    return n_new


# ── High-level refresh ────────────────────────────────────────────────────────

def fetch_all_filings(max_tickers: int = 20) -> int:
    """
    Full refresh cycle:
      1. Pull latest 8-K / 10-K from EDGAR RSS  (broad, any company)
      2. Ticker-specific EFTS lookup for watchlist equities
    Returns total new filings saved.
    """
    total = 0

    # 1. Broad RSS sweep
    for form in ["8-K", "10-K"]:
        rows = fetch_edgar_rss(form, limit=30)
        # Try to map company name → ticker via ENTITY_MAP
        mapped = []
        for row in rows:
            company_lower = row.get("company", "").lower()
            ticker = None
            for name, tk in ENTITY_MAP.items():
                if name in company_lower:
                    ticker = tk
                    break
            if ticker:
                row["ticker"] = ticker
                mapped.append(row)
        total += save_filings(mapped)
        time.sleep(0.5)

    # 2. Per-ticker EFTS for equity watchlist
    tickers = WATCHLIST.get("Equities", [])[:max_tickers]
    for ticker in tickers:
        rows = fetch_filings_for_ticker(ticker, forms=["8-K", "10-K", "10-Q"])
        for r in rows:
            r.setdefault("ticker", ticker)
        total += save_filings(rows)
        time.sleep(0.3)

    log.info("fetch_all_filings: %d new filings", total)
    return total
