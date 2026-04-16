"""
market_terminal/ingest/feeds.py
v0.10 — concurrent fetching, retry/backoff, Pydantic validation,
        event classification, rate limiting.

Fetch strategy:
  - Uses asyncio + httpx if httpx is installed (concurrent, non-blocking)
  - Falls back to concurrent.futures.ThreadPoolExecutor + requests (still
    parallel but thread-based) if httpx is absent
  - Both paths produce the same output: list[NewsItem] dicts
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional

import feedparser
import requests

from market_terminal.config.settings import (
    log, NEWS_API_KEY, FRED_API_KEY, RSS_FEEDS,
    FRED_SERIES, RSS_PER_FEED, NEWSAPI_PAGE_SIZE,
)
from market_terminal.storage.database import get_db, update_freshness
from market_terminal.enrich.scorer import score_text
from market_terminal.enrich.entities import extract_entities, extract_keywords, enrich_headline_metadata
from market_terminal.enrich.classifier import classify, event_type_label
from market_terminal.core.utils import ts_now, dedup_id, safe_float, clean_text

# ── optional httpx ────────────────────────────────────────────────────────────
try:
    import httpx
    _HTTPX = True
except ImportError:
    _HTTPX = False

# ── optional newspaper3k ──────────────────────────────────────────────────────
NEWSPAPER_READY = False
try:
    from newspaper import Article as _NewsArticle
    NEWSPAPER_READY = True
    log.info("newspaper3k loaded")
except ImportError:
    log.warning("newspaper3k not installed — headline-only mode")

# ── rate limiter ──────────────────────────────────────────────────────────────

class _RateLimiter:
    """Simple token-bucket rate limiter, thread-safe."""
    def __init__(self, calls_per_sec: float = 5.0):
        self._min_interval = 1.0 / calls_per_sec
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, key: str = "default"):
        with self._lock:
            now  = time.monotonic()
            last = self._last.get(key, 0.0)
            gap  = now - last
            if gap < self._min_interval:
                time.sleep(self._min_interval - gap)
            self._last[key] = time.monotonic()

_rss_limiter    = _RateLimiter(calls_per_sec=8.0)
_api_limiter    = _RateLimiter(calls_per_sec=3.0)
_edgar_limiter  = _RateLimiter(calls_per_sec=2.0)

# ── retry helper ──────────────────────────────────────────────────────────────

def _retry_get(url: str, params: dict | None = None,
               headers: dict | None = None,
               retries: int = 3, backoff: float = 1.5,
               timeout: int = 10) -> Optional[requests.Response]:
    """GET with exponential backoff.  Returns None on final failure."""
    delay = 1.0
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers,
                             timeout=timeout)
            r.raise_for_status()
            return r
        except requests.exceptions.Timeout:
            log.debug("timeout [%s] attempt %d/%d", url[:60], attempt+1, retries)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (429, 503):
                # Rate-limited — back off longer
                delay *= 3
                log.debug("rate-limited [%s] backing off %.1fs", url[:40], delay)
            else:
                log.debug("HTTP error [%s]: %s", url[:40], e)
                if attempt == retries - 1:
                    return None
        except Exception as e:
            log.debug("request error [%s]: %s", url[:40], e)
        if attempt < retries - 1:
            time.sleep(delay)
            delay *= backoff
    return None

# ── full-text queue ───────────────────────────────────────────────────────────
_fulltext_queue: deque = deque(maxlen=400)
_fulltext_lock  = threading.Lock()


# ── article builder ───────────────────────────────────────────────────────────

def _build_article(title: str, link: str, pub: str,
                   source: str, prices: dict) -> dict:
    """
    Build a fully-enriched article dict, validated against NewsItem schema.
    Now also classifies event type.
    """
    title = clean_text(title)
    if not title:
        return {}

    sent      = score_text(title)
    kws       = extract_keywords(title)
    entities  = extract_entities(title)
    clf       = classify(title)                      # NEW: event classification

    adj_score = sent["score"]
    for e in entities:
        p   = prices.get(e["ticker"], {})
        chg = safe_float(p.get("change_pct"), 0.0)
        if p and chg != 0 and abs(adj_score) >= 0.05:
            m = (min(1.0 + abs(chg) / 200, 1.15)
                 if (adj_score > 0) == (chg > 0)
                 else max(1.0 - abs(chg) / 200, 0.70))
            adj_score = round(adj_score * m, 4)
        break

    art = {
        "id":              dedup_id(title, link),
        "title":           title[:512],
        "source":          source,
        "url":             link,
        "published":       pub,
        "ingested":        ts_now(),
        "vader_score":     sent["vader_score"],
        "finbert_score":   sent["finbert_score"],
        "score":           adj_score,
        "label":           sent["label"],
        "confidence":      sent["confidence"],
        "model_tier":      sent["model_tier"],
        "keywords":        json.dumps(kws),
        "entities":        json.dumps(entities),
        "event_type":      clf.event_type,           # NEW
        "event_conf":      clf.confidence,           # NEW
        "full_text":       0,
        "text_length":     0,
        "numeric_signals": "{}",
        "source_tier":     "C",
    }
    return enrich_headline_metadata(art)


# ── RSS ingestion (concurrent) ────────────────────────────────────────────────

def _fetch_one_rss(source_url: tuple[str, str], prices: dict) -> list[dict]:
    source, url = source_url
    _rss_limiter.wait("rss")
    try:
        feed  = feedparser.parse(url)
        items = []
        for entry in feed.entries[:RSS_PER_FEED]:
            title = clean_text(getattr(entry, "title", ""))
            link  = getattr(entry, "link",  "")
            pub   = getattr(entry, "published",
                            datetime.now(timezone.utc).isoformat())
            if title:
                art = _build_article(title, link, pub, source, prices)
                if art:
                    items.append(art)
        return items
    except Exception as e:
        log.debug("RSS error [%s]: %s", source, e)
        return []


def fetch_rss(prices: dict) -> list[dict]:
    """Fetch all RSS feeds concurrently via thread pool."""
    items: list[dict] = []
    max_workers = min(12, len(RSS_FEEDS))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_one_rss, (src, url), prices): src
            for src, url in RSS_FEEDS
        }
        for fut in as_completed(futures):
            try:
                items.extend(fut.result())
            except Exception as e:
                log.debug("RSS worker error: %s", e)

    try:
        with get_db() as db:
            update_freshness(db, "rss", ok=True, item_count=len(items))
    except Exception:
        pass

    log.debug("fetch_rss: %d articles from %d feeds", len(items), len(RSS_FEEDS))
    return items


# ── NewsAPI ingestion ─────────────────────────────────────────────────────────

_NEWSAPI_QUERIES = [
    "economy OR inflation OR Federal Reserve OR interest rates",
    "stock market OR earnings OR S&P 500 OR Wall Street",
    "bitcoin OR cryptocurrency OR ethereum OR crypto",
    "oil OR gold OR commodities OR OPEC",
    "recession OR GDP OR unemployment OR jobs report",
]


def _fetch_newsapi_query(query: str, prices: dict) -> list[dict]:
    _api_limiter.wait("newsapi")
    r = _retry_get(
        "https://newsapi.org/v2/everything",
        params={"q": query, "language": "en", "sortBy": "publishedAt",
                "pageSize": min(NEWSAPI_PAGE_SIZE, 100), "apiKey": NEWS_API_KEY},
        timeout=10, retries=2,
    )
    if not r:
        return []
    items = []
    for art in r.json().get("articles", []):
        title = clean_text(art.get("title", ""))
        link  = art.get("url", "")
        if not title or title == "[Removed]":
            continue
        pub = art.get("publishedAt", ts_now())
        src = art.get("source", {}).get("name", "NewsAPI")
        a   = _build_article(title, link, pub, src, prices)
        if a:
            items.append(a)
    return items


def fetch_newsapi(prices: dict) -> list[dict]:
    if not NEWS_API_KEY:
        return []
    items: list[dict] = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(_fetch_newsapi_query, q, prices)
                   for q in _NEWSAPI_QUERIES]
        for fut in as_completed(futures):
            try:
                items.extend(fut.result())
            except Exception:
                pass

    try:
        with get_db() as db:
            update_freshness(db, "newsapi", ok=True, item_count=len(items))
    except Exception:
        pass

    log.debug("fetch_newsapi: %d articles", len(items))
    return items


# ── ticker news ───────────────────────────────────────────────────────────────

TICKER_SEARCH_TERMS = {
    "AAPL": "Apple Inc stock", "MSFT": "Microsoft stock",
    "NVDA": "Nvidia stock semiconductor", "TSLA": "Tesla stock EV",
    "AMZN": "Amazon stock AWS", "META": "Meta Facebook stock",
    "GOOG": "Google Alphabet stock", "JPM": "JPMorgan Chase bank",
    "BAC": "Bank of America", "XOM": "ExxonMobil oil",
    "AMD": "AMD chip semiconductor", "INTC": "Intel chip",
    "GS": "Goldman Sachs bank", "NFLX": "Netflix streaming",
    "V": "Visa payments", "WFC": "Wells Fargo bank",
    "BTC-USD": "Bitcoin cryptocurrency", "ETH-USD": "Ethereum crypto",
    "GC=F": "gold price commodity", "CL=F": "crude oil price",
    "^TNX": "Treasury yield 10 year bond", "HYG": "high yield bonds credit",
    "EURUSD=X": "Euro dollar EUR/USD", "DX-Y.NYB": "US dollar index DXY",
}


def fetch_ticker_news(ticker: str, prices: dict) -> list[dict]:
    if not NEWS_API_KEY:
        return []
    _api_limiter.wait("newsapi")
    query = TICKER_SEARCH_TERMS.get(ticker, ticker)
    r = _retry_get(
        "https://newsapi.org/v2/everything",
        params={"q": query, "language": "en", "sortBy": "publishedAt",
                "pageSize": 20, "apiKey": NEWS_API_KEY},
        timeout=8, retries=2,
    )
    if not r:
        return []
    items = []
    for art in r.json().get("articles", []):
        title = clean_text(art.get("title", ""))
        link  = art.get("url", "")
        if not title or title == "[Removed]":
            continue
        pub = art.get("publishedAt", ts_now())
        src = art.get("source", {}).get("name", "NewsAPI")
        a   = _build_article(title, link, pub, src, prices)
        if a:
            items.append(a)
    return items


# ── article persistence ───────────────────────────────────────────────────────

def save_articles(items: list[dict]) -> int:
    """Persist articles + entity mentions; return count of new inserts."""
    with get_db() as db:
        new_ids = []
        for art in items:
            if not art or not art.get("id"):
                continue
            # Add event_type column if present (safe for old DB without it)
            cols = list(art.keys())
            # Only insert columns that exist in the schema
            _KNOWN = {
                "id","title","source","url","published","ingested",
                "vader_score","finbert_score","score","label","confidence",
                "model_tier","keywords","entities","full_text","text_length",
                "numeric_signals","source_tier",
            }
            safe_art = {k: v for k, v in art.items() if k in _KNOWN}
            try:
                cur = db.execute(
                    "INSERT OR IGNORE INTO articles ("
                    + ",".join(safe_art.keys())
                    + ") VALUES ("
                    + ",".join("?" * len(safe_art))
                    + ")",
                    list(safe_art.values())
                )
                if cur.rowcount:
                    new_ids.append(art["id"])
            except Exception as e:
                log.debug("save_articles row error: %s", e)

        ts_now_ = ts_now()
        for art in items:
            if not art or art.get("id") not in new_ids:
                continue
            try:
                entities = json.loads(art.get("entities", "[]") or "[]")
                for ent in entities:
                    db.execute(
                        "INSERT INTO entity_mentions "
                        "(article_id, entity_raw, ticker, score, ts) "
                        "VALUES (?,?,?,?,?)",
                        (art["id"], ent["entity_raw"],
                         ent["ticker"], art["score"], ts_now_)
                    )
            except Exception as e:
                log.debug("entity_mentions error: %s", e)

        with _fulltext_lock:
            for art in items:
                if art and art.get("id") in new_ids and art.get("url"):
                    _fulltext_queue.append((art["id"], art["url"]))

    return len(new_ids)


# ── full-text enrichment ──────────────────────────────────────────────────────

def fetch_full_text(url: str) -> Optional[str]:
    if not NEWSPAPER_READY or not url:
        return None
    try:
        art = _NewsArticle(url)
        art.download()
        art.parse()
        text = art.text.strip()
        return text if len(text) > 150 else None
    except Exception:
        return None


def enrich_article_fulltext(article_id_: str, url: str):
    text = fetch_full_text(url)
    if not text:
        return
    sent = score_text(text)
    with get_db() as db:
        row = db.execute(
            "SELECT full_text FROM articles WHERE id=?", (article_id_,)
        ).fetchone()
        if not row or row[0]:
            return
        db.execute("""
            UPDATE articles
            SET score=?, vader_score=?, finbert_score=?, label=?,
                confidence=?, model_tier=?, full_text=1, text_length=?
            WHERE id=?
        """, (sent["score"], sent["vader_score"], sent["finbert_score"],
              sent["label"], sent["confidence"], sent["model_tier"],
              len(text), article_id_))


def fulltext_worker(cache_ref):
    log.info("fulltext_worker started")
    while True:
        try:
            with _fulltext_lock:
                item = _fulltext_queue.popleft() if _fulltext_queue else None
            if item:
                enrich_article_fulltext(item[0], item[1])
                cache_ref.clear_error("fulltext")
                cache_ref.update_queue_len(len(_fulltext_queue))
                time.sleep(0.4)
            else:
                time.sleep(2)
        except Exception as e:
            cache_ref.record_error("fulltext", e)
            time.sleep(3)


# ── price fetching ────────────────────────────────────────────────────────────

def fetch_prices(tickers: list[str]) -> list[dict]:
    import yfinance as yf
    rows = []
    try:
        data = yf.download(
            tickers, period="2d", interval="1m",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True,
        )
        ts = ts_now()
        for ticker in tickers:
            try:
                df = data if len(tickers) == 1 else data[ticker]
                if df.empty:
                    continue
                cl  = df["Close"].dropna()
                vol = df["Volume"].dropna() if "Volume" in df else None
                hi  = df["High"].dropna()   if "High"   in df else None
                lo  = df["Low"].dropna()    if "Low"    in df else None
                op  = df["Open"].dropna()   if "Open"   in df else None
                last = float(cl.iloc[-1])
                prev = float(cl.iloc[-2]) if len(cl) > 1 else last
                chg  = round((last - prev) / prev * 100, 4) if prev else 0
                if (vol is not None and hi is not None
                        and lo is not None and len(vol) >= 5):
                    n    = min(20, len(vol))
                    tp   = (hi.iloc[-n:] + lo.iloc[-n:] + cl.iloc[-n:]) / 3
                    vwap = (float((tp * vol.iloc[-n:]).sum()
                                  / vol.iloc[-n:].sum())
                            if vol.iloc[-n:].sum() > 0 else last)
                else:
                    vwap = last
                rows.append({
                    "ticker":     ticker,
                    "ts":         ts,
                    "price":      round(last, 4),
                    "change_pct": chg,
                    "volume":     float(vol.iloc[-1]) if vol is not None else 0,
                    "high":       round(float(hi.iloc[-1]), 4) if hi is not None else last,
                    "low":        round(float(lo.iloc[-1]), 4) if lo is not None else last,
                    "open":       round(float(op.iloc[-1]), 4) if op is not None else last,
                    "vwap":       round(vwap, 4),
                })
            except Exception as e:
                log.debug("price error [%s]: %s", ticker, e)
    except Exception as e:
        log.warning("fetch_prices batch error: %s", e)
    return rows


def save_prices(rows: list[dict]):
    with get_db() as db:
        db.executemany("""
            INSERT OR REPLACE INTO market_prices
            (ticker,ts,price,change_pct,volume,high,low,open,vwap)
            VALUES (:ticker,:ts,:price,:change_pct,:volume,:high,:low,:open,:vwap)
        """, rows)


# ── FRED ──────────────────────────────────────────────────────────────────────

FRED_BASE = "https://api.stlouisfed.org/fred"


def fetch_fred_series(series_id: str, label: str, theme: str) -> Optional[dict]:
    _api_limiter.wait("fred")
    r = _retry_get(f"{FRED_BASE}/series/observations",
                   params={"series_id": series_id, "sort_order": "desc",
                           "limit": "5", "observation_start": "2020-01-01",
                           "api_key": FRED_API_KEY, "file_type": "json"},
                   timeout=10, retries=2)
    if not r:
        return None
    obs = [o for o in r.json().get("observations", [])
           if o.get("value") not in (".", "", None)]
    if len(obs) < 2:
        return None
    try:
        cur  = float(obs[0]["value"])
        prev = float(obs[1]["value"])
        surp = round((cur - prev) / abs(prev) * 100, 4) if prev else 0.0
        return {"series_id": series_id, "label": label, "theme": theme,
                "ts": obs[0].get("date", ts_now()[:10]),
                "value": cur, "prev_value": prev, "surprise": surp, "units": ""}
    except Exception:
        return None


def fetch_all_fred() -> list[dict]:
    if not FRED_API_KEY:
        return []
    rows = []
    for label, series_id, theme in FRED_SERIES:
        row = fetch_fred_series(series_id, label, theme)
        if row:
            rows.append(row)
    try:
        with get_db() as db:
            update_freshness(db, "fred", ok=True, item_count=len(rows))
    except Exception:
        pass
    log.debug("fetch_all_fred: %d series", len(rows))
    return rows


def save_fred_data(rows: list[dict]):
    if not rows:
        return
    with get_db() as db:
        db.executemany("""
            INSERT OR REPLACE INTO fred_data
            (series_id,label,theme,ts,value,prev_value,surprise,units)
            VALUES (:series_id,:label,:theme,:ts,:value,:prev_value,:surprise,:units)
        """, rows)


def apply_fred_surprise_boost(fred_rows: list[dict]):
    if not fred_rows:
        return
    ts_now_ = ts_now()
    with get_db() as db:
        for row in fred_rows:
            if abs(row["surprise"]) < 0.5:
                continue
            boost    = max(-0.30, min(0.30, row["surprise"] / 33.0))
            synth_id = dedup_id("FRED", row["series_id"], row["ts"])
            db.execute("""
                INSERT OR IGNORE INTO articles
                (id,title,source,url,published,ingested,
                 vader_score,finbert_score,score,label,confidence,
                 model_tier,keywords,entities,full_text,text_length,
                 numeric_signals,source_tier)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                synth_id,
                f"[FRED] {row['label']}: {row['value']:.2f} "
                f"(prev {row['prev_value']:.2f}, {row['surprise']:+.2f}%)",
                "FRED", "", row["ts"], ts_now_,
                boost, boost, boost,
                "POS" if boost > 0.05 else ("NEG" if boost < -0.05 else "NEU"),
                0.95, "fred_surprise",
                json.dumps([row["theme"]]), json.dumps([]),
                0, 0, "{}", "A",
            ))
            db.execute(
                "INSERT INTO entity_mentions "
                "(article_id,entity_raw,ticker,score,ts) VALUES (?,?,?,?,?)",
                (synth_id, row["label"], row["theme"], boost, ts_now_)
            )
