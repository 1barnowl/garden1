"""
ingestion/feeds.py
All data ingestion: RSS feeds, NewsAPI, full article text (newspaper3k),
price data (yfinance), FRED macro series.
"""

import hashlib
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone, timedelta

import feedparser
import requests
import yfinance as yf

from config.settings import (
    log, NEWS_API_KEY, FRED_API_KEY, RSS_FEEDS,
    FRED_SERIES, RSS_PER_FEED, NEWSAPI_PAGE_SIZE,
)
from db.database import get_db
from nlp.scorer import score_text
from nlp.entities import extract_entities, extract_keywords, enrich_headline_metadata

# ── newspaper3k (optional) ────────────────────────────────────────────────────
NEWSPAPER_READY = False
try:
    from newspaper import Article as _NewsArticle
    NEWSPAPER_READY = True
    log.info("newspaper3k loaded — full article ingestion enabled")
except ImportError:
    log.warning("newspaper3k not installed — headline-only mode")

# ── full-text queue ───────────────────────────────────────────────────────────
_fulltext_queue: deque = deque(maxlen=400)
_fulltext_lock  = threading.Lock()


def article_id(title: str, url: str) -> str:
    return hashlib.md5(f"{title}{url}".encode()).hexdigest()


def market_multiplier(raw_score: float, ticker: str, prices: dict) -> float:
    p   = prices.get(ticker, {})
    chg = p.get("change_pct", 0.0)
    if not p or chg == 0 or abs(raw_score) < 0.05:
        return raw_score
    if (raw_score > 0 and chg > 0) or (raw_score < 0 and chg < 0):
        m = min(1.0 + abs(chg) / 200, 1.15)
    else:
        m = max(1.0 - abs(chg) / 200, 0.70)
    return round(raw_score * m, 4)


def _build_article(title: str, link: str, pub: str,
                   source: str, prices: dict) -> dict:
    sent     = score_text(title)
    kws      = extract_keywords(title)
    entities = extract_entities(title)
    adj_score = sent["score"]
    for e in entities:
        adj_score = market_multiplier(sent["score"], e["ticker"], prices)
        break
    art = {
        "id":            article_id(title, link),
        "title":         title,
        "source":        source,
        "url":           link,
        "published":     pub,
        "ingested":      datetime.now(timezone.utc).isoformat(),
        "vader_score":   sent["vader_score"],
        "finbert_score": sent["finbert_score"],
        "score":         adj_score,
        "label":         sent["label"],
        "confidence":    sent["confidence"],
        "model_tier":    sent["model_tier"],
        "keywords":      json.dumps(kws),
        "entities":      json.dumps(entities),
        "full_text":     0,
        "text_length":   0,
        "numeric_signals": "{}",
        "source_tier":   "C",
    }
    return enrich_headline_metadata(art)


# ── RSS ingestion ─────────────────────────────────────────────────────────────

def fetch_rss(prices: dict) -> list:
    items = []
    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:RSS_PER_FEED]:
                title = getattr(entry, "title", "")
                link  = getattr(entry, "link",  "")
                pub   = getattr(entry, "published",
                                datetime.now(timezone.utc).isoformat())
                if title:
                    items.append(_build_article(title, link, pub, source, prices))
        except Exception as e:
            log.debug("RSS error [%s]: %s", source, e)
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

def fetch_newsapi(prices: dict) -> list:
    if not NEWS_API_KEY:
        return []
    items = []
    for query in _NEWSAPI_QUERIES:
        try:
            r = requests.get(
                "https://newsapi.org/v2/everything",
                params={"q": query, "language": "en", "sortBy": "publishedAt",
                        "pageSize": min(NEWSAPI_PAGE_SIZE, 100),
                        "apiKey": NEWS_API_KEY},
                timeout=10,
            )
            r.raise_for_status()
            for art in r.json().get("articles", []):
                title = art.get("title", "")
                link  = art.get("url", "")
                if not title or title == "[Removed]":
                    continue
                pub = art.get("publishedAt", datetime.now(timezone.utc).isoformat())
                src = art.get("source", {}).get("name", "NewsAPI")
                items.append(_build_article(title, link, pub, src, prices))
            time.sleep(0.2)   # respect rate limit between queries
        except Exception as e:
            log.debug("NewsAPI error [%s]: %s", query[:30], e)
    log.debug("fetch_newsapi: %d articles", len(items))
    return items


# ── Ticker-specific news search via NewsAPI ───────────────────────────────────

# Maps tickers to human-readable search terms for targeted news queries
TICKER_SEARCH_TERMS = {
    "AAPL": "Apple Inc stock",
    "MSFT": "Microsoft stock",
    "NVDA": "Nvidia stock semiconductor",
    "TSLA": "Tesla stock EV",
    "AMZN": "Amazon stock AWS",
    "META": "Meta Facebook stock",
    "GOOG": "Google Alphabet stock",
    "JPM":  "JPMorgan Chase bank",
    "BAC":  "Bank of America",
    "XOM":  "ExxonMobil oil",
    "AMD":  "AMD chip semiconductor",
    "INTC": "Intel chip",
    "GS":   "Goldman Sachs bank",
    "NFLX": "Netflix streaming",
    "V":    "Visa payments",
    "WFC":  "Wells Fargo bank",
    "BTC-USD": "Bitcoin cryptocurrency",
    "ETH-USD": "Ethereum crypto",
    "GC=F":    "gold price commodity",
    "CL=F":    "crude oil price",
    "^TNX":    "Treasury yield 10 year bond",
    "HYG":     "high yield bonds credit",
    "EURUSD=X": "Euro dollar EUR/USD",
    "DX-Y.NYB": "US dollar index DXY",
}

def fetch_ticker_news(ticker: str, prices: dict) -> list:
    """Fetch news specifically mentioning a ticker. Used for enhanced news feed."""
    if not NEWS_API_KEY:
        return []
    query = TICKER_SEARCH_TERMS.get(ticker, ticker)
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": query, "language": "en", "sortBy": "publishedAt",
                    "pageSize": 20, "apiKey": NEWS_API_KEY},
            timeout=8,
        )
        r.raise_for_status()
        items = []
        for art in r.json().get("articles", []):
            title = art.get("title", "")
            link  = art.get("url", "")
            if not title or title == "[Removed]":
                continue
            pub = art.get("publishedAt", datetime.now(timezone.utc).isoformat())
            src = art.get("source", {}).get("name", "NewsAPI")
            items.append(_build_article(title, link, pub, src, prices))
        return items
    except Exception as e:
        log.debug("fetch_ticker_news error [%s]: %s", ticker, e)
        return []


# ── Article persistence ───────────────────────────────────────────────────────

def save_articles(items: list):
    with get_db() as db:
        new_ids = []
        for art in items:
            try:
                db.execute("""
                    INSERT OR IGNORE INTO articles
                    (id,title,source,url,published,ingested,
                     vader_score,finbert_score,score,label,confidence,
                     model_tier,keywords,entities,full_text,text_length,
                     numeric_signals,source_tier)
                    VALUES
                    (:id,:title,:source,:url,:published,:ingested,
                     :vader_score,:finbert_score,:score,:label,:confidence,
                     :model_tier,:keywords,:entities,:full_text,:text_length,
                     :numeric_signals,:source_tier)
                """, art)
                if db.execute("SELECT changes()").fetchone()[0]:
                    new_ids.append((art["id"], art["url"]))
            except Exception as e:
                log.debug("save_articles row error: %s", e)

        # entity mentions
        ts = datetime.now(timezone.utc).isoformat()
        mention_rows = []
        for art in items:
            try:
                for e in json.loads(art.get("entities") or "[]"):
                    mention_rows.append({
                        "article_id": art["id"], "entity_raw": e["entity_raw"],
                        "ticker":     e["ticker"], "score": art["score"], "ts": ts,
                    })
            except Exception:
                pass
        if mention_rows:
            db.executemany("""
                INSERT INTO entity_mentions (article_id,entity_raw,ticker,score,ts)
                VALUES (:article_id,:entity_raw,:ticker,:score,:ts)
            """, mention_rows)

    # queue for full-text enrichment
    from config.settings import FULL_TEXT_ENABLED
    if FULL_TEXT_ENABLED and NEWSPAPER_READY and new_ids:
        with _fulltext_lock:
            for aid, url in new_ids:
                if url:
                    _fulltext_queue.append((aid, url))

    return len(new_ids)


# ── Full-text enrichment ──────────────────────────────────────────────────────

def fetch_full_text(url: str) -> str | None:
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
    """Background thread — drains full-text enrichment queue."""
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


# ── Price fetching (yfinance) ─────────────────────────────────────────────────

def fetch_prices(tickers: list) -> list:
    rows = []
    try:
        data = yf.download(
            tickers, period="2d", interval="1m",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True,
        )
        ts = datetime.now(timezone.utc).isoformat()
        for ticker in tickers:
            try:
                df = data if len(tickers) == 1 else data[ticker]
                if df.empty:
                    continue
                cl   = df["Close"].dropna()
                vol  = df["Volume"].dropna() if "Volume" in df else None
                hi   = df["High"].dropna()   if "High"   in df else None
                lo   = df["Low"].dropna()    if "Low"    in df else None
                op   = df["Open"].dropna()   if "Open"   in df else None

                last = float(cl.iloc[-1])
                prev = float(cl.iloc[-2]) if len(cl) > 1 else last
                chg  = round((last - prev) / prev * 100, 4) if prev else 0

                # simple VWAP approximation over last 20 candles
                if vol is not None and hi is not None and lo is not None and len(vol) >= 5:
                    n     = min(20, len(vol))
                    tp    = ((hi.iloc[-n:] + lo.iloc[-n:] + cl.iloc[-n:]) / 3)
                    vwap  = float((tp * vol.iloc[-n:]).sum() / vol.iloc[-n:].sum()) if vol.iloc[-n:].sum() > 0 else last
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


def save_prices(rows: list):
    with get_db() as db:
        db.executemany("""
            INSERT OR REPLACE INTO market_prices
            (ticker,ts,price,change_pct,volume,high,low,open,vwap)
            VALUES (:ticker,:ts,:price,:change_pct,:volume,:high,:low,:open,:vwap)
        """, rows)


# ── FRED ──────────────────────────────────────────────────────────────────────

FRED_BASE = "https://api.stlouisfed.org/fred"

def _fred_get(endpoint: str, params: dict) -> dict:
    if not FRED_API_KEY:
        return {}
    try:
        params["api_key"]   = FRED_API_KEY
        params["file_type"] = "json"
        r = requests.get(f"{FRED_BASE}/{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("FRED API error: %s", e)
        return {}


def fetch_fred_series(series_id: str, label: str, theme: str) -> dict | None:
    data = _fred_get("series/observations", {
        "series_id": series_id, "sort_order": "desc",
        "limit": "5", "observation_start": "2020-01-01",
    })
    obs = [o for o in data.get("observations", [])
           if o.get("value") not in (".", "", None)]
    if len(obs) < 2:
        return None
    try:
        cur      = float(obs[0]["value"])
        prev     = float(obs[1]["value"])
        surprise = round((cur - prev) / abs(prev) * 100, 4) if prev else 0.0
        return {
            "series_id":  series_id, "label": label, "theme": theme,
            "ts":         obs[0].get("date", datetime.now(timezone.utc).date().isoformat()),
            "value":      cur, "prev_value": prev,
            "surprise":   surprise, "units": "",
        }
    except Exception:
        return None


def fetch_all_fred() -> list:
    if not FRED_API_KEY:
        return []
    rows = []
    for label, series_id, theme in FRED_SERIES:
        row = fetch_fred_series(series_id, label, theme)
        if row:
            rows.append(row)
        time.sleep(0.12)
    log.debug("fetch_all_fred: %d series", len(rows))
    return rows


def save_fred_data(rows: list):
    if not rows:
        return
    with get_db() as db:
        db.executemany("""
            INSERT OR REPLACE INTO fred_data
            (series_id,label,theme,ts,value,prev_value,surprise,units)
            VALUES (:series_id,:label,:theme,:ts,:value,:prev_value,:surprise,:units)
        """, rows)


def apply_fred_surprise_boost(fred_rows: list):
    if not fred_rows:
        return
    ts_now = datetime.now(timezone.utc).isoformat()
    import math as _math
    with get_db() as db:
        for row in fred_rows:
            if abs(row["surprise"]) < 0.5:
                continue
            theme    = row["theme"]
            boost    = max(-0.30, min(0.30, row["surprise"] / 33.0))
            synth_id = hashlib.md5(
                f"FRED:{row['series_id']}:{row['ts']}".encode()
            ).hexdigest()
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
                "FRED", "", row["ts"], ts_now,
                boost, boost, boost,
                "POS" if boost > 0.05 else ("NEG" if boost < -0.05 else "NEU"),
                0.95, "fred_surprise",
                json.dumps([theme]), json.dumps([]),
                0, 0, "{}", "A",
            ))
            db.execute("""
                INSERT INTO entity_mentions (article_id,entity_raw,ticker,score,ts)
                VALUES (?,?,?,?,?)
            """, (synth_id, row["label"], theme, boost, ts_now))
