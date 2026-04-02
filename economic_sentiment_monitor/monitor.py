#!/usr/bin/env python3
"""
Economic Sentiment Monitor  v0.3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Upgrades vs v0.2:
  [1] Full article text ingestion via newspaper3k
      — follows URL, downloads full body text (500-800 words)
      — scores full text with VADER/FinBERT instead of 10-word headline
      — falls back to headline if fetch fails / times out (5s limit)
      — separate fast-headline pass + slow full-text enrichment thread

  [2] Runtime config via ~/.esm/config.yml
      — add/remove tickers, RSS feeds, thresholds without editing code
      — configure poll interval, alert sensitivity, full-text toggle
      — auto-creates sensible defaults on first run

  [3] Sentiment–price correlation column
      — Pearson correlation between ticker's rolling sentiment and
        its subsequent 1h price change, computed from DB history
      — shown on EQUITIES and SECTORS tabs as "Corr" column
      — needs ~4h of accumulated data to become meaningful
      — colour legend: near +1 = sentiment leads price correctly
                       near -1 = market consistently disagrees
"""

import os, sys, time, threading, sqlite3, hashlib, json, re, math
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

# ══════════════════════════════════════════════════════════════════════════════
#  DEPENDENCY CHECK
# ══════════════════════════════════════════════════════════════════════════════
MISSING = []
try:
    from textual.app import App, ComposeResult
    from textual.widgets import (Header, Footer, TabbedContent, TabPane,
                                  DataTable, Static)
    from textual.reactive import reactive
    from textual.timer import Timer
except ImportError:
    MISSING.append("textual")

try:
    import yfinance as yf
except ImportError:
    MISSING.append("yfinance")

try:
    import feedparser
except ImportError:
    MISSING.append("feedparser")

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER = SentimentIntensityAnalyzer()
except ImportError:
    MISSING.append("vaderSentiment")
    VADER = None

try:
    import requests
except ImportError:
    MISSING.append("requests")

if MISSING:
    print(f"\n[ERROR] Missing core packages: {', '.join(MISSING)}")
    print("Run:  pip install " + " ".join(MISSING) + " --break-system-packages")
    sys.exit(1)

# ── optional: newspaper3k (full article text) ─────────────────────────────────
NEWSPAPER_READY = False
try:
    from newspaper import Article
    NEWSPAPER_READY = True
    print("[TEXT] newspaper3k loaded ✓  — full article ingestion enabled")
except ImportError:
    print("[TEXT] newspaper3k not installed — headline-only mode")
    print("       pip install newspaper3k --break-system-packages")

# ── optional: FinBERT ────────────────────────────────────────────────────────
FINBERT_PIPE  = None
FINBERT_READY = False
try:
    from transformers import pipeline as hf_pipeline
    print("[NLP] Loading FinBERT…")
    FINBERT_PIPE = hf_pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert",
        top_k=None,
        device=-1,
        truncation=True,
        max_length=512,
    )
    FINBERT_READY = True
    print("[NLP] FinBERT loaded ✓")
except Exception as _e:
    print(f"[NLP] FinBERT not available ({_e}). Using VADER only.")

# ── optional: SpaCy ──────────────────────────────────────────────────────────
NLP_SPACY   = None
SPACY_READY = False
try:
    import spacy
    for _model in ("en_core_web_sm", "en_core_web_md"):
        try:
            NLP_SPACY   = spacy.load(_model, disable=["parser", "lemmatizer"])
            SPACY_READY = True
            print(f"[NER] SpaCy '{_model}' loaded ✓")
            break
        except OSError:
            continue
    if not SPACY_READY:
        print("[NER] No SpaCy model. Run: python -m spacy download en_core_web_sm")
except ImportError:
    print("[NER] SpaCy not installed. pip install spacy --break-system-packages")

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG  (~/.esm/config.yml — auto-created with defaults)
# ══════════════════════════════════════════════════════════════════════════════
CONFIG_PATH = os.path.expanduser("~/.esm/config.yml")
DB_PATH     = os.path.expanduser("~/.esm/sentiment.db")

DEFAULT_CONFIG = """\
# Economic Sentiment Monitor — runtime config
# Edit freely; changes take effect on next launch.

# API keys (all optional / free)
news_api_key: ""
fred_api_key: ""

# Poll interval in seconds (minimum 30)
refresh_sec: 90

# Enable full article text download via newspaper3k (slower, more accurate)
full_text_enabled: true

# Alert sensitivity: delta threshold vs 4h baseline
alert_warn_delta: 0.15
alert_crit_delta: 0.25

# Minimum article count before alert fires (avoids noise with tiny samples)
alert_min_count: 3

# Watchlists — add/remove tickers freely
watchlist_equities:
  - SPY
  - QQQ
  - AAPL
  - MSFT
  - NVDA
  - TSLA
  - AMZN
  - META
  - GOOG
  - JPM
  - BAC
  - XOM

watchlist_macrofx:
  - GC=F
  - SI=F
  - CL=F
  - BTC-USD
  - ETH-USD
  - EURUSD=X
  - GBPUSD=X
  - JPY=X
  - DX-Y.NYB

watchlist_bonds:
  - ^TNX
  - ^TYX
  - ^FVX
  - TLT
  - HYG
  - LQD

watchlist_sectors:
  - XLK
  - XLF
  - XLE
  - XLV
  - XLI
  - XLC
  - XLB
  - XLU
  - XLRE
  - XLP
  - XLY

# RSS feeds — add/remove as needed
rss_feeds:
  - ["Reuters Business",  "https://feeds.reuters.com/reuters/businessNews"]
  - ["Reuters Markets",   "https://feeds.reuters.com/reuters/financialNews"]
  - ["CNBC Top News",     "https://www.cnbc.com/id/100003114/device/rss/rss.html"]
  - ["MarketWatch",       "https://feeds.marketwatch.com/marketwatch/topstories/"]
  - ["Seeking Alpha",     "https://seekingalpha.com/feed.xml"]
  - ["Investopedia",      "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline"]
  - ["Yahoo Finance",     "https://finance.yahoo.com/news/rssindex"]
  - ["FT Markets",        "https://www.ft.com/markets?format=rss"]
  - ["Bloomberg",         "https://feeds.bloomberg.com/markets/news.rss"]
  - ["The Economist",     "https://www.economist.com/latest/rss.xml"]
"""

def _load_config() -> dict:
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w") as f:
            f.write(DEFAULT_CONFIG)
        print(f"[CFG] Created default config at {CONFIG_PATH}")

    # parse the YAML manually (no pyyaml dependency — simple key:value + lists)
    cfg = {}
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f) or {}
    except ImportError:
        # fallback: read only the scalar values we need (lists stay default)
        with open(CONFIG_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or ":" not in line:
                    continue
                k, _, v = line.partition(":")
                v = v.strip().strip('"').strip("'")
                if v in ("true","false"):
                    cfg[k.strip()] = v == "true"
                elif v.replace(".","").replace("-","").isdigit():
                    cfg[k.strip()] = float(v) if "." in v else int(v)
                elif v:
                    cfg[k.strip()] = v
    return cfg

CFG = _load_config()

# ── resolve config values with fallbacks ──────────────────────────────────────
NEWS_API_KEY      = CFG.get("news_api_key","") or os.environ.get("NEWS_API_KEY","")
FRED_API_KEY      = CFG.get("fred_api_key","") or os.environ.get("FRED_API_KEY","")
REFRESH_SEC       = max(int(CFG.get("refresh_sec", 90)), 30)
FULL_TEXT_ENABLED = bool(CFG.get("full_text_enabled", True)) and NEWSPAPER_READY
ALERT_WARN        = float(CFG.get("alert_warn_delta", 0.15))
ALERT_CRIT        = float(CFG.get("alert_crit_delta", 0.25))
ALERT_MIN_COUNT   = int(CFG.get("alert_min_count", 3))

def _tickers(key: str, default: list) -> list:
    v = CFG.get(key)
    if isinstance(v, list):
        return [str(x) for x in v if x]
    return default

WATCHLIST = {
    "Equities": _tickers("watchlist_equities",
                         ["SPY","QQQ","AAPL","MSFT","NVDA","TSLA","AMZN",
                          "META","GOOG","JPM","BAC","XOM"]),
    "Macro/FX": _tickers("watchlist_macrofx",
                         ["GC=F","SI=F","CL=F","BTC-USD","ETH-USD",
                          "EURUSD=X","GBPUSD=X","JPY=X","DX-Y.NYB"]),
    "Bonds":    _tickers("watchlist_bonds",
                         ["^TNX","^TYX","^FVX","TLT","HYG","LQD"]),
    "Sectors":  _tickers("watchlist_sectors",
                         ["XLK","XLF","XLE","XLV","XLI","XLC",
                          "XLB","XLU","XLRE","XLP","XLY"]),
}

SECTORS = {
    "XLK":"Technology","XLF":"Financials","XLE":"Energy","XLV":"Health Care",
    "XLI":"Industrials","XLC":"Comm Svc","XLB":"Materials","XLU":"Utilities",
    "XLRE":"Real Estate","XLP":"Cons Staples","XLY":"Cons Discr",
}

def _rss_feeds(cfg_feeds) -> list:
    if isinstance(cfg_feeds, list):
        out = []
        for item in cfg_feeds:
            if isinstance(item, list) and len(item) == 2:
                out.append(tuple(item))
            elif isinstance(item, str):
                out.append((item, item))
        return out
    return []

_raw_feeds = CFG.get("rss_feeds", [])
RSS_FEEDS = _rss_feeds(_raw_feeds) if _raw_feeds else [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",  "https://feeds.reuters.com/reuters/financialNews"),
    ("CNBC Top News",    "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch",      "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Seeking Alpha",    "https://seekingalpha.com/feed.xml"),
    ("Investopedia",     "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline"),
    ("Yahoo Finance",    "https://finance.yahoo.com/news/rssindex"),
    ("FT Markets",       "https://www.ft.com/markets?format=rss"),
    ("Bloomberg",        "https://feeds.bloomberg.com/markets/news.rss"),
    ("The Economist",    "https://www.economist.com/latest/rss.xml"),
]

MACRO_KEYWORDS = {
    "inflation":  ["inflation","cpi","pce","price index","consumer prices","core prices"],
    "rates":      ["interest rate","fed funds","federal reserve","fomc","rate hike","rate cut",
                   "powell","basis points","tightening","easing","pivot"],
    "growth":     ["gdp","economic growth","recession","contraction","expansion","output gap"],
    "employment": ["jobs","unemployment","payroll","nonfarm","labor market","jobless claims",
                   "hiring","layoffs","wage growth"],
    "trade":      ["tariff","trade war","exports","imports","current account","trade deficit",
                   "sanctions","supply chain"],
    "credit":     ["credit","default","spread","cds","debt","bond yield","high yield",
                   "investment grade","downgrade","upgrade"],
}

ENTITY_MAP: dict = {
    "apple":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","microsoft corp":"MSFT",
    "nvidia":"NVDA","nvidia corp":"NVDA",
    "tesla":"TSLA","tesla inc":"TSLA",
    "amazon":"AMZN","amazon.com":"AMZN",
    "meta":"META","meta platforms":"META","facebook":"META",
    "alphabet":"GOOG","google":"GOOG",
    "netflix":"NFLX","openai":"MSFT",
    "jpmorgan":"JPM","jp morgan":"JPM","jpmorgan chase":"JPM",
    "bank of america":"BAC","bofa":"BAC",
    "goldman sachs":"GS","goldman":"GS",
    "morgan stanley":"MS","citigroup":"C","citi":"C",
    "wells fargo":"WFC","blackrock":"BLK",
    "berkshire":"BRK-B","berkshire hathaway":"BRK-B",
    "exxon":"XOM","exxonmobil":"XOM","exxon mobil":"XOM",
    "chevron":"CVX","bp":"BP","shell":"SHEL","conocophillips":"COP",
    "federal reserve":"rates","the fed":"rates","fed":"rates","fomc":"rates",
    "jerome powell":"rates","powell":"rates","janet yellen":"rates","yellen":"rates",
    "ecb":"rates","european central bank":"rates",
    "bank of england":"rates","boe":"rates",
    "bank of japan":"rates","boj":"rates",
    "imf":"growth","world bank":"growth","opec":"CL=F",
    "s&p 500":"SPY","s&p500":"SPY","s&p":"SPY","sp500":"SPY",
    "nasdaq":"QQQ","dow jones":"DIA","dow":"DIA","russell 2000":"IWM",
    "gold":"GC=F","silver":"SI=F",
    "oil":"CL=F","crude oil":"CL=F","brent":"CL=F","wti":"CL=F","natural gas":"NG=F",
    "bitcoin":"BTC-USD","btc":"BTC-USD","ethereum":"ETH-USD","eth":"ETH-USD",
    "semiconductor":"XLK","semiconductors":"XLK","chip":"XLK","chips":"XLK",
    "bank":"XLF","banks":"XLF","financial":"XLF","financials":"XLF",
    "healthcare":"XLV","health care":"XLV","pharma":"XLV","biotech":"XLV",
    "energy sector":"XLE","retail":"XLY","consumer discretionary":"XLY",
    "utility":"XLU","utilities":"XLU","real estate":"XLRE","reit":"XLRE",
    "materials":"XLB","industrials":"XLI",
    "treasury":"^TNX","treasuries":"^TNX","10-year":"^TNX","t-bond":"^TNX",
    "10y yield":"^TNX","10yr yield":"^TNX",
    "high yield":"HYG","junk bond":"HYG","junk bonds":"HYG",
    "dollar":"DX-Y.NYB","usd":"DX-Y.NYB","us dollar":"DX-Y.NYB",
    "euro":"EURUSD=X","eur":"EURUSD=X","eurozone":"EURUSD=X",
    "yen":"JPY=X","japanese yen":"JPY=X",
    "pound":"GBPUSD=X","sterling":"GBPUSD=X","gbp":"GBPUSD=X",
}

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id              TEXT PRIMARY KEY,
            title           TEXT,
            source          TEXT,
            url             TEXT,
            published       TEXT,
            ingested        TEXT,
            vader_score     REAL,
            finbert_score   REAL,
            score           REAL,
            label           TEXT,
            confidence      REAL,
            model_tier      TEXT,
            keywords        TEXT,
            entities        TEXT,
            full_text       INTEGER DEFAULT 0,
            text_length     INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS entity_mentions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id  TEXT,
            entity_raw  TEXT,
            ticker      TEXT,
            score       REAL,
            ts          TEXT
        );
        CREATE TABLE IF NOT EXISTS market_prices (
            ticker      TEXT,
            ts          TEXT,
            price       REAL,
            change_pct  REAL,
            volume      REAL,
            PRIMARY KEY (ticker, ts)
        );
        CREATE TABLE IF NOT EXISTS sentiment_scores (
            key     TEXT,
            window  TEXT,
            ts      TEXT,
            score   REAL,
            count   INTEGER,
            PRIMARY KEY (key, window, ts)
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id       TEXT PRIMARY KEY,
            ts       TEXT,
            severity TEXT,
            key      TEXT,
            reason   TEXT,
            score    REAL
        );
        CREATE INDEX IF NOT EXISTS ix_articles_ingested ON articles(ingested);
        CREATE INDEX IF NOT EXISTS ix_entity_ts ON entity_mentions(ts);
        CREATE INDEX IF NOT EXISTS ix_prices_ticker ON market_prices(ticker, ts);
        CREATE INDEX IF NOT EXISTS ix_scores_key ON sentiment_scores(key, window, ts);
        """)
        # safe migration for older DBs
        for col, typ, default in [
            ("full_text",   "INTEGER", "0"),
            ("text_length", "INTEGER", "0"),
            ("vader_score", "REAL",    "NULL"),
            ("finbert_score","REAL",   "NULL"),
            ("model_tier",  "TEXT",    "NULL"),
            ("entities",    "TEXT",    "NULL"),
        ]:
            try:
                db.execute(f"ALTER TABLE articles ADD COLUMN {col} {typ} DEFAULT {default}")
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════════════
#  [1]  FULL ARTICLE TEXT INGESTION
# ══════════════════════════════════════════════════════════════════════════════

# Queue of (article_id, url) awaiting full-text enrichment
_fulltext_queue: deque = deque(maxlen=200)
_fulltext_lock  = threading.Lock()

def fetch_full_text(url: str, timeout: int = 5) -> str | None:
    """
    Download and parse full article body via newspaper3k.
    Hard timeout of 5s — returns None if it fails for any reason.
    """
    if not NEWSPAPER_READY or not url:
        return None
    try:
        art = Article(url)
        art.download()
        art.parse()
        text = art.text.strip()
        return text if len(text) > 100 else None
    except Exception:
        return None

def enrich_article_fulltext(article_id_: str, url: str):
    """
    Fetch full text, re-score, update DB.
    Called from background thread — never blocks ingestion loop.
    """
    text = fetch_full_text(url)
    if not text:
        return
    sent = score_text(text)
    with get_db() as db:
        # only update if article still exists and hasn't been enriched
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

def fulltext_worker():
    """Background thread — drains the full-text enrichment queue."""
    while True:
        try:
            with _fulltext_lock:
                item = _fulltext_queue.popleft() if _fulltext_queue else None
            if item:
                enrich_article_fulltext(item[0], item[1])
                time.sleep(0.5)   # be polite to servers
            else:
                time.sleep(3)
        except Exception:
            time.sleep(3)

# ══════════════════════════════════════════════════════════════════════════════
#  NLP — TWO-TIER SCORING
# ══════════════════════════════════════════════════════════════════════════════

def _vader_score(text: str) -> dict:
    if not VADER or not text:
        return {"score": 0.0, "label": "NEU", "confidence": 0.0}
    s   = VADER.polarity_scores(text[:1000])
    c   = s["compound"]
    lbl = "POS" if c >= 0.05 else ("NEG" if c <= -0.05 else "NEU")
    return {"score": round(c,4), "label": lbl,
            "confidence": round(max(s["pos"],s["neg"],s["neu"]),4)}

def _finbert_score(text: str):
    if not FINBERT_READY or not text:
        return None
    try:
        results   = FINBERT_PIPE(text[:512])[0]
        label_map = {r["label"].lower(): r["score"] for r in results}
        pos, neg  = label_map.get("positive",0.0), label_map.get("negative",0.0)
        neu       = label_map.get("neutral", 0.0)
        compound  = round(pos - neg, 4)
        lbl       = "POS" if compound >= 0.05 else ("NEG" if compound <= -0.05 else "NEU")
        return {"score": compound, "label": lbl,
                "confidence": round(max(pos,neg,neu),4),
                "pos": round(pos,4), "neg": round(neg,4)}
    except Exception:
        return None

def score_text(text: str) -> dict:
    vader   = _vader_score(text)
    finbert = _finbert_score(text)
    if finbert:
        blended = round(0.3 * vader["score"] + 0.7 * finbert["score"], 4)
        lbl = "POS" if blended >= 0.05 else ("NEG" if blended <= -0.05 else "NEU")
        return {"score": blended, "vader_score": vader["score"],
                "finbert_score": finbert["score"], "label": lbl,
                "confidence": finbert["confidence"], "model_tier": "finbert+vader"}
    return {"score": vader["score"], "vader_score": vader["score"],
            "finbert_score": None, "label": vader["label"],
            "confidence": vader["confidence"], "model_tier": "vader"}

# ══════════════════════════════════════════════════════════════════════════════
#  ENTITY LINKING
# ══════════════════════════════════════════════════════════════════════════════

def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())

def extract_entities(text: str) -> list:
    found = {}
    if SPACY_READY and NLP_SPACY:
        doc = NLP_SPACY(text[:300])
        for ent in doc.ents:
            if ent.label_ in ("ORG","GPE","PERSON","PRODUCT","MONEY","NORP"):
                ticker = ENTITY_MAP.get(_norm(ent.text))
                if ticker and ticker not in found:
                    found[ticker] = ent.text
    text_l = text.lower()
    for name, ticker in ENTITY_MAP.items():
        if name in text_l and ticker not in found:
            found[ticker] = name
    return [{"entity_raw": v, "ticker": k} for k, v in found.items()]

def extract_keywords(text: str) -> list:
    tl = text.lower()
    return [t for t, ws in MACRO_KEYWORDS.items() if any(w in tl for w in ws)]

def article_id(title: str, url: str) -> str:
    return hashlib.md5(f"{title}{url}".encode()).hexdigest()

# ══════════════════════════════════════════════════════════════════════════════
#  MARKET-CONTEXT MULTIPLIER
# ══════════════════════════════════════════════════════════════════════════════

def market_multiplier(raw_score: float, ticker: str, prices: dict) -> float:
    p = prices.get(ticker, {})
    if not p:
        return raw_score
    chg = p.get("change_pct", 0.0)
    if chg == 0 or abs(raw_score) < 0.05:
        return raw_score
    if (raw_score > 0 and chg > 0) or (raw_score < 0 and chg < 0):
        m = min(1.0 + abs(chg) / 200, 1.15)
    else:
        m = max(1.0 - abs(chg) / 200, 0.70)
    return round(raw_score * m, 4)

# ══════════════════════════════════════════════════════════════════════════════
#  [3]  SENTIMENT–PRICE CORRELATION
# ══════════════════════════════════════════════════════════════════════════════

def _pearson(xs: list, ys: list) -> float | None:
    """Pure-python Pearson r. Returns None if insufficient data."""
    n = len(xs)
    if n < 6:
        return None
    mx, my = sum(xs)/n, sum(ys)/n
    num  = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den  = math.sqrt(sum((x - mx)**2 for x in xs) *
                     sum((y - my)**2 for y in ys))
    if den == 0:
        return None
    return round(num / den, 3)

def compute_correlations(tickers: list, window_hours: int = 24) -> dict:
    """
    For each ticker, pair each sentiment_score row with the price
    change_pct recorded closest to 1h after that sentiment snapshot.
    Returns {ticker: pearson_r}.

    Requires ~4h of accumulated data to produce meaningful values.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
    result = {}
    with get_db() as db:
        for ticker in tickers:
            # get sentiment snapshots for this ticker
            sent_rows = db.execute("""
                SELECT ts, score FROM sentiment_scores
                WHERE key=? AND window='1h' AND ts > ?
                ORDER BY ts ASC
            """, (ticker, cutoff)).fetchall()

            if len(sent_rows) < 6:
                result[ticker] = None
                continue

            sent_vals, price_vals = [], []
            for ts_str, s_score in sent_rows:
                # find price closest to ts + 1h (lag: sentiment predicts future price)
                target_ts = (datetime.fromisoformat(ts_str.replace("Z",""))
                             + timedelta(hours=1)).isoformat()
                price_row = db.execute("""
                    SELECT change_pct FROM market_prices
                    WHERE ticker=? AND ts >= ?
                    ORDER BY ts ASC LIMIT 1
                """, (ticker, target_ts)).fetchone()
                if price_row:
                    sent_vals.append(s_score)
                    price_vals.append(price_row[0])

            result[ticker] = _pearson(sent_vals, price_vals)
    return result

def fmt_corr(r) -> str:
    if r is None:
        return " n/a"
    sign = "+" if r >= 0 else ""
    return f"{sign}{r:.2f}"

# ══════════════════════════════════════════════════════════════════════════════
#  INGESTION
# ══════════════════════════════════════════════════════════════════════════════

def _build_article(title: str, link: str, pub: str, source: str,
                   prices: dict) -> dict:
    sent     = score_text(title)   # always score headline immediately
    kws      = extract_keywords(title)
    entities = extract_entities(title)
    adj_score = sent["score"]
    for e in entities:
        adj_score = market_multiplier(sent["score"], e["ticker"], prices)
        break
    return {
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
    }

def fetch_rss(prices: dict) -> list:
    items = []
    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:15]:
                title = getattr(entry, "title", "")
                link  = getattr(entry, "link",  "")
                pub   = getattr(entry, "published",
                                datetime.now(timezone.utc).isoformat())
                if title:
                    items.append(_build_article(title, link, pub, source, prices))
        except Exception:
            pass
    return items

def fetch_newsapi(prices: dict,
                  query: str = "economy OR markets OR inflation OR Federal Reserve") -> list:
    if not NEWS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": query, "language":"en", "sortBy":"publishedAt",
                    "pageSize":50, "apiKey": NEWS_API_KEY},
            timeout=10,
        )
        r.raise_for_status()
        items = []
        for art in r.json().get("articles", []):
            title = art.get("title","")
            link  = art.get("url","")
            if not title or title == "[Removed]":
                continue
            pub = art.get("publishedAt", datetime.now(timezone.utc).isoformat())
            src = art.get("source",{}).get("name","NewsAPI")
            items.append(_build_article(title, link, pub, src, prices))
        return items
    except Exception:
        return []

def save_articles(items: list):
    with get_db() as db:
        new_ids = []
        for art in items:
            try:
                db.execute("""
                    INSERT OR IGNORE INTO articles
                    (id,title,source,url,published,ingested,vader_score,finbert_score,
                     score,label,confidence,model_tier,keywords,entities,full_text,text_length)
                    VALUES
                    (:id,:title,:source,:url,:published,:ingested,:vader_score,:finbert_score,
                     :score,:label,:confidence,:model_tier,:keywords,:entities,:full_text,:text_length)
                """, art)
                if db.execute("SELECT changes()").fetchone()[0]:
                    new_ids.append((art["id"], art["url"]))
            except Exception:
                pass

        # entity mentions
        mention_rows = []
        ts = datetime.now(timezone.utc).isoformat()
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

    # queue new articles for full-text enrichment
    if FULL_TEXT_ENABLED and new_ids:
        with _fulltext_lock:
            for aid, url in new_ids:
                if url:
                    _fulltext_queue.append((aid, url))

def fetch_prices(tickers: list) -> list:
    rows = []
    try:
        data = yf.download(tickers, period="2d", interval="1m",
                           group_by="ticker", auto_adjust=True,
                           progress=False, threads=True)
        ts = datetime.now(timezone.utc).isoformat()
        for ticker in tickers:
            try:
                df = data if len(tickers) == 1 else data[ticker]
                if df.empty:
                    continue
                last = float(df["Close"].dropna().iloc[-1])
                prev = float(df["Close"].dropna().iloc[-2]) if len(df) > 1 else last
                chg  = round((last - prev) / prev * 100, 4) if prev else 0
                vol  = float(df["Volume"].dropna().iloc[-1]) if "Volume" in df else 0
                rows.append({"ticker": ticker, "ts": ts,
                             "price": round(last,4), "change_pct": chg, "volume": vol})
            except Exception:
                pass
    except Exception:
        pass
    return rows

def save_prices(rows: list):
    with get_db() as db:
        db.executemany("""
            INSERT OR REPLACE INTO market_prices (ticker,ts,price,change_pct,volume)
            VALUES (:ticker,:ts,:price,:change_pct,:volume)
        """, rows)

# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATION & ALERTS
# ══════════════════════════════════════════════════════════════════════════════

def compute_aggregate_scores():
    cutoffs = {
        "1h":  (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "4h":  (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
        "24h": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
    }
    ts_now = datetime.now(timezone.utc).isoformat()
    with get_db() as db:
        for window, cutoff in cutoffs.items():
            rows = db.execute(
                "SELECT score,keywords FROM articles WHERE ingested > ?", (cutoff,)
            ).fetchall()
            theme_scores  = defaultdict(list)
            global_scores = []
            for score, kws_json in rows:
                global_scores.append(score)
                for kw in json.loads(kws_json or "[]"):
                    theme_scores[kw].append(score)
            if global_scores:
                db.execute("""INSERT OR REPLACE INTO sentiment_scores
                              (key,window,ts,score,count) VALUES (?,?,?,?,?)""",
                           ("GLOBAL", window, ts_now,
                            round(sum(global_scores)/len(global_scores),4),
                            len(global_scores)))
            for theme, scores in theme_scores.items():
                db.execute("""INSERT OR REPLACE INTO sentiment_scores
                              (key,window,ts,score,count) VALUES (?,?,?,?,?)""",
                           (theme, window, ts_now,
                            round(sum(scores)/len(scores),4), len(scores)))
            ticker_rows = db.execute(
                "SELECT ticker,score FROM entity_mentions WHERE ts > ?", (cutoff,)
            ).fetchall()
            ticker_scores = defaultdict(list)
            for ticker, score in ticker_rows:
                ticker_scores[ticker].append(score)
            for ticker, scores in ticker_scores.items():
                db.execute("""INSERT OR REPLACE INTO sentiment_scores
                              (key,window,ts,score,count) VALUES (?,?,?,?,?)""",
                           (ticker, window, ts_now,
                            round(sum(scores)/len(scores),4), len(scores)))

def check_alerts():
    alerts = []
    with get_db() as db:
        rows_1h = db.execute(
            "SELECT key,score,count FROM sentiment_scores WHERE window='1h' ORDER BY ts DESC"
        ).fetchall()
        rows_4h = db.execute(
            "SELECT key,score FROM sentiment_scores WHERE window='4h' ORDER BY ts DESC"
        ).fetchall()
        prev_map = {r[0]: r[1] for r in rows_4h}
        for key, score, count in rows_1h:
            prev = prev_map.get(key)
            if prev is None or count < ALERT_MIN_COUNT:
                continue
            delta = score - prev
            if abs(delta) >= ALERT_WARN:
                severity  = "CRIT" if abs(delta) >= ALERT_CRIT else "WARN"
                direction = "SURGE ▲" if delta > 0 else "DROP  ▼"
                reason    = (f"Sentiment {direction} "
                             f"Δ{abs(delta):.3f} vs 4h baseline (n={count})")
                alert_id  = hashlib.md5(
                    f"{key}{round(score,2)}{datetime.now(timezone.utc).date()}".encode()
                ).hexdigest()
                alerts.append({
                    "id": alert_id, "ts": datetime.now(timezone.utc).isoformat(),
                    "severity": severity, "key": key, "reason": reason, "score": score,
                })
        if alerts:
            db.executemany("""INSERT OR IGNORE INTO alerts
                              (id,ts,severity,key,reason,score)
                              VALUES (:id,:ts,:severity,:key,:reason,:score)""", alerts)
    return alerts

# ══════════════════════════════════════════════════════════════════════════════
#  SHARED DATA CACHE
# ══════════════════════════════════════════════════════════════════════════════

class DataCache:
    def __init__(self):
        self._lock            = threading.Lock()
        self.prices: dict     = {}
        self.scores: dict     = {}
        self.recent_articles  = []
        self.entity_summary   = []
        self.correlations: dict = {}   # ticker -> pearson_r
        self.alerts: deque    = deque(maxlen=50)
        self.last_refresh     = "—"
        self.article_count    = 0
        self.fulltext_count   = 0
        self.model_tier       = "vader"
        self.fulltext_queue_len = 0

    def update_prices(self, rows):
        with self._lock:
            for r in rows:
                self.prices[r["ticker"]] = r

    def update_scores(self):
        with self._lock:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            with get_db() as db:
                rows = db.execute(
                    "SELECT key,window,score,count FROM sentiment_scores WHERE ts > ?",
                    (cutoff,)
                ).fetchall()
                self.scores = defaultdict(dict)
                for key, window, score, count in rows:
                    self.scores[key][window] = {"score": score, "count": count}

    def update_articles(self):
        with self._lock:
            with get_db() as db:
                rows = db.execute("""
                    SELECT title,source,score,label,published,keywords,
                           model_tier,vader_score,finbert_score,full_text,text_length
                    FROM articles ORDER BY ingested DESC LIMIT 100
                """).fetchall()
                self.recent_articles = [
                    {"title": r[0], "source": r[1], "score": r[2], "label": r[3],
                     "published": r[4], "keywords": r[5], "model_tier": r[6],
                     "vader_score": r[7], "finbert_score": r[8],
                     "full_text": r[9], "text_length": r[10]}
                    for r in rows
                ]
                counts = db.execute(
                    "SELECT COUNT(*), SUM(full_text) FROM articles"
                ).fetchone()
                self.article_count  = counts[0] or 0
                self.fulltext_count = counts[1] or 0
                if self.recent_articles:
                    self.model_tier = (self.recent_articles[0].get("model_tier") or "vader")

    def update_entity_summary(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        with self._lock:
            with get_db() as db:
                rows = db.execute("""
                    SELECT ticker, COUNT(*) as mentions,
                           AVG(score) as avg_score,
                           MIN(score) as min_score,
                           MAX(score) as max_score
                    FROM entity_mentions WHERE ts > ?
                    GROUP BY ticker ORDER BY mentions DESC LIMIT 40
                """, (cutoff,)).fetchall()
                self.entity_summary = [
                    {"ticker": r[0], "mentions": r[1],
                     "avg_score": round(r[2],4) if r[2] else 0,
                     "min_score": round(r[3],4) if r[3] else 0,
                     "max_score": round(r[4],4) if r[4] else 0}
                    for r in rows
                ]

    def update_correlations(self, tickers: list):
        corr = compute_correlations(tickers)
        with self._lock:
            self.correlations.update(corr)

    def update_queue_len(self):
        with self._lock:
            with _fulltext_lock:
                self.fulltext_queue_len = len(_fulltext_queue)

    def add_alerts(self, new_alerts):
        with self._lock:
            for a in new_alerts:
                self.alerts.appendleft(a)

    def set_last_refresh(self):
        with self._lock:
            self.last_refresh = datetime.now().strftime("%H:%M:%S")

CACHE = DataCache()

# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND THREADS
# ══════════════════════════════════════════════════════════════════════════════

def run_refresh():
    """Main ingestion loop."""
    while True:
        try:
            all_tickers = [t for tl in WATCHLIST.values() for t in tl]
            price_rows  = fetch_prices(all_tickers)
            save_prices(price_rows)
            CACHE.update_prices(price_rows)
            prices_snap = {r["ticker"]: r for r in price_rows}
            articles    = fetch_rss(prices_snap) + fetch_newsapi(prices_snap)
            save_articles(articles)
            compute_aggregate_scores()
            new_alerts  = check_alerts()
            CACHE.update_scores()
            CACHE.update_articles()
            CACHE.update_entity_summary()
            CACHE.update_queue_len()
            CACHE.add_alerts(new_alerts)
            CACHE.set_last_refresh()
        except Exception:
            pass
        time.sleep(REFRESH_SEC)

def run_correlation_refresh():
    """Correlation computation is slow; run separately every 10 minutes."""
    while True:
        try:
            tickers = (WATCHLIST["Equities"] + WATCHLIST["Sectors"] +
                       WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"])
            CACHE.update_correlations(tickers)
        except Exception:
            pass
        time.sleep(600)

# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def sentiment_bar(score: float, width: int = 12) -> str:
    half   = width // 2
    filled = min(int(abs(score) * half), half)
    if score >= 0.05:
        return "─" * half + "│" + "█" * filled + "─" * (half - filled)
    elif score <= -0.05:
        return "─" * (half - filled) + "█" * filled + "│" + "─" * half
    return "─" * half + "│" + "─" * half

def fmt_score(s) -> str:
    return f"{s:+.4f}" if s is not None else "  n/a "

def fmt_chg(c: float) -> str:
    return f"{'+'if c>=0 else''}{c:.2f}%"

def tier_badge() -> str:
    t = CACHE.model_tier
    if "finbert" in t:
        return "[FinBERT+VADER]"
    return "[VADER only]"

def fulltext_badge() -> str:
    if not FULL_TEXT_ENABLED:
        return "FullText:OFF"
    q = CACHE.fulltext_queue_len
    fc = CACHE.fulltext_count
    return f"FullText:{fc}arts Q:{q}"

# ══════════════════════════════════════════════════════════════════════════════
#  TEXTUAL TUI
# ══════════════════════════════════════════════════════════════════════════════

class SentimentMonitorApp(App):
    CSS = """
    Screen            { background: #080808; }
    Header            { background: #000000; color: #ffcc00; }
    Footer            { background: #111111; }
    TabbedContent     { height: 1fr; }
    TabPane           { padding: 0 1; }
    DataTable         { height: 1fr; background: #0c0c0c; }
    DataTable > .datatable--header { background: #181818; color: #ffcc00; }
    DataTable > .datatable--cursor { background: #1a2e1a; }
    DataTable > .datatable--hover  { background: #141414; }
    #status_bar  { height: 1; background: #111; color: #555;
                   padding: 0 2; content-align: left middle; }
    #status_bar2 { height: 1; background: #0a0a0a; color: #444;
                   padding: 0 2; content-align: left middle; }
    """

    BINDINGS = [
        ("r", "refresh",   "Refresh"),
        ("q", "quit",      "Quit"),
        ("?", "show_help", "Help"),
        ("c", "open_cfg",  "Config"),
    ]

    TITLE     = "ESM  //  Economic Sentiment Monitor  v0.3"
    SUB_TITLE = "initialising…"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="status_bar")
        yield Static("", id="status_bar2")
        with TabbedContent(initial="equities"):
            with TabPane("📈 EQUITIES",  id="equities"):
                yield DataTable(id="eq_table",    zebra_stripes=True)
            with TabPane("🌍 MACRO/FX",  id="macrofx"):
                yield DataTable(id="fx_table",    zebra_stripes=True)
            with TabPane("🗂  SECTORS",   id="sectors"):
                yield DataTable(id="sec_table",   zebra_stripes=True)
            with TabPane("🧠 SENTIMENT", id="sentiment"):
                yield DataTable(id="sent_table",  zebra_stripes=True)
            with TabPane("🔗 ENTITIES",  id="entities"):
                yield DataTable(id="ent_table",   zebra_stripes=True)
            with TabPane("📰 NEWS FEED", id="newsfeed"):
                yield DataTable(id="news_table",  zebra_stripes=True)
            with TabPane("🔔 ALERTS",    id="alerts"):
                yield DataTable(id="alert_table", zebra_stripes=True)
        yield Footer()

    def on_mount(self):
        self._setup_tables()
        # start background threads
        threading.Thread(target=run_refresh,             daemon=True).start()
        threading.Thread(target=run_correlation_refresh, daemon=True).start()
        if FULL_TEXT_ENABLED:
            threading.Thread(target=fulltext_worker,     daemon=True).start()
        self.set_interval(10, self._ui_refresh)
        self.set_timer(3,     self._ui_refresh)

    def _setup_tables(self):
        # EQUITIES — added Corr column
        self.query_one("#eq_table", DataTable).add_columns(
            "Ticker","Price","Chg%","Sent 1h","▓ Bar ▓","Sent 24h","Corr","Tier","Lbl")
        self.query_one("#fx_table", DataTable).add_columns(
            "Asset","Price","Chg%","Sent 1h","▓ Bar ▓","Sent 24h","Corr","Lbl")
        # SECTORS — added Corr column
        self.query_one("#sec_table", DataTable).add_columns(
            "ETF","Sector","Price","Chg%","Sent 1h","▓ Heatmap ▓","Sent 24h","Corr")
        self.query_one("#sent_table", DataTable).add_columns(
            "Theme","1h Score","4h Score","24h Score","Trend","n(1h)","Context")
        self.query_one("#ent_table", DataTable).add_columns(
            "Ticker","Mentions 24h","Avg Sent","Min","Max","▓ Range ▓","In WL")
        # NEWS FEED — added FullTxt column
        self.query_one("#news_table", DataTable).add_columns(
            "Time","Source","Score","FinBERT","VADER","Tier","FT","Words","Headline")
        self.query_one("#alert_table", DataTable).add_columns(
            "Time","Sev","Key","Reason","Score")

    def _ui_refresh(self):
        # status bar 1 — NLP / NER / refresh
        spacy_s = "SpaCy ✓" if SPACY_READY else "SpaCy ✗"
        self.query_one("#status_bar", Static).update(
            f"  NLP: {tier_badge()}  |  NER: {spacy_s}  |  "
            f"{fulltext_badge()}  |  "
            f"Refresh: {CACHE.last_refresh}  |  "
            f"Articles: {CACHE.article_count}  |  Poll: {REFRESH_SEC}s"
        )
        # status bar 2 — config summary
        self.query_one("#status_bar2", Static).update(
            f"  Config: {CONFIG_PATH}  |  "
            f"Alert WARN≥{ALERT_WARN:.2f} CRIT≥{ALERT_CRIT:.2f}  |  "
            f"Corr window: 24h  |  C=open config"
        )
        CACHE.update_scores()
        CACHE.update_articles()
        CACHE.update_entity_summary()
        CACHE.update_queue_len()
        self._refresh_equities()
        self._refresh_macrofx()
        self._refresh_sectors()
        self._refresh_sentiment()
        self._refresh_entities()
        self._refresh_news()
        self._refresh_alerts()

    def _price_row(self, ticker):
        p = CACHE.prices.get(ticker, {})
        price = f"{p['price']:,.4f}" if p.get("price") else "—"
        chg   = p.get("change_pct", 0.0)
        return price, chg, (fmt_chg(chg) if p else "—")

    def _sent_for(self, key):
        d = CACHE.scores.get(key, {})
        return (d.get("1h",{}).get("score"),
                d.get("4h",{}).get("score"),
                d.get("24h",{}).get("score"))

    def _refresh_equities(self):
        t = self.query_one("#eq_table", DataTable)
        t.clear()
        tier_s = "FB+VD" if FINBERT_READY else "VD"
        for ticker in WATCHLIST["Equities"]:
            price, chg, chg_s = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            corr = CACHE.correlations.get(ticker)
            lbl  = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, price, chg_s, fmt_score(s1),
                      sentiment_bar(s1 or 0), fmt_score(s24),
                      fmt_corr(corr), tier_s, lbl)

    def _refresh_macrofx(self):
        t = self.query_one("#fx_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"]:
            price, chg, chg_s = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            corr = CACHE.correlations.get(ticker)
            lbl  = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, price, chg_s, fmt_score(s1),
                      sentiment_bar(s1 or 0), fmt_score(s24),
                      fmt_corr(corr), lbl)

    def _refresh_sectors(self):
        t = self.query_one("#sec_table", DataTable)
        t.clear()
        for ticker, name in SECTORS.items():
            price, chg, chg_s = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            corr = CACHE.correlations.get(ticker)
            t.add_row(ticker, name, price, chg_s,
                      fmt_score(s1), sentiment_bar(s1 or 0),
                      fmt_score(s24), fmt_corr(corr))

    def _refresh_sentiment(self):
        t = self.query_one("#sent_table", DataTable)
        t.clear()
        for theme in list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]:
            d   = CACHE.scores.get(theme, {})
            s1  = d.get("1h",{}).get("score")
            s4  = d.get("4h",{}).get("score")
            s24 = d.get("24h",{}).get("score")
            n1  = d.get("1h",{}).get("count", 0)
            trend = ("▲" if (s1 or 0) > (s4 or 0) + 0.01
                     else ("▼" if (s1 or 0) < (s4 or 0) - 0.01 else "→")
                     ) if s1 is not None and s4 is not None else "—"
            ctx = ("Bullish" if (s1 or 0) > 0.1 else
                   "Bearish" if (s1 or 0) < -0.1 else
                   "Mixed"   if s1 is not None else "—")
            t.add_row(theme.upper(), fmt_score(s1), fmt_score(s4),
                      fmt_score(s24), trend, str(n1 or "—"), ctx)

    def _refresh_entities(self):
        t = self.query_one("#ent_table", DataTable)
        t.clear()
        all_tickers = {tk for tl in WATCHLIST.values() for tk in tl}
        for row in CACHE.entity_summary:
            rng   = round((row["max_score"] - row["min_score"]) * 10)
            bar   = "▓" * min(max(rng, 0), 10)
            in_wl = "✓" if row["ticker"] in all_tickers else "—"
            t.add_row(row["ticker"], str(row["mentions"]),
                      fmt_score(row["avg_score"]),
                      fmt_score(row["min_score"]),
                      fmt_score(row["max_score"]),
                      bar, in_wl)

    def _refresh_news(self):
        t = self.query_one("#news_table", DataTable)
        t.clear()
        for art in CACHE.recent_articles[:80]:
            pub    = (art["published"] or "")[:16]
            tier   = "FB" if (art.get("model_tier") or "").startswith("fin") else "VD"
            ft_s   = "✓" if art.get("full_text") else "·"
            words  = str(art.get("text_length") or "—")
            title  = (art["title"] or "")[:75]
            t.add_row(pub, (art["source"] or "")[:14],
                      fmt_score(art["score"]),
                      fmt_score(art.get("finbert_score")),
                      fmt_score(art.get("vader_score")),
                      tier, ft_s, words, title)

    def _refresh_alerts(self):
        t = self.query_one("#alert_table", DataTable)
        t.clear()
        seen = set()
        with get_db() as db:
            rows = db.execute(
                "SELECT ts,severity,key,reason,score FROM alerts ORDER BY ts DESC LIMIT 40"
            ).fetchall()
        for r in rows:
            key = r[2] + r[3]
            if key not in seen:
                seen.add(key)
                t.add_row(r[0][:19], r[1], r[2], r[3], fmt_score(r[4]))

    def action_refresh(self):
        self._ui_refresh()

    def action_open_cfg(self):
        self.notify(
            f"Config file: {CONFIG_PATH}\n"
            "Edit it with any text editor, then restart ESM.\n"
            "Settings: watchlists, RSS feeds, poll interval,\n"
            "alert thresholds, full-text toggle.",
            title="Config",
            timeout=10,
        )

    def action_show_help(self):
        self.notify(
            "R = Force refresh   Q = Quit   C = Config path   ? = Help\n"
            "\n"
            "v0.3 features:\n"
            "  ✓ Full article text (newspaper3k) — see FT col in NEWS FEED\n"
            "  ✓ Runtime config at ~/.esm/config.yml\n"
            "  ✓ Corr column = Pearson r(sentiment→price, 1h lag, 24h window)\n"
            "    +1.0 = sentiment perfectly predicts price direction\n"
            "    -1.0 = market consistently does the opposite\n"
            "     n/a = insufficient history (<6 data points)\n"
            "\n"
            "NLP: VADER (fast) + FinBERT (accurate, optional)\n"
            "NER: SpaCy + regex → entity → ticker",
            title="ESM v0.3  Help",
            timeout=15,
        )

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 64)
    print("  ECONOMIC SENTIMENT MONITOR  v0.3")
    print("=" * 64)
    init_db()
    print(f"  DB           : {DB_PATH}")
    print(f"  Config       : {CONFIG_PATH}")
    print(f"  Poll interval: {REFRESH_SEC}s")
    print(f"  NewsAPI      : {'SET' if NEWS_API_KEY else 'not set (RSS only)'}")
    print(f"  FRED         : {'SET' if FRED_API_KEY else 'not set'}")
    print(f"  FinBERT      : {'LOADED ✓' if FINBERT_READY else 'not available — VADER only'}")
    print(f"  SpaCy NER    : {'LOADED ✓' if SPACY_READY  else 'not available — regex fallback'}")
    print(f"  Full text    : {'ENABLED ✓' if FULL_TEXT_ENABLED else 'disabled (install newspaper3k)'}")
    print(f"  Correlation  : enabled (needs ~4h history to populate)")
    print("-" * 64)
    if not NEWSPAPER_READY:
        print("  → Full text : pip install newspaper3k --break-system-packages")
    if not FINBERT_READY:
        print("  → FinBERT   : pip install transformers torch --break-system-packages")
    if not SPACY_READY:
        print("  → SpaCy     : pip install spacy --break-system-packages")
        print("                python -m spacy download en_core_web_sm")
    print("=" * 64)
    print("  Starting background threads…")
    time.sleep(0.5)
    SentimentMonitorApp().run()
