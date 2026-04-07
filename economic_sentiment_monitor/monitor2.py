#!/usr/bin/env python3
"""
Economic Sentiment Monitor  v0.4
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Upgrades vs v0.3:
  [1] Full article text ingestion via newspaper3k
  [2] Runtime config via ~/.esm/config.yml
  [3] Sentiment–price correlation column

v0.4 new:
  [4] FRED macro data integration (free API key)
      — live economic series: CPI, PCE, NFP, GDP, unemployment,
        Fed funds rate, 10Y yield, M2, yield curve spread
      — surprise score: actual vs prior period → auto-boosts
        the matched sentiment theme score on release day
      — new MACRO DATA tab showing series values + surprise
      — upcoming release calendar pulled from FRED release dates
      — works without key (series omitted, rest unchanged)

  [5] Z-score anomaly detection replacing fixed delta threshold
      — computes rolling mean + std per key from 7-day history
      — fires WARN when |z| > 2.0, CRIT when |z| > 3.0
      — per-key calibration: volatile keys need bigger moves
        to alert; stable keys alert on small persistent drifts
      — slow-drift detector: flags keys trending same direction
        for 3+ consecutive windows even if no single spike
      — z-score and sigma shown in alert reason text
      — falls back to delta threshold if < 10 history points
"""

import os, sys, time, threading, sqlite3, hashlib, json, re, math, logging, traceback
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

# ── structured logger — writes to ~/.esm/esm.log, rotates at 5 MB ────────────
LOG_PATH = os.path.expanduser("~/.esm/esm.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

from logging.handlers import RotatingFileHandler as _RFH
_handler = _RFH(LOG_PATH, maxBytes=5_000_000, backupCount=2)
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log = logging.getLogger("esm")
log.setLevel(logging.DEBUG)
log.addHandler(_handler)
# also echo WARNING+ to stdout so startup problems are visible
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.WARNING)
_sh.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
log.addHandler(_sh)

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

# Alert sensitivity — z-score thresholds (v0.4)
# WARN fires when |z| exceeds warn_z, CRIT when |z| exceeds crit_z
# Falls back to delta thresholds below if < 10 history points exist
alert_zscore_warn: 2.0
alert_zscore_crit: 3.0
# Slow-drift: alert if key moves same direction N consecutive windows
alert_drift_consecutive: 3

# FRED series to track (requires fred_api_key)
# Format: label: FRED_series_id: linked_theme
fred_series:
  - ["CPI YoY",      "CPIAUCSL",   "inflation"]
  - ["Core CPI",     "CPILFESL",   "inflation"]
  - ["PCE",          "PCEPI",      "inflation"]
  - ["Fed Funds",    "FEDFUNDS",   "rates"]
  - ["Unemployment", "UNRATE",     "employment"]
  - ["Nonfarm Pay",  "PAYEMS",     "employment"]
  - ["GDP Growth",   "A191RL1Q225SBEA", "growth"]
  - ["10Y Yield",    "DGS10",      "rates"]
  - ["2Y Yield",     "DGS2",       "rates"]
  - ["M2 Money",     "M2SL",       "inflation"]
  - ["Credit Spread","BAMLH0A0HYM2","credit"]


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

ALERT_ZSCORE_WARN   = float(CFG.get("alert_zscore_warn",   2.0))
ALERT_ZSCORE_CRIT   = float(CFG.get("alert_zscore_crit",   3.0))
ALERT_DRIFT_N       = int(CFG.get("alert_drift_consecutive", 3))

def _fred_series(cfg_list) -> list:
    """Parse fred_series list from config → [(label, series_id, theme), ...]"""
    if not isinstance(cfg_list, list):
        return []
    out = []
    for item in cfg_list:
        if isinstance(item, list) and len(item) == 3:
            out.append(tuple(str(x) for x in item))
    return out

FRED_SERIES = _fred_series(CFG.get("fred_series", [])) or [
    ("CPI YoY",      "CPIAUCSL",        "inflation"),
    ("Core CPI",     "CPILFESL",        "inflation"),
    ("PCE",          "PCEPI",           "inflation"),
    ("Fed Funds",    "FEDFUNDS",        "rates"),
    ("Unemployment", "UNRATE",          "employment"),
    ("Nonfarm Pay",  "PAYEMS",          "employment"),
    ("GDP Growth",   "A191RL1Q225SBEA", "growth"),
    ("10Y Yield",    "DGS10",           "rates"),
    ("2Y Yield",     "DGS2",            "rates"),
    ("M2 Money",     "M2SL",            "inflation"),
    ("Credit Spread","BAMLH0A0HYM2",    "credit"),
]


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
        CREATE TABLE IF NOT EXISTS fred_data (
            series_id   TEXT,
            label       TEXT,
            theme       TEXT,
            ts          TEXT,
            value       REAL,
            prev_value  REAL,
            surprise    REAL,
            units       TEXT,
            PRIMARY KEY (series_id, ts)
        );
        CREATE INDEX IF NOT EXISTS ix_fred_ts   ON fred_data(ts);
        CREATE INDEX IF NOT EXISTS ix_alerts_ts ON alerts(ts);
        
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
            ("numeric_signals","TEXT", "NULL"),
            ("source_tier", "TEXT",    "NULL"),
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
    log.info("fulltext_worker thread started")
    while True:
        try:
            with _fulltext_lock:
                item = _fulltext_queue.popleft() if _fulltext_queue else None
            if item:
                enrich_article_fulltext(item[0], item[1])
                CACHE.clear_error("fulltext")
                time.sleep(0.5)
            else:
                time.sleep(3)
        except Exception as e:
            CACHE.record_error("fulltext", e)
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
#  [4]  FRED MACRO DATA
# ══════════════════════════════════════════════════════════════════════════════

FRED_BASE = "https://api.stlouisfed.org/fred"

def _fred_get(endpoint: str, params: dict) -> dict:
    """Raw FRED API call — returns {} on any failure."""
    if not FRED_API_KEY:
        return {}
    try:
        params["api_key"]     = FRED_API_KEY
        params["file_type"]   = "json"
        r = requests.get(f"{FRED_BASE}/{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

def fetch_fred_series(series_id: str, label: str, theme: str) -> dict | None:
    """
    Fetch the two most recent observations for a FRED series.
    Returns dict with current value, prior value, and surprise score.
    Surprise = (current - prior) / abs(prior) * 100  (percentage change)
    """
    data = _fred_get("series/observations", {
        "series_id":   series_id,
        "sort_order":  "desc",
        "limit":       "5",
        "observation_start": "2020-01-01",
    })
    obs = [o for o in data.get("observations", [])
           if o.get("value") not in (".", "", None)]
    if len(obs) < 2:
        return None
    try:
        cur  = float(obs[0]["value"])
        prev = float(obs[1]["value"])
        surprise = round((cur - prev) / abs(prev) * 100, 4) if prev else 0.0
        ts_str   = obs[0].get("date", datetime.now(timezone.utc).date().isoformat())
        return {
            "series_id":  series_id,
            "label":      label,
            "theme":      theme,
            "ts":         ts_str,
            "value":      cur,
            "prev_value": prev,
            "surprise":   surprise,
            "units":      "",
        }
    except Exception:
        return None

def fetch_all_fred() -> list:
    """Fetch all configured FRED series. Returns list of row dicts."""
    if not FRED_API_KEY:
        return []
    rows = []
    for label, series_id, theme in FRED_SERIES:
        row = fetch_fred_series(series_id, label, theme)
        if row:
            rows.append(row)
        time.sleep(0.1)   # stay well within free-tier rate limit
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
    """
    When a FRED series has a surprise (non-zero pct change vs prior),
    inject a synthetic sentiment score into the matched theme's aggregate.
    Large positive surprise → positive sentiment boost for that theme.
    Large negative surprise → negative sentiment boost.
    Boost is capped at ±0.30 and scaled by surprise magnitude.
    """
    if not fred_rows:
        return
    ts_now = datetime.now(timezone.utc).isoformat()
    with get_db() as db:
        for row in fred_rows:
            if abs(row["surprise"]) < 0.5:
                continue   # less than 0.5% change — not noteworthy
            theme   = row["theme"]
            # scale surprise to sentiment range: 5% surprise → ~0.15 boost
            boost   = max(-0.30, min(0.30, row["surprise"] / 33.0))
            # insert as a synthetic high-confidence article into scoring
            synth_id = hashlib.md5(
                f"FRED:{row['series_id']}:{row['ts']}".encode()
            ).hexdigest()
            db.execute("""
                INSERT OR IGNORE INTO articles
                (id,title,source,url,published,ingested,vader_score,finbert_score,
                 score,label,confidence,model_tier,keywords,entities,full_text,text_length)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                synth_id,
                f"[FRED] {row['label']}: {row['value']:.2f} (prev {row['prev_value']:.2f}, {row['surprise']:+.2f}%)",
                "FRED", "", row["ts"], ts_now,
                boost, boost, boost,
                "POS" if boost > 0.05 else ("NEG" if boost < -0.05 else "NEU"),
                0.95, "fred_surprise",
                json.dumps([theme]), json.dumps([]),
                0, 0
            ))
            # direct entity mention for the theme
            db.execute("""
                INSERT INTO entity_mentions (article_id,entity_raw,ticker,score,ts)
                VALUES (?,?,?,?,?)
            """, (synth_id, row["label"], theme, boost, ts_now))

# ══════════════════════════════════════════════════════════════════════════════
#  [5]  Z-SCORE ANOMALY DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def _rolling_stats(key: str, window_days: int = 7) -> tuple:
    """
    Compute (mean, std, recent_scores_list) for a key's 1h sentiment
    over the last window_days from the DB.
    Returns (None, None, []) if insufficient data.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (key, cutoff)).fetchall()
    scores = [r[0] for r in rows]
    n = len(scores)
    if n < 10:
        return None, None, scores
    mean = sum(scores) / n
    variance = sum((s - mean) ** 2 for s in scores) / n
    std  = math.sqrt(variance) if variance > 0 else 0.0
    return round(mean, 5), round(std, 5), scores

def _check_slow_drift(scores: list, n: int = 3) -> str | None:
    """
    Detect if the last n scores are all moving in the same direction.
    Returns 'up', 'down', or None.
    """
    if len(scores) < n + 1:
        return None
    tail = scores[-(n+1):]
    diffs = [tail[i+1] - tail[i] for i in range(len(tail)-1)]
    if all(d > 0 for d in diffs):
        return "up"
    if all(d < 0 for d in diffs):
        return "down"
    return None

def check_alerts():
    """
    v0.4 alert engine — z-score based with slow-drift detection.
    Falls back to delta threshold when history is thin (< 10 points).
    """
    alerts = []
    ts_now = datetime.now(timezone.utc).isoformat()

    with get_db() as db:
        # get all keys that have a recent 1h score
        rows_1h = db.execute("""
            SELECT key, score, count FROM sentiment_scores
            WHERE window='1h'
            ORDER BY ts DESC
        """).fetchall()
        # deduplicate — keep only the most recent row per key
        seen_keys = {}
        for key, score, count in rows_1h:
            if key not in seen_keys:
                seen_keys[key] = (score, count)

        for key, (score, count) in seen_keys.items():
            if count < ALERT_MIN_COUNT:
                continue

            mean, std, history = _rolling_stats(key)

            # ── z-score path (enough history) ─────────────────────────────
            if mean is not None and std is not None and std > 0:
                z = (score - mean) / std
                if abs(z) >= ALERT_ZSCORE_CRIT:
                    severity  = "CRIT"
                    direction = "SURGE ▲" if z > 0 else "DROP  ▼"
                    reason    = (f"[Z] {key} {direction} z={z:+.2f}σ "
                                 f"(score={score:+.4f} μ={mean:+.4f} σ={std:.4f})")
                elif abs(z) >= ALERT_ZSCORE_WARN:
                    severity  = "WARN"
                    direction = "SURGE ▲" if z > 0 else "DROP  ▼"
                    reason    = (f"[Z] {key} {direction} z={z:+.2f}σ "
                                 f"(score={score:+.4f} μ={mean:+.4f} σ={std:.4f})")
                else:
                    # check slow drift even if no z-score spike
                    drift = _check_slow_drift(history, ALERT_DRIFT_N)
                    if drift:
                        severity = "WARN"
                        direction = "DRIFT ▲" if drift == "up" else "DRIFT ▼"
                        reason = (f"[DRIFT] {key} {direction} "
                                  f"{ALERT_DRIFT_N} consecutive windows "
                                  f"(last={score:+.4f})")
                    else:
                        continue

            # ── delta fallback (thin history) ──────────────────────────────
            else:
                prev_row = db.execute("""
                    SELECT score FROM sentiment_scores
                    WHERE key=? AND window='4h'
                    ORDER BY ts DESC LIMIT 1
                """, (key,)).fetchone()
                if not prev_row:
                    continue
                delta = score - prev_row[0]
                if abs(delta) < ALERT_WARN:
                    continue
                severity  = "CRIT" if abs(delta) >= ALERT_CRIT else "WARN"
                direction = "SURGE ▲" if delta > 0 else "DROP  ▼"
                reason    = (f"[Δ] {key} {direction} "
                             f"Δ{abs(delta):.3f} vs 4h (n={count}, thin history)")

            alert_id = hashlib.md5(
                f"{key}{round(score,2)}{datetime.now(timezone.utc).date()}".encode()
            ).hexdigest()
            alerts.append({
                "id": alert_id, "ts": ts_now,
                "severity": severity, "key": key,
                "reason": reason, "score": score,
            })

        if alerts:
            db.executemany("""
                INSERT OR IGNORE INTO alerts
                (id,ts,severity,key,reason,score)
                VALUES (:id,:ts,:severity,:key,:reason,:score)
            """, alerts)
    return alerts



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
        "full_text":        0,
        "text_length":      0,
        "numeric_signals":  "{}",
        "source_tier":      "C",
    }

def fetch_rss(prices: dict) -> list:
    items = []
    all_feeds = RSS_FEEDS + EXTENDED_RSS_FEEDS
    for source, url in all_feeds:
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


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED DATA CACHE
# ══════════════════════════════════════════════════════════════════════════════

class DataCache:
    def __init__(self):
        self._lock              = threading.Lock()
        self.prices: dict       = {}
        self.scores: dict       = {}
        self.recent_articles    = []
        self.entity_summary     = []
        self.correlations: dict = {}
        self.fred_data: list    = []
        self.alerts: deque      = deque(maxlen=50)
        self.last_refresh       = "—"
        self.article_count      = 0
        self.fulltext_count     = 0
        self.model_tier         = "vader"
        self.fulltext_queue_len = 0
        # ── error tracking per thread ─────────────────────────────────────
        self.err_counts: dict   = {
            "ingest": 0, "fulltext": 0, "fred": 0, "corr": 0, "analytics": 0
        }
        self.last_errors: dict  = {
            "ingest": "", "fulltext": "", "fred": "", "corr": "", "analytics": ""
        }

    def record_error(self, thread: str, exc: Exception):
        """Log exception to file and increment visible counter."""
        msg = f"{type(exc).__name__}: {exc}"
        tb  = traceback.format_exc()
        log.error("[%s] %s\n%s", thread, msg, tb)
        with self._lock:
            self.err_counts[thread]  = self.err_counts.get(thread, 0) + 1
            self.last_errors[thread] = f"{datetime.now().strftime('%H:%M:%S')} {msg[:60]}"

    def clear_error(self, thread: str):
        """Reset error counter for a thread after a successful cycle."""
        with self._lock:
            self.err_counts[thread]  = 0
            self.last_errors[thread] = ""

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
                           model_tier,vader_score,finbert_score,full_text,text_length,
                           numeric_signals,source_tier
                    FROM articles ORDER BY ingested DESC LIMIT 100
                """).fetchall()
                self.recent_articles = [
                    {"title": r[0], "source": r[1], "score": r[2], "label": r[3],
                     "published": r[4], "keywords": r[5], "model_tier": r[6],
                     "vader_score": r[7], "finbert_score": r[8],
                     "full_text": r[9], "text_length": r[10],
                     "numeric_signals": r[11], "source_tier": r[12]}
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

    def update_fred_data(self):
        """Load most recent value per FRED series from DB."""
        with self._lock:
            with get_db() as db:
                rows = db.execute("""
                    SELECT series_id, label, theme, ts, value, prev_value, surprise
                    FROM fred_data
                    WHERE ts = (
                        SELECT MAX(ts) FROM fred_data f2
                        WHERE f2.series_id = fred_data.series_id
                    )
                    ORDER BY theme, label
                """).fetchall()
                self.fred_data = [
                    {"series_id": r[0], "label": r[1], "theme": r[2],
                     "ts": r[3], "value": r[4], "prev_value": r[5],
                     "surprise": r[6]}
                    for r in rows
                ]

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
    log.info("run_refresh thread started")
    while True:
        try:
            all_tickers = [t for tl in WATCHLIST.values() for t in tl]
            price_rows  = fetch_prices(all_tickers)
            save_prices(price_rows)
            CACHE.update_prices(price_rows)
            prices_snap = {r["ticker"]: r for r in price_rows}
            articles    = fetch_rss(prices_snap) + fetch_newsapi(prices_snap)
            articles    = [enrich_headline_metadata(a) for a in articles]
            save_articles(articles)
            compute_aggregate_scores()
            new_alerts  = check_alerts()
            CACHE.update_scores()
            CACHE.update_articles()
            CACHE.update_entity_summary()
            CACHE.update_queue_len()
            CACHE.update_fred_data()
            CACHE.add_alerts(new_alerts)
            CACHE.set_last_refresh()
            CACHE.clear_error("ingest")
            log.debug("run_refresh cycle OK — %d articles, %d prices, %d alerts",
                      len(articles), len(price_rows), len(new_alerts))
        except Exception as e:
            CACHE.record_error("ingest", e)
        time.sleep(REFRESH_SEC)

def run_fred_refresh():
    """FRED fetch loop — runs every 30 min (data updates infrequently)."""
    log.info("run_fred_refresh thread started (key: %s)", "SET" if FRED_API_KEY else "NOT SET")
    while True:
        try:
            if FRED_API_KEY:
                fred_rows = fetch_all_fred()
                save_fred_data(fred_rows)
                apply_fred_surprise_boost(fred_rows)
                CACHE.update_fred_data()
                CACHE.clear_error("fred")
                log.debug("run_fred_refresh OK — %d series fetched", len(fred_rows))
        except Exception as e:
            CACHE.record_error("fred", e)
        time.sleep(1800)

def run_correlation_refresh():
    """Correlation computation — runs every 10 minutes."""
    log.info("run_correlation_refresh thread started")
    while True:
        try:
            tickers = (WATCHLIST["Equities"] + WATCHLIST["Sectors"] +
                       WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"])
            CACHE.update_correlations(tickers)
            CACHE.clear_error("corr")
            log.debug("run_correlation_refresh OK — %d tickers", len(tickers))
        except Exception as e:
            CACHE.record_error("corr", e)
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
    q  = CACHE.fulltext_queue_len
    fc = CACHE.fulltext_count
    return f"FT:{fc} Q:{q}"

def sparkline(scores: list, width: int = 10) -> str:
    blocks = " ▁▂▃▄▅▆▇█"
    if not scores:
        return "─" * width
    tail = scores[-width:]
    if len(tail) < 2:
        return "─" * width
    mn, mx = min(tail), max(tail)
    span = mx - mn
    if span == 0:
        return "▄" * len(tail) + "─" * (width - len(tail))
    chars = [blocks[max(0, min(int((v - mn) / span * (len(blocks)-1)), len(blocks)-1))]
             for v in tail]
    return "".join(chars).ljust(width, "─")

def fear_greed_index(scores: dict) -> tuple:
    weights = {"rates": 0.25, "growth": 0.20, "employment": 0.15,
               "inflation": 0.15, "credit": 0.15, "trade": 0.10}
    total_w, total_s = 0.0, 0.0
    for theme, w in weights.items():
        s = scores.get(theme, {}).get("1h", {}).get("score")
        if s is not None:
            total_s += s * w
            total_w += w
    if total_w == 0:
        return 0, "NEUTRAL"
    val = round((total_s / total_w) * 100)
    if val >= 30:    label = "EXTREME GREED"
    elif val >= 10:  label = "GREED"
    elif val >= 5:   label = "MILD GREED"
    elif val <= -30: label = "EXTREME FEAR"
    elif val <= -10: label = "FEAR"
    elif val <= -5:  label = "MILD FEAR"
    else:            label = "NEUTRAL"
    return val, label

def fmt_corr(r) -> str:
    if r is None:
        return " n/a"
    return f"{'+' if r >= 0 else ''}{r:.2f}"

def divergence_score(ticker: str, scores: dict, prices: dict) -> str:
    s1  = scores.get(ticker, {}).get("1h", {}).get("score")
    chg = prices.get(ticker, {}).get("change_pct", 0.0)
    if s1 is None:
        return "—"
    if s1 > 0.05 and chg < -0.5:
        return "↑s↓p BULL"
    if s1 < -0.05 and chg > 0.5:
        return "↓s↑p BEAR"
    if abs(s1) > 0.05 and abs(chg) > 0.5:
        return "aligned"
    return "—"

def velocity_label(key: str) -> str:
    d   = CACHE.scores.get(key, {})
    n1  = d.get("1h",  {}).get("count", 0) or 0
    n24 = d.get("24h", {}).get("count", 0) or 0
    avg = n24 / 24 if n24 else 0
    if avg == 0:
        return ""
    if n1 > avg * 3:
        return "HOT"
    if n1 > avg * 1.5:
        return "WARM"
    return ""

def get_sparkline_history(key: str, window_hours: int = 24, points: int = 12) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (key, cutoff)).fetchall()
    scores = [r[0] for r in rows]
    return scores[-points:] if len(scores) > points else scores

# ══════════════════════════════════════════════════════════════════════════════
#  ANALYTICS ENGINE  v0.6
#  [A] Sentiment momentum & lead/lag analysis
#  [B] Regime detection
#  [C] Bayesian source confidence scoring
#  [D] Extended RSS feeds + headline enrichment
# ══════════════════════════════════════════════════════════════════════════════

# ── [A] Sentiment momentum & lead/lag ────────────────────────────────────────

def compute_lead_lag(ticker: str, max_lag_hours: int = 6) -> dict:
    """
    For each lag offset (0.5h, 1h, 2h, 3h, 4h, 6h), compute Pearson r between
    the sentiment score at time T and the price change_pct at time T+lag.
    Returns {lag_hours: r} and the best lag.
    """
    results = {}
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    with get_db() as db:
        sent_rows = db.execute("""
            SELECT ts, score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (ticker, cutoff)).fetchall()

        for lag_h in [0.5, 1, 2, 3, 4, 6]:
            lag_td = timedelta(hours=lag_h)
            xs, ys = [], []
            for ts_str, s_score in sent_rows:
                try:
                    t_base = datetime.fromisoformat(ts_str.replace("Z",""))
                    target = (t_base + lag_td).isoformat()
                    price_row = db.execute("""
                        SELECT change_pct FROM market_prices
                        WHERE ticker=? AND ts >= ?
                        ORDER BY ts ASC LIMIT 1
                    """, (ticker, target)).fetchone()
                    if price_row:
                        xs.append(s_score)
                        ys.append(price_row[0])
                except Exception:
                    pass
            r = _pearson(xs, ys)
            results[lag_h] = r

    best_lag = None
    best_r   = None
    for lag_h, r in results.items():
        if r is not None and (best_r is None or abs(r) > abs(best_r)):
            best_lag = lag_h
            best_r   = r
    return {"by_lag": results, "best_lag": best_lag, "best_r": best_r}

def compute_sentiment_momentum(key: str) -> dict:
    """
    Acceleration: difference between current 1h score and the 1h score
    from 3 windows ago. Positive = accelerating bullish, negative = accelerating bearish.
    Also returns consecutive direction count.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (key, cutoff)).fetchall()
    scores = [r[0] for r in rows]
    if len(scores) < 2:
        return {"acceleration": None, "consecutive": 0, "direction": None}

    acceleration = round(scores[-1] - scores[max(0, len(scores)-4)], 4)
    # count consecutive same-direction moves
    direction = "up" if scores[-1] > scores[-2] else "down"
    consecutive = 1
    for i in range(len(scores)-2, 0, -1):
        if direction == "up"   and scores[i] > scores[i-1]:
            consecutive += 1
        elif direction == "down" and scores[i] < scores[i-1]:
            consecutive += 1
        else:
            break
    return {"acceleration": acceleration, "consecutive": consecutive, "direction": direction}

# ── [B] Regime detection ──────────────────────────────────────────────────────

REGIMES = {
    "risk_on":     "RISK-ON  ▲",
    "risk_off":    "RISK-OFF ▼",
    "high_vol":    "HIGH-VOL ~",
    "low_vol":     "LOW-VOL  —",
    "neutral":     "NEUTRAL  →",
}

def detect_regime() -> dict:
    """
    Classify current market regime from combination of:
    - Global sentiment level and trend
    - Sentiment volatility (std of last 7d 1h scores)
    - Article velocity (global n articles/hour)
    - Fear/greed index
    - Credit spread direction
    Returns {regime, confidence, signals}
    """
    fg_val, _ = fear_greed_index(CACHE.scores)

    # sentiment volatility from 7-day history
    cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key='GLOBAL' AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (cutoff_7d,)).fetchall()
    hist = [r[0] for r in rows]
    n    = len(hist)
    if n < 4:
        return {"regime": "neutral", "label": REGIMES["neutral"],
                "confidence": 0.0, "signals": ["insufficient history"]}

    mean_sent = sum(hist) / n
    variance  = sum((s - mean_sent)**2 for s in hist) / n
    std_sent  = math.sqrt(variance)

    # current momentum
    mom = compute_sentiment_momentum("GLOBAL")
    accel = mom["acceleration"] or 0.0
    consecutive = mom["consecutive"]

    # article velocity
    d    = CACHE.scores.get("GLOBAL", {})
    n1h  = d.get("1h",  {}).get("count", 0) or 0
    n24h = d.get("24h", {}).get("count", 0) or 0
    hourly_avg = n24h / 24 if n24h else 0
    velocity_ratio = n1h / hourly_avg if hourly_avg > 0 else 1.0

    signals = []
    score_vec = 0.0

    # fear/greed contribution
    if fg_val >= 15:
        signals.append(f"FG:+{fg_val} greed")
        score_vec += 1.5
    elif fg_val <= -15:
        signals.append(f"FG:{fg_val} fear")
        score_vec -= 1.5
    else:
        signals.append(f"FG:{fg_val} neutral")

    # sentiment volatility
    if std_sent > 0.12:
        signals.append(f"vol:HIGH σ={std_sent:.3f}")
        score_vec *= 0.5   # high vol makes trend less reliable
    else:
        signals.append(f"vol:low σ={std_sent:.3f}")

    # acceleration
    if accel > 0.05:
        signals.append(f"accel:↑{accel:+.3f}")
        score_vec += 1.0
    elif accel < -0.05:
        signals.append(f"accel:↓{accel:+.3f}")
        score_vec -= 1.0

    # velocity
    if velocity_ratio > 2.5:
        signals.append(f"velocity:HOT {velocity_ratio:.1f}x")
        score_vec *= 1.3

    # consecutive direction
    if consecutive >= 3:
        signals.append(f"streak:{consecutive}x {mom['direction']}")

    # classify
    if std_sent > 0.15 and velocity_ratio > 2.0:
        regime = "high_vol"
        confidence = min(0.5 + std_sent, 0.95)
    elif score_vec >= 1.5:
        regime = "risk_on"
        confidence = min(0.4 + abs(score_vec) * 0.1, 0.92)
    elif score_vec <= -1.5:
        regime = "risk_off"
        confidence = min(0.4 + abs(score_vec) * 0.1, 0.92)
    elif std_sent < 0.05 and abs(fg_val) < 10:
        regime = "low_vol"
        confidence = 0.6
    else:
        regime = "neutral"
        confidence = 0.4

    return {
        "regime": regime,
        "label":  REGIMES[regime],
        "confidence": round(confidence, 2),
        "signals": signals,
        "fg_val": fg_val,
        "std_sent": round(std_sent, 4),
        "accel": round(accel, 4),
    }

# ── [C] Bayesian source confidence scoring ────────────────────────────────────

def compute_bayesian_signal(ticker: str) -> dict:
    """
    Compute a Bayesian-weighted directional confidence for a ticker.

    For each source that has covered this ticker in the last 24h:
      - Look up how many times that source's sentiment direction matched
        the subsequent 1h price direction in the last 30 days.
      - Weight each article's sentiment by that source's historical accuracy.
      - Combine into a posterior direction probability.

    Returns {direction, confidence, sources_used, prior_weight, signal_str}
    """
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    with get_db() as db:
        # recent articles mentioning this ticker
        recent = db.execute("""
            SELECT a.score, a.source, a.ingested
            FROM articles a
            JOIN entity_mentions em ON em.article_id = a.id
            WHERE em.ticker=? AND a.ingested > ?
            ORDER BY a.ingested DESC LIMIT 50
        """, (ticker, cutoff_24h)).fetchall()

        if not recent:
            return {"direction": None, "confidence": 0.0,
                    "sources_used": 0, "signal_str": "no data"}

        # compute per-source historical accuracy over last 30d
        source_acc = {}
        sources = list({r[1] for r in recent if r[1]})
        for src in sources:
            src_rows = db.execute("""
                SELECT a.score, a.ingested
                FROM articles a
                JOIN entity_mentions em ON em.article_id = a.id
                WHERE em.ticker=? AND a.source=? AND a.ingested > ?
                ORDER BY a.ingested ASC
            """, (ticker, src, cutoff_30d)).fetchall()

            correct, total = 0, 0
            for score, ing_ts in src_rows:
                # find price 1h later
                target_ts = (datetime.fromisoformat(ing_ts.replace("Z",""))
                             + timedelta(hours=1)).isoformat()
                p = db.execute("""
                    SELECT change_pct FROM market_prices
                    WHERE ticker=? AND ts >= ?
                    ORDER BY ts ASC LIMIT 1
                """, (ticker, target_ts)).fetchone()
                if p:
                    total += 1
                    sent_bull = score > 0.02
                    price_up  = p[0] > 0
                    if sent_bull == price_up:
                        correct += 1
            # Laplace smoothing: add 1 correct + 1 incorrect to avoid 0/1
            acc = (correct + 1) / (total + 2) if total >= 3 else 0.52
            source_acc[src] = round(acc, 3)

    # bayesian update: start with 50/50 prior
    log_odds = 0.0
    sources_used = 0
    for score, src, _ in recent:
        if abs(score) < 0.03:
            continue   # ignore near-neutral
        acc   = source_acc.get(src, 0.52)
        # convert accuracy to log-odds update
        if score > 0:
            lo = math.log((acc + 1e-9) / (1 - acc + 1e-9))
        else:
            lo = math.log((1 - acc + 1e-9) / (acc + 1e-9))
        log_odds    += lo * min(abs(score) * 2, 1.0)   # scale by signal strength
        sources_used += 1

    # convert log-odds back to probability
    prob_bull = 1 / (1 + math.exp(-log_odds))
    prob_bull = round(prob_bull, 3)

    if prob_bull >= 0.60:
        direction  = "BULL"
        confidence = prob_bull
    elif prob_bull <= 0.40:
        direction  = "BEAR"
        confidence = 1 - prob_bull
    else:
        direction  = "MIXED"
        confidence = 0.5

    top_sources = sorted(source_acc.items(), key=lambda x: abs(x[1]-0.5), reverse=True)[:2]
    src_str = " ".join(f"{s}:{a:.0%}" for s, a in top_sources)

    signal_str = (f"{direction} {confidence:.0%} "
                  f"({sources_used} arts, {src_str})")
    return {
        "direction":    direction,
        "confidence":   confidence,
        "sources_used": sources_used,
        "source_acc":   source_acc,
        "signal_str":   signal_str,
    }

# ── [D] Extended RSS feeds & headline enrichment ──────────────────────────────

EXTENDED_RSS_FEEDS = [
    # Central banks / policy
    ("Fed Reserve",      "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("ECB",              "https://www.ecb.europa.eu/rss/press.html"),
    # Macro / global
    ("WSJ Markets",      "https://feeds.content.dowjones.io/public/rss/mw_marketpulse"),
    ("AP Business",      "https://rsshub.app/apnews/topics/business"),
    ("Guardian Business","https://www.theguardian.com/business/rss"),
    ("BBC Business",     "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("NPR Economy",      "https://feeds.npr.org/1017/rss.xml"),
    # Crypto / digital assets
    ("CoinDesk",         "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph",    "https://cointelegraph.com/rss"),
    # Commodities / energy
    ("OilPrice",         "https://oilprice.com/rss/main"),
    ("Mining.com",       "https://www.mining.com/feed/"),
    # Additional equity / earnings
    ("Motley Fool",      "https://www.fool.com/feeds/index.aspx"),
    ("Barron's",         "https://www.barrons.com/xml/rss/3_7510.xml"),
    ("Zacks",            "https://www.zacks.com/newsroom/rss_feeds/headlines.xml"),
]

# headline enrichment: extract numeric signals from text
_NUM_RE = re.compile(r'([+-]?\d+\.?\d*)\s*(%|bps|bp|billion|million|trillion)', re.I)

def extract_numeric_signals(text: str) -> dict:
    """
    Pull numeric values from headline/body text.
    Returns dict of {signal_type: value} for any found.
    Examples: "inflation rose 3.2%" -> {pct_change: 3.2}
              "cut rates by 25bps"  -> {bps_change: -25}
    """
    signals = {}
    text_l  = text.lower()
    for m in _NUM_RE.finditer(text):
        val  = float(m.group(1))
        unit = m.group(2).lower()
        if unit == "%":
            if any(w in text_l for w in ["fell","drop","declin","cut","reduc","lower"]):
                val = -abs(val)
            elif any(w in text_l for w in ["rose","rise","increas","gain","hike","higher"]):
                val = abs(val)
            signals["pct_change"] = round(val, 3)
        elif unit in ("bps", "bp"):
            if any(w in text_l for w in ["cut","lower","reduc","ease"]):
                val = -abs(val)
            signals["bps_change"] = round(val, 1)
        elif unit in ("billion","million","trillion"):
            mult = {"billion": 1e9, "million": 1e6, "trillion": 1e12}[unit]
            signals["abs_value"] = round(val * mult, 0)
    return signals

def enrich_headline_metadata(art: dict) -> dict:
    """Add numeric signals, source tier, and headline length category to article."""
    title = art.get("title", "") or ""
    art["numeric_signals"] = json.dumps(extract_numeric_signals(title))

    # source trust tier
    high_trust = {"Reuters Business","Reuters Markets","FT Markets","Bloomberg",
                  "WSJ Markets","AP Business","Fed Reserve","ECB","Barron's"}
    med_trust  = {"CNBC Top News","MarketWatch","BBC Business","Guardian Business",
                  "NPR Economy","The Economist"}
    src = art.get("source","")
    if src in high_trust:
        art["source_tier"] = "A"
    elif src in med_trust:
        art["source_tier"] = "B"
    else:
        art["source_tier"] = "C"

    # headline length category
    words = len(title.split())
    art["hl_words"] = words
    return art

# ── DataCache additions for analytics ─────────────────────────────────────────

# Module-level caches for slow analytics (updated by background thread)
_regime_cache: dict     = {}
_momentum_cache: dict   = {}   # key -> momentum dict
_leadlag_cache: dict    = {}   # ticker -> lead_lag dict
_bayes_cache: dict      = {}   # ticker -> bayesian signal dict

def run_analytics_refresh():
    """Background thread: regime, momentum, lead-lag, Bayesian — every 5 min."""
    log.info("run_analytics_refresh thread started")
    while True:
        try:
            global _regime_cache, _momentum_cache, _leadlag_cache, _bayes_cache

            # regime
            _regime_cache = detect_regime()

            # momentum for all themes + global
            keys = list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]
            for k in keys:
                _momentum_cache[k] = compute_sentiment_momentum(k)

            # lead-lag + bayesian for equity watchlist
            for ticker in WATCHLIST["Equities"] + WATCHLIST["Sectors"][:4]:
                _leadlag_cache[ticker] = compute_lead_lag(ticker)
                _bayes_cache[ticker]   = compute_bayesian_signal(ticker)

            CACHE.clear_error("analytics")
            log.debug("run_analytics_refresh OK")
        except Exception as e:
            CACHE.record_error("analytics", e)
        time.sleep(300)   # 5 minutes

# ══════════════════════════════════════════════════════════════════════════════
#  TEXTUAL TUI  v0.6
# ══════════════════════════════════════════════════════════════════════════════

class SentimentMonitorApp(App):
    CSS = """
    Screen          { background: #060606; }
    Header          { background: #000000; color: #ffcc00; text-style: bold; }
    Footer          { background: #0a0a0a; color: #444; }

    /* tabs */
    TabbedContent   { height: 1fr; }
    Tabs            { background: #0a0a0a; border-bottom: solid #ffcc00; height: 3; }
    Tab             { background: #111111; color: #666666; border: solid #2a2a2a;
                      padding: 0 2; height: 3; content-align: center middle; }
    Tab:hover       { background: #1a1a1a; color: #aaaaaa; }
    Tab.-active     { background: #1a1400; color: #ffcc00;
                      border: solid #ffcc00; text-style: bold; }
    TabPane         { padding: 0; }

    /* status bars */
    #status_bar     { height: 1; background: #0d0d0d; color: #556655;
                      padding: 0 2; border-bottom: solid #1a1a1a; }
    #status_bar2    { height: 1; background: #080808; color: #444444;
                      padding: 0 2; border-bottom: solid #111; }
    #fg_bar         { height: 1; background: #0a0a00; color: #888800;
                      padding: 0 2; text-style: bold; }
    #regime_bar     { height: 1; background: #050510; color: #5555aa;
                      padding: 0 2; }

    /* tables */
    DataTable       { height: 1fr; background: #080808; }
    DataTable > .datatable--header  { background: #141414; color: #ffcc00;
                                      text-style: bold; }
    DataTable > .datatable--cursor  { background: #1a2e1a; }
    DataTable > .datatable--hover   { background: #111111; }
    """

    BINDINGS = [
        ("r",   "refresh",              "Refresh"),
        ("q",   "quit",                 "Quit"),
        ("?",   "show_help",            "Help"),
        ("c",   "open_cfg",             "Config"),
        ("1",   "switch_tab('equities')",   "Equities"),
        ("2",   "switch_tab('macrofx')",    "Macro/FX"),
        ("3",   "switch_tab('sectors')",    "Sectors"),
        ("4",   "switch_tab('sentiment')",  "Sentiment"),
        ("5",   "switch_tab('macrodata')",  "FRED"),
        ("6",   "switch_tab('entities')",   "Entities"),
        ("7",   "switch_tab('newsfeed')",   "News"),
        ("8",   "switch_tab('analytics')",  "Analytics"),
        ("9",   "switch_tab('alerts')",     "Alerts"),
        ("0",   "switch_tab('errorlog')",   "Log"),
    ]

    TITLE     = "ESM  //  Economic Sentiment Monitor  v0.6"
    SUB_TITLE = "initialising…"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="status_bar")
        yield Static("", id="status_bar2")
        yield Static("", id="fg_bar")
        yield Static("", id="regime_bar")
        with TabbedContent(initial="equities"):
            with TabPane("1:EQUITIES",   id="equities"):
                yield DataTable(id="eq_table",       zebra_stripes=True)
            with TabPane("2:MACRO/FX",   id="macrofx"):
                yield DataTable(id="fx_table",       zebra_stripes=True)
            with TabPane("3:SECTORS",    id="sectors"):
                yield DataTable(id="sec_table",      zebra_stripes=True)
            with TabPane("4:SENTIMENT",  id="sentiment"):
                yield DataTable(id="sent_table",     zebra_stripes=True)
            with TabPane("5:FRED DATA",  id="macrodata"):
                yield DataTable(id="fred_table",     zebra_stripes=True)
            with TabPane("6:ENTITIES",   id="entities"):
                yield DataTable(id="ent_table",      zebra_stripes=True)
            with TabPane("7:NEWS FEED",  id="newsfeed"):
                yield DataTable(id="news_table",     zebra_stripes=True)
            with TabPane("8:ANALYTICS",  id="analytics"):
                yield DataTable(id="analytics_table",zebra_stripes=True)
            with TabPane("9:ALERTS",     id="alerts"):
                yield DataTable(id="alert_table",    zebra_stripes=True)
            with TabPane("0:LOG",        id="errorlog"):
                yield DataTable(id="log_table",      zebra_stripes=True)
        yield Footer()

    def on_mount(self):
        self._setup_tables()
        threading.Thread(target=run_refresh,             daemon=True).start()
        threading.Thread(target=run_correlation_refresh, daemon=True).start()
        threading.Thread(target=run_fred_refresh,        daemon=True).start()
        threading.Thread(target=run_analytics_refresh,   daemon=True).start()
        if FULL_TEXT_ENABLED:
            threading.Thread(target=fulltext_worker,     daemon=True).start()
        self.set_interval(10, self._ui_refresh)
        self.set_timer(3,     self._ui_refresh)

    def action_switch_tab(self, tab_id: str) -> None:
        self.query_one(TabbedContent).active = tab_id

    def _setup_tables(self):
        # 1: EQUITIES — full analytics columns
        self.query_one("#eq_table", DataTable).add_columns(
            "Ticker", "Price", "Chg%", "Vol",
            "Sent 1h", "Spark", "Sent 24h",
            "Corr", "BestLag", "Bayes Signal", "Diverg")
        # 2: MACRO/FX
        self.query_one("#fx_table", DataTable).add_columns(
            "Asset", "Price", "Chg%",
            "Sent 1h", "Spark", "Sent 24h", "Corr", "Lbl")
        # 3: SECTORS
        self.query_one("#sec_table", DataTable).add_columns(
            "ETF", "Sector", "Price", "Chg%",
            "Sent 1h", "Spark", "Heatmap", "Sent 24h",
            "Corr", "Velocity", "Bayes")
        # 4: SENTIMENT — momentum + acceleration
        self.query_one("#sent_table", DataTable).add_columns(
            "Theme", "1h", "4h", "24h",
            "Spark", "Trend", "Accel", "Consec", "Vel", "n(1h)", "Context")
        # 5: FRED DATA
        self.query_one("#fred_table", DataTable).add_columns(
            "Series", "Theme", "Latest", "Prior", "Surprise %", "As Of", "Signal")
        # 6: ENTITIES — spark + bayesian
        self.query_one("#ent_table", DataTable).add_columns(
            "Ticker", "Mentions", "Avg Sent", "Min", "Max", "Spark", "In WL")
        # 7: NEWS FEED — richer columns
        self.query_one("#news_table", DataTable).add_columns(
            "Time", "Src", "Tier", "Score", "FB", "VD",
            "Nums", "FT", "W", "Keywords", "Headline")
        # 8: ANALYTICS — regime + lead-lag + bayesian summary
        self.query_one("#analytics_table", DataTable).add_columns(
            "Ticker/Theme", "Type", "Value", "Detail", "Confidence")
        # 9: ALERTS
        self.query_one("#alert_table", DataTable).add_columns(
            "Time", "Sev", "Regime", "Key", "Reason", "Score")
        # 0: LOG
        self.query_one("#log_table", DataTable).add_columns(
            "Thread", "Status", "Last Error", "Log File")

    def _ui_refresh(self):
        spacy_s = "NER:✓" if SPACY_READY   else "NER:✗"
        fred_s  = f"FRED:{len(CACHE.fred_data)}s" if FRED_API_KEY else "FRED:✗"
        errs    = CACHE.err_counts
        total_e = sum(errs.values())
        err_s   = (f"⚠ ERR ingest:{errs.get('ingest',0)} ft:{errs.get('fulltext',0)}"
                   f" fred:{errs.get('fred',0)} corr:{errs.get('corr',0)}"
                   f" anlyt:{errs.get('analytics',0)}  │  "
                   if total_e else "")
        self.query_one("#status_bar", Static).update(
            f"{err_s}{tier_badge()}  {spacy_s}  {fulltext_badge()}  {fred_s}"
            f"  │  Refresh:{CACHE.last_refresh}"
            f"  Articles:{CACHE.article_count}"
            f"  Poll:{REFRESH_SEC}s"
        )
        self.query_one("#status_bar2", Static).update(
            f"  CFG:{CONFIG_PATH}"
            f"  │  Alert Z>={ALERT_ZSCORE_WARN:.1f}σ CRIT>={ALERT_ZSCORE_CRIT:.1f}σ"
            f"  │  Drift:{ALERT_DRIFT_N}w  │  LOG:{LOG_PATH}"
            f"  │  Feeds:{len(RSS_FEEDS)+len(EXTENDED_RSS_FEEDS)}"
        )
        fg_val, fg_label = fear_greed_index(CACHE.scores)
        bar_len = 28
        filled  = max(0, min(int((fg_val + 100) / 200 * bar_len), bar_len))
        fg_bar  = "█" * filled + "░" * (bar_len - filled)
        self.query_one("#fg_bar", Static).update(
            f"  FEAR/GREED  [{fg_bar}]  {fg_val:+d}  {fg_label}"
            f"  │  1=Eq 2=FX 3=Sec 4=Sent 5=FRED 6=Ent 7=News 8=Anlyt 9=Alerts 0=Log"
        )
        reg = _regime_cache
        if reg:
            sigs = "  ".join(reg.get("signals", [])[:4])
            self.query_one("#regime_bar", Static).update(
                f"  REGIME: {reg.get('label','—')}  "
                f"conf:{reg.get('confidence',0):.0%}  │  {sigs}"
            )

        CACHE.update_scores()
        CACHE.update_articles()
        CACHE.update_entity_summary()
        CACHE.update_queue_len()
        CACHE.update_fred_data()
        self._refresh_equities()
        self._refresh_macrofx()
        self._refresh_sectors()
        self._refresh_sentiment()
        self._refresh_fred()
        self._refresh_entities()
        self._refresh_news()
        self._refresh_analytics()
        self._refresh_alerts()
        self._refresh_log()

    def _price_row(self, ticker):
        p     = CACHE.prices.get(ticker, {})
        price = f"{p['price']:>10,.4f}" if p.get("price") else "          —"
        chg   = p.get("change_pct", 0.0)
        vol   = p.get("volume", 0)
        vol_s = (f"{vol/1e6:.1f}M" if vol > 1e6 else
                 f"{vol/1e3:.0f}K" if vol > 1e3 else "—")
        return price, chg, (fmt_chg(chg) if p else "—"), vol_s

    def _sent_for(self, key):
        d = CACHE.scores.get(key, {})
        return (d.get("1h",  {}).get("score"),
                d.get("4h",  {}).get("score"),
                d.get("24h", {}).get("score"))

    def _refresh_equities(self):
        t = self.query_one("#eq_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Equities"]:
            price, chg, chg_s, vol_s = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            corr  = CACHE.correlations.get(ticker)
            hist  = get_sparkline_history(ticker)
            spark = sparkline(hist)
            diverg= divergence_score(ticker, CACHE.scores, CACHE.prices)
            # lead-lag best
            ll    = _leadlag_cache.get(ticker, {})
            best_lag = ll.get("best_lag")
            best_r   = ll.get("best_r")
            lag_s = f"{best_lag}h r={best_r:.2f}" if best_lag and best_r else "n/a"
            # bayesian
            bay   = _bayes_cache.get(ticker, {})
            bay_s = bay.get("signal_str", "n/a")[:22]
            lbl   = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, price, chg_s, vol_s,
                      fmt_score(s1), spark, fmt_score(s24),
                      fmt_corr(corr), lag_s, bay_s, diverg)

    def _refresh_macrofx(self):
        t = self.query_one("#fx_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"]:
            price, chg, chg_s, _ = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            corr  = CACHE.correlations.get(ticker)
            hist  = get_sparkline_history(ticker)
            spark = sparkline(hist)
            lbl   = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, price, chg_s,
                      fmt_score(s1), spark, fmt_score(s24),
                      fmt_corr(corr), lbl)

    def _refresh_sectors(self):
        t = self.query_one("#sec_table", DataTable)
        t.clear()
        for ticker, name in SECTORS.items():
            price, chg, chg_s, _ = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            corr  = CACHE.correlations.get(ticker)
            hist  = get_sparkline_history(ticker)
            spark = sparkline(hist)
            vel   = velocity_label(ticker)
            bay   = _bayes_cache.get(ticker, {})
            bay_s = bay.get("signal_str", "—")[:18] if bay else "—"
            t.add_row(ticker, name, price, chg_s,
                      fmt_score(s1), spark, sentiment_bar(s1 or 0),
                      fmt_score(s24), fmt_corr(corr), vel, bay_s)

    def _refresh_sentiment(self):
        t = self.query_one("#sent_table", DataTable)
        t.clear()
        for theme in list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]:
            d   = CACHE.scores.get(theme, {})
            s1  = d.get("1h",  {}).get("score")
            s4  = d.get("4h",  {}).get("score")
            s24 = d.get("24h", {}).get("score")
            n1  = d.get("1h",  {}).get("count", 0)
            hist  = get_sparkline_history(theme)
            spark = sparkline(hist)
            vel   = velocity_label(theme)
            mom   = _momentum_cache.get(theme, {})
            accel = mom.get("acceleration")
            consec= mom.get("consecutive", 0)
            accel_s = f"{accel:+.3f}" if accel is not None else "—"
            trend = ("▲" if (s1 or 0) > (s4 or 0) + 0.01
                     else ("▼" if (s1 or 0) < (s4 or 0) - 0.01 else "→")
                     ) if s1 is not None and s4 is not None else "—"
            ctx   = ("Bullish" if (s1 or 0) > 0.1  else
                     "Bearish" if (s1 or 0) < -0.1 else
                     "Mixed"   if s1 is not None    else "—")
            t.add_row(theme.upper(), fmt_score(s1), fmt_score(s4),
                      fmt_score(s24), spark, trend, accel_s,
                      str(consec), vel, str(n1 or "—"), ctx)

    def _refresh_fred(self):
        t = self.query_one("#fred_table", DataTable)
        t.clear()
        if not CACHE.fred_data:
            t.add_row("—","—","No FRED data — add fred_api_key to config","—","—","—","—")
            return
        for row in CACHE.fred_data:
            val_s  = f"{row['value']:.4f}"      if row["value"]      is not None else "—"
            prev_s = f"{row['prev_value']:.4f}" if row["prev_value"] is not None else "—"
            surp   = row["surprise"] or 0.0
            surp_s = f"{surp:+.2f}%"
            signal = ("↑ boost" if surp >= 0.5 else "↓ dampen" if surp <= -0.5 else "—")
            t.add_row(row["label"], row["theme"].upper(),
                      val_s, prev_s, surp_s, (row["ts"] or "")[:10], signal)

    def _refresh_entities(self):
        t = self.query_one("#ent_table", DataTable)
        t.clear()
        all_tickers = {tk for tl in WATCHLIST.values() for tk in tl}
        for row in CACHE.entity_summary:
            hist  = get_sparkline_history(row["ticker"])
            spark = sparkline(hist, width=8)
            in_wl = "✓" if row["ticker"] in all_tickers else "—"
            t.add_row(row["ticker"], str(row["mentions"]),
                      fmt_score(row["avg_score"]),
                      fmt_score(row["min_score"]),
                      fmt_score(row["max_score"]),
                      spark, in_wl)

    def _refresh_news(self):
        t = self.query_one("#news_table", DataTable)
        t.clear()
        for art in CACHE.recent_articles[:100]:
            pub     = (art["published"] or "")[:16]
            tier    = "FB" if (art.get("model_tier") or "").startswith("fin") else "VD"
            src_t   = art.get("source_tier", "C")
            ft_s    = "✓" if art.get("full_text") else "·"
            words   = str(art.get("text_length") or "—")
            kws     = art.get("keywords", "")
            try:
                kw_list = json.loads(kws) if kws else []
                kw_s    = ",".join(kw_list[:3])
            except Exception:
                kw_s = ""
            # numeric signals
            try:
                nums = json.loads(art.get("numeric_signals", "{}") or "{}")
                num_s = " ".join(f"{k[:3]}:{v}" for k, v in list(nums.items())[:2])
            except Exception:
                num_s = ""
            title   = (art["title"] or "")[:72]
            t.add_row(pub, (art["source"] or "")[:12], src_t,
                      fmt_score(art["score"]),
                      fmt_score(art.get("finbert_score")),
                      fmt_score(art.get("vader_score")),
                      num_s[:12], ft_s, words, kw_s[:14], title)

    def _refresh_analytics(self):
        """8:ANALYTICS — consolidated regime, lead-lag, Bayesian, momentum."""
        t = self.query_one("#analytics_table", DataTable)
        t.clear()

        # Regime
        reg = _regime_cache
        if reg:
            t.add_row("MARKET", "Regime",
                      reg.get("label","—"),
                      "  ".join(reg.get("signals",[])[:5]),
                      f"{reg.get('confidence',0):.0%}")

        # Global momentum
        mom = _momentum_cache.get("GLOBAL", {})
        if mom.get("acceleration") is not None:
            t.add_row("GLOBAL", "Momentum",
                      f"{mom['direction']} {mom['consecutive']}x consec",
                      f"accel={mom['acceleration']:+.4f}",
                      "—")

        # Theme momentum rows
        for theme in MACRO_KEYWORDS.keys():
            mom = _momentum_cache.get(theme, {})
            if mom.get("acceleration") is not None:
                t.add_row(theme.upper(), "Momentum",
                          f"{mom.get('direction','—')} {mom.get('consecutive',0)}x",
                          f"accel={mom.get('acceleration',0):+.4f}",
                          "—")

        # Lead-lag rows
        for ticker, ll in _leadlag_cache.items():
            best_lag = ll.get("best_lag")
            best_r   = ll.get("best_r")
            if best_lag and best_r and abs(best_r) > 0.1:
                lag_detail = "  ".join(
                    f"{lh}h:{r:.2f}" for lh, r in sorted(ll.get("by_lag",{}).items())
                    if r is not None
                )
                t.add_row(ticker, "Lead-Lag",
                          f"best={best_lag}h r={best_r:.3f}",
                          lag_detail[:50],
                          f"|r|={abs(best_r):.2f}")

        # Bayesian signal rows
        for ticker, bay in _bayes_cache.items():
            if bay.get("sources_used", 0) > 0:
                top_srcs = "  ".join(
                    f"{s}:{a:.0%}"
                    for s, a in sorted(bay.get("source_acc",{}).items(),
                                       key=lambda x: abs(x[1]-0.5), reverse=True)[:3]
                )
                t.add_row(ticker, "Bayesian",
                          f"{bay.get('direction','—')} {bay.get('confidence',0):.0%}",
                          f"{bay.get('sources_used',0)} arts  {top_srcs[:40]}",
                          f"{bay.get('confidence',0):.0%}")

    def _refresh_alerts(self):
        t = self.query_one("#alert_table", DataTable)
        t.clear()
        reg_label = _regime_cache.get("label", "—") if _regime_cache else "—"
        seen = set()
        with get_db() as db:
            rows = db.execute(
                "SELECT ts,severity,key,reason,score FROM alerts "
                "ORDER BY ts DESC LIMIT 60"
            ).fetchall()
        for r in rows:
            key = r[2] + r[3]
            if key not in seen:
                seen.add(key)
                t.add_row(r[0][:19], r[1], reg_label, r[2], r[3], fmt_score(r[4]))

    def _refresh_log(self):
        t = self.query_one("#log_table", DataTable)
        t.clear()
        threads = [("ingest","Main ingest"), ("fulltext","Full-text"),
                   ("fred","FRED"), ("corr","Correlation"),
                   ("analytics","Analytics")]
        for key, label in threads:
            n    = CACHE.err_counts.get(key, 0)
            last = CACHE.last_errors.get(key, "") or "—"
            t.add_row(label, "✓ OK" if n == 0 else f"✗ {n} err",
                      last[:90], LOG_PATH)

    def action_refresh(self):
        self._ui_refresh()

    def action_open_cfg(self):
        self.notify(
            f"Config: {CONFIG_PATH}\n"
            "Edit with any text editor, restart to apply.\n"
            "Keys: watchlists, rss_feeds, refresh_sec,\n"
            "      alert_zscore_warn/crit, full_text_enabled",
            title="Config", timeout=10)

    def action_show_help(self):
        self.notify(
            "Keys: 1-9/0 = jump to tab  R = refresh  Q = quit  ? = help\n"
            "\n"
            "v0.6 analytics:\n"
            "  Regime: classifies market env (risk-on/off, high/low vol)\n"
            "  Momentum: acceleration + consecutive direction streak\n"
            "  Lead-Lag: ticker-specific optimal sentiment→price lag\n"
            "  Bayesian: source-weighted directional confidence score\n"
            "  Numeric: extracts % and bps changes from headlines\n"
            f"  Feeds: {len(RSS_FEEDS)+len(EXTENDED_RSS_FEEDS)} total RSS sources\n"
            "\n"
            "8:ANALYTICS tab = consolidated view of all predictive signals\n"
            "Regime bar shows current market environment continuously",
            title="ESM v0.6  Help", timeout=18)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 64)
    print("  ECONOMIC SENTIMENT MONITOR  v0.6")
    print("=" * 64)
    init_db()
    log.info("=" * 50)
    log.info("ESM v0.6 starting up")
    print(f"  DB           : {DB_PATH}")
    print(f"  Config       : {CONFIG_PATH}")
    print(f"  Log          : {LOG_PATH}")
    print(f"  Poll interval: {REFRESH_SEC}s")
    print(f"  NewsAPI      : {'SET' if NEWS_API_KEY else 'not set (RSS only)'}")
    print(f"  FRED         : {'SET - ' + str(len(FRED_SERIES)) + ' series' if FRED_API_KEY else 'not set'}")
    print(f"  FinBERT      : {'LOADED' if FINBERT_READY else 'not available - VADER only'}")
    print(f"  SpaCy NER    : {'LOADED' if SPACY_READY  else 'not available - regex fallback'}")
    print(f"  Full text    : {'ENABLED' if FULL_TEXT_ENABLED else 'disabled'}")
    print(f"  RSS feeds    : {len(RSS_FEEDS) + len(EXTENDED_RSS_FEEDS)} total")
    print(f"  Analytics    : regime + momentum + lead-lag + bayesian")
    print("-" * 64)
    if not FRED_API_KEY:
        print("  -> FRED key : https://fred.stlouisfed.org/docs/api/api_key.html")
    if not NEWSPAPER_READY:
        print("  -> Full text: pip install newspaper3k --break-system-packages")
    if not FINBERT_READY:
        print("  -> FinBERT  : pip install transformers torch --break-system-packages")
    if not SPACY_READY:
        print("  -> SpaCy    : pip install spacy --break-system-packages")
        print("                python3 -m spacy download en_core_web_sm --break-system-packages")
    print("=" * 64)
    print("  Starting...")
    log.info("startup complete - launching TUI v0.6")
    time.sleep(0.5)
    SentimentMonitorApp().run()
