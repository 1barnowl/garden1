#!/usr/bin/env python3
"""
Economic Sentiment Monitor  v0.2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Upgrades vs v0.1:
  • Two-tier NLP  — VADER fast path (always on) + FinBERT accurate tier (if available)
  • Entity linking — SpaCy NER maps headline mentions → canonical tickers
  • Market-context multiplier — negative headline + price-up = reduced weight
  • ENTITIES tab — per-ticker mention count + linked sentiment score
  • Model status bar shows which NLP tier is active
"""

import os, sys, time, threading, sqlite3, hashlib, json, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

# ══════════════════════════════════════════════════════════════════════════════
#  DEPENDENCY CHECK
# ══════════════════════════════════════════════════════════════════════════════
MISSING = []

try:
    from textual.app import App, ComposeResult
    from textual.widgets import (Header, Footer, TabbedContent, TabPane,
                                  DataTable, Label, Static)
    from textual.containers import Horizontal, Vertical
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

# ── optional: FinBERT (accurate tier) ────────────────────────────────────────
FINBERT_PIPE  = None
FINBERT_READY = False
try:
    from transformers import pipeline as hf_pipeline
    print("[NLP] Loading FinBERT… (first run downloads ~500 MB, cached after)")
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

# ── optional: SpaCy entity linker ────────────────────────────────────────────
NLP_SPACY   = None
SPACY_READY = False
try:
    import spacy
    for _model in ("en_core_web_sm", "en_core_web_md"):
        try:
            NLP_SPACY   = spacy.load(_model, disable=["parser", "lemmatizer"])
            SPACY_READY = True
            print(f"[NER] SpaCy model '{_model}' loaded ✓")
            break
        except OSError:
            continue
    if not SPACY_READY:
        print("[NER] No SpaCy model found. Run: python -m spacy download en_core_web_sm")
except ImportError:
    print("[NER] SpaCy not installed. Run: pip install spacy --break-system-packages")

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DB_PATH      = os.path.expanduser("~/.esm/sentiment.db")
REFRESH_SEC  = 90

WATCHLIST = {
    "Equities": ["SPY","QQQ","AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOG","JPM","BAC","XOM"],
    "Macro/FX": ["GC=F","SI=F","CL=F","BTC-USD","ETH-USD","EURUSD=X","GBPUSD=X","JPY=X","DX-Y.NYB"],
    "Bonds":    ["^TNX","^TYX","^FVX","TLT","HYG","LQD"],
    "Sectors":  ["XLK","XLF","XLE","XLV","XLI","XLC","XLB","XLU","XLRE","XLP","XLY"],
}

SECTORS = {
    "XLK":"Technology","XLF":"Financials","XLE":"Energy","XLV":"Health Care",
    "XLI":"Industrials","XLC":"Comm Svc","XLB":"Materials","XLU":"Utilities",
    "XLRE":"Real Estate","XLP":"Cons Staples","XLY":"Cons Discr",
}

RSS_FEEDS = [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",  "https://feeds.reuters.com/reuters/financialNews"),
    ("CNBC Top News",    "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch",      "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Seeking Alpha",    "https://seekingalpha.com/feed.xml"),
    ("Investopedia",     "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline"),
    ("Yahoo Finance",    "https://finance.yahoo.com/news/rssindex"),
    ("FT (Markets)",     "https://www.ft.com/markets?format=rss"),
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

# ── Entity → Ticker mapping ───────────────────────────────────────────────────
ENTITY_MAP: dict = {
    # Big tech
    "apple": "AAPL",  "apple inc": "AAPL",
    "microsoft": "MSFT", "microsoft corp": "MSFT",
    "nvidia": "NVDA", "nvidia corp": "NVDA",
    "tesla": "TSLA",  "tesla inc": "TSLA",
    "amazon": "AMZN", "amazon.com": "AMZN",
    "meta": "META",   "meta platforms": "META", "facebook": "META",
    "alphabet": "GOOG","google": "GOOG",
    "netflix": "NFLX",
    "openai": "MSFT",  # proxy
    # Finance
    "jpmorgan": "JPM", "jp morgan": "JPM", "jpmorgan chase": "JPM",
    "bank of america": "BAC", "bofa": "BAC",
    "goldman sachs": "GS", "goldman": "GS",
    "morgan stanley": "MS",
    "citigroup": "C",  "citi": "C",
    "wells fargo": "WFC",
    "blackrock": "BLK",
    "berkshire": "BRK-B", "berkshire hathaway": "BRK-B",
    # Energy
    "exxon": "XOM", "exxonmobil": "XOM", "exxon mobil": "XOM",
    "chevron": "CVX",
    "bp": "BP",
    "shell": "SHEL",
    "conocophillips": "COP",
    # Macro actors → macro theme keys
    "federal reserve": "rates", "the fed": "rates", "fed": "rates", "fomc": "rates",
    "jerome powell": "rates", "powell": "rates",
    "janet yellen": "rates",  "yellen": "rates",
    "ecb": "rates", "european central bank": "rates",
    "bank of england": "rates", "boe": "rates",
    "bank of japan": "rates",   "boj": "rates",
    "imf": "growth", "world bank": "growth",
    "opec": "CL=F",
    # Indices
    "s&p 500": "SPY", "s&p500": "SPY", "s&p": "SPY", "sp500": "SPY",
    "nasdaq": "QQQ",
    "dow jones": "DIA", "dow": "DIA",
    "russell 2000": "IWM",
    # Commodities / crypto
    "gold": "GC=F",
    "silver": "SI=F",
    "oil": "CL=F", "crude oil": "CL=F", "brent": "CL=F", "wti": "CL=F",
    "natural gas": "NG=F",
    "bitcoin": "BTC-USD", "btc": "BTC-USD",
    "ethereum": "ETH-USD", "eth": "ETH-USD",
    # Sectors
    "semiconductor": "XLK", "semiconductors": "XLK", "chip": "XLK", "chips": "XLK",
    "bank": "XLF", "banks": "XLF", "financial": "XLF", "financials": "XLF",
    "healthcare": "XLV", "health care": "XLV", "pharma": "XLV", "biotech": "XLV",
    "energy sector": "XLE",
    "retail": "XLY", "consumer discretionary": "XLY",
    "utility": "XLU", "utilities": "XLU",
    "real estate": "XLRE", "reit": "XLRE",
    "materials": "XLB",
    "industrials": "XLI",
    # FX / bonds
    "treasury": "^TNX", "treasuries": "^TNX", "10-year": "^TNX",
    "t-bond": "^TNX",   "10y yield": "^TNX", "10yr yield": "^TNX",
    "high yield": "HYG","junk bond": "HYG",  "junk bonds": "HYG",
    "dollar": "DX-Y.NYB", "usd": "DX-Y.NYB", "us dollar": "DX-Y.NYB",
    "euro": "EURUSD=X",   "eur": "EURUSD=X",  "eurozone": "EURUSD=X",
    "yen": "JPY=X",       "japanese yen": "JPY=X",
    "pound": "GBPUSD=X",  "sterling": "GBPUSD=X", "gbp": "GBPUSD=X",
}

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id            TEXT PRIMARY KEY,
            title         TEXT,
            source        TEXT,
            url           TEXT,
            published     TEXT,
            ingested      TEXT,
            vader_score   REAL,
            finbert_score REAL,
            score         REAL,
            label         TEXT,
            confidence    REAL,
            model_tier    TEXT,
            keywords      TEXT,
            entities      TEXT
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
        """)
        # safe migrations for users upgrading from v0.1
        for col, typ in [("vader_score","REAL"),("finbert_score","REAL"),
                          ("model_tier","TEXT"),("entities","TEXT")]:
            try:
                db.execute(f"ALTER TABLE articles ADD COLUMN {col} {typ}")
            except Exception:
                pass

# ══════════════════════════════════════════════════════════════════════════════
#  NLP — TWO-TIER SCORING
# ══════════════════════════════════════════════════════════════════════════════

def _vader_score(text: str) -> dict:
    if not VADER or not text:
        return {"score": 0.0, "label": "NEU", "confidence": 0.0}
    s   = VADER.polarity_scores(text)
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
                "pos": round(pos,4), "neg": round(neg,4), "neu": round(neu,4)}
    except Exception:
        return None

def score_text(text: str) -> dict:
    """
    Two-tier:
      always  → VADER  (fast, <1 ms)
      if avail → FinBERT (accurate, ~50-200 ms CPU)
    Blend: 0.3*vader + 0.7*finbert when both present.
    """
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
    """
    Returns list of {entity_raw, ticker}.
    Layer 1: SpaCy NER over ORG/GPE/PERSON/PRODUCT spans.
    Layer 2: simple string-scan of ENTITY_MAP keys (always runs).
    """
    found = {}   # ticker → entity_raw
    if SPACY_READY and NLP_SPACY:
        doc = NLP_SPACY(text[:300])
        for ent in doc.ents:
            if ent.label_ in ("ORG","GPE","PERSON","PRODUCT","MONEY","NORP"):
                key    = _norm(ent.text)
                ticker = ENTITY_MAP.get(key)
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
    """
    If sentiment and price move AGREE   → amplify  (cap +15%).
    If sentiment and price move DISAGREE → dampen  (floor -30%).
    Neutral zone: no change.
    """
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
#  INGESTION
# ══════════════════════════════════════════════════════════════════════════════

def _build_article(title: str, link: str, pub: str, source: str, prices: dict) -> dict:
    sent     = score_text(title)
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
    }

def fetch_rss(prices: dict) -> list:
    items = []
    for source, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:15]:
                title = getattr(entry,"title","")
                link  = getattr(entry,"link","")
                pub   = getattr(entry,"published",
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
            params={"q": query,"language":"en","sortBy":"publishedAt",
                    "pageSize":50,"apiKey":NEWS_API_KEY},
            timeout=10,
        )
        r.raise_for_status()
        items = []
        for art in r.json().get("articles",[]):
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
        db.executemany("""
            INSERT OR IGNORE INTO articles
            (id,title,source,url,published,ingested,vader_score,finbert_score,
             score,label,confidence,model_tier,keywords,entities)
            VALUES
            (:id,:title,:source,:url,:published,:ingested,:vader_score,:finbert_score,
             :score,:label,:confidence,:model_tier,:keywords,:entities)
        """, items)
        mention_rows = []
        ts = datetime.now(timezone.utc).isoformat()
        for art in items:
            try:
                for e in json.loads(art.get("entities") or "[]"):
                    mention_rows.append({
                        "article_id": art["id"], "entity_raw": e["entity_raw"],
                        "ticker": e["ticker"], "score": art["score"], "ts": ts,
                    })
            except Exception:
                pass
        if mention_rows:
            db.executemany("""
                INSERT INTO entity_mentions (article_id,entity_raw,ticker,score,ts)
                VALUES (:article_id,:entity_raw,:ticker,:score,:ts)
            """, mention_rows)

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
            # per-ticker from entity mentions
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
            if prev is None or count < 3:
                continue
            delta = score - prev
            if abs(delta) >= 0.15:
                severity  = "CRIT" if abs(delta) >= 0.25 else "WARN"
                direction = "SURGE ▲" if delta > 0 else "DROP  ▼"
                reason    = f"Sentiment {direction} Δ{abs(delta):.3f} vs 4h baseline (n={count})"
                alert_id  = hashlib.md5(
                    f"{key}{score}{datetime.now(timezone.utc).date()}".encode()
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
        self._lock           = threading.Lock()
        self.prices: dict    = {}
        self.scores: dict    = {}
        self.recent_articles = []
        self.entity_summary  = []
        self.alerts: deque   = deque(maxlen=50)
        self.last_refresh    = "—"
        self.article_count   = 0
        self.model_tier      = "vader"

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
                           model_tier,vader_score,finbert_score
                    FROM articles ORDER BY ingested DESC LIMIT 100
                """).fetchall()
                self.recent_articles = [
                    {"title": r[0], "source": r[1], "score": r[2], "label": r[3],
                     "published": r[4], "keywords": r[5], "model_tier": r[6],
                     "vader_score": r[7], "finbert_score": r[8]}
                    for r in rows
                ]
                self.article_count = db.execute(
                    "SELECT COUNT(*) FROM articles"
                ).fetchone()[0]
                if self.recent_articles:
                    self.model_tier = (self.recent_articles[0].get("model_tier")
                                       or "vader")

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
                    GROUP BY ticker
                    ORDER BY mentions DESC LIMIT 40
                """, (cutoff,)).fetchall()
                self.entity_summary = [
                    {"ticker": r[0], "mentions": r[1],
                     "avg_score": round(r[2],4) if r[2] else 0,
                     "min_score": round(r[3],4) if r[3] else 0,
                     "max_score": round(r[4],4) if r[4] else 0}
                    for r in rows
                ]

    def add_alerts(self, new_alerts):
        with self._lock:
            for a in new_alerts:
                self.alerts.appendleft(a)

    def set_last_refresh(self):
        with self._lock:
            self.last_refresh = datetime.now().strftime("%H:%M:%S")

CACHE = DataCache()

# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND REFRESH
# ══════════════════════════════════════════════════════════════════════════════

def run_refresh():
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
            CACHE.add_alerts(new_alerts)
            CACHE.set_last_refresh()
        except Exception:
            pass
        time.sleep(REFRESH_SEC)

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
    return "[FinBERT+VADER]" if FINBERT_READY else "[VADER only]"

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
    #status_bar       { height: 1; background: #111; color: #666;
                        padding: 0 2; content-align: left middle; }
    """

    BINDINGS = [
        ("r", "refresh",   "Refresh"),
        ("q", "quit",      "Quit"),
        ("?", "show_help", "Help"),
    ]

    TITLE     = "ESM  //  Economic Sentiment Monitor  v0.2"
    SUB_TITLE = "initialising…"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="status_bar")
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
        threading.Thread(target=run_refresh, daemon=True).start()
        self.set_interval(10, self._ui_refresh)
        self.set_timer(3, self._ui_refresh)

    def _setup_tables(self):
        self.query_one("#eq_table",    DataTable).add_columns(
            "Ticker","Price","Chg%","Sent 1h","▓ Bar ▓","Sent 24h","Tier","Label")
        self.query_one("#fx_table",    DataTable).add_columns(
            "Asset","Price","Chg%","Sent 1h","▓ Bar ▓","Sent 24h","Label")
        self.query_one("#sec_table",   DataTable).add_columns(
            "ETF","Sector","Price","Chg%","Sent 1h","▓ Heatmap ▓","Sent 24h")
        self.query_one("#sent_table",  DataTable).add_columns(
            "Theme","1h Score","4h Score","24h Score","Trend","n(1h)","Context")
        self.query_one("#ent_table",   DataTable).add_columns(
            "Ticker","Mentions 24h","Avg Sent","Min","Max","▓ Range ▓","In WL")
        self.query_one("#news_table",  DataTable).add_columns(
            "Time","Source","Score","FinBERT","VADER","Tier","Headline")
        self.query_one("#alert_table", DataTable).add_columns(
            "Time","Sev","Key","Reason","Score")

    def _ui_refresh(self):
        spacy_s = "SpaCy ✓" if SPACY_READY else "SpaCy ✗"
        self.query_one("#status_bar", Static).update(
            f"  NLP: {tier_badge()}  |  NER: {spacy_s}  |  "
            f"Refresh: {CACHE.last_refresh}  |  "
            f"Articles: {CACHE.article_count}  |  "
            f"Poll interval: {REFRESH_SEC}s"
        )
        CACHE.update_scores()
        CACHE.update_articles()
        CACHE.update_entity_summary()
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
            lbl = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, price, chg_s, fmt_score(s1),
                      sentiment_bar(s1 or 0), fmt_score(s24), tier_s, lbl)

    def _refresh_macrofx(self):
        t = self.query_one("#fx_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"]:
            price, chg, chg_s = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            lbl = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, price, chg_s, fmt_score(s1),
                      sentiment_bar(s1 or 0), fmt_score(s24), lbl)

    def _refresh_sectors(self):
        t = self.query_one("#sec_table", DataTable)
        t.clear()
        for ticker, name in SECTORS.items():
            price, chg, chg_s = self._price_row(ticker)
            s1, _, s24 = self._sent_for(ticker)
            t.add_row(ticker, name, price, chg_s,
                      fmt_score(s1), sentiment_bar(s1 or 0), fmt_score(s24))

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
            rng = round((row["max_score"] - row["min_score"]) * 10)
            bar = "▓" * min(max(rng, 0), 10)
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
            pub   = (art["published"] or "")[:16]
            tier  = "FB" if (art.get("model_tier") or "").startswith("fin") else "VD"
            title = (art["title"] or "")[:82]
            t.add_row(pub, (art["source"] or "")[:16],
                      fmt_score(art["score"]),
                      fmt_score(art.get("finbert_score")),
                      fmt_score(art.get("vader_score")),
                      tier, title)

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

    def action_show_help(self):
        self.notify(
            "Tabs: Equities · Macro/FX · Sectors · Sentiment · Entities · News · Alerts\n"
            "R = Force refresh   Q = Quit   ? = This help\n"
            "\n"
            "NLP tiers:\n"
            "  VD  = VADER (always on, <1ms per headline)\n"
            "  FB  = FinBERT (financial-domain transformer)\n"
            "  FB+VD = 30% VADER + 70% FinBERT blended score\n"
            "\n"
            "ENTITIES tab: SpaCy NER → headline → ticker → linked sentiment",
            title="ESM v0.2  Help",
            timeout=12,
        )

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 62)
    print("  ECONOMIC SENTIMENT MONITOR  v0.2")
    print("=" * 62)
    init_db()
    print(f"  DB         : {DB_PATH}")
    print(f"  NewsAPI    : {'SET' if NEWS_API_KEY else 'not set (RSS only)'}")
    print(f"  FRED       : {'SET' if FRED_API_KEY else 'not set'}")
    print(f"  FinBERT    : {'LOADED ✓' if FINBERT_READY else 'not available — VADER only'}")
    print(f"  SpaCy NER  : {'LOADED ✓' if SPACY_READY  else 'not available — regex fallback'}")
    print("-" * 62)
    if not FINBERT_READY:
        print("  → FinBERT : pip install transformers torch --break-system-packages")
    if not SPACY_READY:
        print("  → SpaCy   : pip install spacy --break-system-packages")
        print("              python -m spacy download en_core_web_sm")
    print("=" * 62)
    print("  Starting background ingestion (first fetch ~30-60s)…")
    time.sleep(0.8)
    SentimentMonitorApp().run()
