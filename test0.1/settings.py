"""
config/settings.py
All runtime configuration, watchlists, feed lists, and constants.
Loaded once at startup; all other modules import from here.
"""

import os
from logging.handlers import RotatingFileHandler as _RFH
import logging, sys

# ── paths ─────────────────────────────────────────────────────────────────────
ESM_DIR     = os.path.expanduser("~/.esm")
DB_PATH     = os.path.join(ESM_DIR, "sentiment.db")
CONFIG_PATH = os.path.join(ESM_DIR, "config.yml")
LOG_PATH    = os.path.join(ESM_DIR, "esm.log")

os.makedirs(ESM_DIR, exist_ok=True)

# ── logger ────────────────────────────────────────────────────────────────────
_handler = _RFH(LOG_PATH, maxBytes=5_000_000, backupCount=2)
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log = logging.getLogger("esm")
log.setLevel(logging.DEBUG)
if not log.handlers:
    log.addHandler(_handler)
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setLevel(logging.WARNING)
    _sh.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    log.addHandler(_sh)

# ── default config file ───────────────────────────────────────────────────────
DEFAULT_CONFIG = """\
# Economic Sentiment Monitor — runtime config v0.7
# Edit freely; changes take effect on next launch.

news_api_key: ""
fred_api_key: ""

# Poll intervals (seconds)
refresh_sec: 30
price_refresh_sec: 15

full_text_enabled: true
alert_warn_delta: 0.15
alert_crit_delta: 0.25
alert_min_count: 3
alert_zscore_warn: 2.0
alert_zscore_crit: 3.0
alert_drift_consecutive: 3

# News feed: max articles per source per cycle
rss_articles_per_feed: 25
newsapi_page_size: 100

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
  - AMD
  - INTC
  - CRM
  - NFLX
  - DIS
  - GS
  - WFC
  - V

watchlist_macrofx:
  - GC=F
  - SI=F
  - CL=F
  - NG=F
  - BTC-USD
  - ETH-USD
  - EURUSD=X
  - GBPUSD=X
  - JPY=X
  - DX-Y.NYB
  - AUDUSD=X
  - CADUSD=X

watchlist_bonds:
  - ^TNX
  - ^TYX
  - ^FVX
  - ^IRX
  - TLT
  - HYG
  - LQD
  - AGG
  - BND
  - TIP

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

fred_series:
  - ["CPI YoY",      "CPIAUCSL",        "inflation"]
  - ["Core CPI",     "CPILFESL",        "inflation"]
  - ["PCE",          "PCEPI",           "inflation"]
  - ["Fed Funds",    "FEDFUNDS",        "rates"]
  - ["Unemployment", "UNRATE",          "employment"]
  - ["Nonfarm Pay",  "PAYEMS",          "employment"]
  - ["GDP Growth",   "A191RL1Q225SBEA", "growth"]
  - ["10Y Yield",    "DGS10",           "rates"]
  - ["2Y Yield",     "DGS2",            "rates"]
  - ["M2 Money",     "M2SL",            "inflation"]
  - ["Credit Spread","BAMLH0A0HYM2",    "credit"]
  - ["VIX",          "VIXCLS",          "growth"]
  - ["Retail Sales", "RSAFS",           "growth"]
  - ["Indust Prod",  "INDPRO",          "growth"]

rss_feeds:
  - ["Reuters Business",  "https://feeds.reuters.com/reuters/businessNews"]
  - ["Reuters Markets",   "https://feeds.reuters.com/reuters/financialNews"]
  - ["CNBC Top News",     "https://www.cnbc.com/id/100003114/device/rss/rss.html"]
  - ["MarketWatch",       "https://feeds.marketwatch.com/marketwatch/topstories/"]
  - ["Seeking Alpha",     "https://seekingalpha.com/feed.xml"]
  - ["Yahoo Finance",     "https://finance.yahoo.com/news/rssindex"]
  - ["FT Markets",        "https://www.ft.com/markets?format=rss"]
  - ["Bloomberg",         "https://feeds.bloomberg.com/markets/news.rss"]
  - ["The Economist",     "https://www.economist.com/latest/rss.xml"]
  - ["Investopedia",      "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline"]
  - ["WSJ Markets",       "https://feeds.content.dowjones.io/public/rss/mw_marketpulse"]
  - ["AP Business",       "https://rsshub.app/apnews/topics/business"]
  - ["Guardian Business", "https://www.theguardian.com/business/rss"]
  - ["BBC Business",      "https://feeds.bbci.co.uk/news/business/rss.xml"]
  - ["NPR Economy",       "https://feeds.npr.org/1017/rss.xml"]
  - ["CoinDesk",          "https://www.coindesk.com/arc/outboundfeeds/rss/"]
  - ["Cointelegraph",     "https://cointelegraph.com/rss"]
  - ["OilPrice",          "https://oilprice.com/rss/main"]
  - ["Mining.com",        "https://www.mining.com/feed/"]
  - ["Barrons",           "https://www.barrons.com/xml/rss/3_7510.xml"]
  - ["Zacks",             "https://www.zacks.com/newsroom/rss_feeds/headlines.xml"]
  - ["Fed Reserve",       "https://www.federalreserve.gov/feeds/press_all.xml"]
  - ["ECB",               "https://www.ecb.europa.eu/rss/press.html"]
  - ["Motley Fool",       "https://www.fool.com/feeds/index.aspx"]
"""

def _load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w") as f:
            f.write(DEFAULT_CONFIG)
        log.info("Created default config at %s", CONFIG_PATH)
    cfg = {}
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f) or {}
    except ImportError:
        with open(CONFIG_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or ":" not in line:
                    continue
                k, _, v = line.partition(":")
                v = v.strip().strip('"').strip("'")
                if v in ("true", "false"):
                    cfg[k.strip()] = v == "true"
                elif v.replace(".", "").replace("-", "").isdigit():
                    cfg[k.strip()] = float(v) if "." in v else int(v)
                elif v:
                    cfg[k.strip()] = v
    return cfg

CFG = _load_config()

# ── resolved scalars ──────────────────────────────────────────────────────────
NEWS_API_KEY        = CFG.get("news_api_key", "") or os.environ.get("NEWS_API_KEY", "")
FRED_API_KEY        = CFG.get("fred_api_key", "") or os.environ.get("FRED_API_KEY", "")
REFRESH_SEC         = max(int(CFG.get("refresh_sec", 30)), 10)
PRICE_REFRESH_SEC   = max(int(CFG.get("price_refresh_sec", 15)), 5)
FULL_TEXT_ENABLED   = bool(CFG.get("full_text_enabled", True))
ALERT_WARN          = float(CFG.get("alert_warn_delta", 0.15))
ALERT_CRIT          = float(CFG.get("alert_crit_delta", 0.25))
ALERT_MIN_COUNT     = int(CFG.get("alert_min_count", 3))
ALERT_ZSCORE_WARN   = float(CFG.get("alert_zscore_warn", 2.0))
ALERT_ZSCORE_CRIT   = float(CFG.get("alert_zscore_crit", 3.0))
ALERT_DRIFT_N       = int(CFG.get("alert_drift_consecutive", 3))
RSS_PER_FEED        = int(CFG.get("rss_articles_per_feed", 25))
NEWSAPI_PAGE_SIZE   = int(CFG.get("newsapi_page_size", 100))

def _tickers(key, default):
    v = CFG.get(key)
    return [str(x) for x in v if x] if isinstance(v, list) else default

WATCHLIST = {
    "Equities": _tickers("watchlist_equities",
        ["SPY","QQQ","AAPL","MSFT","NVDA","TSLA","AMZN","META",
         "GOOG","JPM","BAC","XOM","AMD","INTC","GS","NFLX","V","WFC"]),
    "Macro/FX": _tickers("watchlist_macrofx",
        ["GC=F","SI=F","CL=F","NG=F","BTC-USD","ETH-USD",
         "EURUSD=X","GBPUSD=X","JPY=X","DX-Y.NYB","AUDUSD=X","CADUSD=X"]),
    "Bonds":    _tickers("watchlist_bonds",
        ["^TNX","^TYX","^FVX","^IRX","TLT","HYG","LQD","AGG","BND","TIP"]),
    "Sectors":  _tickers("watchlist_sectors",
        ["XLK","XLF","XLE","XLV","XLI","XLC","XLB","XLU","XLRE","XLP","XLY"]),
}

SECTORS = {
    "XLK": "Technology",   "XLF": "Financials",  "XLE": "Energy",
    "XLV": "Health Care",  "XLI": "Industrials", "XLC": "Comm Svc",
    "XLB": "Materials",    "XLU": "Utilities",   "XLRE": "Real Estate",
    "XLP": "Cons Staples", "XLY": "Cons Discr",
}

def _parse_feeds(cfg_feeds):
    if not isinstance(cfg_feeds, list):
        return []
    out = []
    for item in cfg_feeds:
        if isinstance(item, list) and len(item) == 2:
            out.append(tuple(item))
    return out

_raw = CFG.get("rss_feeds", [])
RSS_FEEDS = _parse_feeds(_raw) if _raw else [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",  "https://feeds.reuters.com/reuters/financialNews"),
    ("CNBC Top News",    "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch",      "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Yahoo Finance",    "https://finance.yahoo.com/news/rssindex"),
    ("Bloomberg",        "https://feeds.bloomberg.com/markets/news.rss"),
    ("FT Markets",       "https://www.ft.com/markets?format=rss"),
    ("The Economist",    "https://www.economist.com/latest/rss.xml"),
    ("CoinDesk",         "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("OilPrice",         "https://oilprice.com/rss/main"),
    ("BBC Business",     "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("AP Business",      "https://rsshub.app/apnews/topics/business"),
    ("Fed Reserve",      "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("Barrons",          "https://www.barrons.com/xml/rss/3_7510.xml"),
    ("Zacks",            "https://www.zacks.com/newsroom/rss_feeds/headlines.xml"),
    ("Seeking Alpha",    "https://seekingalpha.com/feed.xml"),
    ("Investopedia",     "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline"),
    ("Guardian Business","https://www.theguardian.com/business/rss"),
    ("NPR Economy",      "https://feeds.npr.org/1017/rss.xml"),
    ("Cointelegraph",    "https://cointelegraph.com/rss"),
    ("Mining.com",       "https://www.mining.com/feed/"),
    ("Motley Fool",      "https://www.fool.com/feeds/index.aspx"),
    ("Zacks",            "https://www.zacks.com/newsroom/rss_feeds/headlines.xml"),
    ("ECB",              "https://www.ecb.europa.eu/rss/press.html"),
]

def _fred_series(cfg_list):
    if not isinstance(cfg_list, list):
        return []
    return [tuple(str(x) for x in item) for item in cfg_list
            if isinstance(item, list) and len(item) == 3]

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
    ("VIX",          "VIXCLS",          "growth"),
    ("Retail Sales", "RSAFS",           "growth"),
    ("Indust Prod",  "INDPRO",          "growth"),
]

MACRO_KEYWORDS = {
    "inflation":  ["inflation","cpi","pce","price index","consumer prices","core prices","stagflation"],
    "rates":      ["interest rate","fed funds","federal reserve","fomc","rate hike","rate cut",
                   "powell","basis points","tightening","easing","pivot","yield curve"],
    "growth":     ["gdp","economic growth","recession","contraction","expansion","output gap",
                   "slowdown","recovery","gdp growth"],
    "employment": ["jobs","unemployment","payroll","nonfarm","labor market","jobless claims",
                   "hiring","layoffs","wage growth","job market","workforce"],
    "trade":      ["tariff","trade war","exports","imports","current account","trade deficit",
                   "sanctions","supply chain","protectionism","trade deal"],
    "credit":     ["credit","default","spread","cds","debt","bond yield","high yield",
                   "investment grade","downgrade","upgrade","rating","leverage"],
}

ENTITY_MAP = {
    # Big tech
    "apple":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","microsoft corp":"MSFT",
    "nvidia":"NVDA","nvidia corp":"NVDA",
    "tesla":"TSLA","tesla inc":"TSLA",
    "amazon":"AMZN","amazon.com":"AMZN",
    "meta":"META","meta platforms":"META","facebook":"META",
    "alphabet":"GOOG","google":"GOOG",
    "netflix":"NFLX","openai":"MSFT",
    "amd":"AMD","advanced micro devices":"AMD",
    "intel":"INTC","intel corp":"INTC",
    "salesforce":"CRM","disney":"DIS",
    # Finance
    "jpmorgan":"JPM","jp morgan":"JPM","jpmorgan chase":"JPM",
    "bank of america":"BAC","bofa":"BAC",
    "goldman sachs":"GS","goldman":"GS",
    "morgan stanley":"MS","citigroup":"C","citi":"C",
    "wells fargo":"WFC","blackrock":"BLK","visa":"V",
    "berkshire":"BRK-B","berkshire hathaway":"BRK-B",
    # Energy
    "exxon":"XOM","exxonmobil":"XOM","exxon mobil":"XOM",
    "chevron":"CVX","bp":"BP","shell":"SHEL","conocophillips":"COP",
    # Macro actors
    "federal reserve":"rates","the fed":"rates","fed":"rates","fomc":"rates",
    "jerome powell":"rates","powell":"rates","janet yellen":"rates","yellen":"rates",
    "ecb":"rates","european central bank":"rates",
    "bank of england":"rates","boe":"rates",
    "bank of japan":"rates","boj":"rates",
    "imf":"growth","world bank":"growth","opec":"CL=F",
    # Indices
    "s&p 500":"SPY","s&p500":"SPY","s&p":"SPY","sp500":"SPY",
    "nasdaq":"QQQ","dow jones":"DIA","dow":"DIA","russell 2000":"IWM",
    # Commodities / crypto
    "gold":"GC=F","silver":"SI=F",
    "oil":"CL=F","crude oil":"CL=F","brent":"CL=F","wti":"CL=F",
    "natural gas":"NG=F",
    "bitcoin":"BTC-USD","btc":"BTC-USD",
    "ethereum":"ETH-USD","eth":"ETH-USD",
    # Sectors
    "semiconductor":"XLK","semiconductors":"XLK","chip":"XLK","chips":"XLK",
    "bank":"XLF","banks":"XLF","financial":"XLF","financials":"XLF",
    "healthcare":"XLV","health care":"XLV","pharma":"XLV","biotech":"XLV",
    "energy sector":"XLE","retail":"XLY","consumer discretionary":"XLY",
    "utility":"XLU","utilities":"XLU","real estate":"XLRE","reit":"XLRE",
    "materials":"XLB","industrials":"XLI",
    # Bonds / FX
    "treasury":"^TNX","treasuries":"^TNX","10-year":"^TNX","t-bond":"^TNX",
    "10y yield":"^TNX","10yr yield":"^TNX","2y yield":"^FVX",
    "high yield":"HYG","junk bond":"HYG","junk bonds":"HYG",
    "dollar":"DX-Y.NYB","usd":"DX-Y.NYB","us dollar":"DX-Y.NYB",
    "euro":"EURUSD=X","eur":"EURUSD=X","eurozone":"EURUSD=X",
    "yen":"JPY=X","japanese yen":"JPY=X",
    "pound":"GBPUSD=X","sterling":"GBPUSD=X","gbp":"GBPUSD=X",
    "aussie dollar":"AUDUSD=X","aud":"AUDUSD=X",
    "canadian dollar":"CADUSD=X","cad":"CADUSD=X",
}

# Source trust tiers
HIGH_TRUST_SOURCES = {
    "Reuters Business","Reuters Markets","FT Markets","Bloomberg",
    "WSJ Markets","AP Business","Fed Reserve","ECB","Barrons",
}
MED_TRUST_SOURCES = {
    "CNBC Top News","MarketWatch","BBC Business","Guardian Business",
    "NPR Economy","The Economist","Seeking Alpha",
}
