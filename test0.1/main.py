#!/usr/bin/env python3
"""
Economic Sentiment Monitor  v0.7
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Entry point. Run with:  python3 main.py
"""

import sys
import time

# ── dependency check ──────────────────────────────────────────────────────────
MISSING = []
for pkg, name in [
    ("textual",       "textual"),
    ("yfinance",      "yfinance"),
    ("feedparser",    "feedparser"),
    ("vaderSentiment","vaderSentiment"),
    ("requests",      "requests"),
]:
    try:
        __import__(pkg)
    except ImportError:
        MISSING.append(name)

if MISSING:
    print(f"\n[ERROR] Missing core packages: {', '.join(MISSING)}")
    print("Run:  pip install " + " ".join(MISSING) + " --break-system-packages")
    sys.exit(1)

# ── now import project modules (after dep check) ──────────────────────────────
from config.settings import (
    log, DB_PATH, CONFIG_PATH, LOG_PATH,
    NEWS_API_KEY, FRED_API_KEY, REFRESH_SEC, PRICE_REFRESH_SEC,
    RSS_FEEDS, FRED_SERIES, FULL_TEXT_ENABLED,
)
from db.database import init_db
from nlp.scorer  import FINBERT_READY
from nlp.entities import SPACY_READY
from ingestion.feeds import NEWSPAPER_READY
from ui.app import ESMApp


def main():
    print("=" * 66)
    print("  ECONOMIC SENTIMENT MONITOR  v0.7")
    print("=" * 66)

    init_db()
    log.info("ESM v0.7 starting up")

    print(f"  DB             : {DB_PATH}")
    print(f"  Config         : {CONFIG_PATH}")
    print(f"  Log            : {LOG_PATH}")
    print(f"  Price refresh  : every {PRICE_REFRESH_SEC}s")
    print(f"  News refresh   : every {REFRESH_SEC}s")
    print(f"  NewsAPI        : {'SET' if NEWS_API_KEY else 'not set (RSS only)'}")
    print(f"  FRED           : {'SET — ' + str(len(FRED_SERIES)) + ' series' if FRED_API_KEY else 'not set'}")
    print(f"  FinBERT        : {'LOADED ✓' if FINBERT_READY else 'not available — VADER only'}")
    print(f"  SpaCy NER      : {'LOADED ✓' if SPACY_READY   else 'not available — regex fallback'}")
    print(f"  Full-text      : {'ENABLED ✓' if FULL_TEXT_ENABLED and NEWSPAPER_READY else 'disabled'}")
    print(f"  RSS feeds      : {len(RSS_FEEDS)}")
    print(f"  Analytics      : regime + momentum + lead-lag + Bayesian")
    print("-" * 66)

    if not FRED_API_KEY:
        print("  → FRED key   : https://fred.stlouisfed.org/docs/api/api_key.html")
        print("                 add to ~/.esm/config.yml as: fred_api_key: \"KEY\"")
    if not NEWSPAPER_READY:
        print("  → Full text  : pip install newspaper3k --break-system-packages")
    if not FINBERT_READY:
        print("  → FinBERT    : pip install transformers torch --break-system-packages")
    if not SPACY_READY:
        print("  → SpaCy      : pip install spacy --break-system-packages")
        print("                 python3 -m spacy download en_core_web_sm --break-system-packages")

    print("=" * 66)
    print("  Starting...")
    log.info("startup complete — launching TUI")
    time.sleep(0.5)

    ESMApp().run()


if __name__ == "__main__":
    main()
