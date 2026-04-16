#!/usr/bin/env python3
"""Economic Sentiment Monitor v0.11"""
import sys, os, time

MISSING = []
for pkg, name in [("textual","textual"),("yfinance","yfinance"),
                  ("feedparser","feedparser"),("vaderSentiment","vaderSentiment"),
                  ("requests","requests")]:
    try: __import__(pkg)
    except ImportError: MISSING.append(name)
if MISSING:
    print(f"\n[ERROR] Missing: {', '.join(MISSING)}")
    print("Run:  pip install " + " ".join(MISSING) + " --break-system-packages")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from market_terminal.config.settings import (
    log, WATCHLIST, SECTORS, RSS_FEEDS, FRED_SERIES,
    NEWS_API_KEY, FRED_API_KEY, REFRESH_SEC, PRICE_REFRESH_SEC, FULL_TEXT_ENABLED,
)
from market_terminal.storage.database  import init_db
from market_terminal.storage.workspace import WS
from market_terminal.search.indexer    import rebuild_articles_index, rebuild_filings_index
from market_terminal.enrich.scorer     import FINBERT_READY
from market_terminal.enrich.entities   import SPACY_READY
from market_terminal.enrich.classifier import _ML_READY as CLF_ML
from market_terminal.ingest.feeds      import NEWSPAPER_READY
from market_terminal.core.models       import PYDANTIC_AVAILABLE
from market_terminal.ui.app            import ESMApp

def _count(d): return sum(len(v) for v in d.values())

def main():
    print("═" * 62)
    print("  ECONOMIC SENTIMENT MONITOR  v0.11")
    print("═" * 62)
    init_db()
    try: rebuild_articles_index(); rebuild_filings_index()
    except Exception: pass

    total_tickers = _count(WATCHLIST)
    print(f"  Watchlist      : {total_tickers} tickers total")
    for cat, tickers in WATCHLIST.items():
        print(f"    {cat:<12}: {len(tickers)}")
    print(f"  Sectors        : {len(SECTORS)}")
    print(f"  RSS feeds      : {len(RSS_FEEDS)} (concurrent)")
    print(f"  FRED series    : {len(FRED_SERIES)}")
    print(f"  NewsAPI        : {'SET' if NEWS_API_KEY else 'not set'}")
    print(f"  FRED key       : {'SET' if FRED_API_KEY else 'not set'}")
    print(f"  NLP            : {'FB+VD' if FINBERT_READY else 'VD'}  NER:{'✓' if SPACY_READY else '✗'}")
    print(f"  Classifier     : rules + {'sklearn' if CLF_ML else 'rules-only'}")
    print(f"  Pydantic       : {'✓' if PYDANTIC_AVAILABLE else 'stubs'}")
    print(f"  Full-text      : {'✓' if FULL_TEXT_ENABLED and NEWSPAPER_READY else '✗'}")
    print(f"  Workspace      : pins={len(WS.pinned_tickers)}  notes={len(WS._data.get('notes',{}))}")
    print(f"  Refresh        : prices={PRICE_REFRESH_SEC}s  news={REFRESH_SEC}s")
    print("─" * 62)
    print("  Header: 5 clean rows — no file paths")
    print("  Ctrl+P: QUOTE CHART NEWS FILING SEARCH PIN NOTE WATCH")
    print("═" * 62)
    print("  Starting…")
    log.info("ESM v0.11 starting")
    time.sleep(0.3)
    ESMApp().run()

if __name__ == "__main__":
    main()
