#!/usr/bin/env python3
"""Economic Sentiment Monitor v0.10 — entry point."""
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
    log, DB_PATH, CONFIG_PATH, LOG_PATH,
    NEWS_API_KEY, FRED_API_KEY, REFRESH_SEC, PRICE_REFRESH_SEC,
    RSS_FEEDS, FRED_SERIES, FULL_TEXT_ENABLED,
)
from market_terminal.storage.database  import init_db
from market_terminal.storage.workspace import WS, WORKSPACE_PATH
from market_terminal.search.indexer    import rebuild_articles_index, rebuild_filings_index
from market_terminal.enrich.scorer     import FINBERT_READY
from market_terminal.enrich.entities   import SPACY_READY
from market_terminal.enrich.classifier import _ML_READY as CLASSIFIER_ML_READY
from market_terminal.ingest.feeds      import NEWSPAPER_READY
from market_terminal.core.models       import PYDANTIC_AVAILABLE
from market_terminal.ui.app            import ESMApp

def main():
    print("=" * 68)
    print("  ECONOMIC SENTIMENT MONITOR  v0.10")
    print("=" * 68)
    init_db()
    try:
        rebuild_articles_index(); rebuild_filings_index()
    except Exception: pass
    print(f"  DB             : {DB_PATH}")
    print(f"  Config         : {CONFIG_PATH}")
    print(f"  Workspace      : {WORKSPACE_PATH}  ({WS.summary()})")
    print(f"  Log            : {LOG_PATH}")
    print(f"  FinBERT        : {'LOADED ✓' if FINBERT_READY else 'not available'}")
    print(f"  SpaCy NER      : {'LOADED ✓' if SPACY_READY   else 'regex fallback'}")
    print(f"  Pydantic       : {'✓' if PYDANTIC_AVAILABLE else 'stub mode'}")
    print(f"  Classifier ML  : {'✓ sklearn' if CLASSIFIER_ML_READY else 'rules-only'}")
    print(f"  Full-text      : {'✓' if FULL_TEXT_ENABLED and NEWSPAPER_READY else 'disabled'}")
    print(f"  RSS feeds      : {len(RSS_FEEDS)}  (concurrent ThreadPoolExecutor)")
    print(f"  NewsAPI        : {'SET' if NEWS_API_KEY else 'not set (RSS only)'}")
    print(f"  FRED           : {'SET' if FRED_API_KEY else 'not set'}")
    print(f"  FTS5 search    : ✓  │  Event classifier: ✓  │  Ctrl+P=command")
    print(f"  Workspace      : pinned={len(WS.pinned_tickers)}  notes={len(WS._data.get('notes',{}))}")
    print("=" * 68)
    print("  Starting…")
    log.info("ESM v0.10 starting")
    time.sleep(0.4)
    ESMApp().run()

if __name__ == "__main__":
    main()
