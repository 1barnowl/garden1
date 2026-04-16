"""
market_terminal/search/indexer.py

FTS5-backed full-text search index management.
- Triggers keep the index live for new inserts (defined in database.py)
- This module handles: rebuild, vacuum, and stats
"""

from market_terminal.storage.database import get_db
from market_terminal.config.settings import log


def rebuild_articles_index() -> int:
    """Rebuild the articles_fts index from scratch. Returns row count."""
    with get_db() as db:
        try:
            db.execute("INSERT INTO articles_fts(articles_fts) VALUES('rebuild')")
            count = db.execute("SELECT COUNT(*) FROM articles_fts").fetchone()[0]
            log.info("articles_fts rebuilt: %d rows", count)
            return count
        except Exception as e:
            log.warning("articles_fts rebuild: %s", e)
            return 0


def rebuild_filings_index() -> int:
    """Rebuild the filings_fts index from scratch. Returns row count."""
    with get_db() as db:
        try:
            db.execute("INSERT INTO filings_fts(filings_fts) VALUES('rebuild')")
            count = db.execute("SELECT COUNT(*) FROM filings_fts").fetchone()[0]
            log.info("filings_fts rebuilt: %d rows", count)
            return count
        except Exception as e:
            log.warning("filings_fts rebuild: %s", e)
            return 0


def index_stats() -> dict:
    """Return row counts for both FTS tables."""
    with get_db() as db:
        try:
            a = db.execute("SELECT COUNT(*) FROM articles_fts").fetchone()[0]
        except Exception:
            a = 0
        try:
            f = db.execute("SELECT COUNT(*) FROM filings_fts").fetchone()[0]
        except Exception:
            f = 0
    return {"articles_fts": a, "filings_fts": f}


def vacuum_index():
    """Run FTS5 optimize (merge segments, reduce disk). Call periodically."""
    with get_db() as db:
        try:
            db.execute("INSERT INTO articles_fts(articles_fts) VALUES('optimize')")
            db.execute("INSERT INTO filings_fts(filings_fts) VALUES('optimize')")
            log.info("FTS5 optimize complete")
        except Exception as e:
            log.warning("FTS5 optimize: %s", e)
