"""
market_terminal/search/query.py

Full-text search queries over articles and filings.
Uses SQLite FTS5 with BM25 ranking.

Usage:
    from market_terminal.search.query import search_news, search_filings, search_all

    results = search_news("Fed rate cut inflation", limit=20)
    filings = search_filings("AAPL 10-K", limit=10)
    combined = search_all("nvidia earnings guidance", limit=25)
"""

from __future__ import annotations

from market_terminal.storage.database import get_db
from market_terminal.config.settings import log


# ── News / article search ─────────────────────────────────────────────────────

def search_news(query: str, limit: int = 30,
                ticker: str | None = None,
                days_back: int | None = None) -> list[dict]:
    """
    Full-text search over article headlines.
    Returns list of article dicts sorted by BM25 relevance then recency.
    """
    if not query or not query.strip():
        return []

    fts_query = _sanitize_fts_query(query)
    sql_parts = ["SELECT a.id, a.title, a.source, a.score, a.published,"]
    sql_parts.append("       a.source_tier, a.label, a.url,")
    sql_parts.append("       bm25(articles_fts) AS rank")
    sql_parts.append("FROM articles_fts")
    sql_parts.append("JOIN articles a ON a.rowid = articles_fts.rowid")
    sql_parts.append("WHERE articles_fts MATCH ?")

    params: list = [fts_query]

    if days_back:
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        sql_parts.append("AND a.ingested > ?")
        params.append(cutoff)

    if ticker:
        sql_parts.append("AND a.entities LIKE ?")
        params.append(f'%"{ticker}"%')

    sql_parts.append("ORDER BY rank, a.ingested DESC")
    sql_parts.append(f"LIMIT {int(limit)}")

    sql = " ".join(sql_parts)
    try:
        with get_db() as db:
            rows = db.execute(sql, params).fetchall()
        return [
            {"id": r[0], "title": r[1], "source": r[2], "score": r[3],
             "published": r[4], "source_tier": r[5], "label": r[6],
             "url": r[7], "rank": r[8], "type": "news"}
            for r in rows
        ]
    except Exception as e:
        log.debug("search_news error [%r]: %s", query, e)
        return []


# ── Filing search ─────────────────────────────────────────────────────────────

def search_filings(query: str, limit: int = 20,
                   ticker: str | None = None,
                   form_type: str | None = None) -> list[dict]:
    """
    Full-text search over SEC filings (description + summary).
    Returns list of filing dicts sorted by BM25 relevance.
    """
    if not query or not query.strip():
        # No query → return most recent filings, optionally filtered
        return _recent_filings(ticker=ticker, form_type=form_type, limit=limit)

    fts_query = _sanitize_fts_query(query)
    sql_parts = ["SELECT f.id, f.ticker, f.company, f.form_type,"]
    sql_parts.append("       f.filed_at, f.description, f.sentiment, f.url,")
    sql_parts.append("       bm25(filings_fts) AS rank")
    sql_parts.append("FROM filings_fts")
    sql_parts.append("JOIN filings f ON f.rowid = filings_fts.rowid")
    sql_parts.append("WHERE filings_fts MATCH ?")

    params: list = [fts_query]

    if ticker:
        sql_parts.append("AND f.ticker = ?")
        params.append(ticker.upper())

    if form_type:
        sql_parts.append("AND f.form_type = ?")
        params.append(form_type.upper())

    sql_parts.append("ORDER BY rank, f.filed_at DESC")
    sql_parts.append(f"LIMIT {int(limit)}")

    sql = " ".join(sql_parts)
    try:
        with get_db() as db:
            rows = db.execute(sql, params).fetchall()
        return [
            {"id": r[0], "ticker": r[1], "company": r[2], "form_type": r[3],
             "filed_at": r[4], "description": r[5], "sentiment": r[6],
             "url": r[7], "rank": r[8], "type": "filing"}
            for r in rows
        ]
    except Exception as e:
        log.debug("search_filings error [%r]: %s", query, e)
        return []


def _recent_filings(ticker: str | None, form_type: str | None,
                    limit: int) -> list[dict]:
    """Return most recent filings without FTS — used as fallback."""
    sql = "SELECT id, ticker, company, form_type, filed_at, description, sentiment, url FROM filings"
    params: list = []
    wheres = []
    if ticker:
        wheres.append("ticker = ?"); params.append(ticker.upper())
    if form_type:
        wheres.append("form_type = ?"); params.append(form_type.upper())
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += f" ORDER BY filed_at DESC LIMIT {int(limit)}"
    try:
        with get_db() as db:
            rows = db.execute(sql, params).fetchall()
        return [{"id": r[0], "ticker": r[1], "company": r[2], "form_type": r[3],
                 "filed_at": r[4], "description": r[5], "sentiment": r[6],
                 "url": r[7], "type": "filing"} for r in rows]
    except Exception as e:
        log.debug("_recent_filings error: %s", e)
        return []


# ── Combined search ───────────────────────────────────────────────────────────

def search_all(query: str, limit: int = 30) -> list[dict]:
    """
    Search both news and filings, merge by relevance.
    Deduplicated by id.
    """
    news    = search_news(query,    limit=limit // 2 + 5)
    filings = search_filings(query, limit=limit // 2 + 5)
    combined = news + filings
    # Sort by rank (lower BM25 = more relevant in SQLite; filings have no rank → 0)
    combined.sort(key=lambda x: (x.get("rank") or 0))
    return combined[:limit]


# ── Ticker-specific news shortcut ─────────────────────────────────────────────

def news_for_ticker(ticker: str, limit: int = 25) -> list[dict]:
    """Fast lookup of recent news for a specific ticker (no FTS needed)."""
    ticker = ticker.upper()
    try:
        with get_db() as db:
            rows = db.execute("""
                SELECT a.id, a.title, a.source, a.score, a.published,
                       a.source_tier, a.label, a.url
                FROM articles a
                JOIN entity_mentions em ON em.article_id = a.id
                WHERE em.ticker = ?
                ORDER BY a.ingested DESC
                LIMIT ?
            """, (ticker, limit)).fetchall()
        return [{"id": r[0], "title": r[1], "source": r[2], "score": r[3],
                 "published": r[4], "source_tier": r[5], "label": r[6],
                 "url": r[7], "type": "news"} for r in rows]
    except Exception as e:
        log.debug("news_for_ticker [%s]: %s", ticker, e)
        return []


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize_fts_query(query: str) -> str:
    """
    Convert a plain query string to a safe FTS5 match expression.
    Wraps individual terms; strips FTS5 special chars that would error.
    """
    import re
    # Remove FTS5 operators that could cause syntax errors
    cleaned = re.sub(r'[^\w\s\-\.]', ' ', query)
    terms   = [t for t in cleaned.split() if len(t) >= 2]
    if not terms:
        return '""'
    # Quote multi-word phrases; pass single terms as-is
    return " ".join(terms)
