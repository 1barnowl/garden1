"""
market_terminal/storage/database.py
SQLite connection, schema creation, safe migrations.
v0.9 adds: filings table, FTS5 virtual tables, data_freshness tracking.
"""

import sqlite3
import os
from market_terminal.config.settings import DB_PATH, log

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")
    conn.execute("PRAGMA foreign_keys=ON")
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
            text_length     INTEGER DEFAULT 0,
            numeric_signals TEXT,
            source_tier     TEXT
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
            high        REAL,
            low         REAL,
            open        REAL,
            vwap        REAL,
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

        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id          TEXT PRIMARY KEY,
            ticker      TEXT,
            signal_ts   TEXT,
            signal_dir  TEXT,
            signal_conf REAL,
            source      TEXT,
            price_at    REAL,
            price_1h    REAL,
            price_4h    REAL,
            price_24h   REAL,
            correct_1h  INTEGER,
            correct_4h  INTEGER,
            correct_24h INTEGER,
            resolved    INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS filings (
            id          TEXT PRIMARY KEY,
            ticker      TEXT NOT NULL,
            company     TEXT,
            form_type   TEXT,
            filed_at    TEXT,
            period      TEXT,
            url         TEXT,
            description TEXT,
            summary     TEXT,
            sentiment   REAL DEFAULT 0.0,
            keywords    TEXT DEFAULT '[]',
            ingested    TEXT
        );

        CREATE TABLE IF NOT EXISTS data_freshness (
            feed        TEXT PRIMARY KEY,
            last_ok     TEXT,
            last_err    TEXT,
            err_count   INTEGER DEFAULT 0,
            item_count  INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS ix_articles_ingested  ON articles(ingested);
        CREATE INDEX IF NOT EXISTS ix_articles_source    ON articles(source);
        CREATE INDEX IF NOT EXISTS ix_entity_ts          ON entity_mentions(ts);
        CREATE INDEX IF NOT EXISTS ix_entity_ticker      ON entity_mentions(ticker, ts);
        CREATE INDEX IF NOT EXISTS ix_prices_ticker      ON market_prices(ticker, ts);
        CREATE INDEX IF NOT EXISTS ix_scores_key         ON sentiment_scores(key, window, ts);
        CREATE INDEX IF NOT EXISTS ix_fred_ts            ON fred_data(ts);
        CREATE INDEX IF NOT EXISTS ix_alerts_ts          ON alerts(ts);
        CREATE INDEX IF NOT EXISTS ix_outcomes_ticker    ON signal_outcomes(ticker, signal_ts);
        CREATE INDEX IF NOT EXISTS ix_filings_ticker     ON filings(ticker, filed_at);
        CREATE INDEX IF NOT EXISTS ix_filings_form       ON filings(form_type, filed_at);
        """)

        _ensure_fts5(db)

        migrations = [
            ("articles",      "full_text",        "INTEGER", "0"),
            ("articles",      "text_length",       "INTEGER", "0"),
            ("articles",      "vader_score",       "REAL",    "NULL"),
            ("articles",      "finbert_score",     "REAL",    "NULL"),
            ("articles",      "model_tier",        "TEXT",    "NULL"),
            ("articles",      "entities",          "TEXT",    "NULL"),
            ("articles",      "numeric_signals",   "TEXT",    "NULL"),
            ("articles",      "source_tier",       "TEXT",    "NULL"),
            ("market_prices", "high",              "REAL",    "NULL"),
            ("market_prices", "low",               "REAL",    "NULL"),
            ("market_prices", "open",              "REAL",    "NULL"),
            ("market_prices", "vwap",              "REAL",    "NULL"),
        ]
        for table, col, typ, default in migrations:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ} DEFAULT {default}")
            except Exception:
                pass

        log.info("Database initialised at %s", DB_PATH)


def _ensure_fts5(db: sqlite3.Connection):
    try:
        db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _fts5_check USING fts5(x)")
        db.execute("DROP TABLE IF EXISTS _fts5_check")
    except Exception:
        log.warning("SQLite FTS5 not available — full-text search disabled")
        return

    db.executescript("""
    CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts
    USING fts5(id UNINDEXED, title, source UNINDEXED, keywords,
               content=articles, content_rowid=rowid);

    CREATE TRIGGER IF NOT EXISTS articles_fts_ins
    AFTER INSERT ON articles BEGIN
        INSERT INTO articles_fts(rowid,id,title,source,keywords)
        VALUES (new.rowid,new.id,new.title,new.source,new.keywords);
    END;

    CREATE TRIGGER IF NOT EXISTS articles_fts_del
    AFTER DELETE ON articles BEGIN
        INSERT INTO articles_fts(articles_fts,rowid,id,title,source,keywords)
        VALUES ('delete',old.rowid,old.id,old.title,old.source,old.keywords);
    END;

    CREATE VIRTUAL TABLE IF NOT EXISTS filings_fts
    USING fts5(id UNINDEXED, ticker UNINDEXED, company,
               form_type UNINDEXED, description, summary,
               content=filings, content_rowid=rowid);

    CREATE TRIGGER IF NOT EXISTS filings_fts_ins
    AFTER INSERT ON filings BEGIN
        INSERT INTO filings_fts(rowid,id,ticker,company,form_type,description,summary)
        VALUES (new.rowid,new.id,new.ticker,new.company,new.form_type,new.description,new.summary);
    END;

    CREATE TRIGGER IF NOT EXISTS filings_fts_del
    AFTER DELETE ON filings BEGIN
        INSERT INTO filings_fts(filings_fts,rowid,id,ticker,company,form_type,description,summary)
        VALUES ('delete',old.rowid,old.id,old.ticker,old.company,old.form_type,old.description,old.summary);
    END;
    """)


def update_freshness(db: sqlite3.Connection, feed: str, *,
                     ok: bool, item_count: int = 0, err_msg: str = ""):
    from market_terminal.core.utils import ts_now
    now = ts_now()
    if ok:
        db.execute("""INSERT INTO data_freshness (feed,last_ok,err_count,item_count)
                      VALUES (?,?,0,?)
                      ON CONFLICT(feed) DO UPDATE SET
                        last_ok=excluded.last_ok, err_count=0,
                        item_count=excluded.item_count""",
                   (feed, now, item_count))
    else:
        db.execute("""INSERT INTO data_freshness (feed,last_err,err_count)
                      VALUES (?,?,1)
                      ON CONFLICT(feed) DO UPDATE SET
                        last_err=excluded.last_err, err_count=err_count+1""",
                   (feed, now))


def get_freshness(db: sqlite3.Connection) -> dict:
    rows = db.execute(
        "SELECT feed,last_ok,last_err,err_count,item_count FROM data_freshness"
    ).fetchall()
    return {r[0]: {"last_ok": r[1], "last_err": r[2],
                   "err_count": r[3], "item_count": r[4]} for r in rows}


# ── instruments table helpers (added v0.10) ───────────────────────────────────

def seed_instruments(tickers: dict[str, dict]):
    """
    Upsert instruments from WATCHLIST.
    tickers = {symbol: {name, asset_class, sector, currency, exchange}}
    """
    with get_db() as db:
        # Ensure instruments table exists
        db.execute("""
            CREATE TABLE IF NOT EXISTS instruments (
                symbol      TEXT PRIMARY KEY,
                name        TEXT DEFAULT '',
                exchange    TEXT DEFAULT '',
                currency    TEXT DEFAULT 'USD',
                sector      TEXT DEFAULT '',
                asset_class TEXT DEFAULT 'equity',
                figi        TEXT DEFAULT '',
                updated_at  TEXT
            )
        """)
        from market_terminal.core.utils import ts_now
        now = ts_now()
        for symbol, meta in tickers.items():
            db.execute("""
                INSERT INTO instruments (symbol, name, exchange, currency,
                    sector, asset_class, figi, updated_at)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    name=excluded.name, exchange=excluded.exchange,
                    currency=excluded.currency, sector=excluded.sector,
                    asset_class=excluded.asset_class, updated_at=excluded.updated_at
            """, (
                symbol,
                meta.get("name", symbol),
                meta.get("exchange", ""),
                meta.get("currency", "USD"),
                meta.get("sector", ""),
                meta.get("asset_class", "equity"),
                meta.get("figi", ""),
                now,
            ))
