"""
db/database.py
SQLite connection, schema creation, safe migrations.
All other modules call get_db() from here.
"""

import sqlite3
import os
from config.settings import DB_PATH, log

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")   # 32 MB page cache
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

        CREATE INDEX IF NOT EXISTS ix_articles_ingested  ON articles(ingested);
        CREATE INDEX IF NOT EXISTS ix_articles_source    ON articles(source);
        CREATE INDEX IF NOT EXISTS ix_entity_ts          ON entity_mentions(ts);
        CREATE INDEX IF NOT EXISTS ix_entity_ticker      ON entity_mentions(ticker, ts);
        CREATE INDEX IF NOT EXISTS ix_prices_ticker      ON market_prices(ticker, ts);
        CREATE INDEX IF NOT EXISTS ix_scores_key         ON sentiment_scores(key, window, ts);
        CREATE INDEX IF NOT EXISTS ix_fred_ts            ON fred_data(ts);
        CREATE INDEX IF NOT EXISTS ix_alerts_ts          ON alerts(ts);
        CREATE INDEX IF NOT EXISTS ix_outcomes_ticker    ON signal_outcomes(ticker, signal_ts);
        """)

        # safe migrations — add columns introduced across versions
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
