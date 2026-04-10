"""
db/cache.py
Shared in-memory DataCache — single instance imported by all modules.
Thread-safe via internal lock.
"""

import threading
import traceback
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

from config.settings import log
from db.database import get_db


class DataCache:
    def __init__(self):
        self._lock              = threading.Lock()
        self.prices: dict       = {}          # ticker → price row
        self.scores: dict       = {}          # key → {window → {score,count}}
        self.recent_articles    = []          # last 200 articles
        self.entity_summary     = []          # per-ticker 24h mention stats
        self.correlations: dict = {}          # ticker → pearson_r
        self.fred_data: list    = []          # FRED series latest values
        self.alerts: deque      = deque(maxlen=100)
        self.last_refresh       = "—"
        self.article_count      = 0
        self.fulltext_count     = 0
        self.model_tier         = "vader"
        self.fulltext_queue_len = 0
        self.err_counts: dict   = {
            "ingest": 0, "fulltext": 0, "fred": 0,
            "corr": 0, "analytics": 0, "prices": 0,
        }
        self.last_errors: dict  = {
            "ingest": "", "fulltext": "", "fred": "",
            "corr": "", "analytics": "", "prices": "",
        }

    # ── error tracking ────────────────────────────────────────────────────────
    def record_error(self, thread: str, exc: Exception):
        msg = f"{type(exc).__name__}: {exc}"
        tb  = traceback.format_exc()
        log.error("[%s] %s\n%s", thread, msg, tb)
        with self._lock:
            self.err_counts[thread]  = self.err_counts.get(thread, 0) + 1
            self.last_errors[thread] = f"{datetime.now().strftime('%H:%M:%S')} {msg[:70]}"

    def clear_error(self, thread: str):
        with self._lock:
            self.err_counts[thread]  = 0
            self.last_errors[thread] = ""

    # ── price updates ─────────────────────────────────────────────────────────
    def update_prices(self, rows: list):
        with self._lock:
            for r in rows:
                self.prices[r["ticker"]] = r

    # ── score updates ─────────────────────────────────────────────────────────
    def update_scores(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        with get_db() as db:
            rows = db.execute(
                "SELECT key,window,score,count FROM sentiment_scores WHERE ts > ?",
                (cutoff,)
            ).fetchall()
        with self._lock:
            self.scores = defaultdict(dict)
            for key, window, score, count in rows:
                self.scores[key][window] = {"score": score, "count": count}

    # ── article updates ───────────────────────────────────────────────────────
    def update_articles(self):
        with get_db() as db:
            rows = db.execute("""
                SELECT title, source, score, label, published, keywords,
                       model_tier, vader_score, finbert_score,
                       full_text, text_length, numeric_signals, source_tier, url
                FROM articles ORDER BY ingested DESC LIMIT 200
            """).fetchall()
            counts = db.execute(
                "SELECT COUNT(*), SUM(full_text) FROM articles"
            ).fetchone()
        with self._lock:
            self.recent_articles = [
                {"title": r[0], "source": r[1], "score": r[2], "label": r[3],
                 "published": r[4], "keywords": r[5], "model_tier": r[6],
                 "vader_score": r[7], "finbert_score": r[8],
                 "full_text": r[9], "text_length": r[10],
                 "numeric_signals": r[11], "source_tier": r[12], "url": r[13]}
                for r in rows
            ]
            self.article_count  = counts[0] or 0
            self.fulltext_count = counts[1] or 0
            if self.recent_articles:
                self.model_tier = self.recent_articles[0].get("model_tier") or "vader"

    # ── entity summary ────────────────────────────────────────────────────────
    def update_entity_summary(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        with get_db() as db:
            rows = db.execute("""
                SELECT ticker, COUNT(*) as mentions,
                       AVG(score) as avg_score,
                       MIN(score) as min_score,
                       MAX(score) as max_score
                FROM entity_mentions WHERE ts > ?
                GROUP BY ticker ORDER BY mentions DESC LIMIT 60
            """, (cutoff,)).fetchall()
        with self._lock:
            self.entity_summary = [
                {"ticker": r[0], "mentions": r[1],
                 "avg_score": round(r[2], 4) if r[2] else 0,
                 "min_score": round(r[3], 4) if r[3] else 0,
                 "max_score": round(r[4], 4) if r[4] else 0}
                for r in rows
            ]

    # ── correlations ──────────────────────────────────────────────────────────
    def update_correlations(self, corr: dict):
        with self._lock:
            self.correlations.update(corr)

    # ── FRED data ─────────────────────────────────────────────────────────────
    def update_fred_data(self):
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
        with self._lock:
            self.fred_data = [
                {"series_id": r[0], "label": r[1], "theme": r[2],
                 "ts": r[3], "value": r[4], "prev_value": r[5], "surprise": r[6]}
                for r in rows
            ]

    # ── misc ──────────────────────────────────────────────────────────────────
    def update_queue_len(self, n: int):
        with self._lock:
            self.fulltext_queue_len = n

    def add_alerts(self, new_alerts: list):
        with self._lock:
            for a in new_alerts:
                self.alerts.appendleft(a)

    def set_last_refresh(self):
        with self._lock:
            self.last_refresh = datetime.now().strftime("%H:%M:%S")

    def get_price(self, ticker: str) -> dict:
        with self._lock:
            return self.prices.get(ticker, {})

    def get_score(self, key: str) -> dict:
        with self._lock:
            return dict(self.scores.get(key, {}))


# singleton
CACHE = DataCache()
