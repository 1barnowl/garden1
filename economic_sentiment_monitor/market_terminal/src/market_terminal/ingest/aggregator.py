"""
market_terminal/ingest/aggregator.py
Rolls up article scores into time-windowed sentiment aggregates.
Z-score anomaly detection and slow-drift alerts.
"""

import math
import hashlib
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from market_terminal.config.settings import (
    log, ALERT_MIN_COUNT, ALERT_WARN, ALERT_CRIT,
    ALERT_ZSCORE_WARN, ALERT_ZSCORE_CRIT, ALERT_DRIFT_N, MACRO_KEYWORDS,
)
from market_terminal.storage.database import get_db


def compute_aggregate_scores():
    """Roll article scores into 1h/4h/24h windows per theme, ticker, global."""
    cutoffs = {
        "1h":  (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "4h":  (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat(),
        "24h": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
    }
    ts_now = datetime.now(timezone.utc).isoformat()
    with get_db() as db:
        for window, cutoff in cutoffs.items():
            rows = db.execute(
                "SELECT score, keywords FROM articles WHERE ingested > ?", (cutoff,)
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
                            round(sum(global_scores) / len(global_scores), 4),
                            len(global_scores)))

            for theme, scores in theme_scores.items():
                db.execute("""INSERT OR REPLACE INTO sentiment_scores
                              (key,window,ts,score,count) VALUES (?,?,?,?,?)""",
                           (theme, window, ts_now,
                            round(sum(scores) / len(scores), 4), len(scores)))

            # per-ticker from entity mentions
            ticker_rows = db.execute(
                "SELECT ticker, score FROM entity_mentions WHERE ts > ?", (cutoff,)
            ).fetchall()
            ticker_scores = defaultdict(list)
            for ticker, score in ticker_rows:
                ticker_scores[ticker].append(score)
            for ticker, scores in ticker_scores.items():
                db.execute("""INSERT OR REPLACE INTO sentiment_scores
                              (key,window,ts,score,count) VALUES (?,?,?,?,?)""",
                           (ticker, window, ts_now,
                            round(sum(scores) / len(scores), 4), len(scores)))


def _rolling_stats(key: str, window_days: int = 7):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (key, cutoff)).fetchall()
    scores = [r[0] for r in rows]
    n = len(scores)
    if n < 10:
        return None, None, scores
    mean     = sum(scores) / n
    variance = sum((s - mean) ** 2 for s in scores) / n
    std      = math.sqrt(variance) if variance > 0 else 0.0
    return round(mean, 5), round(std, 5), scores


def _check_slow_drift(scores: list, n: int = 3):
    if len(scores) < n + 1:
        return None
    tail  = scores[-(n + 1):]
    diffs = [tail[i + 1] - tail[i] for i in range(len(tail) - 1)]
    if all(d > 0 for d in diffs):
        return "up"
    if all(d < 0 for d in diffs):
        return "down"
    return None


def check_alerts() -> list:
    """Z-score based alerts with slow-drift detection. Falls back to delta."""
    alerts  = []
    ts_now  = datetime.now(timezone.utc).isoformat()
    with get_db() as db:
        rows_1h = db.execute("""
            SELECT key, score, count FROM sentiment_scores
            WHERE window='1h' ORDER BY ts DESC
        """).fetchall()
        seen_keys = {}
        for key, score, count in rows_1h:
            if key not in seen_keys:
                seen_keys[key] = (score, count)

        for key, (score, count) in seen_keys.items():
            if count < ALERT_MIN_COUNT:
                continue
            mean, std, history = _rolling_stats(key)

            if mean is not None and std is not None and std > 0:
                z = (score - mean) / std
                if abs(z) >= ALERT_ZSCORE_CRIT:
                    severity  = "CRIT"
                    direction = "SURGE ▲" if z > 0 else "DROP  ▼"
                    reason    = (f"[Z] {key} {direction} z={z:+.2f}σ "
                                 f"(score={score:+.4f} μ={mean:+.4f} σ={std:.4f})")
                elif abs(z) >= ALERT_ZSCORE_WARN:
                    severity  = "WARN"
                    direction = "SURGE ▲" if z > 0 else "DROP  ▼"
                    reason    = (f"[Z] {key} {direction} z={z:+.2f}σ "
                                 f"(score={score:+.4f} μ={mean:+.4f} σ={std:.4f})")
                else:
                    drift = _check_slow_drift(history, ALERT_DRIFT_N)
                    if drift:
                        severity  = "WARN"
                        direction = "DRIFT ▲" if drift == "up" else "DRIFT ▼"
                        reason    = (f"[DRIFT] {key} {direction} "
                                     f"{ALERT_DRIFT_N} consecutive windows "
                                     f"(last={score:+.4f})")
                    else:
                        continue
            else:
                prev_row = db.execute("""
                    SELECT score FROM sentiment_scores
                    WHERE key=? AND window='4h' ORDER BY ts DESC LIMIT 1
                """, (key,)).fetchone()
                if not prev_row:
                    continue
                delta = score - prev_row[0]
                if abs(delta) < ALERT_WARN:
                    continue
                severity  = "CRIT" if abs(delta) >= ALERT_CRIT else "WARN"
                direction = "SURGE ▲" if delta > 0 else "DROP  ▼"
                reason    = (f"[Δ] {key} {direction} "
                             f"Δ{abs(delta):.3f} vs 4h (n={count}, thin history)")

            alert_id = hashlib.md5(
                f"{key}{round(score, 2)}{datetime.now(timezone.utc).date()}".encode()
            ).hexdigest()
            alerts.append({
                "id": alert_id, "ts": ts_now,
                "severity": severity, "key": key,
                "reason": reason, "score": score,
            })

        if alerts:
            db.executemany("""
                INSERT OR IGNORE INTO alerts (id,ts,severity,key,reason,score)
                VALUES (:id,:ts,:severity,:key,:reason,:score)
            """, alerts)

    return alerts
