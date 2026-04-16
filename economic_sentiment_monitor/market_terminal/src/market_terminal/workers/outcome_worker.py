"""
market_terminal/workers/outcome_worker.py

Signal outcome resolution worker.

For every unresolved row in signal_outcomes, check whether a matching
price record exists 1h / 4h / 24h later.  If so, mark the outcome and
whether the signal direction was correct.

Also exposes aggregate accuracy stats used by the Analytics tab.

Runs every 10 minutes alongside the correlation refresh.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from market_terminal.config.settings import log
from market_terminal.storage.database import get_db


# ── record a new signal ───────────────────────────────────────────────────────

def record_signal(ticker: str, direction: str, confidence: float,
                  source: str = "sentiment") -> str | None:
    """
    Write a new unresolved signal to signal_outcomes.
    Returns the signal id, or None on failure.
    Called from analytics/engine.py whenever a Bayesian signal fires.
    """
    from market_terminal.core.utils import ts_now, dedup_id
    ts    = ts_now()
    sid   = dedup_id(ticker, direction, ts, source)

    # Get current price
    with get_db() as db:
        row = db.execute(
            "SELECT price FROM market_prices WHERE ticker=? ORDER BY ts DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        price_at = row[0] if row else 0.0

        try:
            db.execute("""
                INSERT OR IGNORE INTO signal_outcomes
                (id, ticker, signal_ts, signal_dir, signal_conf,
                 source, price_at, resolved)
                VALUES (?,?,?,?,?,?,?,0)
            """, (sid, ticker.upper(), ts, direction.upper(),
                  round(confidence, 4), source, price_at))
        except Exception as e:
            log.debug("record_signal error: %s", e)
            return None

    return sid


# ── resolve outcomes ──────────────────────────────────────────────────────────

def _resolve_one(db, row: tuple) -> bool:
    """
    Attempt to fill in price_1h / price_4h / price_24h for one signal.
    Returns True if the row is now fully resolved.
    """
    (sid, ticker, signal_ts, signal_dir,
     price_at, p1h, p4h, p24h) = row

    updates: dict[str, object] = {}

    def _get_price(hours: float) -> float | None:
        try:
            target = (datetime.fromisoformat(signal_ts.replace("Z", ""))
                      + timedelta(hours=hours)).isoformat()
            r = db.execute("""
                SELECT price FROM market_prices
                WHERE ticker=? AND ts >= ?
                ORDER BY ts ASC LIMIT 1
            """, (ticker, target)).fetchone()
            return float(r[0]) if r else None
        except Exception:
            return None

    if p1h is None:
        p = _get_price(1)
        if p:
            updates["price_1h"]   = p
            updates["correct_1h"] = int(
                (signal_dir == "BULL" and p > price_at) or
                (signal_dir == "BEAR" and p < price_at)
            ) if price_at and signal_dir in ("BULL", "BEAR") else None

    if p4h is None:
        p = _get_price(4)
        if p:
            updates["price_4h"]   = p
            updates["correct_4h"] = int(
                (signal_dir == "BULL" and p > price_at) or
                (signal_dir == "BEAR" and p < price_at)
            ) if price_at and signal_dir in ("BULL", "BEAR") else None

    if p24h is None:
        p = _get_price(24)
        if p:
            updates["price_24h"]   = p
            updates["correct_24h"] = int(
                (signal_dir == "BULL" and p > price_at) or
                (signal_dir == "BEAR" and p < price_at)
            ) if price_at and signal_dir in ("BULL", "BEAR") else None

    if not updates:
        return False

    # Check if now fully resolved
    merged_1h  = updates.get("price_1h",  p1h)
    merged_4h  = updates.get("price_4h",  p4h)
    merged_24h = updates.get("price_24h", p24h)
    resolved   = 1 if (merged_1h and merged_4h and merged_24h) else 0

    updates["resolved"] = resolved

    set_clause = ", ".join(f"{k}=?" for k in updates)
    values     = list(updates.values()) + [sid]
    db.execute(f"UPDATE signal_outcomes SET {set_clause} WHERE id=?", values)
    return bool(resolved)


def resolve_outcomes(limit: int = 200) -> int:
    """
    Scan unresolved signals and fill in price outcomes.
    Returns number of newly resolved rows.
    """
    resolved_count = 0
    with get_db() as db:
        rows = db.execute("""
            SELECT id, ticker, signal_ts, signal_dir, price_at,
                   price_1h, price_4h, price_24h
            FROM signal_outcomes
            WHERE resolved=0
            ORDER BY signal_ts DESC
            LIMIT ?
        """, (limit,)).fetchall()

        for row in rows:
            if _resolve_one(db, row):
                resolved_count += 1

    return resolved_count


# ── accuracy stats ────────────────────────────────────────────────────────────

def get_accuracy_stats(ticker: str | None = None,
                       days_back: int = 30) -> dict:
    """
    Return directional accuracy stats for resolved signals.
    Optionally filtered by ticker.

    Returns:
        {
          "total": int,
          "correct_1h": float,   # 0.0–1.0
          "correct_4h": float,
          "correct_24h": float,
          "by_ticker": {ticker: {total, correct_1h, ...}},
          "by_direction": {BULL: ..., BEAR: ...},
        }
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()

    with get_db() as db:
        q = """
            SELECT ticker, signal_dir,
                   correct_1h, correct_4h, correct_24h
            FROM signal_outcomes
            WHERE resolved=1
              AND signal_ts > ?
        """
        params = [cutoff]
        if ticker:
            q += " AND ticker=?"
            params.append(ticker.upper())

        rows = db.execute(q, params).fetchall()

    if not rows:
        return {"total": 0, "correct_1h": 0.0, "correct_4h": 0.0,
                "correct_24h": 0.0, "by_ticker": {}, "by_direction": {}}

    total       = len(rows)
    c1h  = sum(r[2] for r in rows if r[2] is not None)
    c4h  = sum(r[3] for r in rows if r[3] is not None)
    c24h = sum(r[4] for r in rows if r[4] is not None)
    n1h  = sum(1 for r in rows if r[2] is not None)
    n4h  = sum(1 for r in rows if r[3] is not None)
    n24h = sum(1 for r in rows if r[4] is not None)

    # per ticker breakdown
    by_ticker: dict[str, dict] = {}
    for tk, _, co1, co4, co24 in rows:
        e = by_ticker.setdefault(tk, {"total": 0, "c1": 0, "c4": 0, "c24": 0,
                                       "n1": 0, "n4": 0, "n24": 0})
        e["total"] += 1
        if co1  is not None: e["c1"]  += co1;  e["n1"]  += 1
        if co4  is not None: e["c4"]  += co4;  e["n4"]  += 1
        if co24 is not None: e["c24"] += co24; e["n24"] += 1

    # per direction breakdown
    by_dir: dict[str, dict] = {}
    for _, direction, co1, co4, co24 in rows:
        d = by_dir.setdefault(direction, {"total": 0, "c1": 0, "n1": 0})
        d["total"] += 1
        if co1 is not None: d["c1"] += co1; d["n1"] += 1

    return {
        "total":       total,
        "correct_1h":  round(c1h  / n1h,  3) if n1h  else 0.0,
        "correct_4h":  round(c4h  / n4h,  3) if n4h  else 0.0,
        "correct_24h": round(c24h / n24h, 3) if n24h else 0.0,
        "by_ticker":   {
            tk: {
                "total":       v["total"],
                "correct_1h":  round(v["c1"]  / v["n1"],  3) if v["n1"]  else 0.0,
                "correct_4h":  round(v["c4"]  / v["n4"],  3) if v["n4"]  else 0.0,
                "correct_24h": round(v["c24"] / v["n24"], 3) if v["n24"] else 0.0,
            }
            for tk, v in sorted(by_ticker.items(), key=lambda x: -x[1]["total"])
        },
        "by_direction": {
            d: {"total": v["total"],
                "correct_1h": round(v["c1"] / v["n1"], 3) if v["n1"] else 0.0}
            for d, v in by_dir.items()
        },
    }


# ── background worker ─────────────────────────────────────────────────────────

def run_outcome_worker(cache_ref=None, interval: int = 600):
    """
    Background thread.  Resolves signal outcomes and logs accuracy.
    Call as: threading.Thread(target=run_outcome_worker, daemon=True).start()
    """
    log.info("outcome_worker started (interval=%ds)", interval)
    while True:
        try:
            n = resolve_outcomes(limit=300)
            if n:
                log.info("outcome_worker: resolved %d signals", n)
                stats = get_accuracy_stats(days_back=7)
                log.info(
                    "signal accuracy 7d: 1h=%.0f%%  4h=%.0f%%  24h=%.0f%%  n=%d",
                    stats["correct_1h"]  * 100,
                    stats["correct_4h"]  * 100,
                    stats["correct_24h"] * 100,
                    stats["total"],
                )
        except Exception as e:
            log.warning("outcome_worker error: %s", e)
        time.sleep(interval)
