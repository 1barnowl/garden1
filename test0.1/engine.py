"""
analytics/engine.py
All predictive analytics:
  - Pearson correlation (sentiment → price)
  - Lead/lag analysis (optimal sentiment time horizon per ticker)
  - Sentiment momentum (acceleration, consecutive streaks)
  - Regime detection (risk-on/off, high/low vol, neutral)
  - Bayesian source confidence scoring
"""

import math
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from config.settings import log, WATCHLIST, MACRO_KEYWORDS
from db.database import get_db


# ── Pearson helper ────────────────────────────────────────────────────────────

def _pearson(xs: list, ys: list):
    n = len(xs)
    if n < 6:
        return None
    mx, my = sum(xs)/n, sum(ys)/n
    num = sum((x - mx)*(y - my) for x, y in zip(xs, ys))
    den = math.sqrt(sum((x - mx)**2 for x in xs) *
                    sum((y - my)**2 for y in ys))
    if den == 0:
        return None
    return round(num / den, 3)


# ── Correlation ───────────────────────────────────────────────────────────────

def compute_correlations(tickers: list, window_hours: int = 24) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
    result = {}
    with get_db() as db:
        for ticker in tickers:
            sent_rows = db.execute("""
                SELECT ts, score FROM sentiment_scores
                WHERE key=? AND window='1h' AND ts > ?
                ORDER BY ts ASC
            """, (ticker, cutoff)).fetchall()
            if len(sent_rows) < 6:
                result[ticker] = None
                continue
            sent_vals, price_vals = [], []
            for ts_str, s_score in sent_rows:
                try:
                    target_ts = (datetime.fromisoformat(ts_str.replace("Z",""))
                                 + timedelta(hours=1)).isoformat()
                    p = db.execute("""
                        SELECT change_pct FROM market_prices
                        WHERE ticker=? AND ts >= ?
                        ORDER BY ts ASC LIMIT 1
                    """, (ticker, target_ts)).fetchone()
                    if p:
                        sent_vals.append(s_score)
                        price_vals.append(p[0])
                except Exception:
                    pass
            result[ticker] = _pearson(sent_vals, price_vals)
    return result


# ── Lead/lag analysis ─────────────────────────────────────────────────────────

def compute_lead_lag(ticker: str, max_lag_hours: int = 6) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    results = {}
    with get_db() as db:
        sent_rows = db.execute("""
            SELECT ts, score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (ticker, cutoff)).fetchall()

        for lag_h in [0.5, 1, 2, 3, 4, 6]:
            xs, ys = [], []
            for ts_str, s_score in sent_rows:
                try:
                    target = (datetime.fromisoformat(ts_str.replace("Z",""))
                              + timedelta(hours=lag_h)).isoformat()
                    p = db.execute("""
                        SELECT change_pct FROM market_prices
                        WHERE ticker=? AND ts >= ?
                        ORDER BY ts ASC LIMIT 1
                    """, (ticker, target)).fetchone()
                    if p:
                        xs.append(s_score)
                        ys.append(p[0])
                except Exception:
                    pass
            results[lag_h] = _pearson(xs, ys)

    best_lag, best_r = None, None
    for lag_h, r in results.items():
        if r is not None and (best_r is None or abs(r) > abs(best_r)):
            best_lag, best_r = lag_h, r
    return {"by_lag": results, "best_lag": best_lag, "best_r": best_r}


# ── Sentiment momentum ────────────────────────────────────────────────────────

def compute_sentiment_momentum(key: str) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (key, cutoff)).fetchall()
    scores = [r[0] for r in rows]
    if len(scores) < 2:
        return {"acceleration": None, "consecutive": 0, "direction": None}
    acceleration = round(scores[-1] - scores[max(0, len(scores)-4)], 4)
    direction    = "up" if scores[-1] > scores[-2] else "down"
    consecutive  = 1
    for i in range(len(scores)-2, 0, -1):
        match = (direction == "up"   and scores[i] > scores[i-1]) or \
                (direction == "down" and scores[i] < scores[i-1])
        if match:
            consecutive += 1
        else:
            break
    return {"acceleration": acceleration, "consecutive": consecutive, "direction": direction}


# ── Regime detection ──────────────────────────────────────────────────────────

REGIME_LABELS = {
    "risk_on":  "RISK-ON  ▲",
    "risk_off": "RISK-OFF ▼",
    "high_vol": "HIGH-VOL ~",
    "low_vol":  "LOW-VOL  —",
    "neutral":  "NEUTRAL  →",
}

def detect_regime(scores_cache: dict) -> dict:
    # fear/greed composite
    weights = {"rates": 0.25, "growth": 0.20, "employment": 0.15,
               "inflation": 0.15, "credit": 0.15, "trade": 0.10}
    total_w, total_s = 0.0, 0.0
    for theme, w in weights.items():
        s = scores_cache.get(theme, {}).get("1h", {}).get("score")
        if s is not None:
            total_s += s * w
            total_w += w
    fg_val = round((total_s / total_w) * 100) if total_w > 0 else 0

    # 7-day global std
    cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key='GLOBAL' AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (cutoff_7d,)).fetchall()
    hist = [r[0] for r in rows]
    n    = len(hist)
    if n < 4:
        return {"regime": "neutral", "label": REGIME_LABELS["neutral"],
                "confidence": 0.0, "signals": ["insufficient history"]}

    mean_sent = sum(hist) / n
    std_sent  = math.sqrt(sum((s - mean_sent)**2 for s in hist) / n)

    mom = compute_sentiment_momentum("GLOBAL")
    accel       = mom["acceleration"] or 0.0
    consecutive = mom["consecutive"]

    d          = scores_cache.get("GLOBAL", {})
    n1h        = d.get("1h",  {}).get("count", 0) or 0
    n24h       = d.get("24h", {}).get("count", 0) or 0
    hourly_avg = n24h / 24 if n24h else 0
    vel_ratio  = n1h / hourly_avg if hourly_avg > 0 else 1.0

    signals, score_vec = [], 0.0

    if fg_val >= 15:
        signals.append(f"FG:+{fg_val} greed"); score_vec += 1.5
    elif fg_val <= -15:
        signals.append(f"FG:{fg_val} fear");   score_vec -= 1.5
    else:
        signals.append(f"FG:{fg_val} neutral")

    if std_sent > 0.12:
        signals.append(f"vol:HIGH σ={std_sent:.3f}"); score_vec *= 0.5
    else:
        signals.append(f"vol:low σ={std_sent:.3f}")

    if accel > 0.05:
        signals.append(f"accel:↑{accel:+.3f}"); score_vec += 1.0
    elif accel < -0.05:
        signals.append(f"accel:↓{accel:+.3f}"); score_vec -= 1.0

    if vel_ratio > 2.5:
        signals.append(f"velocity:HOT {vel_ratio:.1f}x"); score_vec *= 1.3

    if consecutive >= 3:
        signals.append(f"streak:{consecutive}x {mom['direction']}")

    if std_sent > 0.15 and vel_ratio > 2.0:
        regime, confidence = "high_vol", min(0.5 + std_sent, 0.95)
    elif score_vec >= 1.5:
        regime, confidence = "risk_on",  min(0.4 + abs(score_vec) * 0.1, 0.92)
    elif score_vec <= -1.5:
        regime, confidence = "risk_off", min(0.4 + abs(score_vec) * 0.1, 0.92)
    elif std_sent < 0.05 and abs(fg_val) < 10:
        regime, confidence = "low_vol",  0.6
    else:
        regime, confidence = "neutral",  0.4

    return {
        "regime": regime, "label": REGIME_LABELS[regime],
        "confidence": round(confidence, 2), "signals": signals,
        "fg_val": fg_val, "std_sent": round(std_sent, 4), "accel": round(accel, 4),
    }


# ── Bayesian source confidence ────────────────────────────────────────────────

def compute_bayesian_signal(ticker: str) -> dict:
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    with get_db() as db:
        recent = db.execute("""
            SELECT a.score, a.source, a.ingested
            FROM articles a
            JOIN entity_mentions em ON em.article_id = a.id
            WHERE em.ticker=? AND a.ingested > ?
            ORDER BY a.ingested DESC LIMIT 50
        """, (ticker, cutoff_24h)).fetchall()

        if not recent:
            return {"direction": None, "confidence": 0.0,
                    "sources_used": 0, "signal_str": "no data"}

        source_acc = {}
        for src in {r[1] for r in recent if r[1]}:
            src_rows = db.execute("""
                SELECT a.score, a.ingested
                FROM articles a
                JOIN entity_mentions em ON em.article_id = a.id
                WHERE em.ticker=? AND a.source=? AND a.ingested > ?
                ORDER BY a.ingested ASC
            """, (ticker, src, cutoff_30d)).fetchall()

            correct, total = 0, 0
            for score, ing_ts in src_rows:
                try:
                    target_ts = (datetime.fromisoformat(ing_ts.replace("Z",""))
                                 + timedelta(hours=1)).isoformat()
                    p = db.execute("""
                        SELECT change_pct FROM market_prices
                        WHERE ticker=? AND ts >= ?
                        ORDER BY ts ASC LIMIT 1
                    """, (ticker, target_ts)).fetchone()
                    if p:
                        total += 1
                        if (score > 0.02) == (p[0] > 0):
                            correct += 1
                except Exception:
                    pass
            source_acc[src] = round((correct + 1) / (total + 2) if total >= 3 else 0.52, 3)

    log_odds, sources_used = 0.0, 0
    for score, src, _ in recent:
        if abs(score) < 0.03:
            continue
        acc = source_acc.get(src, 0.52)
        lo  = math.log((acc + 1e-9) / (1 - acc + 1e-9)) if score > 0 else \
              math.log((1 - acc + 1e-9) / (acc + 1e-9))
        log_odds     += lo * min(abs(score) * 2, 1.0)
        sources_used += 1

    prob_bull = 1 / (1 + math.exp(-log_odds))
    if prob_bull >= 0.60:
        direction, confidence = "BULL", prob_bull
    elif prob_bull <= 0.40:
        direction, confidence = "BEAR", 1 - prob_bull
    else:
        direction, confidence = "MIXED", 0.5

    top_sources = sorted(source_acc.items(), key=lambda x: abs(x[1]-0.5), reverse=True)[:2]
    src_str     = " ".join(f"{s}:{a:.0%}" for s, a in top_sources)
    signal_str  = f"{direction} {confidence:.0%} ({sources_used} arts, {src_str})"

    return {
        "direction": direction, "confidence": round(confidence, 3),
        "sources_used": sources_used, "source_acc": source_acc,
        "signal_str": signal_str,
    }
