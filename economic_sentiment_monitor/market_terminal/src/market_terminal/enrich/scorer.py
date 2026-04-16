"""
market_terminal/enrich/scorer.py
Two-tier NLP scoring: VADER (fast, always on) + FinBERT (accurate, optional).
Blend: 0.3 * VADER + 0.7 * FinBERT when both available.
"""

from market_terminal.config.settings import log

# ── VADER ─────────────────────────────────────────────────────────────────────
VADER = None
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER = SentimentIntensityAnalyzer()
    log.info("VADER loaded")
except ImportError:
    log.warning("vaderSentiment not installed")

# ── FinBERT ───────────────────────────────────────────────────────────────────
FINBERT_PIPE  = None
FINBERT_READY = False
try:
    from transformers import pipeline as hf_pipeline
    log.info("Loading FinBERT (ProsusAI/finbert)…")
    FINBERT_PIPE = hf_pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert",
        top_k=None,
        device=-1,
        truncation=True,
        max_length=512,
    )
    FINBERT_READY = True
    log.info("FinBERT loaded")
except Exception as e:
    log.warning("FinBERT not available (%s) — VADER only", e)


def _vader_score(text: str) -> dict:
    if not VADER or not text:
        return {"score": 0.0, "label": "NEU", "confidence": 0.0}
    s   = VADER.polarity_scores(text[:1000])
    c   = s["compound"]
    lbl = "POS" if c >= 0.05 else ("NEG" if c <= -0.05 else "NEU")
    return {"score": round(c, 4), "label": lbl,
            "confidence": round(max(s["pos"], s["neg"], s["neu"]), 4)}


def _finbert_score(text: str):
    if not FINBERT_READY or not text:
        return None
    try:
        results   = FINBERT_PIPE(text[:512])[0]
        label_map = {r["label"].lower(): r["score"] for r in results}
        pos, neg  = label_map.get("positive", 0.0), label_map.get("negative", 0.0)
        neu       = label_map.get("neutral",  0.0)
        compound  = round(pos - neg, 4)
        lbl       = "POS" if compound >= 0.05 else ("NEG" if compound <= -0.05 else "NEU")
        return {"score": compound, "label": lbl,
                "confidence": round(max(pos, neg, neu), 4),
                "pos": round(pos, 4), "neg": round(neg, 4)}
    except Exception as e:
        log.debug("FinBERT error: %s", e)
        return None


def score_text(text: str) -> dict:
    """
    Returns:
        score, vader_score, finbert_score, label, confidence, model_tier
    """
    vader   = _vader_score(text)
    finbert = _finbert_score(text)
    if finbert:
        blended = round(0.3 * vader["score"] + 0.7 * finbert["score"], 4)
        lbl = "POS" if blended >= 0.05 else ("NEG" if blended <= -0.05 else "NEU")
        return {
            "score":         blended,
            "vader_score":   vader["score"],
            "finbert_score": finbert["score"],
            "label":         lbl,
            "confidence":    finbert["confidence"],
            "model_tier":    "finbert+vader",
        }
    return {
        "score":         vader["score"],
        "vader_score":   vader["score"],
        "finbert_score": None,
        "label":         vader["label"],
        "confidence":    vader["confidence"],
        "model_tier":    "vader",
    }
