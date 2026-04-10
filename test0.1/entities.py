"""
nlp/entities.py
Entity extraction (SpaCy NER + regex), keyword detection, numeric signal parsing.
"""

import re
import json
from config.settings import ENTITY_MAP, MACRO_KEYWORDS, HIGH_TRUST_SOURCES, MED_TRUST_SOURCES, log

# ── SpaCy ─────────────────────────────────────────────────────────────────────
NLP_SPACY   = None
SPACY_READY = False
try:
    import spacy
    for _model in ("en_core_web_sm", "en_core_web_md"):
        try:
            NLP_SPACY   = spacy.load(_model, disable=["parser", "lemmatizer"])
            SPACY_READY = True
            log.info("SpaCy model '%s' loaded", _model)
            break
        except OSError:
            continue
    if not SPACY_READY:
        log.warning("No SpaCy model found. Run: python3 -m spacy download en_core_web_sm --break-system-packages")
except ImportError:
    log.warning("SpaCy not installed")

# ── numeric signal regex ──────────────────────────────────────────────────────
_NUM_RE = re.compile(r'([+-]?\d+\.?\d*)\s*(%|bps|bp|billion|million|trillion)', re.I)
_BEARISH = ["fell","drop","declin","cut","reduc","lower","slump","plunge","crash","down"]
_BULLISH = ["rose","rise","increas","gain","hike","higher","surged","jump","up","climb"]


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


def extract_entities(text: str) -> list:
    """
    Returns list of {entity_raw, ticker}.
    Layer 1: SpaCy NER over ORG/GPE/PERSON/PRODUCT spans.
    Layer 2: regex string-scan of ENTITY_MAP (always runs).
    """
    found = {}
    if SPACY_READY and NLP_SPACY:
        doc = NLP_SPACY(text[:400])
        for ent in doc.ents:
            if ent.label_ in ("ORG", "GPE", "PERSON", "PRODUCT", "MONEY", "NORP"):
                ticker = ENTITY_MAP.get(_norm(ent.text))
                if ticker and ticker not in found:
                    found[ticker] = ent.text
    text_l = text.lower()
    for name, ticker in ENTITY_MAP.items():
        if name in text_l and ticker not in found:
            found[ticker] = name
    return [{"entity_raw": v, "ticker": k} for k, v in found.items()]


def extract_keywords(text: str) -> list:
    tl = text.lower()
    return [t for t, ws in MACRO_KEYWORDS.items() if any(w in tl for w in ws)]


def extract_numeric_signals(text: str) -> dict:
    """
    Extract percentage changes, basis point moves, and absolute values.
    Infers direction from surrounding context words.
    """
    signals = {}
    text_l  = text.lower()
    for m in _NUM_RE.finditer(text):
        val  = float(m.group(1))
        unit = m.group(2).lower()
        if unit == "%":
            if any(w in text_l for w in _BEARISH):
                val = -abs(val)
            elif any(w in text_l for w in _BULLISH):
                val = abs(val)
            signals["pct_change"] = round(val, 3)
        elif unit in ("bps", "bp"):
            if any(w in text_l for w in ["cut", "lower", "reduc", "ease"]):
                val = -abs(val)
            signals["bps_change"] = round(val, 1)
        elif unit in ("billion", "million", "trillion"):
            mult = {"billion": 1e9, "million": 1e6, "trillion": 1e12}[unit]
            signals["abs_value"] = round(val * mult, 0)
    return signals


def enrich_headline_metadata(art: dict) -> dict:
    """Add numeric_signals, source_tier, hl_words to article dict in-place."""
    title = art.get("title", "") or ""
    art["numeric_signals"] = json.dumps(extract_numeric_signals(title))
    src = art.get("source", "")
    if src in HIGH_TRUST_SOURCES:
        art["source_tier"] = "A"
    elif src in MED_TRUST_SOURCES:
        art["source_tier"] = "B"
    else:
        art["source_tier"] = "C"
    art["hl_words"] = len(title.split())
    return art
