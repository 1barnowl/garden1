"""
market_terminal/enrich/classifier.py

Two-stage event type classifier.

Stage 1 (always on): deterministic keyword/pattern rules.
Stage 2 (optional):  lightweight sklearn LogisticRegression trained on
                     a small labeled seed set.  Activates automatically
                     once sklearn is importable.

Event types (mirrors Bloomberg-style event taxonomy):
  earnings        — quarterly/annual results, EPS, revenue beats/misses
  guidance        — forward guidance, outlook, raised/lowered forecasts
  merger_acq      — M&A, acquisition, merger, takeover, buyout
  analyst_action  — upgrade, downgrade, price target, initiation
  macro_release   — CPI, GDP, jobs report, FOMC, Fed decision
  regulatory      — SEC action, DOJ, antitrust, fine, settlement, probe
  product_launch  — new product, launch, release, partnership
  supply_chain    — supply disruption, shortage, tariff impact, logistics
  filing          — 10-K, 10-Q, 8-K, proxy, annual report
  insider_trade   — insider buy/sell, Form 4
  dividend        — dividend declared/cut/raised, buyback, special dividend
  leadership      — CEO change, resignation, appointment, board change
  rumor           — rumor, report suggests, sources say, unconfirmed
  general         — fallback / unclassified
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


# ── Event type constants ──────────────────────────────────────────────────────

class EventType:
    EARNINGS       = "earnings"
    GUIDANCE       = "guidance"
    MERGER_ACQ     = "merger_acq"
    ANALYST_ACTION = "analyst_action"
    MACRO_RELEASE  = "macro_release"
    REGULATORY     = "regulatory"
    PRODUCT_LAUNCH = "product_launch"
    SUPPLY_CHAIN   = "supply_chain"
    FILING         = "filing"
    INSIDER_TRADE  = "insider_trade"
    DIVIDEND       = "dividend"
    LEADERSHIP     = "leadership"
    RUMOR          = "rumor"
    GENERAL        = "general"

    ALL = [
        EARNINGS, GUIDANCE, MERGER_ACQ, ANALYST_ACTION, MACRO_RELEASE,
        REGULATORY, PRODUCT_LAUNCH, SUPPLY_CHAIN, FILING, INSIDER_TRADE,
        DIVIDEND, LEADERSHIP, RUMOR, GENERAL,
    ]


# ── Rule definitions ──────────────────────────────────────────────────────────
# Each rule is (event_type, weight, pattern_list).
# Patterns are lowercased substring matches.  Weight determines priority
# when multiple rules fire (highest wins).

_RULES: list[tuple[str, float, list[str]]] = [

    (EventType.EARNINGS, 0.95, [
        "earnings", "eps", "quarterly results", "q1 result", "q2 result",
        "q3 result", "q4 result", "annual result", "revenue beat",
        "revenue miss", "profit beat", "profit miss", "net income",
        "operating income", "beats estimate", "misses estimate",
        "beats consensus", "below consensus", "above consensus",
        "quarterly profit", "quarterly loss", "fiscal year",
    ]),

    (EventType.GUIDANCE, 0.92, [
        "guidance", "outlook", "forecast", "raises guidance", "lowers guidance",
        "reaffirms guidance", "full year outlook", "next quarter outlook",
        "revenue guidance", "earnings guidance", "expects revenue",
        "projects earnings", "revised forecast", "forward guidance",
        "raised its outlook", "lowered its outlook",
    ]),

    (EventType.MERGER_ACQ, 0.93, [
        "acquires", "acquisition", "merger", "takeover", "buyout",
        "deal worth", "agreed to buy", "to acquire", "purchase agreement",
        "tender offer", "hostile bid", "strategic combination",
        "m&a", "joint venture", "spinoff", "spin-off", "divestiture",
        "divest", "sells unit", "carve-out",
    ]),

    (EventType.ANALYST_ACTION, 0.90, [
        "upgrade", "downgrade", "initiated coverage", "price target",
        "raises price target", "lowers price target", "buy rating",
        "sell rating", "hold rating", "neutral rating", "overweight",
        "underweight", "outperform", "underperform", "strong buy",
        "analyst cuts", "analyst raises", "wall street",
    ]),

    (EventType.MACRO_RELEASE, 0.94, [
        "cpi", "consumer price index", "pce", "gdp", "unemployment rate",
        "nonfarm payroll", "jobs report", "fomc", "federal reserve",
        "interest rate decision", "rate hike", "rate cut", "fed funds",
        "inflation data", "retail sales", "industrial production",
        "trade balance", "pmi", "ism", "housing starts", "durable goods",
        "consumer sentiment", "jolts", "beige book",
    ]),

    (EventType.REGULATORY, 0.91, [
        "sec charges", "doj investigation", "antitrust", "ftc probe",
        "regulatory", "fine", "settlement", "lawsuit", "litigation",
        "class action", "subpoena", "investigation", "compliance",
        "sanctions", "ban", "penaliz", "criminal charges", "indicted",
        "consent decree", "cease and desist",
    ]),

    (EventType.PRODUCT_LAUNCH, 0.88, [
        "launches", "unveils", "announces new", "new product", "release",
        "partnership", "collaboration", "signs deal", "new model",
        "product line", "new service", "new platform", "introduces",
        "rolls out", "debuts", "available now", "ships",
    ]),

    (EventType.SUPPLY_CHAIN, 0.87, [
        "supply chain", "shortage", "disruption", "tariff", "chip shortage",
        "logistics", "inventory", "backlog", "delivery delay", "production cut",
        "factory shutdown", "plant closure", "recall",
        "import ban", "export restriction", "supplier",
    ]),

    (EventType.FILING, 0.96, [
        "10-k", "10-q", "8-k", "proxy statement", "def 14a",
        "annual report", "sec filing", "edgar", "form 4",
        "quarterly report", "filed with the sec",
    ]),

    (EventType.INSIDER_TRADE, 0.89, [
        "insider", "form 4", "ceo buys", "ceo sells", "executive buys",
        "director buys", "director sells", "insider buying", "insider selling",
        "10b5-1", "stock purchase plan", "shares sold by",
    ]),

    (EventType.DIVIDEND, 0.90, [
        "dividend", "buyback", "share repurchase", "special dividend",
        "dividend cut", "dividend raise", "quarterly dividend",
        "cash distribution", "payout ratio", "yield",
    ]),

    (EventType.LEADERSHIP, 0.92, [
        "ceo", "chief executive", "resigns", "resignation", "appointed",
        "names new", "steps down", "replaced", "leadership change",
        "board of directors", "chairman", "cfo", "coo", "cto",
        "new president", "executive change",
    ]),

    (EventType.RUMOR, 0.86, [
        "rumor", "report suggests", "sources say", "sources familiar",
        "people familiar", "unconfirmed", "reportedly", "said to be",
        "may be considering", "could be", "mulling", "exploring",
        "in talks to", "weighing", "sources say company", "said to be considering",
    ]),
]

# Pre-compile patterns for speed
_COMPILED: list[tuple[str, float, list[re.Pattern]]] = [
    (etype, weight,
     [re.compile(re.escape(p), re.IGNORECASE) for p in patterns])
    for etype, weight, patterns in _RULES
]


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ClassificationResult:
    event_type:  str   = EventType.GENERAL
    confidence:  float = 0.0
    matched_on:  str   = ""
    all_scores:  dict  = field(default_factory=dict)


# ── Stage 1: rule-based classifier ───────────────────────────────────────────

def classify_rule_based(text: str) -> ClassificationResult:
    """
    Fast deterministic classifier.  Returns the highest-scoring event type.
    O(n_rules × n_patterns) but patterns are short and pre-compiled.
    """
    if not text:
        return ClassificationResult()

    text_lower = text.lower()
    scores: dict[str, float] = {}
    matches: dict[str, list[str]] = {}

    for etype, weight, compiled_pats in _COMPILED:
        for pat in compiled_pats:
            m = pat.search(text_lower)
            if m:
                hit = m.group(0)
                if etype not in scores or weight > scores[etype]:
                    scores[etype] = weight
                matches.setdefault(etype, []).append(hit)

    if not scores:
        return ClassificationResult(event_type=EventType.GENERAL,
                                    confidence=0.3,
                                    all_scores={EventType.GENERAL: 0.3})

    best_type = max(scores, key=lambda k: scores[k])
    best_conf = scores[best_type]
    matched   = ", ".join(matches.get(best_type, [])[:3])

    return ClassificationResult(
        event_type  = best_type,
        confidence  = round(best_conf, 3),
        matched_on  = matched,
        all_scores  = scores,
    )


# ── Stage 2: optional ML refinement ──────────────────────────────────────────
# Seed data — small labeled examples so the model boots without external data.
# Add more over time; the model improves with each addition.

_SEED_EXAMPLES: list[tuple[str, str]] = [
    # earnings
    ("Apple reports record quarterly earnings, beats EPS estimates", EventType.EARNINGS),
    ("Microsoft Q2 revenue misses Wall Street consensus by 3%", EventType.EARNINGS),
    ("Tesla quarterly profit falls short of expectations", EventType.EARNINGS),
    ("Amazon net income doubled year-over-year in latest quarter", EventType.EARNINGS),
    # guidance
    ("Nike raises full-year revenue guidance on strong demand", EventType.GUIDANCE),
    ("Intel lowers earnings outlook citing weak PC market", EventType.GUIDANCE),
    ("Salesforce reaffirms annual guidance despite macro headwinds", EventType.GUIDANCE),
    # merger_acq
    ("Microsoft agreed to acquire Activision for $68.7 billion", EventType.MERGER_ACQ),
    ("Exxon completes acquisition of Pioneer Natural Resources", EventType.MERGER_ACQ),
    ("Pfizer buys oncology startup in $10 billion deal", EventType.MERGER_ACQ),
    # analyst_action
    ("Goldman Sachs upgrades Apple to Buy with $220 price target", EventType.ANALYST_ACTION),
    ("Morgan Stanley downgrades Tesla, lowers price target to $180", EventType.ANALYST_ACTION),
    ("JPMorgan initiates coverage of Nvidia with Overweight rating", EventType.ANALYST_ACTION),
    # macro_release
    ("CPI rose 3.2% in March, above economist forecasts", EventType.MACRO_RELEASE),
    ("Fed holds rates steady, signals two cuts in 2024", EventType.MACRO_RELEASE),
    ("Nonfarm payrolls add 275,000 jobs, beating expectations", EventType.MACRO_RELEASE),
    ("GDP growth slowed to 1.6% in first quarter", EventType.MACRO_RELEASE),
    # regulatory
    ("FTC sues Amazon over alleged antitrust violations", EventType.REGULATORY),
    ("SEC charges company with securities fraud", EventType.REGULATORY),
    ("EU fines Meta 1.3 billion euros for data privacy breach", EventType.REGULATORY),
    # product_launch
    ("Apple unveils new iPhone 16 lineup with AI features", EventType.PRODUCT_LAUNCH),
    ("Nvidia launches new H200 AI accelerator chip", EventType.PRODUCT_LAUNCH),
    ("Tesla rolls out new software update to all vehicles", EventType.PRODUCT_LAUNCH),
    # supply_chain
    ("TSMC warns of chip shortage extending into next year", EventType.SUPPLY_CHAIN),
    ("Port strike disrupts supply chain for automakers", EventType.SUPPLY_CHAIN),
    ("Boeing faces production delays due to parts shortage", EventType.SUPPLY_CHAIN),
    # filing
    ("Apple files annual 10-K report with the SEC", EventType.FILING),
    ("Tesla submits quarterly 10-Q showing improved margins", EventType.FILING),
    ("Company files 8-K disclosing material event", EventType.FILING),
    # insider_trade
    ("CEO sells 50,000 shares under 10b5-1 plan", EventType.INSIDER_TRADE),
    ("Insider buying surges as director acquires shares", EventType.INSIDER_TRADE),
    # dividend
    ("Apple raises quarterly dividend by 4 percent", EventType.DIVIDEND),
    ("Company announces $50 billion share buyback program", EventType.DIVIDEND),
    ("Firm cuts dividend amid cash flow concerns", EventType.DIVIDEND),
    # leadership
    ("CEO resigns amid accounting investigation", EventType.LEADERSHIP),
    ("Company names new CFO following leadership shakeup", EventType.LEADERSHIP),
    ("Board appoints veteran executive as president", EventType.LEADERSHIP),
    # rumor
    ("Sources say Apple is considering acquiring AI startup", EventType.RUMOR),
    ("Reportedly in talks to merge with rival, unconfirmed", EventType.RUMOR),
    # general
    ("Markets open higher following overnight session", EventType.GENERAL),
    ("Investors await key economic data this week", EventType.GENERAL),
]

_ML_MODEL  = None
_ML_VECTORIZER = None
_ML_READY  = False


def _try_load_ml():
    """Attempt to train the sklearn model from seed data."""
    global _ML_MODEL, _ML_VECTORIZER, _ML_READY
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.feature_extraction.text import TfidfVectorizer

        texts  = [t for t, _ in _SEED_EXAMPLES]
        labels = [l for _, l in _SEED_EXAMPLES]

        vect  = TfidfVectorizer(ngram_range=(1, 2), max_features=2000,
                                sublinear_tf=True)
        X     = vect.fit_transform(texts)
        model = LogisticRegression(max_iter=500, C=2.0, solver="lbfgs",
                                   multi_class="multinomial")
        model.fit(X, labels)

        _ML_VECTORIZER = vect
        _ML_MODEL      = model
        _ML_READY      = True
    except ImportError:
        pass   # sklearn not installed — rule-based only
    except Exception:
        pass


_try_load_ml()


def classify_ml(text: str) -> Optional[ClassificationResult]:
    """Return ML classification if sklearn is available, else None."""
    if not _ML_READY or not text:
        return None
    try:
        X      = _ML_VECTORIZER.transform([text])
        proba  = _ML_MODEL.predict_proba(X)[0]
        labels = _ML_MODEL.classes_
        top_i  = int(proba.argmax())
        return ClassificationResult(
            event_type = labels[top_i],
            confidence = round(float(proba[top_i]), 3),
            all_scores = {labels[i]: round(float(p), 3)
                          for i, p in enumerate(proba)},
        )
    except Exception:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def _has_strong_rumor(text: str) -> bool:
    """True when the text has two or more rumor-qualifier signals."""
    _QS = [
        'sources say', 'reportedly', 'said to be', 'people familiar',
        'sources familiar', 'unconfirmed', 'mulling', 'exploring a',
        'weighing a', 'may be considering',
    ]
    tl = text.lower()
    return sum(1 for q in _QS if q in tl) >= 2


def classify(text: str) -> ClassificationResult:
    """
    Classify a headline / article text into an EventType.

    Blends rule-based (always on) with ML (if sklearn installed).
    Rules win when confidence is high; ML refines uncertain cases.
    Rumor override: strong rumor qualifiers trump underlying event type.
    """
    rule_result = classify_rule_based(text)

    # Rumor override — 'sources say Apple is mulling acquisition' → RUMOR
    if _has_strong_rumor(text) and rule_result.event_type != EventType.RUMOR:
        return ClassificationResult(
            event_type  = EventType.RUMOR,
            confidence  = 0.88,
            matched_on  = f"rumor_override({rule_result.event_type})",
            all_scores  = {EventType.RUMOR: 0.88,
                           rule_result.event_type: rule_result.confidence},
        )

    # If rules are very confident, trust them
    if rule_result.confidence >= 0.90:
        return rule_result

    ml_result = classify_ml(text)
    if ml_result is None:
        return rule_result

    # Blend: if ML and rules agree, boost confidence
    if ml_result.event_type == rule_result.event_type:
        blended_conf = min(0.99, (rule_result.confidence + ml_result.confidence) / 2 + 0.05)
        rule_result.confidence = round(blended_conf, 3)
        return rule_result

    # If ML is very confident and disagrees with low-confidence rules, defer to ML
    if ml_result.confidence >= 0.75 and rule_result.confidence < 0.60:
        return ml_result

    # Otherwise rules win
    return rule_result


def classify_batch(texts: list[str]) -> list[ClassificationResult]:
    """Classify a list of texts efficiently."""
    return [classify(t) for t in texts]


def event_type_label(etype: str) -> str:
    """Short display label for an event type."""
    labels = {
        EventType.EARNINGS:       "EARN",
        EventType.GUIDANCE:       "GUID",
        EventType.MERGER_ACQ:     "M&A ",
        EventType.ANALYST_ACTION: "ANLYST",
        EventType.MACRO_RELEASE:  "MACRO",
        EventType.REGULATORY:     "REG ",
        EventType.PRODUCT_LAUNCH: "PROD",
        EventType.SUPPLY_CHAIN:   "SUPLY",
        EventType.FILING:         "FILNG",
        EventType.INSIDER_TRADE:  "INSD",
        EventType.DIVIDEND:       "DIV ",
        EventType.LEADERSHIP:     "MGMT",
        EventType.RUMOR:          "RUMR",
        EventType.GENERAL:        "    ",
    }
    return labels.get(etype, etype[:5].upper())
