"""
market_terminal/core/models.py

Canonical Pydantic v2 data models.  Every object crossing a module boundary
must be one of these types — no bare dicts in inter-module APIs.

Validation happens at ingestion time so bad data from messy feeds is caught
before it reaches the DB, analytics, or UI.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

try:
    from pydantic import BaseModel, Field, field_validator, model_validator
    _PYDANTIC = True
except ImportError:
    # Graceful degradation: fall back to plain dataclasses so the app still
    # runs without pydantic installed.  Validation is skipped.
    from dataclasses import dataclass as _dc, field as _field
    BaseModel = object          # type: ignore
    Field = lambda *a, **k: None  # type: ignore
    _PYDANTIC = False


# ── Enumerations ──────────────────────────────────────────────────────────────

class SentLabel(str, Enum):
    POS = "POS"
    NEG = "NEG"
    NEU = "NEU"

class ModelTier(str, Enum):
    VADER        = "vader"
    FINBERT_VADER = "finbert+vader"
    FRED_SYNTH   = "fred_surprise"

class SourceTier(str, Enum):
    A = "A"   # high-trust (Reuters, Bloomberg, FT, Fed)
    B = "B"   # medium-trust (CNBC, MarketWatch, BBC)
    C = "C"   # unknown / community

class FilingType(str, Enum):
    K10  = "10-K"
    Q10  = "10-Q"
    K8   = "8-K"
    DEF14A = "DEF 14A"
    SC13G  = "SC 13G"
    OTHER  = "OTHER"

class Regime(str, Enum):
    RISK_ON  = "risk_on"
    RISK_OFF = "risk_off"
    HIGH_VOL = "high_vol"
    LOW_VOL  = "low_vol"
    NEUTRAL  = "neutral"

class AlertSeverity(str, Enum):
    WARN = "WARN"
    CRIT = "CRIT"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _make_id(*parts: str) -> str:
    return hashlib.md5("".join(parts).encode()).hexdigest()


# ── Instrument (security master) ──────────────────────────────────────────────

if _PYDANTIC:
    class Instrument(BaseModel):
        """Canonical security descriptor — the 'security master' entry."""
        symbol:   str                        # primary key / ticker
        name:     str       = ""
        exchange: str       = ""
        currency: str       = "USD"
        sector:   str       = ""
        figi:     str       = ""             # FIGI if available
        asset_class: str    = "equity"       # equity / fx / crypto / bond / commodity

        @field_validator("symbol")
        @classmethod
        def symbol_upper(cls, v: str) -> str:
            return v.strip().upper()


    # ── Market data ───────────────────────────────────────────────────────────

    class PriceBar(BaseModel):
        """One OHLCV bar for an instrument."""
        symbol:     str
        ts:         datetime
        open:       float
        high:       float
        low:        float
        close:      float
        volume:     float   = 0.0
        vwap:       float   = 0.0
        change_pct: float   = 0.0

        @field_validator("high")
        @classmethod
        def high_gte_low(cls, v: float, info: Any) -> float:
            lo = (info.data or {}).get("low", v)
            return max(v, lo)

        @property
        def bar_id(self) -> str:
            return _make_id(self.symbol, self.ts.isoformat())


    # ── News ──────────────────────────────────────────────────────────────────

    class NewsItem(BaseModel):
        """A single enriched news article / headline."""
        id:             str       = ""
        title:          str
        source:         str       = ""
        url:            str       = ""
        published:      str       = ""
        ingested:       datetime  = Field(default_factory=_utcnow)

        # Sentiment
        score:          float     = 0.0
        vader_score:    Optional[float] = None
        finbert_score:  Optional[float] = None
        label:          SentLabel = SentLabel.NEU
        confidence:     float     = 0.0
        model_tier:     ModelTier = ModelTier.VADER

        # Enrichment
        keywords:       list[str] = Field(default_factory=list)
        entities:       list[dict] = Field(default_factory=list)
        numeric_signals: dict     = Field(default_factory=dict)
        source_tier:    SourceTier = SourceTier.C

        # Full text
        full_text:      bool      = False
        text_length:    int       = 0

        @model_validator(mode="after")
        def generate_id(self) -> "NewsItem":
            if not self.id:
                self.id = _make_id(self.title, self.url)
            return self

        @field_validator("score", "vader_score", "finbert_score", mode="before")
        @classmethod
        def clamp_score(cls, v):
            if v is None:
                return v
            return max(-1.0, min(1.0, float(v)))

        @field_validator("title")
        @classmethod
        def strip_title(cls, v: str) -> str:
            return (v or "").strip()[:512]


    # ── SEC Filings ───────────────────────────────────────────────────────────

    class FilingDoc(BaseModel):
        """An SEC EDGAR filing linked to an instrument."""
        id:           str       = ""
        ticker:       str
        company:      str       = ""
        form_type:    FilingType = FilingType.OTHER
        filed_at:     str       = ""          # EDGAR filing date (ISO)
        period:       str       = ""          # period of report
        url:          str       = ""          # EDGAR filing index URL
        description:  str       = ""
        ingested:     datetime  = Field(default_factory=_utcnow)

        # NLP enrichment (populated after indexing)
        summary:      str       = ""
        sentiment:    float     = 0.0
        keywords:     list[str] = Field(default_factory=list)

        @model_validator(mode="after")
        def generate_id(self) -> "FilingDoc":
            if not self.id:
                self.id = _make_id(self.ticker, self.form_type, self.filed_at, self.url)
            return self

        @field_validator("ticker")
        @classmethod
        def ticker_upper(cls, v: str) -> str:
            return v.strip().upper()


    # ── Macro release ─────────────────────────────────────────────────────────

    class MacroRelease(BaseModel):
        """An economic data observation from FRED or similar."""
        series_id:  str
        label:      str
        theme:      str
        ts:         str         # observation date (YYYY-MM-DD)
        value:      float
        prev_value: float
        surprise:   float       = 0.0
        units:      str         = ""

        @property
        def release_id(self) -> str:
            return _make_id(self.series_id, self.ts)


    # ── Alerts ────────────────────────────────────────────────────────────────

    class AlertEvent(BaseModel):
        """A generated alert with severity, key, and reason."""
        id:       str           = ""
        ts:       datetime      = Field(default_factory=_utcnow)
        severity: AlertSeverity = AlertSeverity.WARN
        key:      str           = ""
        reason:   str           = ""
        score:    float         = 0.0

        @model_validator(mode="after")
        def generate_id(self) -> "AlertEvent":
            if not self.id:
                self.id = _make_id(
                    self.key,
                    str(round(self.score, 2)),
                    self.ts.date().isoformat(),
                )
            return self


    # ── Entity mention ────────────────────────────────────────────────────────

    class EntityMention(BaseModel):
        """A ticker linked to a specific article."""
        article_id:  str
        entity_raw:  str
        ticker:      str
        score:       float
        ts:          datetime = Field(default_factory=_utcnow)


    # ── Signal outcome (for backtesting) ──────────────────────────────────────

    class SignalOutcome(BaseModel):
        """Records whether a sentiment signal was directionally correct."""
        id:          str    = ""
        ticker:      str
        signal_ts:   datetime
        signal_dir:  str    = ""   # BULL / BEAR / MIXED
        signal_conf: float  = 0.0
        source:      str    = ""
        price_at:    float  = 0.0
        price_1h:    Optional[float] = None
        price_4h:    Optional[float] = None
        price_24h:   Optional[float] = None
        correct_1h:  Optional[bool]  = None
        correct_4h:  Optional[bool]  = None
        correct_24h: Optional[bool]  = None
        resolved:    bool   = False

        @model_validator(mode="after")
        def generate_id(self) -> "SignalOutcome":
            if not self.id:
                self.id = _make_id(self.ticker, self.signal_ts.isoformat(), self.source)
            return self

else:
    # ── Minimal fallback stubs when pydantic is absent ─────────────────────
    class _Stub:
        def __init__(self, **kw): self.__dict__.update(kw)
        @classmethod
        def model_validate(cls, d): return cls(**d)

    Instrument    = _Stub
    PriceBar      = _Stub
    NewsItem      = _Stub
    FilingDoc     = _Stub
    MacroRelease  = _Stub
    AlertEvent    = _Stub
    EntityMention = _Stub
    SignalOutcome = _Stub


PYDANTIC_AVAILABLE = _PYDANTIC
