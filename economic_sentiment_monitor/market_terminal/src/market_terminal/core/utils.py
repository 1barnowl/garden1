"""
market_terminal/core/utils.py
Shared utility functions.  No heavy dependencies — importable anywhere.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Optional


# ── Timestamps ────────────────────────────────────────────────────────────────

def ts_now() -> str:
    """Current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def ts_to_dt(ts: str) -> datetime:
    """Parse any ISO-8601 / RFC-3339 string into a UTC-aware datetime."""
    ts = ts.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        # e.g. "Mon, 01 Jan 2024 12:00:00 GMT" from RSS
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def age_seconds(ts: str) -> float:
    """Seconds since the given ISO timestamp."""
    try:
        return (datetime.now(timezone.utc) - ts_to_dt(ts)).total_seconds()
    except Exception:
        return float("inf")


def is_stale(ts: str, threshold_seconds: int = 120) -> bool:
    """Return True if the timestamp is older than threshold_seconds."""
    return age_seconds(ts) > threshold_seconds


# ── Identifiers ───────────────────────────────────────────────────────────────

def dedup_id(*parts: str) -> str:
    """Deterministic MD5 hex digest from one or more string parts."""
    return hashlib.md5("".join(parts).encode()).hexdigest()


# ── Numerics ──────────────────────────────────────────────────────────────────

def safe_float(value, default: float = 0.0) -> float:
    """Parse a value to float, returning default on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def pct_change(new: float, old: float) -> float:
    """Percentage change from old to new; 0 if old is zero."""
    if old == 0:
        return 0.0
    return round((new - old) / abs(old) * 100, 4)


# ── Symbols ───────────────────────────────────────────────────────────────────

_SYMBOL_RE = re.compile(r"[^A-Z0-9=\^.\-]")

def normalize_ticker(ticker: str) -> str:
    """Upper-case and strip illegal chars from a ticker string."""
    return _SYMBOL_RE.sub("", ticker.upper().strip())


# ── Text ─────────────────────────────────────────────────────────────────────

def truncate(text: str, max_len: int = 200, ellipsis: str = "…") -> str:
    """Truncate text to max_len characters."""
    if not text:
        return ""
    text = text.strip()
    return text[:max_len - len(ellipsis)] + ellipsis if len(text) > max_len else text


def clean_text(text: str) -> str:
    """Collapse whitespace and strip control chars."""
    return re.sub(r"\s+", " ", (text or "").strip())


# ── Data quality ──────────────────────────────────────────────────────────────

def staleness_label(ts: Optional[str], warn_s: int = 60, crit_s: int = 300) -> str:
    """
    Return a short human-readable staleness indicator.
    '✓ live'  — fresh
    '⚠ 2m'    — warn
    '✗ 12m'   — critical
    '?'        — unknown
    """
    if not ts:
        return "?"
    try:
        age = age_seconds(ts)
    except Exception:
        return "?"
    if age < warn_s:
        return "✓ live"
    mins = int(age // 60)
    if age < crit_s:
        return f"⚠ {mins}m"
    return f"✗ {mins}m"


# ── ASCII chart helpers ────────────────────────────────────────────────────────

_SPARK_BLOCKS = " ▁▂▃▄▅▆▇█"

def sparkline(values: list[float], width: int = 12) -> str:
    """Unicode block sparkline from a list of floats."""
    if not values:
        return "─" * width
    tail = values[-width:]
    if len(tail) < 2:
        return "─" * width
    mn, mx = min(tail), max(tail)
    span = mx - mn
    if span == 0:
        return "▄" * len(tail) + "─" * (width - len(tail))
    chars = [
        _SPARK_BLOCKS[max(0, min(int((v - mn) / span * (len(_SPARK_BLOCKS) - 1)),
                                  len(_SPARK_BLOCKS) - 1))]
        for v in tail
    ]
    return "".join(chars).ljust(width, "─")


def ascii_chart(values: list[float], width: int = 60, height: int = 8,
                label: str = "") -> str:
    """
    Render a simple ASCII price/score chart.
    Returns a multi-line string ready for a Textual Static widget.
    """
    if len(values) < 2:
        return "(no data)"

    # Downsample to width
    step  = max(1, len(values) // width)
    data  = values[::step][-width:]
    mn    = min(data)
    mx    = max(data)
    span  = mx - mn or 1e-9

    rows = []
    for row in range(height, 0, -1):
        threshold = mn + (row / height) * span
        line = ""
        for v in data:
            if v >= threshold:
                line += "█"
            elif v >= threshold - span / height:
                line += "▄"
            else:
                line += " "
        # y-axis label on right edge
        y_val = mn + (row / height) * span
        rows.append(f"{y_val:+.3f} │{line}│")

    rows.append("       └" + "─" * len(data) + "┘")
    if label:
        rows.append("        " + label[:len(data)])
    return "\n".join(rows)


def sentiment_bar(score: float, width: int = 12) -> str:
    """
    Visual bar: negative fills left of centre, positive fills right.
      -1.0  ████████│────────   0.0  ────────│────────  +1.0  ────────│████████
    """
    half   = width // 2
    filled = min(int(abs(score) * half), half)
    if score >= 0.05:
        return "─" * half + "│" + "█" * filled + "─" * (half - filled)
    elif score <= -0.05:
        return "─" * (half - filled) + "█" * filled + "│" + "─" * half
    return "─" * half + "│" + "─" * half
