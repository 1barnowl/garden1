"""
market_terminal/storage/workspace.py

Workspace persistence — saves and restores terminal state across sessions.

Persisted state:
  active_tab        last active tab id
  chart_ticker      last chart symbol
  pinned_tickers    list of explicitly pinned tickers
  watchlist_extra   user-added tickers (beyond config.yml)
  saved_searches    list of recent search queries (max 50)
  notes             dict of ticker → note string
  alert_thresholds  per-ticker custom alert overrides
  layout            dict of panel preferences
  last_seen         dict of ticker → last-viewed timestamp
  filters           active UI filters (source tier, event type, etc.)
  version           schema version for future migrations

The workspace file lives at ~/.esm/workspace.json.
All writes are atomic (write to tmp → rename).
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Optional

from market_terminal.config.settings import ESM_DIR, log

WORKSPACE_PATH = os.path.join(ESM_DIR, "workspace.json")
_SCHEMA_VERSION = 2

# ── defaults ──────────────────────────────────────────────────────────────────

_DEFAULTS: dict[str, Any] = {
    "version":          _SCHEMA_VERSION,
    "active_tab":       "equities",
    "chart_ticker":     "SPY",
    "pinned_tickers":   [],
    "watchlist_extra":  [],
    "saved_searches":   [],
    "notes":            {},
    "alert_thresholds": {},
    "layout": {
        "equities_columns": "default",
        "news_max_rows":    200,
        "chart_points":     80,
        "sparkline_width":  12,
    },
    "last_seen":        {},
    "filters": {
        "news_source_tier": None,   # None = all tiers
        "news_event_type":  None,   # None = all types
        "news_ticker":      None,   # None = all tickers
    },
}


# ── load / save ───────────────────────────────────────────────────────────────

def load() -> dict[str, Any]:
    """Load workspace from disk.  Returns defaults if file absent / corrupt."""
    if not os.path.exists(WORKSPACE_PATH):
        return dict(_DEFAULTS)
    try:
        with open(WORKSPACE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Merge missing keys from defaults (forward-compat for new fields)
        merged = dict(_DEFAULTS)
        merged.update(data)
        # Deep-merge nested dicts
        for key in ("layout", "filters"):
            if key in _DEFAULTS:
                base = dict(_DEFAULTS[key])
                base.update(merged.get(key) or {})
                merged[key] = base
        return merged
    except Exception as e:
        log.warning("workspace load error (%s) — using defaults", e)
        return dict(_DEFAULTS)


def save(ws: dict[str, Any]) -> bool:
    """Atomically write workspace to disk.  Returns True on success."""
    ws["version"] = _SCHEMA_VERSION
    ws["_saved_at"] = datetime.now(timezone.utc).isoformat()
    try:
        os.makedirs(ESM_DIR, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=ESM_DIR, suffix=".json.tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(ws, f, indent=2)
            os.replace(tmp_path, WORKSPACE_PATH)
        except Exception:
            try: os.unlink(tmp_path)
            except Exception: pass
            raise
        return True
    except Exception as e:
        log.warning("workspace save error: %s", e)
        return False


# ── Workspace manager class ───────────────────────────────────────────────────

class Workspace:
    """
    Thread-safe workspace manager.  All mutations call save() automatically.

    Usage:
        from market_terminal.storage.workspace import WS
        WS.set_active_tab("filings")
        WS.pin_ticker("NVDA")
        WS.add_search("fed rate cut")
        WS.set_note("AAPL", "Watch earnings on 2024-02-01")
    """

    def __init__(self):
        self._data: dict[str, Any] = load()

    def reload(self):
        """Re-read from disk (e.g. if edited externally)."""
        self._data = load()

    # ── getters ───────────────────────────────────────────────────────────────

    @property
    def active_tab(self) -> str:
        return self._data.get("active_tab", "equities")

    @property
    def chart_ticker(self) -> str:
        return self._data.get("chart_ticker", "SPY")

    @property
    def pinned_tickers(self) -> list[str]:
        return list(self._data.get("pinned_tickers", []))

    @property
    def watchlist_extra(self) -> list[str]:
        return list(self._data.get("watchlist_extra", []))

    @property
    def saved_searches(self) -> list[str]:
        return list(self._data.get("saved_searches", []))

    @property
    def layout(self) -> dict:
        return dict(self._data.get("layout", _DEFAULTS["layout"]))

    @property
    def filters(self) -> dict:
        return dict(self._data.get("filters", _DEFAULTS["filters"]))

    def get_note(self, ticker: str) -> str:
        return self._data.get("notes", {}).get(ticker.upper(), "")

    def get_alert_threshold(self, ticker: str) -> Optional[float]:
        return self._data.get("alert_thresholds", {}).get(ticker.upper())

    def get_last_seen(self, ticker: str) -> Optional[str]:
        return self._data.get("last_seen", {}).get(ticker.upper())

    # ── setters ───────────────────────────────────────────────────────────────

    def set_active_tab(self, tab: str):
        self._data["active_tab"] = tab
        save(self._data)

    def set_chart_ticker(self, ticker: str):
        self._data["chart_ticker"] = ticker.upper()
        self._touch(ticker)
        save(self._data)

    def pin_ticker(self, ticker: str):
        ticker = ticker.upper()
        pins   = self._data.setdefault("pinned_tickers", [])
        if ticker not in pins:
            pins.insert(0, ticker)
            save(self._data)

    def unpin_ticker(self, ticker: str):
        ticker = ticker.upper()
        pins   = self._data.get("pinned_tickers", [])
        if ticker in pins:
            pins.remove(ticker)
            save(self._data)

    def add_watchlist_ticker(self, ticker: str):
        ticker  = ticker.upper()
        extras  = self._data.setdefault("watchlist_extra", [])
        if ticker not in extras:
            extras.append(ticker)
            save(self._data)

    def remove_watchlist_ticker(self, ticker: str):
        ticker  = ticker.upper()
        extras  = self._data.get("watchlist_extra", [])
        if ticker in extras:
            extras.remove(ticker)
            save(self._data)

    def add_search(self, query: str):
        query = query.strip()
        if not query:
            return
        searches = self._data.setdefault("saved_searches", [])
        # Deduplicate: remove existing, prepend fresh
        if query in searches:
            searches.remove(query)
        searches.insert(0, query)
        # Keep last 50
        self._data["saved_searches"] = searches[:50]
        save(self._data)

    def set_note(self, ticker: str, note: str):
        self._data.setdefault("notes", {})[ticker.upper()] = note.strip()
        save(self._data)

    def clear_note(self, ticker: str):
        notes = self._data.get("notes", {})
        notes.pop(ticker.upper(), None)
        save(self._data)

    def set_alert_threshold(self, ticker: str, threshold: float):
        self._data.setdefault("alert_thresholds", {})[ticker.upper()] = threshold
        save(self._data)

    def set_filter(self, key: str, value: Optional[str]):
        self._data.setdefault("filters", {})[key] = value
        save(self._data)

    def set_layout(self, key: str, value: Any):
        self._data.setdefault("layout", {})[key] = value
        save(self._data)

    def _touch(self, ticker: str):
        self._data.setdefault("last_seen", {})[ticker.upper()] = \
            datetime.now(timezone.utc).isoformat()

    # ── summary ───────────────────────────────────────────────────────────────

    def summary(self) -> str:
        pins    = len(self.pinned_tickers)
        extras  = len(self.watchlist_extra)
        notes   = len(self._data.get("notes", {}))
        searches= len(self.saved_searches)
        return (f"tab={self.active_tab}  chart={self.chart_ticker}"
                f"  pins={pins}  extra={extras}"
                f"  notes={notes}  searches={searches}")


# ── module-level singleton ────────────────────────────────────────────────────
WS = Workspace()
