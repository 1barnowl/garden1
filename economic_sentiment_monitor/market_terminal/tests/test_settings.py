"""tests/test_settings.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from market_terminal.config.settings import (
    WATCHLIST, RSS_FEEDS, FRED_SERIES, MACRO_KEYWORDS,
    ENTITY_MAP, HIGH_TRUST_SOURCES, MED_TRUST_SOURCES,
    ALERT_WARN, ALERT_CRIT, ALERT_MIN_COUNT,
)


def test_watchlist_non_empty():
    for category, tickers in WATCHLIST.items():
        assert len(tickers) > 0, f"Watchlist '{category}' is empty"


def test_rss_feeds_are_tuples():
    for item in RSS_FEEDS:
        assert isinstance(item, tuple) and len(item) == 2


def test_fred_series_are_triples():
    for item in FRED_SERIES:
        assert isinstance(item, tuple) and len(item) == 3


def test_macro_keywords_non_empty():
    assert len(MACRO_KEYWORDS) >= 6
    for theme, words in MACRO_KEYWORDS.items():
        assert len(words) > 0


def test_entity_map_values_are_strings():
    for k, v in ENTITY_MAP.items():
        assert isinstance(k, str) and isinstance(v, str)


def test_trust_sources_are_sets():
    assert isinstance(HIGH_TRUST_SOURCES, set)
    assert isinstance(MED_TRUST_SOURCES, set)


def test_alert_thresholds_positive():
    assert ALERT_WARN > 0
    assert ALERT_CRIT > ALERT_WARN
    assert ALERT_MIN_COUNT >= 1
