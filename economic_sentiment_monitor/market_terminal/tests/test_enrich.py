"""tests/test_enrich.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def test_vader_scores_positive_text():
    from market_terminal.enrich.scorer import score_text
    result = score_text("Stocks surge to record highs on strong earnings")
    assert result["score"] > 0
    assert result["label"] in ("POS", "NEU", "NEG")
    assert result["model_tier"] in ("vader", "finbert+vader")


def test_vader_scores_negative_text():
    from market_terminal.enrich.scorer import score_text
    result = score_text("Markets crash as recession fears deepen")
    assert result["score"] < 0.1   # should be negative or near-neutral


def test_score_text_returns_required_keys():
    from market_terminal.enrich.scorer import score_text
    result = score_text("Federal Reserve raises interest rates")
    for key in ("score", "vader_score", "label", "confidence", "model_tier"):
        assert key in result


def test_extract_keywords_inflation():
    from market_terminal.enrich.entities import extract_keywords
    kws = extract_keywords("CPI rose 0.4% as inflation remains elevated")
    assert "inflation" in kws


def test_extract_keywords_rates():
    from market_terminal.enrich.entities import extract_keywords
    kws = extract_keywords("Federal Reserve signals rate hike at next FOMC")
    assert "rates" in kws


def test_extract_entities_finds_apple():
    from market_terminal.enrich.entities import extract_entities
    ents = extract_entities("Apple reported record quarterly revenue")
    tickers = [e["ticker"] for e in ents]
    assert "AAPL" in tickers


def test_extract_entities_finds_bitcoin():
    from market_terminal.enrich.entities import extract_entities
    ents = extract_entities("Bitcoin surges past $70,000 amid ETF optimism")
    tickers = [e["ticker"] for e in ents]
    assert "BTC-USD" in tickers


def test_extract_numeric_pct():
    from market_terminal.enrich.entities import extract_numeric_signals
    sigs = extract_numeric_signals("Inflation rose 3.2% in March")
    assert "pct_change" in sigs


def test_extract_numeric_bps():
    from market_terminal.enrich.entities import extract_numeric_signals
    sigs = extract_numeric_signals("Fed cut rates by 25bps at today's meeting")
    assert "bps_change" in sigs
    assert sigs["bps_change"] < 0   # cut = negative


def test_enrich_headline_sets_source_tier():
    from market_terminal.enrich.entities import enrich_headline_metadata
    art = {"title": "Stocks rise", "source": "Reuters Business"}
    out = enrich_headline_metadata(art)
    assert out["source_tier"] == "A"

    art2 = {"title": "Stocks rise", "source": "Unknown Blog"}
    out2 = enrich_headline_metadata(art2)
    assert out2["source_tier"] == "C"
