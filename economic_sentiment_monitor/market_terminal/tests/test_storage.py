"""tests/test_storage.py"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _make_test_db(tmp_path):
    """Patch DB_PATH to a temp file, init schema, return path."""
    import market_terminal.storage.database as db_mod
    import market_terminal.config.settings as cfg_mod
    test_db = os.path.join(tmp_path, "test.db")
    original = db_mod.DB_PATH
    db_mod.DB_PATH = test_db
    cfg_mod.DB_PATH = test_db
    db_mod.init_db()
    return test_db, original, db_mod, cfg_mod


def test_init_db_creates_tables():
    with tempfile.TemporaryDirectory() as tmp:
        test_db, orig, db_mod, cfg_mod = _make_test_db(tmp)
        conn = db_mod.get_db()
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "articles" in tables
        assert "entity_mentions" in tables
        assert "market_prices" in tables
        assert "sentiment_scores" in tables
        assert "alerts" in tables
        assert "fred_data" in tables
        # restore
        db_mod.DB_PATH = orig
        cfg_mod.DB_PATH = orig


def test_article_insert_and_query():
    with tempfile.TemporaryDirectory() as tmp:
        test_db, orig, db_mod, cfg_mod = _make_test_db(tmp)
        with db_mod.get_db() as db:
            db.execute("""
                INSERT INTO articles
                (id, title, source, url, published, ingested,
                 score, label, confidence, model_tier,
                 keywords, entities, full_text, text_length,
                 numeric_signals, source_tier)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, ("abc123", "Test headline", "Reuters", "http://x.com",
                  "2024-01-01", "2024-01-01T00:00:00",
                  0.15, "POS", 0.8, "vader",
                  "[]", "[]", 0, 0, "{}", "A"))
        with db_mod.get_db() as db:
            row = db.execute(
                "SELECT title, score FROM articles WHERE id=?", ("abc123",)
            ).fetchone()
        assert row is not None
        assert row[0] == "Test headline"
        assert abs(row[1] - 0.15) < 1e-6
        # restore
        db_mod.DB_PATH = orig
        cfg_mod.DB_PATH = orig
