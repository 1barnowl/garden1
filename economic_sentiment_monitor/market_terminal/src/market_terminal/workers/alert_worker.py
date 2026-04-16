"""
market_terminal/workers/alert_worker.py

Dedicated alert evaluation worker — decoupled from the news ingestion loop.
Runs on its own cadence (every 2 minutes) so slow NLP doesn't delay alerts.

Responsibilities:
  - Run check_alerts() from ingest/aggregator.py
  - Push new alerts into CACHE
  - Update data_freshness for the 'alerts' feed
  - Log alert events for audit trail
"""

import time
from datetime import datetime, timezone

from market_terminal.config.settings import log
from market_terminal.ingest.aggregator import check_alerts
from market_terminal.storage.database import get_db, update_freshness


ALERT_INTERVAL_SEC = 120   # run every 2 minutes


def run_alert_worker(cache_ref, interval: int = ALERT_INTERVAL_SEC):
    """
    Background thread entry-point.
    Call as: threading.Thread(target=run_alert_worker, args=(CACHE,), daemon=True).start()
    """
    log.info("alert_worker started (interval=%ds)", interval)

    while True:
        try:
            new_alerts = check_alerts()

            if new_alerts:
                cache_ref.add_alerts(new_alerts)
                for a in new_alerts:
                    log.warning(
                        "[ALERT] %s | %s | %s | score=%+.4f",
                        a["severity"], a["key"], a["reason"][:60], a["score"]
                    )

            # Update freshness tracking
            with get_db() as db:
                update_freshness(db, "alerts", ok=True,
                                 item_count=len(new_alerts))

            cache_ref.clear_error("analytics")

        except Exception as e:
            cache_ref.record_error("analytics", e)
            try:
                with get_db() as db:
                    update_freshness(db, "alerts", ok=False,
                                     err_msg=str(e)[:200])
            except Exception:
                pass

        time.sleep(interval)


def run_filings_worker(cache_ref, interval: int = 1800):
    """
    Background thread for SEC EDGAR filing ingestion.
    Runs every 30 minutes — EDGAR rate-limits requests.
    """
    from market_terminal.ingest.filings import fetch_all_filings

    log.info("filings_worker started (interval=%ds)", interval)

    while True:
        try:
            n = fetch_all_filings(max_tickers=15)
            cache_ref.update_filings()
            with get_db() as db:
                update_freshness(db, "filings", ok=True, item_count=n)
            cache_ref.clear_error("filings")
            log.info("filings_worker: %d new filings", n)
        except Exception as e:
            cache_ref.record_error("filings", e)
            try:
                with get_db() as db:
                    update_freshness(db, "filings", ok=False, err_msg=str(e)[:200])
            except Exception:
                pass
        time.sleep(interval)
