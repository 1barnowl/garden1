"""
ui/app.py
Textual TUI v0.7 — Bloomberg-style terminal with 10 tabs.
Faster price refresh (separate 15s loop), richer per-tab columns,
per-asset news feed.
"""

import threading
import time
import json
from datetime import datetime, timezone, timedelta

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, TabbedContent, TabPane, DataTable, Static
from textual.reactive import reactive

from config.settings import (
    log, CONFIG_PATH, LOG_PATH, REFRESH_SEC, PRICE_REFRESH_SEC,
    WATCHLIST, SECTORS, RSS_FEEDS, FRED_SERIES,
    ALERT_ZSCORE_WARN, ALERT_ZSCORE_CRIT, ALERT_DRIFT_N,
    FULL_TEXT_ENABLED, NEWS_API_KEY, FRED_API_KEY,
)
from db.database import get_db
from db.cache import CACHE
from analytics.engine import (
    compute_correlations, compute_lead_lag,
    compute_sentiment_momentum, detect_regime, compute_bayesian_signal,
)
from ingestion.feeds import (
    fetch_rss, fetch_newsapi, fetch_prices, save_prices,
    save_articles, fetch_all_fred, save_fred_data,
    apply_fred_surprise_boost, fulltext_worker,
    fetch_ticker_news, NEWSPAPER_READY,
)
from ingestion.aggregator import compute_aggregate_scores, check_alerts
from nlp.scorer import FINBERT_READY
from nlp.entities import SPACY_READY

# ── analytics caches (updated by analytics thread) ───────────────────────────
_regime_cache:   dict = {}
_momentum_cache: dict = {}
_leadlag_cache:  dict = {}
_bayes_cache:    dict = {}

# ── display helpers ───────────────────────────────────────────────────────────

def sentiment_bar(score: float, width: int = 12) -> str:
    half   = width // 2
    filled = min(int(abs(score) * half), half)
    if score >= 0.05:
        return "─" * half + "│" + "█" * filled + "─" * (half - filled)
    elif score <= -0.05:
        return "─" * (half - filled) + "█" * filled + "│" + "─" * half
    return "─" * half + "│" + "─" * half

def sparkline(scores: list, width: int = 10) -> str:
    blocks = " ▁▂▃▄▅▆▇█"
    if not scores:
        return "─" * width
    tail = scores[-width:]
    if len(tail) < 2:
        return "─" * width
    mn, mx = min(tail), max(tail)
    span   = mx - mn
    if span == 0:
        return "▄" * len(tail) + "─" * (width - len(tail))
    chars = [blocks[max(0, min(int((v-mn)/span*(len(blocks)-1)), len(blocks)-1))]
             for v in tail]
    return "".join(chars).ljust(width, "─")

def fmt_score(s) -> str:
    return f"{s:+.4f}" if s is not None else "  n/a "

def fmt_chg(c: float) -> str:
    return f"{'+'if c>=0 else''}{c:.2f}%"

def fmt_corr(r) -> str:
    if r is None: return " n/a"
    return f"{'+' if r>=0 else ''}{r:.2f}"

def fmt_vol(v: float) -> str:
    if v > 1e9:  return f"{v/1e9:.1f}B"
    if v > 1e6:  return f"{v/1e6:.1f}M"
    if v > 1e3:  return f"{v/1e3:.0f}K"
    return str(int(v)) if v else "—"

def tier_badge() -> str:
    return "[FB+VD]" if FINBERT_READY else "[VD]"

def fear_greed_index(scores: dict) -> tuple:
    weights = {"rates": 0.25, "growth": 0.20, "employment": 0.15,
               "inflation": 0.15, "credit": 0.15, "trade": 0.10}
    total_w, total_s = 0.0, 0.0
    for theme, w in weights.items():
        s = scores.get(theme, {}).get("1h", {}).get("score")
        if s is not None:
            total_s += s * w; total_w += w
    if total_w == 0: return 0, "NEUTRAL"
    val = round((total_s / total_w) * 100)
    if val >= 30:    label = "EXTREME GREED"
    elif val >= 10:  label = "GREED"
    elif val >= 5:   label = "MILD GREED"
    elif val <= -30: label = "EXTREME FEAR"
    elif val <= -10: label = "FEAR"
    elif val <= -5:  label = "MILD FEAR"
    else:            label = "NEUTRAL"
    return val, label

def velocity_label(key: str) -> str:
    d   = CACHE.scores.get(key, {})
    n1  = d.get("1h",  {}).get("count", 0) or 0
    n24 = d.get("24h", {}).get("count", 0) or 0
    avg = n24 / 24 if n24 else 0
    if avg == 0: return ""
    if n1 > avg * 3:   return "HOT"
    if n1 > avg * 1.5: return "WARM"
    return ""

def divergence_score(ticker: str) -> str:
    s1  = CACHE.scores.get(ticker, {}).get("1h", {}).get("score")
    chg = CACHE.prices.get(ticker, {}).get("change_pct", 0.0)
    if s1 is None: return "—"
    if s1 > 0.05 and chg < -0.5: return "↑s↓p"
    if s1 < -0.05 and chg > 0.5: return "↓s↑p"
    if abs(s1) > 0.05 and abs(chg) > 0.5: return "align"
    return "—"

def get_sparkline_history(key: str, points: int = 12) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT score FROM sentiment_scores
            WHERE key=? AND window='1h' AND ts > ?
            ORDER BY ts ASC
        """, (key, cutoff)).fetchall()
    s = [r[0] for r in rows]
    return s[-points:] if len(s) > points else s

# ── background threads ────────────────────────────────────────────────────────

def _run_price_refresh():
    """Fast price loop — 15s independent of news ingestion."""
    log.info("price refresh thread started (%ds)", PRICE_REFRESH_SEC)
    while True:
        try:
            all_tickers = [t for tl in WATCHLIST.values() for t in tl]
            rows = fetch_prices(all_tickers)
            save_prices(rows)
            CACHE.update_prices(rows)
            CACHE.clear_error("prices")
        except Exception as e:
            CACHE.record_error("prices", e)
        time.sleep(PRICE_REFRESH_SEC)

def _run_news_refresh():
    """News ingestion loop — 30s."""
    log.info("news refresh thread started (%ds)", REFRESH_SEC)
    while True:
        try:
            prices_snap = dict(CACHE.prices)
            articles    = fetch_rss(prices_snap) + fetch_newsapi(prices_snap)
            n_new       = save_articles(articles)

            # targeted per-ticker news (cycle through watchlist tickers)
            if NEWS_API_KEY:
                equity_tickers = WATCHLIST["Equities"]
                macro_tickers  = list(WATCHLIST["Macro/FX"])[:6]
                for ticker in equity_tickers + macro_tickers:
                    ticker_arts = fetch_ticker_news(ticker, prices_snap)
                    save_articles(ticker_arts)
                    time.sleep(0.1)

            compute_aggregate_scores()
            new_alerts = check_alerts()
            CACHE.update_scores()
            CACHE.update_articles()
            CACHE.update_entity_summary()
            CACHE.add_alerts(new_alerts)
            CACHE.set_last_refresh()
            CACHE.clear_error("ingest")
            log.debug("news cycle OK — %d new articles, %d alerts", n_new, len(new_alerts))
        except Exception as e:
            CACHE.record_error("ingest", e)
        time.sleep(REFRESH_SEC)

def _run_fred_refresh():
    log.info("FRED refresh thread started")
    while True:
        try:
            if FRED_API_KEY:
                rows = fetch_all_fred()
                save_fred_data(rows)
                apply_fred_surprise_boost(rows)
                CACHE.update_fred_data()
                CACHE.clear_error("fred")
        except Exception as e:
            CACHE.record_error("fred", e)
        time.sleep(1800)

def _run_correlation_refresh():
    log.info("correlation refresh thread started")
    while True:
        try:
            tickers = (WATCHLIST["Equities"] + WATCHLIST["Sectors"] +
                       WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"])
            corr = compute_correlations(tickers)
            CACHE.update_correlations(corr)
            CACHE.clear_error("corr")
        except Exception as e:
            CACHE.record_error("corr", e)
        time.sleep(600)

def _run_analytics_refresh():
    global _regime_cache, _momentum_cache, _leadlag_cache, _bayes_cache
    log.info("analytics refresh thread started")
    while True:
        try:
            _regime_cache = detect_regime(CACHE.scores)
            from config.settings import MACRO_KEYWORDS
            for k in list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]:
                _momentum_cache[k] = compute_sentiment_momentum(k)
            for ticker in WATCHLIST["Equities"] + WATCHLIST["Sectors"][:4]:
                _leadlag_cache[ticker] = compute_lead_lag(ticker)
                _bayes_cache[ticker]   = compute_bayesian_signal(ticker)
            CACHE.clear_error("analytics")
        except Exception as e:
            CACHE.record_error("analytics", e)
        time.sleep(300)

# ══════════════════════════════════════════════════════════════════════════════
#  TEXTUAL APP
# ══════════════════════════════════════════════════════════════════════════════

class ESMApp(App):
    CSS = """
    Screen       { background: #060606; }
    Header       { background: #000000; color: #ffcc00; text-style: bold; }
    Footer       { background: #0a0a0a; color: #444; }
    TabbedContent { height: 1fr; }
    Tabs         { background: #0a0a0a; border-bottom: solid #ffcc00; height: 3; }
    Tab          { background: #111; color: #555; border: solid #222;
                   padding: 0 2; height: 3; content-align: center middle; }
    Tab:hover    { background: #1a1a1a; color: #aaa; }
    Tab.-active  { background: #1a1400; color: #ffcc00;
                   border: solid #ffcc00; text-style: bold; }
    TabPane      { padding: 0; }
    #status_bar  { height: 1; background: #0d0d0d; color: #4a6a4a;
                   padding: 0 2; border-bottom: solid #1a1a1a; }
    #status_bar2 { height: 1; background: #080808; color: #3a3a3a;
                   padding: 0 2; border-bottom: solid #111; }
    #fg_bar      { height: 1; background: #0a0a00; color: #888800;
                   padding: 0 2; text-style: bold; }
    #regime_bar  { height: 1; background: #050510; color: #5555cc;
                   padding: 0 2; }
    DataTable    { height: 1fr; background: #080808; }
    DataTable > .datatable--header { background: #141414; color: #ffcc00;
                                     text-style: bold; }
    DataTable > .datatable--cursor { background: #1a2e1a; }
    DataTable > .datatable--hover  { background: #101010; }
    """

    BINDINGS = [
        ("r", "refresh",   "Refresh"),
        ("q", "quit",      "Quit"),
        ("?", "show_help", "Help"),
        ("c", "open_cfg",  "Config"),
        ("1", "switch_tab('equities')",   "Equities"),
        ("2", "switch_tab('macrofx')",    "Macro/FX"),
        ("3", "switch_tab('bonds')",      "Bonds"),
        ("4", "switch_tab('sectors')",    "Sectors"),
        ("5", "switch_tab('sentiment')",  "Sentiment"),
        ("6", "switch_tab('macrodata')",  "FRED"),
        ("7", "switch_tab('newsfeed')",   "News"),
        ("8", "switch_tab('entities')",   "Entities"),
        ("9", "switch_tab('analytics')",  "Analytics"),
        ("0", "switch_tab('alerts')",     "Alerts"),
        ("l", "switch_tab('errorlog')",   "Log"),
    ]

    TITLE     = "ESM  //  Economic Sentiment Monitor  v0.7"
    SUB_TITLE = "initialising…"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="status_bar")
        yield Static("", id="status_bar2")
        yield Static("", id="fg_bar")
        yield Static("", id="regime_bar")
        with TabbedContent(initial="equities"):
            with TabPane("1:EQUITIES",  id="equities"):
                yield DataTable(id="eq_table",       zebra_stripes=True)
            with TabPane("2:MACRO/FX",  id="macrofx"):
                yield DataTable(id="fx_table",       zebra_stripes=True)
            with TabPane("3:BONDS",     id="bonds"):
                yield DataTable(id="bond_table",     zebra_stripes=True)
            with TabPane("4:SECTORS",   id="sectors"):
                yield DataTable(id="sec_table",      zebra_stripes=True)
            with TabPane("5:SENTIMENT", id="sentiment"):
                yield DataTable(id="sent_table",     zebra_stripes=True)
            with TabPane("6:FRED DATA", id="macrodata"):
                yield DataTable(id="fred_table",     zebra_stripes=True)
            with TabPane("7:NEWS FEED", id="newsfeed"):
                yield DataTable(id="news_table",     zebra_stripes=True)
            with TabPane("8:ENTITIES",  id="entities"):
                yield DataTable(id="ent_table",      zebra_stripes=True)
            with TabPane("9:ANALYTICS", id="analytics"):
                yield DataTable(id="analytics_table",zebra_stripes=True)
            with TabPane("0:ALERTS",    id="alerts"):
                yield DataTable(id="alert_table",    zebra_stripes=True)
            with TabPane("L:LOG",       id="errorlog"):
                yield DataTable(id="log_table",      zebra_stripes=True)
        yield Footer()

    def on_mount(self):
        self._setup_tables()
        threading.Thread(target=_run_price_refresh,       daemon=True).start()
        threading.Thread(target=_run_news_refresh,        daemon=True).start()
        threading.Thread(target=_run_fred_refresh,        daemon=True).start()
        threading.Thread(target=_run_correlation_refresh, daemon=True).start()
        threading.Thread(target=_run_analytics_refresh,   daemon=True).start()
        if FULL_TEXT_ENABLED and NEWSPAPER_READY:
            threading.Thread(target=fulltext_worker,
                             args=(CACHE,), daemon=True).start()
        self.set_interval(8,  self._ui_refresh)   # UI refresh every 8s
        self.set_timer(4,     self._ui_refresh)

    def action_switch_tab(self, tab_id: str):
        self.query_one(TabbedContent).active = tab_id

    # ── table setup ───────────────────────────────────────────────────────────
    def _setup_tables(self):
        # 1: EQUITIES
        self.query_one("#eq_table", DataTable).add_columns(
            "Ticker", "Price", "Chg%", "Open", "High", "Low",
            "Volume", "VWAP", "Sent 1h", "Spark",
            "Sent 4h", "Sent 24h", "Corr", "Lag", "Bayes", "Diverg")

        # 2: MACRO/FX
        self.query_one("#fx_table", DataTable).add_columns(
            "Asset", "Price", "Chg%", "Open", "High", "Low",
            "Volume", "Sent 1h", "Spark", "Sent 24h",
            "Corr", "Trend", "Vel", "Label")

        # 3: BONDS
        self.query_one("#bond_table", DataTable).add_columns(
            "Instrument", "Price/Yield", "Chg%", "Open", "High", "Low",
            "Volume", "Sent 1h", "Spark", "Sent 24h",
            "Corr", "Credit Theme", "Label")

        # 4: SECTORS
        self.query_one("#sec_table", DataTable).add_columns(
            "ETF", "Sector", "Price", "Chg%", "Open", "High", "Low",
            "Volume", "Sent 1h", "Spark", "Heatmap",
            "Sent 24h", "Corr", "Vel", "Bayes")

        # 5: SENTIMENT
        self.query_one("#sent_table", DataTable).add_columns(
            "Theme", "1h", "4h", "24h", "Spark",
            "Trend", "Accel", "Consec", "Vel", "n(1h)", "Context")

        # 6: FRED
        self.query_one("#fred_table", DataTable).add_columns(
            "Series", "Theme", "Latest", "Prior",
            "Surprise %", "As Of", "Signal")

        # 7: NEWS FEED — per-asset headlines + richer metadata
        self.query_one("#news_table", DataTable).add_columns(
            "Time", "Source", "Tier", "Score", "FB", "VD",
            "Nums", "FT", "Kw", "Ticker", "Headline")

        # 8: ENTITIES
        self.query_one("#ent_table", DataTable).add_columns(
            "Ticker", "Mentions 24h", "Avg Sent",
            "Min", "Max", "Spark", "In WL")

        # 9: ANALYTICS
        self.query_one("#analytics_table", DataTable).add_columns(
            "Asset/Theme", "Type", "Value", "Detail", "Conf")

        # 0: ALERTS
        self.query_one("#alert_table", DataTable).add_columns(
            "Time", "Sev", "Regime", "Key", "Reason", "Score")

        # L: LOG
        self.query_one("#log_table", DataTable).add_columns(
            "Thread", "Status", "Last Error", "Log File")

    # ── master UI refresh ─────────────────────────────────────────────────────
    def _ui_refresh(self):
        errs    = CACHE.err_counts
        total_e = sum(errs.values())
        err_s   = (f"⚠ ERR "
                   + " ".join(f"{k}:{v}" for k,v in errs.items() if v > 0)
                   + "  │  " if total_e else "")
        ner_s   = "NER:✓" if SPACY_READY else "NER:✗"
        fred_s  = f"FRED:{len(CACHE.fred_data)}s" if FRED_API_KEY else "FRED:✗"
        ft_s    = f"FT:{CACHE.fulltext_count}" if FULL_TEXT_ENABLED else "FT:off"

        self.query_one("#status_bar", Static).update(
            f"{err_s}{tier_badge()}  {ner_s}  {ft_s}  {fred_s}"
            f"  │  Price:{PRICE_REFRESH_SEC}s  News:{REFRESH_SEC}s"
            f"  │  Refresh:{CACHE.last_refresh}"
            f"  Articles:{CACHE.article_count}"
        )
        self.query_one("#status_bar2", Static).update(
            f"  CFG:{CONFIG_PATH}"
            f"  │  Alert Z>={ALERT_ZSCORE_WARN:.1f}σ CRIT>={ALERT_ZSCORE_CRIT:.1f}σ"
            f"  │  Drift:{ALERT_DRIFT_N}w"
            f"  │  Feeds:{len(RSS_FEEDS)}"
            f"  │  LOG:{LOG_PATH}"
        )

        fg_val, fg_label = fear_greed_index(CACHE.scores)
        bar_len = 26
        filled  = max(0, min(int((fg_val+100)/200*bar_len), bar_len))
        fg_bar  = "█" * filled + "░" * (bar_len - filled)
        self.query_one("#fg_bar", Static).update(
            f"  FEAR/GREED [{fg_bar}] {fg_val:+d} {fg_label}"
            f"  │  1=Eq 2=FX 3=Bonds 4=Sec 5=Sent 6=FRED "
            f"7=News 8=Ent 9=Anlyt 0=Alerts L=Log"
        )

        reg = _regime_cache
        if reg:
            sigs = "  ".join(reg.get("signals", [])[:5])
            self.query_one("#regime_bar", Static).update(
                f"  REGIME: {reg.get('label','—')}"
                f"  conf:{reg.get('confidence',0):.0%}"
                f"  │  {sigs}"
            )

        CACHE.update_scores()
        CACHE.update_articles()
        CACHE.update_entity_summary()
        CACHE.update_fred_data()

        self._refresh_equities()
        self._refresh_macrofx()
        self._refresh_bonds()
        self._refresh_sectors()
        self._refresh_sentiment()
        self._refresh_fred()
        self._refresh_news()
        self._refresh_entities()
        self._refresh_analytics()
        self._refresh_alerts()
        self._refresh_log()

    # ── helpers ───────────────────────────────────────────────────────────────
    def _p(self, ticker: str) -> dict:
        return CACHE.prices.get(ticker, {})

    def _price_cols(self, ticker: str):
        p = self._p(ticker)
        if not p:
            return ("—", "—", "—", "—", "—", "—", "—")
        pr   = f"{p.get('price',0):,.4f}"
        chg  = fmt_chg(p.get("change_pct", 0))
        op   = f"{p.get('open',0):.4f}"  if p.get("open")  else "—"
        hi   = f"{p.get('high',0):.4f}"  if p.get("high")  else "—"
        lo   = f"{p.get('low',0):.4f}"   if p.get("low")   else "—"
        vol  = fmt_vol(p.get("volume", 0))
        vwap = f"{p.get('vwap',0):.4f}"  if p.get("vwap")  else "—"
        return pr, chg, op, hi, lo, vol, vwap

    def _s(self, key: str):
        d = CACHE.scores.get(key, {})
        return (d.get("1h",  {}).get("score"),
                d.get("4h",  {}).get("score"),
                d.get("24h", {}).get("score"))

    # ── tab refreshers ────────────────────────────────────────────────────────
    def _refresh_equities(self):
        t = self.query_one("#eq_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Equities"]:
            pr, chg, op, hi, lo, vol, vwap = self._price_cols(ticker)
            s1, s4, s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            ll    = _leadlag_cache.get(ticker, {})
            lag_s = (f"{ll['best_lag']}h/{ll['best_r']:.2f}"
                     if ll.get("best_lag") and ll.get("best_r") else "n/a")
            bay   = _bayes_cache.get(ticker, {})
            bay_s = bay.get("signal_str", "n/a")[:20] if bay else "n/a"
            div   = divergence_score(ticker)
            t.add_row(ticker, pr, chg, op, hi, lo, vol, vwap,
                      fmt_score(s1), spark, fmt_score(s4), fmt_score(s24),
                      fmt_corr(corr), lag_s, bay_s, div)

    def _refresh_macrofx(self):
        t = self.query_one("#fx_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Macro/FX"]:
            pr, chg, op, hi, lo, vol, _ = self._price_cols(ticker)
            s1, s4, s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            trend = ("▲" if (s1 or 0) > (s4 or 0)+0.01
                     else "▼" if (s1 or 0) < (s4 or 0)-0.01 else "→"
                     ) if s1 is not None and s4 is not None else "—"
            vel   = velocity_label(ticker)
            lbl   = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, pr, chg, op, hi, lo, vol,
                      fmt_score(s1), spark, fmt_score(s24),
                      fmt_corr(corr), trend, vel, lbl)

    def _refresh_bonds(self):
        t = self.query_one("#bond_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Bonds"]:
            pr, chg, op, hi, lo, vol, _ = self._price_cols(ticker)
            s1, s4, s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            # credit theme score as proxy
            credit_s = CACHE.scores.get("credit", {}).get("1h", {}).get("score")
            credit_str = fmt_score(credit_s)
            lbl = "POS" if (s1 or 0) >= 0.05 else ("NEG" if (s1 or 0) <= -0.05 else "NEU")
            t.add_row(ticker, pr, chg, op, hi, lo, vol,
                      fmt_score(s1), spark, fmt_score(s24),
                      fmt_corr(corr), credit_str, lbl)

    def _refresh_sectors(self):
        t = self.query_one("#sec_table", DataTable)
        t.clear()
        for ticker, name in SECTORS.items():
            pr, chg, op, hi, lo, vol, _ = self._price_cols(ticker)
            s1, s4, s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            vel   = velocity_label(ticker)
            bay   = _bayes_cache.get(ticker, {})
            bay_s = bay.get("signal_str", "—")[:16] if bay else "—"
            t.add_row(ticker, name, pr, chg, op, hi, lo, vol,
                      fmt_score(s1), spark, sentiment_bar(s1 or 0),
                      fmt_score(s24), fmt_corr(corr), vel, bay_s)

    def _refresh_sentiment(self):
        t = self.query_one("#sent_table", DataTable)
        t.clear()
        from config.settings import MACRO_KEYWORDS
        for theme in list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]:
            d   = CACHE.scores.get(theme, {})
            s1  = d.get("1h",  {}).get("score")
            s4  = d.get("4h",  {}).get("score")
            s24 = d.get("24h", {}).get("score")
            n1  = d.get("1h",  {}).get("count", 0)
            spark = sparkline(get_sparkline_history(theme))
            vel   = velocity_label(theme)
            mom   = _momentum_cache.get(theme, {})
            accel = mom.get("acceleration")
            consec= mom.get("consecutive", 0)
            accel_s = f"{accel:+.3f}" if accel is not None else "—"
            trend = ("▲" if (s1 or 0) > (s4 or 0)+0.01
                     else "▼" if (s1 or 0) < (s4 or 0)-0.01 else "→"
                     ) if s1 and s4 else "—"
            ctx   = ("Bullish" if (s1 or 0) > 0.1 else
                     "Bearish" if (s1 or 0) < -0.1 else
                     "Mixed"   if s1 is not None else "—")
            t.add_row(theme.upper(), fmt_score(s1), fmt_score(s4),
                      fmt_score(s24), spark, trend, accel_s,
                      str(consec), vel, str(n1 or "—"), ctx)

    def _refresh_fred(self):
        t = self.query_one("#fred_table", DataTable)
        t.clear()
        if not CACHE.fred_data:
            t.add_row("—","—","No FRED data — add fred_api_key to config",
                      "—","—","—","—")
            return
        for row in CACHE.fred_data:
            val_s  = f"{row['value']:.4f}"      if row["value"]      is not None else "—"
            prev_s = f"{row['prev_value']:.4f}" if row["prev_value"] is not None else "—"
            surp   = row["surprise"] or 0.0
            signal = "↑ boost" if surp >= 0.5 else ("↓ dampen" if surp <= -0.5 else "—")
            t.add_row(row["label"], row["theme"].upper(),
                      val_s, prev_s, f"{surp:+.2f}%",
                      (row["ts"] or "")[:10], signal)

    def _refresh_news(self):
        """
        News feed — shows up to 200 articles.
        For each article we show: source tier, both scores, numeric signals,
        full-text flag, matched keywords, and importantly — which ticker
        was linked so you can filter mentally by asset.
        """
        t = self.query_one("#news_table", DataTable)
        t.clear()
        for art in CACHE.recent_articles[:200]:
            pub    = (art["published"] or "")[:16]
            src    = (art["source"] or "")[:14]
            tier   = art.get("source_tier", "C")
            ft_s   = "✓" if art.get("full_text") else "·"
            kws    = art.get("keywords", "")
            try:
                kw_s = ",".join(json.loads(kws or "[]")[:2])
            except Exception:
                kw_s = ""
            try:
                nums = json.loads(art.get("numeric_signals","{}") or "{}")
                num_s = " ".join(f"{k[:3]}:{v}" for k,v in list(nums.items())[:2])
            except Exception:
                num_s = ""
            # linked tickers from entity mentions
            entities_str = art.get("entities", "[]") or "[]"
            try:
                ents = json.loads(entities_str)
                linked = ",".join(e["ticker"] for e in ents[:2]) if ents else "—"
            except Exception:
                linked = "—"
            title = (art["title"] or "")[:75]
            t.add_row(pub, src, tier,
                      fmt_score(art["score"]),
                      fmt_score(art.get("finbert_score")),
                      fmt_score(art.get("vader_score")),
                      num_s[:14], ft_s, kw_s[:12], linked[:14], title)

    def _refresh_entities(self):
        t = self.query_one("#ent_table", DataTable)
        t.clear()
        all_tickers = {tk for tl in WATCHLIST.values() for tk in tl}
        for row in CACHE.entity_summary:
            spark = sparkline(get_sparkline_history(row["ticker"]), width=8)
            in_wl = "✓" if row["ticker"] in all_tickers else "—"
            t.add_row(row["ticker"], str(row["mentions"]),
                      fmt_score(row["avg_score"]),
                      fmt_score(row["min_score"]),
                      fmt_score(row["max_score"]),
                      spark, in_wl)

    def _refresh_analytics(self):
        t = self.query_one("#analytics_table", DataTable)
        t.clear()
        reg = _regime_cache
        if reg:
            t.add_row("MARKET", "Regime",
                      reg.get("label","—"),
                      "  ".join(reg.get("signals",[])[:5]),
                      f"{reg.get('confidence',0):.0%}")
        mom = _momentum_cache.get("GLOBAL",{})
        if mom.get("acceleration") is not None:
            t.add_row("GLOBAL","Momentum",
                      f"{mom['direction']} {mom['consecutive']}x",
                      f"accel={mom['acceleration']:+.4f}", "—")
        from config.settings import MACRO_KEYWORDS
        for theme in MACRO_KEYWORDS:
            mom = _momentum_cache.get(theme,{})
            if mom.get("acceleration") is not None:
                t.add_row(theme.upper(),"Momentum",
                          f"{mom.get('direction','—')} {mom.get('consecutive',0)}x",
                          f"accel={mom.get('acceleration',0):+.4f}", "—")
        for ticker, ll in _leadlag_cache.items():
            if ll.get("best_r") and abs(ll["best_r"]) > 0.1:
                lag_detail = "  ".join(
                    f"{h}h:{r:.2f}" for h,r in sorted(ll.get("by_lag",{}).items())
                    if r is not None)
                t.add_row(ticker,"Lead-Lag",
                          f"best={ll['best_lag']}h r={ll['best_r']:.3f}",
                          lag_detail[:50], f"|r|={abs(ll['best_r']):.2f}")
        for ticker, bay in _bayes_cache.items():
            if bay.get("sources_used",0) > 0:
                top = "  ".join(
                    f"{s}:{a:.0%}" for s,a in
                    sorted(bay.get("source_acc",{}).items(),
                           key=lambda x: abs(x[1]-0.5), reverse=True)[:3])
                t.add_row(ticker,"Bayesian",
                          f"{bay.get('direction','—')} {bay.get('confidence',0):.0%}",
                          f"{bay.get('sources_used',0)} arts  {top[:40]}",
                          f"{bay.get('confidence',0):.0%}")

    def _refresh_alerts(self):
        t = self.query_one("#alert_table", DataTable)
        t.clear()
        reg_label = _regime_cache.get("label","—") if _regime_cache else "—"
        seen = set()
        with get_db() as db:
            rows = db.execute(
                "SELECT ts,severity,key,reason,score FROM alerts "
                "ORDER BY ts DESC LIMIT 80"
            ).fetchall()
        for r in rows:
            k = r[2] + r[3]
            if k not in seen:
                seen.add(k)
                t.add_row(r[0][:19], r[1], reg_label, r[2], r[3], fmt_score(r[4]))

    def _refresh_log(self):
        t = self.query_one("#log_table", DataTable)
        t.clear()
        for key, label in [
            ("ingest","News ingest"),("prices","Price feed"),
            ("fulltext","Full-text"),("fred","FRED"),
            ("corr","Correlation"),("analytics","Analytics"),
        ]:
            n    = CACHE.err_counts.get(key, 0)
            last = CACHE.last_errors.get(key, "") or "—"
            t.add_row(label, "✓ OK" if n == 0 else f"✗ {n} err",
                      last[:90], LOG_PATH)

    # ── actions ───────────────────────────────────────────────────────────────
    def action_refresh(self):
        self._ui_refresh()

    def action_open_cfg(self):
        self.notify(
            f"Config: {CONFIG_PATH}\n"
            "Edit with any text editor, restart to apply.\n"
            "Keys: watchlists, rss_feeds, refresh_sec,\n"
            "      price_refresh_sec, alert thresholds",
            title="Config", timeout=10)

    def action_show_help(self):
        self.notify(
            "Keys: 1-9/0/L = tabs  R = refresh  Q = quit  ? = help\n\n"
            "Tabs: Equities · Macro/FX · Bonds · Sectors · Sentiment\n"
            "      FRED · News Feed · Entities · Analytics · Alerts · Log\n\n"
            "v0.7 changes:\n"
            "  Price refresh: every 15s (independent of news loop)\n"
            "  News loop: every 30s with per-ticker targeted queries\n"
            "  Bonds tab: dedicated 10-instrument bond/rate view\n"
            "  All tabs: Open/High/Low/Vol/VWAP columns\n"
            "  News feed: 200 articles, linked tickers, numeric signals\n"
            "  Analytics: regime + momentum + lead-lag + Bayesian\n",
            title="ESM v0.7  Help", timeout=20)
