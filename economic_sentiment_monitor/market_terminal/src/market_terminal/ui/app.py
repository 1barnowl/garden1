"""
market_terminal/ui/app.py  v0.10
New: workspace persistence, event type column, signal accuracy tab,
     enhanced command palette (PIN/UNPIN/NOTE/WATCH), note display,
     filters bar, concurrent RSS, outcome worker.
"""

import threading
import time
import json
from datetime import datetime, timezone, timedelta

from textual.app import App, ComposeResult
from textual.widgets import (
    Header, Footer, TabbedContent, TabPane, DataTable, Static
)

try:
    from textual.command import Provider, Hit, Hits
    _CMD_PALETTE = True
except ImportError:
    _CMD_PALETTE = False

from market_terminal.config.settings import (
    log, REFRESH_SEC, PRICE_REFRESH_SEC,
    WATCHLIST, SECTORS, RSS_FEEDS, FRED_SERIES,
    ALERT_ZSCORE_WARN, ALERT_ZSCORE_CRIT, ALERT_DRIFT_N,
    FULL_TEXT_ENABLED, NEWS_API_KEY, FRED_API_KEY, MACRO_KEYWORDS,
)
from market_terminal.storage.database import get_db, seed_instruments
from market_terminal.storage.cache import CACHE
from market_terminal.storage.workspace import WS
from market_terminal.analytics.engine import (
    compute_correlations, compute_lead_lag,
    compute_sentiment_momentum, detect_regime, compute_bayesian_signal,
)
from market_terminal.ingest.feeds import (
    fetch_rss, fetch_newsapi, fetch_prices, save_prices,
    save_articles, fetch_all_fred, save_fred_data,
    apply_fred_surprise_boost, fulltext_worker,
    fetch_ticker_news, NEWSPAPER_READY,
)
from market_terminal.ingest.aggregator import compute_aggregate_scores
from market_terminal.workers.alert_worker import run_alert_worker, run_filings_worker
from market_terminal.workers.outcome_worker import (
    run_outcome_worker, get_accuracy_stats, record_signal
)
from market_terminal.enrich.scorer import FINBERT_READY
from market_terminal.enrich.entities import SPACY_READY
from market_terminal.enrich.classifier import event_type_label, EventType
from market_terminal.core.utils import (
    sparkline, ascii_chart, sentiment_bar, staleness_label
)
from market_terminal.search.query import (
    search_news, search_filings, news_for_ticker, search_all
)

_regime_cache:   dict = {}
_momentum_cache: dict = {}
_leadlag_cache:  dict = {}
_bayes_cache:    dict = {}
_accuracy_stats: dict = {}
_search_results: list = []
_chart_ticker          = WS.chart_ticker
_chart_data:     list  = []


# ── display helpers ───────────────────────────────────────────────────────────

def fmt_score(s) -> str:
    return f"{s:+.4f}" if s is not None else "  n/a "

def fmt_chg(c: float) -> str:
    arrow = "▲" if c > 0 else ("▼" if c < 0 else "─")
    return f"{arrow}{abs(c):.2f}%"

def fmt_corr(r) -> str:
    if r is None: return " n/a"
    return f"{'+' if r>=0 else ''}{r:.2f}"

def fmt_vol(v: float) -> str:
    if v > 1e9:  return f"{v/1e9:.1f}B"
    if v > 1e6:  return f"{v/1e6:.1f}M"
    if v > 1e3:  return f"{v/1e3:.0f}K"
    return str(int(v)) if v else "—"

def fmt_pct(v: float) -> str:
    return f"{v:.0%}" if v else "—"

def tier_badge() -> str:
    return "[FB+VD]" if FINBERT_READY else "[VD]  "

def fear_greed_index(scores: dict) -> tuple:
    weights = {"rates": 0.25, "growth": 0.20, "employment": 0.15,
               "inflation": 0.15, "credit": 0.15, "trade": 0.10}
    tw, ts = 0.0, 0.0
    for theme, w in weights.items():
        s = scores.get(theme, {}).get("1h", {}).get("score")
        if s is not None: ts += s * w; tw += w
    if tw == 0: return 0, "NEUTRAL"
    val = round((ts / tw) * 100)
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
    if n1 > avg * 3:   return "🔥"
    if n1 > avg * 1.5: return "~"
    return ""

def divergence_score(ticker: str) -> str:
    s1  = CACHE.scores.get(ticker, {}).get("1h", {}).get("score")
    chg = CACHE.prices.get(ticker, {}).get("change_pct", 0.0)
    if s1 is None: return "—"
    if s1 > 0.05  and chg < -0.5: return "↑s↓p"
    if s1 < -0.05 and chg > 0.5:  return "↓s↑p"
    if abs(s1) > 0.05 and abs(chg) > 0.5: return "algn"
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

def get_price_history(ticker: str, points: int = 80) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    with get_db() as db:
        rows = db.execute("""
            SELECT price FROM market_prices
            WHERE ticker=? AND ts > ? ORDER BY ts ASC
        """, (ticker, cutoff)).fetchall()
    vals = [r[0] for r in rows if r[0]]
    return vals[-points:] if len(vals) > points else vals


# ── command palette ───────────────────────────────────────────────────────────

if _CMD_PALETTE:
    class TerminalCommands(Provider):
        """
        Commands:
          QUOTE <T>   price/sentiment snapshot
          CHART <T>   ASCII price+sentiment chart
          NEWS <T>    news filtered to ticker
          FILING <T>  SEC filings for ticker
          SEARCH <q>  full-text search
          PIN <T>     pin ticker to top
          UNPIN <T>   remove pin
          NOTE <T> <text>  save a note for ticker
          WATCH <T>   add ticker to session watchlist
        """
        async def search(self, query: str) -> Hits:
            app = self.app
            q   = query.strip()

            if not q:
                # Show pinned + recent searches
                for t in WS.pinned_tickers[:4]:
                    yield Hit(1.0, lambda tk=t: app.do_quote(tk),
                              f"📌 QUOTE {tk}", highlight=f"Pinned: {tk}")
                for s in WS.saved_searches[:3]:
                    yield Hit(0.7, lambda sq=s: app.do_search(sq),
                              f"🔍 SEARCH {s}", highlight=f"Recent: {s}")
                return

            upper = q.upper()
            parts = upper.split(None, 1)
            cmd   = parts[0]
            arg   = parts[1] if len(parts) > 1 else ""

            dispatch = {
                "QUOTE":  lambda: app.do_quote(arg or upper),
                "Q":      lambda: app.do_quote(arg or upper),
                "CHART":  lambda: app.do_chart(arg or upper),
                "C":      lambda: app.do_chart(arg or upper),
                "NEWS":   lambda: app.do_news(arg or upper),
                "N":      lambda: app.do_news(arg or upper),
                "FILING": lambda: app.do_filing(arg or upper),
                "F":      lambda: app.do_filing(arg or upper),
                "SEARCH": lambda: app.do_search(arg or q),
                "S":      lambda: app.do_search(arg or q),
                "PIN":    lambda: app.do_pin(arg or upper),
                "UNPIN":  lambda: app.do_unpin(arg or upper),
                "WATCH":  lambda: app.do_watch(arg or upper),
            }

            if cmd in dispatch:
                label = f"{cmd} {arg}".strip()
                yield Hit(1.0, dispatch[cmd], label,
                          highlight=f"{cmd} command → {arg or q}")
                return

            # NOTE command with inline text
            if cmd == "NOTE":
                note_parts = arg.split(None, 1)
                if len(note_parts) >= 2:
                    tk, note = note_parts[0], note_parts[1]
                    yield Hit(1.0, lambda t=tk, n=note: app.do_note(t, n),
                              f"NOTE {tk}: {note[:30]}",
                              highlight=f"Save note for {tk}")
                    return

            # Bare ticker
            if (len(q) <= 8
                    and q.replace("-","").replace(".","").replace("^","").isalnum()):
                tk = q.upper()
                pin_label = "📌 " if tk in WS.pinned_tickers else ""
                note = WS.get_note(tk)
                note_hint = f" [{note[:20]}]" if note else ""
                yield Hit(0.9, lambda t=tk: app.do_quote(t),
                          f"QUOTE {tk}{note_hint}", highlight=f"{pin_label}Price/sentiment")
                yield Hit(0.85, lambda t=tk: app.do_chart(t),
                          f"CHART {tk}", highlight="Chart")
                yield Hit(0.8, lambda t=tk: app.do_news(t),
                          f"NEWS {tk}", highlight="News")
                yield Hit(0.75, lambda t=tk: app.do_filing(t),
                          f"FILING {tk}", highlight="Filings")
                if tk in WS.pinned_tickers:
                    yield Hit(0.6, lambda t=tk: app.do_unpin(t),
                              f"UNPIN {tk}", highlight="Remove pin")
                else:
                    yield Hit(0.6, lambda t=tk: app.do_pin(t),
                              f"PIN {tk}", highlight="Pin ticker")
                return

            # Fallback: search
            yield Hit(0.7, lambda sq=q: app.do_search(sq),
                      f"SEARCH {q}", highlight=f"Search: {q}")


# ── background threads ────────────────────────────────────────────────────────

def _run_price_refresh():
    log.info("price thread started (%ds)", PRICE_REFRESH_SEC)
    while True:
        try:
            all_tickers = [t for tl in WATCHLIST.values() for t in tl]
            all_tickers += WS.watchlist_extra
            rows = fetch_prices(all_tickers)
            save_prices(rows)
            CACHE.update_prices(rows)
            CACHE.clear_error("prices")
        except Exception as e:
            CACHE.record_error("prices", e)
        time.sleep(PRICE_REFRESH_SEC)

def _run_news_refresh():
    log.info("news thread started (%ds)", REFRESH_SEC)
    while True:
        try:
            prices_snap = dict(CACHE.prices)
            articles    = fetch_rss(prices_snap) + fetch_newsapi(prices_snap)
            n_new       = save_articles(articles)
            if NEWS_API_KEY:
                for ticker in WATCHLIST["Equities"] + list(WATCHLIST["Macro/FX"])[:6]:
                    save_articles(fetch_ticker_news(ticker, prices_snap))
                    time.sleep(0.05)
            compute_aggregate_scores()
            CACHE.update_scores()
            CACHE.update_articles()
            CACHE.update_entity_summary()
            CACHE.set_last_refresh()
            CACHE.clear_error("ingest")
        except Exception as e:
            CACHE.record_error("ingest", e)
        time.sleep(REFRESH_SEC)

def _run_fred_refresh():
    log.info("FRED thread started")
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
    log.info("correlation thread started")
    while True:
        try:
            tickers = (WATCHLIST["Equities"] + WATCHLIST["Sectors"] +
                       WATCHLIST["Macro/FX"] + WATCHLIST["Bonds"])
            CACHE.update_correlations(compute_correlations(tickers))
            CACHE.clear_error("corr")
        except Exception as e:
            CACHE.record_error("corr", e)
        time.sleep(600)

def _run_analytics_refresh():
    global _regime_cache, _momentum_cache, _leadlag_cache, _bayes_cache, _accuracy_stats
    log.info("analytics thread started")
    while True:
        try:
            _regime_cache   = detect_regime(CACHE.scores)
            for k in list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]:
                _momentum_cache[k] = compute_sentiment_momentum(k)
            for ticker in WATCHLIST["Equities"] + WATCHLIST["Sectors"][:4]:
                bay = compute_bayesian_signal(ticker)
                _leadlag_cache[ticker] = compute_lead_lag(ticker)
                _bayes_cache[ticker]   = bay
                # Record signal for outcome tracking
                if bay.get("direction") and bay.get("confidence", 0) >= 0.60:
                    record_signal(ticker, bay["direction"],
                                  bay["confidence"], source="bayesian")
            _accuracy_stats = get_accuracy_stats(days_back=30)
            CACHE.clear_error("analytics")
        except Exception as e:
            CACHE.record_error("analytics", e)
        time.sleep(300)


# ══════════════════════════════════════════════════════════════════════════════
#  APP
# ══════════════════════════════════════════════════════════════════════════════

class ESMApp(App):
    CSS = """
    Screen        { background: #080808; }
    Header        { display: none; }
    Footer        { background: #0c0c0c; color: #3a3a3a; height: 1; }
    TabbedContent { height: 1fr; }
    Tabs          { background: #0c0c0c; border-bottom: solid #2a2a00; height: 2; }
    Tab           { background: #0c0c0c; color: #404040; padding: 0 1;
                    height: 2; content-align: center middle; }
    Tab:hover     { background: #141414; color: #888855; }
    Tab.-active   { background: #0c0c00; color: #ccaa00;
                    border-bottom: solid #ccaa00; text-style: bold; }
    TabPane       { padding: 0; }

    /* ── header rows ──────────────────────────────────────────────────── */
    #hdr_main    { height: 1; background: #0a0800; color: #ccaa00;
                   text-style: bold; padding: 0 1; }
    #hdr_fg      { height: 1; background: #0c0900; color: #aa8800;
                   padding: 0 1; }
    #hdr_regime  { height: 1; background: #08080f; color: #5566bb;
                   padding: 0 1; }
    #hdr_macro   { height: 1; background: #080808; color: #336633;
                   padding: 0 1; }
    #hdr_ws      { height: 1; background: #090908; color: #445544;
                   padding: 0 1; }

    /* ── tables ───────────────────────────────────────────────────────── */
    DataTable     { height: 1fr; background: #080808; }
    DataTable > .datatable--header { background: #101008; color: #ccaa00;
                                     text-style: bold; }
    DataTable > .datatable--cursor { background: #182818; }
    DataTable > .datatable--hover  { background: #0f0f0f; }

    /* ── chart / static views ─────────────────────────────────────────── */
    #chart_view   { height: 1fr; background: #080808; color: #33aa33;
                    padding: 1 2; overflow-y: auto; }
    """

    if _CMD_PALETTE:
        COMMANDS = {TerminalCommands}

    BINDINGS = [
        ("ctrl+p", "command_palette", "Command"),
        ("r",      "refresh",         "Refresh"),
        ("q",      "quit",            "Quit"),
        ("?",      "show_help",       "Help"),
        ("c",      "open_cfg",        "Config"),
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
        ("f", "switch_tab('filings')",    "Filings"),
        ("x", "switch_tab('charts')",     "Charts"),
        ("b", "switch_tab('backtest')",   "Backtest"),
        ("l", "switch_tab('errorlog')",   "Log"),
    ]

    TITLE     = "ESM  //  Economic Sentiment Monitor  v0.10"
    SUB_TITLE = "Ctrl+P = command  │  ? = help"

    def compose(self) -> ComposeResult:
        # ── compact 5-row header — no file paths anywhere ─────────────────
        yield Static("", id="hdr_main")    # row 1: model + counts + clock
        yield Static("", id="hdr_fg")      # row 2: fear/greed + tab shortcuts
        yield Static("", id="hdr_regime")  # row 3: regime + signals
        yield Static("", id="hdr_macro")   # row 4: live macro sentiment scores
        yield Static("", id="hdr_ws")      # row 5: workspace (pins, searches)
        with TabbedContent(initial=WS.active_tab):
            with TabPane("1:EQUITIES",   id="equities"):
                yield DataTable(id="eq_table",        zebra_stripes=True)
            with TabPane("2:MACRO/FX",   id="macrofx"):
                yield DataTable(id="fx_table",        zebra_stripes=True)
            with TabPane("3:BONDS",      id="bonds"):
                yield DataTable(id="bond_table",      zebra_stripes=True)
            with TabPane("4:SECTORS",    id="sectors"):
                yield DataTable(id="sec_table",       zebra_stripes=True)
            with TabPane("5:SENTIMENT",  id="sentiment"):
                yield DataTable(id="sent_table",      zebra_stripes=True)
            with TabPane("6:FRED DATA",  id="macrodata"):
                yield DataTable(id="fred_table",      zebra_stripes=True)
            with TabPane("7:NEWS FEED",  id="newsfeed"):
                yield DataTable(id="news_table",      zebra_stripes=True)
            with TabPane("8:ENTITIES",   id="entities"):
                yield DataTable(id="ent_table",       zebra_stripes=True)
            with TabPane("9:ANALYTICS",  id="analytics"):
                yield DataTable(id="analytics_table", zebra_stripes=True)
            with TabPane("0:ALERTS",     id="alerts"):
                yield DataTable(id="alert_table",     zebra_stripes=True)
            with TabPane("F:FILINGS",    id="filings"):
                yield DataTable(id="filing_table",    zebra_stripes=True)
            with TabPane("X:CHARTS",     id="charts"):
                yield Static("Loading…", id="chart_view")
            with TabPane("B:BACKTEST",   id="backtest"):
                yield DataTable(id="backtest_table",  zebra_stripes=True)
            with TabPane("L:LOG",        id="errorlog"):
                yield DataTable(id="log_table",       zebra_stripes=True)
        yield Footer()

    def on_mount(self):
        self._setup_tables()
        # Seed instruments table
        try:
            from market_terminal.config.settings import SECTORS, WATCHLIST
            tickers = {}
            for symbol in WATCHLIST.get("Equities", []):
                tickers[symbol] = {"asset_class": "equity"}
            for symbol in WATCHLIST.get("Macro/FX", []):
                tickers[symbol] = {"asset_class": "fx" if "=" in symbol else "commodity"}
            for symbol in WATCHLIST.get("Bonds", []):
                tickers[symbol] = {"asset_class": "bond", "currency": "USD"}
            for symbol, name in SECTORS.items():
                tickers[symbol] = {"asset_class": "etf", "sector": name}
            seed_instruments(tickers)
        except Exception as e:
            log.warning("seed_instruments: %s", e)

        # Start background threads
        threading.Thread(target=_run_price_refresh,       daemon=True).start()
        threading.Thread(target=_run_news_refresh,        daemon=True).start()
        threading.Thread(target=_run_fred_refresh,        daemon=True).start()
        threading.Thread(target=_run_correlation_refresh, daemon=True).start()
        threading.Thread(target=_run_analytics_refresh,   daemon=True).start()
        threading.Thread(target=run_alert_worker,   args=(CACHE,), daemon=True).start()
        threading.Thread(target=run_filings_worker, args=(CACHE,), daemon=True).start()
        threading.Thread(target=run_outcome_worker, daemon=True).start()
        if FULL_TEXT_ENABLED and NEWSPAPER_READY:
            threading.Thread(target=fulltext_worker, args=(CACHE,), daemon=True).start()

        self.set_interval(8, self._ui_refresh)
        self.set_timer(3,    self._ui_refresh)

    def on_tabbed_content_tab_activated(self, event):
        """Persist active tab to workspace."""
        WS.set_active_tab(event.tab.id)

    def action_switch_tab(self, tab_id: str):
        self.query_one(TabbedContent).active = tab_id
        WS.set_active_tab(tab_id)

    # ── command actions ───────────────────────────────────────────────────────

    def do_quote(self, ticker: str):
        ticker = ticker.upper().strip()
        WS._touch(ticker)
        p = CACHE.prices.get(ticker, {})
        s1, s4, s24 = self._s(ticker)
        note = WS.get_note(ticker)
        msg = (f"QUOTE {ticker}\n"
               f"Price: {p.get('price','—')}  Chg: {p.get('change_pct','—')}%\n"
               f"Sent 1h: {fmt_score(s1)}  24h: {fmt_score(s24)}\n")
        if note:
            msg += f"📝 Note: {note}"
        self.notify(msg, title=f"QUOTE {ticker}", timeout=8)
        self.action_switch_tab("equities")

    def do_chart(self, ticker: str):
        global _chart_ticker, _chart_data
        _chart_ticker = ticker.upper().strip()
        _chart_data   = get_price_history(_chart_ticker, 80)
        WS.set_chart_ticker(_chart_ticker)
        self.action_switch_tab("charts")
        self._refresh_chart()

    def do_news(self, ticker: str):
        global _search_results
        ticker = ticker.upper().strip()
        _search_results = news_for_ticker(ticker, limit=40)
        WS.add_search(f"NEWS:{ticker}")
        self.action_switch_tab("newsfeed")
        self._refresh_news()

    def do_filing(self, ticker: str):
        global _search_results
        ticker = ticker.upper().strip()
        _search_results = search_filings("", ticker=ticker, limit=40)
        WS.add_search(f"FILING:{ticker}")
        self.action_switch_tab("filings")
        self._refresh_filings()

    def do_search(self, query: str):
        global _search_results
        _search_results = search_all(query, limit=40)
        WS.add_search(query)
        self.notify(f"Search '{query}' → {len(_search_results)} results", timeout=4)
        self.action_switch_tab("newsfeed")
        self._refresh_news()

    def do_pin(self, ticker: str):
        ticker = ticker.upper().strip()
        WS.pin_ticker(ticker)
        self.notify(f"📌 Pinned {ticker}", timeout=3)

    def do_unpin(self, ticker: str):
        ticker = ticker.upper().strip()
        WS.unpin_ticker(ticker)
        self.notify(f"Unpinned {ticker}", timeout=3)

    def do_watch(self, ticker: str):
        ticker = ticker.upper().strip()
        WS.add_watchlist_ticker(ticker)
        self.notify(f"Added {ticker} to session watchlist", timeout=3)

    def do_note(self, ticker: str, note: str):
        ticker = ticker.upper().strip()
        WS.set_note(ticker, note)
        self.notify(f"📝 Note saved for {ticker}", timeout=3)

    # ── table setup ───────────────────────────────────────────────────────────
    def _setup_tables(self):
        self.query_one("#eq_table", DataTable).add_columns(
            "📌", "Ticker", "Price", "Chg%", "Open", "High", "Low",
            "Vol", "Fresh", "Event", "Sent 1h", "Spark",
            "Sent 4h", "Sent 24h", "Corr", "Lag", "Bayes", "Div")
        self.query_one("#fx_table", DataTable).add_columns(
            "Asset", "Price", "Chg%", "Open", "High", "Low",
            "Vol", "Fresh", "Sent 1h", "Spark", "Sent 24h",
            "Corr", "Trend", "Vel", "Label")
        self.query_one("#bond_table", DataTable).add_columns(
            "Instrument", "Price/Yield", "Chg%", "Open", "High", "Low",
            "Vol", "Fresh", "Sent 1h", "Spark", "Sent 24h",
            "Corr", "Credit", "Label")
        self.query_one("#sec_table", DataTable).add_columns(
            "ETF", "Sector", "Price", "Chg%", "Open", "High", "Low",
            "Vol", "Fresh", "Event", "Sent 1h", "Spark", "Bar",
            "Sent 24h", "Corr", "Vel", "Bayes")
        self.query_one("#sent_table", DataTable).add_columns(
            "Theme", "1h", "4h", "24h", "Spark",
            "Trend", "Accel", "Consec", "Vel", "n(1h)", "Context")
        self.query_one("#fred_table", DataTable).add_columns(
            "Series", "Theme", "Latest", "Prior", "Surprise%", "As Of", "Signal")
        self.query_one("#news_table", DataTable).add_columns(
            "Time", "Source", "Tier", "Event", "Score",
            "FB", "VD", "Nums", "FT", "Ticker", "Headline")
        self.query_one("#ent_table", DataTable).add_columns(
            "Ticker", "📝", "Mentions 24h", "Avg Sent",
            "Min", "Max", "Spark", "WL")
        self.query_one("#analytics_table", DataTable).add_columns(
            "Asset/Theme", "Type", "Value", "Detail", "Conf")
        self.query_one("#alert_table", DataTable).add_columns(
            "Time", "Sev", "Regime", "Key", "Reason", "Score")
        self.query_one("#filing_table", DataTable).add_columns(
            "Ticker", "Company", "Form", "Filed", "Sentiment", "Description")
        self.query_one("#backtest_table", DataTable).add_columns(
            "Ticker", "Signals", "1h Acc", "4h Acc", "24h Acc", "Best Horizon")
        self.query_one("#log_table", DataTable).add_columns(
            "Thread", "Status", "Last OK", "Errs", "Detail")

    # ── master UI refresh ─────────────────────────────────────────────────────
    def _ui_refresh(self):
        # ── ROW 1: status strip ───────────────────────────────────────────────
        errs    = CACHE.err_counts
        total_e = sum(errs.values())
        err_s   = ("⚠ " + " ".join(f"{k[0].upper()}:{v}"
                                    for k,v in errs.items() if v>0) + "  "
                   if total_e else "")
        nlp_s   = ("FB+VD" if FINBERT_READY else "VD") + ("·NER" if SPACY_READY else "")
        ft_s    = f"FT:{CACHE.fulltext_count}"
        art_s   = f"Arts:{CACHE.article_count:,}"
        fil_s   = f"Fil:{CACHE.filing_count}"
        feed_s  = f"Feeds:{len(RSS_FEEDS)}"
        api_s   = ("API:✓" if NEWS_API_KEY else "") + ("  FRED:✓" if FRED_API_KEY else "")

        # Data freshness — show only stale feeds
        CACHE.update_freshness()
        fresh = CACHE.freshness
        stale_parts = []
        for feed in ["rss","newsapi","fred","filings","alerts"]:
            f  = fresh.get(feed, {})
            lb = staleness_label(f.get("last_ok"), warn_s=180, crit_s=600)
            if lb != "✓ live":
                stale_parts.append(f"{feed[0].upper()}:{lb}")
        stale_s = "  ⚠ " + " ".join(stale_parts) if stale_parts else ""
        price_fresh = CACHE.get_price_staleness("SPY")
        price_ok    = "" if price_fresh == "✓ live" else f"  SPY:{price_fresh}"

        self.query_one("#hdr_main", Static).update(
            f" ESM v0.11  │  {nlp_s}  │  {art_s}  {ft_s}  {fil_s}  {feed_s}"
            f"  │  ↻{CACHE.last_refresh}  P:{PRICE_REFRESH_SEC}s N:{REFRESH_SEC}s"
            f"  │  {api_s}"
            f"{price_ok}{stale_s}"
            f"{('  │  ' + err_s) if err_s else ''}"
            f"  │  Ctrl+P"
        )

        # ── ROW 2: fear/greed bar + tab shortcuts ─────────────────────────────
        fg_val, fg_label = fear_greed_index(CACHE.scores)
        bar_len = 20
        filled  = max(0, min(int((fg_val+100)/200*bar_len), bar_len))
        fg_bar  = "█"*filled + "░"*(bar_len-filled)
        self.query_one("#hdr_fg", Static).update(
            f" F/G [{fg_bar}] {fg_val:+d} {fg_label}"
            f"  │  1=Eq 2=FX 3=Bnd 4=Sec 5=Snt 6=FRD"
            f" 7=Nws 8=Ent 9=Anl 0=Alrt F=Fil X=Chrt B=Bkt L=Log"
        )

        # ── ROW 3: regime ─────────────────────────────────────────────────────
        reg = _regime_cache
        if reg:
            sigs = "  ".join(reg.get("signals",[])[:6])
            self.query_one("#hdr_regime", Static).update(
                f" {reg.get('label','NEUTRAL →')}"
                f"  {reg.get('confidence',0):.0%}"
                f"  FG={reg.get('fg_val',0):+d}"
                f"  σ={reg.get('std_sent',0):.3f}"
                f"  a={reg.get('accel',0):+.3f}"
                f"  │  {sigs}"
            )
        else:
            self.query_one("#hdr_regime", Static).update(
                " NEUTRAL →  awaiting analytics…"
            )

        # ── ROW 4: live macro sentiment scores (compact) ──────────────────────
        macro_parts = []
        for theme in ["rates","inflation","growth","employment","credit","trade","GLOBAL"]:
            s = CACHE.scores.get(theme, {}).get("1h", {}).get("score")
            if s is not None:
                short = {"rates":"RATE","inflation":"INFL","growth":"GRWTH",
                         "employment":"EMPL","credit":"CRDT","trade":"TRAD",
                         "GLOBAL":"GLOB"}.get(theme, theme[:4].upper())
                macro_parts.append(f"{short}:{s:+.3f}")
        self.query_one("#hdr_macro", Static).update(
            " " + "  ".join(macro_parts) if macro_parts
            else " macro scores loading…"
        )

        # ── ROW 5: workspace ──────────────────────────────────────────────────
        pins = WS.pinned_tickers[:8]
        pin_s  = " ".join(f"◆{t}" for t in pins) if pins else ""
        srch   = WS.saved_searches[:4]
        srch_s = "  ".join(f"/{s[:16]}" for s in srch) if srch else ""
        extras = WS.watchlist_extra[:4]
        ext_s  = " ".join(f"+{t}" for t in extras) if extras else ""
        parts  = []
        if pin_s:  parts.append(pin_s)
        if ext_s:  parts.append(ext_s)
        if srch_s: parts.append(f"srch: {srch_s}")
        n_notes = len(WS._data.get("notes", {}))
        if n_notes: parts.append(f"📝{n_notes}")
        self.query_one("#hdr_ws", Static).update(
            " " + ("  │  ".join(parts) if parts else "Ctrl+P → PIN <TICKER>  NOTE <TICKER> <text>  SEARCH <query>")
        )

        CACHE.update_scores()
        CACHE.update_articles()
        CACHE.update_entity_summary()
        CACHE.update_fred_data()
        CACHE.update_filings()

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
        self._refresh_filings()
        self._refresh_chart()
        self._refresh_backtest()
        self._refresh_log()

    # ── helpers ───────────────────────────────────────────────────────────────
    def _p(self, t): return CACHE.prices.get(t, {})

    def _price_cols(self, ticker: str):
        p = self._p(ticker)
        if not p: return ("—","—","—","—","—","—","—")
        return (f"{p.get('price',0):,.4f}",
                fmt_chg(p.get("change_pct",0)),
                f"{p.get('open',0):.4f}"  if p.get("open")  else "—",
                f"{p.get('high',0):.4f}"  if p.get("high")  else "—",
                f"{p.get('low', 0):.4f}"  if p.get("low")   else "—",
                fmt_vol(p.get("volume",0)),
                f"{p.get('vwap',0):.4f}"  if p.get("vwap")  else "—")

    def _s(self, key: str):
        d = CACHE.scores.get(key, {})
        return (d.get("1h",{}).get("score"),
                d.get("4h",{}).get("score"),
                d.get("24h",{}).get("score"))

    def _latest_event_type(self, ticker: str) -> str:
        """Get most recent event type for a ticker from entity_mentions → articles."""
        try:
            with get_db() as db:
                row = db.execute("""
                    SELECT a.event_type FROM articles a
                    JOIN entity_mentions em ON em.article_id=a.id
                    WHERE em.ticker=? AND a.event_type IS NOT NULL
                      AND a.event_type != 'general'
                    ORDER BY a.ingested DESC LIMIT 1
                """, (ticker,)).fetchone()
            return row[0] if row else ""
        except Exception:
            return ""

    # ── tab refreshers ────────────────────────────────────────────────────────
    def _refresh_equities(self):
        t = self.query_one("#eq_table", DataTable)
        t.clear()
        pins = set(WS.pinned_tickers)
        tickers = WS.pinned_tickers + [
            tk for tk in WATCHLIST["Equities"] if tk not in pins
        ] + [tk for tk in WS.watchlist_extra if tk not in pins]
        for ticker in tickers:
            pr,chg,op,hi,lo,vol,vwap = self._price_cols(ticker)
            s1,s4,s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            ll    = _leadlag_cache.get(ticker,{})
            lag_s = (f"{ll['best_lag']}h/{ll['best_r']:.2f}"
                     if ll.get("best_lag") and ll.get("best_r") else "n/a")
            bay   = _bayes_cache.get(ticker,{})
            bay_s = bay.get("signal_str","n/a")[:16] if bay else "n/a"
            div   = divergence_score(ticker)
            fresh = CACHE.get_price_staleness(ticker)
            evt   = event_type_label(self._latest_event_type(ticker))
            pin_m = "📌" if ticker in pins else ""
            t.add_row(pin_m, ticker, pr, chg, op, hi, lo, vol, fresh, evt,
                      fmt_score(s1), spark, fmt_score(s4), fmt_score(s24),
                      fmt_corr(corr), lag_s, bay_s, div)

    def _refresh_macrofx(self):
        t = self.query_one("#fx_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Macro/FX"]:
            pr,chg,op,hi,lo,vol,_ = self._price_cols(ticker)
            s1,s4,s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            trend = ("▲" if (s1 or 0)>(s4 or 0)+0.01 else
                     "▼" if (s1 or 0)<(s4 or 0)-0.01 else "→"
                     ) if s1 is not None and s4 is not None else "—"
            vel   = velocity_label(ticker)
            lbl   = "POS" if (s1 or 0)>=0.05 else ("NEG" if (s1 or 0)<=-0.05 else "NEU")
            fresh = CACHE.get_price_staleness(ticker)
            t.add_row(ticker,pr,chg,op,hi,lo,vol,fresh,
                      fmt_score(s1),spark,fmt_score(s24),
                      fmt_corr(corr),trend,vel,lbl)

    def _refresh_bonds(self):
        t = self.query_one("#bond_table", DataTable)
        t.clear()
        for ticker in WATCHLIST["Bonds"]:
            pr,chg,op,hi,lo,vol,_ = self._price_cols(ticker)
            s1,s4,s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            credit_s = fmt_score(CACHE.scores.get("credit",{}).get("1h",{}).get("score"))
            lbl  = "POS" if (s1 or 0)>=0.05 else ("NEG" if (s1 or 0)<=-0.05 else "NEU")
            fresh= CACHE.get_price_staleness(ticker)
            t.add_row(ticker,pr,chg,op,hi,lo,vol,fresh,
                      fmt_score(s1),spark,fmt_score(s24),
                      fmt_corr(corr),credit_s,lbl)

    def _refresh_sectors(self):
        t = self.query_one("#sec_table", DataTable)
        t.clear()
        for ticker, name in SECTORS.items():
            pr,chg,op,hi,lo,vol,_ = self._price_cols(ticker)
            s1,s4,s24 = self._s(ticker)
            spark = sparkline(get_sparkline_history(ticker))
            corr  = CACHE.correlations.get(ticker)
            vel   = velocity_label(ticker)
            bay   = _bayes_cache.get(ticker,{})
            bay_s = bay.get("signal_str","—")[:14] if bay else "—"
            fresh = CACHE.get_price_staleness(ticker)
            evt   = event_type_label(self._latest_event_type(ticker))
            t.add_row(ticker,name,pr,chg,op,hi,lo,vol,fresh,evt,
                      fmt_score(s1),spark,sentiment_bar(s1 or 0),
                      fmt_score(s24),fmt_corr(corr),vel,bay_s)

    def _refresh_sentiment(self):
        t = self.query_one("#sent_table", DataTable)
        t.clear()
        for theme in list(MACRO_KEYWORDS.keys()) + ["GLOBAL"]:
            d   = CACHE.scores.get(theme,{})
            s1  = d.get("1h",{}).get("score")
            s4  = d.get("4h",{}).get("score")
            s24 = d.get("24h",{}).get("score")
            n1  = d.get("1h",{}).get("count",0)
            spark  = sparkline(get_sparkline_history(theme))
            vel    = velocity_label(theme)
            mom    = _momentum_cache.get(theme,{})
            accel  = mom.get("acceleration")
            consec = mom.get("consecutive",0)
            accel_s= f"{accel:+.3f}" if accel is not None else "—"
            trend  = ("▲" if (s1 or 0)>(s4 or 0)+0.01 else
                      "▼" if (s1 or 0)<(s4 or 0)-0.01 else "→"
                      ) if s1 and s4 else "—"
            ctx    = ("Bullish" if (s1 or 0)>0.1 else
                      "Bearish" if (s1 or 0)<-0.1 else
                      "Mixed"   if s1 is not None else "—")
            t.add_row(theme.upper(),fmt_score(s1),fmt_score(s4),
                      fmt_score(s24),spark,trend,accel_s,
                      str(consec),vel,str(n1 or "—"),ctx)

    def _refresh_fred(self):
        t = self.query_one("#fred_table", DataTable)
        t.clear()
        if not CACHE.fred_data:
            t.add_row("—","—","No FRED data — set fred_api_key","—","—","—","—"); return
        for row in CACHE.fred_data:
            val_s  = f"{row['value']:.4f}"      if row["value"]      is not None else "—"
            prev_s = f"{row['prev_value']:.4f}" if row["prev_value"] is not None else "—"
            surp   = row["surprise"] or 0.0
            signal = "↑ boost" if surp>=0.5 else ("↓ dampen" if surp<=-0.5 else "—")
            t.add_row(row["label"],row["theme"].upper(),val_s,prev_s,
                      f"{surp:+.2f}%",(row["ts"] or "")[:10],signal)

    def _refresh_news(self):
        t = self.query_one("#news_table", DataTable)
        t.clear()
        source = _search_results if _search_results else CACHE.recent_articles
        for art in source[:200]:
            pub  = (art.get("published") or "")[:16]
            src  = (art.get("source")    or "")[:14]
            tier = art.get("source_tier","C")
            ft_s = "✓" if art.get("full_text") else "·"
            evt  = event_type_label(art.get("event_type",""))
            try:   nums = json.loads(art.get("numeric_signals","{}") or "{}"); \
                   num_s = " ".join(f"{k[:3]}:{v}" for k,v in list(nums.items())[:2])
            except: num_s = ""
            try:
                ents   = json.loads(art.get("entities","[]") or "[]")
                linked = ",".join(e["ticker"] for e in ents[:2]) if ents else "—"
            except: linked = art.get("ticker","—") or "—"
            title = (art.get("title") or "")[:75]
            t.add_row(pub,src,tier,evt,
                      fmt_score(art.get("score")),
                      fmt_score(art.get("finbert_score")),
                      fmt_score(art.get("vader_score")),
                      num_s[:14],ft_s,linked[:14],title)

    def _refresh_entities(self):
        t = self.query_one("#ent_table", DataTable)
        t.clear()
        all_wl = {tk for tl in WATCHLIST.values() for tk in tl}
        for row in CACHE.entity_summary:
            spk   = sparkline(get_sparkline_history(row["ticker"]),8)
            in_wl = "✓" if row["ticker"] in all_wl else "—"
            note  = "📝" if WS.get_note(row["ticker"]) else ""
            t.add_row(row["ticker"],note,str(row["mentions"]),
                      fmt_score(row["avg_score"]),fmt_score(row["min_score"]),
                      fmt_score(row["max_score"]),spk,in_wl)

    def _refresh_analytics(self):
        t = self.query_one("#analytics_table", DataTable)
        t.clear()
        reg = _regime_cache
        if reg:
            t.add_row("MARKET","Regime",reg.get("label","—"),
                      "  ".join(reg.get("signals",[])[:5]),
                      f"{reg.get('confidence',0):.0%}")
        mom = _momentum_cache.get("GLOBAL",{})
        if mom.get("acceleration") is not None:
            t.add_row("GLOBAL","Momentum",
                      f"{mom['direction']} {mom['consecutive']}x",
                      f"accel={mom['acceleration']:+.4f}","—")
        for theme in MACRO_KEYWORDS:
            mom = _momentum_cache.get(theme,{})
            if mom.get("acceleration") is not None:
                t.add_row(theme.upper(),"Momentum",
                          f"{mom.get('direction','—')} {mom.get('consecutive',0)}x",
                          f"accel={mom.get('acceleration',0):+.4f}","—")
        for ticker,ll in _leadlag_cache.items():
            if ll.get("best_r") and abs(ll["best_r"])>0.1:
                ld = "  ".join(f"{h}h:{r:.2f}" for h,r in
                               sorted(ll.get("by_lag",{}).items()) if r is not None)
                t.add_row(ticker,"Lead-Lag",
                          f"best={ll['best_lag']}h r={ll['best_r']:.3f}",
                          ld[:50],f"|r|={abs(ll['best_r']):.2f}")
        for ticker,bay in _bayes_cache.items():
            if bay.get("sources_used",0)>0:
                top = "  ".join(
                    f"{s}:{a:.0%}" for s,a in
                    sorted(bay.get("source_acc",{}).items(),
                           key=lambda x:abs(x[1]-0.5),reverse=True)[:3])
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
            k = r[2]+r[3]
            if k not in seen:
                seen.add(k)
                t.add_row(r[0][:19],r[1],reg_label,r[2],r[3][:60],fmt_score(r[4]))

    def _refresh_filings(self):
        t = self.query_one("#filing_table", DataTable)
        t.clear()
        source = _search_results if _search_results else CACHE.filings
        if not source:
            t.add_row("—","—","—","—","—","EDGAR worker initialising (30min cycle)"); return
        for f in source[:150]:
            sent_s = f"{f.get('sentiment',0.0):+.3f}" if f.get('sentiment') else "n/a"
            t.add_row(f.get("ticker","—"),(f.get("company") or "—")[:28],
                      f.get("form_type","—"),(f.get("filed_at") or "—")[:10],
                      sent_s,(f.get("description") or "")[:65])

    def _refresh_chart(self):
        widget = self.query_one("#chart_view", Static)
        data   = _chart_data or get_price_history(_chart_ticker, 80)
        sent_hist = get_sparkline_history(_chart_ticker, 40)

        p   = CACHE.prices.get(_chart_ticker, {})
        s1,s4,s24 = self._s(_chart_ticker)
        corr = CACHE.correlations.get(_chart_ticker)
        bay  = _bayes_cache.get(_chart_ticker,{})
        ll   = _leadlag_cache.get(_chart_ticker,{})
        note = WS.get_note(_chart_ticker)

        price_str = f"{p['price']:,.4f}" if p.get("price") else "—"
        chg_str   = fmt_chg(p.get("change_pct", 0)) if p else "—"
        lag_str   = ll.get("best_lag", "?")
        bay_str   = bay.get("signal_str", "—")[:22]
        header = (
            f"\n  ┌─ {_chart_ticker}"
            f"  {price_str}"
            f"  {chg_str}"
            f"  │  Sent1h:{fmt_score(s1)}  24h:{fmt_score(s24)}"
            f"  Corr:{fmt_corr(corr)}"
            f"  Lag:{lag_str}h"
            f"  │  {bay_str}\n"
        )
        if note:
            header += f"  📝 {note[:70]}\n"

        if len(data) >= 2:
            chart = ascii_chart(data, width=60, height=10,
                                label=f"{_chart_ticker} PRICE")
        else:
            chart = "  (awaiting price history — prices accumulate after first refresh)"

        sent_section = (
            f"\n  SENTIMENT (1h windows, 24h history) "
            f"─────────────────────────────────────\n"
            f"  {sparkline(sent_hist, width=60)}\n"
        )
        pins_line = (
            "  📌 Pinned: " + "  ".join(WS.pinned_tickers[:8])
            if WS.pinned_tickers else ""
        )
        controls = "\n  Ctrl+P: CHART <TICKER>  │  PIN <TICKER>  │  NOTE <TICKER> <text>"
        widget.update(header + chart + sent_section + pins_line + controls)

    def _refresh_backtest(self):
        """NEW: signal accuracy from resolved signal_outcomes."""
        t = self.query_one("#backtest_table", DataTable)
        t.clear()
        stats = _accuracy_stats
        if not stats or not stats.get("total"):
            t.add_row("—","0","—","—","—","Signals accumulate after 24h — check again later")
            return
        # Global row
        t.add_row("ALL",
                  str(stats["total"]),
                  fmt_pct(stats["correct_1h"]),
                  fmt_pct(stats["correct_4h"]),
                  fmt_pct(stats["correct_24h"]),
                  _best_horizon(stats))
        # Per-ticker
        for ticker, tk_stats in list(stats.get("by_ticker",{}).items())[:30]:
            best = _best_horizon(tk_stats)
            t.add_row(ticker,
                      str(tk_stats["total"]),
                      fmt_pct(tk_stats["correct_1h"]),
                      fmt_pct(tk_stats["correct_4h"]),
                      fmt_pct(tk_stats["correct_24h"]),
                      best)

    def _refresh_log(self):
        t = self.query_one("#log_table", DataTable)
        t.clear()
        fresh = CACHE.freshness
        for key, label in [
            ("ingest","News ingest"),("prices","Price feed"),
            ("fulltext","Full-text"),("fred","FRED macro"),
            ("filings","EDGAR filings"),("corr","Correlation"),
            ("analytics","Analytics"),
        ]:
            n    = CACHE.err_counts.get(key,0)
            last = CACHE.last_errors.get(key,"") or "—"
            f    = fresh.get(key,{})
            last_ok    = (f.get("last_ok") or "—")[:19]
            item_count = f.get("item_count", 0)
            t.add_row(label,
                      "✓ OK" if n==0 else f"✗ {n} err",
                      last_ok,
                      str(f.get("err_count", n)),
                      f"{item_count} items" if item_count else last[:55])

    # ── actions ───────────────────────────────────────────────────────────────
    def action_refresh(self): self._ui_refresh()

    def action_open_cfg(self):
        self.notify(
            "Edit ~/.esm/config.yml to customise:\n"
            "  watchlists, rss_feeds, refresh intervals,\n"
            "  alert thresholds, API keys\n\n"
            "Restart to apply changes.\n"
            "Workspace state auto-saves to ~/.esm/workspace.json",
            title="⚙  Configuration", timeout=12)

    def action_show_help(self):
        self.notify(
            "COMMAND PALETTE  Ctrl+P\n"
            "  QUOTE <T>    price + sentiment\n"
            "  CHART <T>    ASCII price chart\n"
            "  NEWS <T>     news filtered to ticker\n"
            "  FILING <T>   SEC filings for ticker\n"
            "  SEARCH <q>   full-text search\n"
            "  PIN <T>      pin to top of Equities\n"
            "  UNPIN <T>    remove pin\n"
            "  WATCH <T>    add to session watchlist\n"
            "  NOTE <T> <text>  save note for ticker\n\n"
            "TABS\n"
            "  1=Equities  2=Macro/FX  3=Bonds  4=Sectors\n"
            "  5=Sentiment 6=FRED      7=News   8=Entities\n"
            "  9=Analytics 0=Alerts   F=Filings X=Chart\n"
            "  B=Backtest  L=Log\n\n"
            "KEYS  R=Refresh  C=Config  Q=Quit  ?=Help",
            title="ESM v0.11  Help", timeout=30)


def _best_horizon(stats: dict) -> str:
    best_h, best_v = "—", 0.0
    for h, k in [(1, "correct_1h"), (4, "correct_4h"), (24, "correct_24h")]:
        v = stats.get(k, 0.0) or 0.0
        if v > best_v:
            best_v, best_h = v, f"{h}h ({v:.0%})"
    return best_h
