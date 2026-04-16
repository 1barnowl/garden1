# Economic Sentiment Monitor  v0.11

A Bloomberg-style terminal TUI that ingests financial news, scores sentiment with VADER + FinBERT, tracks market prices, and surfaces real-time macro signals — all in a local SQLite-backed Python app.

```
┌─────────────────────────────────────────────────────────────────┐
│  ESM  //  Economic Sentiment Monitor  v0.8                      │
├─────────────────────────────────────────────────────────────────┤
│  [FB+VD]  NER:✓  FT:142  FRED:14s  │  Price:15s  News:30s     │
│  FEAR/GREED [████████░░░░░░░░░░░░░░░░░░] +24 GREED             │
│  REGIME: RISK-ON ▲  conf:72%  │  FG:+24 greed  vol:low        │
├──────────────────────────────────────────────────────────────────┤
│  1:EQUITIES  2:MACRO/FX  3:BONDS  4:SECTORS  5:SENTIMENT  ...  │
├──────────────────────────────────────────────────────────────────┤
│  Ticker  Price      Chg%    Sent 1h   Spark       Corr  Bayes  │
│  AAPL    189.4200  +0.84%  +0.1240  ▄▅▆▇█───   +0.41  BULL   │
│  MSFT    415.8100  +0.23%  +0.0890  ▃▄▄▅▆───   +0.33  BULL   │
│  NVDA    875.2300  +1.47%  +0.2180  ▅▆▇███───  +0.68  BULL   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Features

- **11 live tabs**: Equities, Macro/FX, Bonds, Sectors, Sentiment, FRED, News Feed, Entities, Analytics, Alerts, Log
- **Dual NLP scoring**: VADER (always on) + FinBERT (optional, 70/30 blend)
- **SpaCy NER** for entity extraction mapped to tickers
- **FRED macro data**: CPI, Fed Funds, GDP, unemployment, yields, VIX + surprise detection
- **Regime detection**: risk-on/off, high-vol, low-vol, neutral with fear/greed composite
- **Analytics**: Pearson correlation, lead/lag, sentiment momentum, Bayesian source reliability
- **Z-score alerts** with slow-drift detection
- **Full-text enrichment** via newspaper3k (optional)
- **Configurable** watchlists, RSS feeds, thresholds via `~/.esm/config.yml`

---

## Install

### 1. Clone

```bash
git clone https://github.com/yourname/market-terminal.git
cd market-terminal
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 3. Install core dependencies

```bash
pip install -r requirements.txt
```

### 4. Optional upgrades

```bash
# Better sentiment (FinBERT — needs ~2 GB RAM, slow first load)
pip install transformers torch

# Better entity extraction (SpaCy)
pip install spacy
python -m spacy download en_core_web_sm

# Full article body ingestion
pip install newspaper3k
```

---

## Configure `.env`

```bash
cp .env.example .env
# Edit .env:
NEWS_API_KEY=your_newsapi_key_here   # https://newsapi.org/
FRED_API_KEY=your_fred_key_here      # https://fred.stlouisfed.org/docs/api/api_key.html
```

API keys are optional — the app runs on RSS-only without them.

You can also edit `~/.esm/config.yml` (auto-created on first run) to customise watchlists, RSS feeds, refresh intervals, and alert thresholds.

---

## Initialize the database

The database is created automatically on first run. To do it explicitly:

```bash
python scripts/init_db.py
```

---

## Run

```bash
python main.py
```

### Key bindings

| Key | Action |
|-----|--------|
| `1–9`, `0`, `L` | Switch tabs |
| `R` | Force UI refresh |
| `C` | Show config path |
| `?` | Help |
| `Q` | Quit |

---

## Run tests

```bash
pip install pytest pytest-asyncio
pytest tests/
```

---

## Project layout

```
market_terminal/
├─ main.py                        # Entry point
├─ requirements.txt
├─ pyproject.toml
├─ .env.example
├─ src/
│  └─ market_terminal/
│     ├─ config/
│     │  └─ settings.py           # All config, watchlists, constants
│     ├─ storage/
│     │  ├─ database.py           # SQLite schema + migrations
│     │  └─ cache.py              # Thread-safe in-memory cache singleton
│     ├─ enrich/
│     │  ├─ scorer.py             # VADER + FinBERT scoring
│     │  └─ entities.py           # SpaCy NER + regex entity extraction
│     ├─ ingest/
│     │  ├─ feeds.py              # RSS, NewsAPI, yfinance, FRED ingestion
│     │  └─ aggregator.py         # Score roll-ups + alert detection
│     ├─ analytics/
│     │  └─ engine.py             # Correlation, lead/lag, regime, Bayesian
│     └─ ui/
│        └─ app.py                # Textual TUI + background threads
├─ tests/
├─ scripts/
│  └─ init_db.py
└─ data/
```

---

## Roadmap (v0.9+)

- [ ] FastAPI REST + WebSocket layer for web UI
- [ ] SEC EDGAR filing ingestion + full-text search
- [ ] Intraday chart widget (Plotly / Unicode sparklines)
- [ ] Command palette (`/` to search symbols, news, filings)
- [ ] PostgreSQL + TimescaleDB backend option
- [ ] Redis Streams for decoupled event pipeline
- [ ] Prometheus metrics + OpenTelemetry tracing
- [ ] Docker / docker-compose for one-command setup
