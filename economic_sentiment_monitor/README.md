# Economic Sentiment Monitor (ESM)

A terminal dashboard for real-time economic sentiment analysis.
100% free data sources. No paid subscriptions required.

```
╔═══════════════════════════════════════════════════════════════╗
║  ESM // Economic Sentiment Monitor          Last: 14:32:01   ║
╠═══╦═══════╦═══════╦══════════╦══════════╦═══════════╦════════╣
║ ↑ ║TICKER ║ PRICE ║  CHG%    ║  SENT 1h ║    BAR    ║ SENT24h║
╠═══╬═══════╬═══════╬══════════╬══════════╬═══════════╬════════╣
║   ║ SPY   ║ 523.4 ║  +0.34%  ║  +0.1234 ║────│███── ║+0.0987 ║
║   ║ QQQ   ║ 441.2 ║  -0.12%  ║  -0.0432 ║──█│────── ║-0.0211 ║
╚═══╩═══════╩═══════╩══════════╩══════════╩═══════════╩════════╝
   [EQUITIES] [MACRO/FX] [SECTORS] [SENTIMENT] [NEWS] [ALERTS]
```

## Quick Start

```bash
# 1. Clone / download the files
cd economic_sentiment_monitor

# 2. Run setup (installs deps)
bash setup.sh

# 3. Launch (works with no API keys — uses RSS + yfinance)
python3 monitor.py

# 4. Optional: add free API keys for more data
export NEWS_API_KEY=your_key_here
export FRED_API_KEY=your_key_here
python3 monitor.py
```

## Tabs

| Tab | Content |
|-----|---------|
|  EQUITIES | SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, META, JPM + sentiment overlay |
|  MACRO/FX | Gold, Oil, BTC, ETH, EUR/USD, GBP/USD, JPY, USD Index, bonds |
|  SECTORS | All 11 S&P sectors via ETF (XLK, XLF, XLE…) with heatmap |
|  SENTIMENT | Theme scores: inflation, rates, growth, employment, trade, credit |
|  NEWS FEED | Latest 60 headlines with VADER sentiment score |
|  ALERTS | Sharp sentiment-shift alerts (WARN/CRIT) |

## Data Sources (all free)

| Source | What | Key needed? |
|--------|------|-------------|
| RSS feeds (10 feeds) | Headlines from Reuters, CNBC, MarketWatch, Yahoo Finance, FT, Bloomberg, Economist | No |
| yfinance | Real-time prices, volume for all tickers | No |
| NewsAPI | Broader news search | Free key at newsapi.org |
| FRED | Macro indicators (CPI, GDP, unemployment) | Free key at fred.stlouisfed.org |

## Sentiment Score

Scores range from **-1.0** (very negative) to **+1.0** (very positive).

- **+0.05 to +1.0** → Positive (green)
- **-0.05 to +0.05** → Neutral (white)
- **-0.05 to -1.0** → Negative (red)

Uses VADER (fast-tier) for real-time scoring. Can be upgraded to FinBERT for higher accuracy.

## Alerts

Alerts fire when 1h sentiment deviates ≥ 0.15 from the 4h rolling baseline.

- **WARN** — delta ≥ 0.15
- **CRIT** — delta ≥ 0.25

## Architecture

```
RSS + NewsAPI + yfinance
       │
   Ingestion
   (feedparser, requests)
       │
   Normalizer
   (dedup, UTC timestamps)
       │
   NLP (VADER)
   (score, label, keywords)
       │
   SQLite store (~/.esm/sentiment.db)
       │
   Aggregator
   (1h/4h/24h rolling windows)
       │
   Alert engine
   (z-score threshold)
       │
   Textual TUI
   (tabbed Bloomberg-style terminal)
```

## Upgrading to FinBERT (optional, higher accuracy)

```bash
pip install transformers torch --break-system-packages
```

Then in `monitor.py`, replace the `score_text()` function with the FinBERT transformer pipeline.
See comments in `monitor.py` for the hook.

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `R` | Force refresh |
| `?` | Help / shortcuts |
| `Q` | Quit |
| `Tab` / click | Switch tabs |

## Roadmap

- [ ] FinBERT accurate-tier scoring
- [ ] FRED macro indicator overlays
- [ ] Entity linking (headline → ticker)
- [ ] TimescaleDB / InfluxDB for production storage
- [ ] Redis Streams / Kafka for high-volume ingestion
- [ ] REST API endpoint for signals
- [ ] Grafana dashboard export
- [ ] Multi-language support
