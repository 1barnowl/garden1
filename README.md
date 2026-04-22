# Garden Core

## Core Idea

Garden1 functions as a distributed, event-driven economic intelligence subsystem that ingests, canonicalizes, and enriches heterogeneous streams of unstructured financial text—news, filings, research reports, social media, and macroeconomic calendars—alongside synchronous market price and volume feeds to produce a continuous, provenance‑rich stream of quantifiable sentiment, thematic, and event‑based signals. It employs a tiered natural language processing pipeline, combining low‑latency lexicon‑based classifiers with high‑accuracy transformer‑based models, to extract entities, topics, sentiment polarity, and discrete economic events, then normalizes these outputs against a canonical entity registry that maps textual mentions to exchange‑standard tickers, sectors, and geographies. Each derived signal is temporally aligned with a market context snapshot—price deltas, volatility regimes, and spread metrics—enabling the computation of market‑adjusted sentiment scores and confidence‑weighted composite indicators. The subsystem aggregates these atomic signals across multiple granularities (ticker, sector, region, asset class, and macro‑topic) using configurable weighting schemas that incorporate source authority, recency decay, and market impact heuristics, while maintaining a full, cryptographically verifiable provenance chain from raw ingest to final aggregate. This ensures that every output—whether a real‑time sentiment heatmap, an anomaly alert, or a rolling composite score—is explainable, auditable, and suitable for downstream consumption by trading engines, risk models, and operational dashboards, thereby establishing Garden as the platform’s authoritative source of quantified market narrative and forward‑looking economic context.

## Constituent Subsystems

- Marketplace Center
- Economic Sentiment Monitor

## Comprehensive Capabilities

- Multi‑source ingestion from REST APIs, WebSocket streams, RSS feeds, S3 research drops, FTP legacy feeds, and gated terminal scraping
- Hybrid scheduling with continuous stream listeners and configurable cron‑like polling with checkpointed offsets
- Canonical event schema enforcement with UTC ISO8601 timestamp alignment and Unicode normalization
- Feed‑level deduplication via feed ID and raw payload hashing, plus near‑duplicate detection using title/snippet similarity
- Dead‑letter queuing for malformed, unprocessable, or policy‑violating payloads with inspection and replay tooling
- Entity canonicalization mapping textual mentions to exchange‑standard tickers, CIK, ISO country codes, and sector classifications via fuzzy matching and external reference data
- Market context enrichment attaching nearest‑prior price, volume, spread, and volatility snapshots to each text event
- Language detection and multi‑lingual preprocessing pipelines with tokenization, lemmatization, and stopword removal
- Named entity recognition for companies, persons, policy actions, financial instruments, geographies, and economic indicators
- Two‑tier sentiment modeling: fast lexicon/VADER tier for low‑latency signals and transformer‑based tier (FinBERT/distil variants) for high‑accuracy scoring
- Structured event extraction for earnings surprises, guidance changes, rate expectations, credit stress, M&A, and layoffs
- Topic modeling via supervised classifiers for known themes (inflation, growth, supply chain) and unsupervised clustering for emergent narratives
- Embedding generation for semantic search, similarity deduplication, and narrative clustering
- Market‑adjusted sentiment scoring that weights raw sentiment by contemporaneous price movement and volatility context
- Time‑series aggregation across ticker, sector, region, asset class, macro‑topic, and user‑defined baskets over configurable windows (minutes to months)
- Composite scoring with directional bias, intensity, volatility adjustment, sentiment momentum, and confidence intervals
- Anomaly detection over aggregated sentiment baselines using statistical and machine‑learning methods
- Configurable alerting engine with threshold and anomaly triggers, suppression logic, and delivery via Slack, email, SMS, webhook
- REST and WebSocket APIs for real‑time and historical query of scores, raw signals, entity metadata, and full‑text search
- Interactive dashboards with heatmaps, timeline playback, entity drilldowns, price correlation overlays, and watchlist management
- Explainer UI displaying source excerpts, confidence scores, model provenance, and contribution weights for every signal
- Time‑series database storage for numeric signals and aggregates with multi‑resolution retention policies
- Document store indexing for full‑text search, faceted queries, and embedding‑based semantic retrieval
- Graph store for entity relationship modeling and multi‑hop link analysis
- Data lake persistence of raw ingests and versioned model training datasets with schema‑on‑read access
- Model registry with versioned artifacts, A/B testing, and continuous retraining pipelines incorporating human‑in‑the‑loop labeling
- Backtesting engine for evaluating sentiment signal efficacy against historical market moves and event outcomes
- Observability stack exposing ingestion latency, deduplication rates, model inference times, and aggregation freshness via Prometheus metrics
- Structured logging and distributed tracing across ingestion, NLP, enrichment, and output stages
- Security controls including API key vaulting, encryption at rest and in transit, PII scrubbing, and role‑based access enforcement
- Legal and compliance guardrails for source redistribution restrictions, licensing terms, and data retention policies
