#!/bin/bash
# Economic Sentiment Monitor v0.2 - Setup for Kali Linux
set -e

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║    ECONOMIC SENTIMENT MONITOR v0.2 - SETUP          ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

PYTHON=$(which python3)
PY_VER=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "[*] Python version: $PY_VER"

echo "[*] Installing core packages..."
pip install textual yfinance feedparser vaderSentiment requests \
    --break-system-packages -q
echo "[+] Core packages installed."

echo ""
read -p "[?] Install FinBERT accurate-tier NLP? (recommended, ~500MB download, cached after) [y/N]: " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo "[*] Installing transformers + torch (this may take a few minutes)..."
    pip install transformers torch --break-system-packages -q
    echo "[+] FinBERT dependencies installed. Model downloads on first launch."
else
    echo "[~] Skipped FinBERT — will use VADER-only mode."
fi

echo ""
read -p "[?] Install SpaCy entity linking? (recommended, ~50MB) [y/N]: " ans2
if [[ "$ans2" =~ ^[Yy]$ ]]; then
    echo "[*] Installing SpaCy..."
    pip install spacy --break-system-packages -q
    echo "[*] Downloading en_core_web_sm model..."
    python3 -m spacy download en_core_web_sm
    echo "[+] SpaCy NER installed."
else
    echo "[~] Skipped SpaCy — regex entity fallback will be used."
fi

mkdir -p ~/.esm
if [ ! -f ~/.esm/.env ]; then
cat > ~/.esm/.env << 'EOF'
# Free API keys (all optional)
# NewsAPI  → https://newsapi.org/register
NEWS_API_KEY=
# FRED     → https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY=
EOF
echo "[*] Created ~/.esm/.env — add your free API keys there."
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║                   READY TO LAUNCH                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║                                                      ║"
echo "║  Run (no API keys needed):                          ║"
echo "║    python3 monitor.py                               ║"
echo "║                                                      ║"
echo "║  Run with optional free API keys:                   ║"
echo "║    source ~/.esm/.env && python3 monitor.py         ║"
echo "║                                                      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
