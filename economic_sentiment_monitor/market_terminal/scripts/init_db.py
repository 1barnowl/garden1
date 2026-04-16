#!/usr/bin/env python3
"""
scripts/init_db.py
Explicitly initialise (or migrate) the SQLite database.
Run from the project root: python scripts/init_db.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from market_terminal.storage.database import init_db

if __name__ == "__main__":
    print("Initialising database…")
    init_db()
    print("Done.")
