#!/usr/bin/env bash
# Render build script: install deps + collect initial articles
set -e

pip install -r requirements.txt

# Collect articles on deploy (no AI to avoid API cost during build)
python main.py collect --no-ai || echo "Article collection skipped (non-fatal)"
