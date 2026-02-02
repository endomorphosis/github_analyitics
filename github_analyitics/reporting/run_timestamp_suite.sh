#!/bin/bash
# Run the unified timestamp suite (GitHub API + local + optional ZFS).
#
# Usage:
#   ./run_timestamp_suite.sh
#   ./run_timestamp_suite.sh 2024-01-01 2024-12-31

set -e

PY=""
if [ -x "./.venv/bin/python" ]; then
    PY="./.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PY="python3"
else
    PY="python"
fi

if ! command -v gh >/dev/null 2>&1; then
    echo "Error: GitHub CLI (gh) is not installed or not in PATH"
    echo "Run: ./install.sh"
    echo "Or install it from: https://cli.github.com/"
    exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
    echo "Error: gh is not authenticated."
    echo "Run: gh auth login"
    exit 1
fi

if ! "$PY" -c "import pandas" 2>/dev/null; then
    echo "Installing dependencies..."
    "$PY" -m pip install -r requirements.txt
fi

if [ $# -eq 0 ]; then
    echo "Running unified timestamp suite (all time)..."
    "$PY" -m github_analyitics.timestamp_audit.timestamp_suite
elif [ $# -eq 2 ]; then
    echo "Running unified timestamp suite from $1 to $2..."
    "$PY" -m github_analyitics.timestamp_audit.timestamp_suite --start-date "$1" --end-date "$2"
else
    echo "Usage: $0 [start-date end-date]"
    exit 1
fi

echo "Saved under: data_reports/<timestamp>/"
