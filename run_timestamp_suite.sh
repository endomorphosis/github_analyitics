#!/bin/bash
# Run the unified timestamp suite (GitHub API + local + optional ZFS).
#
# Usage:
#   ./run_timestamp_suite.sh
#   ./run_timestamp_suite.sh 2024-01-01 2024-12-31

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/github_analyitics/reporting/run_timestamp_suite.sh" "$@"

if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please create it from .env.example:" 
    echo "  cp .env.example .env"
    exit 1
fi

if ! python -c "import pandas" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

OUT="github_analytics_timestamps_suite.xlsx"

if [ $# -eq 0 ]; then
    echo "Running unified timestamp suite (all time)..."
    python -m github_analyitics.timestamp_audit.timestamp_suite --output "$OUT" --sources github,local
elif [ $# -eq 2 ]; then
    echo "Running unified timestamp suite from $1 to $2..."
    python -m github_analyitics.timestamp_audit.timestamp_suite --output "$OUT" --sources github,local --start-date "$1" --end-date "$2"
else
    echo "Usage: $0 [start-date end-date]"
    exit 1
fi

echo "Saved: $OUT"
