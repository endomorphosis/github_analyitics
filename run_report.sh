#!/bin/bash
# Quick script to run GitHub analytics report
# 
# Usage:
#   ./run_report.sh                          # Run for all time
#   ./run_report.sh 2024-01-01 2024-12-31    # Run for date range

set -e

PY=""
if [ -x "./.venv/bin/python" ]; then
    PY="./.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PY="python3"
else
    PY="python"
fi

# Check if gh is available
if ! command -v gh >/dev/null 2>&1; then
    echo "Error: GitHub CLI (gh) is not installed or not in PATH"
    echo "Run: ./install.sh"
    echo "Or install it from: https://cli.github.com/"
    exit 1
fi

# Check if gh is authenticated
if ! gh auth status >/dev/null 2>&1; then
    echo "Error: gh is not authenticated."
    echo "Run: gh auth login"
    exit 1
fi

# Check if Python dependencies are installed
if ! "$PY" -c "import pandas" 2>/dev/null; then
    echo "Installing dependencies..."
    "$PY" -m pip install -r requirements.txt
fi

# Run the analytics
if [ $# -eq 0 ]; then
    echo "Running GitHub analytics for all time..."
    "$PY" -m github_analyitics.reporting.github_analytics
elif [ $# -eq 2 ]; then
    echo "Running GitHub analytics from $1 to $2..."
    "$PY" -m github_analyitics.reporting.github_analytics --start-date "$1" --end-date "$2"
else
    echo "Usage: $0 [start-date end-date]"
    echo "Example: $0 2024-01-01 2024-12-31"
    exit 1
fi

echo ""
echo "Report generation complete!"
echo "Check the data_reports/<timestamp>/ folder for the generated .xlsx file"
