#!/bin/bash
# Quick script to run GitHub analytics report
# 
# Usage:
#   ./run_report.sh                          # Run for all time
#   ./run_report.sh 2024-01-01 2024-12-31    # Run for date range

set -e

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please create it from .env.example:"
    echo "  cp .env.example .env"
    echo "  # Then edit .env with your GitHub token and username"
    exit 1
fi

# Check if Python dependencies are installed
if ! python -c "import pandas" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

# Run the analytics
if [ $# -eq 0 ]; then
    echo "Running GitHub analytics for all time..."
    python -m github_analyitics.reporting.github_analytics
elif [ $# -eq 2 ]; then
    echo "Running GitHub analytics from $1 to $2..."
    python -m github_analyitics.reporting.github_analytics --start-date "$1" --end-date "$2"
else
    echo "Usage: $0 [start-date end-date]"
    echo "Example: $0 2024-01-01 2024-12-31"
    exit 1
fi

echo ""
echo "Report generation complete!"
echo "Check the current directory for the generated .xlsx file"
