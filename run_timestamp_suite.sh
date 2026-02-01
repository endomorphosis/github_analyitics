#!/bin/bash
# Run the unified timestamp suite (GitHub API + local + optional ZFS).
#
# Usage:
#   ./run_timestamp_suite.sh
#   ./run_timestamp_suite.sh 2024-01-01 2024-12-31

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/github_analyitics/reporting/run_timestamp_suite.sh" "$@"
