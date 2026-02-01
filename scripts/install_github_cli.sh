#!/bin/bash
set -euo pipefail

# Backward-compatible wrapper.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/install_cli_tools.sh"
