#!/bin/bash
set -e

# Bootstrap installer for this repo.
# - Installs required CLI tools (best-effort, may require sudo)
# - Creates/updates a local .venv and installs Python dependencies into it

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/scripts/install_cli_tools.sh"

PYTHON=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON="python"
else
  echo "Error: python not found in PATH" >&2
  exit 1
fi

VENV_DIR="$SCRIPT_DIR/.venv"
VENV_PY="$VENV_DIR/bin/python"

if [ ! -x "$VENV_PY" ]; then
  echo "Creating virtual environment at $VENV_DIR"

  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y python3-venv || true
  fi

  "$PYTHON" -m venv "$VENV_DIR"
fi

if [ ! -x "$VENV_PY" ]; then
  echo "Error: failed to create venv at $VENV_DIR" >&2
  exit 1
fi

"$VENV_PY" -m pip install --upgrade pip
"$VENV_PY" -m pip install -r "$SCRIPT_DIR/requirements.txt"

echo "Done. Next:"
echo "- Authenticate: gh auth login"
echo "- Run: ./run_report.sh"
echo "- Or: $VENV_PY -m github_analyitics.reporting.github_analytics"
