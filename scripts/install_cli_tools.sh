#!/bin/bash
set -euo pipefail

# Best-effort installer for required CLI tools.
# Currently required:
# - gh (GitHub CLI)
# - git

need_sudo_install=false

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

install_with_apt() {
  sudo apt-get update
  sudo apt-get install -y "$@"
}

install_with_dnf() {
  sudo dnf install -y "$@"
}

install_with_brew() {
  brew install "$@"
}

install_missing_tools() {
  local missing=()

  if ! have_cmd gh; then
    missing+=("gh")
  fi

  if ! have_cmd git; then
    missing+=("git")
  fi

  if [ ${#missing[@]} -eq 0 ]; then
    echo "All required CLI tools are already installed."
    echo "- gh: $(gh --version | head -n 1)"
    echo "- git: $(git --version)"
    return 0
  fi

  echo "Missing CLI tools: ${missing[*]}"

  if have_cmd apt-get; then
    echo "Installing via apt (Ubuntu/Debian)..."
    install_with_apt "${missing[@]}"
    return 0
  fi

  if have_cmd dnf; then
    echo "Installing via dnf (Fedora/RHEL)..."
    install_with_dnf "${missing[@]}"
    return 0
  fi

  if have_cmd brew; then
    echo "Installing via brew (macOS/Linuxbrew)..."
    install_with_brew "${missing[@]}"
    return 0
  fi

  echo "No supported package manager found to install: ${missing[*]}" >&2
  echo "Please install them manually:" >&2
  echo "- GitHub CLI: https://cli.github.com/" >&2
  echo "- Git: https://git-scm.com/downloads" >&2
  return 1
}

install_missing_tools
