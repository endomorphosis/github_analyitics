#!/usr/bin/env python3
"""Entry point wrapper for scanning another user's GitHub repositories.

The main timestamp suite already supports `--github-username`, but this wrapper
provides a convenience console script that takes the target username as a
positional argument:

    github-analyitics-timestamps-user <github_username> [suite args...]
"""

from __future__ import annotations

import sys
from typing import List


def build_forwarded_argv(argv: List[str]) -> List[str]:
    """Inject `--github-username <user>` when provided positionally.

    Rules:
    - If `--github-username` is already present, do nothing.
    - If the first arg is a non-flag value, treat it as the username.
    - If no username is provided, raise SystemExit with usage.
    """
    if not argv:
        raise SystemExit("Error: argv is empty")

    # Wrapper-level help (no username).
    if len(argv) == 2 and argv[1] in {"-h", "--help"}:
        raise SystemExit(
            "Usage: github-analyitics-timestamps-user <github_username> [timestamp_suite args...]\n"
            "\n"
            "This runs the unified timestamp suite but scans repositories owned by <github_username>.\n"
            "Example:\n"
            "  github-analyitics-timestamps-user octocat --sources github --start-date 2026-01-01\n"
            "\n"
            "Tip: `--help` after the username shows the full suite help:\n"
            "  github-analyitics-timestamps-user octocat --help\n"
        )

    # If the caller already specified a username flag, trust it.
    if "--github-username" in argv[1:]:
        return list(argv)

    if len(argv) < 2:
        raise SystemExit(
            "Error: missing <github_username>.\n"
            "Usage: github-analyitics-timestamps-user <github_username> [timestamp_suite args...]"
        )

    candidate = argv[1]
    if candidate.startswith("-"):
        raise SystemExit(
            "Error: expected <github_username> as the first argument.\n"
            "Usage: github-analyitics-timestamps-user <github_username> [timestamp_suite args...]"
        )

    return [argv[0], "--github-username", candidate, *argv[2:]]


def main() -> None:
    from github_analyitics.timestamp_audit import timestamp_suite

    forwarded = build_forwarded_argv(sys.argv)

    old = sys.argv
    try:
        sys.argv = forwarded
        timestamp_suite.main()
    finally:
        sys.argv = old


if __name__ == "__main__":
    main()
