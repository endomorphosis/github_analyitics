#!/usr/bin/env python3
"""
Collect file modification timestamps from working trees.

Scans working directories (git repositories) under a base path and records
file modification times from the filesystem.
"""

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


DEFAULT_EXCLUDES = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "node_modules",
    "dist",
    "build",
    "__pycache__",
}


def find_git_roots(base_path: Path, max_depth: int) -> List[Path]:
    git_roots: List[Path] = []

    def walk(current: Path, depth: int) -> None:
        if depth > max_depth:
            return
        if (current / ".git").is_dir():
            git_roots.append(current)
            return
        for child in current.iterdir():
            if child.is_dir() and not child.name.startswith("."):
                walk(child, depth + 1)

    walk(base_path, 0)
    return git_roots


def iter_files(repo_root: Path, excludes: Iterable[str]) -> Iterable[Path]:
    exclude_set = set(excludes)
    for root, dirs, files in os.walk(repo_root):
        dirs[:] = [d for d in dirs if d not in exclude_set]
        for name in files:
            if name.startswith("."):
                continue
            path = Path(root) / name
            yield path


def collect_timestamps(
    repo_root: Path,
    user: str,
    excludes: Iterable[str],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for path in iter_files(repo_root, excludes):
        try:
            stat = path.stat()
        except (OSError, FileNotFoundError):
            continue
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        rows.append(
            {
                "repository": repo_root.name,
                "file": str(path.relative_to(repo_root)).replace("\\", "/"),
                "event_timestamp": mtime.isoformat(),
                "user": user,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect file modification timestamps from working trees."
    )
    parser.add_argument(
        "--base-path",
        default=str(Path.home()),
        help="Base path to scan for git repositories (default: user home)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output Excel file path",
    )
    parser.add_argument(
        "--user",
        default=os.getlogin(),
        help="User name to record in output (default: current OS user)",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="Maximum directory depth to search for repositories",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Directory name to exclude (can be repeated)",
    )

    args = parser.parse_args()

    base_path = Path(args.base_path).expanduser().resolve()
    excludes = sorted(set(DEFAULT_EXCLUDES).union(args.exclude))

    print(f"Scanning for git repositories under: {base_path}")
    repos = find_git_roots(base_path, args.max_depth)
    print(f"Found {len(repos)} repositories")

    all_rows: List[Dict[str, str]] = []
    for repo in repos:
        print(f"Collecting timestamps from: {repo}")
        all_rows.extend(collect_timestamps(repo, args.user, excludes))

    if not all_rows:
        print("No files found.")
        return

    df = pd.DataFrame(all_rows)
    df = df.sort_values(["repository", "event_timestamp"], ascending=[True, False])

    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Working Tree Timestamps", index=False)

    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
