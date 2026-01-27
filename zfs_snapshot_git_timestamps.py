#!/usr/bin/env python3
"""
Collect file timestamps from Git working trees found in ZFS snapshots.

Scans snapshot directories (e.g., /mnt/pool/.zfs/snapshot/*) for git repos
and records file modification times for each snapshot.
"""

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

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


def list_snapshots(snapshot_root: Path) -> List[Path]:
    if not snapshot_root.exists():
        return []
    return [p for p in snapshot_root.iterdir() if p.is_dir()]


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
            yield Path(root) / name


def collect_snapshot_rows(
    snapshot_name: str,
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
                "snapshot": snapshot_name,
                "repository": repo_root.name,
                "file": str(path.relative_to(repo_root)).replace("\\", "/"),
                "event_timestamp": mtime.isoformat(),
                "user": user,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect file timestamps from git repos inside ZFS snapshots."
    )
    parser.add_argument(
        "--snapshot-root",
        default="/mnt/pool/.zfs/snapshot",
        help="Snapshot root path (default: /mnt/pool/.zfs/snapshot)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output Excel file path",
    )
    parser.add_argument(
        "--user",
        default="unknown",
        help="User name to record in output",
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

    snapshot_root = Path(args.snapshot_root).expanduser().resolve()
    excludes = sorted(set(DEFAULT_EXCLUDES).union(args.exclude))

    snapshots = list_snapshots(snapshot_root)
    print(f"Snapshot root: {snapshot_root}")
    print(f"Found {len(snapshots)} snapshots")

    all_rows: List[Dict[str, str]] = []
    for snap in snapshots:
        repos = find_git_roots(snap, args.max_depth)
        if not repos:
            continue
        print(f"Snapshot {snap.name}: {len(repos)} repos")
        for repo in repos:
            all_rows.extend(
                collect_snapshot_rows(snap.name, repo, args.user, excludes)
            )

    if not all_rows:
        print("No files found.")
        return

    df = pd.DataFrame(all_rows)
    df = df.sort_values(["snapshot", "repository", "event_timestamp"], ascending=[True, True, False])

    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ZFS Snapshot Timestamps", index=False)

    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
