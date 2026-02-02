#!/usr/bin/env python3
"""Collect timestamps from multiple sources into one unified report.

Sources supported:
- Local git history (commit + file modification events) via local_git_analytics
- Working tree filesystem mtimes (optional) via local_git_analytics
- ZFS snapshot working tree mtimes (optional) via zfs_snapshot_git_timestamps

The output format mirrors local_git_analytics sheets so timesheet_from_timestamps.py
can consume it directly.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import shutil
import sys
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from local_git_analytics import LocalGitAnalytics
from zfs_snapshot_git_timestamps import (
    DEFAULT_EXCLUDES as ZFS_DEFAULT_EXCLUDES,
    collect_snapshot_rows,
    find_git_roots,
    list_snapshots,
)


def collect_local_git_and_zfs_sweep(
    *,
    repos_path: Path,
    max_depth: int,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    default_user: str,
    include_working_tree_timestamps: bool,
    working_tree_excludes: List[str],
    snapshot_roots: List[Path],
    allow_sudo: bool,
    zfs_scan_mode: str,
    zfs_snapshots_limit: int,
    zfs_granularity: str,
    zfs_excludes: List[str],
    zfs_max_seconds_per_root: Optional[float],
) -> Tuple[pd.DataFrame, List[Dict], List[Dict], List[Dict]]:
    """Run the local git + working-tree + ZFS snapshot sweep.

    Returns:
        (summary_df, commit_events, file_events, zfs_rows)
    """

    if snapshot_roots and os.geteuid() != 0:
        for root in snapshot_roots:
            try:
                probe_snapshot_access(root)
            except PermissionError:
                maybe_reexec_with_sudo(f"traverse ZFS snapshots under {root}", enabled=allow_sudo)

    analytics = LocalGitAnalytics(str(repos_path))
    summary_df = analytics.analyze_all_repositories(
        start_date=start_date,
        end_date=end_date,
        include_repos=None,
        exclude_repos=None,
        max_depth=max_depth,
        use_session_estimation=False,
        allowed_users=None,
        include_working_tree_timestamps=include_working_tree_timestamps,
        working_tree_user=default_user,
        working_tree_excludes=working_tree_excludes,
    )

    zfs_rows: List[Dict] = []
    if snapshot_roots:
        for snapshot_root in snapshot_roots:
            try:
                try:
                    next(iter(snapshot_root.iterdir()), None)
                except PermissionError:
                    maybe_reexec_with_sudo(
                        f"read ZFS snapshot directory {snapshot_root}",
                        enabled=allow_sudo,
                    )

                relative = None
                if zfs_scan_mode == 'match-repos-path':
                    mountpoint = snapshot_root_mountpoint(snapshot_root)
                    if mountpoint is not None:
                        try:
                            relative = repos_path.relative_to(mountpoint)
                        except Exception:
                            relative = None

                zfs_rows.extend(
                    collect_zfs_events(
                        snapshot_root,
                        default_user,
                        max_depth,
                        zfs_excludes,
                        scan_relative_to_mountpoint=relative,
                        start_date=start_date,
                        end_date=end_date,
                        snapshots_limit=max(int(zfs_snapshots_limit), 0),
                        granularity=zfs_granularity,
                        max_seconds=zfs_max_seconds_per_root,
                    )
                )
            except PermissionError:
                if os.geteuid() == 0:
                    print(f"Warning: Permission denied while scanning {snapshot_root} even as root; skipping.")
                    continue

                maybe_reexec_with_sudo(
                    f"scan ZFS snapshot directory {snapshot_root}",
                    enabled=allow_sudo,
                )
            except Exception as e:
                print(f"Warning: ZFS snapshot scan failed for {snapshot_root}: {e}")

    file_events = list(analytics.file_events)
    if zfs_rows:
        file_events.extend(zfs_rows)

    commit_events = list(analytics.commit_events)

    return summary_df, commit_events, file_events, zfs_rows


def parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def snapshot_root_mountpoint(snapshot_root: Path) -> Optional[Path]:
    # snapshot_root is typically <mountpoint>/.zfs/snapshot
    try:
        mountpoint = snapshot_root.parent.parent
        if mountpoint.is_dir():
            return mountpoint
    except Exception:
        return None
    return None


def parse_snapshot_date_from_name(name: str) -> Optional[datetime]:
    # Common patterns include: zfs-auto-snap_hourly-2026-01-27-0217
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except Exception:
        return None


def run(cmd: Sequence[str]) -> Tuple[int, str]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        out = (proc.stdout or '').strip()
        if not out:
            out = (proc.stderr or '').strip()
        return proc.returncode, out
    except Exception as e:
        return 1, str(e)


def ensure_sudo_credentials() -> bool:
    """Ensure sudo credentials are cached.

    This intentionally prompts the user for their sudo password when required.
    """
    if shutil.which('sudo') is None:
        return False

    print("ZFS detection may require sudo. If prompted, enter your sudo password.")
    try:
        proc = subprocess.run(['sudo', '-v'], check=False)
        return proc.returncode == 0
    except Exception:
        return False


def maybe_reexec_with_sudo(reason: str, enabled: bool) -> None:
    """Re-run this script under sudo when elevated access is required.

    This will prompt for the user's sudo password via `sudo -v`.
    """
    if not enabled:
        raise PermissionError(reason)

    if os.geteuid() == 0:
        raise PermissionError(reason)

    # Prevent infinite loops
    if (os.getenv('GITHUB_ANALYTICS_REEXECED_WITH_SUDO') or '').strip() == '1':
        raise PermissionError(reason)

    if shutil.which('sudo') is None:
        raise PermissionError(f"{reason} (sudo not found)")

    print(f"Permission required to {reason}.")
    if not ensure_sudo_credentials():
        raise PermissionError(f"{reason} (sudo auth failed)")

    env = os.environ.copy()
    env['GITHUB_ANALYTICS_REEXECED_WITH_SUDO'] = '1'

    # Re-exec using the current interpreter (works for venvs as well).
    cmd = ['sudo', '-E', sys.executable] + sys.argv
    os.execvpe('sudo', cmd, env)


def detect_github_username() -> Optional[str]:
    for env_name in ("GITHUB_USERNAME", "GITHUB_USER"):
        value = (os.getenv(env_name) or '').strip()
        if value:
            return value

    code, out = run(["gh", "api", "user", "-q", ".login"])
    if code == 0 and out:
        return out.splitlines()[0].strip()

    return None


def is_git_repo_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if (path / '.git').is_dir():
        return True

    # Bare repo signature
    head = path / 'HEAD'
    objects_dir = path / 'objects'
    refs_dir = path / 'refs'
    packed_refs = path / 'packed-refs'
    config = path / 'config'
    return (
        head.is_file()
        and objects_dir.is_dir()
        and (refs_dir.is_dir() or packed_refs.is_file())
        and config.is_file()
    )


def guess_repos_base_path(explicit: Optional[str]) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()

    env_override = (os.getenv("GITHUB_ANALYTICS_REPOS_PATH") or '').strip()
    if env_override:
        return Path(env_override).expanduser().resolve()

    username = detect_github_username()
    script_dir = Path(__file__).parent.resolve()
    if username:
        # Common clone_and_analyze cache locations
        candidates = [
            script_dir / '.cache' / f'github_analysis_{username}',
            script_dir / '.cache' / f'github_worktrees_{username}',
            Path.home() / '.cache' / f'github_analysis_{username}',
            Path.home() / '.cache' / f'github_worktrees_{username}',
        ]
        for candidate in candidates:
            if candidate.is_dir():
                return candidate

    # Common local working tree locations (choose the one that looks most like a repo-root).
    home = Path.home()
    common = [
        home / 'src',
        home / 'code',
        home / 'projects',
        home / 'repos',
        home / 'github',
        home / 'work',
    ]

    best_path = None
    best_score = -1
    for candidate in common:
        if not candidate.is_dir():
            continue
        score = 0
        try:
            for child in candidate.iterdir():
                if child.is_dir() and not child.name.startswith('.') and is_git_repo_dir(child):
                    score += 1
        except Exception:
            score = 0
        if score > best_score:
            best_score = score
            best_path = candidate
    if best_path is not None and best_score > 0:
        return best_path.resolve()

    # Fall back to current working directory.
    return Path.cwd().resolve()


def detect_max_depth(base_path: Path, explicit: Optional[int]) -> int:
    if explicit is not None:
        return explicit

    # If it looks like a repo cache where repos are direct children, keep depth low.
    try:
        child_repo_count = 0
        for child in base_path.iterdir():
            if child.is_dir() and not child.name.startswith('.') and is_git_repo_dir(child):
                child_repo_count += 1
                if child_repo_count >= 3:
                    return 2
    except Exception:
        pass

    return 5


def parse_proc_mounts() -> List[Tuple[str, str]]:
    mounts: List[Tuple[str, str]] = []
    for proc_path in (Path('/proc/self/mounts'), Path('/proc/mounts')):
        if not proc_path.is_file():
            continue
        try:
            for line in proc_path.read_text(encoding='utf-8', errors='replace').splitlines():
                parts = line.split()
                if len(parts) < 3:
                    continue
                mount_point = parts[1]
                fs_type = parts[2]
                mounts.append((mount_point, fs_type))
            break
        except Exception:
            continue
    return mounts


def parse_zfs_mountpoints_via_cli(use_sudo: bool) -> List[Path]:
    """Return ZFS dataset mountpoints via `zfs list` (best-effort)."""
    if shutil.which('zfs') is None:
        return []

    cmd = ['zfs', 'list', '-H', '-o', 'mountpoint']
    if use_sudo:
        cmd = ['sudo'] + cmd

    code, out = run(cmd)
    if code != 0 or not out:
        return []

    mountpoints: List[Path] = []
    for line in out.splitlines():
        value = (line or '').strip()
        if not value or value in {'-', 'none', 'legacy'}:
            continue
        mp = Path(value)
        if mp.is_absolute():
            mountpoints.append(mp)
    return mountpoints


def detect_zfs_snapshot_roots(explicit: Optional[str]) -> List[Path]:
    if explicit:
        root = Path(explicit).expanduser().resolve()
        return [root] if root.exists() else []

    env_override = (os.getenv("ZFS_SNAPSHOT_ROOT") or '').strip()
    if env_override:
        root = Path(env_override).expanduser().resolve()
        return [root] if root.exists() else []

    candidates: List[Path] = []

    # First: check mountpoints for `.zfs/snapshot` (fast and doesn't assume fstype labeling).
    for mount_point, _fs_type in parse_proc_mounts():
        mp = Path(mount_point)
        snap = mp / '.zfs' / 'snapshot'
        try:
            if snap.is_dir():
                candidates.append(snap.resolve())
        except Exception:
            continue

    # Second: ask ZFS directly for mountpoints.
    zfs_mounts = parse_zfs_mountpoints_via_cli(use_sudo=False)
    if not zfs_mounts:
        # If ZFS is present but requires privileges, prompt for sudo password.
        if shutil.which('zfs') is not None and ensure_sudo_credentials():
            zfs_mounts = parse_zfs_mountpoints_via_cli(use_sudo=True)

    for mp in zfs_mounts:
        snap = mp / '.zfs' / 'snapshot'
        try:
            if snap.is_dir():
                candidates.append(snap.resolve())
        except Exception:
            continue

    # Third: common dataset roots (one level deep)
    for root in (Path('/mnt'), Path('/media'), Path('/storage'), Path('/srv'), Path('/pool'), Path('/tank')):
        if not root.is_dir():
            continue
        try:
            for child in root.iterdir():
                if not child.is_dir() or child.name.startswith('.'):
                    continue
                snap = child / '.zfs' / 'snapshot'
                if snap.is_dir():
                    candidates.append(snap.resolve())
        except Exception:
            continue

    # Fallbacks
    for fallback in (Path('/.zfs/snapshot'), Path('/mnt/pool/.zfs/snapshot')):
        if fallback.is_dir():
            candidates.append(fallback.resolve())

    # Dedupe, preserve order
    seen = set()
    roots: List[Path] = []
    for c in candidates:
        key = str(c)
        if key in seen:
            continue
        seen.add(key)
        roots.append(c)
    return roots


def rank_snapshot_roots(roots: List[Path]) -> List[Path]:
    """Rank snapshot roots so we scan the most relevant ones first."""
    def score(root: Path) -> Tuple[int, int]:
        # Prefer user/data mounts over system mounts.
        path_str = str(root)
        preferred_prefixes = (
            '/storage/',
            '/mnt/',
            '/media/',
            '/tank/',
            '/pool/',
            '/home/',
        )
        preferred = 1 if path_str.startswith(preferred_prefixes) else 0
        try:
            snapshot_count = sum(1 for p in root.iterdir() if p.is_dir())
        except Exception:
            snapshot_count = 0
        return (preferred, snapshot_count)

    return sorted(roots, key=score, reverse=True)


def probe_snapshot_access(snapshot_root: Path) -> None:
    """Best-effort probe that we can traverse into at least one snapshot."""
    first_snapshot = None
    for p in snapshot_root.iterdir():
        if p.is_dir():
            first_snapshot = p
            break

    if first_snapshot is None:
        return

    # If we cannot traverse the snapshot directory, we'll almost certainly fail deeper.
    if not os.access(first_snapshot, os.R_OK | os.X_OK):
        raise PermissionError(str(first_snapshot))


def collect_zfs_events(
    snapshot_root: Path,
    user: str,
    max_depth: int,
    excludes: List[str],
    scan_relative_to_mountpoint: Optional[Path],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    snapshots_limit: int,
    granularity: str,
    max_seconds: Optional[float],
) -> List[Dict]:
    all_rows: List[Dict] = []
    snapshots = list_snapshots(snapshot_root)

    # Prefer newest snapshots; names usually sort chronologically.
    try:
        snapshots = sorted(snapshots, key=lambda p: p.name, reverse=True)
    except Exception:
        pass

    if snapshots_limit > 0:
        snapshots = snapshots[:snapshots_limit]

    start_time = time.time()

    for snap_index, snap in enumerate(snapshots, 1):
        if max_seconds is not None and (time.time() - start_time) > max_seconds:
            print(f"[ZFS] Time budget reached for {snapshot_root}; stopping early.")
            break

        if snap_index % 10 == 0:
            print(f"[ZFS] {snapshot_root}: snapshot {snap_index}/{len(snapshots)} ({snap.name})")

        snap_dt = parse_snapshot_date_from_name(snap.name)
        if snap_dt is not None:
            if start_date and snap_dt < start_date:
                continue
            if end_date and snap_dt > end_date:
                continue

        scan_base = snap
        if scan_relative_to_mountpoint is not None:
            candidate = snap / scan_relative_to_mountpoint
            if candidate.exists():
                scan_base = candidate

        if (scan_base / '.git').is_dir():
            repos = [scan_base]
        else:
            repos = find_git_roots(scan_base, max_depth)
        for repo in repos:
            all_rows.extend(
                collect_snapshot_rows(
                    snap.name,
                    repo,
                    user,
                    excludes,
                    granularity=granularity,
                )
            )
    return all_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect timestamps from multiple sources into one report.")
    parser.add_argument("--repos-path", default=None, help="Base path to scan for git repos (default: auto)")
    parser.add_argument("--zfs-snapshot-root", default=None, help="ZFS snapshot root path (default: auto-detect)")
    parser.add_argument(
        "--no-sudo",
        action="store_true",
        help="Do not attempt to prompt for sudo or re-run under sudo",
    )
    parser.add_argument(
        "--all-zfs-snapshot-roots",
        action="store_true",
        help="Scan every detected ZFS snapshot root (can be slow)",
    )
    parser.add_argument(
        "--zfs-snapshot-roots-limit",
        type=int,
        default=5,
        help="Max number of detected ZFS snapshot roots to scan (default: 5)",
    )
    parser.add_argument(
        "--zfs-scan-mode",
        choices=["match-repos-path", "full"],
        default="match-repos-path",
        help="How to scan within each snapshot (default: match-repos-path)",
    )
    parser.add_argument(
        "--zfs-snapshots-limit",
        type=int,
        default=25,
        help="Max snapshots to scan per root (default: 25)",
    )
    parser.add_argument(
        "--zfs-granularity",
        choices=['repo_index', 'repo_root', 'file'],
        default='repo_index',
        help="ZFS event granularity (default: repo_index)",
    )

    parser.add_argument(
        "--zfs-full-scan",
        action="store_true",
        help=(
            "Scan all detected ZFS datasets and all snapshots (exhaustive). "
            "Implies --all-zfs-snapshot-roots, --zfs-snapshot-roots-limit 0, --zfs-snapshots-limit 0, --zfs-scan-mode full"
        ),
    )
    parser.add_argument(
        "--zfs-max-seconds-per-root",
        type=float,
        default=None,
        help="Optional time budget per ZFS snapshot root (seconds).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Excel file (default: data_reports/<timestamp>/local_git_timestamps.xlsx)",
    )
    parser.add_argument(
        "--output-dir",
        default="data_reports",
        help="Base directory for timestamped outputs when --output is not provided (default: data_reports)",
    )
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD) for git history")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD) for git history")
    parser.add_argument("--max-depth", type=int, default=None, help="Max directory depth to search (default: auto)")
    parser.add_argument("--user", default=None, help="Default user attribution for non-git-history sources")
    parser.add_argument(
        "--include-working-tree-timestamps",
        action="store_true",
        help="Include filesystem mtime events from non-bare working trees",
    )
    parser.add_argument(
        "--working-tree-exclude",
        action="append",
        default=[],
        help="Directory name to exclude when scanning working trees (repeatable)",
    )
    parser.add_argument(
        "--zfs-exclude",
        action="append",
        default=[],
        help="Directory name to exclude when scanning ZFS snapshots (repeatable)",
    )

    args = parser.parse_args()

    if args.zfs_full_scan:
        args.all_zfs_snapshot_roots = True
        args.zfs_snapshot_roots_limit = 0
        args.zfs_snapshots_limit = 0
        args.zfs_scan_mode = 'full'

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        from github_analyitics.reporting.report_paths import default_xlsx_path

        output_path = default_xlsx_path(
            "local_git_timestamps.xlsx",
            base_dir=args.output_dir,
        )
    repos_path = guess_repos_base_path(args.repos_path)
    max_depth = detect_max_depth(repos_path, args.max_depth)
    snapshot_roots = detect_zfs_snapshot_roots(args.zfs_snapshot_root)
    allow_sudo = not args.no_sudo

    if args.zfs_snapshot_root is None and snapshot_roots:
        snapshot_roots = rank_snapshot_roots(snapshot_roots)
        if not args.all_zfs_snapshot_roots:
            limit = int(args.zfs_snapshot_roots_limit)
            if limit > 0:
                snapshot_roots = snapshot_roots[: max(limit, 1)]

    default_user = args.user or detect_github_username() or os.getenv('USER') or 'Unknown'

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    print(f"Repo scan base path: {repos_path}")
    print(f"Repo scan max depth: {max_depth}")
    if snapshot_roots:
        print(f"Detected ZFS snapshot roots: {', '.join(str(p) for p in snapshot_roots)}")
    else:
        print("Detected ZFS snapshot roots: (none)")

    zfs_excludes = sorted(set(ZFS_DEFAULT_EXCLUDES).union(args.zfs_exclude))

    summary_df, commit_events, file_events, zfs_rows = collect_local_git_and_zfs_sweep(
        repos_path=repos_path,
        max_depth=max_depth,
        start_date=start_date,
        end_date=end_date,
        default_user=default_user,
        include_working_tree_timestamps=args.include_working_tree_timestamps,
        working_tree_excludes=args.working_tree_exclude,
        snapshot_roots=snapshot_roots,
        allow_sudo=allow_sudo,
        zfs_scan_mode=args.zfs_scan_mode,
        zfs_snapshots_limit=args.zfs_snapshots_limit,
        zfs_granularity=args.zfs_granularity,
        zfs_excludes=zfs_excludes,
        zfs_max_seconds_per_root=args.zfs_max_seconds_per_root,
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        if not summary_df.empty:
            summary_df.to_excel(writer, sheet_name="Detailed Report", index=False)

            user_summary = (
                summary_df.groupby('user')
                .agg({
                    'commits': 'sum',
                    'lines_added': 'sum',
                    'lines_deleted': 'sum',
                    'total_lines_changed': 'sum',
                    'files_modified': 'sum',
                    'estimated_hours': 'sum',
                })
                .reset_index()
                .sort_values('estimated_hours', ascending=False)
            )
            user_summary.to_excel(writer, sheet_name="User Summary", index=False)

            date_summary = (
                summary_df.groupby('date')
                .agg({
                    'commits': 'sum',
                    'lines_added': 'sum',
                    'lines_deleted': 'sum',
                    'total_lines_changed': 'sum',
                    'files_modified': 'sum',
                    'estimated_hours': 'sum',
                    'user': 'count',
                })
                .reset_index()
                .rename(columns={'user': 'active_users'})
                .sort_values('date', ascending=False)
            )
            date_summary.to_excel(writer, sheet_name="Daily Summary", index=False)

        if file_events:
            file_events_df = pd.DataFrame(file_events).sort_values('event_timestamp', ascending=False)
            file_events_df.to_excel(writer, sheet_name="File Events", index=False)

            cols = ['repository', 'file', 'event_timestamp', 'user', 'commit', 'status']
            available = [c for c in cols if c in file_events_df.columns]
            if available:
                file_events_df[available].to_excel(writer, sheet_name="File Timestamp List", index=False)

        if commit_events:
            commit_events_df = pd.DataFrame(commit_events).sort_values('event_timestamp', ascending=False)
            commit_events_df.to_excel(writer, sheet_name="Commit Events", index=False)

        if file_events or commit_events:
            combined: List[Dict] = []
            for event in commit_events:
                combined.append({'event_type': 'commit', **event})
            for event in file_events:
                combined.append({'event_type': 'file', **event})
            timeline_df = pd.DataFrame(combined).sort_values('event_timestamp', ascending=False)
            timeline_df.to_excel(writer, sheet_name="User Timeline", index=False)

        if zfs_rows:
            zfs_df = pd.DataFrame(zfs_rows).sort_values(
                ['snapshot', 'repository', 'event_timestamp'],
                ascending=[True, True, False],
            )
            zfs_df.to_excel(writer, sheet_name="ZFS Snapshot Timestamps", index=False)

    print(f"Wrote combined timestamp report: {output_path}")


if __name__ == "__main__":
    main()
