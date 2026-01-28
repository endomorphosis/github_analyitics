#!/usr/bin/env python3
"""
Collect file timestamps from Git working trees found in ZFS snapshots.

Scans snapshot directories (e.g., /mnt/pool/.zfs/snapshot/*) for git repos
and records file modification times for each snapshot.
"""

import argparse
import os
import subprocess
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
import configparser
import re
from typing import Dict, Iterable, List, Literal, Optional

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


def ensure_sudo_credentials() -> bool:
    if shutil.which('sudo') is None:
        return False
    print("ZFS snapshot root auto-detection may require sudo. If prompted, enter your sudo password.")
    try:
        proc = subprocess.run(['sudo', '-v'], check=False)
        return proc.returncode == 0
    except Exception:
        return False


def maybe_reexec_with_sudo(reason: str, enabled: bool) -> None:
    """Re-run this script under sudo when elevated access is required."""
    if not enabled:
        raise PermissionError(reason)

    if os.geteuid() == 0:
        raise PermissionError(reason)

    if (os.getenv('GITHUB_ANALYTICS_REEXECED_WITH_SUDO') or '').strip() == '1':
        raise PermissionError(reason)

    if shutil.which('sudo') is None:
        raise PermissionError(f"{reason} (sudo not found)")

    print(f"Permission required to {reason}.")
    if not ensure_sudo_credentials():
        raise PermissionError(f"{reason} (sudo auth failed)")

    env = os.environ.copy()
    env['GITHUB_ANALYTICS_REEXECED_WITH_SUDO'] = '1'
    cmd = ['sudo', '-E', sys.executable] + sys.argv
    os.execvpe('sudo', cmd, env)


def parse_proc_mounts() -> List[str]:
    for proc_path in (Path('/proc/self/mounts'), Path('/proc/mounts')):
        if not proc_path.is_file():
            continue
        try:
            mount_points: List[str] = []
            for line in proc_path.read_text(encoding='utf-8', errors='replace').splitlines():
                parts = line.split()
                if len(parts) >= 2:
                    mount_points.append(parts[1])
            return mount_points
        except Exception:
            continue
    return []


def run(cmd: List[str]) -> tuple[int, str]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        out = (proc.stdout or '').strip()
        if not out:
            out = (proc.stderr or '').strip()
        return proc.returncode, out
    except Exception as e:
        return 1, str(e)


def parse_zfs_mountpoints(use_sudo: bool) -> List[Path]:
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
        v = (line or '').strip()
        if not v or v in {'-', 'none', 'legacy'}:
            continue
        p = Path(v)
        if p.is_absolute():
            mountpoints.append(p)
    return mountpoints


def auto_detect_snapshot_root() -> Optional[Path]:
    candidates: List[Path] = []

    for mount_point in parse_proc_mounts():
        mp = Path(mount_point)
        snap = mp / '.zfs' / 'snapshot'
        try:
            if snap.is_dir():
                candidates.append(snap.resolve())
        except Exception:
            continue

    zfs_mounts = parse_zfs_mountpoints(use_sudo=False)
    if not zfs_mounts and shutil.which('zfs') is not None and ensure_sudo_credentials():
        zfs_mounts = parse_zfs_mountpoints(use_sudo=True)

    for mp in zfs_mounts:
        snap = mp / '.zfs' / 'snapshot'
        try:
            if snap.is_dir():
                candidates.append(snap.resolve())
        except Exception:
            continue

    for root in (Path('/mnt'), Path('/media'), Path('/storage'), Path('/srv'), Path('/pool'), Path('/tank')):
        if not root.is_dir():
            continue
        try:
            for child in root.iterdir():
                if child.is_dir() and not child.name.startswith('.'):
                    snap = child / '.zfs' / 'snapshot'
                    if snap.is_dir():
                        candidates.append(snap.resolve())
        except Exception:
            continue

    for fallback in (Path('/.zfs/snapshot'), Path('/mnt/pool/.zfs/snapshot')):
        if fallback.is_dir():
            candidates.append(fallback.resolve())

    # Choose the candidate with the most snapshots.
    best = None
    best_count = -1
    for cand in candidates:
        try:
            count = sum(1 for p in cand.iterdir() if p.is_dir())
        except Exception:
            continue
        if count > best_count:
            best = cand
            best_count = count
    return best


def list_snapshots(snapshot_root: Path) -> List[Path]:
    if not snapshot_root.exists():
        return []
    return [p for p in snapshot_root.iterdir() if p.is_dir()]


def probe_snapshot_access(snapshot_root: Path) -> None:
    first_snapshot = None
    for p in snapshot_root.iterdir():
        if p.is_dir():
            first_snapshot = p
            break
    if first_snapshot is None:
        return
    if not os.access(first_snapshot, os.R_OK | os.X_OK):
        raise PermissionError(str(first_snapshot))


def find_git_roots(base_path: Path, max_depth: int) -> List[Path]:
    git_roots: List[Path] = []

    def walk(current: Path, depth: int) -> None:
        if depth > max_depth:
            return
        try:
            if (current / ".git").is_dir():
                git_roots.append(current)
                return

            for child in current.iterdir():
                try:
                    if child.is_dir() and not child.name.startswith("."):
                        walk(child, depth + 1)
                except (PermissionError, OSError):
                    continue
        except (PermissionError, OSError):
            return

    walk(base_path, 0)
    return git_roots


def parse_repo_full_name_from_remote_url(url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    # git@github.com:owner/repo.git
    m = re.match(r"^git@github\.com:([^/]+)/([^/]+?)(?:\.git)?$", url)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    # https://github.com/owner/repo(.git)
    m = re.match(r"^https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$", url)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return None


def infer_repository_identifier(repo_root: Path) -> str:
    """Best-effort repository identifier.

    Prefers GitHub-style owner/repo derived from origin URL, falling back to
    directory name.
    """
    config_path = repo_root / ".git" / "config"
    if config_path.is_file():
        try:
            parser = configparser.ConfigParser()
            parser.read(config_path)
            section = 'remote "origin"'
            if parser.has_section(section):
                url = parser.get(section, 'url', fallback='')
                full_name = parse_repo_full_name_from_remote_url(url)
                if full_name:
                    return full_name
        except Exception:
            pass

    return repo_root.name


def iter_files(repo_root: Path, excludes: Iterable[str]) -> Iterable[Path]:
    exclude_set = set(excludes)
    for root, dirs, files in os.walk(repo_root):
        dirs[:] = [d for d in dirs if d not in exclude_set]
        for name in files:
            if name.startswith("."):
                continue
            yield Path(root) / name


ZfsGranularity = Literal['file', 'repo_index', 'repo_root']


def collect_snapshot_rows(
    snapshot_name: str,
    repo_root: Path,
    user: str,
    excludes: Iterable[str],
    granularity: ZfsGranularity = 'file',
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    repo_id = infer_repository_identifier(repo_root)

    if granularity != 'file':
        if granularity == 'repo_index':
            ts_path = repo_root / '.git' / 'index'
            if not ts_path.exists():
                ts_path = repo_root
        else:
            ts_path = repo_root

        try:
            stat = ts_path.stat()
        except (OSError, FileNotFoundError):
            return rows

        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        rows.append(
            {
                "snapshot": snapshot_name,
                "repository": repo_id,
                "file": "",
                "event_timestamp": mtime.isoformat(),
                "user": user,
                "author": user,
                "attributed_user": user,
                "status": "WT",
                "commit": None,
                "source": "zfs_snapshot",
                "granularity": granularity,
            }
        )
        return rows

    for path in iter_files(repo_root, excludes):
        try:
            stat = path.stat()
        except (OSError, FileNotFoundError):
            continue
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        rows.append(
            {
                "snapshot": snapshot_name,
                "repository": repo_id,
                "file": str(path.relative_to(repo_root)).replace("\\", "/"),
                "event_timestamp": mtime.isoformat(),
                "user": user,
                "author": user,
                "attributed_user": user,
                "status": "WT",
                "commit": None,
                "source": "zfs_snapshot",
                "granularity": "file",
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
        "--granularity",
        choices=['file', 'repo_index', 'repo_root'],
        default='file',
        help="Event granularity: file (slow), repo_index (fast), repo_root (fast)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Directory name to exclude (can be repeated)",
    )
    parser.add_argument(
        "--no-sudo",
        action="store_true",
        help="Do not attempt to prompt for sudo or re-run under sudo",
    )
    parser.add_argument(
        "--emit-standard-sheets",
        action="store_true",
        help="Also write File Events / File Timestamp List / User Timeline sheets",
    )

    args = parser.parse_args()

    allow_sudo = not args.no_sudo

    snapshot_root = Path(args.snapshot_root).expanduser().resolve()
    if not snapshot_root.exists():
        detected = auto_detect_snapshot_root()
        if detected is not None:
            print(f"Provided snapshot root not found; using detected: {detected}")
            snapshot_root = detected
        else:
            print(f"Snapshot root not found and could not auto-detect: {snapshot_root}")
            return

    if os.geteuid() != 0:
        try:
            probe_snapshot_access(snapshot_root)
        except PermissionError:
            maybe_reexec_with_sudo(f"traverse ZFS snapshots under {snapshot_root}", enabled=allow_sudo)

    # If snapshot_root exists but is not readable, re-run under sudo.
    try:
        next(iter(snapshot_root.iterdir()), None)
    except PermissionError:
        maybe_reexec_with_sudo(f"read ZFS snapshot directory {snapshot_root}", enabled=allow_sudo)

    excludes = sorted(set(DEFAULT_EXCLUDES).union(args.exclude))

    try:
        snapshots = list_snapshots(snapshot_root)
    except PermissionError:
        maybe_reexec_with_sudo(f"list snapshots under {snapshot_root}", enabled=allow_sudo)
        return
    print(f"Snapshot root: {snapshot_root}")
    print(f"Found {len(snapshots)} snapshots")

    all_rows: List[Dict[str, str]] = []
    for snap in snapshots:
        try:
            repos = find_git_roots(snap, args.max_depth)
        except PermissionError:
            maybe_reexec_with_sudo(f"scan snapshot {snap}", enabled=allow_sudo)
            return

        if not repos:
            continue
        print(f"Snapshot {snap.name}: {len(repos)} repos")
        for repo in repos:
            try:
                all_rows.extend(
                    collect_snapshot_rows(snap.name, repo, args.user, excludes, granularity=args.granularity)
                )
            except PermissionError:
                maybe_reexec_with_sudo(f"read files in snapshot repo {repo}", enabled=allow_sudo)
                return

    if not all_rows:
        print("No files found.")
        return

    df = pd.DataFrame(all_rows)
    df = df.sort_values(["snapshot", "repository", "event_timestamp"], ascending=[True, True, False])

    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ZFS Snapshot Timestamps", index=False)

        if args.emit_standard_sheets:
            # Align with local_git_analytics sheet names so timesheet_from_timestamps can consume.
            file_events_df = df.copy()
            file_events_df = file_events_df.sort_values('event_timestamp', ascending=False)
            file_events_df.to_excel(writer, sheet_name='File Events', index=False)

            file_timestamp_columns = [
                'repository',
                'file',
                'event_timestamp',
                'user',
                'commit',
                'status'
            ]
            available_columns = [c for c in file_timestamp_columns if c in file_events_df.columns]
            file_timestamp_df = file_events_df[available_columns]
            file_timestamp_df.to_excel(writer, sheet_name='File Timestamp List', index=False)

            timeline_df = file_events_df.copy()
            timeline_df.insert(0, 'event_type', 'file')
            timeline_df = timeline_df.sort_values('event_timestamp', ascending=False)
            timeline_df.to_excel(writer, sheet_name='User Timeline', index=False)

    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
