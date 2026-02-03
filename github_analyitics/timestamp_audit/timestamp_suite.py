#!/usr/bin/env python3
"""Unified timestamp collection suite.

This orchestrates *all* timestamp sources in one sweep and writes a single
multi-sheet Excel workbook.

Sources supported:
- GitHub API: commits, PRs (created/closed/merged + optional comments/reviews), issues + comments
- Local git history: commit + file modification events (via git log)
- Working tree mtimes (optional)
- ZFS snapshot working tree mtimes (optional)

Primary output:
- "All Events": normalized event table for downstream analysis
- "User Timeline": alias of All Events (kept for compatibility with timesheet_from_timestamps.py)

It also includes the native per-source sheets (Detailed/User/Daily summaries, etc)
when available.
"""

from __future__ import annotations

import argparse
import os
import time
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from dotenv import load_dotenv

from github_analyitics.timestamp_audit.collect_all_timestamps import (
    collect_local_git_and_zfs_sweep,
    detect_github_username,
    detect_max_depth,
    detect_zfs_snapshot_roots,
    guess_repos_base_path,
    probe_snapshot_access,
    rank_snapshot_roots,
    snapshot_root_mountpoint,
    ensure_sudo_credentials,
    maybe_reexec_with_sudo,
)
from github_analyitics.reporting.github_analytics import GitHubAnalytics
from github_analyitics.timestamp_audit.local_git_analytics import LocalGitAnalytics
from github_analyitics.timestamp_audit.zfs_snapshot_git_timestamps import (
    DEFAULT_EXCLUDES as ZFS_DEFAULT_EXCLUDES,
    find_git_roots,
    is_git_repo_dir,
    list_snapshots,
)


def _run(cmd: List[str]) -> tuple[int, str]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        out = (proc.stdout or '').strip()
        err = (proc.stderr or '').strip()
        return proc.returncode, out or err
    except Exception as e:
        return 1, str(e)


def _zfs_dataset_for_mountpoint(mountpoint: Path) -> Optional[str]:
    if shutil.which('zfs') is None:
        return None
    code, out = _run(['zfs', 'list', '-H', '-o', 'name,mountpoint'])
    if code != 0 or not out:
        return None
    mp = str(mountpoint)
    for line in out.splitlines():
        parts = (line or '').split('\t')
        if len(parts) < 2:
            # Some zfs versions separate by spaces when not using -p; be tolerant.
            parts = (line or '').split()
        if len(parts) < 2:
            continue
        name = parts[0].strip()
        mpt = parts[1].strip()
        if mpt == mp:
            return name
    return None


def _zfs_get_property(dataset: str, prop: str) -> Optional[str]:
    if not dataset or shutil.which('zfs') is None:
        return None
    code, out = _run(['zfs', 'get', '-H', '-o', 'value', prop, dataset])
    if code != 0 or not out:
        return None
    return out.splitlines()[0].strip()


def _zfs_count_snapshots(dataset: str) -> Optional[int]:
    if not dataset or shutil.which('zfs') is None:
        return None
    # Count snapshots in the dataset subtree.
    code, out = _run(['zfs', 'list', '-H', '-t', 'snapshot', '-o', 'name', '-r', dataset])
    if code != 0:
        return None
    if not out.strip():
        return 0
    return len([ln for ln in out.splitlines() if (ln or '').strip()])


def _resolve_allowed_users_path(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        return Path(explicit).expanduser()

    for candidate in (
        Path.cwd() / 'allowed_users.txt',
        Path.cwd() / '_allowed_users.txt',
    ):
        if candidate.exists():
            return candidate
    return None


def parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def ensure_utc_iso(value: str) -> str:
    """Best-effort normalize timestamps to UTC ISO8601 strings."""
    try:
        dt = pd.to_datetime(value, utc=True)
        if pd.isna(dt):
            return value
        return dt.to_pydatetime().astimezone(timezone.utc).isoformat()
    except Exception:
        return value


def add_source(events: List[Dict], source: str) -> List[Dict]:
    out: List[Dict] = []
    for ev in events or []:
        row = dict(ev)
        row.setdefault('source', source)
        if 'event_timestamp' in row and row['event_timestamp']:
            row['event_timestamp'] = ensure_utc_iso(str(row['event_timestamp']))
        out.append(row)
    return out


def normalize_github_events(
    *,
    commit_events: List[Dict],
    pr_events: List[Dict],
    issue_events: List[Dict],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    def norm_user(row: Dict) -> Dict:
        row = dict(row)
        author = row.get('author')
        if 'user' not in row:
            row['user'] = author
        if 'attributed_user' not in row:
            row['attributed_user'] = author
        return row

    commits = [norm_user(ev) for ev in commit_events or []]
    prs = [norm_user(ev) for ev in pr_events or []]
    issues = [norm_user(ev) for ev in issue_events or []]

    # Align commit keys with local where possible
    for ev in commits:
        ev.setdefault('commit', ev.get('commit'))
        ev.setdefault('subject', ev.get('subject', ''))

    return commits, prs, issues


def dataframe_or_empty(rows: List[Dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _excel_max_rows() -> int:
    # Excel worksheet row limit is 1,048,576.
    # Allow overriding for tests (and power users) via env.
    try:
        return int(os.getenv('GITHUB_ANALYTICS_EXCEL_MAX_ROWS', '1048576') or '1048576')
    except Exception:
        return 1048576


def _sheet_name_with_suffix(base: str, index: int) -> str:
    # Excel sheet name limit is 31 chars.
    base = (base or '').strip() or 'Sheet'
    if index <= 1:
        return base[:31]

    suffix = f" ({index})"
    max_base_len = max(1, 31 - len(suffix))
    return f"{base[:max_base_len]}{suffix}"


def write_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    allow_empty: bool = False,
) -> None:
    if df is None:
        return
    if df.empty and not allow_empty:
        return

    max_rows = _excel_max_rows()
    max_data_rows = max(1, max_rows - 1)
    row_count = int(len(df))

    if row_count <= max_data_rows:
        df.to_excel(writer, sheet_name=_sheet_name_with_suffix(sheet_name, 1), index=False)
        return

    # Split across multiple worksheets to stay within Excel row limits.
    total_sheets = (row_count + max_data_rows - 1) // max_data_rows
    print(f"[XLSX] Sheet '{sheet_name}' has {row_count} rows; splitting into {total_sheets} sheets")

    for i, start in enumerate(range(0, row_count, max_data_rows), 1):
        chunk = df.iloc[start : start + max_data_rows]
        chunk.to_excel(writer, sheet_name=_sheet_name_with_suffix(sheet_name, i), index=False)


def build_all_events(*event_lists: Sequence[Dict]) -> pd.DataFrame:
    combined: List[Dict] = []
    for events in event_lists:
        for ev in events:
            combined.append(dict(ev))

    df = pd.DataFrame(combined)
    if df.empty:
        return df

    if 'event_timestamp' in df.columns:
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], utc=True, errors='coerce')

    # Put the most useful columns first when present
    preferred = [
        'event_timestamp',
        'source',
        'event_type',
        'repository',
        'user',
        'attributed_user',
        'author',
        'email',
        'number',
        'title',
        'commit',
        'file',
        'status',
        'snapshot',
        'granularity',
        'url',
        'subject',
    ]
    ordered = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[ordered]

    if 'event_timestamp' in df.columns:
        df = df.sort_values('event_timestamp', ascending=False)
        # Keep as ISO strings for downstream scripts.
        df['event_timestamp'] = df['event_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    return df


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Collect all timestamps (GitHub + local + ZFS) into one workbook.")
    parser.add_argument('--output', default=None, help='Output Excel file (default: data_reports/<timestamp>/github_analytics_timestamps_suite.xlsx)')
    parser.add_argument(
        '--output-dir',
        default='data_reports',
        help='Base directory for timestamped outputs when --output is not provided (default: data_reports)',
    )
    parser.add_argument('--start-date', default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default=None, help='End date (YYYY-MM-DD)')

    parser.add_argument(
        '--allowed-users-file',
        default=None,
        help='Path to allowed users list (one identifier per line). Only these users will appear in output sheets.',
    )

    parser.add_argument('--verbose', action='store_true', help='Verbose progress logging (prints phase timings and gh commands)')
    parser.add_argument(
        '--local-progress-every-seconds',
        type=float,
        default=60.0,
        help='Local git: watchdog heartbeat interval while running `git log` (0 disables; default: 60)',
    )
    parser.add_argument(
        '--gh-timeout-seconds',
        type=float,
        default=120.0,
        help='Timeout (seconds) for individual `gh` commands when collecting GitHub data (0 disables)',
    )

    parser.add_argument(
        '--sources',
        default='github,local,zfs',
        help='Comma-separated sources: github,local,zfs (default: github,local,zfs)',
    )

    # GitHub options
    parser.add_argument('--github-token', default=None, help='(Deprecated) GitHub token (not required when using gh auth)')
    parser.add_argument('--github-username', default=None, help='GitHub username (default: env GITHUB_USERNAME)')
    parser.add_argument('--skip-file-modifications', action='store_true', help='GitHub: skip file modifications')
    parser.add_argument('--skip-commit-stats', action='store_true', help='GitHub: skip commit stats lookup')
    parser.add_argument('--disable-rate-limiting', action='store_true', help='GitHub: disable rate limiting')
    parser.add_argument(
        '--include-pr-comments',
        action='store_true',
        default=True,
        help='GitHub: include PR comments + review comments (deprecated; enabled by default; use --skip-pr-comments to disable)',
    )
    parser.add_argument('--skip-pr-comments', action='store_true', help='GitHub: do not include PR comments/review comments')
    parser.add_argument('--skip-pr-review-comments', action='store_true', help='GitHub: do not include inline review comments')
    parser.add_argument(
        '--include-pr-review-events',
        action='store_true',
        default=True,
        help='GitHub: include PR review submission events (deprecated; enabled by default; use --skip-pr-review-events to disable)',
    )
    parser.add_argument('--skip-pr-review-events', action='store_true', help='GitHub: do not include PR review submission events')
    parser.add_argument(
        '--include-pr-issue-comments',
        action='store_true',
        default=True,
        help='GitHub: include PR comments via Issues API (deprecated; enabled by default; use --skip-pr-issue-comments to disable)',
    )
    parser.add_argument('--skip-pr-issue-comments', action='store_true', help='GitHub: do not include PR comments via Issues API')

    # Local/ZFS options
    parser.add_argument('--repos-path', default=None, help='Local repo scan base path (default: auto)')
    parser.add_argument('--max-depth', type=int, default=None, help='Local repo scan max depth (default: auto)')
    parser.add_argument('--user', default=None, help='Default user attribution for non-git-history sources')

    parser.add_argument(
        '--include-working-tree-timestamps',
        action='store_true',
        default=True,
        help='Include working tree mtimes (deprecated; enabled by default; use --skip-working-tree-timestamps to disable)',
    )
    parser.add_argument('--skip-working-tree-timestamps', action='store_true', help='Do not include working tree mtimes')
    parser.add_argument('--working-tree-exclude', action='append', default=[], help='Working tree exclude dir name (repeatable)')

    parser.add_argument(
        '--zfs-snapshot-root',
        default=None,
        help=(
            'ZFS snapshot root path to prioritize/include. By default the suite scans all detected snapshot roots; '
            'use --zfs-snapshot-root-only to restrict scanning to just this root.'
        ),
    )
    parser.add_argument(
        '--zfs-snapshot-root-only',
        action='store_true',
        help='ZFS: scan only the --zfs-snapshot-root (or env override) instead of scanning all detected roots',
    )
    parser.add_argument('--no-sudo', action='store_true', help='ZFS: do not prompt for sudo / re-exec under sudo')
    parser.add_argument('--all-zfs-snapshot-roots', action='store_true', help='ZFS: scan all detected roots (deprecated; default behavior is now exhaustive)')
    parser.add_argument('--zfs-snapshot-roots-limit', type=int, default=0, help='ZFS: max detected roots to scan (0=all; default: 0)')
    parser.add_argument('--zfs-scan-mode', choices=['match-repos-path', 'full'], default='full')
    parser.add_argument('--zfs-snapshots-limit', type=int, default=0, help='ZFS: max snapshots per root (0=all; default: 0)')
    parser.add_argument('--zfs-granularity', choices=['repo_index', 'repo_root', 'file'], default='file')
    parser.add_argument('--zfs-exclude', action='append', default=[], help='ZFS: exclude dir name (repeatable)')
    parser.add_argument('--zfs-max-seconds-per-root', type=float, default=None, help='ZFS: time budget per root')
    parser.add_argument(
        '--zfs-progress-every-seconds',
        type=float,
        default=30.0,
        help='ZFS: when --verbose, print a watchdog heartbeat every N seconds (0 disables; default: 30)',
    )

    args = parser.parse_args()

    # Enable watchdog heartbeats during slow local `git log` calls.
    # This is intentionally not gated behind --verbose so default runs are still comprehensible.
    try:
        os.environ['GITHUB_ANALYTICS_LOCAL_PROGRESS_EVERY_SECONDS'] = str(float(args.local_progress_every_seconds))
    except Exception:
        os.environ['GITHUB_ANALYTICS_LOCAL_PROGRESS_EVERY_SECONDS'] = '60'

    verbose = bool(getattr(args, 'verbose', False))
    if verbose:
        os.environ['GITHUB_ANALYTICS_VERBOSE'] = '1'

    try:
        if args.gh_timeout_seconds and float(args.gh_timeout_seconds) > 0:
            os.environ['GITHUB_ANALYTICS_GH_TIMEOUT_SECONDS'] = str(float(args.gh_timeout_seconds))
        else:
            os.environ.pop('GITHUB_ANALYTICS_GH_TIMEOUT_SECONDS', None)
    except Exception:
        pass

    sources = {s.strip().lower() for s in (args.sources or '').split(',') if s.strip()}
    want_github = 'github' in sources
    want_local = 'local' in sources
    want_zfs = 'zfs' in sources

    # Comprehensive defaults, with opt-out flags.
    include_working_tree_timestamps = not bool(getattr(args, 'skip_working_tree_timestamps', False))
    include_pr_comments = not bool(getattr(args, 'skip_pr_comments', False))
    include_pr_review_events = not bool(getattr(args, 'skip_pr_review_events', False))
    include_pr_issue_comments = not bool(getattr(args, 'skip_pr_issue_comments', False))

    allowed_users_path = _resolve_allowed_users_path(args.allowed_users_file)
    if not allowed_users_path or not allowed_users_path.exists():
        raise SystemExit(
            "Error: allowed users file not found. "
            "Create allowed_users.txt (or _allowed_users.txt) or pass --allowed-users-file PATH."
        )

    allowed_users = LocalGitAnalytics.load_allowed_users(str(allowed_users_path))
    if not allowed_users:
        raise SystemExit(
            f"Error: allowed users file is empty or invalid: {allowed_users_path}. "
            "Add one username/email/name per line."
        )

    if verbose:
        print(f"Sources enabled: {', '.join(sorted(sources))}")
        print(f"Source flags: want_github={want_github} want_local={want_local} want_zfs={want_zfs}")

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        from github_analyitics.reporting.report_paths import default_xlsx_path

        output_path = default_xlsx_path(
            'github_analytics_timestamps_suite.xlsx',
            base_dir=args.output_dir,
        )

    if verbose:
        print(f"Output: {output_path}")

    # If we run under sudo, prefer the original invoking user.
    default_user = args.user or detect_github_username() or os.getenv('SUDO_USER') or os.getenv('USER') or 'Unknown'

    # --- Local / ZFS sweep ---
    local_summary_df = pd.DataFrame()
    local_commit_events: List[Dict] = []
    local_file_events: List[Dict] = []
    zfs_rows: List[Dict] = []

    if want_local or want_zfs:
        t0 = time.perf_counter()
        repos_path = guess_repos_base_path(args.repos_path)
        max_depth = detect_max_depth(repos_path, args.max_depth)
        allow_sudo = not args.no_sudo

        if verbose:
            print(f"Local repo scan base path: {repos_path}")
            print(f"Local repo scan max depth: {max_depth}")
            print(f"ZFS enabled: {want_zfs} (allow_sudo={allow_sudo})")

        snapshot_roots: List[Path] = []
        zfs_scan_summary: List[Dict] = []
        if want_zfs:
            if verbose:
                print("Detecting ZFS snapshot roots...")

            # Default behavior: scan *all* detected snapshot roots.
            # If the user explicitly wants a single root (useful for deterministic tests),
            # --zfs-snapshot-root-only preserves the prior single-root semantics.
            if args.zfs_snapshot_root_only:
                snapshot_roots = detect_zfs_snapshot_roots(args.zfs_snapshot_root)
            else:
                snapshot_roots = detect_zfs_snapshot_roots(None)
                if args.zfs_snapshot_root:
                    explicit = detect_zfs_snapshot_roots(args.zfs_snapshot_root)
                    for r in explicit:
                        if all(str(r) != str(existing) for existing in snapshot_roots):
                            snapshot_roots.append(r)

                if snapshot_roots:
                    snapshot_roots = rank_snapshot_roots(snapshot_roots)
                    limit = 0 if args.all_zfs_snapshot_roots else int(args.zfs_snapshot_roots_limit)
                    if limit > 0:
                        snapshot_roots = snapshot_roots[: max(limit, 1)]

            if verbose:
                if snapshot_roots:
                    print("ZFS snapshot roots:")
                    for p in snapshot_roots:
                        print(f"- {p}")
                else:
                    print("ZFS snapshot roots: (none found)")

            # Lightweight scan summary so users can tell whether ZFS scanning found anything.
            now_iso = datetime.now(timezone.utc).isoformat()
            if snapshot_roots:
                for root in snapshot_roots:
                    try:
                        snapshots = list_snapshots(root)
                        snapshot_count = len(snapshots)
                    except Exception:
                        snapshots = []
                        snapshot_count = None

                    example_snapshot = None
                    repos_found: Optional[int] = 0 if snapshot_count == 0 else None

                    mountpoint = snapshot_root_mountpoint(root)
                    dataset = _zfs_dataset_for_mountpoint(mountpoint) if mountpoint else None
                    snapdir_value = _zfs_get_property(dataset, 'snapdir') if dataset else None
                    cli_snapshot_count = _zfs_count_snapshots(dataset) if dataset else None
                    snapdir_hidden_warning = None
                    if snapdir_value == 'hidden' and (cli_snapshot_count or 0) > 0:
                        snapdir_hidden_warning = (
                            'snapdir=hidden: snapshots may exist (per zfs CLI) but are not visible under .zfs/snapshot '
                            'for filesystem scanning'
                        )
                    try:
                        if snapshots:
                            # Prefer newest snapshot names when possible.
                            try:
                                snapshots_sorted = sorted(snapshots, key=lambda p: p.name, reverse=True)
                            except Exception:
                                snapshots_sorted = snapshots
                            snap = snapshots_sorted[0]
                            example_snapshot = snap.name

                            scan_base = snap
                            if args.zfs_scan_mode == 'match-repos-path':
                                mountpoint = snapshot_root_mountpoint(root)
                                rel = None
                                if mountpoint is not None:
                                    try:
                                        rel = repos_path.relative_to(mountpoint)
                                    except Exception:
                                        rel = None
                                if rel is not None:
                                    candidate = snap / rel
                                    if candidate.exists():
                                        scan_base = candidate

                            if is_git_repo_dir(scan_base):
                                repos_found = 1
                            else:
                                repos_found = len(find_git_roots(scan_base, max_depth=max_depth))
                    except Exception:
                        pass
                    zfs_scan_summary.append(
                        {
                            'event_timestamp': now_iso,
                            'snapshot_root': str(root),
                            'snapshots_detected': snapshot_count,
                            'zfs_dataset': dataset,
                            'zfs_snapdir': snapdir_value,
                            'zfs_cli_snapshots_detected': cli_snapshot_count,
                            'zfs_warning': snapdir_hidden_warning,
                            'example_snapshot': example_snapshot,
                            'repos_found_in_example_snapshot': repos_found,
                            'scan_mode': args.zfs_scan_mode,
                            'snapshots_limit': int(args.zfs_snapshots_limit),
                            'granularity': args.zfs_granularity,
                            'max_seconds_per_root': args.zfs_max_seconds_per_root,
                        }
                    )
            else:
                zfs_scan_summary.append(
                    {
                        'event_timestamp': now_iso,
                        'snapshot_root': None,
                        'snapshots_detected': 0,
                        'zfs_dataset': None,
                        'zfs_snapdir': None,
                        'zfs_cli_snapshots_detected': 0,
                        'zfs_warning': None,
                        'example_snapshot': None,
                        'repos_found_in_example_snapshot': 0,
                        'scan_mode': args.zfs_scan_mode,
                        'snapshots_limit': int(args.zfs_snapshots_limit),
                        'granularity': args.zfs_granularity,
                        'max_seconds_per_root': args.zfs_max_seconds_per_root,
                    }
                )

            # Prompt for sudo up-front (instead of mid-scan) when required.
            # Avoid prompting unnecessarily (e.g. when snapshot roots are already readable).
            if snapshot_roots and allow_sudo and os.geteuid() != 0:
                need_sudo = False
                for root in snapshot_roots:
                    try:
                        probe_snapshot_access(root)
                    except PermissionError:
                        need_sudo = True
                        break

                if need_sudo:
                    ensure_sudo_credentials()
                    for root in snapshot_roots:
                        try:
                            probe_snapshot_access(root)
                        except PermissionError:
                            maybe_reexec_with_sudo(
                                f"traverse ZFS snapshots under {root}",
                                enabled=True,
                            )

        zfs_excludes = sorted(set(ZFS_DEFAULT_EXCLUDES).union(args.zfs_exclude))

        # Comprehensive default: local git history scan is included whenever we run the
        # local/ZFS sweep. ZFS progress is handled inside the sweep functions.
        local_summary_df, local_commit_events, local_file_events, zfs_rows = collect_local_git_and_zfs_sweep(
            repos_path=repos_path,
            max_depth=max_depth,
            start_date=start_date,
            end_date=end_date,
            default_user=default_user,
            include_working_tree_timestamps=include_working_tree_timestamps,
            working_tree_excludes=args.working_tree_exclude,
            snapshot_roots=snapshot_roots,
            allow_sudo=allow_sudo,
            zfs_scan_mode=args.zfs_scan_mode,
            zfs_snapshots_limit=args.zfs_snapshots_limit,
            zfs_granularity=args.zfs_granularity,
            zfs_excludes=zfs_excludes,
            zfs_max_seconds_per_root=args.zfs_max_seconds_per_root,
            zfs_progress_every_seconds=(float(args.zfs_progress_every_seconds) if verbose else None),
            verbose=verbose,
            allowed_users=allowed_users,
        )

        if verbose:
            elapsed = time.perf_counter() - t0
            print(f"Local/ZFS sweep complete in {elapsed:.2f}s")

    # --- GitHub API sweep ---
    gh_summary_df = pd.DataFrame()
    gh_commit_events: List[Dict] = []
    gh_pr_events: List[Dict] = []
    gh_issue_events: List[Dict] = []

    if want_github:
        t0 = time.perf_counter()
        username = (args.github_username or os.getenv('GITHUB_USERNAME') or '').strip()
        if not username:
            username = (detect_github_username() or '').strip()
        if not username:
            raise SystemExit('Error: GitHub source enabled but no username found (run `gh auth login` or pass --github-username).')

        if verbose:
            print(f"GitHub username: {username}")
            if args.skip_commit_stats:
                print("GitHub: skipping per-commit stats")
            if args.skip_file_modifications:
                print("GitHub: skipping file modification lists")
            if not include_pr_comments:
                print("GitHub: skipping PR comments")
            if not include_pr_review_events:
                print("GitHub: skipping PR review submission events")
            if not include_pr_issue_comments:
                print("GitHub: skipping PR issue comments")

        gh = GitHubAnalytics("", username, enable_rate_limiting=not args.disable_rate_limiting)
        gh_summary_df = gh.analyze_all_repositories(
            start_date=start_date,
            end_date=end_date,
            include_repos=None,
            exclude_repos=None,
            filter_by_user_contribution=None,
            skip_file_modifications=args.skip_file_modifications,
            skip_commit_stats=args.skip_commit_stats,
            restrict_to_collaborators=True,
            restrict_to_owner_namespace=True,
            fast_mode=False,
            include_pr_comments=include_pr_comments,
            include_pr_review_comments=(include_pr_comments and (not args.skip_pr_review_comments)),
            include_pr_review_events=include_pr_review_events,
            include_issue_pr_comments=include_pr_issue_comments,
            allowed_users=set(allowed_users),
        )

        gh_commit_events, gh_pr_events, gh_issue_events = normalize_github_events(
            commit_events=gh.commit_events,
            pr_events=gh.pr_events,
            issue_events=gh.issue_events,
        )

        if verbose:
            elapsed = time.perf_counter() - t0
            print(f"GitHub sweep complete in {elapsed:.2f}s")

    # --- Normalize and combine ---
    local_commit_events = add_source(local_commit_events, 'local_git')
    local_file_events = add_source(local_file_events, 'local_git')
    zfs_rows = add_source(zfs_rows, 'zfs_snapshot')

    gh_commit_events = add_source(gh_commit_events, 'github_api')
    gh_pr_events = add_source(gh_pr_events, 'github_api')
    gh_issue_events = add_source(gh_issue_events, 'github_api')

    # Build normalized event table
    if verbose:
        print("Building normalized event table...")
    all_events_df = build_all_events(
        local_commit_events,
        local_file_events,
        zfs_rows,
        gh_commit_events,
        gh_pr_events,
        gh_issue_events,
    )

    # Safety net: ensure output contains only allowlisted identities.
    if allowed_users:
        allowed_lower = {str(u).strip().lower() for u in allowed_users if u and str(u).strip()}
        for col in ("user", "author", "attributed_user"):
            if col in all_events_df.columns:
                s = all_events_df[col]
                mask = s.isna() | (s.astype(str).str.strip() == "") | s.astype(str).str.strip().str.lower().isin(allowed_lower)
                all_events_df = all_events_df[mask]

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        if verbose:
            print("Writing workbook...")

        # Always emit the ZFS sheet (even if there are no rows) so it's obvious whether ZFS ran.
        zfs_df = dataframe_or_empty(zfs_rows)
        if zfs_df.empty:
            zfs_df = pd.DataFrame(
                columns=[
                    'event_timestamp',
                    'source',
                    'repository',
                    'file',
                    'snapshot',
                    'granularity',
                    'user',
                    'attributed_user',
                    'author',
                    'email',
                    'commit',
                    'status',
                    'copilot_involved',
                    'invoker_source',
                    'raw_author',
                    'raw_email',
                ]
            )
        write_sheet(writer, 'ZFS Snapshot Timestamps', zfs_df, allow_empty=True)
        write_sheet(writer, 'ZFS Scan Summary', dataframe_or_empty(zfs_scan_summary), allow_empty=True)

        write_sheet(writer, 'All Events', all_events_df, allow_empty=True)
        # Compatibility: timesheet_from_timestamps prefers User Timeline.
        write_sheet(writer, 'User Timeline', all_events_df, allow_empty=True)

        # Local sheets
        if not local_summary_df.empty:
            write_sheet(writer, 'Local Detailed Report', local_summary_df)

            user_summary = (
                local_summary_df.groupby('user')
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
            write_sheet(writer, 'Local User Summary', user_summary)

            date_summary = (
                local_summary_df.groupby('date')
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
            write_sheet(writer, 'Local Daily Summary', date_summary)

        write_sheet(writer, 'Commit Events', dataframe_or_empty(local_commit_events))
        write_sheet(writer, 'File Events', dataframe_or_empty(local_file_events))

        # GitHub sheets
        if not gh_summary_df.empty:
            write_sheet(writer, 'GitHub Detailed Report', gh_summary_df)

            gh_user_summary = (
                gh_summary_df.groupby('user')
                .agg({
                    'commits': 'sum',
                    'lines_added': 'sum',
                    'lines_deleted': 'sum',
                    'total_lines_changed': 'sum',
                    'files_modified': 'sum',
                    'prs_created': 'sum',
                    'prs_merged': 'sum',
                    'issues_created': 'sum',
                    'issues_closed': 'sum',
                    'issue_comments': 'sum',
                    'estimated_hours': 'sum',
                })
                .reset_index()
                .sort_values('estimated_hours', ascending=False)
            )
            write_sheet(writer, 'GitHub User Summary', gh_user_summary)

            gh_date_summary = (
                gh_summary_df.groupby('date')
                .agg({
                    'commits': 'sum',
                    'lines_added': 'sum',
                    'lines_deleted': 'sum',
                    'total_lines_changed': 'sum',
                    'files_modified': 'sum',
                    'prs_created': 'sum',
                    'prs_merged': 'sum',
                    'issues_created': 'sum',
                    'issues_closed': 'sum',
                    'issue_comments': 'sum',
                    'estimated_hours': 'sum',
                    'user': 'count',
                })
                .reset_index()
                .rename(columns={'user': 'active_users'})
                .sort_values('date', ascending=False)
            )
            write_sheet(writer, 'GitHub Daily Summary', gh_date_summary)

        write_sheet(writer, 'GitHub Commit Events', dataframe_or_empty(gh_commit_events))
        write_sheet(writer, 'PR Events', dataframe_or_empty(gh_pr_events))
        write_sheet(writer, 'Issue Events', dataframe_or_empty(gh_issue_events))

    if verbose:
        print(f"Wrote unified timestamp suite workbook: {output_path}")
    else:
        print(f"Wrote unified timestamp suite workbook: {output_path}")


if __name__ == '__main__':
    main()
