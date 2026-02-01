#!/usr/bin/env python3
"""
Clone all GitHub repositories and analyze them locally.

Uses gh CLI for authentication and clones repos as bare repositories
(git history only, no working files) for fast analysis.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from datetime import datetime

from github_analyitics.timestamp_audit.local_git_analytics import LocalGitAnalytics


def run_command(cmd, cwd=None, capture=True):
    """Run a command and return output."""
    try:
        if capture:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=True,
                shell=True
            )
            return result.stdout.strip()
        else:
            subprocess.run(cmd, cwd=cwd, check=True, shell=True)
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
        print(f"Error: {e}")
        if capture and e.stderr:
            print(f"Stderr: {e.stderr}")
        return None


def check_gh_cli():
    """Check if gh CLI is installed and authenticated."""
    print("Checking gh CLI authentication...")
    result = run_command("gh auth status")
    if result is None:
        print("Error: gh CLI is not authenticated.")
        print("Please run: gh auth login")
        return False
    print("[OK] gh CLI authenticated")
    return True


def get_username():
    """Get GitHub username from gh CLI."""
    username = run_command("gh api user -q .login")
    if username:
        print(f"[OK] GitHub username: {username}")
        return username
    return None


def list_repositories(username):
    """List all repositories for the authenticated user."""
    print(f"\nFetching repository list for {username}...")
    
    # Use gh CLI to list all repos (including private ones)
    cmd = f'gh repo list {username} --limit 1000 --json name,nameWithOwner'
    output = run_command(cmd)
    
    if not output:
        return []
    
    import json
    repos = json.loads(output)
    print(f"[OK] Found {len(repos)} repositories")
    return repos


def list_orgs() -> list[str]:
    """List organizations for the authenticated user (best-effort)."""
    output = run_command("gh api user/orgs --paginate -q '.[].login'")
    if not output:
        return []
    return [line.strip() for line in output.splitlines() if line.strip()]


def list_repositories_for_owners(owners: list[str]) -> list[dict]:
    """List repositories for multiple owners (users/orgs) and dedupe by nameWithOwner."""
    seen = set()
    all_repos: list[dict] = []
    for owner in owners:
        repos = list_repositories(owner)
        for repo in repos:
            key = repo.get('nameWithOwner')
            if not key or key in seen:
                continue
            seen.add(key)
            all_repos.append(repo)
    return all_repos


def clone_repository(repo_full_name, target_dir, clone_mode: str):
    """Clone a repository (bare or full)."""
    repo_name = repo_full_name.split('/')[-1]
    target_path = target_dir / repo_name

    if clone_mode == 'bare':
        cmd = f'gh repo clone {repo_full_name} "{target_path}" -- --bare --no-tags'
    else:
        cmd = f'gh repo clone {repo_full_name} "{target_path}" -- --no-tags'

    mode_desc = 'bare' if clone_mode == 'bare' else 'full'
    print(f"  Cloning {repo_name} ({mode_desc}, no tags)...")
    for attempt in range(1, 4):
        run_command(cmd, capture=False)
        if target_path.exists():
            return target_path
        # Cleanup and retry
        if target_path.exists():
            shutil.rmtree(target_path, ignore_errors=True)
        print(f"  Retry {attempt}/3 failed for {repo_name}")
    return None


def update_repository(repo_path: Path, clone_mode: str) -> bool:
    """Fetch latest data for an existing repository (bare or full)."""
    fetch_cmd = f'git -C "{repo_path}" fetch --all --prune --no-tags'
    print(f"  Fetching updates for {repo_path.name}...")
    run_command(fetch_cmd, capture=False)

    if clone_mode != 'bare':
        # Best-effort: update the checked-out branch as well.
        pull_cmd = f'git -C "{repo_path}" pull --ff-only'
        run_command(pull_cmd, capture=False)

    return True


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Clone repositories with gh and run local git analytics.'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for analysis (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for analysis (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (default: data_reports/<timestamp>/github_analysis_with_file_timestamps.xlsx)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data_reports',
        help='Base directory for timestamped outputs when --output is not provided (default: data_reports)'
    )
    parser.add_argument(
        '--cache-dir',
        type=str,
        help='Directory to store/reuse bare repositories (default: .cache/github_analysis_{username})'
    )
    parser.add_argument(
        '--copilot-invokers',
        type=str,
        help='Path to JSON or CSV mapping Copilot identities to invoker usernames'
    )
    parser.add_argument(
        '--include-repos',
        type=str,
        default=None,
        help='Comma-separated repository names to include (filters clone list)'
    )
    parser.add_argument(
        '--exclude-repos',
        type=str,
        default=None,
        help='Comma-separated repository names to exclude (filters clone list)'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Delete temporary clones without prompting'
    )
    parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='Keep temporary clones without prompting'
    )
    parser.add_argument(
        '--clone-mode',
        choices=['bare', 'full'],
        default='bare',
        help='Clone mode: bare (history only) or full (working tree) (default: bare)'
    )
    parser.add_argument(
        '--include-orgs',
        action='store_true',
        help='Also include repositories from orgs you belong to'
    )
    parser.add_argument(
        '--owners',
        type=str,
        default=None,
        help='Comma-separated owners (users/orgs) to include (default: authenticated user)'
    )
    parser.add_argument(
        '--include-working-tree-timestamps',
        action='store_true',
        help='Include filesystem mtime events from cloned working trees (requires --clone-mode full)'
    )
    parser.add_argument(
        '--working-tree-user',
        type=str,
        default=None,
        help='User to attribute working tree mtime events to (passed to local_git_analytics)'
    )

    args = parser.parse_args()

    print("=" * 70)
    print("GitHub Repository Analyzer")
    print("=" * 70)
    print()
    
    # Check gh CLI
    if not check_gh_cli():
        return 1
    
    # Get username
    username = get_username()
    if not username:
        print("Error: Could not determine GitHub username")
        return 1

    # Determine output path
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        from github_analyitics.reporting.report_paths import default_xlsx_path

        output_path = default_xlsx_path(
            'github_analysis_with_file_timestamps.xlsx',
            base_dir=args.output_dir,
        )

    # List repositories
    owners = [username]
    if args.owners:
        owners = [o.strip() for o in args.owners.split(',') if o.strip()]
    if args.include_orgs:
        orgs = list_orgs()
        for org in orgs:
            if org not in owners:
                owners.append(org)

    repos = list_repositories_for_owners(owners)
    if not repos:
        print("Error: No repositories found or could not fetch repository list")
        return 1

    include_set = None
    exclude_set = None
    if args.include_repos:
        include_set = {r.strip() for r in args.include_repos.split(',') if r.strip()}
    if args.exclude_repos:
        exclude_set = {r.strip() for r in args.exclude_repos.split(',') if r.strip()}

    if include_set or exclude_set:
        filtered = []
        for repo in repos:
            name = repo.get('name')
            if not name:
                continue
            if exclude_set and name in exclude_set:
                continue
            if include_set and name not in include_set:
                continue
            filtered.append(repo)
        repos = filtered
        print(f"[OK] Repositories after filtering: {len(repos)}")
    
    # Create/reuse cache directory for bare clones
    script_dir = Path(__file__).parent
    default_cache = script_dir / '.cache' / f'github_analysis_{username}'
    cache_dir = Path(args.cache_dir).expanduser().resolve() if args.cache_dir else default_cache
    cache_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[OK] Using repository cache directory: {cache_dir}")
    print()
    
    try:
        # Clone repositories
        print("=" * 70)
        if args.clone_mode == 'bare':
            print("Syncing repositories (bare - git history only)...")
        else:
            print("Syncing repositories (full - includes working tree)...")
        print("=" * 70)
        
        cloned_repos = []
        failed_repos = []
        
        for idx, repo in enumerate(repos, 1):
            repo_full_name = repo['nameWithOwner']
            print(f"[{idx}/{len(repos)}] {repo_full_name}")
            
            repo_name = repo_full_name.split('/')[-1]
            repo_path = cache_dir / repo_name
            if repo_path.exists():
                updated = update_repository(repo_path, args.clone_mode)
                if updated:
                    cloned_repos.append(repo_path)
                else:
                    failed_repos.append(repo_full_name)
                continue

            repo_path = clone_repository(repo_full_name, cache_dir, args.clone_mode)
            if repo_path:
                cloned_repos.append(repo_path)
            else:
                failed_repos.append(repo_full_name)
        
        print()
        print(f"[OK] Successfully cloned: {len(cloned_repos)} repositories")
        if failed_repos:
            print(f"✗ Failed to clone: {len(failed_repos)} repositories")
            for repo in failed_repos:
                print(f"  - {repo}")
        print()
        
        # Run analysis
        if cloned_repos:
            print("=" * 70)
            print("Running local git analysis...")
            print("=" * 70)
            print()
            
            output_file = str(output_path)

            def parse_date(value: str | None) -> datetime | None:
                if not value:
                    return None
                return datetime.strptime(value, '%Y-%m-%d')

            start_date = parse_date(args.start_date)
            end_date = parse_date(args.end_date)

            include_working_tree_timestamps = bool(args.include_working_tree_timestamps)
            if include_working_tree_timestamps and args.clone_mode != 'full':
                print("Warning: --include-working-tree-timestamps requires --clone-mode full; skipping.")
                include_working_tree_timestamps = False

            analytics = LocalGitAnalytics(str(cache_dir), args.copilot_invokers, allowed_users=None)
            analytics.generate_report(
                output_file=output_file,
                start_date=start_date,
                end_date=end_date,
                include_repos=None,
                exclude_repos=None,
                max_depth=2,
                use_session_estimation=True,
                allowed_users=None,
                include_working_tree_timestamps=include_working_tree_timestamps,
                working_tree_user=args.working_tree_user,
                working_tree_excludes=None,
            )
            
            print()
            print("=" * 70)
            print(f"[OK] Analysis complete!")
            print(f"[OK] Report saved to: {output_file}")
            print("=" * 70)
        
    finally:
        # Cleanup
        print()
        if args.cleanup:
            print(f"Cleaning up repository cache directory: {cache_dir}")
            shutil.rmtree(cache_dir, ignore_errors=True)
            print("[OK] Cleanup complete")
        else:
            print(f"Repository cache kept at: {cache_dir}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
