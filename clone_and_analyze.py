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


def clone_bare_repository(repo_full_name, target_dir):
    """Clone a repository as a bare repository (git history only)."""
    repo_name = repo_full_name.split('/')[-1]
    target_path = target_dir / repo_name

    # Clone as bare repository (no working files, just git data)
    cmd = f'gh repo clone {repo_full_name} "{target_path}" -- --bare --no-tags'

    print(f"  Cloning {repo_name} (bare, no tags)...")
    for attempt in range(1, 4):
        run_command(cmd, capture=False)
        if target_path.exists():
            return target_path
        # Cleanup and retry
        if target_path.exists():
            shutil.rmtree(target_path, ignore_errors=True)
        print(f"  Retry {attempt}/3 failed for {repo_name}")
    return None


def update_bare_repository(repo_path: Path) -> bool:
    """Fetch latest data for an existing bare repository."""
    cmd = f'git -C "{repo_path}" fetch --all --prune --no-tags'
    print(f"  Fetching updates for {repo_path.name}...")
    result = run_command(cmd, capture=False)
    return result is None or result == ''


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
        help='Output file path (default: github_analysis_{username}_{timestamp}.xlsx)'
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
        '--cleanup',
        action='store_true',
        help='Delete temporary clones without prompting'
    )
    parser.add_argument(
        '--no-cleanup',
        action='store_true',
        help='Keep temporary clones without prompting'
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
    
    # List repositories
    repos = list_repositories(username)
    if not repos:
        print("Error: No repositories found or could not fetch repository list")
        return 1
    
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
        print("Syncing repositories (bare - git history only)...")
        print("=" * 70)
        
        cloned_repos = []
        failed_repos = []
        
        for idx, repo in enumerate(repos, 1):
            repo_full_name = repo['nameWithOwner']
            print(f"[{idx}/{len(repos)}] {repo_full_name}")
            
            repo_name = repo_full_name.split('/')[-1]
            repo_path = cache_dir / repo_name
            if repo_path.exists():
                updated = update_bare_repository(repo_path)
                if updated:
                    cloned_repos.append(repo_path)
                else:
                    failed_repos.append(repo_full_name)
                continue

            repo_path = clone_bare_repository(repo_full_name, cache_dir)
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
            
            # Generate output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = args.output or f'github_analysis_{username}_{timestamp}.xlsx'
            
            # Run local_git_analytics.py
            analytics_script = script_dir / 'local_git_analytics.py'
            
            if not analytics_script.exists():
                print(f"Error: local_git_analytics.py not found at {analytics_script}")
                return 1
            
            # Run with session-based estimation
            python_exec = sys.executable
            cmd_parts = [
                python_exec,
                str(analytics_script),
                str(cache_dir),
                '--use-sessions',
                '--output', output_file
            ]

            if args.start_date:
                cmd_parts.extend(['--start-date', args.start_date])
            if args.end_date:
                cmd_parts.extend(['--end-date', args.end_date])
            if args.copilot_invokers:
                cmd_parts.extend(['--copilot-invokers', args.copilot_invokers])

            print(f"Running: {' '.join(cmd_parts)}")
            print()

            subprocess.run(cmd_parts, check=True)
            
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
