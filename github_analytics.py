#!/usr/bin/env python3
"""
GitHub Analytics Tool

Analyzes commit history, pull requests, issues, and file modifications
to calculate per-user statistics including commits, lines of code, and
estimated work hours. Generates daily breakdown spreadsheets.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Set
import pandas as pd
from github import Github, GithubException, RateLimitExceededException
from dotenv import load_dotenv


class GitHubAnalytics:
    """Analyzes GitHub repository activity and generates reports."""
    
    def __init__(self, token: str, username: str, enable_rate_limiting: bool = True):
        """
        Initialize GitHub Analytics.
        
        Args:
            token: GitHub Personal Access Token
            username: GitHub username whose repositories to analyze
            enable_rate_limiting: Enable automatic rate limit handling (default: True)
        """
        self.github = Github(token)
        self.username = username
        self.user = self.github.get_user(username)
        self.enable_rate_limiting = enable_rate_limiting
        self.api_calls_made = 0
        self.backoff_time = 1  # Initial backoff time in seconds
        
    def check_rate_limit(self):
        """
        Check GitHub API rate limit and wait if necessary.
        
        Implements exponential backoff when approaching rate limits.
        """
        if not self.enable_rate_limiting:
            return
            
        try:
            rate_limit = self.github.get_rate_limit()
            core = rate_limit.core
            
            # Log rate limit status every 100 calls
            if self.api_calls_made % 100 == 0:
                print(f"  [API] Rate limit: {core.remaining}/{core.limit} remaining, resets at {core.reset}")
            
            # If we're getting close to the limit (less than 100 calls remaining)
            if core.remaining < 100:
                wait_time = (core.reset - datetime.utcnow()).total_seconds() + 10
                if wait_time > 0:
                    print(f"  [API] Rate limit low ({core.remaining} remaining). Waiting {wait_time:.0f} seconds...")
                    time.sleep(wait_time)
                    self.backoff_time = 1  # Reset backoff
            # If we're getting low (less than 500 calls), slow down
            elif core.remaining < 500:
                time.sleep(self.backoff_time)
                self.backoff_time = min(self.backoff_time * 1.5, 10)  # Exponential backoff, max 10s
            else:
                self.backoff_time = 1  # Reset backoff when we have plenty of calls
                
            self.api_calls_made += 1
            
        except Exception as e:
            print(f"  [API] Warning: Could not check rate limit: {e}")
    
    def api_call_with_retry(self, func, max_retries: int = 3):
        """
        Execute an API call with retry logic and rate limiting.
        
        Args:
            func: Function to call
            max_retries: Maximum number of retries (default: 3)
            
        Returns:
            Result of the function call
        """
        self.check_rate_limit()
        
        for attempt in range(max_retries):
            try:
                return func()
            except RateLimitExceededException as e:
                if attempt < max_retries - 1:
                    wait_time = 60 * (attempt + 1)  # Wait 60s, 120s, 180s
                    print(f"  [API] Rate limit exceeded. Waiting {wait_time} seconds (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    raise
            except GithubException as e:
                if attempt < max_retries - 1 and e.status in [502, 503, 504]:  # Server errors
                    wait_time = 5 * (attempt + 1)
                    print(f"  [API] Server error {e.status}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
        
        return None
        
    def estimate_hours_from_commits(self, commits_count: int, lines_changed: int) -> float:
        """
        Estimate work hours based on commits and lines of code.
        
        Uses heuristic: ~10-50 lines per hour for code changes, 
        plus 0.5 hours per commit for planning/testing.
        
        Args:
            commits_count: Number of commits
            lines_changed: Total lines added + deleted
            
        Returns:
            Estimated hours of work
        """
        # Base time per commit (planning, testing, reviewing)
        base_hours = commits_count * 0.5
        
        # Time for coding (assuming 30 lines per hour average)
        coding_hours = lines_changed / 30.0
        
        return round(base_hours + coding_hours, 2)
    
    def analyze_commits(self, repo, start_date=None, end_date=None) -> Dict:
        """
        Analyze commits in a repository.
        
        Args:
            repo: GitHub repository object
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            
        Returns:
            Dictionary with per-user, per-day statistics
        """
        data = defaultdict(lambda: defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'total_changes': 0
        }))
        
        try:
            commits = self.api_call_with_retry(lambda: repo.get_commits())
            
            for commit in commits:
                self.check_rate_limit()  # Check before processing each commit
                try:
                    # Get commit date
                    commit_date = commit.commit.author.date
                    
                    # Filter by date range if specified
                    if start_date and commit_date < start_date:
                        continue
                    if end_date and commit_date > end_date:
                        continue
                    
                    # Get author information
                    author = commit.author.login if commit.author else commit.commit.author.name
                    date_key = commit_date.strftime('%Y-%m-%d')
                    
                    # Update statistics
                    data[author][date_key]['commits'] += 1
                    
                    # Get file statistics
                    try:
                        stats = commit.stats
                        data[author][date_key]['additions'] += stats.additions
                        data[author][date_key]['deletions'] += stats.deletions
                        data[author][date_key]['total_changes'] += stats.additions + stats.deletions
                    except Exception:
                        pass  # Some commits may not have stats
                        
                except Exception as e:
                    print(f"Warning: Error processing commit in {repo.name}: {e}")
                    continue
                    
        except GithubException as e:
            print(f"Warning: Error accessing commits for {repo.name}: {e}")
            
        return data
    
    def analyze_pull_requests(self, repo, start_date=None, end_date=None) -> Dict:
        """
        Analyze pull requests in a repository.
        
        Args:
            repo: GitHub repository object
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            
        Returns:
            Dictionary with per-user, per-day PR statistics
        """
        data = defaultdict(lambda: defaultdict(lambda: {
            'prs_created': 0,
            'prs_merged': 0,
            'pr_additions': 0,
            'pr_deletions': 0
        }))
        
        try:
            # Get all pull requests (open and closed)
            prs = self.api_call_with_retry(lambda: repo.get_pulls(state='all'))
            
            for pr in prs:
                self.check_rate_limit()  # Check before processing each PR
                try:
                    # Created date
                    created_date = pr.created_at
                    if start_date and created_date < start_date:
                        continue
                    if end_date and created_date > end_date:
                        continue
                    
                    author = pr.user.login if pr.user else "Unknown"
                    date_key = created_date.strftime('%Y-%m-%d')
                    
                    data[author][date_key]['prs_created'] += 1
                    
                    # Add line changes from PR
                    data[author][date_key]['pr_additions'] += pr.additions
                    data[author][date_key]['pr_deletions'] += pr.deletions
                    
                    # Check if merged
                    if pr.merged:
                        merged_date = pr.merged_at
                        if merged_date:
                            merged_key = merged_date.strftime('%Y-%m-%d')
                            data[author][merged_key]['prs_merged'] += 1
                            
                except Exception as e:
                    print(f"Warning: Error processing PR in {repo.name}: {e}")
                    continue
                    
        except GithubException as e:
            print(f"Warning: Error accessing PRs for {repo.name}: {e}")
            
        return data
    
    def analyze_issues(self, repo, start_date=None, end_date=None) -> Dict:
        """
        Analyze issues in a repository.
        
        Args:
            repo: GitHub repository object
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            
        Returns:
            Dictionary with per-user, per-day issue statistics
        """
        data = defaultdict(lambda: defaultdict(lambda: {
            'issues_created': 0,
            'issues_closed': 0,
            'issue_comments': 0
        }))
        
        try:
            issues = self.api_call_with_retry(lambda: repo.get_issues(state='all'))
            
            for issue in issues:
                self.check_rate_limit()  # Check before processing each issue
                try:
                    # Skip pull requests (they show up in issues too)
                    if issue.pull_request:
                        continue
                    
                    # Created date
                    created_date = issue.created_at
                    if start_date and created_date < start_date:
                        continue
                    if end_date and created_date > end_date:
                        continue
                    
                    author = issue.user.login if issue.user else "Unknown"
                    date_key = created_date.strftime('%Y-%m-%d')
                    
                    data[author][date_key]['issues_created'] += 1
                    
                    # Check if closed
                    if issue.closed_at:
                        closed_date = issue.closed_at
                        closed_key = closed_date.strftime('%Y-%m-%d')
                        data[author][closed_key]['issues_closed'] += 1
                    
                    # Count comments
                    try:
                        comments = issue.get_comments()
                        for comment in comments:
                            comment_date = comment.created_at
                            if start_date and comment_date < start_date:
                                continue
                            if end_date and comment_date > end_date:
                                continue
                            
                            commenter = comment.user.login if comment.user else "Unknown"
                            comment_key = comment_date.strftime('%Y-%m-%d')
                            data[commenter][comment_key]['issue_comments'] += 1
                    except Exception:
                        pass
                        
                except Exception as e:
                    print(f"Warning: Error processing issue in {repo.name}: {e}")
                    continue
                    
        except GithubException as e:
            print(f"Warning: Error accessing issues for {repo.name}: {e}")
            
        return data
    
    def analyze_file_modifications(self, repo, start_date=None, end_date=None) -> Dict:
        """
        Analyze file modification timestamps in a repository.
        
        This tracks file activity patterns through commit history, helping identify
        activity even when commits are sparse by counting unique files modified per day.
        
        Args:
            repo: GitHub repository object
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            
        Returns:
            Dictionary with per-user, per-day file modification statistics
        """
        data = defaultdict(lambda: defaultdict(lambda: {
            'files_modified': 0
        }))
        
        try:
            # Get all commits to track file modifications
            commits = self.api_call_with_retry(lambda: repo.get_commits())
            
            # Track files and their last modification per day
            file_modifications = {}
            
            for commit in commits:
                self.check_rate_limit()  # Check before processing each commit
                try:
                    commit_date = commit.commit.author.date
                    
                    # Filter by date range if specified
                    if start_date and commit_date < start_date:
                        continue
                    if end_date and commit_date > end_date:
                        continue
                    
                    author = commit.author.login if commit.author else commit.commit.author.name
                    date_key = commit_date.strftime('%Y-%m-%d')
                    
                    # Get files modified in this commit
                    try:
                        files = commit.files
                        for file in files:
                            file_path = file.filename
                            
                            # Track this file modification (unique per day)
                            file_key = f"{file_path}:{date_key}"
                            if file_key not in file_modifications:
                                data[author][date_key]['files_modified'] += 1
                                file_modifications[file_key] = True
                    except Exception:
                        pass  # Some commits may not have file details
                        
                except Exception as e:
                    continue
                    
        except GithubException as e:
            print(f"Warning: Error analyzing file modifications for {repo.name}: {e}")
            
        return data
    
    def merge_data(self, *data_dicts) -> Dict:
        """
        Merge multiple data dictionaries.
        
        Args:
            *data_dicts: Variable number of data dictionaries to merge
            
        Returns:
            Merged dictionary with combined statistics
        """
        merged = defaultdict(lambda: defaultdict(dict))
        
        for data in data_dicts:
            for user, dates in data.items():
                for date, stats in dates.items():
                    if date not in merged[user]:
                        merged[user][date] = {}
                    merged[user][date].update(stats)
        
        return merged
    
    def should_include_repository(self, repo, 
                                  include_repos: Optional[Set[str]] = None,
                                  exclude_repos: Optional[Set[str]] = None,
                                  filter_by_user_contribution: Optional[str] = None) -> bool:
        """
        Determine if a repository should be included in analysis.
        
        Args:
            repo: GitHub repository object
            include_repos: Set of repository names to include (None = include all)
            exclude_repos: Set of repository names to exclude (None = exclude none)
            filter_by_user_contribution: Only include repos where this user has contributed
            
        Returns:
            True if repository should be included, False otherwise
        """
        repo_name = repo.name
        
        # Check exclude list first
        if exclude_repos and repo_name in exclude_repos:
            return False
        
        # Check include list if specified
        if include_repos and repo_name not in include_repos:
            return False
        
        # Check user contribution filter
        if filter_by_user_contribution:
            try:
                # Check if user has any commits in this repo
                commits = self.api_call_with_retry(
                    lambda: repo.get_commits(author=filter_by_user_contribution).get_page(0)
                )
                if not commits:
                    return False
            except Exception as e:
                print(f"  [FILTER] Could not check contributions for {repo_name}: {e}")
                return False
        
        return True
    
    def analyze_all_repositories(self, 
                                 start_date=None, 
                                 end_date=None,
                                 include_repos: Optional[List[str]] = None,
                                 exclude_repos: Optional[List[str]] = None,
                                 filter_by_user_contribution: Optional[str] = None) -> pd.DataFrame:
        """
        Analyze all repositories for the user with filtering options.
        
        Args:
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            include_repos: List of repository names to include (None = include all)
            exclude_repos: List of repository names to exclude (None = exclude none)
            filter_by_user_contribution: Only include repos where this user has contributed
            
        Returns:
            Pandas DataFrame with comprehensive statistics
        """
        print(f"Fetching repositories for user: {self.username}")
        
        # Convert lists to sets for faster lookup
        include_set = set(include_repos) if include_repos else None
        exclude_set = set(exclude_repos) if exclude_repos else None
        
        if include_set:
            print(f"  Including only: {', '.join(sorted(include_set))}")
        if exclude_set:
            print(f"  Excluding: {', '.join(sorted(exclude_set))}")
        if filter_by_user_contribution:
            print(f"  Filtering by contributions from: {filter_by_user_contribution}")
        
        try:
            repos = self.api_call_with_retry(lambda: self.user.get_repos())
        except GithubException as e:
            print(f"Error fetching repositories: {e}")
            return pd.DataFrame()
        
        all_data = defaultdict(lambda: defaultdict(dict))
        
        repo_count = 0
        skipped_count = 0
        for repo in repos:
            # Check if repository should be included
            if not self.should_include_repository(repo, include_set, exclude_set, filter_by_user_contribution):
                skipped_count += 1
                print(f"Skipping repository: {repo.name}")
                continue
                
            repo_count += 1
            print(f"Analyzing repository {repo_count}: {repo.name}")
            
            # Analyze commits
            print(f"  - Analyzing commits...")
            commit_data = self.analyze_commits(repo, start_date, end_date)
            
            # Analyze pull requests
            print(f"  - Analyzing pull requests...")
            pr_data = self.analyze_pull_requests(repo, start_date, end_date)
            
            # Analyze issues
            print(f"  - Analyzing issues...")
            issue_data = self.analyze_issues(repo, start_date, end_date)
            
            # Analyze file modifications
            print(f"  - Analyzing file modifications...")
            file_mod_data = self.analyze_file_modifications(repo, start_date, end_date)
            
            # Merge data for this repository
            repo_data = self.merge_data(commit_data, pr_data, issue_data, file_mod_data)
            
            # Add to overall data
            for user, dates in repo_data.items():
                for date, stats in dates.items():
                    if date not in all_data[user]:
                        all_data[user][date] = defaultdict(int)
                    for key, value in stats.items():
                        all_data[user][date][key] += value
        
        print(f"Total repositories analyzed: {repo_count}")
        if skipped_count > 0:
            print(f"Total repositories skipped: {skipped_count}")
        
        # Convert to DataFrame
        rows = []
        for user, dates in all_data.items():
            for date, stats in dates.items():
                # Calculate total changes
                total_changes = stats.get('total_changes', 0) + \
                               stats.get('pr_additions', 0) + \
                               stats.get('pr_deletions', 0)
                
                # Estimate hours
                commits_count = stats.get('commits', 0)
                estimated_hours = self.estimate_hours_from_commits(commits_count, total_changes)
                
                row = {
                    'date': date,
                    'user': user,
                    'commits': stats.get('commits', 0),
                    'lines_added': stats.get('additions', 0) + stats.get('pr_additions', 0),
                    'lines_deleted': stats.get('deletions', 0) + stats.get('pr_deletions', 0),
                    'total_lines_changed': total_changes,
                    'files_modified': stats.get('files_modified', 0),
                    'prs_created': stats.get('prs_created', 0),
                    'prs_merged': stats.get('prs_merged', 0),
                    'issues_created': stats.get('issues_created', 0),
                    'issues_closed': stats.get('issues_closed', 0),
                    'issue_comments': stats.get('issue_comments', 0),
                    'estimated_hours': estimated_hours
                }
                rows.append(row)
        
        # Create DataFrame and sort
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['date', 'user'], ascending=[False, True])
        
        return df
    
    def generate_report(self, 
                       output_file: str = None, 
                       start_date=None, 
                       end_date=None,
                       include_repos: Optional[List[str]] = None,
                       exclude_repos: Optional[List[str]] = None,
                       filter_by_user_contribution: Optional[str] = None):
        """
        Generate a comprehensive report and save to Excel.
        
        Args:
            output_file: Output file path (defaults to github_analytics_{timestamp}.xlsx)
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            include_repos: List of repository names to include (None = include all)
            exclude_repos: List of repository names to exclude (None = exclude none)
            filter_by_user_contribution: Only include repos where this user has contributed
        """
        # Analyze all repositories
        df = self.analyze_all_repositories(
            start_date, 
            end_date, 
            include_repos, 
            exclude_repos, 
            filter_by_user_contribution
        )
        
        if df.empty:
            print("No data found to generate report.")
            return
        
        # Generate default filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'github_analytics_{timestamp}.xlsx'
        
        # Create Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main detailed report
            df.to_excel(writer, sheet_name='Detailed Report', index=False)
            
            # Summary by user
            user_summary = df.groupby('user').agg({
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
                'estimated_hours': 'sum'
            }).reset_index()
            user_summary = user_summary.sort_values('estimated_hours', ascending=False)
            user_summary.to_excel(writer, sheet_name='User Summary', index=False)
            
            # Summary by date
            date_summary = df.groupby('date').agg({
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
                'user': 'count'  # Number of active users per day
            }).reset_index()
            date_summary.rename(columns={'user': 'active_users'}, inplace=True)
            date_summary = date_summary.sort_values('date', ascending=False)
            date_summary.to_excel(writer, sheet_name='Daily Summary', index=False)
        
        print(f"\nReport generated successfully: {output_file}")
        print(f"Total users: {df['user'].nunique()}")
        print(f"Total commits: {df['commits'].sum()}")
        print(f"Total lines changed: {df['total_lines_changed'].sum()}")
        print(f"Total estimated hours: {df['estimated_hours'].sum():.2f}")


def main():
    """Main entry point for the GitHub Analytics tool."""
    # Load environment variables
    load_dotenv()
    
    # Get configuration
    token = os.getenv('GITHUB_TOKEN')
    username = os.getenv('GITHUB_USERNAME')
    
    if not token:
        print("Error: GITHUB_TOKEN not found in environment variables.")
        print("Please create a .env file with your GitHub token.")
        print("See .env.example for reference.")
        sys.exit(1)
    
    if not username:
        print("Error: GITHUB_USERNAME not found in environment variables.")
        print("Please create a .env file with your GitHub username.")
        print("See .env.example for reference.")
        sys.exit(1)
    
    # Optional: Parse command line arguments
    start_date = None
    end_date = None
    output_file = None
    include_repos = None
    exclude_repos = None
    filter_by_user = None
    disable_rate_limiting = False
    
    if len(sys.argv) > 1:
        # Simple command line parsing
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == '--start-date' and i + 1 < len(sys.argv):
                start_date = datetime.strptime(sys.argv[i + 1], '%Y-%m-%d')
                i += 2
            elif sys.argv[i] == '--end-date' and i + 1 < len(sys.argv):
                end_date = datetime.strptime(sys.argv[i + 1], '%Y-%m-%d')
                i += 2
            elif sys.argv[i] == '--output' and i + 1 < len(sys.argv):
                output_file = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == '--include-repos' and i + 1 < len(sys.argv):
                include_repos = sys.argv[i + 1].split(',')
                i += 2
            elif sys.argv[i] == '--exclude-repos' and i + 1 < len(sys.argv):
                exclude_repos = sys.argv[i + 1].split(',')
                i += 2
            elif sys.argv[i] == '--filter-by-user' and i + 1 < len(sys.argv):
                filter_by_user = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == '--disable-rate-limiting':
                disable_rate_limiting = True
                i += 1
            elif sys.argv[i] in ['--help', '-h']:
                print("Usage: python github_analytics.py [OPTIONS]")
                print("\nOptions:")
                print("  --start-date YYYY-MM-DD        Start date for analysis")
                print("  --end-date YYYY-MM-DD          End date for analysis")
                print("  --output FILE                  Output file path")
                print("  --include-repos REPO1,REPO2    Only analyze these repositories")
                print("  --exclude-repos REPO1,REPO2    Exclude these repositories")
                print("  --filter-by-user USERNAME      Only include repos with contributions from user")
                print("  --disable-rate-limiting        Disable automatic rate limit handling")
                print("  --help, -h                     Show this help message")
                sys.exit(0)
            else:
                i += 1
    
    print("=" * 60)
    print("GitHub Analytics Tool")
    print("=" * 60)
    print(f"Username: {username}")
    if start_date:
        print(f"Start Date: {start_date.strftime('%Y-%m-%d')}")
    if end_date:
        print(f"End Date: {end_date.strftime('%Y-%m-%d')}")
    if include_repos:
        print(f"Including repos: {', '.join(include_repos)}")
    if exclude_repos:
        print(f"Excluding repos: {', '.join(exclude_repos)}")
    if filter_by_user:
        print(f"Filtering by user: {filter_by_user}")
    print(f"Rate limiting: {'Disabled' if disable_rate_limiting else 'Enabled'}")
    print("=" * 60)
    print()
    
    # Create analytics instance
    analytics = GitHubAnalytics(token, username, enable_rate_limiting=not disable_rate_limiting)
    
    # Generate report
    analytics.generate_report(
        output_file, 
        start_date, 
        end_date,
        include_repos,
        exclude_repos,
        filter_by_user
    )


if __name__ == '__main__':
    main()
