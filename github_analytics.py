#!/usr/bin/env python3
"""
GitHub Analytics Tool

Analyzes commit history, pull requests, issues, and file modifications
to calculate per-user statistics including commits, lines of code, and
estimated work hours. Generates daily breakdown spreadsheets.
"""

import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
import pandas as pd
from github import Github, GithubException
from dotenv import load_dotenv


class GitHubAnalytics:
    """Analyzes GitHub repository activity and generates reports."""
    
    def __init__(self, token: str, username: str):
        """
        Initialize GitHub Analytics.
        
        Args:
            token: GitHub Personal Access Token
            username: GitHub username whose repositories to analyze
        """
        self.github = Github(token)
        self.username = username
        self.user = self.github.get_user(username)
        
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
            commits = repo.get_commits()
            
            for commit in commits:
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
            prs = repo.get_pulls(state='all')
            
            for pr in prs:
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
            issues = repo.get_issues(state='all')
            
            for issue in issues:
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
    
    def analyze_all_repositories(self, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Analyze all repositories for the user.
        
        Args:
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            
        Returns:
            Pandas DataFrame with comprehensive statistics
        """
        print(f"Fetching repositories for user: {self.username}")
        
        try:
            repos = self.user.get_repos()
        except GithubException as e:
            print(f"Error fetching repositories: {e}")
            return pd.DataFrame()
        
        all_data = defaultdict(lambda: defaultdict(dict))
        
        repo_count = 0
        for repo in repos:
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
            
            # Merge data for this repository
            repo_data = self.merge_data(commit_data, pr_data, issue_data)
            
            # Add to overall data
            for user, dates in repo_data.items():
                for date, stats in dates.items():
                    if date not in all_data[user]:
                        all_data[user][date] = defaultdict(int)
                    for key, value in stats.items():
                        all_data[user][date][key] += value
        
        print(f"Total repositories analyzed: {repo_count}")
        
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
    
    def generate_report(self, output_file: str = None, start_date=None, end_date=None):
        """
        Generate a comprehensive report and save to Excel.
        
        Args:
            output_file: Output file path (defaults to github_analytics_{timestamp}.xlsx)
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
        """
        # Analyze all repositories
        df = self.analyze_all_repositories(start_date, end_date)
        
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
    
    # Optional: Parse command line arguments for date range
    start_date = None
    end_date = None
    output_file = None
    
    if len(sys.argv) > 1:
        # Simple command line parsing
        for i, arg in enumerate(sys.argv[1:]):
            if arg == '--start-date' and i + 2 < len(sys.argv):
                start_date = datetime.strptime(sys.argv[i + 2], '%Y-%m-%d')
            elif arg == '--end-date' and i + 2 < len(sys.argv):
                end_date = datetime.strptime(sys.argv[i + 2], '%Y-%m-%d')
            elif arg == '--output' and i + 2 < len(sys.argv):
                output_file = sys.argv[i + 2]
    
    print("=" * 60)
    print("GitHub Analytics Tool")
    print("=" * 60)
    print(f"Username: {username}")
    if start_date:
        print(f"Start Date: {start_date.strftime('%Y-%m-%d')}")
    if end_date:
        print(f"End Date: {end_date.strftime('%Y-%m-%d')}")
    print("=" * 60)
    print()
    
    # Create analytics instance
    analytics = GitHubAnalytics(token, username)
    
    # Generate report
    analytics.generate_report(output_file, start_date, end_date)


if __name__ == '__main__':
    main()
