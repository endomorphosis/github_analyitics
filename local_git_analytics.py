#!/usr/bin/env python3
"""
Local Git Analytics Tool

Analyzes local git repositories to track commits and estimated work hours
by directly reading git history without using the GitHub API.
"""

import os
import sys
import json
import csv
import subprocess
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd


class LocalGitAnalytics:
    """Analyzes local git repositories without using the GitHub API."""
    
    def __init__(self, base_path: str, copilot_invokers_path: Optional[str] = None):
        """
        Initialize Local Git Analytics.
        
        Args:
            base_path: Base directory to search for git repositories
            copilot_invokers_path: Optional mapping file for Copilot invokers
        """
        self.base_path = Path(base_path).resolve()
        self.file_events: List[Dict] = []
        self.commit_events: List[Dict] = []
        self.copilot_invokers = self.load_copilot_invokers(copilot_invokers_path)

    @staticmethod
    def normalize_identity(value: Optional[str]) -> str:
        return (value or '').strip().lower()

    @staticmethod
    def is_copilot_identity(name: str, email: str) -> bool:
        identity = f"{name} {email}".lower()
        return 'copilot' in identity

    @staticmethod
    def has_copilot_trailer(message: str) -> bool:
        if not message:
            return False
        for line in message.splitlines():
            if line.lower().startswith('co-authored-by:') and 'copilot' in line.lower():
                return True
        return False

    def resolve_invoker_details(
        self,
        author: str,
        email: str,
        copilot_involved: bool,
        invoker_override: Optional[str] = None,
        copilot_identity: Optional[str] = None,
    ) -> Tuple[str, str]:
        if not copilot_involved:
            return author, 'author'

        if invoker_override:
            return invoker_override, 'co-author'

        if copilot_identity:
            identity_key = self.normalize_identity(copilot_identity)
            mapped = self.copilot_invokers.get(identity_key)
            if mapped:
                return mapped, 'mapping'

        author_key = self.normalize_identity(author)
        email_key = self.normalize_identity(email)

        return (
            self.copilot_invokers.get(author_key)
            or self.copilot_invokers.get(email_key)
            or author,
            'author'
        )

    @staticmethod
    def parse_co_authors(message: str) -> List[Tuple[str, str]]:
        co_authors = []
        if not message:
            return co_authors
        for line in message.splitlines():
            if not line.lower().startswith('co-authored-by:'):
                continue
            _, value = line.split(':', 1)
            value = value.strip()
            if '<' in value and value.endswith('>'):
                name, email = value.rsplit('<', 1)
                co_authors.append((name.strip(), email[:-1].strip()))
            else:
                co_authors.append((value, ''))
        return co_authors

    @staticmethod
    def load_copilot_invokers(path: Optional[str]) -> Dict[str, str]:
        if not path:
            return {}

        invokers: Dict[str, str] = {}
        mapping_path = Path(path)
        if not mapping_path.exists():
            print(f"Warning: Copilot invoker mapping file not found: {mapping_path}")
            return {}

        if mapping_path.suffix.lower() == '.json':
            with mapping_path.open('r', encoding='utf-8') as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                for key, value in data.items():
                    if key and value:
                        invokers[str(key).strip().lower()] = str(value).strip()
        elif mapping_path.suffix.lower() == '.csv':
            with mapping_path.open('r', encoding='utf-8') as handle:
                reader = csv.reader(handle)
                for row in reader:
                    if len(row) < 2:
                        continue
                    key = row[0].strip()
                    value = row[1].strip()
                    if not key or not value or key.lower() == 'copilot_id':
                        continue
                    invokers[key.lower()] = value
        else:
            print(f"Warning: Unsupported copilot invoker mapping format: {mapping_path.suffix}")

        return invokers
        
    @staticmethod
    def estimate_hours_from_commits(commits_count: int, lines_changed: int) -> float:
        """
        Estimate work hours based on commits and lines of code.
        
        Uses heuristic: ~30 lines per hour for code changes, 
        plus 0.5 hours per commit for planning/testing.
        
        Args:
            commits_count: Number of commits
            lines_changed: Total lines added + deleted
            
        Returns:
            Estimated hours of work
        """
        base_hours = commits_count * 0.5
        coding_hours = lines_changed / 30.0
        return round(base_hours + coding_hours, 2)
    
    def find_git_repositories(self, max_depth: int = 5) -> List[Path]:
        """
        Find all git repositories under the base path.
        
        Args:
            max_depth: Maximum directory depth to search
            
        Returns:
            List of paths to git repositories
        """
        repos = []

        def is_bare_git_repo(path: Path) -> bool:
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
        
        def search_dir(path: Path, depth: int):
            if depth > max_depth:
                return
            
            try:
                # Check if this directory is a git repo
                git_dir = path / '.git'
                if git_dir.exists() and git_dir.is_dir():
                    repos.append(path)
                    return  # Don't search inside git repos

                # Check if this directory is a bare git repo
                if is_bare_git_repo(path):
                    repos.append(path)
                    return  # Don't search inside git repos
                
                # Search subdirectories
                if path.is_dir():
                    for item in path.iterdir():
                        if item.is_dir() and not item.name.startswith('.'):
                            search_dir(item, depth + 1)
            except (PermissionError, OSError):
                pass  # Skip directories we can't access
        
        print(f"Scanning for git repositories in: {self.base_path}")
        search_dir(self.base_path, 0)
        print(f"Found {len(repos)} git repositories")
        
        return repos
    
    def get_git_log(self, repo_path: Path, start_date: Optional[datetime] = None, 
                    end_date: Optional[datetime] = None,
                    copilot_info: Optional[Dict[str, Dict]] = None) -> List[Dict]:
        """
        Get git log data from a repository.
        
        Args:
            repo_path: Path to the git repository
            start_date: Start date for filtering commits
            end_date: End date for filtering commits
            
        Returns:
            List of commit data dictionaries
        """
        # Build git log command with custom format
        # Format: commit_hash|author_name|author_email|date_iso|subject
        cmd = [
            'git', 'log',
            '--all',  # All branches
            '--pretty=format:%H|%an|%ae|%aI|%s',
            '--numstat',  # Show file statistics
        ]
        
        # Add date filters if specified
        if start_date:
            cmd.append(f'--since={start_date.isoformat()}')
        if end_date:
            cmd.append(f'--until={end_date.isoformat()}')
        
        try:
            result = subprocess.run(
                cmd,
                cwd=repo_path,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                check=True
            )
            
            return self.parse_git_log(result.stdout, copilot_info)
            
        except subprocess.CalledProcessError as e:
            print(f"Warning: Error reading git log from {repo_path.name}: {e}")
            return []
    
    def parse_git_log(self, log_output: str, copilot_info: Optional[Dict[str, Dict]] = None) -> List[Dict]:
        """
        Parse git log output with numstat.
        
        Args:
            log_output: Raw git log output
            
        Returns:
            List of parsed commit dictionaries
        """
        commits = []
        lines = log_output.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if not line:
                i += 1
                continue
            
            # Check if this is a commit line (contains |)
            if '|' in line and not '\t' in line:
                parts = line.split('|')
                if len(parts) >= 5:
                    commit_hash = parts[0]
                    author_name = parts[1]
                    author_email = parts[2]
                    date_str = parts[3]
                    subject = '|'.join(parts[4:])  # Rejoin in case subject had |
                    
                    # Parse date
                    try:
                        commit_date = datetime.fromisoformat(date_str)
                    except ValueError:
                        i += 1
                        continue
                    
                    copilot_involved = False
                    invoker_override = None
                    copilot_identity = None
                    if copilot_info is not None and commit_hash in copilot_info:
                        info = copilot_info[commit_hash]
                        copilot_involved = info.get('copilot_involved', False)
                        invoker_override = info.get('invoker_override')
                        copilot_identity = info.get('copilot_identity')
                    if self.is_copilot_identity(author_name, author_email):
                        copilot_involved = True

                    # Initialize commit data
                    commit_data = {
                        'hash': commit_hash,
                        'author': author_name,
                        'email': author_email,
                        'date': commit_date,
                        'subject': subject,
                        'additions': 0,
                        'deletions': 0,
                        'files_changed': 0,
                        'copilot_involved': copilot_involved,
                        'invoker_override': invoker_override,
                        'copilot_identity': copilot_identity
                    }
                    
                    # Parse numstat lines (format: additions\tdeletions\tfilename)
                    i += 1
                    while i < len(lines):
                        stat_line = lines[i]
                        if not stat_line or not '\t' in stat_line:
                            break
                        
                        parts = stat_line.split('\t')
                        if len(parts) >= 3:
                            try:
                                adds = int(parts[0]) if parts[0] != '-' else 0
                                dels = int(parts[1]) if parts[1] != '-' else 0
                                commit_data['additions'] += adds
                                commit_data['deletions'] += dels
                                commit_data['files_changed'] += 1
                            except ValueError:
                                pass
                        i += 1
                    
                    commits.append(commit_data)
                    continue
            
            i += 1
        
        return commits
    
    def get_file_modifications(self, repo_path: Path, start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None,
                               copilot_info: Optional[Dict[str, Dict]] = None) -> List[Dict]:
        """
        Get detailed file modification data from repository commits.
        
        Args:
            repo_path: Path to the git repository
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            List of file modification events with timestamps
        """
        cmd = [
            'git', 'log',
            '--all',
            '--pretty=format:%H|%an|%ae|%aI',
            '--name-status',  # Show file status (M=modified, A=added, D=deleted)
        ]
        
        if start_date:
            cmd.append(f'--since={start_date.isoformat()}')
        if end_date:
            cmd.append(f'--until={end_date.isoformat()}')
        
        try:
            result = subprocess.run(
                cmd,
                cwd=repo_path,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                check=True
            )
            
            modifications = []
            lines = result.stdout.split('\n')
            
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                
                if not line:
                    i += 1
                    continue
                
                # Check for commit line
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 4:
                        commit_hash = parts[0]
                        author = parts[1]
                        email = parts[2]
                        date_str = parts[3]
                        
                        try:
                            commit_date = datetime.fromisoformat(date_str)
                        except ValueError:
                            i += 1
                            continue
                        
                        copilot_involved = False
                        invoker_override = None
                        copilot_identity = None
                        if copilot_info is not None and commit_hash in copilot_info:
                            info = copilot_info[commit_hash]
                            copilot_involved = info.get('copilot_involved', False)
                            invoker_override = info.get('invoker_override')
                            copilot_identity = info.get('copilot_identity')
                        if self.is_copilot_identity(author, email):
                            copilot_involved = True

                        # Parse file status lines
                        i += 1
                        while i < len(lines):
                            file_line = lines[i].strip()
                            if not file_line or '|' in file_line:
                                break
                            
                            # Format: M\tfilename or A\tfilename or D\tfilename
                            if '\t' in file_line:
                                parts = file_line.split('\t')
                                if len(parts) >= 2:
                                    status = parts[0]
                                    filename = parts[1]
                                    
                                    modifications.append({
                                        'commit': commit_hash,
                                        'author': author,
                                        'email': email,
                                        'date': commit_date,
                                        'status': status,
                                        'file': filename,
                                        'copilot_involved': copilot_involved,
                                        'invoker_override': invoker_override,
                                        'copilot_identity': copilot_identity
                                    })
                            
                            i += 1
                        continue
                
                i += 1
            
            return modifications
            
        except subprocess.CalledProcessError as e:
            print(f"Warning: Error reading file modifications from {repo_path.name}: {e}")
            return []

    def get_copilot_commit_info(self, repo_path: Path,
                                start_date: Optional[datetime] = None,
                                end_date: Optional[datetime] = None) -> Dict[str, Dict]:
        cmd = [
            'git', 'log',
            '--all',
            '--pretty=format:%H%x1f%an%x1f%ae%x1f%B%x1e',
        ]

        if start_date:
            cmd.append(f'--since={start_date.isoformat()}')
        if end_date:
            cmd.append(f'--until={end_date.isoformat()}')

        try:
            result = subprocess.run(
                cmd,
                cwd=repo_path,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Warning: Error reading copilot markers from {repo_path.name}: {e}")
            return {}

        copilot_commits: Dict[str, Dict] = {}
        records = result.stdout.split('\x1e')
        for record in records:
            if not record.strip():
                continue
            parts = record.split('\x1f')
            if len(parts) < 4:
                continue
            commit_hash = parts[0].strip()
            author_name = parts[1].strip()
            author_email = parts[2].strip()
            message_body = parts[3]

            co_authors = self.parse_co_authors(message_body)
            copilot_involved = self.is_copilot_identity(author_name, author_email)
            copilot_identity = author_email or author_name
            invoker_override = None

            for name, email in co_authors:
                if self.is_copilot_identity(name, email):
                    copilot_involved = True
                    copilot_identity = email or name
                elif not invoker_override:
                    invoker_override = name

            if self.has_copilot_trailer(message_body):
                copilot_involved = True

            if copilot_involved:
                copilot_commits[commit_hash] = {
                    'copilot_involved': copilot_involved,
                    'invoker_override': invoker_override,
                    'copilot_identity': copilot_identity
                }

        return copilot_commits
    
    def estimate_hours_from_sessions(self, commits: List[Dict]) -> float:
        """
        Estimate work hours based on commit session clustering.
        
        Groups commits that are close together (within 2 hours) as the same work session.
        
        Args:
            commits: List of commit dictionaries with 'date' field
            
        Returns:
            Estimated hours based on work sessions
        """
        if not commits:
            return 0.0
        
        # Sort commits by date
        sorted_commits = sorted(commits, key=lambda c: c['date'])
        
        # Cluster commits into sessions (commits within 2 hours are same session)
        session_gap_hours = 2
        sessions = []
        current_session_start = sorted_commits[0]['date']
        current_session_end = sorted_commits[0]['date']
        
        for commit in sorted_commits[1:]:
            time_diff = (commit['date'] - current_session_end).total_seconds() / 3600
            
            if time_diff <= session_gap_hours:
                # Same session - extend end time
                current_session_end = commit['date']
            else:
                # New session
                session_duration = (current_session_end - current_session_start).total_seconds() / 3600
                # Minimum 0.5 hours per session, maximum 8 hours
                sessions.append(min(max(session_duration + 0.5, 0.5), 8.0))
                
                current_session_start = commit['date']
                current_session_end = commit['date']
        
        # Add final session
        session_duration = (current_session_end - current_session_start).total_seconds() / 3600
        sessions.append(min(max(session_duration + 0.5, 0.5), 8.0))
        
        return round(sum(sessions), 2)
    
    def analyze_repository(self, repo_path: Path, start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None, 
                          use_session_estimation: bool = False) -> Dict:
        """
        Analyze a single git repository.
        
        Args:
            repo_path: Path to the git repository
            start_date: Start date for filtering
            end_date: End date for filtering
            use_session_estimation: Use session-based hour estimation instead of simple formula
            
        Returns:
            Dictionary with per-user, per-day statistics including file modifications
        """
        data = defaultdict(lambda: defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'total_changes': 0,
            'files_modified': set(),
            'commit_times': []
        }))
        
        copilot_info = self.get_copilot_commit_info(repo_path, start_date, end_date)
        commits = self.get_git_log(repo_path, start_date, end_date, copilot_info)
        modifications = self.get_file_modifications(repo_path, start_date, end_date, copilot_info)
        
        # Track commits
        for commit in commits:
            author = commit['author']
            email = commit['email']
            date_key = commit['date'].strftime('%Y-%m-%d')
            copilot_involved = commit.get('copilot_involved', False)
            invoker_override = commit.get('invoker_override')
            copilot_identity = commit.get('copilot_identity')
            attributed_user, invoker_source = self.resolve_invoker_details(
                author,
                email,
                copilot_involved,
                invoker_override,
                copilot_identity
            )
            
            data[attributed_user][date_key]['commits'] += 1
            data[attributed_user][date_key]['additions'] += commit['additions']
            data[attributed_user][date_key]['deletions'] += commit['deletions']
            data[attributed_user][date_key]['total_changes'] += commit['additions'] + commit['deletions']
            data[attributed_user][date_key]['commit_times'].append({'date': commit['date']})

            if self.commit_events is not None:
                self.commit_events.append({
                    'repository': repo_path.name,
                    'author': author,
                    'attributed_user': attributed_user,
                    'copilot_involved': copilot_involved,
                    'invoker_source': invoker_source,
                    'email': email,
                    'event_timestamp': commit['date'].isoformat(),
                    'commit': commit['hash'],
                    'subject': commit['subject']
                })
        
        # Track file modifications
        for mod in modifications:
            author = mod['author']
            email = mod['email']
            date_key = mod['date'].strftime('%Y-%m-%d')
            copilot_involved = mod.get('copilot_involved', False)
            invoker_override = mod.get('invoker_override')
            copilot_identity = mod.get('copilot_identity')
            attributed_user, invoker_source = self.resolve_invoker_details(
                author,
                email,
                copilot_involved,
                invoker_override,
                copilot_identity
            )
            
            # Track unique files modified
            data[attributed_user][date_key]['files_modified'].add(mod['file'])

            if self.file_events is not None:
                self.file_events.append({
                    'repository': repo_path.name,
                    'author': author,
                    'attributed_user': attributed_user,
                    'copilot_involved': copilot_involved,
                    'invoker_source': invoker_source,
                    'email': email,
                    'event_timestamp': mod['date'].isoformat(),
                    'status': mod['status'],
                    'file': mod['file']
                })
        
        # Convert sets to counts and calculate session-based hours if requested
        for author in data:
            for date in data[author]:
                data[author][date]['files_modified'] = len(data[author][date]['files_modified'])
                
                if use_session_estimation:
                    # Use session-based estimation
                    data[author][date]['session_hours'] = self.estimate_hours_from_sessions(
                        data[author][date]['commit_times']
                    )
                
                # Remove commit_times as it's no longer needed
                del data[author][date]['commit_times']
        
        return data
    
    def merge_data(self, *data_dicts) -> Dict:
        """
        Merge multiple data dictionaries.
        
        Args:
            *data_dicts: Variable number of data dictionaries to merge
            
        Returns:
            Merged dictionary with combined statistics
        """
        merged = defaultdict(lambda: defaultdict(lambda: {
            'commits': 0,
            'additions': 0,
            'deletions': 0,
            'total_changes': 0,
            'files_modified': 0
        }))
        
        for data in data_dicts:
            for user, dates in data.items():
                for date, stats in dates.items():
                    merged[user][date]['commits'] += stats.get('commits', 0)
                    merged[user][date]['additions'] += stats.get('additions', 0)
                    merged[user][date]['deletions'] += stats.get('deletions', 0)
                    merged[user][date]['total_changes'] += stats.get('total_changes', 0)
                    merged[user][date]['files_modified'] += stats.get('files_modified', 0)
        
        return merged
    
    def analyze_all_repositories(self, 
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None,
                                 include_repos: Optional[List[str]] = None,
                                 exclude_repos: Optional[List[str]] = None,
                                 max_depth: int = 5,
                                 use_session_estimation: bool = False) -> pd.DataFrame:
        """
        Analyze all git repositories found under the base path.
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            include_repos: List of repository names to include
            exclude_repos: List of repository names to exclude
            max_depth: Maximum directory depth to search
            use_session_estimation: Use session clustering for hour estimation
            
        Returns:
            Pandas DataFrame with comprehensive statistics
        """
        # Find all repositories
        self.file_events = []
        self.commit_events = []
        repos = self.find_git_repositories(max_depth)
        
        # Filter repositories
        include_set = set(include_repos) if include_repos else None
        exclude_set = set(exclude_repos) if exclude_repos else None
        
        filtered_repos = []
        for repo in repos:
            repo_name = repo.name
            
            if exclude_set and repo_name in exclude_set:
                print(f"Skipping excluded repository: {repo_name}")
                continue
            
            if include_set and repo_name not in include_set:
                continue
            
            filtered_repos.append(repo)
        
        print(f"Analyzing {len(filtered_repos)} repositories...")
        
        # Analyze each repository
        all_data = defaultdict(lambda: defaultdict(dict))
        
        for idx, repo in enumerate(filtered_repos, 1):
            print(f"[{idx}/{len(filtered_repos)}] Analyzing: {repo.name}")
            
            try:
                repo_data = self.analyze_repository(repo, start_date, end_date, use_session_estimation)
                
                # Merge into all_data
                for user, dates in repo_data.items():
                    for date, stats in dates.items():
                        if date not in all_data[user]:
                            all_data[user][date] = defaultdict(int)
                        for key, value in stats.items():
                            all_data[user][date][key] += value
                
            except Exception as e:
                print(f"  Error: {e}")
                continue
        
        # Convert to DataFrame
        rows = []
        for user, dates in all_data.items():
            for date, stats in dates.items():
                total_changes = stats.get('total_changes', 0)
                commits_count = stats.get('commits', 0)
                
                # Use session-based hours if available, otherwise use simple formula
                if use_session_estimation and 'session_hours' in stats:
                    estimated_hours = stats.get('session_hours', 0)
                else:
                    estimated_hours = self.estimate_hours_from_commits(commits_count, total_changes)
                
                row = {
                    'date': date,
                    'user': user,
                    'commits': commits_count,
                    'lines_added': stats.get('additions', 0),
                    'lines_deleted': stats.get('deletions', 0),
                    'total_lines_changed': total_changes,
                    'files_modified': stats.get('files_modified', 0),
                    'estimated_hours': estimated_hours
                }
                rows.append(row)
        
        # Create DataFrame and sort
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['date', 'user'], ascending=[False, True])
        
        return df
    
    def generate_report(self,
                       output_file: Optional[str] = None,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       include_repos: Optional[List[str]] = None,
                       exclude_repos: Optional[List[str]] = None,
                       max_depth: int = 5,
                       use_session_estimation: bool = False):
        """
        Generate a comprehensive report and save to Excel.
        
        Args:
            output_file: Output file path (defaults to local_git_analytics_{timestamp}.xlsx)
            start_date: Start date for analysis
            end_date: End date for analysis
            include_repos: List of repository names to include
            exclude_repos: List of repository names to exclude
            max_depth: Maximum directory depth to search
            use_session_estimation: Use session clustering for hour estimation
        """
        # Analyze all repositories
        df = self.analyze_all_repositories(
            start_date,
            end_date,
            include_repos,
            exclude_repos,
            max_depth,
            use_session_estimation
        )
        
        if df.empty:
            print("No data found to generate report.")
            return
        
        # Generate default filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'local_git_analytics_{timestamp}.xlsx'
        
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
                'estimated_hours': 'sum',
                'user': 'count'
            }).reset_index()
            date_summary.rename(columns={'user': 'active_users'}, inplace=True)
            date_summary = date_summary.sort_values('date', ascending=False)
            date_summary.to_excel(writer, sheet_name='Daily Summary', index=False)

            if self.file_events:
                file_events_df = pd.DataFrame(self.file_events)
                file_events_df = file_events_df.sort_values('event_timestamp', ascending=False)
                file_events_df.to_excel(writer, sheet_name='File Events', index=False)

            if self.commit_events:
                commit_events_df = pd.DataFrame(self.commit_events)
                commit_events_df = commit_events_df.sort_values('event_timestamp', ascending=False)
                commit_events_df.to_excel(writer, sheet_name='Commit Events', index=False)

            if self.commit_events or self.file_events:
                combined_events: List[Dict] = []
                if self.commit_events:
                    for event in self.commit_events:
                        combined_events.append({
                            'event_type': 'commit',
                            **event
                        })
                if self.file_events:
                    for event in self.file_events:
                        combined_events.append({
                            'event_type': 'file',
                            **event
                        })
                timeline_df = pd.DataFrame(combined_events)
                timeline_df = timeline_df.sort_values('event_timestamp', ascending=False)
                timeline_df.to_excel(writer, sheet_name='User Timeline', index=False)
        
        print(f"\n{'='*60}")
        print(f"Report generated successfully: {output_file}")
        print(f"{'='*60}")
        print(f"Total users: {df['user'].nunique()}")
        print(f"Total commits: {df['commits'].sum()}")
        print(f"Total lines changed: {df['total_lines_changed'].sum()}")
        print(f"Total estimated hours: {df['estimated_hours'].sum():.2f}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")


def main():
    """Main entry point for the Local Git Analytics tool."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyze local git repositories to track commits and estimated work hours.'
    )
    parser.add_argument(
        'base_path',
        nargs='?',
        default='.',
        help='Base directory to search for git repositories (default: current directory)'
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
        help='Output file path (default: local_git_analytics_{timestamp}.xlsx)'
    )
    parser.add_argument(
        '--include-repos',
        type=str,
        help='Comma-separated list of repository names to include'
    )
    parser.add_argument(
        '--exclude-repos',
        type=str,
        help='Comma-separated list of repository names to exclude'
    )
    parser.add_argument(
        '--max-depth',
        type=int,
        default=5,
        help='Maximum directory depth to search for repositories (default: 5)'
    )
    parser.add_argument(
        '--use-sessions',
        action='store_true',
        help='Use session-based hour estimation (groups commits by time)'
    )
    parser.add_argument(
        '--copilot-invokers',
        type=str,
        help='Path to JSON or CSV mapping Copilot identities to invoker usernames'
    )
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    # Parse repository filters
    include_repos = None
    exclude_repos = None
    if args.include_repos:
        include_repos = [r.strip() for r in args.include_repos.split(',')]
    if args.exclude_repos:
        exclude_repos = [r.strip() for r in args.exclude_repos.split(',')]
    
    print("=" * 60)
    print("Local Git Analytics Tool")
    print("=" * 60)
    print(f"Base path: {args.base_path}")
    if start_date:
        print(f"Start date: {start_date.strftime('%Y-%m-%d')}")
    if end_date:
        print(f"End date: {end_date.strftime('%Y-%m-%d')}")
    if include_repos:
        print(f"Including repos: {', '.join(include_repos)}")
    if exclude_repos:
        print(f"Excluding repos: {', '.join(exclude_repos)}")
    print(f"Max search depth: {args.max_depth}")
    if args.use_sessions:
        print(f"Hour estimation: Session-based (clusters commits by time)")
    else:
        print(f"Hour estimation: Simple formula (commits + lines changed)")
    print("=" * 60)
    print()
    
    # Create analytics instance
    analytics = LocalGitAnalytics(args.base_path, args.copilot_invokers)
    
    # Generate report
    analytics.generate_report(
        args.output,
        start_date,
        end_date,
        include_repos,
        exclude_repos,
        args.max_depth,
        args.use_sessions
    )


if __name__ == '__main__':
    main()
