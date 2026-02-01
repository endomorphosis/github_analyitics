#!/usr/bin/env python3
"""Legacy shim.

The maintained implementation lives in the package module:
`python -m github_analyitics.reporting.github_analytics`

This wrapper exists so older scripts/tasks that call `github_analytics.py`
continue to work, while using the gh-backed implementation.
"""

from __future__ import annotations


def main() -> None:
    from github_analyitics.reporting.github_analytics import main as _main

    _main()

    @staticmethod
    def normalize_datetime(dt: Optional[datetime]) -> Optional[datetime]:
        """Normalize a datetime to UTC and ensure timezone awareness."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
        
    def check_rate_limit(self):
        """
        Check GitHub API rate limit and wait if necessary.
        
        Implements exponential backoff when approaching rate limits.
        """
        if not self.enable_rate_limiting:
            return

        # Avoid calling the rate_limit endpoint too frequently
        now_ts = time.time()
        if now_ts - self.last_rate_limit_check < 30:
            # Lightweight backoff if we're already throttling
            if self.backoff_time > 1:
                time.sleep(min(self.backoff_time, 2))
            return
            
        try:
            rate_limit = self.github.get_rate_limit()
            self.last_rate_limit_check = time.time()
            core = getattr(rate_limit, 'core', None)
            if core is None and hasattr(rate_limit, 'resources'):
                core = getattr(rate_limit.resources, 'core', None)
            if core is None:
                raise AttributeError("Rate limit core not available")
            
            # Increment counter first
            self.api_calls_made += 1
            
            # Log rate limit status every 100 calls
            if self.api_calls_made % 100 == 0:
                print(f"  [API] Rate limit: {core.remaining}/{core.limit} remaining, resets at {core.reset}")
            
            # If we're getting close to the limit (less than 100 calls remaining)
            if core.remaining < 100:
                wait_time = (core.reset - datetime.now(timezone.utc)).total_seconds() + 10
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
            
        except Exception as e:
            print(f"  [API] Warning: Could not check rate limit: {e}")
            # Conservative backoff when rate limit cannot be checked
            time.sleep(min(self.backoff_time, 5))
            self.backoff_time = min(self.backoff_time * 1.5, 10)
    
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

    def get_allowed_users(self, repo) -> Set[str]:
        """Return logins allowed for attribution within this repo."""
        allowed: Set[str] = set()
        try:
            owner = getattr(repo, 'owner', None)
            if owner and owner.login:
                allowed.add(owner.login)
        except Exception:
            pass

        try:
            collaborators = self.api_call_with_retry(lambda: repo.get_collaborators())
            for user in collaborators:
                if user and user.login:
                    allowed.add(user.login)
        except Exception as e:
            print(f"  [FILTER] Could not load collaborators for {repo.name}: {e}")

        if not allowed:
            allowed.add(self.username)

        return allowed
        
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
    
    def analyze_commits(self, repo, start_date=None, end_date=None, include_stats: bool = True,
                        allowed_users: Optional[Set[str]] = None) -> Dict:
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

        start_date = self.normalize_datetime(start_date)
        end_date = self.normalize_datetime(end_date)
        
        try:
            if allowed_users:
                commits_iterables = []
                for login in sorted(allowed_users):
                    if start_date or end_date:
                        commits_iterables.append(
                            self.api_call_with_retry(lambda l=login: repo.get_commits(author=l, since=start_date, until=end_date))
                        )
                    else:
                        commits_iterables.append(
                            self.api_call_with_retry(lambda l=login: repo.get_commits(author=l))
                        )
            else:
                if start_date or end_date:
                    commits_iterables = [self.api_call_with_retry(lambda: repo.get_commits(since=start_date, until=end_date))]
                else:
                    commits_iterables = [self.api_call_with_retry(lambda: repo.get_commits())]

            commit_index = 0
            for commits in commits_iterables:
                if commits is None:
                    continue
                for commit in commits:
                    commit_index += 1
                    if commit_index % 25 == 0:
                        self.check_rate_limit()  # Check periodically
                    try:
                        # Get commit date
                        commit_date = self.normalize_datetime(commit.commit.author.date)
                        
                        # Filter by date range if specified
                        if start_date and commit_date < start_date:
                            continue
                        if end_date and commit_date > end_date:
                            continue
                        
                        # Get author information
                        author = commit.author.login if commit.author else commit.commit.author.name
                        author_login = commit.author.login if commit.author else None
                        author_email = commit.commit.author.email if commit.commit.author else ""
                        date_key = commit_date.strftime('%Y-%m-%d')

                        if allowed_users is not None:
                            if not author_login or author_login not in allowed_users:
                                continue
                        
                        # Update statistics
                        data[author][date_key]['commits'] += 1

                        if self.commit_events is not None:
                            repo_full_name = getattr(repo, 'full_name', repo.name)
                            self.commit_events.append({
                                'repository': repo_full_name,
                                'commit': commit.sha,
                                'author': author,
                                'email': author_email,
                                'event_type': 'commit',
                                'event_timestamp': commit_date.isoformat(),
                                'subject': commit.commit.message.splitlines()[0] if commit.commit.message else ""
                            })
                        
                        # Get file statistics
                        if include_stats:
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
    
    def analyze_pull_requests(self, repo, start_date=None, end_date=None,
                              allowed_users: Optional[Set[str]] = None,
                              include_comments: bool = False,
                              include_review_comments: bool = True,
                              include_review_events: bool = False) -> Dict:
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

        start_date = self.normalize_datetime(start_date)
        end_date = self.normalize_datetime(end_date)
        
        try:
            # Get all pull requests (open and closed)
            prs = self.api_call_with_retry(lambda: repo.get_pulls(state='all'))
            
            pr_index = 0
            for pr in prs:
                pr_index += 1
                if pr_index % 10 == 0:
                    self.check_rate_limit()  # Check periodically
                try:
                    # Created date
                    created_date = self.normalize_datetime(pr.created_at)
                    created_in_range = True
                    if start_date and created_date < start_date:
                        created_in_range = False
                    if end_date and created_date > end_date:
                        created_in_range = False
                    
                    author = pr.user.login if pr.user else "Unknown"
                    if allowed_users is not None:
                        if not pr.user or pr.user.login not in allowed_users:
                            continue
                    date_key = created_date.strftime('%Y-%m-%d')
                    
                    if created_in_range:
                        data[author][date_key]['prs_created'] += 1
                    
                    # Add line changes from PR
                    if created_in_range:
                        data[author][date_key]['pr_additions'] += pr.additions
                        data[author][date_key]['pr_deletions'] += pr.deletions
                    
                    # Check if merged
                    if pr.merged:
                        merged_date = self.normalize_datetime(pr.merged_at)
                        if merged_date:
                            merged_in_range = True
                            if start_date and merged_date < start_date:
                                merged_in_range = False
                            if end_date and merged_date > end_date:
                                merged_in_range = False
                            if merged_in_range:
                                merged_key = merged_date.strftime('%Y-%m-%d')
                                data[author][merged_key]['prs_merged'] += 1

                    if self.pr_events is not None:
                        repo_full_name = getattr(repo, 'full_name', repo.name)
                        if created_in_range:
                            self.pr_events.append({
                                'repository': repo_full_name,
                                'number': pr.number,
                                'title': pr.title,
                                'author': author,
                                'event_type': 'created',
                                'event_timestamp': created_date.isoformat(),
                                'url': pr.html_url
                            })

                        closed_date = self.normalize_datetime(pr.closed_at)
                        if closed_date:
                            closed_in_range = True
                            if start_date and closed_date < start_date:
                                closed_in_range = False
                            if end_date and closed_date > end_date:
                                closed_in_range = False
                        if closed_date and closed_in_range:
                            self.pr_events.append({
                                'repository': repo_full_name,
                                'number': pr.number,
                                'title': pr.title,
                                'author': author,
                                'event_type': 'closed',
                                'event_timestamp': closed_date.isoformat(),
                                'url': pr.html_url
                            })

                        if pr.merged and pr.merged_at:
                            merged_date = self.normalize_datetime(pr.merged_at)
                            if merged_date:
                                merged_in_range = True
                                if start_date and merged_date < start_date:
                                    merged_in_range = False
                                if end_date and merged_date > end_date:
                                    merged_in_range = False
                            if merged_date and merged_in_range:
                                self.pr_events.append({
                                    'repository': repo_full_name,
                                    'number': pr.number,
                                    'title': pr.title,
                                    'author': author,
                                    'event_type': 'merged',
                                    'event_timestamp': merged_date.isoformat(),
                                    'url': pr.html_url
                                })

                        if include_comments:
                            # Issue comments on the PR conversation
                            try:
                                comments = self.api_call_with_retry(lambda: pr.get_issue_comments())
                                if comments is not None:
                                    for comment in comments:
                                        try:
                                            comment_date = self.normalize_datetime(comment.created_at)
                                            if comment_date is None:
                                                continue
                                            if start_date and comment_date < start_date:
                                                continue
                                            if end_date and comment_date > end_date:
                                                continue

                                            commenter = comment.user.login if comment.user else "Unknown"
                                            if allowed_users is not None:
                                                if not comment.user or comment.user.login not in allowed_users:
                                                    continue

                                            self.pr_events.append({
                                                'repository': repo_full_name,
                                                'number': pr.number,
                                                'title': pr.title,
                                                'author': commenter,
                                                'event_type': 'comment',
                                                'event_timestamp': comment_date.isoformat(),
                                                'url': pr.html_url
                                            })
                                        except Exception:
                                            continue
                            except Exception:
                                pass

                            # Review comments (inline)
                            if include_review_comments:
                                try:
                                    review_comments = self.api_call_with_retry(lambda: pr.get_review_comments())
                                    if review_comments is not None:
                                        for comment in review_comments:
                                            try:
                                                comment_date = self.normalize_datetime(comment.created_at)
                                                if comment_date is None:
                                                    continue
                                                if start_date and comment_date < start_date:
                                                    continue
                                                if end_date and comment_date > end_date:
                                                    continue

                                                commenter = comment.user.login if comment.user else "Unknown"
                                                if allowed_users is not None:
                                                    if not comment.user or comment.user.login not in allowed_users:
                                                        continue

                                                self.pr_events.append({
                                                    'repository': repo_full_name,
                                                    'number': pr.number,
                                                    'title': pr.title,
                                                    'author': commenter,
                                                    'event_type': 'review_comment',
                                                    'event_timestamp': comment_date.isoformat(),
                                                    'url': pr.html_url
                                                })
                                            except Exception:
                                                continue
                                except Exception:
                                    pass

                            if include_review_events:
                                # Review submit timestamps (APPROVED/CHANGES_REQUESTED/etc)
                                try:
                                    reviews = self.api_call_with_retry(lambda: pr.get_reviews())
                                    if reviews is not None:
                                        for review in reviews:
                                            try:
                                                submitted = self.normalize_datetime(getattr(review, 'submitted_at', None))
                                                if submitted is None:
                                                    continue
                                                if start_date and submitted < start_date:
                                                    continue
                                                if end_date and submitted > end_date:
                                                    continue

                                                reviewer = review.user.login if review.user else "Unknown"
                                                if allowed_users is not None:
                                                    if not review.user or review.user.login not in allowed_users:
                                                        continue

                                                self.pr_events.append({
                                                    'repository': repo_full_name,
                                                    'number': pr.number,
                                                    'title': pr.title,
                                                    'author': reviewer,
                                                    'event_type': f"review_{(getattr(review, 'state', '') or '').lower()}",
                                                    'event_timestamp': submitted.isoformat(),
                                                    'url': pr.html_url
                                                })
                                            except Exception:
                                                continue
                                except Exception:
                                    pass
                            
                except Exception as e:
                    print(f"Warning: Error processing PR in {repo.name}: {e}")
                    continue
                    
        except GithubException as e:
            print(f"Warning: Error accessing PRs for {repo.name}: {e}")
            
        return data
    
    def analyze_issues(self, repo, start_date=None, end_date=None,
                       allowed_users: Optional[Set[str]] = None,
                       include_pull_requests: bool = False,
                       include_pull_request_comments_only: bool = True) -> Dict:
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

        start_date = self.normalize_datetime(start_date)
        end_date = self.normalize_datetime(end_date)
        
        try:
            issues = self.api_call_with_retry(lambda: repo.get_issues(state='all'))
            
            issue_index = 0
            for issue in issues:
                issue_index += 1
                if issue_index % 10 == 0:
                    self.check_rate_limit()  # Check periodically
                try:
                    # Pull requests show up in issues too.
                    # By default we skip them, but we can optionally capture PR comments via the issue API.
                    is_pr = bool(getattr(issue, 'pull_request', None))
                    if is_pr and not include_pull_requests:
                        continue
                    
                    # Created date
                    created_date = self.normalize_datetime(issue.created_at)
                    created_in_range = True
                    if start_date and created_date < start_date:
                        created_in_range = False
                    if end_date and created_date > end_date:
                        created_in_range = False
                    
                    author = issue.user.login if issue.user else "Unknown"
                    if allowed_users is not None:
                        if not issue.user or issue.user.login not in allowed_users:
                            continue
                    date_key = created_date.strftime('%Y-%m-%d')
                    
                    repo_full_name = getattr(repo, 'full_name', repo.name)

                    if not (is_pr and include_pull_request_comments_only):
                        if created_in_range:
                            data[author][date_key]['issues_created'] += 1

                        if self.issue_events is not None:
                            if created_in_range:
                                self.issue_events.append({
                                    'repository': repo_full_name,
                                    'number': issue.number,
                                    'title': issue.title,
                                    'author': author,
                                    'event_type': 'created' if not is_pr else 'pr_created',
                                    'event_timestamp': created_date.isoformat(),
                                    'url': issue.html_url
                                })
                    
                    # Check if closed
                    if issue.closed_at and not (is_pr and include_pull_request_comments_only):
                        closed_date = self.normalize_datetime(issue.closed_at)
                        closed_in_range = True
                        if start_date and closed_date < start_date:
                            closed_in_range = False
                        if end_date and closed_date > end_date:
                            closed_in_range = False
                        if closed_in_range:
                            closed_key = closed_date.strftime('%Y-%m-%d')
                            data[author][closed_key]['issues_closed'] += 1

                        if self.issue_events is not None and closed_in_range:
                            self.issue_events.append({
                                'repository': repo_full_name,
                                'number': issue.number,
                                'title': issue.title,
                                'author': author,
                                'event_type': 'closed' if not is_pr else 'pr_closed',
                                'event_timestamp': closed_date.isoformat(),
                                'url': issue.html_url
                            })
                    
                    # Count comments
                    try:
                        comments = issue.get_comments()
                        for comment in comments:
                            comment_date = self.normalize_datetime(comment.created_at)
                            if start_date and comment_date < start_date:
                                continue
                            if end_date and comment_date > end_date:
                                continue
                            
                            commenter = comment.user.login if comment.user else "Unknown"
                            if allowed_users is not None:
                                if not comment.user or comment.user.login not in allowed_users:
                                    continue
                            comment_key = comment_date.strftime('%Y-%m-%d')
                            data[commenter][comment_key]['issue_comments'] += 1

                            if self.issue_events is not None:
                                self.issue_events.append({
                                    'repository': repo_full_name,
                                    'number': issue.number,
                                    'title': issue.title,
                                    'author': commenter,
                                    'event_type': 'comment' if not is_pr else 'pr_comment',
                                    'event_timestamp': comment_date.isoformat(),
                                    'url': issue.html_url
                                })
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

        start_date = self.normalize_datetime(start_date)
        end_date = self.normalize_datetime(end_date)
        
        try:
            # Get commits to track file modifications (filtered server-side if possible)
            if start_date or end_date:
                commits = self.api_call_with_retry(lambda: repo.get_commits(since=start_date, until=end_date))
            else:
                commits = self.api_call_with_retry(lambda: repo.get_commits())
            
            # Track files and their last modification per day
            file_modifications = {}
            
            commit_index = 0
            for commit in commits:
                commit_index += 1
                if commit_index % 25 == 0:
                    self.check_rate_limit()  # Check periodically
                try:
                    commit_date = self.normalize_datetime(commit.commit.author.date)
                    
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
                # More efficient: Check if user is in contributors list
                contributors = self.api_call_with_retry(lambda: repo.get_contributors())
                contributor_logins = [c.login for c in contributors if c.login]
                if filter_by_user_contribution not in contributor_logins:
                    return False
            except Exception as e:
                # Fallback to commit check if contributors API fails
                try:
                    commits = self.api_call_with_retry(
                        lambda: list(repo.get_commits(author=filter_by_user_contribution)[:1])
                    )
                    if not commits:
                        return False
                except Exception as e2:
                    print(f"  [FILTER] Could not check contributions for {repo_name}: {e2}")
                    return False
        
        return True
    
    def analyze_all_repositories(self, 
                                 start_date=None, 
                                 end_date=None,
                                 include_repos: Optional[List[str]] = None,
                                 exclude_repos: Optional[List[str]] = None,
                                 filter_by_user_contribution: Optional[str] = None,
                                 skip_file_modifications: bool = False,
                                 skip_commit_stats: bool = False,
                                 restrict_to_collaborators: bool = True,
                                 restrict_to_owner_namespace: bool = True,
                                 fast_mode: bool = False,
                                 include_pr_comments: bool = False,
                                 include_pr_review_comments: bool = True,
                                 include_pr_review_events: bool = False,
                                 include_issue_pr_comments: bool = False) -> pd.DataFrame:
        """
        Analyze all repositories for the user with filtering options.
        
        Args:
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            include_repos: List of repository names to include (None = include all)
            exclude_repos: List of repository names to exclude (None = exclude none)
            filter_by_user_contribution: Only include repos where this user has contributed
            skip_file_modifications: Skip file modification analysis (faster)
            fast_mode: Skip PRs/issues/file mods and commit stats (fastest)
            
        Returns:
            Pandas DataFrame with comprehensive statistics
        """
        start_date = self.normalize_datetime(start_date)
        end_date = self.normalize_datetime(end_date)

        print(f"Fetching repositories for user: {self.username}")

        self.commit_events = []
        self.pr_events = []
        self.issue_events = []
        
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
            if restrict_to_owner_namespace:
                owner = getattr(repo, 'owner', None)
                owner_login = owner.login if owner else None
                if owner_login and owner_login != self.username:
                    skipped_count += 1
                    print(f"Skipping non-owned repository: {repo.name} ({owner_login})")
                    continue
            # Check if repository should be included
            if not self.should_include_repository(repo, include_set, exclude_set, filter_by_user_contribution):
                skipped_count += 1
                print(f"Skipping repository: {repo.name}")
                continue
                
            repo_count += 1
            print(f"Analyzing repository {repo_count}: {repo.name}")

            allowed_users = None
            if restrict_to_collaborators:
                allowed_users = self.get_allowed_users(repo)
            
            # Analyze commits
            print(f"  - Analyzing commits...")
            commit_data = self.analyze_commits(
                repo,
                start_date,
                end_date,
                include_stats=not fast_mode and not skip_commit_stats,
                allowed_users=allowed_users
            )
            
            if fast_mode:
                print(f"  - Skipping pull requests (fast mode)...")
                pr_data = {}
                print(f"  - Skipping issues (fast mode)...")
                issue_data = {}
                print(f"  - Skipping file modifications (fast mode)...")
                file_mod_data = {}
            else:
                # Analyze pull requests
                print(f"  - Analyzing pull requests...")
                pr_data = self.analyze_pull_requests(
                    repo,
                    start_date,
                    end_date,
                    allowed_users=allowed_users,
                    include_comments=include_pr_comments,
                    include_review_comments=include_pr_review_comments,
                    include_review_events=include_pr_review_events,
                )
                
                # Analyze issues
                print(f"  - Analyzing issues...")
                issue_data = self.analyze_issues(
                    repo,
                    start_date,
                    end_date,
                    allowed_users=allowed_users,
                    include_pull_requests=include_issue_pr_comments,
                    include_pull_request_comments_only=True,
                )
                
                # Analyze file modifications
                if skip_file_modifications:
                    print(f"  - Skipping file modifications...")
                    file_mod_data = {}
                else:
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
                       filter_by_user_contribution: Optional[str] = None,
                       skip_file_modifications: bool = False,
                       skip_commit_stats: bool = False,
                       restrict_to_collaborators: bool = True,
                       restrict_to_owner_namespace: bool = True,
                       fast_mode: bool = False,
                       include_pr_comments: bool = False,
                       include_pr_review_comments: bool = True,
                       include_pr_review_events: bool = False,
                       include_issue_pr_comments: bool = False):
        """
        Generate a comprehensive report and save to Excel.
        
        Args:
            output_file: Output file path (defaults to github_analytics_{timestamp}.xlsx)
            start_date: Start date for analysis (optional)
            end_date: End date for analysis (optional)
            include_repos: List of repository names to include (None = include all)
            exclude_repos: List of repository names to exclude (None = exclude none)
            filter_by_user_contribution: Only include repos where this user has contributed
            skip_file_modifications: Skip file modification analysis (faster)
            fast_mode: Skip PRs/issues/file mods and commit stats (fastest)
        """
        # Analyze all repositories
        df = self.analyze_all_repositories(
            start_date, 
            end_date, 
            include_repos, 
            exclude_repos, 
            filter_by_user_contribution,
            skip_file_modifications,
            skip_commit_stats,
            restrict_to_collaborators,
            restrict_to_owner_namespace,
            fast_mode,
            include_pr_comments,
            include_pr_review_comments,
            include_pr_review_events,
            include_issue_pr_comments,
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

            if self.pr_events:
                pr_events_df = pd.DataFrame(self.pr_events)
                pr_events_df = pr_events_df.sort_values('event_timestamp', ascending=False)
                pr_events_df.to_excel(writer, sheet_name='PR Events', index=False)

            if self.issue_events:
                issue_events_df = pd.DataFrame(self.issue_events)
                issue_events_df = issue_events_df.sort_values('event_timestamp', ascending=False)
                issue_events_df.to_excel(writer, sheet_name='Issue Events', index=False)

            if self.commit_events or self.pr_events or self.issue_events:
                timeline_events = []
                if self.commit_events:
                    for event in self.commit_events:
                        timeline_events.append({
                            'event_type': 'commit',
                            **event
                        })
                if self.pr_events:
                    for event in self.pr_events:
                        timeline_events.append({
                            'event_type': 'pull_request',
                            **event
                        })
                if self.issue_events:
                    for event in self.issue_events:
                        timeline_events.append({
                            'event_type': 'issue',
                            **event
                        })
                timeline_df = pd.DataFrame(timeline_events)
                timeline_df = timeline_df.sort_values('event_timestamp', ascending=False)
                timeline_df.to_excel(writer, sheet_name='User Timeline', index=False)
        
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
    skip_file_modifications = False
    skip_commit_stats = False
    include_all_authors = False
    include_non_owned = False
    disable_rate_limiting = False
    fast_mode = False
    include_pr_comments = False
    include_pr_review_comments = True
    include_pr_review_events = False
    include_issue_pr_comments = False
    
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
            elif sys.argv[i] == '--skip-file-modifications':
                skip_file_modifications = True
                i += 1
            elif sys.argv[i] == '--skip-commit-stats':
                skip_commit_stats = True
                i += 1
            elif sys.argv[i] == '--include-all-authors':
                include_all_authors = True
                i += 1
            elif sys.argv[i] == '--include-non-owned':
                include_non_owned = True
                i += 1
            elif sys.argv[i] == '--fast':
                fast_mode = True
                i += 1
            elif sys.argv[i] == '--include-pr-comments':
                include_pr_comments = True
                i += 1
            elif sys.argv[i] == '--skip-pr-review-comments':
                include_pr_review_comments = False
                i += 1
            elif sys.argv[i] == '--include-pr-review-events':
                include_pr_review_events = True
                i += 1
            elif sys.argv[i] == '--include-pr-issue-comments':
                # Capture PR comments via the Issues API as well (PRs show up in Issues list).
                include_issue_pr_comments = True
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
                print("  --skip-file-modifications      Skip file modification analysis (faster)")
                print("  --skip-commit-stats             Skip commit stats lookup (faster)")
                print("  --include-all-authors          Include all authors (disable collaborator-only filter)")
                print("  --include-non-owned            Include repositories not owned by this user")
                print("  --fast                         Skip PRs/issues/file mods and commit stats (fastest)")
                print("  --include-pr-comments          Include PR issue comments and review comments")
                print("  --skip-pr-review-comments      Do not include inline PR review comments")
                print("  --include-pr-review-events     Include PR review submission events")
                print("  --include-pr-issue-comments    Include PR conversation comments via Issues API")
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
        filter_by_user,
        skip_file_modifications,
        skip_commit_stats,
        not include_all_authors,
        not include_non_owned,
        fast_mode,
        include_pr_comments,
        include_pr_review_comments,
        include_pr_review_events,
        include_issue_pr_comments,
    )


if __name__ == '__main__':
    main()
