#!/usr/bin/env python3
"""GitHub Analytics backed by the GitHub CLI (`gh`).

This module exists so the project can use the GitHub CLI for auth and API calls
instead of a Python GitHub SDK.

Public API intentionally mirrors the original `github_analytics.py` module:
- `GitHubAnalytics` class (with `generate_report` and helpers)
- `main()` CLI entry point

Notes:
- `token` is accepted for backward compatibility but is not required when `gh`
  is authenticated (`gh auth login`).
- Rate limiting is handled by GitHub + gh; the old PyGithub-specific
  rate-limit code is not used.
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from dotenv import load_dotenv

from github_analyitics.reporting.gh_cli import (
    GhCliError,
    GhCliNotFound,
    ensure_gh_available,
    gh_api_json,
    gh_auth_login,
)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_iso8601(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = pd.to_datetime(value, utc=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def _date_key(dt: datetime) -> str:
    return _to_utc(dt).date().isoformat()


class GitHubAnalytics:
    """Analyzes GitHub repository activity via `gh api` and generates reports."""

    def __init__(self, token: str, username: str, enable_rate_limiting: bool = True):
        self.username = (username or "").strip()
        self.enable_rate_limiting = enable_rate_limiting

        # Event streams (used by other tools/tests)
        self.commit_events: List[Dict] = []
        self.pr_events: List[Dict] = []
        self.issue_events: List[Dict] = []

    @staticmethod
    def estimate_hours_from_commits(commits: int, total_lines_changed: int) -> float:
        if commits <= 0 and total_lines_changed <= 0:
            return 0.0
        hours = commits * 0.5 + (total_lines_changed / 30.0)
        return round(hours, 2)

    @staticmethod
    def merge_data(*data_dicts: Dict) -> Dict:
        merged: Dict = {}
        for data in data_dicts:
            for user, dates in (data or {}).items():
                merged.setdefault(user, {})
                for date, metrics in (dates or {}).items():
                    merged[user].setdefault(date, {})
                    for key, value in (metrics or {}).items():
                        if isinstance(value, (int, float)):
                            merged[user][date][key] = merged[user][date].get(key, 0) + value
                        else:
                            merged[user][date][key] = value
        return merged

    def _list_repos(self, *, restrict_to_owner_namespace: bool) -> List[Dict]:
        # If the requested username matches the authenticated user, prefer /user/repos
        # (includes private repos you can access). Otherwise fall back to /users/{u}/repos.
        auth_login = None
        try:
            auth_login = (gh_auth_login() or "").strip()
        except Exception:
            auth_login = None

        if auth_login and self.username and auth_login.lower() == self.username.lower():
            repos = gh_api_json(
                "/user/repos",
                params={"per_page": "100", "type": "all", "sort": "updated"},
                paginate=True,
            )
        else:
            repos = gh_api_json(
                f"/users/{self.username}/repos",
                params={"per_page": "100", "type": "all", "sort": "updated"},
                paginate=True,
            )

        repos = repos or []
        if restrict_to_owner_namespace:
            # Keep repos whose owner login matches the username.
            out = []
            for r in repos:
                owner = ((r or {}).get("owner") or {}).get("login")
                if owner and self.username and owner.lower() == self.username.lower():
                    out.append(r)
            return out
        return list(repos)

    def _iter_commits(
        self,
        full_name: str,
        *,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
    ) -> List[Dict]:
        params: Dict[str, str] = {"per_page": "100"}
        if start_date:
            params["since"] = _to_utc(start_date).isoformat().replace("+00:00", "Z")
        if end_date:
            params["until"] = _to_utc(end_date).isoformat().replace("+00:00", "Z")

        items = gh_api_json(f"/repos/{full_name}/commits", params=params, paginate=True) or []
        return list(items)

    def _get_commit_detail(self, full_name: str, sha: str) -> Dict:
        return gh_api_json(f"/repos/{full_name}/commits/{sha}") or {}

    def _iter_pulls(self, full_name: str) -> List[Dict]:
        return list(
            gh_api_json(
                f"/repos/{full_name}/pulls",
                params={"state": "all", "per_page": "100"},
                paginate=True,
            )
            or []
        )

    def _iter_issues(self, full_name: str, *, start_date: Optional[datetime]) -> List[Dict]:
        params: Dict[str, str] = {"state": "all", "per_page": "100"}
        # This filters by update time (not create time), but helps keep payload manageable.
        if start_date:
            params["since"] = _to_utc(start_date).isoformat().replace("+00:00", "Z")
        return list(gh_api_json(f"/repos/{full_name}/issues", params=params, paginate=True) or [])

    def _iter_issue_comments(self, full_name: str, number: int) -> List[Dict]:
        return list(
            gh_api_json(
                f"/repos/{full_name}/issues/{number}/comments",
                params={"per_page": "100"},
                paginate=True,
            )
            or []
        )

    def _iter_pr_review_comments(self, full_name: str, number: int) -> List[Dict]:
        return list(
            gh_api_json(
                f"/repos/{full_name}/pulls/{number}/comments",
                params={"per_page": "100"},
                paginate=True,
            )
            or []
        )

    def _iter_pr_reviews(self, full_name: str, number: int) -> List[Dict]:
        return list(
            gh_api_json(
                f"/repos/{full_name}/pulls/{number}/reviews",
                params={"per_page": "100"},
                paginate=True,
            )
            or []
        )

    def analyze_all_repositories(
        self,
        *,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        include_repos: Optional[List[str]],
        exclude_repos: Optional[List[str]],
        filter_by_user_contribution: Optional[str],
        skip_file_modifications: bool,
        skip_commit_stats: bool,
        restrict_to_collaborators: bool,
        restrict_to_owner_namespace: bool,
        fast_mode: bool,
        include_pr_comments: bool,
        include_pr_review_comments: bool,
        include_pr_review_events: bool,
        include_issue_pr_comments: bool,
        allowed_users: Optional[Set[str]] = None,
    ) -> pd.DataFrame:
        allowed_lower: Optional[Set[str]] = None
        # Note: restrict_to_collaborators and filter_by_user_contribution are not enforced
        # in this gh-backed implementation (they require extra API calls).
        _ = (restrict_to_collaborators, filter_by_user_contribution)

        repos = self._list_repos(restrict_to_owner_namespace=restrict_to_owner_namespace)

        if allowed_users:
            allowed_lower = {str(u).strip().lower() for u in allowed_users if u and str(u).strip()}

        def is_allowed(user: str) -> bool:
            if not allowed_lower:
                return True
            if not user:
                return False
            return str(user).strip().lower() in allowed_lower

        if include_repos:
            include_set = {r.strip() for r in include_repos if r and r.strip()}
            repos = [r for r in repos if (r.get("name") in include_set or r.get("full_name") in include_set)]
        if exclude_repos:
            exclude_set = {r.strip() for r in exclude_repos if r and r.strip()}
            repos = [r for r in repos if (r.get("name") not in exclude_set and r.get("full_name") not in exclude_set)]

        # Aggregation structure: user -> date -> metrics
        data: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        files_by_user_day: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

        self.commit_events = []
        self.pr_events = []
        self.issue_events = []

        for repo in repos:
            full_name = repo.get("full_name") or repo.get("nameWithOwner")
            if not full_name:
                continue

            # --- Commits ---
            commits = self._iter_commits(full_name, start_date=start_date, end_date=end_date)
            for item in commits:
                sha = item.get("sha")
                commit_obj = item.get("commit") or {}
                msg = ((commit_obj.get("message") or "").splitlines() or [""])[0]

                author_login = None
                if item.get("author") and isinstance(item.get("author"), dict):
                    author_login = item["author"].get("login")
                if not author_login:
                    author_login = ((commit_obj.get("author") or {}).get("name") or "Unknown")

                dt = _parse_iso8601(((commit_obj.get("author") or {}).get("date")))
                if not dt:
                    dt = _parse_iso8601(((commit_obj.get("committer") or {}).get("date")))
                if not dt:
                    continue

                if start_date and _to_utc(dt) < _to_utc(start_date):
                    continue
                if end_date and _to_utc(dt) > _to_utc(end_date):
                    continue

                if not is_allowed(author_login):
                    continue

                date = _date_key(dt)
                data[author_login][date]["commits"] += 1

                additions = 0
                deletions = 0
                files_modified = []

                if not skip_commit_stats and sha:
                    detail = self._get_commit_detail(full_name, sha)
                    stats = detail.get("stats") or {}
                    additions = int(stats.get("additions") or 0)
                    deletions = int(stats.get("deletions") or 0)

                    if not skip_file_modifications:
                        for f in (detail.get("files") or []):
                            fn = f.get("filename")
                            if fn:
                                files_modified.append(fn)

                if additions or deletions:
                    data[author_login][date]["lines_added"] += additions
                    data[author_login][date]["lines_deleted"] += deletions
                    data[author_login][date]["total_lines_changed"] += additions + deletions

                for fn in files_modified:
                    files_by_user_day[(author_login, date)].add(fn)

                self.commit_events.append(
                    {
                        "repository": full_name,
                        "commit": sha,
                        "author": author_login,
                        "event_timestamp": _to_utc(dt).isoformat().replace("+00:00", "Z"),
                        "subject": msg,
                    }
                )

            if fast_mode:
                continue

            # --- Pull requests ---
            pulls = self._iter_pulls(full_name)
            for pr in pulls:
                number = pr.get("number")
                title = pr.get("title")
                url = pr.get("html_url")
                author = ((pr.get("user") or {}).get("login") or "Unknown")

                created_at = _parse_iso8601(pr.get("created_at"))
                closed_at = _parse_iso8601(pr.get("closed_at"))
                merged_at = _parse_iso8601(pr.get("merged_at"))

                if created_at and (not start_date or _to_utc(created_at) >= _to_utc(start_date)) and (
                    not end_date or _to_utc(created_at) <= _to_utc(end_date)
                ):
                    if is_allowed(author):
                        date = _date_key(created_at)
                        data[author][date]["prs_created"] += 1
                        self.pr_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": author,
                                "event_type": "created",
                                "event_timestamp": _to_utc(created_at).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

                if merged_at and (not start_date or _to_utc(merged_at) >= _to_utc(start_date)) and (
                    not end_date or _to_utc(merged_at) <= _to_utc(end_date)
                ):
                    if is_allowed(author):
                        date = _date_key(merged_at)
                        data[author][date]["prs_merged"] += 1
                        self.pr_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": author,
                                "event_type": "merged",
                                "event_timestamp": _to_utc(merged_at).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

                if closed_at and (not start_date or _to_utc(closed_at) >= _to_utc(start_date)) and (
                    not end_date or _to_utc(closed_at) <= _to_utc(end_date)
                ):
                    if is_allowed(author):
                        self.pr_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": author,
                                "event_type": "closed",
                                "event_timestamp": _to_utc(closed_at).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

                if include_pr_comments and number is not None:
                    # PR conversation comments are issue comments.
                    for c in self._iter_issue_comments(full_name, int(number)):
                        c_user = ((c.get("user") or {}).get("login") or "Unknown")
                        if not is_allowed(c_user):
                            continue

                        c_dt = _parse_iso8601(c.get("created_at"))
                        if not c_dt:
                            continue
                        if start_date and _to_utc(c_dt) < _to_utc(start_date):
                            continue
                        if end_date and _to_utc(c_dt) > _to_utc(end_date):
                            continue
                        date = _date_key(c_dt)
                        data[c_user][date]["issue_comments"] += 1
                        self.pr_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": c_user,
                                "event_type": "comment",
                                "event_timestamp": _to_utc(c_dt).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

                    if include_pr_review_comments:
                        for c in self._iter_pr_review_comments(full_name, int(number)):
                            c_user = ((c.get("user") or {}).get("login") or "Unknown")
                            if not is_allowed(c_user):
                                continue

                            c_dt = _parse_iso8601(c.get("created_at"))
                            if not c_dt:
                                continue
                            if start_date and _to_utc(c_dt) < _to_utc(start_date):
                                continue
                            if end_date and _to_utc(c_dt) > _to_utc(end_date):
                                continue
                            date = _date_key(c_dt)
                            data[c_user][date]["issue_comments"] += 1
                            self.pr_events.append(
                                {
                                    "repository": full_name,
                                    "number": number,
                                    "title": title,
                                    "author": c_user,
                                    "event_type": "review_comment",
                                    "event_timestamp": _to_utc(c_dt).isoformat().replace("+00:00", "Z"),
                                    "url": url,
                                }
                            )

                    if include_pr_review_events:
                        for r in self._iter_pr_reviews(full_name, int(number)):
                            r_user = ((r.get("user") or {}).get("login") or "Unknown")
                            if not is_allowed(r_user):
                                continue

                            r_dt = _parse_iso8601(r.get("submitted_at"))
                            if not r_dt:
                                continue
                            if start_date and _to_utc(r_dt) < _to_utc(start_date):
                                continue
                            if end_date and _to_utc(r_dt) > _to_utc(end_date):
                                continue
                            self.pr_events.append(
                                {
                                    "repository": full_name,
                                    "number": number,
                                    "title": title,
                                    "author": r_user,
                                    "event_type": "review_submitted",
                                    "event_timestamp": _to_utc(r_dt).isoformat().replace("+00:00", "Z"),
                                    "url": url,
                                }
                            )

            # --- Issues ---
            issues = self._iter_issues(full_name, start_date=start_date)
            for issue in issues:
                is_pr = bool(issue.get("pull_request"))
                if is_pr and not include_issue_pr_comments:
                    continue

                number = issue.get("number")
                title = issue.get("title")
                url = issue.get("html_url")
                author = ((issue.get("user") or {}).get("login") or "Unknown")

                created_at = _parse_iso8601(issue.get("created_at"))
                closed_at = _parse_iso8601(issue.get("closed_at"))

                if created_at and (not start_date or _to_utc(created_at) >= _to_utc(start_date)) and (
                    not end_date or _to_utc(created_at) <= _to_utc(end_date)
                ):
                    if is_allowed(author):
                        date = _date_key(created_at)
                        data[author][date]["issues_created"] += 1
                        self.issue_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": author,
                                "event_type": "created",
                                "event_timestamp": _to_utc(created_at).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

                if closed_at and (not start_date or _to_utc(closed_at) >= _to_utc(start_date)) and (
                    not end_date or _to_utc(closed_at) <= _to_utc(end_date)
                ):
                    if is_allowed(author):
                        date = _date_key(closed_at)
                        data[author][date]["issues_closed"] += 1
                        self.issue_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": author,
                                "event_type": "closed",
                                "event_timestamp": _to_utc(closed_at).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

                # Comments
                if number is not None:
                    for c in self._iter_issue_comments(full_name, int(number)):
                        c_user = ((c.get("user") or {}).get("login") or "Unknown")
                        if not is_allowed(c_user):
                            continue

                        c_dt = _parse_iso8601(c.get("created_at"))
                        if not c_dt:
                            continue
                        if start_date and _to_utc(c_dt) < _to_utc(start_date):
                            continue
                        if end_date and _to_utc(c_dt) > _to_utc(end_date):
                            continue
                        date = _date_key(c_dt)
                        data[c_user][date]["issue_comments"] += 1
                        self.issue_events.append(
                            {
                                "repository": full_name,
                                "number": number,
                                "title": title,
                                "author": c_user,
                                "event_type": "comment",
                                "event_timestamp": _to_utc(c_dt).isoformat().replace("+00:00", "Z"),
                                "url": url,
                            }
                        )

        # Convert to DataFrame
        rows: List[Dict] = []
        for user, dates in data.items():
            for date, metrics in dates.items():
                commits = int(metrics.get("commits", 0))
                lines_added = int(metrics.get("lines_added", 0))
                lines_deleted = int(metrics.get("lines_deleted", 0))
                total_lines_changed = int(metrics.get("total_lines_changed", 0))

                files_modified = len(files_by_user_day.get((user, date), set()))

                prs_created = int(metrics.get("prs_created", 0))
                prs_merged = int(metrics.get("prs_merged", 0))
                issues_created = int(metrics.get("issues_created", 0))
                issues_closed = int(metrics.get("issues_closed", 0))
                issue_comments = int(metrics.get("issue_comments", 0))

                estimated_hours = self.estimate_hours_from_commits(commits, total_lines_changed)

                rows.append(
                    {
                        "date": date,
                        "user": user,
                        "commits": commits,
                        "lines_added": lines_added,
                        "lines_deleted": lines_deleted,
                        "total_lines_changed": total_lines_changed,
                        "files_modified": files_modified,
                        "prs_created": prs_created,
                        "prs_merged": prs_merged,
                        "issues_created": issues_created,
                        "issues_closed": issues_closed,
                        "issue_comments": issue_comments,
                        "estimated_hours": estimated_hours,
                    }
                )

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["date", "user"], ascending=[False, True])
        return df

    def generate_report(
        self,
        output_file: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        include_repos: Optional[List[str]],
        exclude_repos: Optional[List[str]],
        filter_by_user_contribution: Optional[str],
        skip_file_modifications: bool,
        skip_commit_stats: bool,
        restrict_to_collaborators: bool,
        restrict_to_owner_namespace: bool,
        fast_mode: bool,
        include_pr_comments: bool,
        include_pr_review_comments: bool,
        include_pr_review_events: bool,
        include_issue_pr_comments: bool,
    ) -> None:
        if not output_file:
            from github_analyitics.reporting.report_paths import default_xlsx_path

            output_file = str(default_xlsx_path("github_analytics.xlsx"))

        df = self.analyze_all_repositories(
            start_date=start_date,
            end_date=end_date,
            include_repos=include_repos,
            exclude_repos=exclude_repos,
            filter_by_user_contribution=filter_by_user_contribution,
            skip_file_modifications=skip_file_modifications,
            skip_commit_stats=skip_commit_stats,
            restrict_to_collaborators=restrict_to_collaborators,
            restrict_to_owner_namespace=restrict_to_owner_namespace,
            fast_mode=fast_mode,
            include_pr_comments=include_pr_comments,
            include_pr_review_comments=include_pr_review_comments,
            include_pr_review_events=include_pr_review_events,
            include_issue_pr_comments=include_issue_pr_comments,
        )

        if df.empty:
            print("No activity found. Writing empty report.")

        # Summary sheets
        user_summary = pd.DataFrame()
        daily_summary = pd.DataFrame()

        if not df.empty:
            user_summary = (
                df.groupby("user")
                .agg(
                    {
                        "commits": "sum",
                        "lines_added": "sum",
                        "lines_deleted": "sum",
                        "total_lines_changed": "sum",
                        "files_modified": "sum",
                        "prs_created": "sum",
                        "prs_merged": "sum",
                        "issues_created": "sum",
                        "issues_closed": "sum",
                        "issue_comments": "sum",
                        "estimated_hours": "sum",
                    }
                )
                .reset_index()
                .sort_values("estimated_hours", ascending=False)
            )

            daily_summary = (
                df.groupby("date")
                .agg(
                    {
                        "commits": "sum",
                        "lines_added": "sum",
                        "lines_deleted": "sum",
                        "total_lines_changed": "sum",
                        "files_modified": "sum",
                        "prs_created": "sum",
                        "prs_merged": "sum",
                        "issues_created": "sum",
                        "issues_closed": "sum",
                        "issue_comments": "sum",
                        "estimated_hours": "sum",
                        "user": "nunique",
                    }
                )
                .reset_index()
                .rename(columns={"user": "active_users"})
                .sort_values("date", ascending=False)
            )

        # Write workbook
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Detailed Report", index=False)
            user_summary.to_excel(writer, sheet_name="User Summary", index=False)
            daily_summary.to_excel(writer, sheet_name="Daily Summary", index=False)

            if self.pr_events:
                pd.DataFrame(self.pr_events).to_excel(writer, sheet_name="PR Events", index=False)
            if self.issue_events:
                pd.DataFrame(self.issue_events).to_excel(writer, sheet_name="Issue Events", index=False)

            # Unified timeline (commits + PR + issues)
            timeline_events: List[Dict] = []
            for ev in self.commit_events:
                timeline_events.append({"event_type": "commit", **ev})
            for ev in self.pr_events:
                timeline_events.append({"event_type": "pull_request", **ev})
            for ev in self.issue_events:
                timeline_events.append({"event_type": "issue", **ev})

            if timeline_events:
                timeline_df = pd.DataFrame(timeline_events)
                if "event_timestamp" in timeline_df.columns:
                    timeline_df["event_timestamp"] = pd.to_datetime(timeline_df["event_timestamp"], utc=True, errors="coerce")
                    timeline_df = timeline_df.sort_values("event_timestamp", ascending=False)
                    timeline_df["event_timestamp"] = timeline_df["event_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                timeline_df.to_excel(writer, sheet_name="User Timeline", index=False)

        print(f"\nReport generated successfully: {output_file}")


def main() -> None:
    load_dotenv()

    # Ensure gh exists early with a clear error.
    try:
        ensure_gh_available()
    except GhCliNotFound as e:
        print(f"Error: {e}")
        print("Tip: on Linux, try: scripts/install_github_cli.sh")
        raise SystemExit(1)

    username = (os.getenv("GITHUB_USERNAME") or "").strip()
    if not username:
        try:
            username = gh_auth_login().strip()
        except Exception:
            username = ""

    # Optional: Parse command line arguments (kept compatible with prior script)
    start_date = None
    end_date = None
    output_file = None
    output_dir = None
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
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--start-date" and i + 1 < len(sys.argv):
                start_date = datetime.strptime(sys.argv[i + 1], "%Y-%m-%d")
                i += 2
            elif sys.argv[i] == "--end-date" and i + 1 < len(sys.argv):
                end_date = datetime.strptime(sys.argv[i + 1], "%Y-%m-%d")
                i += 2
            elif sys.argv[i] == "--output" and i + 1 < len(sys.argv):
                output_file = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--output-dir" and i + 1 < len(sys.argv):
                output_dir = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--include-repos" and i + 1 < len(sys.argv):
                include_repos = sys.argv[i + 1].split(",")
                i += 2
            elif sys.argv[i] == "--exclude-repos" and i + 1 < len(sys.argv):
                exclude_repos = sys.argv[i + 1].split(",")
                i += 2
            elif sys.argv[i] == "--filter-by-user" and i + 1 < len(sys.argv):
                filter_by_user = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--disable-rate-limiting":
                disable_rate_limiting = True
                i += 1
            elif sys.argv[i] == "--skip-file-modifications":
                skip_file_modifications = True
                i += 1
            elif sys.argv[i] == "--skip-commit-stats":
                skip_commit_stats = True
                i += 1
            elif sys.argv[i] == "--include-all-authors":
                include_all_authors = True
                i += 1
            elif sys.argv[i] == "--include-non-owned":
                include_non_owned = True
                i += 1
            elif sys.argv[i] == "--fast":
                fast_mode = True
                i += 1
            elif sys.argv[i] == "--include-pr-comments":
                include_pr_comments = True
                i += 1
            elif sys.argv[i] == "--skip-pr-review-comments":
                include_pr_review_comments = False
                i += 1
            elif sys.argv[i] == "--include-pr-review-events":
                include_pr_review_events = True
                i += 1
            elif sys.argv[i] == "--include-pr-issue-comments":
                include_issue_pr_comments = True
                i += 1
            elif sys.argv[i] in ["--help", "-h"]:
                print("Usage: python -m github_analyitics.reporting.github_analytics [OPTIONS]")
                print("\nOptions:")
                print("  --start-date YYYY-MM-DD        Start date for analysis")
                print("  --end-date YYYY-MM-DD          End date for analysis")
                print("  --output FILE                  Output file path")
                print("  --output-dir DIR               Base directory for timestamped outputs (when --output not set)")
                print("  --include-repos REPO1,REPO2    Only analyze these repositories")
                print("  --exclude-repos REPO1,REPO2    Exclude these repositories")
                print("  --filter-by-user USERNAME      (Not enforced in gh backend)")
                print("  --disable-rate-limiting        (No-op in gh backend)")
                print("  --skip-file-modifications      Skip file list per commit (faster)")
                print("  --skip-commit-stats            Skip per-commit stats lookup (faster)")
                print("  --include-all-authors          (Not enforced in gh backend)")
                print("  --include-non-owned            (No-op; repos are discovered via gh)")
                print("  --fast                         Skip PRs/issues and commit stats")
                print("  --include-pr-comments          Include PR issue comments and review comments")
                print("  --skip-pr-review-comments      Do not include inline PR review comments")
                print("  --include-pr-review-events     Include PR review submission events")
                print("  --include-pr-issue-comments    Include PR conversation comments via Issues API")
                print("  --help, -h                     Show this help message")
                raise SystemExit(0)
            else:
                i += 1

    if not username:
        print("Error: could not determine GitHub username.")
        print("Set GITHUB_USERNAME or run `gh auth login`.")
        raise SystemExit(1)

    analytics = GitHubAnalytics(token="", username=username, enable_rate_limiting=not disable_rate_limiting)

    if not output_file and output_dir:
        from github_analyitics.reporting.report_paths import default_xlsx_path

        output_file = str(default_xlsx_path("github_analytics.xlsx", base_dir=output_dir))

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


if __name__ == "__main__":
    main()
