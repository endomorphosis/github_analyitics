#!/usr/bin/env python3

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd

from github_analyitics.timestamp_audit.local_git_analytics import LocalGitAnalytics


def _require_git() -> bool:
    return shutil.which("git") is not None


def _artifact_dir() -> Path | None:
    value = (os.getenv("TIMESTAMP_TEST_ARTIFACTS_DIR") or "").strip()
    if not value:
        return None
    path = Path(value).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _export_artifact(path: Path, name: str) -> None:
    dest_dir = _artifact_dir()
    if dest_dir is None:
        return
    dest = dest_dir / name
    try:
        shutil.copy2(path, dest)
    except Exception:
        # Never fail tests just because export didn't work.
        return


def _run(cmd: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return (result.stdout or "").strip()


def _create_git_repo(base_dir: Path, name: str, *, commit_dt: datetime) -> Path:
    repo = base_dir / name
    repo.mkdir(parents=True, exist_ok=True)

    _run(["git", "init"], cwd=repo)
    _run(["git", "config", "user.name", "Test User"], cwd=repo)
    _run(["git", "config", "user.email", "test@example.com"], cwd=repo)

    (repo / "hello.txt").write_text("hello\n", encoding="utf-8")
    _run(["git", "add", "hello.txt"], cwd=repo)

    env = os.environ.copy()
    iso = commit_dt.replace(tzinfo=timezone.utc).isoformat()
    env["GIT_AUTHOR_DATE"] = iso
    env["GIT_COMMITTER_DATE"] = iso
    _run(["git", "commit", "-m", "initial"], cwd=repo, env=env)

    # Second commit with a modification for a file event
    (repo / "hello.txt").write_text("hello world\n", encoding="utf-8")
    _run(["git", "add", "hello.txt"], cwd=repo)
    env2 = os.environ.copy()
    iso2 = (commit_dt.replace(tzinfo=timezone.utc)).isoformat()
    env2["GIT_AUTHOR_DATE"] = iso2
    env2["GIT_COMMITTER_DATE"] = iso2
    _run(["git", "commit", "-m", "modify"], cwd=repo, env=env2)

    return repo


def _git_commit_with_message(
    repo: Path,
    *,
    filename: str,
    content: str,
    author_name: str,
    author_email: str,
    commit_dt: datetime,
    message: str,
) -> None:
    (repo / filename).write_text(content, encoding="utf-8")
    _run(["git", "add", filename], cwd=repo)

    msg_path = repo / "_msg.txt"
    msg_path.write_text(message, encoding="utf-8")

    env = os.environ.copy()
    iso = commit_dt.replace(tzinfo=timezone.utc).isoformat()
    env["GIT_AUTHOR_DATE"] = iso
    env["GIT_COMMITTER_DATE"] = iso
    env["GIT_AUTHOR_NAME"] = author_name
    env["GIT_AUTHOR_EMAIL"] = author_email
    env["GIT_COMMITTER_NAME"] = author_name
    env["GIT_COMMITTER_EMAIL"] = author_email

    _run(["git", "commit", "-F", str(msg_path)], cwd=repo, env=env)


def _read_xlsx_sheets(path: Path) -> set[str]:
    xls = pd.ExcelFile(path)
    return set(xls.sheet_names)


def _assert_timestamp_column_parseable(testcase: unittest.TestCase, df: pd.DataFrame, column: str) -> None:
    testcase.assertIn(column, df.columns)
    parsed = pd.to_datetime(df[column], utc=True, errors="coerce")
    testcase.assertFalse(parsed.isna().all(), f"All timestamps in {column} failed to parse")


@contextmanager
def _argv(argv: list[str]) -> Iterator[None]:
    old = sys.argv[:]
    try:
        sys.argv = argv
        yield
    finally:
        sys.argv = old


class TestTimestampSpreadsheets(unittest.TestCase):
    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_local_git_analytics_generates_expected_spreadsheet(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 1, 12, 0, 0))
            out = base / "local_git.xlsx"

            analytics = LocalGitAnalytics(str(base))
            analytics.generate_report(
                output_file=str(out),
                start_date=None,
                end_date=None,
                include_repos=None,
                exclude_repos=None,
                max_depth=2,
                use_session_estimation=False,
                allowed_users=None,
                include_working_tree_timestamps=False,
                working_tree_user=None,
                working_tree_excludes=None,
            )

            _export_artifact(out, "local_git.xlsx")

            sheets = _read_xlsx_sheets(out)
            for expected in {
                "Detailed Report",
                "User Summary",
                "Daily Summary",
                "Commit Events",
                "File Events",
                "User Timeline",
            }:
                self.assertIn(expected, sheets)

            commits = pd.read_excel(out, sheet_name="Commit Events")
            self.assertGreaterEqual(len(commits), 1)
            self.assertTrue((commits["repository"] == repo.name).any())
            _assert_timestamp_column_parseable(self, commits, "event_timestamp")

            files = pd.read_excel(out, sheet_name="File Events")
            self.assertGreaterEqual(len(files), 1)
            self.assertTrue((files["repository"] == repo.name).any())
            _assert_timestamp_column_parseable(self, files, "event_timestamp")

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_working_tree_timestamps_generates_spreadsheet(self):
        from github_analyitics.timestamp_audit import working_tree_timestamps as wtt

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 2, 12, 0, 0))
            out = base / "working_tree.xlsx"

            with _argv(
                [
                    "working_tree_timestamps",
                    "--base-path",
                    str(base),
                    "--output",
                    str(out),
                    "--user",
                    "TestUser",
                    "--max-depth",
                    "2",
                ]
            ):
                wtt.main()

            _export_artifact(out, "working_tree.xlsx")

            sheets = _read_xlsx_sheets(out)
            self.assertIn("Working Tree Timestamps", sheets)

            df = pd.read_excel(out, sheet_name="Working Tree Timestamps")
            self.assertGreaterEqual(len(df), 1)
            _assert_timestamp_column_parseable(self, df, "event_timestamp")
            self.assertIn("repository", df.columns)
            self.assertIn("file", df.columns)

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_zfs_snapshot_git_timestamps_generates_spreadsheet(self):
        from github_analyitics.timestamp_audit import zfs_snapshot_git_timestamps as zfs

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            snap_root = base / "snapshots"
            snap_root.mkdir()

            snap1 = snap_root / "snap1"
            snap1.mkdir()
            _create_git_repo(snap1, "repo1", commit_dt=datetime(2026, 1, 3, 12, 0, 0))

            out = base / "zfs.xlsx"
            with _argv(
                [
                    "zfs_snapshot_git_timestamps",
                    "--snapshot-root",
                    str(snap_root),
                    "--output",
                    str(out),
                    "--user",
                    "TestUser",
                    "--max-depth",
                    "3",
                    "--granularity",
                    "file",
                    "--no-sudo",
                ]
            ):
                zfs.main()

            _export_artifact(out, "zfs_snapshot.xlsx")

            sheets = _read_xlsx_sheets(out)
            self.assertIn("ZFS Snapshot Timestamps", sheets)

            df = pd.read_excel(out, sheet_name="ZFS Snapshot Timestamps")
            self.assertGreaterEqual(len(df), 1)
            self.assertIn("snapshot", df.columns)
            self.assertIn("repository", df.columns)
            self.assertIn("file", df.columns)
            _assert_timestamp_column_parseable(self, df, "event_timestamp")

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_collect_all_timestamps_generates_spreadsheet(self):
        from github_analyitics.timestamp_audit import collect_all_timestamps as cat

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 4, 12, 0, 0))
            out = base / "collect_all.xlsx"

            allowed = base / "_allowed_users.txt"
            allowed.write_text("Test User\ntest@example.com\n", encoding="utf-8")

            # Force-disable ZFS auto-detection during test for determinism
            env = os.environ.copy()
            env["ZFS_SNAPSHOT_ROOT"] = str(base / "__nope__")

            with unittest.mock.patch.dict(os.environ, env, clear=False):
                with _argv(
                    [
                        "collect_all_timestamps",
                        "--repos-path",
                        str(base),
                        "--output",
                        str(out),
                        "--max-depth",
                        "2",
                        "--allowed-users-file",
                        str(allowed),
                        "--zfs-snapshot-root",
                        str(base / "__nope__"),
                    ]
                ):
                    cat.main()

                    _export_artifact(out, "collect_all_timestamps.xlsx")

            sheets = _read_xlsx_sheets(out)
            for expected in {"Detailed Report", "Commit Events", "File Events", "User Timeline"}:
                self.assertIn(expected, sheets)

            timeline = pd.read_excel(out, sheet_name="User Timeline")
            self.assertGreaterEqual(len(timeline), 1)
            _assert_timestamp_column_parseable(self, timeline, "event_timestamp")

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_timestamp_suite_local_source_generates_all_events(self):
        from github_analyitics.timestamp_audit import timestamp_suite as suite

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 5, 12, 0, 0))
            out = base / "suite.xlsx"

            allowed = base / "_allowed_users.txt"
            allowed.write_text("Test User\ntest@example.com\n", encoding="utf-8")

            with _argv(
                [
                    "timestamp_suite",
                    "--output",
                    str(out),
                    "--sources",
                    "local",
                    "--repos-path",
                    str(base),
                    "--max-depth",
                    "2",
                    "--allowed-users-file",
                    str(allowed),
                ]
            ):
                suite.main()

            _export_artifact(out, "timestamp_suite_local.xlsx")

            sheets = _read_xlsx_sheets(out)
            self.assertIn("All Events", sheets)
            self.assertIn("User Timeline", sheets)

            df = pd.read_excel(out, sheet_name="All Events")
            self.assertGreaterEqual(len(df), 1)
            self.assertIn("source", df.columns)
            self.assertTrue((df["source"] == "local_git").any())
            _assert_timestamp_column_parseable(self, df, "event_timestamp")

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_copilot_authored_commit_is_attributed_to_coauthor_invoker(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 6, 12, 0, 0))

            _git_commit_with_message(
                repo,
                filename="hello.txt",
                content="hello from copilot\n",
                author_name="GitHub Copilot",
                author_email="copilot@github.com",
                commit_dt=datetime(2026, 1, 6, 12, 5, 0),
                message=(
                    "copilot change\n\n"
                    "Co-authored-by: Real User <real@example.com>\n"
                ),
            )

            analytics = LocalGitAnalytics(str(base))
            analytics.analyze_all_repositories(max_depth=2)

            commit_events = analytics.commit_events or []
            file_events = analytics.file_events or []

            self.assertTrue(
                any(
                    (ev.get("author") == "Real User")
                    and (ev.get("attributed_user") == "Real User")
                    and (ev.get("copilot_involved") is True)
                    and (ev.get("invoker_source") == "co-author")
                    and (ev.get("raw_author") == "GitHub Copilot")
                    for ev in commit_events
                ),
                "Expected Copilot-authored commit to be attributed to co-author invoker",
            )
            self.assertTrue(
                any(
                    (ev.get("author") == "Real User")
                    and (ev.get("attributed_user") == "Real User")
                    and (ev.get("copilot_involved") is True)
                    and (ev.get("invoker_source") == "co-author")
                    and (ev.get("raw_author") == "GitHub Copilot")
                    for ev in file_events
                ),
                "Expected Copilot-authored file event to be attributed to co-author invoker",
            )

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_copilot_authored_commit_can_be_attributed_via_mapping_file(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 7, 12, 0, 0))

            mapping = base / "copilot_invokers.json"
            mapping.write_text('{"copilot@github.com": "Real User"}', encoding="utf-8")

            _git_commit_with_message(
                repo,
                filename="hello.txt",
                content="hello from copilot (mapped)\n",
                author_name="GitHub Copilot",
                author_email="copilot@github.com",
                commit_dt=datetime(2026, 1, 7, 12, 5, 0),
                message="copilot change (mapped)\n",
            )

            analytics = LocalGitAnalytics(str(base), copilot_invokers_path=str(mapping))
            analytics.analyze_all_repositories(max_depth=2)
            commit_events = analytics.commit_events or []

            self.assertTrue(
                any(
                    (ev.get("author") == "Real User")
                    and (ev.get("attributed_user") == "Real User")
                    and (ev.get("copilot_involved") is True)
                    and (ev.get("invoker_source") == "mapping")
                    and (ev.get("raw_author") == "GitHub Copilot")
                    for ev in commit_events
                ),
                "Expected Copilot-authored commit to be attributed via mapping",
            )

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_bot_authored_commit_with_copilot_trailer_is_attributed_to_human(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 8, 12, 0, 0))

            _git_commit_with_message(
                repo,
                filename="hello.txt",
                content="hello from bot\n",
                author_name="github-actions[bot]",
                author_email="github-actions[bot]@users.noreply.github.com",
                commit_dt=datetime(2026, 1, 8, 12, 5, 0),
                message=(
                    "bot change\n\n"
                    "Co-authored-by: GitHub Copilot <copilot@github.com>\n"
                    "Co-authored-by: Real User <real@example.com>\n"
                ),
            )

            analytics = LocalGitAnalytics(str(base))
            analytics.analyze_all_repositories(max_depth=2)
            commit_events = analytics.commit_events or []

            self.assertTrue(
                any(
                    (ev.get("author") == "Real User")
                    and (ev.get("attributed_user") == "Real User")
                    and (ev.get("copilot_involved") is True)
                    and (ev.get("invoker_source") == "co-author")
                    and (ev.get("raw_author") == "github-actions[bot]")
                    for ev in commit_events
                ),
                "Expected bot-authored Copilot commit to be attributed to human co-author",
            )

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_timestamp_suite_all_events_preserves_copilot_invoker_attribution(self):
        from github_analyitics.timestamp_audit import timestamp_suite as suite

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 9, 12, 0, 0))

            _git_commit_with_message(
                repo,
                filename="hello.txt",
                content="suite copilot\n",
                author_name="GitHub Copilot",
                author_email="copilot@github.com",
                commit_dt=datetime(2026, 1, 9, 12, 5, 0),
                message=(
                    "suite copilot change\n\n"
                    "Co-authored-by: Real User <real@example.com>\n"
                ),
            )

            allowed = base / "_allowed_users.txt"
            allowed.write_text("Real User\nreal@example.com\n", encoding="utf-8")

            out = base / "suite.xlsx"
            with _argv(
                [
                    "timestamp_suite",
                    "--output",
                    str(out),
                    "--sources",
                    "local",
                    "--repos-path",
                    str(base),
                    "--max-depth",
                    "2",
                    "--allowed-users-file",
                    str(allowed),
                ]
            ):
                suite.main()

            df = pd.read_excel(out, sheet_name="All Events")
            self.assertIn("attributed_user", df.columns)
            self.assertIn("author", df.columns)
            self.assertIn("copilot_involved", df.columns)
            self.assertIn("invoker_source", df.columns)
            self.assertIn("raw_author", df.columns)
            self.assertTrue(
                ((df["author"] == "Real User")
                 & (df["raw_author"] == "GitHub Copilot")
                 & (df["attributed_user"] == "Real User")
                 & (df["copilot_involved"] == True)
                 & (df["invoker_source"] == "co-author")).any()
            )

    @unittest.skipUnless(_require_git(), "git is required for integration timestamp tests")
    def test_zfs_snapshot_repo_events_attribute_copilot_to_invoker(self):
        from github_analyitics.timestamp_audit.collect_all_timestamps import collect_zfs_events

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            repo = _create_git_repo(base, "repo1", commit_dt=datetime(2026, 1, 10, 12, 0, 0))

            _git_commit_with_message(
                repo,
                filename="hello.txt",
                content="zfs copilot\n",
                author_name="GitHub Copilot",
                author_email="copilot@github.com",
                commit_dt=datetime(2026, 1, 10, 12, 5, 0),
                message=(
                    "zfs copilot change\n\n"
                    "Co-authored-by: Real User <real@example.com>\n"
                ),
            )

            snapshot_root = base / "pool" / ".zfs" / "snapshot"
            snap = snapshot_root / "2026-01-10T12-10-00Z"
            snap.mkdir(parents=True, exist_ok=True)

            # Copy repo into a fake snapshot directory.
            shutil.copytree(repo, snap / "repo1", dirs_exist_ok=True)

            rows = collect_zfs_events(
                snapshot_root=snapshot_root,
                user="unknown",
                max_depth=4,
                excludes=[],
                scan_relative_to_mountpoint=None,
                start_date=None,
                end_date=None,
                snapshots_limit=0,
                granularity="repo_index",
                max_seconds=None,
            )

            self.assertGreaterEqual(len(rows), 1)
            self.assertTrue(
                any(
                    (r.get("source") == "zfs_snapshot")
                    and (r.get("author") == "Real User")
                    and (r.get("raw_author") == "GitHub Copilot")
                    and (r.get("attributed_user") == "Real User")
                    and (r.get("copilot_involved") is True)
                    and (r.get("invoker_source") == "co-author")
                    for r in rows
                ),
                "Expected ZFS snapshot repo-level events to attribute Copilot to human invoker",
            )

    def test_github_analytics_report_can_be_written_from_mocked_data(self):
        from github_analyitics.reporting.github_analytics import GitHubAnalytics

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            out = base / "github_mock.xlsx"

            # Build a minimal Detailed Report DF that matches expectations.
            detailed = pd.DataFrame(
                [
                    {
                        "date": "2026-01-01",
                        "user": "alice",
                        "commits": 1,
                        "lines_added": 10,
                        "lines_deleted": 0,
                        "total_lines_changed": 10,
                        "files_modified": 1,
                        "prs_created": 0,
                        "prs_merged": 0,
                        "issues_created": 0,
                        "issues_closed": 0,
                        "issue_comments": 0,
                        "estimated_hours": 1.0,
                    }
                ]
            )

            analytics = GitHubAnalytics("fake", "test_user")

            def fake_analyze_all_repositories(*args, **kwargs):
                # Populate some event lists so the event sheets have data
                analytics.commit_events = [
                    {
                        "repository": "owner/repo",
                        "commit": "deadbeef",
                        "author": "alice",
                        "event_timestamp": "2026-01-01T12:00:00Z",
                        "subject": "test",
                    }
                ]
                analytics.pr_events = [
                    {
                        "repository": "owner/repo",
                        "number": 1,
                        "title": "PR",
                        "author": "alice",
                        "event_type": "created",
                        "event_timestamp": "2026-01-01T12:00:00Z",
                        "url": "https://example.invalid/pr/1",
                    }
                ]
                analytics.issue_events = [
                    {
                        "repository": "owner/repo",
                        "number": 2,
                        "title": "Issue",
                        "author": "alice",
                        "event_type": "created",
                        "event_timestamp": "2026-01-01T12:00:00Z",
                        "url": "https://example.invalid/issue/2",
                    }
                ]
                return detailed

            analytics.analyze_all_repositories = fake_analyze_all_repositories  # type: ignore[assignment]

            analytics.generate_report(
                output_file=str(out),
                start_date=None,
                end_date=None,
                include_repos=None,
                exclude_repos=None,
                filter_by_user_contribution=None,
                skip_file_modifications=True,
                skip_commit_stats=True,
                restrict_to_collaborators=False,
                restrict_to_owner_namespace=True,
                fast_mode=False,
                include_pr_comments=False,
                include_pr_review_comments=True,
                include_pr_review_events=False,
                include_issue_pr_comments=False,
            )

            _export_artifact(out, "github_analytics_mock.xlsx")

            sheets = _read_xlsx_sheets(out)
            # Core report sheets
            for expected in {"Detailed Report", "User Summary", "Daily Summary"}:
                self.assertIn(expected, sheets)

            # Event sheets should exist when populated
            for expected in {"PR Events", "Issue Events"}:
                self.assertIn(expected, sheets)

            pr_df = pd.read_excel(out, sheet_name="PR Events")
            self.assertGreaterEqual(len(pr_df), 1)
            _assert_timestamp_column_parseable(self, pr_df, "event_timestamp")


if __name__ == "__main__":
    unittest.main()
