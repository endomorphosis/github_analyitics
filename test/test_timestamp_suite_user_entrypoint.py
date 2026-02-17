#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path


# Allow running this test module directly (without installing the package).
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


class TestTimestampSuiteUserEntrypoint(unittest.TestCase):
    def test_injects_github_username_flag(self):
        from github_analyitics.timestamp_audit.timestamp_suite_user import build_forwarded_argv

        argv = ["github-analyitics-timestamps-user", "octocat", "--sources", "local"]
        forwarded = build_forwarded_argv(argv)

        self.assertEqual(
            forwarded,
            [
                "github-analyitics-timestamps-user",
                "--github-username",
                "octocat",
                "--sources",
                "local",
            ],
        )

    def test_does_not_override_existing_github_username(self):
        from github_analyitics.timestamp_audit.timestamp_suite_user import build_forwarded_argv

        argv = ["prog", "--github-username", "someone", "--sources", "github"]
        forwarded = build_forwarded_argv(argv)
        self.assertEqual(forwarded, argv)

    def test_errors_when_missing_username(self):
        from github_analyitics.timestamp_audit.timestamp_suite_user import build_forwarded_argv

        with self.assertRaises(SystemExit):
            build_forwarded_argv(["prog"])

        with self.assertRaises(SystemExit):
            build_forwarded_argv(["prog", "--sources", "local"])


if __name__ == "__main__":
    unittest.main()
