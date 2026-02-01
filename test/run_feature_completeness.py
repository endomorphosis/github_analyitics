#!/usr/bin/env python3

"""Iteratively generate per-source timestamp reports and validate them.

This runs the integration-style spreadsheet tests (which generate XLSX outputs)
and, if requested, exports each generated XLSX into a persistent artifacts folder
for manual inspection.

Usage:
  /path/to/python test/run_feature_completeness.py
  /path/to/python test/run_feature_completeness.py --artifacts-dir data_reports/feature_checks

Environment:
  TIMESTAMP_TEST_ARTIFACTS_DIR  When set, integration tests copy generated XLSX
                               reports into this directory.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
import unittest


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    parser = argparse.ArgumentParser(description="Run feature completeness checks for timestamp generators.")
    parser.add_argument(
        "--artifacts-dir",
        default=None,
        help="Directory to store generated XLSX artifacts (optional).",
    )
    parser.add_argument(
        "--include-unit-tests",
        action="store_true",
        help="Also run unit tests under tests/ (default: only test/ integration suite).",
    )

    args = parser.parse_args()

    if args.artifacts_dir:
        root = Path(args.artifacts_dir).expanduser().resolve()
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = root / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        os.environ["TIMESTAMP_TEST_ARTIFACTS_DIR"] = str(out_dir)
        print(f"[artifacts] Writing XLSX outputs to: {out_dir}")

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.discover(start_dir=str(Path("test")), pattern="test_*.py"))

    if args.include_unit_tests:
        suite.addTests(loader.discover(start_dir=str(Path("tests")), pattern="test_*.py"))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
