# GitHub Analytics

A Python tool that analyzes GitHub repository activity to track commits, lines of code, and estimated work hours per user with daily breakdowns.

## Features

- **Comprehensive Analysis**: Analyzes commits, pull requests, issues, and comments across all repositories in a user's namespace
- **API Rate Limiting**: Automatic rate limit handling with exponential backoff to prevent hitting GitHub's 5000 calls/hour limit
- **Repository Filtering**: Include/exclude specific repositories or filter by user contributions
- **File Modification Tracking**: Tracks file modification timestamps to identify activity even with sparse commits
- **Per-User Statistics**: Tracks individual contributor activity and performance
- **Daily Breakdown**: Provides day-by-day activity metrics with estimated hours per day
- **Work Hour Estimation**: Calculates estimated hours based on commits and lines of code changed
- **Excel Reports**: Generates detailed spreadsheets with multiple summary views

## Installation

1. Clone this repository:
```bash
git clone https://github.com/endomorphosis/github_analyitics.git
cd github_analyitics
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

Alternatively, use the bootstrap installer (best-effort installs required CLI tools like `gh` and `git`, and Python deps into `.venv`):

```bash
./install.sh
```

3. Install and authenticate GitHub CLI (`gh`):
```bash
gh --version
gh auth login
```
On Linux, you can optionally run the CLI-only installer: `./scripts/install_cli_tools.sh`

4. (Optional) Configure environment variables:
- `GITHUB_USERNAME` (defaults to the authenticated `gh` user)

## Usage

### Unified timestamp suite (recommended)

If you want *all timestamps* in one workbook (GitHub API events + local git commit/file events, with optional working-tree and ZFS snapshot mtimes), run:

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite
```

This writes a workbook to `data_reports/<timestamp>/github_analytics_timestamps_suite.xlsx` by default.

Defaults (no flags):

- DuckDB export is enabled by default (faster + lower memory). Use `--no-duckdb` to disable.
- Native filesystem scan (source `fs`) is enabled by default.

ZFS note: when ZFS is enabled (default), the suite auto-detects and scans all visible `.zfs/snapshot` roots by default. If you want to restrict ZFS scanning to a single root for a deterministic run, pass `--zfs-snapshot-root <path> --zfs-snapshot-root-only`.

Native filesystem note: you can optionally include a filesystem-rooted scan (source `fs`). This is OS-aware:

- Linux/Unix: scans ext2/ext3/ext4 roots
- Windows: scans NTFS roots

Enable it with `--sources ... ,fs`. If you don’t pass `--fs-root`, it defaults to scanning the `--repos-path` base.

With a date range:

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite --start-date 2024-01-01 --end-date 2024-12-31
```

Convenience script:

```bash
./run_timestamp_suite.sh
```

### Performance & scaling

If you’re scanning many local repos and/or large ZFS snapshot trees, these options help speed things up without hammering GitHub (they only affect local + ZFS work):

- DuckDB is enabled by default. Use `--no-duckdb` to disable.
- `--duckdb-path PATH`: Override the default `<output>.duckdb` path.
- `--local-workers N`: Scans local repositories in parallel using **processes**.
- `--zfs-root-workers N`: Scans multiple ZFS snapshot roots in parallel using **threads**.
- `--zfs-git-workers N`: When `--zfs-granularity file`, runs per-file `git log -1 -- <file>` attribution concurrently (threads).
- `--zfs-git-max-inflight N`: Global cap on concurrent per-file `git log` subprocesses across all roots (useful when combining the above).

Suggested starting point for big ZFS runs (adjust to your machine):

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite \
  --sources local,zfs \
  --use-duckdb \
  --local-workers 4 \
  --zfs-root-workers 2 \
  --zfs-git-workers 4 \
  --zfs-git-max-inflight 4
```

Notes:

- Total concurrent per-file `git log` work is approximately bounded by `--zfs-git-max-inflight` (if set), otherwise it can approach `--zfs-root-workers × --zfs-git-workers`.
- If you see the system get sluggish, reduce `--zfs-git-max-inflight` first.

Native filesystem (`fs`) options:

- `--fs-root PATH`: Add one or more scan roots (repeatable)
- `--fs-exclude NAME`: Exclude directory names (repeatable)
- `--fs-max-files N`: Cap emitted rows per root (useful for quick tests)
- `--fs-progress-every-seconds N`: Heartbeat interval during large scans

### Quick Start Script

For convenience, use the provided script:

```bash
# Run for all time
./run_report.sh

# Run for a specific date range
./run_report.sh 2024-01-01 2024-12-31
```

### Basic Usage

Run the tool with default settings (analyzes all repositories for the configured user):

```bash
python -m github_analyitics.reporting.github_analytics
```

This will generate an Excel file `github_analytics_YYYYMMDD_HHMMSS.xlsx` with comprehensive statistics.

### Command Line Options

**Date Filtering:**
```bash
python -m github_analyitics.reporting.github_analytics --start-date 2024-01-01 --end-date 2024-12-31
```

**Repository Filtering:**
```bash
# Only analyze specific repositories
python github_analytics.py --include-repos repo1,repo2,repo3

# Exclude specific repositories
python github_analytics.py --exclude-repos test-repo,demo-repo

# Only analyze repos where a specific user has contributed
python github_analytics.py --filter-by-user username
```

**Output Options:**
```bash
python github_analytics.py --output my_report.xlsx
```

**Rate Limiting:**
```bash
# Disable automatic rate limiting (not recommended for large accounts)
python github_analytics.py --disable-rate-limiting
```

**Combined Example:**
```bash
python github_analytics.py \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --exclude-repos test-repo,demo \
  --filter-by-user contributor-name \
  --output quarterly_report.xlsx
```

**Help:**
```bash
python github_analytics.py --help
```

## Output

The generated Excel file contains six sheets:

### 1. Detailed Report
Daily breakdown per user including:
- Date and username
- Number of commits
- Lines added/deleted
- Total lines changed
- Files modified (tracked via commit history)
- Pull requests created/merged
- Issues created/closed
- Issue comments
- Estimated work hours per day

### 2. User Summary
Aggregated statistics per user:
- Total commits
- Total lines changed
- Total files modified
- Total PRs and issues
- Total estimated hours
- Sorted by estimated hours (highest to lowest)

### 3. Daily Summary
Aggregated statistics per day:
- Total activity across all users
- Total estimated hours per day
- Number of active users
- Daily trends in commits, PRs, and issues

### 4. PR Events
Timestamped pull request events (created/closed/merged) with repository, PR number, author, and URL.

### 5. Issue Events
Timestamped issue events (created/closed/comments) with repository, issue number, author, and URL.

### 6. User Timeline
Unified timestamped activity across PR and issue events.

## Metrics Explained

### Lines of Code
- Counts both additions and deletions from commits and pull requests
- Provides insight into code volume and refactoring activity

### Files Modified
- Tracks unique files changed per day via commit history
- Helps identify activity patterns even when commits are sparse
- Useful for understanding which files are actively being worked on

### Estimated Hours
Calculated using a heuristic formula:
- Base: 0.5 hours per commit (for planning, testing, reviewing)
- Coding time: Lines changed / 30 (assuming ~30 lines per hour)
- Formula: `hours = (commits × 0.5) + (lines_changed / 30)`
- Aggregated per day in the Daily Summary sheet

This provides a rough estimate of development effort. Actual hours may vary based on code complexity.

## Requirements

- Python 3.7+
- GitHub Personal Access Token with appropriate permissions
- Dependencies listed in requirements.txt

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

See LICENSE file for details.
