# GitHub Analytics Tool - User Guide

## Quick Start

### Step 1: Setup

1. Install Python 3.10+
2. Clone this repository
3. Install dependencies:

```bash
pip install -r requirements.txt
```

### Step 2: Authenticate GitHub CLI

This project uses the GitHub CLI (`gh`) for authentication and API access.

```bash
gh --version
gh auth login
```

Optionally set `GITHUB_USERNAME` (otherwise it defaults to the authenticated `gh` user):

```bash
cp .env.example .env
# edit .env if you want to override GITHUB_USERNAME
```

### Step 3: Run Analytics

Basic usage (analyze all repositories):
```bash
python github_analytics.py
```

Unified timestamp suite (recommended; GitHub + local + ZFS into one workbook):

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite --allowed-users-file _allowed_users.txt
```

ZFS scanning defaults:

- By default, when ZFS is enabled, the suite scans all auto-detected `.zfs/snapshot` roots (exhaustive).
- To restrict to a single root for a deterministic run, use `--zfs-snapshot-root <path> --zfs-snapshot-root-only`.

## Advanced Usage

### Analyze Specific Date Range

```bash
python github_analytics.py --start-date 2024-01-01 --end-date 2024-12-31
```

### Custom Output File

```bash
python github_analytics.py --output my_report.xlsx
```

### PR comment timestamps

To include PR conversation comments and inline review comments (more API calls):

```bash
python github_analytics.py --include-pr-comments
```

### Combine Options

```bash
python github_analytics.py \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --output q1_2024_report.xlsx
```

## Understanding the Report

The generated Excel file contains six sheets:

### Sheet 1: Detailed Report

Daily breakdown per user with:
- **Date**: Activity date (YYYY-MM-DD)
- **User**: GitHub username
- **Commits**: Number of commits
- **Lines Added**: Lines of code added
- **Lines Deleted**: Lines of code deleted
- **Total Lines Changed**: Sum of additions and deletions
- **Files Modified**: Unique files changed (tracks activity even with sparse commits)
- **PRs Created**: Pull requests opened
- **PRs Merged**: Pull requests merged
- **Issues Created**: New issues opened
- **Issues Closed**: Issues resolved
- **Issue Comments**: Comments on issues
- **Estimated Hours**: Calculated work hours per day

### Sheet 2: User Summary

Totals per contributor:
- All metrics aggregated by user
- Includes total files modified
- Sorted by estimated hours (highest to lowest)
- Useful for identifying top contributors

### Sheet 3: Daily Summary

Totals per day:
- All metrics aggregated by date
- **Estimated Hours**: Total hours worked per day across all users
- **Active Users**: Number of unique contributors per day
- Useful for tracking team velocity and activity trends

### Sheet 4: PR Events
Timestamped pull request events (created/closed/merged), including repository, PR number, author, and URL.

### Sheet 5: Issue Events
Timestamped issue events (created/closed/comments), including repository, issue number, author, and URL.

### Sheet 6: User Timeline
Unified timestamped activity across PR and issue events.

## Metrics Explained

### Commits
Total number of commits made by each user.

### Lines of Code
- **Lines Added**: New code lines
- **Lines Deleted**: Removed code lines
- **Total Lines Changed**: Sum of additions and deletions

Includes both direct commits and pull request changes.

### Estimated Hours
Formula: `hours = (commits × 0.5) + (lines_changed / 30)`

This heuristic assumes:
- 0.5 hours per commit for overhead (planning, testing, reviewing)
- ~30 lines of code per hour (industry average)

**Note**: This is an estimate. Actual hours vary based on:
- Code complexity
- Review cycles
- Testing requirements
- Documentation needs

### Pull Requests
- **PRs Created**: Total PRs opened
- **PRs Merged**: Successfully merged PRs

### Issues
- **Issues Created**: New issues opened
- **Issues Closed**: Issues resolved
- **Issue Comments**: Comments on any issues

## Data Sources

The tool analyzes:
1. **Commit History**: All commits across all repositories
2. **File Modifications**: Tracks when files were changed via commit timestamps (helps identify activity even with sparse commits)
3. **Pull Requests**: Open, closed, and merged PRs
4. **Issues**: Open and closed issues with comments
5. **Statistics**: Line changes from commit stats

## Privacy & Security

- Authentication is handled by the GitHub CLI (`gh`) via `gh auth login`
- The tool only reads data (no modifications)
- All data processing happens locally
- Generated reports are saved locally

## Troubleshooting

### "gh is not authenticated"
Run: `gh auth login`

### "GITHUB_USERNAME not found"
Add your GitHub username to the `.env` file.

### "403 Forbidden" Errors
- Verify you have access to the repositories
- Re-run `gh auth login` if your auth is stale

### Rate Limiting
GitHub API has rate limits:
- Authenticated: 5,000 requests/hour
- For large repositories, the tool may hit rate limits
- If this happens, wait an hour and resume

### Empty Report
- Verify the username is correct
- Ensure you have repositories in your namespace
- Check date range (if specified)

## Performance Tips

1. **Date Ranges**: Use `--start-date` and `--end-date` to limit analysis
2. **Smaller Repos**: Analysis is faster for repositories with less history
3. **Network**: A stable, fast internet connection helps
4. **Patience**: Large organizations with many repos may take several minutes

### Performance tips for the unified timestamp suite

The unified timestamp suite (`python -m github_analyitics.timestamp_audit.timestamp_suite`) can become very large (millions of events) when ZFS is enabled and `--zfs-granularity file` is used.

Recommended options:

- DuckDB is enabled by default (reduces memory pressure and exports to Excel in chunks). Use `--no-duckdb` to disable.
- `--local-workers N`: Parallelize local repo scanning (processes). Start with `N=2..4`.
- `--zfs-root-workers N`: Parallelize scanning across ZFS snapshot roots (threads). Start with `N=1..2`.
- `--zfs-git-workers N`: When `--zfs-granularity file`, parallelize per-file `git log` attribution (threads). Start with `N=2..4`.
- `--zfs-git-max-inflight N`: Global cap across all roots for concurrent per-file `git log` subprocesses (prevents overload). Start with `N=2..8`.

Optional native filesystem source (`fs`):

- Enable with `--sources ... ,fs`
- (Default) `fs` is enabled in the suite’s default `--sources` value; remove it by passing `--sources github,local,zfs`.
- Linux/Unix: scans ext2/ext3/ext4 roots; Windows: scans NTFS roots
- `--fs-root PATH` (repeatable) controls what gets scanned; if omitted, defaults to `--repos-path` base
- `--fs-max-files N` is handy for quick smoke checks

Example:

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite \
  --allowed-users-file _allowed_users.txt \
  --sources local,zfs \
  --use-duckdb \
  --local-workers 4 \
  --zfs-root-workers 2 \
  --zfs-git-workers 4 \
  --zfs-git-max-inflight 4
```

If your machine becomes sluggish or ZFS scanning starts timing out, reduce `--zfs-git-max-inflight` first.

## Examples

See `example_usage.py` for:
- Basic analysis
- Date range filtering
- Custom data processing
- Quarterly reports

Run examples:
```bash
python example_usage.py
```

## Support

For issues or questions:
1. Check this user guide
2. Review `README.md`
3. Examine `example_usage.py`
4. Run unit tests: `python test_github_analytics.py`

## Version Information

- Python: 3.10+
- pandas: 2.0.0+
- openpyxl: 3.1.0+

## Contributing

Contributions are welcome! Areas for enhancement:
- Additional metrics (code review time, response time)
- More export formats (CSV, JSON, HTML)
- Visualization/charts
- Multi-threaded analysis for faster processing
- Caching to avoid re-fetching data
