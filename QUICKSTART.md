# Quick Start Guide

## Generate Your First Report in 3 Steps

### Step 1: Authenticate GitHub CLI

This project uses the GitHub CLI (`gh`) for authentication and API access.

```bash
gh --version
gh auth login
```

Optionally set `GITHUB_USERNAME` (otherwise it defaults to the authenticated `gh` user):

```bash
cp .env.example .env
# (optional) edit .env and set GITHUB_USERNAME
```

### Step 2: Install Dependencies

```bash
./install.sh
```

This will (best-effort) install required CLI tools (like `gh` and `git`) and install Python dependencies into `.venv`.

### Step 3: Run the Report

```bash
# Option A: Use the convenience script (easiest)
./run_report.sh

# Option A2: Unified timestamp suite (recommended for timestamp analysis)
./run_timestamp_suite.sh

# Option B: Run Python directly
python -m github_analyitics.reporting.github_analytics

# Option B2: Unified timestamp suite
python -m github_analyitics.timestamp_audit.timestamp_suite

Outputs are written to `data_reports/<timestamp>/` by default.

# Option C: For a specific date range
./run_report.sh 2024-01-01 2024-12-31

# Option C2: Unified timestamp suite for a date range
./run_timestamp_suite.sh 2024-01-01 2024-12-31

# Option D: Filter repositories
python -m github_analyitics.reporting.github_analytics --include-repos repo1,repo2

# Option E: Exclude test repositories
python -m github_analyitics.reporting.github_analytics --exclude-repos test-repo,demo-repo

# Option F: Only repos where user contributed
python -m github_analyitics.reporting.github_analytics --filter-by-user username
```

### Scaling the unified timestamp suite

For large local + ZFS runs, prefer DuckDB and bounded parallelism:

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite \
	--use-duckdb \
	--local-workers 4 \
	--zfs-root-workers 2 \
	--zfs-git-workers 4 \
	--zfs-git-max-inflight 4
```

If the machine gets sluggish, reduce `--zfs-git-max-inflight` first.

Optional: include native filesystem timestamps (source `fs`):

```bash
python -m github_analyitics.timestamp_audit.timestamp_suite \
	--sources local,zfs,fs \
	--fs-root "$HOME" \
	--fs-max-files 200000
```

## Advanced Features

### API Rate Limiting
The tool automatically handles GitHub's API rate limits (5000 calls/hour):
- Monitors remaining API calls
- Implements exponential backoff when approaching limits
- Automatically waits and retries on rate limit errors

### Repository Filtering
Control which repositories to analyze:
- `--include-repos`: Only analyze specific repositories
- `--exclude-repos`: Skip specific repositories
- `--filter-by-user`: Only include repos where a user has contributed

## What You'll Get

The tool will generate an Excel file (e.g., `github_analytics_20260123_053716.xlsx`) with three sheets:

1. **Detailed Report** - Per-user, per-day breakdown
2. **User Summary** - Total stats per contributor
3. **Daily Summary** - Total stats per day

## Example Output

The report includes:
- Commits per user per day
- Lines of code added/deleted
- Files modified
- Pull requests created/merged
- Issues created/closed/commented
- Estimated work hours

## Troubleshooting

**"gh is not authenticated"**
- Run: `gh auth login`

**"Module not found"**
- Run: `pip install -r requirements.txt`

**Rate limiting**
- GitHub API allows 5,000 requests/hour for authenticated users
- For large organizations, you may need to wait between runs

## Next Steps

- See `README.md` for full documentation
- See `USER_GUIDE.md` for detailed usage instructions
- See `example_usage.py` for advanced usage patterns
