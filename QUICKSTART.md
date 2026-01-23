# Quick Start Guide

## Generate Your First Report in 3 Steps

### Step 1: Set Up Credentials

```bash
# Copy the example configuration file
cp .env.example .env

# Edit .env with your information
# Replace 'your_github_token_here' with your actual token
# Replace 'your_username_here' with your GitHub username
```

To create a GitHub token:
1. Go to https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Select scopes: `repo` and `read:user`
4. Copy the generated token

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Run the Report

```bash
# Option A: Use the convenience script (easiest)
./run_report.sh

# Option B: Run Python directly
python github_analytics.py

# Option C: For a specific date range
./run_report.sh 2024-01-01 2024-12-31

# Option D: Filter repositories
python github_analytics.py --include-repos repo1,repo2

# Option E: Exclude test repositories
python github_analytics.py --exclude-repos test-repo,demo-repo

# Option F: Only repos where user contributed
python github_analytics.py --filter-by-user username
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

**"GITHUB_TOKEN not found"**
- Make sure you created `.env` from `.env.example`
- Add your GitHub token to the `.env` file

**"Module not found"**
- Run: `pip install -r requirements.txt`

**Rate limiting**
- GitHub API allows 5,000 requests/hour for authenticated users
- For large organizations, you may need to wait between runs

## Next Steps

- See `README.md` for full documentation
- See `USER_GUIDE.md` for detailed usage instructions
- See `example_usage.py` for advanced usage patterns
