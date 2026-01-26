# GitHub Analytics Tool - User Guide

## Quick Start

### Step 1: Setup

1. Install Python 3.7 or higher
2. Clone this repository
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Step 2: Configure GitHub Access

1. Create a GitHub Personal Access Token at https://github.com/settings/tokens
2. Required scopes: `repo` and `read:user`
3. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
4. Edit `.env` and add your credentials:
   ```
   GITHUB_TOKEN=your_actual_token_here
   GITHUB_USERNAME=your_github_username
   ```

### Step 3: Run Analytics

Basic usage (analyze all repositories):
```bash
python github_analytics.py
```

This will generate a timestamped Excel file with comprehensive statistics.

## Advanced Usage

### Analyze Specific Date Range

```bash
python github_analytics.py --start-date 2024-01-01 --end-date 2024-12-31
```

### Custom Output File

```bash
python github_analytics.py --output my_report.xlsx
```

### Combine Options

```bash
python github_analytics.py \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --output q1_2024_report.xlsx
```

## Understanding the Report

The generated Excel file contains five sheets:

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

- Your GitHub token is stored only in `.env` (not committed)
- The tool only reads data (no modifications)
- All data processing happens locally
- Generated reports are saved locally

## Troubleshooting

### "GITHUB_TOKEN not found"
Create a `.env` file with your token. See `.env.example` for format.

### "GITHUB_USERNAME not found"
Add your GitHub username to the `.env` file.

### "403 Forbidden" Errors
- Check that your token has correct scopes (`repo`, `read:user`)
- Ensure the token hasn't expired
- Verify you have access to the repositories

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

- Python: 3.7+
- PyGithub: 2.1.1+
- pandas: 2.0.0+
- openpyxl: 3.1.0+

## Contributing

Contributions are welcome! Areas for enhancement:
- Additional metrics (code review time, response time)
- More export formats (CSV, JSON, HTML)
- Visualization/charts
- Multi-threaded analysis for faster processing
- Caching to avoid re-fetching data
