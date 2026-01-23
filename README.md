# GitHub Analytics

A Python tool that analyzes GitHub repository activity to track commits, lines of code, and estimated work hours per user with daily breakdowns.

## Features

- **Comprehensive Analysis**: Analyzes commits, pull requests, issues, and comments across all repositories in a user's namespace
- **Per-User Statistics**: Tracks individual contributor activity and performance
- **Daily Breakdown**: Provides day-by-day activity metrics
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

3. Create a GitHub Personal Access Token:
   - Go to https://github.com/settings/tokens
   - Click "Generate new token" (classic)
   - Select scopes: `repo` and `read:user`
   - Copy the generated token

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env and add your token and username
```

## Usage

### Basic Usage

Run the tool with default settings (analyzes all repositories for the configured user):

```bash
python github_analytics.py
```

This will generate an Excel file `github_analytics_YYYYMMDD_HHMMSS.xlsx` with comprehensive statistics.

### Command Line Options

Analyze data for a specific date range:

```bash
python github_analytics.py --start-date 2024-01-01 --end-date 2024-12-31
```

Specify custom output file:

```bash
python github_analytics.py --output my_report.xlsx
```

Combine options:

```bash
python github_analytics.py --start-date 2024-01-01 --end-date 2024-12-31 --output q1_2024.xlsx
```

## Output

The generated Excel file contains three sheets:

### 1. Detailed Report
Daily breakdown per user including:
- Date and username
- Number of commits
- Lines added/deleted
- Total lines changed
- Pull requests created/merged
- Issues created/closed
- Issue comments
- Estimated work hours

### 2. User Summary
Aggregated statistics per user:
- Total commits
- Total lines changed
- Total PRs and issues
- Total estimated hours
- Sorted by estimated hours (highest to lowest)

### 3. Daily Summary
Aggregated statistics per day:
- Total activity across all users
- Number of active users
- Daily trends in commits, PRs, and issues

## Metrics Explained

### Lines of Code
- Counts both additions and deletions from commits and pull requests
- Provides insight into code volume and refactoring activity

### Estimated Hours
Calculated using a heuristic formula:
- Base: 0.5 hours per commit (for planning, testing, reviewing)
- Coding time: Lines changed / 30 (assuming ~30 lines per hour)
- Formula: `hours = (commits × 0.5) + (lines_changed / 30)`

This provides a rough estimate of development effort. Actual hours may vary based on code complexity.

## Requirements

- Python 3.7+
- GitHub Personal Access Token with appropriate permissions
- Dependencies listed in requirements.txt

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

See LICENSE file for details.
