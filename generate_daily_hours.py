import os
import subprocess
from datetime import datetime, timedelta, timezone
import pandas as pd

from github_analytics import GitHubAnalytics


def get_gh_credentials() -> tuple[str, str]:
    try:
        token = subprocess.check_output(["gh", "auth", "token"], text=True).strip()
        username = subprocess.check_output(["gh", "api", "user", "-q", ".login"], text=True).strip()
    except Exception as exc:
        raise SystemExit(f"Failed to read GitHub credentials from gh CLI: {exc}")

    if not token or not username:
        raise SystemExit("Missing GitHub credentials from gh CLI.")

    return token, username


def main() -> None:
    token, username = get_gh_credentials()

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=90)

    output_file = os.path.join(os.getcwd(), "github_analytics_latest.xlsx")

    analytics = GitHubAnalytics(token, username, enable_rate_limiting=True)
    analytics.generate_report(
        output_file=output_file,
        start_date=start_date,
        end_date=end_date,
        include_repos=None,
        exclude_repos=None,
        filter_by_user_contribution=None,
        skip_file_modifications=True,
        fast_mode=True,
    )

    if os.path.exists(output_file):
        daily = pd.read_excel(output_file, sheet_name="Daily Summary")
        if daily.empty:
            print("No rows in Daily Summary.")
            return

        daily = daily.sort_values("date", ascending=True)
        print("DAILY_HOURS_START")
        for _, row in daily.iterrows():
            print(f"{row['date']}: {row['estimated_hours']}")
        print("DAILY_HOURS_END")
    else:
        print("Report file not created.")


if __name__ == "__main__":
    main()
