#!/usr/bin/env python3
"""
Example usage of GitHub Analytics tool.

This script demonstrates different ways to use the github_analytics module.
"""

import os
from datetime import datetime, timedelta
from github_analytics import GitHubAnalytics
from dotenv import load_dotenv


def example_basic_analysis():
    """Example: Basic analysis of all repositories."""
    print("\n" + "="*60)
    print("Example 1: Basic Analysis")
    print("="*60)
    
    # Load credentials
    load_dotenv()
    token = os.getenv('GITHUB_TOKEN')
    username = os.getenv('GITHUB_USERNAME')
    
    if not token or not username:
        print("Please set GITHUB_TOKEN and GITHUB_USERNAME in .env file")
        return
    
    # Create analytics instance
    analytics = GitHubAnalytics(token, username)
    
    # Generate report for all time
    analytics.generate_report(output_file='full_history_report.xlsx')


def example_date_range_analysis():
    """Example: Analysis for a specific date range."""
    print("\n" + "="*60)
    print("Example 2: Date Range Analysis")
    print("="*60)
    
    load_dotenv()
    token = os.getenv('GITHUB_TOKEN')
    username = os.getenv('GITHUB_USERNAME')
    
    if not token or not username:
        print("Please set GITHUB_TOKEN and GITHUB_USERNAME in .env file")
        return
    
    analytics = GitHubAnalytics(token, username)
    
    # Analyze last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"Analyzing from {start_date.date()} to {end_date.date()}")
    analytics.generate_report(
        output_file='last_30_days.xlsx',
        start_date=start_date,
        end_date=end_date
    )


def example_custom_analysis():
    """Example: Custom analysis with manual data processing."""
    print("\n" + "="*60)
    print("Example 3: Custom Analysis")
    print("="*60)
    
    load_dotenv()
    token = os.getenv('GITHUB_TOKEN')
    username = os.getenv('GITHUB_USERNAME')
    
    if not token or not username:
        print("Please set GITHUB_TOKEN and GITHUB_USERNAME in .env file")
        return
    
    analytics = GitHubAnalytics(token, username)
    
    # Get the data as a DataFrame
    df = analytics.analyze_all_repositories()
    
    if not df.empty:
        print("\n--- Top 10 Contributors by Estimated Hours ---")
        top_contributors = df.groupby('user')['estimated_hours'].sum().sort_values(ascending=False).head(10)
        for user, hours in top_contributors.items():
            print(f"{user:30} {hours:>10.2f} hours")
        
        print("\n--- Most Active Days ---")
        active_days = df.groupby('date')['commits'].sum().sort_values(ascending=False).head(10)
        for date, commits in active_days.items():
            print(f"{date:15} {commits:>5} commits")
        
        print("\n--- Language Changes (Total Lines) ---")
        total_lines = df['total_lines_changed'].sum()
        total_commits = df['commits'].sum()
        print(f"Total commits: {total_commits}")
        print(f"Total lines changed: {total_lines:,}")
        print(f"Average lines per commit: {total_lines/total_commits:.1f}" if total_commits > 0 else "N/A")


def example_quarterly_reports():
    """Example: Generate quarterly reports for the year."""
    print("\n" + "="*60)
    print("Example 4: Quarterly Reports")
    print("="*60)
    
    load_dotenv()
    token = os.getenv('GITHUB_TOKEN')
    username = os.getenv('GITHUB_USERNAME')
    
    if not token or not username:
        print("Please set GITHUB_TOKEN and GITHUB_USERNAME in .env file")
        return
    
    analytics = GitHubAnalytics(token, username)
    
    year = 2024
    quarters = [
        ('Q1', datetime(year, 1, 1), datetime(year, 3, 31)),
        ('Q2', datetime(year, 4, 1), datetime(year, 6, 30)),
        ('Q3', datetime(year, 7, 1), datetime(year, 9, 30)),
        ('Q4', datetime(year, 10, 1), datetime(year, 12, 31)),
    ]
    
    for quarter_name, start, end in quarters:
        print(f"\nGenerating {quarter_name} {year} report...")
        analytics.generate_report(
            output_file=f'{year}_{quarter_name}_report.xlsx',
            start_date=start,
            end_date=end
        )


def main():
    """Run examples based on user selection."""
    print("GitHub Analytics - Example Usage")
    print("="*60)
    print("\nAvailable examples:")
    print("1. Basic analysis (all repositories, all time)")
    print("2. Date range analysis (last 30 days)")
    print("3. Custom analysis with data processing")
    print("4. Generate quarterly reports")
    print("5. Run all examples")
    print("0. Exit")
    
    try:
        choice = input("\nSelect an example (0-5): ").strip()
        
        if choice == '1':
            example_basic_analysis()
        elif choice == '2':
            example_date_range_analysis()
        elif choice == '3':
            example_custom_analysis()
        elif choice == '4':
            example_quarterly_reports()
        elif choice == '5':
            example_basic_analysis()
            example_date_range_analysis()
            example_custom_analysis()
            example_quarterly_reports()
        elif choice == '0':
            print("Exiting...")
        else:
            print("Invalid choice")
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")


if __name__ == '__main__':
    main()
