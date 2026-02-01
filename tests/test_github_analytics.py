#!/usr/bin/env python3
"""
Unit tests for GitHub Analytics tool.

Tests basic functionality without requiring GitHub API access.
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
from github_analyitics.reporting.github_analytics import GitHubAnalytics
import pandas as pd


class TestGitHubAnalytics(unittest.TestCase):
    """Test cases for GitHubAnalytics class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create an instance without requiring network/gh.
        self.analytics = GitHubAnalytics('fake_token', 'test_user')
    
    def test_estimate_hours_basic(self):
        """Test work hour estimation with basic inputs."""
        # Test with 1 commit and 30 lines
        hours = self.analytics.estimate_hours_from_commits(1, 30)
        self.assertEqual(hours, 1.5)  # 0.5 (base) + 1.0 (30/30)
        
        # Test with 10 commits and 300 lines
        hours = self.analytics.estimate_hours_from_commits(10, 300)
        self.assertEqual(hours, 15.0)  # 5.0 (base) + 10.0 (300/30)
    
    def test_estimate_hours_zero(self):
        """Test work hour estimation with zero values."""
        hours = self.analytics.estimate_hours_from_commits(0, 0)
        self.assertEqual(hours, 0.0)
    
    def test_estimate_hours_large_values(self):
        """Test work hour estimation with large values."""
        # 100 commits, 5000 lines
        hours = self.analytics.estimate_hours_from_commits(100, 5000)
        expected = round(100 * 0.5 + 5000 / 30.0, 2)
        self.assertEqual(hours, expected)
    
    def test_merge_data_empty(self):
        """Test merging empty data dictionaries."""
        result = self.analytics.merge_data({}, {})
        self.assertEqual(len(result), 0)
    
    def test_merge_data_single(self):
        """Test merging a single data dictionary."""
        data = {
            'user1': {
                '2024-01-01': {'commits': 5, 'additions': 100}
            }
        }
        result = self.analytics.merge_data(data)
        self.assertEqual(result['user1']['2024-01-01']['commits'], 5)
        self.assertEqual(result['user1']['2024-01-01']['additions'], 100)
    
    def test_merge_data_multiple(self):
        """Test merging multiple data dictionaries."""
        data1 = {
            'user1': {
                '2024-01-01': {'commits': 5}
            }
        }
        data2 = {
            'user1': {
                '2024-01-01': {'prs_created': 2}
            }
        }
        result = self.analytics.merge_data(data1, data2)
        self.assertEqual(result['user1']['2024-01-01']['commits'], 5)
        self.assertEqual(result['user1']['2024-01-01']['prs_created'], 2)
    
    def test_merge_data_different_users(self):
        """Test merging data from different users."""
        data1 = {
            'user1': {'2024-01-01': {'commits': 5}}
        }
        data2 = {
            'user2': {'2024-01-01': {'commits': 3}}
        }
        result = self.analytics.merge_data(data1, data2)
        self.assertEqual(result['user1']['2024-01-01']['commits'], 5)
        self.assertEqual(result['user2']['2024-01-01']['commits'], 3)
    
    def test_merge_data_different_dates(self):
        """Test merging data from different dates."""
        data1 = {
            'user1': {'2024-01-01': {'commits': 5}}
        }
        data2 = {
            'user1': {'2024-01-02': {'commits': 3}}
        }
        result = self.analytics.merge_data(data1, data2)
        self.assertEqual(result['user1']['2024-01-01']['commits'], 5)
        self.assertEqual(result['user1']['2024-01-02']['commits'], 3)


class TestDataProcessing(unittest.TestCase):
    """Test data processing and DataFrame operations."""
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        self.assertTrue(df.empty)
    
    def test_dataframe_creation(self):
        """Test creation of analytics DataFrame."""
        data = [
            {
                'date': '2024-01-01',
                'user': 'user1',
                'commits': 5,
                'lines_added': 100,
                'lines_deleted': 20,
                'estimated_hours': 2.5
            }
        ]
        df = pd.DataFrame(data)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['commits'], 5)
        self.assertEqual(df.iloc[0]['user'], 'user1')
    
    def test_dataframe_aggregation(self):
        """Test DataFrame aggregation operations."""
        data = [
            {'user': 'user1', 'commits': 5, 'estimated_hours': 2.5},
            {'user': 'user1', 'commits': 3, 'estimated_hours': 1.5},
            {'user': 'user2', 'commits': 10, 'estimated_hours': 5.0},
        ]
        df = pd.DataFrame(data)
        
        user_summary = df.groupby('user').agg({
            'commits': 'sum',
            'estimated_hours': 'sum'
        }).reset_index()
        
        user1_data = user_summary[user_summary['user'] == 'user1'].iloc[0]
        self.assertEqual(user1_data['commits'], 8)
        self.assertEqual(user1_data['estimated_hours'], 4.0)


def run_tests():
    """Run all tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestGitHubAnalytics))
    suite.addTests(loader.loadTestsFromTestCase(TestDataProcessing))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return success status
    return result.wasSuccessful()


if __name__ == '__main__':
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)
