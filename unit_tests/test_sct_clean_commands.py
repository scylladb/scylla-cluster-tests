# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2025 ScyllaDB

import unittest
from unittest.mock import patch
import os

from click.testing import CliRunner

import sct


class TestCleanResourcesCommand(unittest.TestCase):

    ENV_VARS = {
        'SCT_REGION_NAME': 'us-east-1',
        'SCT_GCE_DATACENTER': 'us-central1-a',
        'SCT_AZURE_REGION_NAME': 'eastus'
    }

    def setUp(self):
        self.runner = CliRunner()

    @patch('sct.clean_cloud_resources')
    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_resources_with_clean_runners_flag_and_user(self, mock_logger, mock_clean_runners, mock_clean_resources):
        """Test clean-resources with --clean-runners flag and --user option."""
        mock_clean_resources.return_value = None
        mock_clean_runners.return_value = None

        with patch.dict(os.environ, self.ENV_VARS):
            result = self.runner.invoke(
                sct.clean_resources, ['--user', 'test.user', '--clean-runners', '--dry-run'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_status="",
            test_runner_ip=None,
            backend=None,
            user="test.user",
            test_id=None,
            dry_run=True,
            force=True)
        mock_clean_resources.assert_called_once()

    @patch('sct.clean_cloud_resources')
    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_resources_with_clean_runners_flag_and_test_id(self, mock_logger, mock_clean_runners, mock_clean_resources):
        """Test clean-resources with --clean-runners flag and --test-id option."""
        mock_clean_resources.return_value = None
        mock_clean_runners.return_value = None

        with patch.dict(os.environ, self.ENV_VARS):
            result = self.runner.invoke(
                sct.clean_resources, ['--test-id', 'test-123', '--clean-runners', '--backend', 'aws'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_status="",
            test_runner_ip=None,
            backend="aws",
            user=None,
            test_id="test-123",
            dry_run=False,
            force=True)

    @patch('sct.add_file_logger')
    def test_clean_resources_clean_runners_requires_user_or_test_id(self, mock_logger):
        """Test that --clean-runners requires --user or --test-id."""
        result = self.runner.invoke(sct.clean_resources, ['--clean-runners'])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("--clean-runners requires --user and/or --test-id", result.output)

    @patch('sct.clean_cloud_resources')
    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_resources_with_both_user_and_test_id(self, mock_logger, mock_clean_runners, mock_clean_resources):
        """Test clean-resources with both --user and --test-id options."""
        mock_clean_resources.return_value = None
        mock_clean_runners.return_value = None

        with patch.dict(os.environ, self.ENV_VARS):
            result = self.runner.invoke(
                sct.clean_resources,
                ['--user', 'test.user', '--test-id', 'test-123', '--clean-runners', '--backend', 'gce'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_status="",
            test_runner_ip=None,
            backend="gce",
            user="test.user",
            test_id="test-123",
            dry_run=False,
            force=True)

    @patch('sct.clean_cloud_resources')
    @patch('sct.add_file_logger')
    def test_clean_resources_without_clean_runners_flag(self, mock_logger, mock_clean_resources):
        """Test clean-resources without --clean-runners flag."""
        mock_clean_resources.return_value = None

        with patch.dict(os.environ, self.ENV_VARS):
            result = self.runner.invoke(sct.clean_resources, ['--user', 'test.user'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_resources.assert_called_once()
        with patch('sct.clean_sct_runners') as mock_clean_runners:
            mock_clean_runners.assert_not_called()


class TestCleanRunnerInstancesCommand(unittest.TestCase):

    def setUp(self):
        self.runner = CliRunner()

    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_runner_instances_with_user(self, mock_logger, mock_clean_runners):
        """Test clean-runner-instances with --user option."""
        mock_clean_runners.return_value = None

        result = self.runner.invoke(sct.clean_runner_instances, ['--user', 'test.user'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_runner_ip='',
            test_status=None,
            backend=None,
            user="test.user",
            test_id=(),
            dry_run=False,
            force=False)

    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_runner_instances_with_test_id(self, mock_logger, mock_clean_runners):
        """Test clean-runner-instances with --test-id option."""
        mock_clean_runners.return_value = None

        result = self.runner.invoke(sct.clean_runner_instances, ['--test-id', 'test-123'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_runner_ip='',
            test_status=None,
            backend=None,
            user=None,
            test_id=('test-123',),
            dry_run=False,
            force=False)

    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_runner_instances_with_runner_ip(self, mock_logger, mock_clean_runners):
        """Test clean-runner-instances with --runner-ip option."""
        mock_clean_runners.return_value = None

        result = self.runner.invoke(sct.clean_runner_instances, ['--runner-ip', '1.2.3.4'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_runner_ip="1.2.3.4",
            test_status=None,
            backend=None,
            user=None,
            test_id=(),
            dry_run=False,
            force=False)

    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_runner_instances_force(self, mock_logger, mock_clean_runners):
        """Test clean-runner-instances with --force flag."""
        mock_clean_runners.return_value = None

        result = self.runner.invoke(sct.clean_runner_instances, ['--runner-ip', '1.2.3.4', '--force'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_runner_ip="1.2.3.4",
            test_status=None,
            backend=None,
            user=None,
            test_id=(),
            dry_run=False,
            force=True)

    @patch('sct.clean_sct_runners')
    @patch('sct.add_file_logger')
    def test_clean_runner_instances_all_options(self, mock_logger, mock_clean_runners):
        """Test clean-runner-instances with all available options."""
        mock_clean_runners.return_value = None

        result = self.runner.invoke(sct.clean_runner_instances, [
            '--runner-ip', '1.2.3.4',
            '--test-status', 'timeout',
            '--backend', 'gce',
            '--user', 'test.user',
            '--test-id', 'test-123',
            '--dry-run',
            '--force'])

        self.assertEqual(result.exit_code, 0)
        mock_clean_runners.assert_called_once_with(
            test_runner_ip="1.2.3.4",
            test_status="timeout",
            backend="gce",
            user="test.user",
            test_id=('test-123',),
            dry_run=True,
            force=True)
