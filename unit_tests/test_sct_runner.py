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
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from sdcm.sct_runner import (
    list_sct_runners,
    clean_sct_runners,
    SctRunnerInfo,
    AwsSctRunner,
    GceSctRunner,
    AzureSctRunner,
)


class TestListSctRunners(unittest.TestCase):
    """Test the enhanced list_sct_runners function."""

    def setUp(self):
        self.aws_runner = SctRunnerInfo(
            sct_runner_class=AwsSctRunner,
            cloud_service_instance=None,
            region_az="us-east-1a",
            instance={"Tags": [{"Key": "RunByUser", "Value": "user1"}], "InstanceId": "i-aws1"},
            instance_name="aws-runner-1",
            public_ips=["1.2.3.4"],
            test_id="test-id-1")

        self.gce_runner = SctRunnerInfo(
            sct_runner_class=GceSctRunner,
            cloud_service_instance=None,
            region_az="us-central1-a",
            instance=MagicMock(metadata=MagicMock()),
            instance_name="gce-runner-1",
            public_ips=["5.6.7.8"],
            test_id="test-id-2")

        self.azure_runner = SctRunnerInfo(
            sct_runner_class=AzureSctRunner,
            cloud_service_instance=None,
            region_az="eastus-1",
            instance=MagicMock(tags={"RunByUser": "user2"}),
            instance_name="azure-runner-1",
            public_ips=["9.10.11.12"],
            test_id="test-id-1")

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    def test_list_sct_runners_no_filters(self, mock_azure, mock_gce, mock_aws):
        """Test listing all runners without filters."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]

        runners = list_sct_runners(verbose=False)

        self.assertEqual(len(runners), 3)
        self.assertIn(self.aws_runner, runners)
        self.assertIn(self.gce_runner, runners)
        self.assertIn(self.azure_runner, runners)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner._get_runner_user_tag')
    def test_list_sct_runners_filter_by_user(self, mock_get_user_tag, mock_azure, mock_gce, mock_aws):
        """Test filtering runners by user."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]

        def side_effect(runner_info):
            if runner_info == self.aws_runner:
                return "user1"
            elif runner_info == self.gce_runner:
                return "user1"
            elif runner_info == self.azure_runner:
                return "user2"
            return None
        mock_get_user_tag.side_effect = side_effect

        runners = list_sct_runners(user="user1", verbose=False)

        self.assertEqual(len(runners), 2)
        self.assertIn(self.aws_runner, runners)
        self.assertIn(self.gce_runner, runners)
        self.assertNotIn(self.azure_runner, runners)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    def test_list_sct_runners_filter_by_test_id(self, mock_azure, mock_gce, mock_aws):
        """Test filtering runners by test_id."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]

        runners = list_sct_runners(test_id="test-id-1", verbose=False)

        self.assertEqual(len(runners), 2)
        self.assertIn(self.aws_runner, runners)
        self.assertNotIn(self.gce_runner, runners)
        self.assertIn(self.azure_runner, runners)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    def test_list_sct_runners_filter_by_ip(self, mock_azure, mock_gce, mock_aws):
        """Test filtering runners by IP address."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]

        runners = list_sct_runners(test_runner_ip="5.6.7.8", verbose=False)

        self.assertEqual(len(runners), 1)
        self.assertNotIn(self.aws_runner, runners)
        self.assertIn(self.gce_runner, runners)
        self.assertNotIn(self.azure_runner, runners)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner._get_runner_user_tag')
    def test_list_sct_runners_mixed_filters(self, mock_get_user_tag, mock_azure, mock_gce, mock_aws):
        """Test user and test_id filters."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]

        def side_effect(runner_info):
            if runner_info == self.aws_runner:
                return "user1"
            elif runner_info == self.azure_runner:
                return "user1"
            return "other_user"
        mock_get_user_tag.side_effect = side_effect

        runners = list_sct_runners(user="user1", test_id="test-id-1", verbose=False)

        self.assertEqual(len(runners), 2)
        self.assertIn(self.aws_runner, runners)
        self.assertNotIn(self.gce_runner, runners)
        self.assertIn(self.azure_runner, runners)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    def test_list_sct_runners_empty_results(self, mock_azure, mock_gce, mock_aws):
        """Test no runners match filters."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]

        runners = list_sct_runners(test_id="non-existent-test-id", verbose=False)

        self.assertEqual(len(runners), 0)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    def test_list_sct_runners_backend_filtering_aws(self, mock_aws):
        """Test backend-specific filtering for AWS."""
        mock_aws.return_value = [self.aws_runner]

        with patch('sdcm.sct_runner.GceSctRunner.list_sct_runners') as mock_gce:
            with patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners') as mock_azure:
                runners = list_sct_runners(backend="aws", verbose=False)

                mock_aws.assert_called_once()
                mock_gce.assert_not_called()
                mock_azure.assert_not_called()
                self.assertEqual(len(runners), 1)
                self.assertIn(self.aws_runner, runners)


class TestCleanSctRunners(unittest.TestCase):
    """Test the clean_sct_runners function."""

    def setUp(self):
        self.mock_runner_with_keep = MagicMock(
            keep="24",
            keep_action="terminate",
            launch_time=datetime.now(timezone.utc),
            public_ips=["1.2.3.4"],
            cloud_provider="aws",
            instance_name="test-runner-1",
            region_az="us-east-1a",
            test_id="test-123")

        self.mock_runner_no_keep = MagicMock(
            keep=None,
            keep_action=None,
            launch_time=datetime.now(timezone.utc),
            public_ips=["5.6.7.8"],
            cloud_provider="gce",
            instance_name="test-runner-2",
            region_az="us-central1-a",
            test_id="test-456")

    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_by_user(self, mock_list_runners):
        """Test cleanup filtered by user."""
        mock_list_runners.return_value = [self.mock_runner_no_keep]

        clean_sct_runners(test_status="", user="test_user", dry_run=True)
        mock_list_runners.assert_called_once_with(backend=None, test_runner_ip=None, user="test_user", test_id=None)

    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_by_test_id(self, mock_list_runners):
        """Test cleanup filtered by test_id."""
        mock_list_runners.return_value = [self.mock_runner_no_keep]

        clean_sct_runners(test_status="", test_id="test-id-123", dry_run=True)
        mock_list_runners.assert_called_once_with(backend=None, test_runner_ip=None, user=None, test_id="test-id-123")

    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_mixed_filters(self, mock_list_runners):
        """Test mix of user and test_id filters."""
        mock_list_runners.return_value = [self.mock_runner_no_keep]

        clean_sct_runners(
            test_status="completed", user="test_user", test_id="test-id-123", backend="aws", dry_run=True)
        mock_list_runners.assert_called_once_with(
            backend="aws", test_runner_ip=None, user="test_user", test_id="test-id-123")

    @patch('sdcm.sct_runner.ssh_run_cmd')
    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_force(self, mock_list_runners, mock_ssh_cmd):
        """Test force cleanup ignoring keep tags."""
        mock_list_runners.return_value = [self.mock_runner_with_keep]
        mock_ssh_cmd.return_value = MagicMock(stdout="")

        clean_sct_runners(test_status="", user="test_user", force=True, dry_run=True)
        mock_list_runners.assert_called_once()

    @patch('sdcm.sct_runner.ssh_run_cmd')
    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_respect_keep_tags(self, mock_list_runners, mock_ssh_cmd):
        """Test keep tags are respected when clean is not forced."""
        # runner with keep 'alive' tag
        mock_runner_keep_alive = MagicMock(
            keep="alive",
            keep_action="terminate",
            launch_time=datetime.now(timezone.utc),
            public_ips=["1.2.3.4"],
            cloud_provider="aws",
            instance_name="test-runner-alive",
            region_az="us-east-1a")

        mock_list_runners.return_value = [mock_runner_keep_alive]
        mock_ssh_cmd.return_value = MagicMock(stdout="")

        clean_sct_runners(test_status="", user="test_user", force=False, dry_run=False)
        mock_runner_keep_alive.terminate.assert_not_called()

    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_no_runners_found(self, mock_list_runners):
        """Test when no runners match filters."""
        mock_list_runners.return_value = []

        clean_sct_runners(test_status="", user="nonexistent_user", dry_run=True)
        mock_list_runners.assert_called_once()
