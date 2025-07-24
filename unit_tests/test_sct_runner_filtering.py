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
# Copyright (c) 2023 ScyllaDB

import unittest
from unittest.mock import Mock, patch, MagicMock

from sdcm.utils.common import aws_tags_to_dict, gce_meta_to_dict


class TestSctRunnerFiltering(unittest.TestCase):
    """Test the new SCT runner filtering functionality."""
    
    def test_aws_tags_to_dict_with_user(self):
        """Test extracting RunByUser from AWS tags."""
        tags = [
            {"Key": "RunByUser", "Value": "test.user"},
            {"Key": "TestId", "Value": "test-123"},
            {"Key": "NodeType", "Value": "sct-runner"}
        ]
        result = aws_tags_to_dict(tags)
        self.assertEqual(result.get("RunByUser"), "test.user")
        self.assertEqual(result.get("TestId"), "test-123")
    
    def test_aws_tags_to_dict_without_user(self):
        """Test when RunByUser tag is not present."""
        tags = [
            {"Key": "TestId", "Value": "test-123"},
            {"Key": "NodeType", "Value": "sct-runner"}
        ]
        result = aws_tags_to_dict(tags)
        self.assertIsNone(result.get("RunByUser"))
        self.assertEqual(result.get("TestId"), "test-123")
    
    def test_gce_meta_to_dict_with_user(self):
        """Test extracting RunByUser from GCE metadata."""
        # Mock GCE metadata structure
        metadata = Mock()
        metadata.items = [
            Mock(key="RunByUser", value="test.user"),
            Mock(key="TestId", value="test-123"),
            Mock(key="NodeType", value="sct-runner")
        ]
        result = gce_meta_to_dict(metadata)
        self.assertEqual(result.get("RunByUser"), "test.user")
        self.assertEqual(result.get("TestId"), "test-123")

    @patch('sdcm.sct_runner.aws_tags_to_dict')
    def test_get_runner_user_tag_aws(self, mock_aws_tags):
        """Test _get_runner_user_tag for AWS runner."""
        from sdcm.sct_runner import _get_runner_user_tag, SctRunnerInfo
        
        # Mock AWS runner
        mock_runner = Mock(spec=SctRunnerInfo)
        mock_runner.cloud_provider = "aws"
        mock_runner.instance = {"Tags": [{"Key": "RunByUser", "Value": "test.user"}]}
        
        mock_aws_tags.return_value = {"RunByUser": "test.user"}
        
        result = _get_runner_user_tag(mock_runner)
        self.assertEqual(result, "test.user")
        mock_aws_tags.assert_called_once_with([{"Key": "RunByUser", "Value": "test.user"}])

    @patch('sdcm.sct_runner.gce_meta_to_dict')
    def test_get_runner_user_tag_gce(self, mock_gce_meta):
        """Test _get_runner_user_tag for GCE runner."""
        from sdcm.sct_runner import _get_runner_user_tag, SctRunnerInfo
        
        # Mock GCE runner
        mock_runner = Mock(spec=SctRunnerInfo)
        mock_runner.cloud_provider = "gce"
        mock_runner.instance = Mock()
        mock_runner.instance.metadata = Mock()
        
        mock_gce_meta.return_value = {"RunByUser": "test.user"}
        
        result = _get_runner_user_tag(mock_runner)
        self.assertEqual(result, "test.user")
        mock_gce_meta.assert_called_once_with(mock_runner.instance.metadata)

    def test_get_runner_user_tag_azure(self):
        """Test _get_runner_user_tag for Azure runner."""
        from sdcm.sct_runner import _get_runner_user_tag, SctRunnerInfo
        
        # Mock Azure runner
        mock_runner = Mock(spec=SctRunnerInfo)
        mock_runner.cloud_provider = "azure"
        mock_runner.instance = Mock()
        mock_runner.instance.tags = {"RunByUser": "test.user"}
        
        result = _get_runner_user_tag(mock_runner)
        self.assertEqual(result, "test.user")

    def test_get_runner_user_tag_no_user(self):
        """Test _get_runner_user_tag when no user tag exists."""
        from sdcm.sct_runner import _get_runner_user_tag, SctRunnerInfo
        
        # Mock runner with no RunByUser tag
        mock_runner = Mock(spec=SctRunnerInfo)
        mock_runner.cloud_provider = "unknown"
        
        result = _get_runner_user_tag(mock_runner)
        self.assertIsNone(result)

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners') 
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner._get_runner_user_tag')
    def test_list_sct_runners_with_user_filter(self, mock_get_user, mock_azure_list, mock_gce_list, mock_aws_list):
        """Test list_sct_runners with user filtering."""
        from sdcm.sct_runner import list_sct_runners, SctRunnerInfo
        
        # Create mock runners
        runner1 = Mock(spec=SctRunnerInfo)
        runner1.test_id = "test-123"
        runner1.public_ips = ["1.2.3.4"]
        
        runner2 = Mock(spec=SctRunnerInfo)
        runner2.test_id = "test-456"
        runner2.public_ips = ["5.6.7.8"]
        
        # Mock the class list methods to return our test runners
        mock_aws_list.return_value = [runner1, runner2]
        mock_gce_list.return_value = []
        mock_azure_list.return_value = []
        
        # Mock user tag extraction
        def mock_user_side_effect(runner):
            if runner.test_id == "test-123":
                return "target.user"
            return "other.user"
        
        mock_get_user.side_effect = mock_user_side_effect
        
        # Test filtering by user
        result = list_sct_runners(user="target.user", verbose=False)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].test_id, "test-123")

    @patch('sdcm.sct_runner.AwsSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.GceSctRunner.list_sct_runners')
    @patch('sdcm.sct_runner.AzureSctRunner.list_sct_runners')
    def test_list_sct_runners_with_test_id_filter(self, mock_azure_list, mock_gce_list, mock_aws_list):
        """Test list_sct_runners with test_id filtering."""
        from sdcm.sct_runner import list_sct_runners, SctRunnerInfo
        
        # Create mock runners
        runner1 = Mock(spec=SctRunnerInfo)
        runner1.test_id = "test-123"
        runner1.public_ips = ["1.2.3.4"]
        
        runner2 = Mock(spec=SctRunnerInfo)
        runner2.test_id = "test-456"
        runner2.public_ips = ["5.6.7.8"]
        
        # Mock the class list methods
        mock_aws_list.return_value = [runner1, runner2]
        mock_gce_list.return_value = []
        mock_azure_list.return_value = []
        
        # Test filtering by test_id
        result = list_sct_runners(test_id="test-456", verbose=False)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].test_id, "test-456")

    @patch('sdcm.sct_runner.list_sct_runners')
    def test_clean_sct_runners_with_new_params(self, mock_list_runners):
        """Test clean_sct_runners accepts and passes new parameters."""
        from sdcm.sct_runner import clean_sct_runners
        
        mock_list_runners.return_value = []
        
        # Test that clean_sct_runners accepts the new parameters
        clean_sct_runners(
            test_status="PASSED",
            test_runner_ip="1.2.3.4",
            backend="aws", 
            user="test.user",
            test_id="test-123",
            dry_run=True
        )
        
        # Verify list_sct_runners was called with the correct parameters
        mock_list_runners.assert_called_once_with(
            backend="aws",
            test_runner_ip="1.2.3.4", 
            user="test.user",
            test_id="test-123"
        )