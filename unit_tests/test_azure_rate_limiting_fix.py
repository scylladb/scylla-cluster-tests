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
from unittest.mock import Mock, patch, MagicMock
from azure.core.exceptions import HttpResponseError

from sdcm.utils.cloud_monitor.resources.static_ips import StaticIPs
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances


class TestAzureRateLimitingFix(unittest.TestCase):
    """Test that Azure rate limiting errors are handled gracefully and don't stop the entire cloud report."""

    @patch('sdcm.utils.cloud_monitor.resources.static_ips.AzureService')
    @patch('sdcm.utils.cloud_monitor.resources.static_ips.list_elastic_ips_aws')
    @patch('sdcm.utils.cloud_monitor.resources.static_ips.list_static_ips_gce')
    def test_static_ips_azure_failure_continues_with_other_clouds(self, mock_gce_list, mock_aws_list, mock_azure_service):
        """Test that if Azure static IPs fail, AWS and GCE are still processed."""
        # Setup mocks
        mock_aws_list.return_value = {}
        mock_gce_list.return_value = []
        
        # Mock Azure to raise rate limiting error
        mock_azure_instance = Mock()
        mock_azure_instance.resource_graph_query.side_effect = HttpResponseError("RateLimiting error")
        mock_azure_service.return_value = mock_azure_instance
        
        # Create mock cloud instances
        mock_cloud_instances = {
            "aws": [],
            "gce": [],
            "azure": []
        }

        # Execute
        static_ips = StaticIPs(mock_cloud_instances)
        
        # Verify that Azure is empty but others were attempted
        self.assertEqual(static_ips["azure"], [])
        mock_aws_list.assert_called_once()
        mock_gce_list.assert_called_once()
        mock_azure_service.assert_called_once()

    @patch('sdcm.utils.cloud_monitor.resources.instances.AzureService')
    @patch('sdcm.utils.cloud_monitor.resources.instances.list_instances_aws')
    @patch('sdcm.utils.cloud_monitor.resources.instances.list_instances_gce')
    @patch('sdcm.utils.gce_utils.SUPPORTED_PROJECTS', ['test-project'])
    @patch('sdcm.utils.cloud_monitor.resources.instances.environment')
    def test_cloud_instances_azure_failure_continues_with_other_clouds(self, mock_env, mock_gce_list, mock_aws_list, mock_azure_service):
        """Test that if Azure instances fail, AWS and GCE are still processed."""
        # Setup mocks
        mock_aws_list.return_value = []
        mock_gce_list.return_value = []
        mock_env.return_value.__enter__ = Mock()
        mock_env.return_value.__exit__ = Mock()
        
        # Mock Azure to raise rate limiting error
        mock_azure_instance = Mock()
        mock_azure_instance.resource_graph_query.side_effect = HttpResponseError("RateLimiting error")
        mock_azure_service.return_value = mock_azure_instance
        
        # Execute
        cloud_instances = CloudInstances()
        
        # Verify that Azure is empty but others were attempted
        self.assertEqual(cloud_instances["azure"], [])
        mock_aws_list.assert_called_once()
        mock_gce_list.assert_called_once()
        mock_azure_service.assert_called_once()

    @patch('sdcm.utils.azure_utils.time.sleep')
    def test_azure_resource_graph_query_rate_limiting_retry(self, mock_sleep):
        """Test that Azure resource graph query retries with exponential backoff on rate limiting."""
        from sdcm.utils.azure_utils import AzureService
        
        # Create mock resource graph client
        mock_resource_graph = Mock()
        
        # First two calls raise rate limiting error, third succeeds
        rate_limit_error = HttpResponseError("RateLimiting - Client application has been throttled")
        success_response = Mock()
        success_response.data = [{"test": "data"}]
        success_response.skip_token = None
        success_response.result_truncated = "false"
        
        mock_resource_graph.resources.side_effect = [rate_limit_error, rate_limit_error, success_response]
        
        # Create AzureService instance and mock resource_graph property
        azure_service = AzureService()
        with patch.object(azure_service, 'resource_graph', mock_resource_graph), \
             patch.object(azure_service, 'subscription_id', 'test-subscription'):
            
            # Execute query
            result = list(azure_service.resource_graph_query("test query"))
            
            # Verify results
            self.assertEqual(result, [{"test": "data"}])
            
            # Verify retries happened with exponential backoff
            self.assertEqual(mock_resource_graph.resources.call_count, 3)
            self.assertEqual(mock_sleep.call_count, 2)
            
            # Verify sleep times (exponential backoff: 2, 4)
            sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
            self.assertEqual(sleep_calls, [2, 4])

    @patch('sdcm.utils.azure_utils.time.sleep')
    def test_azure_resource_graph_query_max_retries_exceeded(self, mock_sleep):
        """Test that Azure resource graph query raises error after max retries exceeded."""
        from sdcm.utils.azure_utils import AzureService
        
        # Create mock resource graph client that always returns rate limiting error
        mock_resource_graph = Mock()
        rate_limit_error = HttpResponseError("RateLimiting - Client application has been throttled")
        mock_resource_graph.resources.side_effect = rate_limit_error
        
        # Create AzureService instance and mock resource_graph property
        azure_service = AzureService()
        with patch.object(azure_service, 'resource_graph', mock_resource_graph), \
             patch.object(azure_service, 'subscription_id', 'test-subscription'):
            
            # Execute query and expect exception
            with self.assertRaises(HttpResponseError):
                list(azure_service.resource_graph_query("test query"))
            
            # Verify max retries (5) + initial attempt = 6 total calls
            self.assertEqual(mock_resource_graph.resources.call_count, 6)
            
            # Verify 5 sleep calls (for 5 retries)
            self.assertEqual(mock_sleep.call_count, 5)

    @patch('sdcm.utils.azure_utils.time.sleep')
    def test_azure_resource_graph_query_non_rate_limiting_error_not_retried(self, mock_sleep):
        """Test that non-rate limiting errors are not retried."""
        from sdcm.utils.azure_utils import AzureService
        
        # Create mock resource graph client that returns non-rate limiting error
        mock_resource_graph = Mock()
        other_error = HttpResponseError("Some other error")
        mock_resource_graph.resources.side_effect = other_error
        
        # Create AzureService instance and mock resource_graph property
        azure_service = AzureService()
        with patch.object(azure_service, 'resource_graph', mock_resource_graph), \
             patch.object(azure_service, 'subscription_id', 'test-subscription'):
            
            # Execute query and expect immediate exception
            with self.assertRaises(HttpResponseError):
                list(azure_service.resource_graph_query("test query"))
            
            # Verify only one call (no retries)
            self.assertEqual(mock_resource_graph.resources.call_count, 1)
            
            # Verify no sleep calls
            self.assertEqual(mock_sleep.call_count, 0)