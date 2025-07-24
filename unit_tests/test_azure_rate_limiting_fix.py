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

import pytest
from unittest.mock import Mock, patch

from azure.core.exceptions import HttpResponseError

from sdcm.utils.azure_utils import AzureService
from sdcm.utils.cloud_monitor.resources.static_ips import StaticIPs
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances


def test_static_ips_azure_failure_continues_with_other_clouds():
    """Test that if Azure static IPs fail, AWS and GCE are still processed."""
    with patch('sdcm.utils.cloud_monitor.resources.static_ips.AzureService') as mock_azure_service, \
            patch('sdcm.utils.cloud_monitor.resources.static_ips.list_elastic_ips_aws') as mock_aws_list, \
            patch('sdcm.utils.cloud_monitor.resources.static_ips.list_static_ips_gce') as mock_gce_list:
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
        assert static_ips["azure"] == []
        mock_aws_list.assert_called_once()
        mock_gce_list.assert_called_once()
        mock_azure_service.assert_called_once()


def test_cloud_instances_azure_failure_continues_with_other_clouds():
    """Test that if Azure instances fail, AWS and GCE are still processed."""
    with patch('sdcm.utils.cloud_monitor.resources.instances.AzureService') as mock_azure_service, \
            patch('sdcm.utils.cloud_monitor.resources.instances.list_instances_aws') as mock_aws_list, \
            patch('sdcm.utils.cloud_monitor.resources.instances.list_instances_gce') as mock_gce_list, \
            patch('sdcm.utils.cloud_monitor.resources.instances.SUPPORTED_PROJECTS', ['test-project']), \
            patch('sdcm.utils.cloud_monitor.resources.instances.environment') as mock_env:
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
        assert cloud_instances["azure"] == []
        mock_aws_list.assert_called_once()
        mock_gce_list.assert_called_once()
        mock_azure_service.assert_called_once()


@pytest.mark.parametrize("side_effect, expected_call_count, expected_sleep_calls", [
    ([HttpResponseError("RateLimiting - Client application has been throttled"), HttpResponseError("RateLimiting - Client application has been throttled"),
     Mock(data=[{"test": "data"}], skip_token=None, result_truncated="false")], 3, 2),
    (HttpResponseError("RateLimiting - Client application has been throttled"), 6, 5),
    (HttpResponseError("Some other error"), 1, 0)
])
def test_azure_resource_graph_query_rate_limiting_retry(side_effect, expected_call_count, expected_sleep_calls):
    """Test that Azure resource graph query retries with exponential backoff on rate limiting."""

    with patch('sdcm.utils.azure_utils.time.sleep') as mock_sleep:
        # Create mock resource graph client
        mock_resource_graph = Mock()

        # Set side effect for mock resource graph based on test parameterization
        mock_resource_graph.resources.side_effect = side_effect

        # Create AzureService instance and mock resource_graph property
        azure_service = AzureService()
        with patch.object(azure_service, 'resource_graph', mock_resource_graph), \
                patch.object(azure_service, 'subscription_id', 'test-subscription'):

            # Execute query
            if isinstance(side_effect, list):
                result = list(azure_service.resource_graph_query("test query"))
                # Verify results
                assert result == [{"test": "data"}]
            else:
                with pytest.raises(HttpResponseError):
                    list(azure_service.resource_graph_query("test query"))

            # Verify call counts
            assert mock_resource_graph.resources.call_count == expected_call_count
            assert mock_sleep.call_count == expected_sleep_calls

            # If exponential backoff is expected, verify sleep times
            if expected_sleep_calls > 0:
                # Verify sleep times (exponential backoff: 2, 4, 8, ...)
                sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
                expected_sleep_times = [2 ** i for i in range(1, expected_sleep_calls + 1)]
                assert sleep_calls == pytest.approx(expected_sleep_times, rel=1.5)
