"""Unit tests for cloud_api_client module."""
from unittest.mock import Mock, MagicMock, patch

import pytest

from sdcm.cloud_api_client import ScyllaCloudAPIClient


class TestScyllaCloudAPIClient:
    """Test suite for ScyllaCloudAPIClient."""

    @pytest.fixture
    def api_client(self):
        """Create a mock API client for testing."""
        with patch.object(ScyllaCloudAPIClient, '_create_session'):
            client = ScyllaCloudAPIClient(
                api_url="https://api.test.cloud",
                auth_token="test-token",
                raise_for_status=False
            )
            client.session = MagicMock()
            return client

    def test_get_instance_types_without_target(self, api_client):
        """Test get_instance_types without target parameter."""
        # Mock the response
        api_client.request = Mock(return_value={
            'instances': [
                {'id': 1, 'externalId': 't3.small'},
                {'id': 2, 'externalId': 't3.medium'},
            ]
        })

        result = api_client.get_instance_types(cloud_provider_id=1, region_id=1)

        # Verify the request was made correctly
        api_client.request.assert_called_once_with(
            'GET', '/deployment/cloud-provider/1/region/1', params={})
        assert result == {
            'instances': [
                {'id': 1, 'externalId': 't3.small'},
                {'id': 2, 'externalId': 't3.medium'},
            ]
        }

    def test_get_instance_types_with_target_vector_search(self, api_client):
        """Test get_instance_types with target=VECTOR_SEARCH parameter."""
        # Mock the response for Vector Search instances
        api_client.request = Mock(return_value={
            'instances': [
                {'id': 175, 'externalId': 't4g.small'},
                {'id': 176, 'externalId': 't4g.medium'},
                {'id': 177, 'externalId': 'r7g.medium'},
            ]
        })

        result = api_client.get_instance_types(
            cloud_provider_id=1, region_id=1, target='VECTOR_SEARCH')

        # Verify the request was made with target parameter
        api_client.request.assert_called_once_with(
            'GET', '/deployment/cloud-provider/1/region/1',
            params={'target': 'VECTOR_SEARCH'})
        assert len(result['instances']) == 3
        assert result['instances'][0]['externalId'] == 't4g.small'

    def test_get_instance_types_with_defaults(self, api_client):
        """Test get_instance_types with defaults parameter."""
        api_client.request = Mock(return_value={'instances': []})

        api_client.get_instance_types(
            cloud_provider_id=1, region_id=1, defaults=True)

        # Verify the request was made with defaults parameter
        api_client.request.assert_called_once_with(
            'GET', '/deployment/cloud-provider/1/region/1',
            params={'defaults': 'true'})

    def test_get_instance_types_with_all_params(self, api_client):
        """Test get_instance_types with all parameters."""
        api_client.request = Mock(return_value={'instances': []})

        api_client.get_instance_types(
            cloud_provider_id=1, region_id=1, defaults=True, target='VECTOR_SEARCH')

        # Verify the request was made with all parameters
        api_client.request.assert_called_once_with(
            'GET', '/deployment/cloud-provider/1/region/1',
            params={'defaults': 'true', 'target': 'VECTOR_SEARCH'})

    def test_get_vector_search_instance_types(self, api_client):
        """Test get_vector_search_instance_types convenience method."""
        # Mock the response
        api_client.request = Mock(return_value={
            'instances': [
                {'id': 175, 'externalId': 't4g.small'},
                {'id': 176, 'externalId': 't4g.medium'},
                {'id': 177, 'externalId': 'r7g.medium'},
            ]
        })

        result = api_client.get_vector_search_instance_types(
            cloud_provider_id=1, region_id=1)

        # Verify the request was made with VECTOR_SEARCH target
        api_client.request.assert_called_once_with(
            'GET', '/deployment/cloud-provider/1/region/1',
            params={'target': 'VECTOR_SEARCH'})

        # Verify the result is a dictionary mapping externalId to id
        assert result == {
            't4g.small': 175,
            't4g.medium': 176,
            'r7g.medium': 177,
        }

    def test_get_vector_search_instance_types_gce(self, api_client):
        """Test get_vector_search_instance_types for GCE."""
        # Mock the response for GCE
        api_client.request = Mock(return_value={
            'instances': [
                {'id': 178, 'externalId': 'e2-small'},
                {'id': 179, 'externalId': 'e2-medium'},
                {'id': 180, 'externalId': 'n4-highmem-2'},
            ]
        })

        result = api_client.get_vector_search_instance_types(
            cloud_provider_id=2, region_id=5)

        # Verify the result
        assert result == {
            'e2-small': 178,
            'e2-medium': 179,
            'n4-highmem-2': 180,
        }

    def test_get_vector_search_instance_types_empty(self, api_client):
        """Test get_vector_search_instance_types with empty response."""
        # Mock empty response
        api_client.request = Mock(return_value={'instances': []})

        result = api_client.get_vector_search_instance_types(
            cloud_provider_id=1, region_id=1)

        # Verify the result is an empty dictionary
        assert result == {}
