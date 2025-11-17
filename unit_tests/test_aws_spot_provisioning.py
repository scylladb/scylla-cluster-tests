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

"""Unit tests for AWS spot instance provisioning error handling."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.aws.utils import (
    get_provisioned_spot_instance_ids,
    get_provisioned_fleet_instance_ids,
)
from sdcm.provision.aws.constants import (
    SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
    SPOT_PRICE_TOO_LOW,
    STATUS_FULFILLED,
    SPOT_STATUS_UNEXPECTED_ERROR,
    FLEET_LIMIT_EXCEEDED_ERROR,
)


@pytest.fixture
def mock_ec2_client():
    """Mock EC2 client for testing."""
    with patch('sdcm.provision.aws.utils.ec2_clients') as mock_clients:
        yield mock_clients


class TestGetProvisionedSpotInstanceIds:
    """Tests for get_provisioned_spot_instance_ids function."""

    def test_successful_provisioning(self, mock_ec2_client, caplog):
        """Test successful spot instance provisioning."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_instance_requests.return_value = {
            'SpotInstanceRequests': [
                {
                    'SpotInstanceRequestId': 'sir-12345',
                    'Status': {'Code': STATUS_FULFILLED, 'Message': 'Request fulfilled'},
                    'State': 'active',
                    'InstanceId': 'i-12345',
                }
            ]
        }
        
        with caplog.at_level(logging.INFO):
            result = get_provisioned_spot_instance_ids('us-east-1', ['sir-12345'])
        
        assert result == ['i-12345']
        assert len(caplog.records) == 0  # No errors or warnings

    def test_capacity_not_available_error(self, mock_ec2_client, caplog):
        """Test capacity not available error logging."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_instance_requests.return_value = {
            'SpotInstanceRequests': [
                {
                    'SpotInstanceRequestId': 'sir-12345',
                    'Status': {
                        'Code': SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
                        'Message': 'No capacity available in this AZ',
                    },
                    'State': 'open',
                }
            ]
        }
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_spot_instance_ids('us-east-1', ['sir-12345'])
        
        assert result is None
        assert len(caplog.records) == 1
        assert 'Critical spot provisioning failure' in caplog.records[0].message
        assert 'capacity-not-available' in caplog.records[0].message
        assert 'No capacity available in this AZ' in caplog.records[0].message
        assert 'sir-12345' in caplog.records[0].message

    def test_price_too_low_error(self, mock_ec2_client, caplog):
        """Test price too low error logging."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_instance_requests.return_value = {
            'SpotInstanceRequests': [
                {
                    'SpotInstanceRequestId': 'sir-67890',
                    'Status': {
                        'Code': SPOT_PRICE_TOO_LOW,
                        'Message': 'Your Spot request price is lower than the minimum',
                    },
                    'State': 'open',
                }
            ]
        }
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_spot_instance_ids('us-west-2', ['sir-67890'])
        
        assert result is None
        assert len(caplog.records) == 1
        assert 'Critical spot provisioning failure' in caplog.records[0].message
        assert 'price-too-low' in caplog.records[0].message
        assert 'Your Spot request price is lower' in caplog.records[0].message

    def test_pending_request_warning(self, mock_ec2_client, caplog):
        """Test warning for pending spot request."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_instance_requests.return_value = {
            'SpotInstanceRequests': [
                {
                    'SpotInstanceRequestId': 'sir-pending',
                    'Status': {
                        'Code': 'pending-evaluation',
                        'Message': 'Your Spot request is being evaluated',
                    },
                    'State': 'open',
                }
            ]
        }
        
        with caplog.at_level(logging.WARNING):
            result = get_provisioned_spot_instance_ids('eu-west-1', ['sir-pending'])
        
        assert result == []
        assert len(caplog.records) == 1
        assert 'Spot instance request not yet fulfilled' in caplog.records[0].message
        assert 'pending-evaluation' in caplog.records[0].message

    def test_api_exception_handling(self, mock_ec2_client, caplog):
        """Test exception handling for API errors."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_instance_requests.side_effect = Exception('API Error')
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_spot_instance_ids('us-east-1', ['sir-error'])
        
        assert result == []
        assert len(caplog.records) == 1
        assert 'Failed to describe spot instance requests' in caplog.records[0].message
        assert 'API Error' in caplog.records[0].message


class TestGetProvisionedFleetInstanceIds:
    """Tests for get_provisioned_fleet_instance_ids function."""

    def test_successful_fleet_provisioning(self, mock_ec2_client, caplog):
        """Test successful spot fleet provisioning."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [
                {
                    'SpotFleetRequestId': 'sfr-12345',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': STATUS_FULFILLED,
                }
            ]
        }
        
        mock_client.describe_spot_fleet_instances.return_value = {
            'ActiveInstances': [
                {'InstanceId': 'i-fleet-1'},
                {'InstanceId': 'i-fleet-2'},
            ]
        }
        
        with caplog.at_level(logging.INFO):
            result = get_provisioned_fleet_instance_ids('us-east-1', ['sfr-12345'])
        
        assert result == ['i-fleet-1', 'i-fleet-2']
        assert len(caplog.records) == 0  # No errors or warnings

    def test_fleet_capacity_error(self, mock_ec2_client, caplog):
        """Test fleet capacity not available error logging."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [
                {
                    'SpotFleetRequestId': 'sfr-error',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': SPOT_STATUS_UNEXPECTED_ERROR,
                }
            ]
        }
        
        mock_client.describe_spot_fleet_request_history.return_value = {
            'HistoryRecords': [
                {
                    'EventType': 'error',
                    'EventInformation': {
                        'EventSubType': SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
                        'EventDescription': 'Insufficient capacity in availability zone',
                    }
                }
            ]
        }
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_fleet_instance_ids('us-east-1', ['sfr-error'])
        
        assert result is None
        assert len(caplog.records) == 1
        assert 'Critical spot fleet provisioning failure' in caplog.records[0].message
        assert 'capacity-not-available' in caplog.records[0].message
        assert 'Insufficient capacity' in caplog.records[0].message

    def test_fleet_limit_exceeded_error(self, mock_ec2_client, caplog):
        """Test fleet limit exceeded error logging."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [
                {
                    'SpotFleetRequestId': 'sfr-limit',
                    'SpotFleetRequestState': 'failed',
                    'ActivityStatus': SPOT_STATUS_UNEXPECTED_ERROR,
                }
            ]
        }
        
        mock_client.describe_spot_fleet_request_history.return_value = {
            'HistoryRecords': [
                {
                    'EventType': 'fleetRequestChange',
                    'EventInformation': {
                        'EventSubType': FLEET_LIMIT_EXCEEDED_ERROR,
                        'EventDescription': 'You have exceeded your spot instance limit',
                    }
                }
            ]
        }
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_fleet_instance_ids('us-west-1', ['sfr-limit'])
        
        assert result is None
        assert len(caplog.records) == 1
        assert 'Critical spot fleet provisioning failure' in caplog.records[0].message
        assert 'spotInstanceCountLimitExceeded' in caplog.records[0].message
        assert 'exceeded your spot instance limit' in caplog.records[0].message

    def test_fleet_pending_warning(self, mock_ec2_client, caplog):
        """Test warning for pending fleet request."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [
                {
                    'SpotFleetRequestId': 'sfr-pending',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': 'pending_fulfillment',
                }
            ]
        }
        
        with caplog.at_level(logging.WARNING):
            result = get_provisioned_fleet_instance_ids('ap-south-1', ['sfr-pending'])
        
        assert result == []
        assert len(caplog.records) == 1
        assert 'Spot fleet request not yet fulfilled' in caplog.records[0].message

    def test_fleet_api_exception(self, mock_ec2_client, caplog):
        """Test exception handling for fleet API errors."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_fleet_requests.side_effect = Exception('Fleet API Error')
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_fleet_instance_ids('eu-central-1', ['sfr-error'])
        
        assert result == []
        assert len(caplog.records) == 1
        assert 'Failed to describe spot fleet requests' in caplog.records[0].message
        assert 'Fleet API Error' in caplog.records[0].message

    def test_fleet_instances_describe_exception(self, mock_ec2_client, caplog):
        """Test exception handling when describing fleet instances."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        
        mock_client.describe_spot_fleet_requests.return_value = {
            'SpotFleetRequestConfigs': [
                {
                    'SpotFleetRequestId': 'sfr-12345',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': STATUS_FULFILLED,
                }
            ]
        }
        
        mock_client.describe_spot_fleet_instances.side_effect = Exception('Instance describe error')
        
        with caplog.at_level(logging.ERROR):
            result = get_provisioned_fleet_instance_ids('us-east-1', ['sfr-12345'])
        
        assert result is None
        assert len(caplog.records) == 1
        assert 'Failed to describe spot fleet instances' in caplog.records[0].message
        assert 'Instance describe error' in caplog.records[0].message
