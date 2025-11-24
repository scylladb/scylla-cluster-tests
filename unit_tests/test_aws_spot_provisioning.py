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

    @pytest.mark.parametrize("test_case", [
        {
            "id": "successful_provisioning",
            "description": "Test successful spot instance provisioning",
            "region": "us-east-1",
            "request_id": "sir-12345",
            "response": {
                'SpotInstanceRequests': [{
                    'SpotInstanceRequestId': 'sir-12345',
                    'Status': {'Code': STATUS_FULFILLED, 'Message': 'Request fulfilled'},
                    'State': 'active',
                    'InstanceId': 'i-12345',
                }]
            },
            "expected_result": ['i-12345'],
            "expected_log_count": 0,
            "log_level": logging.INFO,
            "expected_log_messages": [],
        },
        {
            "id": "capacity_not_available_error",
            "description": "Test capacity not available error logging",
            "region": "us-east-1",
            "request_id": "sir-12345",
            "response": {
                'SpotInstanceRequests': [{
                    'SpotInstanceRequestId': 'sir-12345',
                    'Status': {
                        'Code': SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
                        'Message': 'No capacity available in this AZ',
                    },
                    'State': 'open',
                }]
            },
            "expected_result": None,
            "expected_log_count": 1,
            "log_level": logging.ERROR,
            "expected_log_messages": [
                'Critical spot provisioning failure',
                'capacity-not-available',
                'No capacity available in this AZ',
                'sir-12345',
            ],
        },
        {
            "id": "price_too_low_error",
            "description": "Test price too low error logging",
            "region": "us-west-2",
            "request_id": "sir-67890",
            "response": {
                'SpotInstanceRequests': [{
                    'SpotInstanceRequestId': 'sir-67890',
                    'Status': {
                        'Code': SPOT_PRICE_TOO_LOW,
                        'Message': 'Your Spot request price is lower than the minimum',
                    },
                    'State': 'open',
                }]
            },
            "expected_result": None,
            "expected_log_count": 1,
            "log_level": logging.ERROR,
            "expected_log_messages": [
                'Critical spot provisioning failure',
                'price-too-low',
                'Your Spot request price is lower',
            ],
        },
        {
            "id": "pending_request_warning",
            "description": "Test warning for pending spot request",
            "region": "eu-west-1",
            "request_id": "sir-pending",
            "response": {
                'SpotInstanceRequests': [{
                    'SpotInstanceRequestId': 'sir-pending',
                    'Status': {
                        'Code': 'pending-evaluation',
                        'Message': 'Your Spot request is being evaluated',
                    },
                    'State': 'open',
                }]
            },
            "expected_result": [],
            "expected_log_count": 1,
            "log_level": logging.WARNING,
            "expected_log_messages": [
                'Spot instance request not yet fulfilled',
                'pending-evaluation',
            ],
        },
    ], ids=lambda tc: tc["id"])
    def test_spot_instance_scenarios(self, mock_ec2_client, caplog, test_case):
        """Test various spot instance provisioning scenarios."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        mock_client.describe_spot_instance_requests.return_value = test_case["response"]

        with caplog.at_level(test_case["log_level"]):
            result = get_provisioned_spot_instance_ids(test_case["region"], [test_case["request_id"]])

        assert result == test_case["expected_result"]
        assert len(caplog.records) == test_case["expected_log_count"]
        for expected_msg in test_case["expected_log_messages"]:
            assert expected_msg in caplog.records[0].message if caplog.records else True

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

    @pytest.mark.parametrize("test_case", [
        {
            "id": "successful_fleet_provisioning",
            "description": "Test successful spot fleet provisioning",
            "region": "us-east-1",
            "request_id": "sfr-12345",
            "fleet_response": {
                'SpotFleetRequestConfigs': [{
                    'SpotFleetRequestId': 'sfr-12345',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': STATUS_FULFILLED,
                }]
            },
            "instances_response": {
                'ActiveInstances': [
                    {'InstanceId': 'i-fleet-1'},
                    {'InstanceId': 'i-fleet-2'},
                ]
            },
            "expected_result": ['i-fleet-1', 'i-fleet-2'],
            "expected_log_count": 0,
            "log_level": logging.INFO,
            "expected_log_messages": [],
        },
        {
            "id": "fleet_capacity_error",
            "description": "Test fleet capacity not available error logging",
            "region": "us-east-1",
            "request_id": "sfr-error",
            "fleet_response": {
                'SpotFleetRequestConfigs': [{
                    'SpotFleetRequestId': 'sfr-error',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': SPOT_STATUS_UNEXPECTED_ERROR,
                }]
            },
            "history_response": {
                'HistoryRecords': [{
                    'EventType': 'error',
                    'EventInformation': {
                        'EventSubType': SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
                        'EventDescription': 'Insufficient capacity in availability zone',
                    }
                }]
            },
            "expected_result": None,
            "expected_log_count": 1,
            "log_level": logging.ERROR,
            "expected_log_messages": [
                'Critical spot fleet provisioning failure',
                'capacity-not-available',
                'Insufficient capacity',
            ],
        },
        {
            "id": "fleet_limit_exceeded_error",
            "description": "Test fleet limit exceeded error logging",
            "region": "us-west-1",
            "request_id": "sfr-limit",
            "fleet_response": {
                'SpotFleetRequestConfigs': [{
                    'SpotFleetRequestId': 'sfr-limit',
                    'SpotFleetRequestState': 'failed',
                    'ActivityStatus': SPOT_STATUS_UNEXPECTED_ERROR,
                }]
            },
            "history_response": {
                'HistoryRecords': [{
                    'EventType': 'fleetRequestChange',
                    'EventInformation': {
                        'EventSubType': FLEET_LIMIT_EXCEEDED_ERROR,
                        'EventDescription': 'You have exceeded your spot instance limit',
                    }
                }]
            },
            "expected_result": None,
            "expected_log_count": 1,
            "log_level": logging.ERROR,
            "expected_log_messages": [
                'Critical spot fleet provisioning failure',
                'spotInstanceCountLimitExceeded',
                'exceeded your spot instance limit',
            ],
        },
        {
            "id": "fleet_pending_warning",
            "description": "Test warning for pending fleet request",
            "region": "ap-south-1",
            "request_id": "sfr-pending",
            "fleet_response": {
                'SpotFleetRequestConfigs': [{
                    'SpotFleetRequestId': 'sfr-pending',
                    'SpotFleetRequestState': 'active',
                    'ActivityStatus': 'pending_fulfillment',
                }]
            },
            "expected_result": [],
            "expected_log_count": 1,
            "log_level": logging.WARNING,
            "expected_log_messages": [
                'Spot fleet request not yet fulfilled',
            ],
        },
    ], ids=lambda tc: tc["id"])
    def test_fleet_scenarios(self, mock_ec2_client, caplog, test_case):
        """Test various spot fleet provisioning scenarios."""
        mock_client = MagicMock()
        mock_ec2_client.__getitem__.return_value = mock_client
        mock_client.describe_spot_fleet_requests.return_value = test_case["fleet_response"]

        # Set up history response if needed
        if "history_response" in test_case:
            mock_client.describe_spot_fleet_request_history.return_value = test_case["history_response"]

        # Set up instances response if needed
        if "instances_response" in test_case:
            mock_client.describe_spot_fleet_instances.return_value = test_case["instances_response"]

        with caplog.at_level(test_case["log_level"]):
            result = get_provisioned_fleet_instance_ids(test_case["region"], [test_case["request_id"]])

        assert result == test_case["expected_result"]
        assert len(caplog.records) == test_case["expected_log_count"]
        for expected_msg in test_case["expected_log_messages"]:
            assert expected_msg in caplog.records[0].message if caplog.records else True

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
            'SpotFleetRequestConfigs': [{
                'SpotFleetRequestId': 'sfr-12345',
                'SpotFleetRequestState': 'active',
                'ActivityStatus': STATUS_FULFILLED,
            }]
        }
        mock_client.describe_spot_fleet_instances.side_effect = Exception('Instance describe error')

        with caplog.at_level(logging.ERROR):
            result = get_provisioned_fleet_instance_ids('us-east-1', ['sfr-12345'])

        assert result is None
        assert len(caplog.records) == 1
        assert 'Failed to describe spot fleet instances' in caplog.records[0].message
        assert 'Instance describe error' in caplog.records[0].message
