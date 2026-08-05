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

"""Unit tests for AWS spot instance and EC2 Fleet provisioning."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.aws.utils import (
    build_launch_template_data,
    create_ec2_fleet_instance_request,
    delete_ec2_fleet,
    get_provisioned_spot_instance_ids,
    is_ec2_fleet_unfulfillable,
    split_instance_types,
)
from sdcm.provision.aws.constants import (
    EC2_FLEET_ALLOCATION_STRATEGY,
    EC2_FLEET_TYPE_INSTANT,
    SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
    SPOT_PRICE_TOO_LOW,
    STATUS_FULFILLED,
)


@pytest.fixture
def mock_ec2_client():
    """Mock EC2 client for testing."""
    with patch("sdcm.provision.aws.utils.ec2_clients") as mock_clients:
        yield mock_clients


class TestGetProvisionedSpotInstanceIds:
    """Tests for get_provisioned_spot_instance_ids function."""

    @pytest.mark.parametrize(
        "test_case",
        [
            {
                "id": "successful_provisioning",
                "description": "Test successful spot instance provisioning",
                "region": "us-east-1",
                "request_id": "sir-12345",
                "response": {
                    "SpotInstanceRequests": [
                        {
                            "SpotInstanceRequestId": "sir-12345",
                            "Status": {"Code": STATUS_FULFILLED, "Message": "Request fulfilled"},
                            "State": "active",
                            "InstanceId": "i-12345",
                        }
                    ]
                },
                "expected_result": ["i-12345"],
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
                    "SpotInstanceRequests": [
                        {
                            "SpotInstanceRequestId": "sir-12345",
                            "Status": {
                                "Code": SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
                                "Message": "No capacity available in this AZ",
                            },
                            "State": "open",
                        }
                    ]
                },
                "expected_result": None,
                "expected_log_count": 1,
                "log_level": logging.ERROR,
                "expected_log_messages": [
                    "Critical spot provisioning failure",
                    "capacity-not-available",
                    "No capacity available in this AZ",
                    "sir-12345",
                ],
            },
            {
                "id": "price_too_low_error",
                "description": "Test price too low error logging",
                "region": "us-west-2",
                "request_id": "sir-67890",
                "response": {
                    "SpotInstanceRequests": [
                        {
                            "SpotInstanceRequestId": "sir-67890",
                            "Status": {
                                "Code": SPOT_PRICE_TOO_LOW,
                                "Message": "Your Spot request price is lower than the minimum",
                            },
                            "State": "open",
                        }
                    ]
                },
                "expected_result": None,
                "expected_log_count": 1,
                "log_level": logging.ERROR,
                "expected_log_messages": [
                    "Critical spot provisioning failure",
                    "price-too-low",
                    "Your Spot request price is lower",
                ],
            },
            {
                "id": "pending_request_warning",
                "description": "Test warning for pending spot request",
                "region": "eu-west-1",
                "request_id": "sir-pending",
                "response": {
                    "SpotInstanceRequests": [
                        {
                            "SpotInstanceRequestId": "sir-pending",
                            "Status": {
                                "Code": "pending-evaluation",
                                "Message": "Your Spot request is being evaluated",
                            },
                            "State": "open",
                        }
                    ]
                },
                "expected_result": [],
                "expected_log_count": 1,
                "log_level": logging.WARNING,
                "expected_log_messages": [
                    "Spot instance request not yet fulfilled",
                    "pending-evaluation",
                ],
            },
        ],
        ids=lambda tc: tc["id"],
    )
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
        mock_client.describe_spot_instance_requests.side_effect = Exception("API Error")

        with caplog.at_level(logging.ERROR):
            result = get_provisioned_spot_instance_ids("us-east-1", ["sir-error"])

        assert result == []
        assert len(caplog.records) == 1
        assert "Failed to describe spot instance requests" in caplog.records[0].message
        assert "API Error" in caplog.records[0].message


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("i7i.large", ["i7i.large"], id="single_type"),
        pytest.param("i7i.large,i7ie.large,i4i.large", ["i7i.large", "i7ie.large", "i4i.large"], id="three_types"),
        pytest.param(" i7i.large , i7ie.large ", ["i7i.large", "i7ie.large"], id="whitespace_is_stripped"),
        pytest.param("i7i.large,i7i.large", ["i7i.large"], id="duplicates_removed_order_kept"),
        pytest.param("i7i.large,,i4i.large", ["i7i.large", "i4i.large"], id="empty_entries_ignored"),
        pytest.param("", [], id="empty_value"),
        pytest.param(None, [], id="none_value"),
    ],
)
def test_split_instance_types(value, expected):
    assert split_instance_types(value) == expected


def test_build_launch_template_data_drops_unsupported_keys():
    """CreateLaunchTemplate rejects RunInstances-only keys; InstanceType comes from fleet overrides."""
    launch_template_data = build_launch_template_data(
        {
            "ImageId": "ami-1234",
            "KeyName": "sct-key",
            "InstanceType": "i7i.large",
            "AddressingType": "public",
            "SubnetId": "subnet-1234",
            "SecurityGroups": ["sg-1234"],
            "NetworkInterfaces": [{"DeviceIndex": 0, "SubnetId": "subnet-1234", "Groups": ["sg-1234"]}],
        }
    )

    assert "InstanceType" not in launch_template_data
    assert "AddressingType" not in launch_template_data
    assert "SubnetId" not in launch_template_data
    assert "SecurityGroups" not in launch_template_data
    assert launch_template_data["ImageId"] == "ami-1234"
    assert launch_template_data["NetworkInterfaces"][0]["SubnetId"] == "subnet-1234"


def test_create_ec2_fleet_request_diversifies_across_instance_types(mock_ec2_client):
    """The whole point of moving off Spot Fleet: one request, several candidate instance pools."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.create_fleet.return_value = {
        "FleetId": "fleet-1234",
        "Instances": [
            {"InstanceIds": ["i-1", "i-2"]},
            {"InstanceIds": ["i-3"]},
        ],
        "Errors": [],
    }

    fleet_id, instance_ids, errors = create_ec2_fleet_instance_request(
        region_name="us-east-1",
        count=3,
        template_id="lt-1234",
        instance_types=["i7i.large", "i7ie.large", "i4i.large"],
    )

    assert fleet_id == "fleet-1234"
    assert instance_ids == ["i-1", "i-2", "i-3"]
    assert errors == []

    request = mock_client.create_fleet.call_args.kwargs
    assert request["Type"] == EC2_FLEET_TYPE_INSTANT
    assert request["TargetCapacitySpecification"] == {
        "TotalTargetCapacity": 3,
        "DefaultTargetCapacityType": "spot",
    }
    assert request["SpotOptions"] == {"AllocationStrategy": EC2_FLEET_ALLOCATION_STRATEGY}
    launch_template_config = request["LaunchTemplateConfigs"][0]
    assert launch_template_config["LaunchTemplateSpecification"]["LaunchTemplateId"] == "lt-1234"
    assert launch_template_config["Overrides"] == [
        {"InstanceType": "i7i.large"},
        {"InstanceType": "i7ie.large"},
        {"InstanceType": "i4i.large"},
    ]


def test_create_ec2_fleet_request_on_demand_has_no_spot_options(mock_ec2_client):
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.create_fleet.return_value = {"FleetId": "fleet-1", "Instances": [{"InstanceIds": ["i-1"]}]}

    create_ec2_fleet_instance_request(
        region_name="us-east-1", count=1, template_id="lt-1", instance_types=["i7i.large"], spot=False
    )

    request = mock_client.create_fleet.call_args.kwargs
    assert request["TargetCapacitySpecification"]["DefaultTargetCapacityType"] == "on-demand"
    assert "SpotOptions" not in request


def test_create_ec2_fleet_request_returns_partial_fulfillment_with_errors(mock_ec2_client):
    """`instant` fleets can return fewer instances than requested plus per-pool errors."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.create_fleet.return_value = {
        "FleetId": "fleet-partial",
        "Instances": [{"InstanceIds": ["i-1"]}],
        "Errors": [
            {
                "ErrorCode": "InsufficientInstanceCapacity",
                "ErrorMessage": "There is no Spot capacity available",
                "LaunchTemplateAndOverrides": {"Overrides": {"InstanceType": "i7i.large"}},
            }
        ],
    }

    _, instance_ids, errors = create_ec2_fleet_instance_request(
        region_name="us-east-1", count=3, template_id="lt-1", instance_types=["i7i.large", "i4i.large"]
    )

    assert instance_ids == ["i-1"]
    assert is_ec2_fleet_unfulfillable(errors) is True


@pytest.mark.parametrize(
    "errors, expected",
    [
        pytest.param([], False, id="no_errors"),
        pytest.param([{"ErrorCode": "InsufficientInstanceCapacity"}], True, id="capacity_exhausted"),
        pytest.param([{"ErrorCode": "MaxSpotInstanceCountExceeded"}], True, id="account_limit"),
        pytest.param([{"ErrorCode": "SpotMaxPriceTooLow"}], True, id="price_too_low"),
        pytest.param([{"ErrorCode": "RequestLimitExceeded"}], False, id="throttling_is_retryable"),
    ],
)
def test_is_ec2_fleet_unfulfillable(errors, expected):
    assert is_ec2_fleet_unfulfillable(errors) is expected


def test_delete_ec2_fleet_is_best_effort(mock_ec2_client, caplog):
    """Cleanup must never mask the real provisioning failure."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.delete_fleets.side_effect = Exception("Fleet delete error")

    with caplog.at_level(logging.WARNING):
        delete_ec2_fleet(region_name="us-east-1", fleet_id="fleet-1", terminate_instances=True)

    assert "Failed to delete EC2 fleet" in caplog.records[0].message


def test_delete_ec2_fleet_terminates_instances_when_delete_raises(mock_ec2_client):
    """A failed delete must fall back to terminating the tracked instances (no leaks)."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.delete_fleets.side_effect = Exception("Fleet delete error")

    delete_ec2_fleet(
        region_name="us-east-1",
        fleet_id="fleet-1",
        terminate_instances=True,
        instance_ids=["i-1", "i-2"],
    )

    mock_client.terminate_instances.assert_called_once_with(InstanceIds=["i-1", "i-2"])


def test_delete_ec2_fleet_terminates_instances_on_unsuccessful_deletion(mock_ec2_client):
    """UnsuccessfulFleetDeletions in the response must trigger direct instance termination."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.delete_fleets.return_value = {
        "UnsuccessfulFleetDeletions": [{"FleetId": "fleet-1", "Error": {"Code": "fleetIdDoesNotExist"}}],
        "SuccessfulFleetDeletions": [],
    }

    delete_ec2_fleet(
        region_name="us-east-1",
        fleet_id="fleet-1",
        terminate_instances=True,
        instance_ids=["i-1"],
    )

    mock_client.terminate_instances.assert_called_once_with(InstanceIds=["i-1"])


def test_delete_ec2_fleet_no_terminate_on_successful_deletion(mock_ec2_client):
    """A clean deletion (no UnsuccessfulFleetDeletions) must not double-terminate instances."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.delete_fleets.return_value = {"UnsuccessfulFleetDeletions": [], "SuccessfulFleetDeletions": []}

    delete_ec2_fleet(
        region_name="us-east-1",
        fleet_id="fleet-1",
        terminate_instances=True,
        instance_ids=["i-1"],
    )

    mock_client.terminate_instances.assert_not_called()


def test_delete_ec2_fleet_ignores_missing_fleet_id(mock_ec2_client):
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client

    delete_ec2_fleet(region_name="us-east-1", fleet_id=None, terminate_instances=True)

    mock_client.delete_fleets.assert_not_called()
