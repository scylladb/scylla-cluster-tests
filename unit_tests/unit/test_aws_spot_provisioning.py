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

from sdcm.provision.aws.constants import (
    EC2_FLEET_ALLOCATION_STRATEGY,
    EC2_FLEET_TYPE_INSTANT,
    SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
    SPOT_PRICE_TOO_LOW,
    STATUS_FULFILLED,
)
from sdcm.provision.aws.utils import (
    build_launch_template_data,
    count_instance_types_by_id,
    create_ec2_fleet_instance_request,
    create_launch_template,
    delete_ec2_fleet,
    filter_instance_types_offered,
    get_provisioned_spot_instance_ids,
    get_spot_request_instance_ids,
    is_ec2_fleet_retryable,
    split_instance_types,
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
        # aws_fleet_db_alternative_types is a list, so an actual list is the common input
        pytest.param(["i7i.large", "i4i.large"], ["i7i.large", "i4i.large"], id="list_input"),
        pytest.param(["i7i.large", "i7i.large"], ["i7i.large"], id="list_duplicates_removed"),
        pytest.param([" i7i.large ", "i4i.large"], ["i7i.large", "i4i.large"], id="list_whitespace_stripped"),
        pytest.param([], [], id="empty_list"),
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


@pytest.mark.parametrize(
    "tags, expected_tag_specifications",
    [
        pytest.param(
            [{"Key": "TestId", "Value": "test-1"}],
            [{"ResourceType": "launch-template", "Tags": [{"Key": "TestId", "Value": "test-1"}]}],
            id="tagged",
        ),
        pytest.param(None, None, id="untagged"),
    ],
)
def test_create_launch_template_tags_the_template(mock_ec2_client, tags, expected_tag_specifications):
    """SCT-779: the template itself is tagged, so clean-resources can find one left behind."""
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.create_launch_template.return_value = {"LaunchTemplate": {"LaunchTemplateId": "lt-1234"}}

    template_id = create_launch_template(
        region_name="us-east-1", template_name="sct-fleet-1", instance_parameters={"ImageId": "ami-1"}, tags=tags
    )

    assert template_id == "lt-1234"
    request = mock_client.create_launch_template.call_args.kwargs
    assert request["LaunchTemplateName"] == "sct-fleet-1"
    assert request.get("TagSpecifications") == expected_tag_specifications


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
    # plain `capacity-optimized` ignores priorities, which would make the primary type no more
    # likely to be picked than any alternative
    assert EC2_FLEET_ALLOCATION_STRATEGY == "capacity-optimized-prioritized"
    # all-or-nothing: AWS launches all 3 or none, instead of SCT launching a partial set and rolling it back
    assert request["SpotOptions"] == {
        "AllocationStrategy": EC2_FLEET_ALLOCATION_STRATEGY,
        "MinTargetCapacity": 3,
        "SingleAvailabilityZone": True,
    }
    launch_template_config = request["LaunchTemplateConfigs"][0]
    assert launch_template_config["LaunchTemplateSpecification"]["LaunchTemplateId"] == "lt-1234"
    # list order is preference order: the primary (first) type gets the highest priority, 0
    assert launch_template_config["Overrides"] == [
        {"InstanceType": "i7i.large", "Priority": 0.0},
        {"InstanceType": "i7ie.large", "Priority": 1.0},
        {"InstanceType": "i4i.large", "Priority": 2.0},
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
    assert request["OnDemandOptions"] == {"MinTargetCapacity": 1, "SingleAvailabilityZone": True}


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
    # running out of capacity is not transient: retrying the same pools right away won't help
    assert is_ec2_fleet_retryable(errors) is False


@pytest.mark.parametrize(
    "errors, expected",
    [
        pytest.param([], False, id="no_errors"),
        pytest.param([{"ErrorCode": "InsufficientInstanceCapacity"}], False, id="capacity_exhausted"),
        pytest.param([{"ErrorCode": "MaxSpotInstanceCountExceeded"}], False, id="account_limit"),
        pytest.param([{"ErrorCode": "SpotMaxPriceTooLow"}], False, id="price_too_low"),
        pytest.param([{"ErrorCode": "InvalidParameterValue"}], False, id="bad_request_is_not_retried"),
        pytest.param([{"ErrorCode": "RequestLimitExceeded"}], True, id="throttling_is_retryable"),
        pytest.param([{"ErrorCode": "InternalError"}], True, id="aws_internal_error_is_retryable"),
        pytest.param(
            [{"ErrorCode": "InsufficientInstanceCapacity"}, {"ErrorCode": "RequestLimitExceeded"}],
            True,
            id="throttled_pool_may_still_have_capacity",
        ),
    ],
)
def test_is_ec2_fleet_retryable(errors, expected):
    assert is_ec2_fleet_retryable(errors) is expected


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


def test_count_instance_types_by_id_reads_all_pages_in_one_paginated_call(mock_ec2_client):
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.get_paginator.return_value.paginate.return_value = [
        {"Reservations": [{"Instances": [{"InstanceType": "i7i.large"}, {"InstanceType": "i3en.large"}]}]},
        {"Reservations": [{"Instances": [{"InstanceType": "i7i.large"}]}]},
    ]

    counts = count_instance_types_by_id("us-east-1", ["i-1", "i-2", "i-3"])

    assert counts == {"i7i.large": 2, "i3en.large": 1}
    mock_client.get_paginator.assert_called_once_with("describe_instances")
    mock_client.get_paginator.return_value.paginate.assert_called_once_with(InstanceIds=["i-1", "i-2", "i-3"])


def test_count_instance_types_by_id_never_raises(mock_ec2_client):
    """It only feeds reporting, so an AWS error must yield an empty result rather than fail provisioning."""
    mock_ec2_client.__getitem__.return_value.get_paginator.side_effect = Exception("throttled")

    assert count_instance_types_by_id("us-east-1", ["i-1"]) == {}


def _offerings(mock_ec2_client, *offered):
    mock_client = MagicMock()
    mock_ec2_client.__getitem__.return_value = mock_client
    mock_client.get_paginator.return_value.paginate.return_value = [
        {"InstanceTypeOfferings": [{"InstanceType": instance_type} for instance_type in offered]}
    ]
    return mock_client


def test_alternatives_not_offered_in_the_az_are_dropped(mock_ec2_client):
    client = _offerings(mock_ec2_client, "i7i.large", "i4i.large")

    kept = filter_instance_types_offered("eu-west-1", "eu-west-1a", ["i7i.large", "i7ie.large", "i4i.large"])

    assert kept == ["i7i.large", "i4i.large"]
    client.get_paginator.return_value.paginate.assert_called_once_with(
        LocationType="availability-zone",
        Filters=[
            {"Name": "location", "Values": ["eu-west-1a"]},
            {"Name": "instance-type", "Values": ["i7i.large", "i7ie.large", "i4i.large"]},
        ],
    )


def test_the_preferred_type_is_never_dropped(mock_ec2_client):
    """Configuration already validated instance_type_db; the offering check only prunes alternatives."""
    _offerings(mock_ec2_client, "i4i.large")

    assert filter_instance_types_offered("eu-west-1", "eu-west-1a", ["i7i.large", "i4i.large"]) == [
        "i7i.large",
        "i4i.large",
    ]


@pytest.mark.parametrize(
    "availability_zone, instance_types",
    [
        pytest.param("eu-west-1a", ["i7i.large"], id="single_type"),
        pytest.param(None, ["i7i.large", "i4i.large"], id="no_az"),
    ],
)
def test_offering_check_is_skipped_when_there_is_nothing_to_prune(mock_ec2_client, availability_zone, instance_types):
    assert filter_instance_types_offered("eu-west-1", availability_zone, instance_types) == instance_types
    mock_ec2_client.__getitem__.assert_not_called()


def test_a_failed_offering_lookup_keeps_every_type(mock_ec2_client):
    mock_ec2_client.__getitem__.return_value.get_paginator.side_effect = Exception("throttled")

    assert filter_instance_types_offered("eu-west-1", "eu-west-1a", ["i7i.large", "i4i.large"]) == [
        "i7i.large",
        "i4i.large",
    ]


def test_spot_request_instance_ids_include_requests_that_are_not_fulfilled_yet(mock_ec2_client):
    """A request can launch its instance and still not count as fulfilled, e.g. while one of its siblings fails."""
    mock_ec2_client.__getitem__.return_value.describe_spot_instance_requests.return_value = {
        "SpotInstanceRequests": [
            {"SpotInstanceRequestId": "sir-1", "InstanceId": "i-1"},
            {"SpotInstanceRequestId": "sir-2"},
            {"SpotInstanceRequestId": "sir-3", "InstanceId": "i-3"},
        ]
    }

    assert get_spot_request_instance_ids("us-east-1", ["sir-1", "sir-2", "sir-3"]) == ["i-1", "i-3"]


def test_spot_request_instance_ids_never_describe_every_request_in_the_region(mock_ec2_client):
    """Without ids AWS would list every spot request in the region, including other tests' requests."""
    assert get_spot_request_instance_ids("us-east-1", []) == []
    mock_ec2_client.__getitem__.return_value.describe_spot_instance_requests.assert_not_called()


def test_spot_request_instance_ids_never_raise(mock_ec2_client):
    mock_ec2_client.__getitem__.return_value.describe_spot_instance_requests.side_effect = Exception("throttled")

    assert get_spot_request_instance_ids("us-east-1", ["sir-1"]) == []
