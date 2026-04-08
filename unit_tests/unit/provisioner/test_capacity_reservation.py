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
# Copyright (c) 2024 ScyllaDB

from unittest.mock import MagicMock, patch
import pytest

from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.exceptions import CapacityReservationError


@pytest.fixture(autouse=True)
def reset_reservations():
    SCTCapacityReservation.reservations = {}


@pytest.fixture(name="params")
def params_fixture():
    class DotDict(dict):
        __getattr__ = dict.__getitem__
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__

    return DotDict(
        {
            "test_id": "example-test-id",
            "region_name": ["us-east-1"],
            "use_capacity_reservation": True,
            "instance_provision": "on_demand",
            "availability_zone": "d",
            "cluster_backend": "aws",
            "region_names": ["us-east-1"],
            "n_db_nodes": 1,
            "instance_type_db": "i4i.large",
            "cluster_target_size": 1,
            "n_loaders": 1,
            "instance_type_loader": "t3.small",
            "n_monitor_nodes": 1,
            "instance_type_monitor": "t3.small",
            "test_duration": 180,
        }
    )


@pytest.fixture(name="mock_ec2")
def mock_ec2_fixture():
    with patch("boto3.client") as mock_boto3_client:
        mock_ec2 = MagicMock()
        mock_boto3_client.return_value = mock_ec2
        mock_ec2.describe_instance_type_offerings.return_value = {
            "InstanceTypeOfferings": [
                {"Location": "us-east-1a", "InstanceType": "i4i.large"},
                {"Location": "us-east-1b", "InstanceType": "i4i.large"},
                {"Location": "us-east-1d", "InstanceType": "i4i.large"},
                {"Location": "us-east-1a", "InstanceType": "t3.small"},
                {"Location": "us-east-1d", "InstanceType": "t3.small"},
                {"Location": "us-east-1c", "InstanceType": "t3.micro"},
            ]
        }
        yield mock_ec2


def test_reserve_success(mock_ec2, params):
    mock_ec2.create_capacity_reservation.side_effect = [
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7d"}},
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7e"}},
    ]

    SCTCapacityReservation.reserve(params)
    reservations = SCTCapacityReservation.reservations
    assert len(reservations.keys()) == 1
    for key, value in reservations.items():
        assert key == "d"  # initial availability zone
        assert value == {"i4i.large": "cr-0e2dec33c962ffa7d", "t3.small": "cr-0e2dec33c962ffa7e"}


def test_reserve_failure(mock_ec2, params):
    mock_ec2.create_capacity_reservation.side_effect = Exception("Failed to create capacity reservation")

    with pytest.raises(CapacityReservationError):
        SCTCapacityReservation.reserve(params)


def test_reserve_fallback(mock_ec2, params):
    mock_ec2.create_capacity_reservation.side_effect = [
        Exception("Failed to create capacity reservation"),
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7d"}},
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7e"}},
    ]
    SCTCapacityReservation.reserve(params)
    result = SCTCapacityReservation.reservations
    assert len(result.keys()) == 1
    for value in result.values():
        assert value == {"i4i.large": "cr-0e2dec33c962ffa7d", "t3.small": "cr-0e2dec33c962ffa7e"}


def test_reservation_cancelled_when_one_reservation_fails(mock_ec2, params):
    mock_ec2.create_capacity_reservation.side_effect = [
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7d"}},
        Exception("Failed to create capacity reservation"),
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7e"}},
        {"CapacityReservation": {"CapacityReservationId": "cr-0e2dec33c962ffa7f"}},
    ]
    SCTCapacityReservation.reserve(params)
    result = SCTCapacityReservation.reservations
    assert len(result.keys()) == 1
    for value in result.values():
        assert value == {"i4i.large": "cr-0e2dec33c962ffa7e", "t3.small": "cr-0e2dec33c962ffa7f"}

    # verify can be cancelled
    SCTCapacityReservation.cancel(params)
    assert SCTCapacityReservation.reservations == {}


def test_can_cancel_reservation(mock_ec2, params):
    mock_ec2.describe_capacity_reservations.return_value = {
        "CapacityReservations": [
            {
                "AvailabilityZone": "us-east-1b",
                "CapacityReservationId": "cr-0e2dec33c962ffa7d",
                "InstanceType": "i4i.large",
                "State": "active",
            }
        ]
    }
    SCTCapacityReservation.get_cr_from_aws(params)
    SCTCapacityReservation.cancel(params)
    mock_ec2.cancel_capacity_reservation.assert_called_with(CapacityReservationId="cr-0e2dec33c962ffa7d")
