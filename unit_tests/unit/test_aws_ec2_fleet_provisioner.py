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
# Copyright (c) 2026 ScyllaDB

"""Unit tests for AWSInstanceProvisioner EC2 Fleet provisioning."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.aws.instance_parameters import AWSInstanceParams
from sdcm.provision.aws.provisioner import AWSInstanceProvisioner
from sdcm.provision.common.provisioner import ProvisionParameters


def make_instance_parameters(instance_type: str) -> AWSInstanceParams:
    return AWSInstanceParams(
        ImageId="ami-1234",
        KeyName="sct-key",
        InstanceType=instance_type,
        UserData="#!/bin/bash\necho hello",
        NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": "subnet-1234", "Groups": ["sg-1234"]}],
    )


@pytest.fixture
def provision_parameters() -> ProvisionParameters:
    return ProvisionParameters(name="test", region_name="us-east-1", availability_zone="a", spot=True)


@pytest.fixture
def fleet_mocks():
    """Patch every AWS-facing call used by the EC2 Fleet path."""
    with (
        patch("sdcm.provision.aws.provisioner.create_launch_template", return_value="lt-1234") as create_template,
        patch("sdcm.provision.aws.provisioner.delete_launch_template") as delete_template,
        patch("sdcm.provision.aws.provisioner.create_ec2_fleet_instance_request") as create_fleet,
        patch("sdcm.provision.aws.provisioner.delete_ec2_fleet") as delete_fleet,
        patch("sdcm.provision.aws.provisioner.set_tags_on_instances"),
        patch(
            "sdcm.provision.aws.provisioner.find_instance_by_id",
            side_effect=lambda region_name, instance_id: instance_id,
        ),
    ):
        yield {
            "create_template": create_template,
            "delete_template": delete_template,
            "create_fleet": create_fleet,
            "delete_fleet": delete_fleet,
        }


@pytest.mark.parametrize(
    "instance_parameters, expected_types",
    [
        pytest.param(["i7i.large"], ["i7i.large"], id="single_type"),
        pytest.param(
            ["i7i.large", "i7ie.large", "i4i.large"], ["i7i.large", "i7ie.large", "i4i.large"], id="three_types"
        ),
        pytest.param(["i7i.large", "i7i.large"], ["i7i.large"], id="duplicate_types_collapsed"),
    ],
)
def test_fleet_request_offers_every_configured_instance_type(
    provision_parameters, fleet_mocks, instance_parameters, expected_types
):
    fleet_mocks["create_fleet"].return_value = ("fleet-1", ["i-1", "i-2"], [])
    provisioner = AWSInstanceProvisioner()

    instances = provisioner._execute_ec2_fleet_instance_request(
        provision_parameters=provision_parameters,
        instance_parameters=[make_instance_parameters(instance_type) for instance_type in instance_parameters],
        count=2,
        tags=[{"NodeIndex": "1"}, {"NodeIndex": "2"}],
    )

    assert instances == ["i-1", "i-2"]
    assert fleet_mocks["create_fleet"].call_args.kwargs["instance_types"] == expected_types
    # instances were handed over to the cluster, so the fleet record must not terminate them
    fleet_mocks["delete_fleet"].assert_called_once_with(
        region_name="us-east-1", fleet_id="fleet-1", terminate_instances=False
    )
    fleet_mocks["delete_template"].assert_called_once_with(region_name="us-east-1", template_id="lt-1234")


def test_partial_fulfillment_is_rolled_back(provision_parameters, fleet_mocks):
    """A half-filled fleet is useless to SCT and would leak instances if kept."""
    fleet_mocks["create_fleet"].return_value = (
        "fleet-1",
        ["i-1"],
        [{"ErrorCode": "InsufficientInstanceCapacity", "ErrorMessage": "no capacity"}],
    )
    provisioner = AWSInstanceProvisioner()

    instances = provisioner._execute_ec2_fleet_instance_request(
        provision_parameters=provision_parameters,
        instance_parameters=[make_instance_parameters("i7i.large")],
        count=3,
        tags=[{"NodeIndex": str(idx)} for idx in range(3)],
    )

    assert instances == []
    fleet_mocks["delete_fleet"].assert_called_once_with(
        region_name="us-east-1", fleet_id="fleet-1", terminate_instances=True
    )
    fleet_mocks["delete_template"].assert_called_once()


def test_launch_template_is_deleted_when_fleet_request_raises(provision_parameters, fleet_mocks):
    fleet_mocks["create_fleet"].side_effect = Exception("boom")
    provisioner = AWSInstanceProvisioner()

    with pytest.raises(Exception, match="boom"):
        provisioner._execute_ec2_fleet_instance_request(
            provision_parameters=provision_parameters,
            instance_parameters=[make_instance_parameters("i7i.large")],
            count=2,
            tags=[{"NodeIndex": "1"}, {"NodeIndex": "2"}],
        )

    fleet_mocks["delete_template"].assert_called_once_with(region_name="us-east-1", template_id="lt-1234")


@pytest.mark.parametrize(
    "instance_parameters, expected",
    [
        pytest.param(make_instance_parameters("i7i.large"), 1, id="single_object_is_wrapped"),
        pytest.param([make_instance_parameters("i7i.large")], 1, id="list_passes_through"),
        pytest.param(
            [make_instance_parameters("i7i.large"), make_instance_parameters("i4i.large")], 2, id="multi_type_list"
        ),
    ],
)
def test_as_instance_parameters_list(instance_parameters, expected):
    assert len(AWSInstanceProvisioner._as_instance_parameters_list(instance_parameters)) == expected


def test_as_instance_parameters_list_rejects_empty_list():
    with pytest.raises(AssertionError):
        AWSInstanceProvisioner._as_instance_parameters_list([])


def test_small_batches_still_use_plain_spot_requests(provision_parameters):
    """Below SPOT_CNT_LIMIT nothing changes: no launch template, no fleet."""
    provisioner = AWSInstanceProvisioner()
    with (
        patch.object(provisioner, "_execute_spot_instance_request", return_value=["i-1"]) as spot_request,
        patch.object(provisioner, "_execute_ec2_fleet_instance_request") as fleet_request,
    ):
        provisioner._provision_spot_instances(
            provision_parameters=provision_parameters,
            instance_parameters=[make_instance_parameters("i7i.large"), make_instance_parameters("i4i.large")],
            count=1,
            tags=[{"NodeIndex": "1"}],
        )

    fleet_request.assert_not_called()
    # single-type APIs only get the preferred instance type
    assert spot_request.call_args.kwargs["instance_parameters"].InstanceType == "i7i.large"


def test_on_demand_uses_preferred_instance_type_only(provision_parameters):
    with (
        patch("sdcm.provision.aws.provisioner.ec2_services") as ec2_services,
        patch("sdcm.provision.aws.provisioner.set_tags_on_instances"),
    ):
        service = MagicMock()
        ec2_services.__getitem__.return_value = service
        service.create_instances.return_value = []

        AWSInstanceProvisioner._provision_on_demand_instances(
            provision_parameters=provision_parameters,
            instance_parameters=[make_instance_parameters("i7i.large"), make_instance_parameters("i4i.large")],
            count=1,
            tags=[{"NodeIndex": "1"}],
        )

    assert service.create_instances.call_args.kwargs["InstanceType"] == "i7i.large"
