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

"""`instance_provision: spot_fleet` on the LEGACY AWS path - mid-test `add_nodes` (cluster growth, nemesis).

The upfront provision-resources path goes through `AWSInstanceProvisioner`; nodes added during the test go
through `AWSCluster._create_instances` and `EC2ClientWrapper.create_spot_fleet`. Both must launch through EC2
Fleet with the DB instance type alternatives, all-or-nothing, and never fall back to on-demand.
"""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from sdcm.cluster_aws import AWSCluster
from sdcm.ec2_client import CreateSpotFleetError, EC2ClientWrapper
from sdcm.sct_provision.common.utils import INSTANCE_PROVISION_SPOT_FLEET

ALTERNATIVES = ["i7ie.large", "i4i.large"]


@pytest.fixture
def cluster():
    """A bare AWSCluster with only what `_create_instances`/`_create_spot_instances` touch."""
    cluster = AWSCluster.__new__(AWSCluster)
    cluster.instance_provision = INSTANCE_PROVISION_SPOT_FLEET
    cluster.region_names = ["eu-west-1"]
    cluster._ec2_instance_type = "i7i.large"
    cluster._ec2_ami_id = ["ami-1"]
    cluster._ec2_subnet_id = [[["subnet-1"]]]
    cluster._ec2_security_group_ids = [["sg-1"]]
    cluster._ec2_user_data = ""
    cluster._ec2_block_device_mappings = []
    cluster._credentials = [MagicMock(key_pair_name="sct-key")]
    cluster.placement_group_name = None
    cluster.node_type = "scylla-db"
    cluster.log = MagicMock()
    cluster.params = {
        "availability_zone": "a",
        "instance_type_db": "i7i.large",
        "aws_instance_type_db_alternatives": ALTERNATIVES,
        # spot_fleet must ignore this
        "instance_provision_fallback_on_demand": True,
    }
    with (
        patch.object(AWSCluster, "tags", new_callable=PropertyMock, return_value={"TestId": "t-1"}),
        patch.object(AWSCluster, "instance_profile_name", new_callable=PropertyMock, return_value=None),
        patch("sdcm.cluster_aws.network_interfaces_count", return_value=1),
        patch("sdcm.provision.common.spot_outcome.SpotProvisionOutcomeEvent"),
    ):
        yield cluster


@pytest.fixture
def ec2_wrapper():
    with patch("sdcm.cluster_aws.ec2_client.EC2ClientWrapper") as wrapper_cls:
        wrapper = wrapper_cls.return_value
        wrapper.get_subnet_info.return_value = {"AvailabilityZone": "eu-west-1a"}
        wrapper.create_spot_fleet.side_effect = lambda count, **_: [f"i-{n}" for n in range(count)]
        yield wrapper


@pytest.mark.parametrize("count", [1, 50])
def test_spot_fleet_add_nodes_never_falls_back_to_on_demand(cluster, ec2_wrapper, count):
    """Even a single node goes through EC2 Fleet, and a fleet failure raises rather than going on-demand."""
    ec2_wrapper.create_spot_fleet.side_effect = CreateSpotFleetError(
        error_response={"Error": {"Code": "capacity-not-available", "Message": "no capacity"}},
        operation_name="create_spot_fleet",
    )
    with (
        patch.object(AWSCluster, "_create_on_demand_instances") as on_demand,
        pytest.raises(CreateSpotFleetError),
    ):
        cluster._create_instances(count=count)

    on_demand.assert_not_called()
    ec2_wrapper.create_spot_instances.assert_not_called()


def test_spot_fleet_add_nodes_is_one_request_with_the_db_alternatives(cluster, ec2_wrapper):
    instances = cluster._create_instances(count=50)

    assert len(instances) == 50
    ec2_wrapper.create_spot_fleet.assert_called_once()
    call = ec2_wrapper.create_spot_fleet.call_args.kwargs
    assert call["count"] == 50
    assert call["instance_type"] == "i7i.large"
    assert call["alternative_instance_types"] == ALTERNATIVES


@pytest.mark.parametrize(
    "node_type, instance_type",
    [
        pytest.param("loader", "i7i.large", id="loader_has_no_alternatives_option"),
        pytest.param("scylla-db", "i4i.xlarge", id="explicit_type_eg_zero_token_or_migration_target"),
    ],
)
def test_only_db_nodes_of_instance_type_db_get_alternatives(cluster, ec2_wrapper, node_type, instance_type):
    cluster.node_type = node_type
    cluster._create_instances(count=3, instance_type=instance_type)

    assert ec2_wrapper.create_spot_fleet.call_args.kwargs["alternative_instance_types"] == []


@pytest.fixture
def wrapper():
    with patch("sdcm.ec2_client.boto3"):
        yield EC2ClientWrapper(region_name="eu-west-1")


def test_create_spot_fleet_launches_through_ec2_fleet(wrapper):
    tags = [{"Key": "TestId", "Value": "t-1"}, {"Key": "Name", "Value": "db-node"}]
    with (
        patch("sdcm.ec2_client.provision_ec2_fleet", return_value=["i-1", "i-2"]) as provision,
        patch.object(EC2ClientWrapper, "add_tags"),
        patch.object(EC2ClientWrapper, "get_instance", side_effect=lambda instance_id: instance_id),
    ):
        instances = wrapper.create_spot_fleet(
            instance_type="i7i.large",
            image_id="ami-1",
            region_name="eu-west-1a",
            network_if=[{"DeviceIndex": 0, "SubnetId": "subnet-1", "Groups": ["sg-1"]}],
            key_pair="sct-key",
            user_data="echo hi",
            count=2,
            tag_specifications=[{"Tags": tags}],
            alternative_instance_types=["i7ie.large", "i7i.large"],
        )

    assert instances == ["i-1", "i-2"]
    call = provision.call_args.kwargs
    assert call["region_name"] == "eu-west-1"
    assert call["count"] == 2
    # preferred first, duplicates collapsed
    assert call["instance_types"] == ["i7i.large", "i7ie.large"]
    # the per-node Name is not a property of the shared fleet and launch template
    assert call["resource_tags"] == [{"Key": "TestId", "Value": "t-1"}]
    parameters = call["instance_parameters"]
    assert parameters["Placement"] == {"AvailabilityZone": "eu-west-1a"}
    assert parameters["KeyName"] == "sct-key"
    assert parameters["UserData"] == "ZWNobyBoaQ=="  # base64, as launch templates require
    assert parameters["TagSpecifications"] == [{"ResourceType": "instance", "Tags": tags}]


def test_create_spot_fleet_raises_when_aws_cannot_launch_them_all(wrapper):
    with (
        patch("sdcm.ec2_client.provision_ec2_fleet", return_value=[]),
        pytest.raises(CreateSpotFleetError, match="could not launch 3 spot instances"),
    ):
        wrapper.create_spot_fleet(
            instance_type="i7i.large",
            image_id="ami-1",
            region_name="eu-west-1a",
            network_if=[],
            count=3,
            tag_specifications=[{"Tags": [{"Key": "TestId", "Value": "t-1"}]}],
        )
