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

"""Tests for GCE cluster multi-AZ node placement."""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster_gce import GCECluster


def _instance(name: str, zone: str) -> MagicMock:
    instance = MagicMock()
    instance.name = name
    instance.zone = f"projects/test-project/zones/{zone}"
    return instance


def _cluster(availability_zone: str, region: str = "us-east1") -> MagicMock:
    cluster = MagicMock(spec=GCECluster)
    cluster.log = MagicMock()
    cluster.params = MagicMock()
    cluster.params.get.side_effect = lambda key, *a, **kw: {"availability_zone": availability_zone}.get(key)
    provisioner = MagicMock()
    provisioner.region = region
    provisioner.availability_zones = [f"{region}-{letter}" for letter in availability_zone.split(",")]
    cluster.provisioners = [provisioner]
    return cluster


@pytest.mark.parametrize(
    "az_idx,expected",
    [
        pytest.param(0, ["db-1", "db-4"], id="rack-0"),
        pytest.param(1, ["db-2", "db-5"], id="rack-1"),
        pytest.param(2, ["db-3", "db-6"], id="rack-2"),
    ],
)
def test_instances_in_az_buckets_by_configured_zone_order(az_idx, expected):
    """Each rack must see only the instances living in its own zone."""
    cluster = _cluster("a,b,c")
    instances = [
        _instance("db-1", "us-east1-a"),
        _instance("db-2", "us-east1-b"),
        _instance("db-3", "us-east1-c"),
        _instance("db-4", "us-east1-a"),
        _instance("db-5", "us-east1-b"),
        _instance("db-6", "us-east1-c"),
    ]

    found = GCECluster._instances_in_az(cluster, instances, az_idx)

    assert [instance.name for instance in found] == expected


def test_instances_in_az_finds_instances_moved_to_an_unconfigured_zone():
    """Bucketing goes by the real zone, so AZ fallback placements are still found."""
    cluster = _cluster("a,b")
    instances = [_instance("db-1", "us-east1-a"), _instance("db-2", "us-east1-d")]

    assert [i.name for i in GCECluster._instances_in_az(cluster, instances, 0)] == ["db-1"]
    assert [i.name for i in GCECluster._instances_in_az(cluster, instances, 1)] == ["db-2"]


def test_instances_in_az_returns_nothing_for_a_zone_without_instances():
    cluster = _cluster("a,b,c")

    assert GCECluster._instances_in_az(cluster, [_instance("db-1", "us-east1-a")], 2) == []


@pytest.mark.parametrize("availability_zone", ["a", ""], ids=["single-az", "no-az"])
def test_instances_in_az_is_a_no_op_for_a_single_rack(availability_zone):
    """With one rack there is nothing to split: every instance of the DC belongs to it."""
    cluster = _cluster(availability_zone)
    instances = [_instance("db-1", "us-east1-a"), _instance("db-2", "us-east1-b")]

    assert GCECluster._instances_in_az(cluster, instances, 0) == instances


def test_get_instances_by_name_searches_every_zone_of_the_region():
    """A multi-AZ cluster spreads nodes over zones, so the search cannot be pinned to one zone."""
    cluster = _cluster("a,b,c")
    wanted = _instance("db-3", "us-east1-c")
    cluster._gce_service = MagicMock()
    cluster._gce_service.aggregated_list.return_value = [
        ("zones/us-east1-a", MagicMock(instances=[_instance("db-1", "us-east1-a")])),
        ("zones/us-east1-c", MagicMock(instances=[wanted])),
        ("zones/us-west1-c", MagicMock(instances=[_instance("db-3", "us-west1-c")])),
    ]
    cluster.project = "test-project"

    assert GCECluster._get_instances_by_name(cluster, name="db-3", dc_idx=0) is wanted


def test_get_instances_by_name_ignores_other_regions():
    cluster = _cluster("a", region="us-east1")
    cluster._gce_service = MagicMock()
    cluster._gce_service.aggregated_list.return_value = [
        ("zones/us-east10-a", MagicMock(instances=[_instance("db-1", "us-east10-a")])),
    ]
    cluster.project = "test-project"

    assert GCECluster._get_instances_by_name(cluster, name="db-1", dc_idx=0) is None
