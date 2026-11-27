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

"""aws_placement_group_strategy: 'spread' puts only DB nodes in the group; 'cluster' is unchanged."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.aws import utils as aws_utils
from sdcm.provision.aws.utils import create_cluster_placement_groups_aws, joins_placement_group
from sdcm.sct_config.config import SCTConfiguration
from sdcm.sct_provision.aws.cluster import DBCluster, LoaderCluster, MonitoringCluster, PlacementGroup


class _Params(dict):
    region_names = ["us-east-1"]


def _params(**overrides) -> _Params:
    return _Params(
        {
            "cluster_backend": "aws",
            "user_prefix": "test",
            "use_placement_group": True,
            "aws_placement_group_strategy": "cluster",
            "availability_zone": "a",
            "n_db_nodes": [3],
            "use_capacity_reservation": False,
            "use_dedicated_host": False,
            **overrides,
        }
    )


def _group_names(params) -> dict:
    return {
        cls.__name__: cls.model_construct(params=params, test_id="testid123", common_tags={}).placement_group_name
        for cls in (PlacementGroup, DBCluster, LoaderCluster, MonitoringCluster)
    }


def test_cluster_strategy_keeps_db_and_loaders_in_the_group():
    names = _group_names(_params())
    assert names == {
        "PlacementGroup": "test-placement_group-testid12",
        "DBCluster": "test-placement_group-testid12",
        "LoaderCluster": "test-placement_group-testid12",
        "MonitoringCluster": None,
    }


def test_spread_strategy_keeps_only_db_nodes_in_the_group():
    names = _group_names(_params(aws_placement_group_strategy="spread"))
    assert names == {
        "PlacementGroup": "test-placement_group-testid12",
        "DBCluster": "test-placement_group-testid12",
        "LoaderCluster": None,
        "MonitoringCluster": None,
    }


@pytest.mark.parametrize(
    "strategy,node_type,joins",
    [
        ("cluster", "scylla-db", True),
        ("cluster", "loader", True),
        ("spread", "scylla-db", True),
        ("spread", "loader", False),
        ("spread", "monitor", False),
        ("spread", "oracle-db", False),
    ],
)
def test_runtime_added_nodes_follow_the_same_rule(strategy, node_type, joins):
    assert joins_placement_group(_params(aws_placement_group_strategy=strategy), node_type) is joins


@pytest.mark.parametrize("strategy", ["cluster", "spread"])
def test_group_is_created_with_the_configured_strategy(strategy):
    ec2 = MagicMock()
    with patch.dict(aws_utils.ec2_clients, {"us-east-1": ec2}):
        PlacementGroup.model_construct(
            params=_params(aws_placement_group_strategy=strategy), test_id="testid123", common_tags={}
        ).provision()
    assert ec2.create_placement_group.call_args.kwargs["Strategy"] == strategy


def test_group_creation_defaults_to_cluster():
    ec2 = MagicMock()
    with patch.dict(aws_utils.ec2_clients, {"us-east-1": ec2}):
        create_cluster_placement_groups_aws(name="pg", tags={}, region="us-east-1")
    assert ec2.create_placement_group.call_args.kwargs["Strategy"] == "cluster"


@pytest.mark.parametrize(
    "overrides,error",
    [
        ({"n_db_nodes": [8]}, "at most 7 instances per AZ, got 8 DB nodes"),
        ({"use_capacity_reservation": True}, "can't be used with use_capacity_reservation"),
        ({"use_dedicated_host": True}, "can't be used with use_dedicated_host"),
    ],
)
def test_spread_rejects_configs_aws_cannot_place(overrides, error):
    params = _params(aws_placement_group_strategy="spread", **overrides)
    with pytest.raises(AssertionError, match=error):
        SCTConfiguration._validate_placement_group_required_values(params)


@pytest.mark.parametrize(
    "overrides",
    [
        {"aws_placement_group_strategy": "spread", "n_db_nodes": [7]},
        {"n_db_nodes": [9], "use_capacity_reservation": True},
    ],
)
def test_valid_placement_configs_pass(overrides):
    SCTConfiguration._validate_placement_group_required_values(_params(**overrides))
