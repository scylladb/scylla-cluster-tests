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

"""Tests for GCE cluster instance lookup."""

from unittest.mock import MagicMock, patch

from google.cloud.compute_v1.types import Instance, Items, Metadata, NetworkInterface

from sdcm.cluster_gce import GCECluster


def test_get_instances_skips_a_stopped_vm():
    """A preempted VM keeps its internal IP, but must not be adopted as a node, or setup waits on SSH."""
    tags = Metadata(items=[Items(key="TestId", value="t1"), Items(key="NodeType", value="scylla-db")])
    instances = [
        Instance(
            name=name,
            status=status,
            zone="projects/p/zones/us-east1-b",
            metadata=tags,
            network_interfaces=[NetworkInterface(network_i_p="10.0.0.1")],
        )
        for name, status in [("db-running", "RUNNING"), ("db-preempted", "TERMINATED")]
    ]
    client = MagicMock()
    client.aggregated_list.return_value = [("zones/us-east1-b", MagicMock(instances=instances))]
    cluster = MagicMock(spec=GCECluster)
    cluster._gce_zone_names = ["us-east1-b"]
    cluster.test_config.test_id.return_value = "t1"
    cluster.node_type = "scylla-db"
    cluster.nodes = []
    cluster._node_public_ips = []

    with patch("sdcm.utils.common.get_gce_compute_instances_client", return_value=(client, {"project_id": "p"})):
        found = GCECluster._get_instances(cluster, dc_idx=0)

    assert [i.name for i in found] == ["db-running"]
