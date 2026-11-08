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
from sdcm.cluster_k8s.gke import MonitorSetGKE


def _cluster(zone: str) -> MagicMock:
    cluster = MagicMock(spec=GCECluster)
    cluster.log = MagicMock()
    provisioner = MagicMock()
    provisioner.availability_zone = zone
    cluster.provisioners = [provisioner]
    return cluster


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
    cluster = _cluster("us-east1-b")
    cluster.test_config.test_id.return_value = "t1"
    cluster.node_type = "scylla-db"
    cluster.nodes = []
    cluster._node_public_ips = []

    with patch("sdcm.utils.common.get_gce_compute_instances_client", return_value=(client, {"project_id": "p"})):
        found = GCECluster._get_instances(cluster, dc_idx=0)

    assert [i.name for i in found] == ["db-running"]


def test_gke_monitor_looks_up_only_running_instances():
    """The GKE monitor adopts instances the same way, so it must not see stopped VMs either."""
    monitor = MagicMock(spec=MonitorSetGKE)
    monitor.monitor_id = "m1"
    monitor.node_type = "monitor"
    monitor._get_instances_by_prefix.return_value = []
    monitor._node_public_ips = []

    with patch("sdcm.cluster_k8s.gke.list_instances_gce", return_value=[]) as list_instances:
        MonitorSetGKE._get_instances(monitor, dc_idx=0)

    assert list_instances.call_args.kwargs["running"] is True
