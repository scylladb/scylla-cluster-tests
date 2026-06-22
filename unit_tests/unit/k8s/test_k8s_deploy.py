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

"""Unit tests for deploy_k8s_eks_cluster and deploy_k8s_gke_cluster.

Regression: IntOrList normalization means params.get("n_loaders") now always
returns list[int]. Both deploy functions used:

    if params.get("n_loaders"):

which is truthy for [0] — creating an empty loader node pool even when no
loaders are configured. Fix: if sum(params.get("n_loaders")) > 0:

All k8s_cluster calls are absorbed by MagicMock; EksNodePool / GkeNodePool
are patched so no real AWS/GCE API calls are made.
"""

from unittest.mock import MagicMock, patch


from sdcm.cluster_k8s.eks import deploy_k8s_eks_cluster
from sdcm.cluster_k8s.gke import deploy_k8s_gke_cluster


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

EKS_PARAMS = {
    "n_db_nodes": [3],
    "n_loaders": None,  # overridden per test
    "n_monitor_nodes": [1],
    "k8s_n_auxiliary_nodes": 3,
    "k8s_instance_type_auxiliary": "m5.xlarge",
    "instance_type_db": "i4i.large",
    "instance_type_loader": "i4i.large",
    "instance_type_monitor": "m5.xlarge",
    "eks_nodegroup_role_arn": "arn:aws:iam::123456789:role/fake-role",
    "root_disk_size_db": 100,
    "root_disk_size_monitor": 100,
    "k8s_deploy_monitoring": False,
    "k8s_enable_sni": False,
    "k8s_use_chaos_mesh": False,
    "use_mgmt": False,
    "k8s_n_monitor_nodes": None,
    "k8s_instance_type_monitor": None,
}

GKE_PARAMS = {
    "n_db_nodes": [3],
    "n_loaders": None,  # overridden per test
    "n_monitor_nodes": [1],
    "gce_instance_type_db": "n2-highmem-4",
    "gce_instance_type_loader": "e2-standard-2",
    "gce_instance_type_monitor": "e2-standard-4",
    "gce_root_disk_type_db": "pd-ssd",
    "gce_n_local_ssd_disk_db": 0,
    "root_disk_size_db": 100,
    "root_disk_size_monitor": 100,
    "gce_root_disk_type_monitor": "pd-ssd",
    "gce_n_local_ssd_disk_monitor": 0,
    "k8s_deploy_monitoring": False,
    "k8s_enable_sni": False,
    "k8s_use_chaos_mesh": False,
    "use_mgmt": False,
    "k8s_n_monitor_nodes": None,
    "k8s_instance_type_monitor": None,
}


def make_k8s_cluster(params: dict) -> MagicMock:
    """Build a MagicMock k8s_cluster with real string pool-name constants."""
    k8s_cluster = MagicMock()
    k8s_cluster.AUXILIARY_POOL_NAME = "auxiliary-pool"
    k8s_cluster.SCYLLA_POOL_NAME = "scylla-pool"
    k8s_cluster.LOADER_POOL_NAME = "loader-pool"
    k8s_cluster.MONITORING_POOL_NAME = "monitoring-pool"
    k8s_cluster.params = params
    return k8s_cluster


# ---------------------------------------------------------------------------
# EKS — deploy_k8s_eks_cluster
# ---------------------------------------------------------------------------


@patch("sdcm.cluster_k8s.eks.EksNodePool")
def test_deploy_eks_does_not_create_loader_pool_when_n_loaders_zero(mock_eks_pool):
    """n_loaders=[0] must not create a loader node pool.

    Regression: 'if params.get("n_loaders"):' was truthy for [0].
    """
    k8s_cluster = make_k8s_cluster(EKS_PARAMS | {"n_loaders": [0]})

    deploy_k8s_eks_cluster(k8s_cluster)

    instantiated_names = [c.kwargs["name"] for c in mock_eks_pool.call_args_list]
    assert "loader-pool" not in instantiated_names


@patch("sdcm.cluster_k8s.eks.EksNodePool")
def test_deploy_eks_creates_loader_pool_when_n_loaders_nonzero(mock_eks_pool):
    """n_loaders=[2] must create a loader node pool with num_nodes=2."""
    k8s_cluster = make_k8s_cluster(EKS_PARAMS | {"n_loaders": [2]})

    deploy_k8s_eks_cluster(k8s_cluster)

    mock_eks_pool.assert_any_call(
        name="loader-pool",
        num_nodes=2,
        instance_type="i4i.large",
        role_arn="arn:aws:iam::123456789:role/fake-role",
        disk_size=100,
        labels={"scylla.scylladb.com/node-type": "scylla"},
        k8s_cluster=k8s_cluster,
    )


# ---------------------------------------------------------------------------
# GKE — deploy_k8s_gke_cluster
# ---------------------------------------------------------------------------


@patch("sdcm.cluster_k8s.gke.GkeNodePool")
def test_deploy_gke_does_not_create_loader_pool_when_n_loaders_zero(mock_gke_pool):
    """n_loaders=[0] must not create a loader node pool.

    Regression: 'if params.get("n_loaders"):' was truthy for [0].
    """
    k8s_cluster = make_k8s_cluster(GKE_PARAMS | {"n_loaders": [0]})

    deploy_k8s_gke_cluster(k8s_cluster)

    instantiated_names = [c.kwargs["name"] for c in mock_gke_pool.call_args_list]
    assert "loader-pool" not in instantiated_names


@patch("sdcm.cluster_k8s.gke.GkeNodePool")
def test_deploy_gke_creates_loader_pool_when_n_loaders_nonzero(mock_gke_pool):
    """n_loaders=[2] must create a loader node pool with num_nodes=2."""
    k8s_cluster = make_k8s_cluster(GKE_PARAMS | {"n_loaders": [2]})

    deploy_k8s_gke_cluster(k8s_cluster)

    mock_gke_pool.assert_any_call(
        name="loader-pool",
        instance_type="e2-standard-2",
        num_nodes=2,
        k8s_cluster=k8s_cluster,
    )
