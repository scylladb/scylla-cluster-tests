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

"""Tests for ClusterTester.init_resources and the node-counting logic
it delegates to in each backend-specific ``get_cluster_*`` method.

These tests specifically verify that the ``IntOrList`` pydantic type
(which converts space-separated strings like ``"3 3 3"`` into
``list[int]``) is correctly handled by every backend helper and by
the ``_get_total_loaders`` / ``_get_total_db_nodes`` helpers in
``sdcm.mgmt.operations``.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.mgmt.operations import SnapshotOperations, StressLoadOperations
from sdcm.tester import ClusterTester


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_info(n_nodes=None, type_=None, disk_size=None, disk_type=None, n_local_ssd=None, device_mappings=None):
    """Return a fresh ``*_info`` dict matching what ``init_resources`` builds."""
    return {
        "n_nodes": n_nodes,
        "type": type_,
        "disk_size": disk_size,
        "disk_type": disk_type,
        "n_local_ssd": n_local_ssd,
        "device_mappings": device_mappings,
    }


def _call_ignoring_infra_errors(func, *args, **kwargs):
    """Call a ``get_cluster_*`` method, suppressing errors after node counting.

    The ``get_cluster_*`` methods perform extensive infrastructure setup
    (provisioner creation, cluster instantiation, etc.) which is intentionally
    not mocked in these tests.  We only care about the node-counting logic
    at the top of each method, so any exception raised *after* that point is
    expected and safely ignored.

    Note: if the node-counting code itself raises, the info dicts remain
    unmodified (n_nodes stays None), so downstream assertions will still fail.
    """
    try:
        func(*args, **kwargs)
    except Exception:  # noqa: BLE001
        pass


def _make_tester_mock(**extra_params_attrs):
    """Create a mock ClusterTester with params, credentials, and fail wired up."""
    tester = MagicMock(spec=ClusterTester)
    tester.params = MagicMock()
    tester.credentials = []
    tester.fail.side_effect = AssertionError
    for attr, value in extra_params_attrs.items():
        setattr(tester.params, attr, value)
    return tester


# ---------------------------------------------------------------------------
# init_resources – backend dispatch
# ---------------------------------------------------------------------------


BACKEND_METHOD_MAP = {
    "aws": "get_cluster_aws",
    "aws-siren": "get_cluster_aws",
    "gce": "get_cluster_gce",
    "gce-siren": "get_cluster_gce",
    "docker": "get_cluster_docker",
    "baremetal": "get_cluster_baremetal",
    "k8s-local-kind": "get_cluster_k8s_local_kind_cluster",
    "k8s-local-kind-aws": "get_cluster_k8s_local_kind_cluster",
    "k8s-local-kind-gce": "get_cluster_k8s_local_kind_cluster",
    "k8s-gke": "get_cluster_k8s_gke",
    "k8s-eks": "get_cluster_k8s_eks",
    "azure": "get_cluster_azure",
    "xcloud": "get_cluster_cloud",
}


@pytest.mark.parametrize("backend,method_name", list(BACKEND_METHOD_MAP.items()))
@patch("sdcm.tester.start_posting_grafana_annotations")
def test_init_resources_dispatches_to_correct_backend(_mock_grafana, backend, method_name):
    tester = _make_tester_mock(gce_datacenters=["us-east1-b"], region_names=["us-east-1"])
    tester.params.get.return_value = backend

    ClusterTester.init_resources(tester)

    getattr(tester, method_name).assert_called_once()


@patch("sdcm.tester.start_posting_grafana_annotations")
def test_init_resources_default_backend_is_aws(_mock_grafana):
    """When ``cluster_backend`` is None the default is ``aws``."""
    tester = _make_tester_mock()
    tester.params.get.return_value = None

    ClusterTester.init_resources(tester)

    tester.get_cluster_aws.assert_called_once()


@patch("sdcm.tester.start_posting_grafana_annotations")
def test_init_resources_always_calls_get_cluster_kafka(_mock_grafana):
    tester = _make_tester_mock()
    tester.params.get.return_value = "docker"

    ClusterTester.init_resources(tester)

    tester.get_cluster_kafka.assert_called_once()


@patch("sdcm.tester.start_posting_grafana_annotations")
def test_init_resources_starts_grafana_annotations(mock_grafana):
    tester = _make_tester_mock()
    tester.params.get.return_value = "docker"

    ClusterTester.init_resources(tester)

    mock_grafana.assert_called_once()


# ---------------------------------------------------------------------------
# Node-counting logic inside get_cluster_* methods
# ---------------------------------------------------------------------------

# Backend-specific configuration for node-counting tests.
# Each entry: (backend_method, params_dict_factory, extra_params_attrs, patches)

_GCE_PARAMS = {
    "availability_zone": "a",
    "gce_network": "default",
    "gce_instance_type_cpu_db": None,
    "gce_instance_type_mem_db": None,
    "gce_instance_type_db": "n1-standard-4",
}

_AZURE_PARAMS = {
    "azure_region_name": ["eastus"],
    "availability_zone": "1",
    "azure_instance_type_db": "Standard_L8s_v3",
}


@pytest.mark.parametrize(
    "n_loaders,expected",
    [
        pytest.param(1, [1], id="int_single"),
        pytest.param([2, 3], [2, 3], id="list_multi_dc"),
        pytest.param([1, 1, 1, 1, 1], [1, 1, 1, 1, 1], id="list_many_dc"),
    ],
)
def test_aws_loader_node_count(n_loaders, expected):
    tester = _make_tester_mock()
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": n_loaders,
        "region_name": "us-east-1",
    }.get(key)

    loader_info = _make_info()
    _call_ignoring_infra_errors(
        ClusterTester.get_cluster_aws, tester,
        loader_info=loader_info, db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
    )

    assert loader_info["n_nodes"] == expected


def test_aws_loader_list_does_not_fail():
    """Regression: ``list[int]`` input must NOT call ``self.fail``."""
    tester = _make_tester_mock()
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": [2, 3],
        "region_name": "us-east-1",
    }.get(key)

    _call_ignoring_infra_errors(
        ClusterTester.get_cluster_aws, tester,
        loader_info=_make_info(), db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
    )

    tester.fail.assert_not_called()


@pytest.mark.parametrize(
    "n_loaders,expected",
    [
        pytest.param(1, [1], id="int_single"),
        pytest.param([2, 3], [2, 3], id="list_multi_dc"),
    ],
)
def test_gce_loader_node_count(n_loaders, expected):
    tester = _make_tester_mock(gce_datacenters=["us-east1-b"])
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": n_loaders, "n_db_nodes": 3, **_GCE_PARAMS,
    }.get(key)

    loader_info = _make_info()
    with patch("sdcm.tester.provisioner_factory"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_gce, tester,
            loader_info=loader_info, db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
        )

    assert loader_info["n_nodes"] == expected


@pytest.mark.parametrize(
    "n_db_nodes,expected",
    [
        pytest.param(3, [3], id="int_single"),
        pytest.param([3, 3, 3], [3, 3, 3], id="list_multi_dc"),
    ],
)
def test_gce_db_node_count(n_db_nodes, expected):
    tester = _make_tester_mock(gce_datacenters=["us-east1-b"])
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_db_nodes": n_db_nodes, "n_loaders": 1, **_GCE_PARAMS,
    }.get(key)

    db_info = _make_info()
    with patch("sdcm.tester.provisioner_factory"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_gce, tester,
            loader_info=_make_info(n_nodes=[1]), db_info=db_info, monitor_info=_make_info(n_nodes=0),
        )

    assert db_info["n_nodes"] == expected


def test_gce_loader_list_does_not_fail():
    """Regression: ``list[int]`` input must NOT call ``self.fail``."""
    tester = _make_tester_mock(gce_datacenters=["us-east1-b"])
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": [2, 3], "n_db_nodes": 3, **_GCE_PARAMS,
    }.get(key)

    with patch("sdcm.tester.provisioner_factory"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_gce, tester,
            loader_info=_make_info(), db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
        )

    tester.fail.assert_not_called()


@pytest.mark.parametrize(
    "n_loaders,expected",
    [
        pytest.param(1, [1], id="int_single"),
        pytest.param([2, 3], [2, 3], id="list_multi_dc"),
    ],
)
def test_azure_loader_node_count(n_loaders, expected):
    tester = _make_tester_mock()
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": n_loaders, "n_db_nodes": 3, **_AZURE_PARAMS,
    }.get(key)

    loader_info = _make_info()
    with patch("sdcm.tester.provisioner_factory"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_azure, tester,
            loader_info=loader_info, db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
        )

    assert loader_info["n_nodes"] == expected


@pytest.mark.parametrize(
    "n_db_nodes,expected",
    [
        pytest.param(3, [3], id="int_single"),
        pytest.param([3, 3], [3, 3], id="list_multi_dc"),
    ],
)
def test_azure_db_node_count(n_db_nodes, expected):
    tester = _make_tester_mock()
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_db_nodes": n_db_nodes, "n_loaders": 1, **_AZURE_PARAMS,
    }.get(key)

    db_info = _make_info()
    with patch("sdcm.tester.provisioner_factory"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_azure, tester,
            loader_info=_make_info(n_nodes=[1]), db_info=db_info, monitor_info=_make_info(n_nodes=0),
        )

    assert db_info["n_nodes"] == expected


@pytest.mark.parametrize(
    "n_loaders,expected",
    [
        pytest.param(1, [1], id="int_single"),
        pytest.param([2, 3], [2, 3], id="list_multi_dc"),
    ],
)
def test_cloud_loader_node_count(n_loaders, expected):
    tester = _make_tester_mock(
        cloud_env_credentials={"base_url": "http://example.com", "api_token": "token"},
    )
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": n_loaders, "n_db_nodes": 3,
    }.get(key)

    loader_info = _make_info()
    with patch("sdcm.tester.ScyllaCloudAPIClient"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_cloud, tester,
            loader_info=loader_info, db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
        )

    assert loader_info["n_nodes"] == expected


@pytest.mark.parametrize(
    "n_db_nodes,expected",
    [
        pytest.param(3, [3], id="int_single"),
        pytest.param([3, 3, 3], [3, 3, 3], id="list_multi_dc"),
    ],
)
def test_cloud_db_node_count(n_db_nodes, expected):
    tester = _make_tester_mock(
        cloud_env_credentials={"base_url": "http://example.com", "api_token": "token"},
    )
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_db_nodes": n_db_nodes, "n_loaders": 1,
    }.get(key)

    db_info = _make_info()
    with patch("sdcm.tester.ScyllaCloudAPIClient"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_cloud, tester,
            loader_info=_make_info(n_nodes=[1]), db_info=db_info, monitor_info=_make_info(n_nodes=0),
        )

    assert db_info["n_nodes"] == expected


def test_cloud_loader_list_does_not_fail():
    """Regression: ``list[int]`` input must NOT call ``self.fail``."""
    tester = _make_tester_mock(
        cloud_env_credentials={"base_url": "http://example.com", "api_token": "token"},
    )
    tester.params.get.side_effect = lambda key, *a, **kw: {
        "n_loaders": [2, 3], "n_db_nodes": 3,
    }.get(key)

    with patch("sdcm.tester.ScyllaCloudAPIClient"):
        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_cloud, tester,
            loader_info=_make_info(), db_info=_make_info(n_nodes=[3]), monitor_info=_make_info(n_nodes=0),
        )

    tester.fail.assert_not_called()


# ---------------------------------------------------------------------------
# _get_total_loaders / _get_total_db_nodes  (sdcm.mgmt.operations)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ops_class,method_name,param_value,expected",
    [
        pytest.param(SnapshotOperations, "_get_total_loaders", 5, 5, id="snapshot_loaders_int"),
        pytest.param(SnapshotOperations, "_get_total_loaders", [3, 3, 3], 9, id="snapshot_loaders_list"),
        pytest.param(SnapshotOperations, "_get_total_loaders", [1, 2, 3, 4, 5], 15, id="snapshot_loaders_list_many"),
        pytest.param(StressLoadOperations, "_get_total_loaders", 5, 5, id="stress_loaders_int"),
        pytest.param(StressLoadOperations, "_get_total_loaders", [3, 3, 3], 9, id="stress_loaders_list"),
        pytest.param(StressLoadOperations, "_get_total_loaders", [1, 2, 3, 4, 5], 15, id="stress_loaders_list_many"),
        pytest.param(StressLoadOperations, "_get_total_db_nodes", 6, 6, id="stress_db_nodes_int"),
        pytest.param(StressLoadOperations, "_get_total_db_nodes", [3, 3, 3], 9, id="stress_db_nodes_list"),
        pytest.param(StressLoadOperations, "_get_total_db_nodes", [1, 2, 3, 4, 5], 15, id="stress_db_nodes_list_many"),
    ],
)
def test_get_total_nodes_handles_int_and_list(ops_class, method_name, param_value, expected):
    obj = MagicMock(spec=ops_class)
    obj.params = MagicMock()
    obj.params.get.return_value = param_value

    result = getattr(ops_class, method_name)(obj)
    assert result == expected
