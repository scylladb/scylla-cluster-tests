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
    """
    try:
        func(*args, **kwargs)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# init_resources – backend dispatch
# ---------------------------------------------------------------------------


@patch("sdcm.tester.start_posting_grafana_annotations")
class TestInitResourcesDispatch:
    """Verify that ``init_resources`` calls the right backend method."""

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
    def test_dispatch(self, _mock_grafana, backend, method_name):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.return_value = backend
        # For k8s-gke / k8s-eks the method receives extra args derived from params
        tester.params.gce_datacenters = ["us-east1-b"]
        tester.params.region_names = ["us-east-1"]

        # Call the real init_resources on the mock
        ClusterTester.init_resources(tester)

        getattr(tester, method_name).assert_called_once()

    def test_default_backend_is_aws(self, _mock_grafana):
        """When ``cluster_backend`` is None the default is ``aws``."""
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.return_value = None

        ClusterTester.init_resources(tester)

        tester.get_cluster_aws.assert_called_once()

    def test_get_cluster_kafka_always_called(self, _mock_grafana):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.return_value = "docker"

        ClusterTester.init_resources(tester)

        tester.get_cluster_kafka.assert_called_once()

    def test_start_posting_grafana_annotations_called(self, mock_grafana):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.return_value = "docker"

        ClusterTester.init_resources(tester)

        mock_grafana.assert_called_once()


# ---------------------------------------------------------------------------
# Node-counting logic inside get_cluster_* methods
# ---------------------------------------------------------------------------


class TestNodeCountingAWS:
    """Node counting in ``get_cluster_aws``."""

    @pytest.mark.parametrize(
        "n_loaders,expected",
        [
            (1, [1]),
            ([2, 3], [2, 3]),
            ([1, 1, 1, 1, 1], [1, 1, 1, 1, 1]),
        ],
    )
    def test_loader_node_count(self, n_loaders, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": n_loaders,
            "region_name": "us-east-1",
        }.get(key)
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_aws, tester, loader_info=loader_info, db_info=db_info, monitor_info=monitor_info
        )

        assert loader_info["n_nodes"] == expected

    def test_loader_list_does_not_fail(self):
        """Regression: ``list[int]`` input must NOT call ``self.fail``."""
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": [2, 3],
            "region_name": "us-east-1",
        }.get(key)
        tester.credentials = []

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        _call_ignoring_infra_errors(
            ClusterTester.get_cluster_aws, tester, loader_info=loader_info, db_info=db_info, monitor_info=monitor_info
        )

        tester.fail.assert_not_called()


class TestNodeCountingGCE:
    """Node counting in ``get_cluster_gce``."""

    @pytest.mark.parametrize(
        "n_loaders,expected",
        [
            (1, [1]),
            ([2, 3], [2, 3]),
        ],
    )
    def test_loader_node_count(self, n_loaders, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.gce_datacenters = ["us-east1-b"]
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": n_loaders,
            "n_db_nodes": 3,
            "availability_zone": "a",
            "gce_network": "default",
            "gce_instance_type_cpu_db": None,
            "gce_instance_type_mem_db": None,
            "gce_instance_type_db": "n1-standard-4",
        }.get(key)
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.provisioner_factory"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_gce,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        assert loader_info["n_nodes"] == expected

    @pytest.mark.parametrize(
        "n_db_nodes,expected",
        [
            (3, [3]),
            ([3, 3, 3], [3, 3, 3]),
        ],
    )
    def test_db_node_count(self, n_db_nodes, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.gce_datacenters = ["us-east1-b"]
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_db_nodes": n_db_nodes,
            "n_loaders": 1,
            "availability_zone": "a",
            "gce_network": "default",
            "gce_instance_type_cpu_db": None,
            "gce_instance_type_mem_db": None,
            "gce_instance_type_db": "n1-standard-4",
        }.get(key)
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info(n_nodes=[1])
        db_info = _make_info()
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.provisioner_factory"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_gce,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        assert db_info["n_nodes"] == expected

    def test_loader_list_does_not_fail(self):
        """Regression: ``list[int]`` input must NOT call ``self.fail``."""
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.gce_datacenters = ["us-east1-b"]
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": [2, 3],
            "n_db_nodes": 3,
            "availability_zone": "a",
            "gce_network": "default",
            "gce_instance_type_cpu_db": None,
            "gce_instance_type_mem_db": None,
            "gce_instance_type_db": "n1-standard-4",
        }.get(key)
        tester.credentials = []

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.provisioner_factory"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_gce,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        tester.fail.assert_not_called()


class TestNodeCountingAzure:
    """Node counting in ``get_cluster_azure`` (already correct, testing as baseline)."""

    @pytest.mark.parametrize(
        "n_loaders,expected",
        [
            (1, [1]),
            ([2, 3], [2, 3]),
        ],
    )
    def test_loader_node_count(self, n_loaders, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": n_loaders,
            "n_db_nodes": 3,
            "azure_region_name": ["eastus"],
            "availability_zone": "1",
            "azure_instance_type_db": "Standard_L8s_v3",
        }.get(key)
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.provisioner_factory"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_azure,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        assert loader_info["n_nodes"] == expected

    @pytest.mark.parametrize(
        "n_db_nodes,expected",
        [
            (3, [3]),
            ([3, 3], [3, 3]),
        ],
    )
    def test_db_node_count(self, n_db_nodes, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_db_nodes": n_db_nodes,
            "n_loaders": 1,
            "azure_region_name": ["eastus"],
            "availability_zone": "1",
            "azure_instance_type_db": "Standard_L8s_v3",
        }.get(key)
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info(n_nodes=[1])
        db_info = _make_info()
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.provisioner_factory"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_azure,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        assert db_info["n_nodes"] == expected


class TestNodeCountingCloud:
    """Node counting in ``get_cluster_cloud`` (xcloud backend)."""

    @pytest.mark.parametrize(
        "n_loaders,expected",
        [
            (1, [1]),
            ([2, 3], [2, 3]),
        ],
    )
    def test_loader_node_count(self, n_loaders, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": n_loaders,
            "n_db_nodes": 3,
        }.get(key)
        tester.params.cloud_env_credentials = {"base_url": "http://example.com", "api_token": "token"}
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.ScyllaCloudAPIClient"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_cloud,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        assert loader_info["n_nodes"] == expected

    @pytest.mark.parametrize(
        "n_db_nodes,expected",
        [
            (3, [3]),
            ([3, 3, 3], [3, 3, 3]),
        ],
    )
    def test_db_node_count(self, n_db_nodes, expected):
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_db_nodes": n_db_nodes,
            "n_loaders": 1,
        }.get(key)
        tester.params.cloud_env_credentials = {"base_url": "http://example.com", "api_token": "token"}
        tester.credentials = []
        tester.fail.side_effect = AssertionError

        loader_info = _make_info(n_nodes=[1])
        db_info = _make_info()
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.ScyllaCloudAPIClient"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_cloud,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        assert db_info["n_nodes"] == expected

    def test_loader_list_does_not_fail(self):
        """Regression: ``list[int]`` input must NOT call ``self.fail``."""
        tester = MagicMock(spec=ClusterTester)
        tester.params = MagicMock()
        tester.params.get.side_effect = lambda key, *a, **kw: {
            "n_loaders": [2, 3],
            "n_db_nodes": 3,
        }.get(key)
        tester.params.cloud_env_credentials = {"base_url": "http://example.com", "api_token": "token"}
        tester.credentials = []

        loader_info = _make_info()
        db_info = _make_info(n_nodes=[3])
        monitor_info = _make_info(n_nodes=0)

        with patch("sdcm.tester.ScyllaCloudAPIClient"):
            _call_ignoring_infra_errors(
                ClusterTester.get_cluster_cloud,
                tester,
                loader_info=loader_info,
                db_info=db_info,
                monitor_info=monitor_info,
            )

        tester.fail.assert_not_called()


# ---------------------------------------------------------------------------
# _get_total_loaders / _get_total_db_nodes  (sdcm.mgmt.operations)
# ---------------------------------------------------------------------------


class TestGetTotalLoadersSnapshotOperations:
    """``SnapshotOperations._get_total_loaders`` handles int, list, and str."""

    @pytest.mark.parametrize(
        "n_loaders,expected",
        [
            (5, 5),
            ([3, 3, 3], 9),
            ([1, 2, 3, 4, 5], 15),
        ],
    )
    def test_returns_correct_total(self, n_loaders, expected):
        obj = MagicMock(spec=SnapshotOperations)
        obj.params = MagicMock()
        obj.params.get.return_value = n_loaders

        result = SnapshotOperations._get_total_loaders(obj)
        assert result == expected


class TestGetTotalLoadersStressLoadOperations:
    """``StressLoadOperations._get_total_loaders`` handles int, list, and str."""

    @pytest.mark.parametrize(
        "n_loaders,expected",
        [
            (5, 5),
            ([3, 3, 3], 9),
            ([1, 2, 3, 4, 5], 15),
        ],
    )
    def test_returns_correct_total(self, n_loaders, expected):
        obj = MagicMock(spec=StressLoadOperations)
        obj.params = MagicMock()
        obj.params.get.return_value = n_loaders

        result = StressLoadOperations._get_total_loaders(obj)
        assert result == expected


class TestGetTotalDbNodesStressLoadOperations:
    """``StressLoadOperations._get_total_db_nodes`` handles int, list, and str."""

    @pytest.mark.parametrize(
        "n_db_nodes,expected",
        [
            (6, 6),
            ([3, 3, 3], 9),
            ([1, 2, 3, 4, 5], 15),
        ],
    )
    def test_returns_correct_total(self, n_db_nodes, expected):
        obj = MagicMock(spec=StressLoadOperations)
        obj.params = MagicMock()
        obj.params.get.return_value = n_db_nodes

        result = StressLoadOperations._get_total_db_nodes(obj)
        assert result == expected
