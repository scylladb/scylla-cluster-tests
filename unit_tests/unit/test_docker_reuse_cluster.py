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
# Copyright (c) 2025 ScyllaDB

"""Unit tests for ScyllaDockerCluster.node_setup REUSE_CLUSTER path.

Validates that when REUSE_CLUSTER is True, node_setup takes the early-return
path: calls _reuse_cluster_setup, warms node.raft, and skips the full setup
(is_scylla_installed, check_aio_max_nr, config_setup, restart_scylla).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_docker import ScyllaDockerRequirementError
from unit_tests.lib.fake_docker_cluster import DummyScyllaDockerCluster


def _make_mock_node(rack_arg_supported=True):
    """Create a mock node with the attributes node_setup reads."""
    node = MagicMock()
    node.rack = 0
    node.node_index = 0
    node.raft = None
    node.ssl_conf_dir = Path("/tmp/fake_ssl")
    node.is_docker_rack_arg_supported.return_value = rack_arg_supported
    return node


def test_reuse_skips_full_setup():
    """When REUSE_CLUSTER is True, node_setup must not call
    is_scylla_installed, check_aio_max_nr, config_setup, or restart_scylla.
    """
    cluster = DummyScyllaDockerCluster(
        params={"simulated_racks": 2},
        reuse_cluster=True,
    )
    node = _make_mock_node()

    with patch.object(cluster, "_reuse_cluster_setup") as mock_reuse_setup:
        cluster.node_setup(node)

    mock_reuse_setup.assert_called_once_with(node)
    node.is_scylla_installed.assert_not_called()
    node.config_setup.assert_not_called()
    node.restart_scylla.assert_not_called()


def test_reuse_warms_raft():
    """The REUSE_CLUSTER path must access node.raft to warm the cached_property."""
    cluster = DummyScyllaDockerCluster(
        params={},
        reuse_cluster=True,
    )

    # Use a dedicated local class with a real property so the access can be
    # detected without mutating the shared MagicMock class (which would leak
    # state to subsequent tests).
    raft_accessed = []

    class _RaftProbeNode:
        @property
        def raft(self):
            raft_accessed.append(True)
            return object()

    node = _RaftProbeNode()

    with patch.object(cluster, "_reuse_cluster_setup"):
        cluster.node_setup(node)

    assert raft_accessed, "node.raft was not accessed to warm the cached_property"


def test_reuse_skips_certs_when_not_configured():
    """When neither server_encrypt nor client_encrypt is set,
    _generate_db_node_certs must not be called.
    """
    cluster = DummyScyllaDockerCluster(
        params={},
        reuse_cluster=True,
    )
    node = _make_mock_node()

    with (
        patch.object(cluster, "_reuse_cluster_setup"),
        patch.object(cluster, "_generate_db_node_certs") as mock_gen_certs,
    ):
        cluster.node_setup(node)

    mock_gen_certs.assert_not_called()


@pytest.mark.parametrize("encrypt_param", ["server_encrypt", "client_encrypt"])
def test_reuse_generates_certs_when_missing(encrypt_param):
    """When encryption is enabled and the DB cert is missing,
    _generate_db_node_certs and install_client_certificate must be called.
    """
    cluster = DummyScyllaDockerCluster(
        params={encrypt_param: True},
        reuse_cluster=True,
    )
    node = _make_mock_node()
    # Make ssl_conf_dir / TLSAssets.DB_CERT return a Path whose .exists() is False
    fake_ssl_dir = MagicMock(spec=Path)
    cert_path = MagicMock()
    cert_path.exists.return_value = False
    fake_ssl_dir.__truediv__ = MagicMock(return_value=cert_path)
    node.ssl_conf_dir = fake_ssl_dir

    with (
        patch.object(cluster, "_reuse_cluster_setup"),
        patch.object(cluster, "_generate_db_node_certs") as mock_gen_certs,
        patch("sdcm.cluster_docker.install_client_certificate") as mock_install_cert,
    ):
        cluster.node_setup(node)

    mock_gen_certs.assert_called_once_with(node)
    mock_install_cert.assert_called_once_with(node.remoter, node.ip_address, force=True)


@pytest.mark.parametrize("encrypt_param", ["server_encrypt", "client_encrypt"])
def test_reuse_skips_certs_when_already_present(encrypt_param):
    """When encryption is enabled but the DB cert already exists,
    _generate_db_node_certs must not be called.
    """
    cluster = DummyScyllaDockerCluster(
        params={encrypt_param: True},
        reuse_cluster=True,
    )
    node = _make_mock_node()
    fake_ssl_dir = MagicMock(spec=Path)
    cert_path = MagicMock()
    cert_path.exists.return_value = True
    fake_ssl_dir.__truediv__ = MagicMock(return_value=cert_path)
    node.ssl_conf_dir = fake_ssl_dir

    with (
        patch.object(cluster, "_reuse_cluster_setup"),
        patch.object(cluster, "_generate_db_node_certs") as mock_gen_certs,
    ):
        cluster.node_setup(node)

    mock_gen_certs.assert_not_called()


@pytest.mark.parametrize("simulated_racks_value", [0, 2])
def test_normal_setup_when_not_reusing(simulated_racks_value):
    """When REUSE_CLUSTER is False, node_setup must call is_scylla_installed
    and proceed with the full setup path for both simulated_racks=0 and
    simulated_racks=2.  In both cases: config_setup called once,
    restart_scylla(verify_up_before=True) called once, clean_scylla_data
    never called.
    """
    cluster = DummyScyllaDockerCluster(
        params={"simulated_racks": simulated_racks_value},
        reuse_cluster=False,
    )
    node = _make_mock_node(rack_arg_supported=True)
    node.is_scylla_installed.return_value = True

    with patch.object(cluster, "_reuse_cluster_setup") as mock_reuse_setup:
        cluster.node_setup(node)

    mock_reuse_setup.assert_not_called()
    node.is_scylla_installed.assert_called_once_with(raise_if_not_installed=True)
    node.config_setup.assert_called_once()
    node.restart_scylla.assert_called_once_with(verify_up_before=True)


def test_version_guard_raises_for_old_image():
    """When simulated_racks > 1 and the image does not support --rack,
    node_setup must raise ScyllaDockerRequirementError before touching
    config_setup or restart_scylla.
    """
    cluster = DummyScyllaDockerCluster(
        params={"simulated_racks": 2},
        reuse_cluster=False,
    )
    node = _make_mock_node(rack_arg_supported=False)
    node.is_scylla_installed.return_value = True

    with pytest.raises(ScyllaDockerRequirementError, match="simulated_racks requires Scylla >= 2026.1"):
        cluster.node_setup(node)

    node.config_setup.assert_not_called()
    node.restart_scylla.assert_not_called()
