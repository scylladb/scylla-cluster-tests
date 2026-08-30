"""Tests for dropping a conflicting scylla-node-exporter before the manager backend install.

Monitoring images ship an independently versioned ``scylla-node-exporter`` (epoch based,
e.g. ``1:1.11.1-2``). The manager backend is an older Scylla release whose ``scylla``
package pins it exactly, and the package manager refuses to downgrade across the epoch.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseMonitorSet, BaseNode


@pytest.fixture
def node():
    node = MagicMock()
    node.distro.is_rhel_like = False
    node.distro.is_sles = False
    return node


def test_remove_package_uses_apt_on_debian_like(node):
    BaseNode.remove_package(node, "scylla-node-exporter")

    remove_cmd = node.remoter.sudo.call_args.args[0]
    assert "apt-get" in remove_cmd
    assert remove_cmd.endswith("remove -y scylla-node-exporter")
    assert node.remoter.sudo.call_args.kwargs["ignore_status"] is True


def test_remove_package_uses_the_rpm_manager_on_rhel_like(node):
    node.distro.is_rhel_like = True

    BaseNode.remove_package(node, "scylla-node-exporter")

    remove_cmd = node.remoter.sudo.call_args.args[0]
    assert "remove -y scylla-node-exporter" in remove_cmd
    assert "apt-get" not in remove_cmd


def test_remove_package_uses_zypper_on_sles(node):
    node.distro.is_sles = True

    BaseNode.remove_package(node, "scylla-node-exporter")

    node.remoter.sudo.assert_called_once_with("zypper remove -y scylla-node-exporter", ignore_status=True)


def test_remove_package_tolerates_a_package_that_is_not_installed(node):
    """A missing package must not fail the setup, so the removal ignores the exit code."""
    BaseNode.remove_package(node, "scylla-node-exporter")

    assert node.remoter.sudo.call_args.kwargs["ignore_status"] is True


def test_manager_backend_install_drops_the_conflicting_node_exporter(node):
    monitor_set = MagicMock()
    monitor_set.params.get.side_effect = {
        "scylla_repo_m": "https://downloads.scylladb.com/deb/debian/scylla-2025.4.list",
        "scylla_mgmt_pkg": "",
    }.get

    BaseMonitorSet.install_scylla_manager(monitor_set, node)

    node.remove_package.assert_called_once_with("scylla-node-exporter")


def test_node_exporter_is_dropped_before_scylla_is_installed(node):
    """Removing it afterwards would undo the version the Scylla install just pulled in."""
    monitor_set = MagicMock()
    monitor_set.params.get.side_effect = {
        "scylla_repo_m": "https://downloads.scylladb.com/deb/debian/scylla-2025.4.list",
        "scylla_mgmt_pkg": "",
    }.get

    BaseMonitorSet.install_scylla_manager(monitor_set, node)

    called = [name for name, _, _ in node.mock_calls]
    assert called.index("remove_package") < called.index("install_scylla")
