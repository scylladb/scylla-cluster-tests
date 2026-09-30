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
"""Packages must not be installed from the package lists baked into an image.

On OCI with vector logging, the boot-time scripts marked the node as done without ever refreshing
the apt lists, so the SSH-run configuration script skipped its own refresh too, and a node in
another availability domain failed `apt-get install rsync` with "libpopt0 ... is not installable"
on every one of its retries.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest
from invoke.exceptions import UnexpectedExit

from sdcm.cluster import BaseNode
from sdcm.provision.common.utils import update_repo_cache
from sdcm.sct_provision.user_data_objects.vector_dev import VectorDevUserDataObject


def _fake_node(rhel_like: bool = False, sles: bool = False) -> MagicMock:
    node = MagicMock()
    node.distro.is_rhel_like = rhel_like
    node.distro.is_sles = sles
    node.is_kubernetes.return_value = False
    return node


def _sudo_cmds(node: MagicMock) -> list[str]:
    return [call.args[0] for call in node.remoter.sudo.call_args_list]


def test_boot_time_vector_script_refreshes_the_repo_cache():
    """It is the last boot-time script to touch the package manager before the node is marked done."""
    test_config = Mock()
    test_config.get_logging_service_host_port.return_value = ("10.0.0.1", 32768)
    user_data_object = VectorDevUserDataObject(
        test_config=test_config, params={"logs_transport": "vector"}, instance_name="node-1", node_type="scylla-db"
    )

    assert user_data_object.script_to_run.endswith(update_repo_cache())


@patch("sdcm.utils.decorators.time.sleep")
def test_failed_apt_install_refreshes_the_lists_before_the_retry(_sleep):
    node = _fake_node()
    node.remoter.sudo.side_effect = [UnexpectedExit(MagicMock()), MagicMock(), MagicMock()]

    BaseNode.install_package(node, "rsync")

    install, update, retried_install = _sudo_cmds(node)
    assert "install -y rsync" in install
    assert "apt-get" in update and " update" in update
    assert retried_install == install


@pytest.mark.parametrize(("rhel_like", "sles"), [(True, False), (False, True)])
@patch("sdcm.utils.decorators.time.sleep")
def test_failed_rpm_or_zypper_install_does_not_upgrade_the_system(_sleep, rhel_like, sles):
    """Their `update_cmd` is a system upgrade, not a metadata refresh."""
    node = _fake_node(rhel_like=rhel_like, sles=sles)
    node.remoter.run.return_value = MagicMock(ok=True)
    node.remoter.sudo.side_effect = [UnexpectedExit(MagicMock()), MagicMock()]

    BaseNode.install_package(node, "rsync")

    assert all("install -y rsync" in cmd for cmd in _sudo_cmds(node))
