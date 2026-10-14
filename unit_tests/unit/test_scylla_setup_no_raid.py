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

"""Tests for scylla_setup without a data disk (scylla_setup_no_raid)."""

import logging
from types import MethodType
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseNode, BaseScyllaCluster


def _node(mounts: str = "") -> BaseNode:
    node = MagicMock(spec=BaseNode)
    node.log = logging.getLogger("test_node")
    node.parent_cluster = MagicMock()
    node.parent_cluster.params.get.side_effect = {"append_scylla_setup_args": "", "unified_package": None}.get
    node.is_service_exists.return_value = False
    node._run_scylla_setup_with_dns_retry = MagicMock()
    node.scylla_setup = MethodType(BaseNode.scylla_setup, node)

    def run(cmd, **_):
        if cmd == "cat /proc/mounts":
            return MagicMock(stdout=mounts)
        return MagicMock(stdout="--swap-directory" if "--help" in cmd else "")

    node.remoter = MagicMock()
    node.remoter.run.side_effect = run
    return node


def test_scylla_setup_without_disks_skips_the_raid_setup():
    """The root filesystem holds /var/lib/scylla, so it is no mount point to check."""
    node = _node(mounts="/dev/nvme0n1p4 / xfs rw 0 0\n")

    node.scylla_setup([], "eth0")

    node._run_scylla_setup_with_dns_retry.assert_called_once_with(
        "sudo /usr/lib/scylla/scylla_setup --nic eth0 --no-raid-setup  --swap-directory / "
    )


def test_scylla_setup_with_disks_sets_them_up_and_checks_the_mount():
    node = _node(mounts="/dev/md0 /var/lib/scylla xfs rw 0 0\n")

    node.scylla_setup(["/dev/nvme1n1", "/dev/nvme2n1"], "eth0")

    node._run_scylla_setup_with_dns_retry.assert_called_once_with(
        "sudo /usr/lib/scylla/scylla_setup --nic eth0 --disks /dev/nvme1n1,/dev/nvme2n1 --setup-nic-and-disks  "
        "--swap-directory / "
    )


def test_scylla_setup_with_disks_still_fails_when_scylla_dir_is_not_mounted():
    node = _node(mounts="/dev/nvme0n1p4 / xfs rw 0 0\n")

    with pytest.raises(AssertionError, match="RAID setup failed"):
        node.scylla_setup(["/dev/nvme1n1"], "eth0")


@pytest.mark.parametrize(
    ("no_raid", "expected_disks"),
    [
        pytest.param(True, [], id="no-raid-detects-no-disks"),
        pytest.param(False, ["/dev/nvme1n1"], id="default-detects-the-disks"),
    ],
)
def test_node_setup_passes_the_disks_to_scylla_setup(no_raid, expected_disks):
    node = MagicMock()
    node.parent_cluster.params.get.side_effect = {"scylla_setup_no_raid": no_raid}.get
    node.detect_disks.return_value = ["/dev/nvme1n1"]
    node.check_dns_ready.return_value = True

    BaseScyllaCluster._scylla_post_install(node, new_scylla_installed=True, devname="eth0")

    node.scylla_setup.assert_called_once_with(expected_disks, "eth0")
    assert node.detect_disks.called is not no_raid
