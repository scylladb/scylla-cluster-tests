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

"""Tests for simulated rack placement on the baremetal backend.

A physical host has no availability zone to derive a rack from, so without
`simulated_racks` every node lands in RACK0.  On ScyllaDB >= 2025.3, where
`rf_rack_valid_keyspaces` is enabled by default, a single rack rejects any
keyspace with RF > 1 as not RF-rack-valid.
"""

import re
from unittest.mock import MagicMock

import pytest

from sdcm import cluster as sdcm_cluster
from sdcm.cluster_baremetal import (
    NodeIpsNotConfiguredError,
    PhysicalMachineCluster,
    PhysicalHost,
    PhysicalMachineNode,
)


def _cluster(node_count: int, racks_count: int) -> MagicMock:
    """A stand-in carrying only what add_nodes() touches."""
    cluster = MagicMock()
    cluster.node_prefix = "db-node"
    cluster.racks_count = racks_count
    cluster.nodes = []
    cluster._node_index = 0  # BaseCluster.__init__ sets this and add_nodes advances it
    cluster._node_public_ips = [f"1.2.3.{index}" for index in range(node_count)]
    cluster._node_private_ips = [f"10.0.0.{index}" for index in range(node_count)]
    return cluster


def _racks_assigned(cluster: MagicMock) -> list[int]:
    return [call.kwargs["rack"] for call in cluster._create_node.call_args_list]


@pytest.mark.parametrize(
    "node_count, racks_count, expected",
    [
        pytest.param(3, 3, [0, 1, 2], id="one-node-per-rack"),
        pytest.param(6, 3, [0, 1, 2, 0, 1, 2], id="round-robin-wraps"),
        pytest.param(4, 3, [0, 1, 2, 0], id="uneven-spread"),
        pytest.param(3, 1, [0, 0, 0], id="single-rack-stays-in-rack0"),
    ],
)
def test_nodes_spread_over_simulated_racks(node_count, racks_count, expected):
    """BaseCluster passes rack=None when simulated_racks is set: spread them ourselves."""
    cluster = _cluster(node_count, racks_count)

    PhysicalMachineCluster.add_nodes(cluster, count=node_count, rack=None)

    assert _racks_assigned(cluster) == expected


def test_explicit_rack_is_honoured():
    """Without simulated_racks the caller pins the rack, and it must not be recomputed."""
    cluster = _cluster(node_count=3, racks_count=3)

    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=2)

    assert _racks_assigned(cluster) == [2, 2, 2]


def test_addresses_still_follow_the_node_index():
    """Rack spreading must not disturb which host each node is bound to."""
    cluster = _cluster(node_count=3, racks_count=3)

    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    positional = [call.args[1:3] for call in cluster._create_node.call_args_list]
    assert positional == [("1.2.3.0", "10.0.0.0"), ("1.2.3.1", "10.0.0.1"), ("1.2.3.2", "10.0.0.2")]


def test_different_instance_types_are_rejected():
    """Pre-provisioned hosts cannot be re-typed; the guard predates rack support."""
    cluster = _cluster(node_count=1, racks_count=1)

    with pytest.raises(AssertionError):
        PhysicalMachineCluster.add_nodes(cluster, count=1, instance_type="i4i.large")


def test_add_nodes_returns_the_nodes_it_created():
    """BaseCluster.add_nodes is documented to return the list of nodes, as AWS and GCE do."""
    cluster = _cluster(node_count=3, racks_count=3)

    added = PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    assert added == cluster.nodes
    assert len(added) == 3


def test_a_second_call_continues_instead_of_restarting():
    """A grow/nemesis call must take the next host, not re-take the first one."""
    cluster = _cluster(node_count=4, racks_count=3)

    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)
    PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None)

    assert _racks_assigned(cluster) == [0, 1, 2, 0]
    names = [call.args[0] for call in cluster._create_node.call_args_list]
    assert names == ["db-node-0", "db-node-1", "db-node-2", "db-node-3"]
    addresses = [call.args[1] for call in cluster._create_node.call_args_list]
    assert addresses == ["1.2.3.0", "1.2.3.1", "1.2.3.2", "1.2.3.3"]


def test_running_out_of_configured_hosts_is_reported_clearly():
    """Physical hosts are a fixed list; asking past the end must say so, not IndexError."""
    cluster = _cluster(node_count=2, racks_count=2)

    with pytest.raises(NodeIpsNotConfiguredError, match="only 2 physical host"):
        PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)


def test_an_oversized_request_creates_nothing():
    """Validation happens up front: a request that cannot be satisfied in full must not leave
    half the nodes on the cluster, with _node_index advanced, while the caller gets an
    exception instead of the list it needs to initialize them with."""
    cluster = _cluster(node_count=2, racks_count=2)

    with pytest.raises(NodeIpsNotConfiguredError):
        PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    assert cluster._create_node.call_count == 0
    assert cluster.nodes == []
    assert cluster._node_index == 0


def test_a_grow_past_the_end_leaves_the_existing_nodes_alone():
    """The same, for a second call: the first three nodes stay, nothing partial is added."""
    cluster = _cluster(node_count=3, racks_count=3)
    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    with pytest.raises(NodeIpsNotConfiguredError, match="1 node\\(s\\) requested from index 3"):
        PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None)

    assert cluster._create_node.call_count == 3
    assert len(cluster.nodes) == 3
    assert cluster._node_index == 3


def test_disruption_name_is_accepted_and_marks_the_new_nodes():
    """Every other backend decorates add_nodes with @mark_new_nodes_as_running_nemesis;
    without it `_add_and_init_new_cluster_nodes()` -- which always passes disruption_name --
    raises TypeError before a node is added."""
    cluster = _cluster(node_count=2, racks_count=2)
    allocator = MagicMock()
    cluster.test_config.tester_obj.return_value.nemesis_allocator = allocator

    added = PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None, disruption_name="GrowShrink")

    assert len(added) == 1
    allocator.set_running_nemesis.assert_called_once_with(added[0], "GrowShrink")


def test_after_config_reaches_the_node():
    """BaseScyllaCluster.node_setup runs BaseNode.after_config; dropping it here silently
    skips the caller's post-install hook."""
    cluster = _cluster(node_count=1, racks_count=1)
    callback = MagicMock()

    PhysicalMachineCluster.add_nodes(cluster, count=1, rack=0, after_config=callback)

    assert cluster._create_node.call_args.kwargs["after_config"] is callback


def test_capacity_counts_address_pairs_not_public_ips():
    """A host needs both addresses; a short private list must not slip past the guard."""
    cluster = _cluster(node_count=3, racks_count=1)
    cluster._node_private_ips = cluster._node_private_ips[:2]  # one host lacks a private address

    with pytest.raises(NodeIpsNotConfiguredError, match="only 2 physical host"):
        PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)


def test_the_rack_survives_the_hops_into_basenode(monkeypatch):
    """The tests above stop at a mocked `_create_node`, so they would still pass if the rack were
    dropped on the way to the node.  Exercise the two real hops --
    `_create_node()` -> `PhysicalMachineNode` -> `BaseNode` -- since that is the handoff the whole
    feature rests on: `SnitchConfig` reads `node.rack` to write cassandra-rackdc.properties.
    """
    recorded = {}

    def record_base_init(self, *args, **kwargs):
        recorded.update(kwargs)

    monkeypatch.setattr(sdcm_cluster.BaseNode, "__init__", record_base_init)
    monkeypatch.setattr(PhysicalMachineNode, "init", lambda self: None)

    parent = MagicMock()
    parent.credentials = [MagicMock(key_file="/dev/null")]
    callback = MagicMock()

    node = PhysicalMachineCluster._create_node(
        parent,
        "db-node-1",
        "1.2.3.1",
        "10.0.0.1",
        dc_idx=0,
        rack=2,
        node_index=1,
        after_config=callback,
    )

    assert recorded["rack"] == 2, "rack dropped between _create_node() and BaseNode"
    assert recorded["after_config"] is callback, "after_config dropped on the way to the node"
    assert node.node_index == 1
    assert node._public_ip == "1.2.3.1"
    assert node._private_ip == "10.0.0.1"


def _node_with_disk_state(
    mount_source: str = "",
    mdstat: str = "",
    other_mounts: list[tuple[str, str]] | None = None,
    mounted_by_unit: bool = True,
    failing_probe: str = "",
    fails_after_stop: bool = False,
    unit_present: bool = True,
    coredump_bind: bool = False,
):
    """A node whose remoter answers the reset's probes from the given host state.

    `other_mounts` are (source, target) mounts besides /var/lib/scylla, and the devices on a disk are the disk and
    the md arrays built from it. As on a real host, stopping var-lib-scylla.mount unmounts /var/lib/scylla, unless it
    was mounted otherwise. The probe starting with `failing_probe` fails, only once the unit is stopped if
    `fails_after_stop`: remoter.run() raises, unless told to ignore the exit status. `unit_present` tells whether
    scylla_setup's var-lib-scylla.mount exists; a device is a partition when its name ends in p<N>. With
    `coredump_bind`, scylla_setup's var-lib-systemd-coredump.mount binds /var/lib/scylla/coredump, on the same disk,
    to /var/lib/systemd/coredump, as on a real host, until that unit is stopped.
    """
    state = {"source": mount_source, "stopped": False, "coredump": coredump_bind and bool(mount_source)}

    def sudo(cmd, **_):
        if cmd == "systemctl stop var-lib-systemd-coredump.mount":
            state["coredump"] = False
        elif cmd == "systemctl stop var-lib-scylla.mount":
            state["stopped"] = True
            if mounted_by_unit:
                state["source"] = ""
        elif cmd == "umount /var/lib/scylla":
            assert state["source"], "umount: /var/lib/scylla: not mounted."
            state["source"] = ""
        return MagicMock(stdout="")

    def run(cmd, ignore_status=False, **_):
        if failing_probe and cmd.startswith(failing_probe) and (state["stopped"] or not fails_after_stop):
            if ignore_status:
                return MagicMock(stdout="", exit_status=1)
            raise RuntimeError(f"{cmd}: exited with 1")
        stdout = ""
        if cmd == "test -e /etc/systemd/system/var-lib-scylla.mount":
            return MagicMock(stdout="", ok=unit_present)
        if cmd == "test -e /etc/systemd/system/var-lib-systemd-coredump.mount":
            return MagicMock(stdout="", ok=coredump_bind)
        if cmd.startswith("lsblk -ndo TYPE "):
            stdout = "part" if re.search(r"p\d+$", cmd.rsplit(" ", 1)[1]) else "disk"
        elif cmd == "findmnt -rn -o SOURCE,TARGET":
            mounts = [(state["source"], "/var/lib/scylla")] if state["source"] else []
            if state["coredump"]:
                mounts.append((f"{mount_source}[/coredump]", "/var/lib/systemd/coredump"))
            stdout = "".join(f"{source} {target}\n" for source, target in mounts + (other_mounts or []))
        elif cmd == "if [ -e /proc/mdstat ]; then cat /proc/mdstat; fi":
            stdout = mdstat
        elif cmd.startswith("lsblk -nrp -o NAME "):
            disk = cmd.rsplit(" ", 1)[1]
            arrays = re.findall(rf"^(md\d+) : .*\b{disk.removeprefix('/dev/')}\[", mdstat, re.MULTILINE)
            stdout = "".join(f"{device}\n" for device in [disk, *(f"/dev/{array}" for array in arrays)])
        return MagicMock(stdout=stdout)

    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node.log = MagicMock()
    node.remoter = MagicMock()
    node.remoter.run.side_effect = run
    node.remoter.sudo.side_effect = sudo
    return node


def _sudo_cmds(node) -> list[str]:
    return [call.args[0] for call in node.remoter.sudo.call_args_list]


def test_reset_on_a_clean_host_only_wipes_the_setup_disks():
    node = _node_with_disk_state()

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    cmds = _sudo_cmds(node)
    assert "umount /var/lib/scylla" not in cmds
    assert not any(cmd.startswith("mdadm --stop") for cmd in cmds)
    assert "wipefs -a /dev/nvme1n1" in cmds
    assert cmds[-1] == "rm -f /etc/scylla.d/io.conf /etc/scylla.d/io_properties.yaml"


def test_reset_undoes_an_earlier_single_disk_setup():
    """i4i.large: scylla_setup put XFS straight on its one NVMe disk, mounted through var-lib-scylla.mount."""
    node = _node_with_disk_state(mount_source="/dev/nvme1n1")

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    cmds = _sudo_cmds(node)
    # stopping the unit already unmounted it: a umount on top fails with "not mounted", exit 32
    assert "umount /var/lib/scylla" not in cmds
    unit_removed = next(i for i, cmd in enumerate(cmds) if "rm -f /etc/systemd/system/var-lib-scylla.mount" in cmd)
    assert cmds.index("systemctl stop var-lib-scylla.mount") < unit_removed < cmds.index("systemctl daemon-reload")
    assert cmds.index("systemctl stop var-lib-scylla.mount") < cmds.index("wipefs -a /dev/nvme1n1")


def test_reset_unmounts_what_the_unit_did_not_mount():
    node = _node_with_disk_state(mount_source="/dev/nvme1n1", mounted_by_unit=False)

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    cmds = _sudo_cmds(node)
    assert cmds.index("systemctl stop var-lib-scylla.mount") < cmds.index("umount /var/lib/scylla")
    assert cmds.index("umount /var/lib/scylla") < cmds.index("wipefs -a /dev/nvme1n1")


def test_reset_accepts_a_bind_mount_of_a_setup_disk():
    node = _node_with_disk_state(mount_source="/dev/nvme1n1[/scylla]", mounted_by_unit=False)

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    cmds = _sudo_cmds(node)
    assert cmds.index("umount /var/lib/scylla") < cmds.index("wipefs -a /dev/nvme1n1")


def test_reset_stops_the_raid_built_from_the_setup_disks():
    node = _node_with_disk_state(
        mount_source="/dev/md0",
        mdstat="Personalities : [raid0]\nmd0 : active raid0 nvme2n1[1] nvme1n1[0]\n      3749396480 blocks\n",
    )

    node._reset_scylla_disk_setup(["/dev/nvme1n1", "/dev/nvme2n1"])

    cmds = _sudo_cmds(node)
    assert (
        cmds.index("systemctl stop var-lib-scylla.mount")
        < cmds.index("mdadm --stop /dev/md0")
        < cmds.index("wipefs -a /dev/nvme1n1")
    )
    assert "wipefs -a /dev/nvme2n1" in cmds


@pytest.mark.parametrize(
    "state, disks, reason",
    [
        pytest.param({"mount_source": "/dev/sdb"}, ["/dev/nvme1n1"], "mounted from /dev/sdb", id="foreign-mount"),
        pytest.param(
            {"mount_source": "/dev/nvme1n1", "other_mounts": [("/dev/sdb", "/var/lib/scylla")]},
            ["/dev/nvme1n1"],
            "mounted from /dev/sdb",
            id="foreign-mount-stacked-on-the-disk",
        ),
        pytest.param(
            {"mdstat": "md5 : active raid1 nvme1n1[0] sda[1]\n"},
            ["/dev/nvme1n1"],
            "/dev/md5 also uses sda",
            id="array-with-other-devices",
        ),
        pytest.param(
            {"other_mounts": [("/dev/nvme1n1", "/data")]},
            ["/dev/nvme1n1"],
            "mounted at /data",
            id="disk-mounted-elsewhere",
        ),
        pytest.param(
            {
                "mount_source": "/dev/nvme1n1",
                "other_mounts": [("/dev/nvme2n1", "/data")],
            },
            ["/dev/nvme1n1", "/dev/nvme2n1"],
            "/dev/nvme2n1 is mounted at /data",
            id="a-later-disk-mounted-elsewhere",
        ),
        pytest.param(
            {
                "mdstat": "md0 : active raid0 nvme2n1[1] nvme1n1[0]\n",
                "other_mounts": [("/dev/md0", "/data")],
            },
            ["/dev/nvme1n1", "/dev/nvme2n1"],
            "/dev/nvme1n1 is mounted at /data",
            id="array-mounted-elsewhere",
        ),
        pytest.param(
            {"mount_source": "/dev/nvme1n1", "other_mounts": [("/dev/nvme1n1[/data]", "/srv/data")]},
            ["/dev/nvme1n1"],
            "mounted at /srv/data",
            id="also-bind-mounted-elsewhere",
        ),
    ],
)
def test_reset_refuses_to_touch_other_devices(state, disks, reason):
    node = _node_with_disk_state(**state)

    with pytest.raises(sdcm_cluster.NodeSetupFailed) as failure:
        node._reset_scylla_disk_setup(disks)

    assert reason in failure.value.error_msg

    # refused before changing anything: no unmount, no array stopped, no disk wiped
    assert _sudo_cmds(node) == []


@pytest.mark.parametrize("probe", ["findmnt -rn", "lsblk", "if [ -e /proc/mdstat ]"])
def test_reset_changes_nothing_when_a_mount_probe_fails(probe):
    node = _node_with_disk_state(mount_source="/dev/nvme1n1", failing_probe=probe)

    with pytest.raises(RuntimeError, match="exited with 1"):
        node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    assert _sudo_cmds(node) == []


def test_reset_wipes_nothing_when_the_mount_probe_fails_after_stopping_the_unit():
    node = _node_with_disk_state(mount_source="/dev/nvme1n1", failing_probe="findmnt -rn", fails_after_stop=True)

    with pytest.raises(RuntimeError, match="exited with 1"):
        node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    assert _sudo_cmds(node) == ["systemctl stop var-lib-scylla.mount"]


def test_scylla_setup_resets_first_and_no_longer_swallows_failures(monkeypatch):
    node = _node_with_disk_state()
    calls = []
    monkeypatch.setattr(node, "_reset_scylla_disk_setup", lambda disks: calls.append(("reset", disks)))

    def failing_setup(self, disks, devname):
        calls.append(("setup", disks))
        raise RuntimeError("scylla_setup: /etc/systemd/system/var-lib-scylla.mount already exists")

    monkeypatch.setattr(sdcm_cluster.BaseNode, "scylla_setup", failing_setup)

    with pytest.raises(RuntimeError, match="already exists"):
        node.scylla_setup(["/dev/nvme1n1"], "eth0")

    assert calls == [("reset", ["/dev/nvme1n1"]), ("setup", ["/dev/nvme1n1"])]


def _host_with_disk_state(**state) -> PhysicalHost:
    """A bare-metal host as the cleanup phase sees it, whose remoter answers from the given host state."""
    host = PhysicalHost(name="scylla-db 1.2.3.4", remoter=_node_with_disk_state(**state).remoter)
    host.log = MagicMock()
    return host


def test_cleanup_resets_a_single_disk_host():
    host = _host_with_disk_state(mount_source="/dev/nvme1n1")

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=True)

    cmds = _sudo_cmds(host)
    assert cmds.index("systemctl stop scylla-server.service") < cmds.index("wipefs -a /dev/nvme1n1")


def test_cleanup_resets_the_raid_scylla_setup_built():
    host = _host_with_disk_state(
        mount_source="/dev/md0",
        mdstat="Personalities : [raid0]\nmd0 : active raid0 nvme2n1[1] nvme1n1[0]\n      3749396480 blocks\n",
    )

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=True)

    cmds = _sudo_cmds(host)
    assert "mdadm --stop /dev/md0" in cmds
    assert {"wipefs -a /dev/nvme1n1", "wipefs -a /dev/nvme2n1"} <= set(cmds)


@pytest.mark.parametrize(
    "state",
    [
        pytest.param({"unit_present": False}, id="host-scylla_setup-never-ran-on"),
        pytest.param({"unit_present": False, "mount_source": "/dev/nvme1n1"}, id="mounted-without-scylla-setup"),
        pytest.param({"mount_source": "/dev/nvme1n1p1"}, id="hand-prepared-partition"),
        pytest.param({}, id="unit-left-but-nothing-mounted"),
    ],
)
def test_cleanup_leaves_disks_scylla_setup_did_not_set_up_alone(state):
    host = _host_with_disk_state(**state)

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=True)

    assert _sudo_cmds(host) == []


def test_cleanup_skips_the_disk_reset_when_not_asked():
    """A loader or monitor host, or a preinstalled Scylla, whose disk setup the next run relies on."""
    host = _host_with_disk_state(mount_source="/dev/nvme1n1")

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=False)

    assert _sudo_cmds(host) == []


def test_a_failed_disk_reset_is_only_logged():
    host = _host_with_disk_state(mount_source="/dev/nvme1n1", failing_probe="lsblk -nrp -o NAME")

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=True)

    assert "wipefs -a /dev/nvme1n1" not in _sudo_cmds(host)
    assert "the next setup will" in host.log.warning.call_args.args[0]


@pytest.mark.parametrize("tunnel_ports", [True, False])
def test_cleanup_frees_tunnel_ports_only_when_asked(tunnel_ports):
    host = _host_with_disk_state()
    host._release_stale_tunnel_ports = MagicMock()

    host.clean_up_host(tunnel_ports=tunnel_ports, scylla_disk_setup=False)

    assert host._release_stale_tunnel_ports.called is tunnel_ports


def test_a_failed_tunnel_cleanup_does_not_skip_the_disk_reset():
    host = _host_with_disk_state(mount_source="/dev/nvme1n1")
    host._release_stale_tunnel_ports = MagicMock(side_effect=RuntimeError("ss: exited with 1"))

    host.clean_up_host(tunnel_ports=True, scylla_disk_setup=True)

    assert "wipefs -a /dev/nvme1n1" in _sudo_cmds(host)
    assert "the next run will" in host.log.warning.call_args_list[0].args[0]


def test_destroy_leaves_the_host_alone(monkeypatch):
    """destroy() also runs mid-test, e.g. after a decommission: nothing may be wiped before the logs are collected."""
    node = _node_with_disk_state(mount_source="/dev/nvme1n1")
    node.stop_task_threads = MagicMock()
    node.wait_till_tasks_threads_are_stopped = MagicMock()
    monkeypatch.setattr(sdcm_cluster.BaseNode, "destroy", lambda self: None)

    node.destroy()

    assert _sudo_cmds(node) == []
    node.remoter.run.assert_not_called()


SS_LISTENING = """\
LISTEN 0      128        127.0.0.1:5001       0.0.0.0:*    users:(("sshd-session",pid=1574,fd=10))
LISTEN 0      128        127.0.0.1:5000       0.0.0.0:*    users:(("sshd-session",pid=1601,fd=11))
LISTEN 0      4096         0.0.0.0:9042       0.0.0.0:*    users:(("scylla",pid=2200,fd=40))
LISTEN 0      128          0.0.0.0:22         0.0.0.0:*    users:(("sshd",pid=900,fd=3))
LISTEN 0      4096       127.0.0.1:5001       0.0.0.0:*    users:(("other-daemon",pid=3000,fd=5))
LISTEN 0      128            [::1]:5001          [::]:*    users:(("sshd-session",pid=1574,fd=12))
"""


def _node_with_listeners(ss_output: str) -> PhysicalMachineNode:
    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node.test_config = MagicMock(SYSLOGNG_SSH_TUNNEL_LOCAL_PORT=5000, VECTOR_SSH_TUNNEL_LOCAL_PORT=5001)
    node.log = MagicMock()
    node.remoter = MagicMock()
    node.remoter.run.return_value.stdout = ss_output
    return node


def test_stale_tunnel_sessions_are_killed():
    """A reused host keeps the reverse tunnels of an earlier run bound: free their ports for ours."""
    node = _node_with_listeners(SS_LISTENING)

    node._release_stale_tunnel_ports()

    node.remoter.sudo.assert_called_once_with("kill 1574 1601", ignore_status=True)


def test_nothing_is_killed_without_stale_tunnels():
    node = _node_with_listeners(
        'LISTEN 0 4096 0.0.0.0:9042 0.0.0.0:* users:(("scylla",pid=2200,fd=40))\n'
        'LISTEN 0 128 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=900,fd=3))\n'
    )

    node._release_stale_tunnel_ports()

    node.remoter.sudo.assert_not_called()


@pytest.mark.parametrize("ip_ssh_connections, released", [("public", True), ("private", False)])
def test_stale_tunnels_released_only_when_tunnels_are_used(ip_ssh_connections, released, monkeypatch):
    node = _node_with_listeners("")
    node.test_config.IP_SSH_CONNECTIONS = ip_ssh_connections
    release = MagicMock()
    monkeypatch.setattr(node, "_release_stale_tunnel_ports", release)
    monkeypatch.setattr(sdcm_cluster.BaseNode, "_init_port_mapping", lambda self: None)

    node._init_port_mapping()

    assert release.called is released


def test_reset_undoes_the_coredump_bind_scylla_setup_made():
    """scylla_setup binds /var/lib/scylla/coredump to /var/lib/systemd/coredump: a second mount of the data disk."""
    node = _node_with_disk_state(mount_source="/dev/nvme1n1", coredump_bind=True)

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    cmds = _sudo_cmds(node)
    assert cmds.index("systemctl stop var-lib-systemd-coredump.mount") < cmds.index(
        "systemctl stop var-lib-scylla.mount"
    )
    assert any("rm -f /etc/systemd/system/var-lib-systemd-coredump.mount" in cmd for cmd in cmds)
    assert cmds.index("systemctl stop var-lib-scylla.mount") < cmds.index("wipefs -a /dev/nvme1n1")


def test_the_coredump_target_is_no_loophole_without_scylla_coredump_unit():
    """Without scylla_setup's unit, a mount of the disk at /var/lib/systemd/coredump is someone else's."""
    node = _node_with_disk_state(
        mount_source="/dev/nvme1n1", other_mounts=[("/dev/nvme1n1[/dumps]", "/var/lib/systemd/coredump")]
    )

    with pytest.raises(sdcm_cluster.NodeSetupFailed) as failure:
        node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    assert "mounted at /var/lib/systemd/coredump" in failure.value.error_msg
    assert not any(cmd.startswith("wipefs") for cmd in _sudo_cmds(node))


FQDN = "ip-10-4-13-234.eu-west-1.compute.internal"


def _node_answering_hostname(tmp_path, vector_address=None, syslogng_address=None) -> PhysicalMachineNode:
    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node._short_hostname = None
    node.logdir = str(tmp_path / "node")
    node.test_config = MagicMock(VECTOR_ADDRESS=vector_address, SYSLOGNG_ADDRESS=syslogng_address)
    node.test_config.logdir.return_value = str(tmp_path)
    node.remoter = MagicMock()
    node.remoter.run.side_effect = lambda cmd, **_: MagicMock(
        stdout=FQDN.split(".", maxsplit=1)[0] if cmd == "hostname -s" else FQDN
    )
    return node


def test_vector_logs_are_looked_up_under_the_full_hostname(tmp_path):
    """vector files a host's logs under the hostname its journal reports, which a physical host keeps whole."""
    node = _node_answering_hostname(tmp_path, vector_address=("10.0.0.1", 49153))

    assert node.short_hostname == FQDN
    assert node.system_log == str(tmp_path / "hosts" / FQDN / "messages.log")
    node.remoter.run.assert_called_once_with("hostname")


def test_other_transports_keep_the_short_hostname(tmp_path):
    node = _node_answering_hostname(tmp_path, syslogng_address=("10.0.0.1", 514))

    assert node.short_hostname == "ip-10-4-13-234"
    node.remoter.run.assert_called_once_with("hostname -s")
