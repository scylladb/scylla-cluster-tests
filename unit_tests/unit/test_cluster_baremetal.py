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
import shlex
from fnmatch import fnmatch
from unittest.mock import MagicMock

import pytest

from sdcm import cluster as sdcm_cluster
from sdcm.cluster_baremetal import (
    BASELINE_MAX_AGE_DAYS,
    LIST_PACKAGES,
    LIST_RUNNING_SERVICES,
    PACKAGE_BASELINE,
    SCYLLA_COREDUMP_MOUNT_UNIT,
    SERVICE_BASELINE,
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
    exception instead of the list it needs to initialize them with.
    """
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
    raises TypeError before a node is added.
    """
    cluster = _cluster(node_count=2, racks_count=2)
    allocator = MagicMock()
    cluster.test_config.tester_obj.return_value.nemesis_allocator = allocator

    added = PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None, disruption_name="GrowShrink")

    assert len(added) == 1
    allocator.set_running_nemesis.assert_called_once_with(added[0], "GrowShrink")


def test_after_config_reaches_the_node():
    """BaseScyllaCluster.node_setup runs BaseNode.after_config; dropping it here silently
    skips the caller's post-install hook.
    """
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
    coredump_bound_by_unit: bool = True,
):
    """A node whose remoter answers the reset's probes from the given host state.

    `other_mounts` are (source, target) mounts besides /var/lib/scylla, and the devices on a disk are the disk and
    the md arrays built from it. As on a real host, stopping var-lib-scylla.mount unmounts /var/lib/scylla, unless it
    was mounted otherwise. The probe starting with `failing_probe` fails, only once the unit is stopped if
    `fails_after_stop`: remoter.run() raises, unless told to ignore the exit status. `unit_present` tells whether
    scylla_setup's var-lib-scylla.mount exists; a device is a partition when its name ends in p<N>. With
    `coredump_bind`, scylla_setup's var-lib-systemd-coredump.mount binds /var/lib/scylla/coredump, on the same disk,
    to /var/lib/systemd/coredump, as on a real host, until that unit is stopped, or, unless
    `coredump_bound_by_unit`, until it is unmounted.
    """
    state = {"source": mount_source, "stopped": False, "coredump": coredump_bind and bool(mount_source)}

    def sudo(cmd, **_):
        if cmd in (f"test -s {PACKAGE_BASELINE}", f"test -s {SERVICE_BASELINE}"):
            return MagicMock(stdout="", ok=False)
        if cmd == "systemctl stop var-lib-systemd-coredump.mount":
            if coredump_bound_by_unit:
                state["coredump"] = False
        elif cmd == "umount /var/lib/systemd/coredump":
            assert state["coredump"], "umount: /var/lib/systemd/coredump: not mounted."
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
        elif cmd == "findmnt -rn -o SOURCE --mountpoint /var/lib/systemd/coredump":
            binds = [source for source, target in other_mounts or [] if target == "/var/lib/systemd/coredump"]
            stdout = "".join(f"{source}\n" for source in [f"{mount_source}[/coredump]"] * state["coredump"] + binds)
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


# Every host cleanup removes the scripts of the log followers a run left, see test_*log_followers*
REMOVE_LOG_FOLLOWER_SCRIPTS = "rm -f /tmp/logger_*.pid /tmp/logger_cmd_*.sh"


# The cleanup steps read their baselines, and dry-run the removal of packages, with sudo, which changes nothing on the
# host
BASELINE_READS = tuple(
    f"{cmd} {baseline}" for cmd in ("test -s", "cat") for baseline in (PACKAGE_BASELINE, SERVICE_BASELINE)
) + tuple(
    f"find {baseline} -mmin +{BASELINE_MAX_AGE_DAYS * 24 * 60}" for baseline in (PACKAGE_BASELINE, SERVICE_BASELINE)
)
DRY_RUNS = ("rpm -e --test ", "dpkg --dry-run --purge ")


def _sudo_cmds(node) -> list[str]:
    return [
        call.args[0]
        for call in node.remoter.sudo.call_args_list
        if call.args[0] != REMOVE_LOG_FOLLOWER_SCRIPTS
        and call.args[0] not in BASELINE_READS
        and not call.args[0].startswith(DRY_RUNS)
    ]


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


def test_scylla_setup_without_disks_resets_nothing(monkeypatch):
    """scylla_setup_no_raid: /var/lib/scylla is a directory on the root filesystem, there is no disk to reset."""
    node = _node_with_disk_state()
    calls = []
    monkeypatch.setattr(node, "_reset_scylla_disk_setup", lambda disks: calls.append(("reset", disks)))
    monkeypatch.setattr(
        sdcm_cluster.BaseNode, "scylla_setup", lambda self, disks, devname: calls.append(("setup", disks))
    )

    node.scylla_setup([], "eth0")

    assert calls == [("setup", [])]


def test_scylla_setup_without_disks_refuses_a_scylla_dir_left_on_a_disk(monkeypatch):
    node = _node_with_disk_state(mount_source="/dev/nvme1n1")
    setup = MagicMock()
    monkeypatch.setattr(sdcm_cluster.BaseNode, "scylla_setup", setup)

    with pytest.raises(sdcm_cluster.NodeSetupFailed) as failure:
        node.scylla_setup([], "eth0")

    assert "hydra clean-resources" in failure.value.error_msg
    setup.assert_not_called()


def _node_with_leftovers(existing: list[str], mounts: str = ""):
    """A node whose host holds `existing` paths, and the given `findmnt -rn -o SOURCE,TARGET` mounts."""

    def run(cmd, **_):
        if cmd.startswith("ls -d "):
            patterns = cmd.removeprefix("ls -d ").removesuffix(" 2>/dev/null").split()
            found = [path for path in existing if any(fnmatch(path, pattern) for pattern in patterns)]
            return MagicMock(stdout="".join(f"{path}\n" for path in found))
        if cmd == "findmnt -rn -o SOURCE,TARGET":
            return MagicMock(stdout=mounts)
        if cmd == "findmnt -rn -o SOURCE --mountpoint /var/lib/systemd/coredump":
            binds = [line.split()[0] for line in mounts.splitlines() if line.endswith(" /var/lib/systemd/coredump")]
            return MagicMock(stdout="".join(f"{source}\n" for source in binds))
        return MagicMock(stdout="")

    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node.log = MagicMock()
    node.remoter = MagicMock()
    node.remoter.run.side_effect = run
    return node


def test_a_clean_host_has_no_leftovers_to_remove():
    node = _node_with_leftovers(existing=[])

    node.remove_scylla_leftovers()

    node.remoter.sudo.assert_not_called()


def test_leftovers_of_an_earlier_install_are_removed():
    """A shared host, after removing the Scylla 2025.3 packages of the artifact test."""
    node = _node_with_leftovers(
        existing=[
            "/etc/yum.repos.d/scylla.repo",
            "/etc/scylla",
            "/etc/scylla.d",
            "/etc/systemd/system/scylla-server.service.d",
            "/etc/systemd/system/scylla-node-exporter.service.d",
            "/opt/scylladb",
            "/var/lib/scylla-housekeeping",
            "/var/lib/scylla",
        ]
    )

    node.remove_scylla_leftovers()

    assert _sudo_cmds(node) == [
        "systemctl stop var-lib-systemd-coredump.mount",
        f"rm -f {SCYLLA_COREDUMP_MOUNT_UNIT} /etc/systemd/system/*.wants/var-lib-systemd-coredump.mount",
        "rm -rf /etc/yum.repos.d/scylla.repo /etc/scylla /etc/scylla.d /etc/systemd/system/scylla-server.service.d "
        "/etc/systemd/system/scylla-node-exporter.service.d /opt/scylladb /var/lib/scylla-housekeeping "
        "/var/lib/scylla",
        "systemctl daemon-reload",
        "systemctl reset-failed 'scylla-*'",
    ]


def test_the_coredump_bind_is_unmounted_before_the_scylla_dir_is_removed():
    """scylla_setup binds /var/lib/scylla/coredump on the root filesystem too, which keeps the directory busy."""
    node = _node_with_leftovers(
        existing=["/var/lib/scylla", "/etc/systemd/system/var-lib-systemd-coredump.mount"],
        mounts="/dev/nvme0n1p4 /\n/dev/nvme0n1p4[/var/lib/scylla/coredump] /var/lib/systemd/coredump\n",
    )

    node.remove_scylla_leftovers()

    cmds = _sudo_cmds(node)
    assert cmds.index("umount /var/lib/systemd/coredump") < cmds.index(
        "rm -rf /var/lib/scylla /etc/systemd/system/var-lib-systemd-coredump.mount"
    )


def test_a_scylla_dir_on_a_setup_disk_is_left_to_the_disk_reset():
    node = _node_with_leftovers(existing=["/etc/scylla", "/var/lib/scylla"], mounts="/dev/nvme1n1 /var/lib/scylla\n")

    node.remove_scylla_leftovers()

    assert _sudo_cmds(node) == ["rm -rf /etc/scylla", "systemctl daemon-reload", "systemctl reset-failed 'scylla-*'"]


def test_clean_scylla_removes_the_leftovers_after_the_packages(monkeypatch):
    node = _node_with_leftovers(existing=[])
    calls = []
    monkeypatch.setattr(sdcm_cluster.BaseNode, "clean_scylla", lambda self: calls.append("uninstall"))
    monkeypatch.setattr(node, "remove_scylla_leftovers", lambda: calls.append("leftovers"))

    node.clean_scylla()

    assert calls == ["uninstall", "leftovers"]


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

    assert _sudo_cmds(host) == ["systemctl stop scylla-server.service"]


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


def test_reset_unmounts_a_coredump_bind_its_unit_did_not():
    """A bind made some other way (fstab, by hand) is still there once scylla_setup's unit is stopped."""
    node = _node_with_disk_state(mount_source="/dev/nvme1n1", coredump_bind=True, coredump_bound_by_unit=False)

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    cmds = _sudo_cmds(node)
    assert cmds.index("systemctl stop var-lib-systemd-coredump.mount") < cmds.index("umount /var/lib/systemd/coredump")
    assert cmds.index("umount /var/lib/systemd/coredump") < cmds.index("wipefs -a /dev/nvme1n1")


def test_reset_leaves_the_coredump_dir_alone_once_its_unit_unmounted_it():
    node = _node_with_disk_state(mount_source="/dev/nvme1n1", coredump_bind=True)

    node._reset_scylla_disk_setup(["/dev/nvme1n1"])

    assert "umount /var/lib/systemd/coredump" not in _sudo_cmds(node)


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
    """Vector files a host's logs under the hostname its journal reports, which a physical host keeps whole."""
    node = _node_answering_hostname(tmp_path, vector_address=("10.0.0.1", 49153))

    assert node.short_hostname == FQDN
    assert node.system_log == str(tmp_path / "hosts" / FQDN / "messages.log")
    node.remoter.run.assert_called_once_with("hostname")


def test_other_transports_keep_the_short_hostname(tmp_path):
    node = _node_answering_hostname(tmp_path, syslogng_address=("10.0.0.1", 514))

    assert node.short_hostname == "ip-10-4-13-234"
    node.remoter.run.assert_called_once_with("hostname -s")


TEST_ID = "7fba7abb-891c-4199-b61d-bf9ca12d671d"


def _node_answering_date(date_output: str) -> PhysicalMachineNode:
    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node.test_config = MagicMock()
    node.test_config.test_id.return_value = TEST_ID
    node.log = MagicMock()
    node.remoter = MagicMock()
    node.remoter.run.return_value = MagicMock(stdout=date_output)
    return node


def test_the_journal_is_read_from_the_start_of_the_run():
    """A reused host's journal holds every earlier run on it: read it from the mark this run writes."""
    node = _node_answering_date("1791382530\n")

    node.mark_run_start_in_journal()

    assert node.journal_since == "@1791382530"
    cmd = node.remoter.run.call_args.args[0]
    assert cmd.index("date +%s") < cmd.index("logger"), "the mark must come after the time the journal is read from"
    assert f"SCT test {TEST_ID} starts on this host" in cmd


@pytest.mark.parametrize("date_output", ["", "date: command not found\n", "2026-10-07 14:05:30\n"])
def test_the_whole_journal_is_read_when_the_host_time_is_unknown(date_output):
    node = _node_answering_date(date_output)

    node.mark_run_start_in_journal()

    assert node.journal_since is None
    node.log.warning.assert_called_once()


def test_the_start_of_the_run_is_marked_once():
    node = _node_answering_date("1791382530\n")
    node.mark_run_start_in_journal()
    node.remoter.run.return_value = MagicMock(stdout="1791386000\n")

    node.mark_run_start_in_journal()

    assert node.journal_since == "@1791382530"
    node.remoter.run.assert_called_once()


def test_the_run_is_marked_before_the_log_followers_start(monkeypatch):
    node = _node_answering_date("1791382530\n")
    seen_by_task_threads = []
    monkeypatch.setattr(
        sdcm_cluster.BaseNode, "start_task_threads", lambda self: seen_by_task_threads.append(self.journal_since)
    )

    node.start_task_threads()

    assert seen_by_task_threads == ["@1791382530"]


def _host_with_packages(
    installed: set[str],
    baseline: set[str] | None,
    debian: bool = False,
    stuck: set[str] = (),
    pulled_in: set[str] = (),
    service_baseline: bool | None = None,
    needs: dict[str, set[str]] | None = None,
    dry_run_error: str = "",
    stale: set[str] = (),
):
    """A host whose package manager installs and removes for real, except the `stuck` packages it fails on.

    Reinstalling adds the `pulled_in` packages too, as a newer version of a baseline package may need. The host has a
    service baseline when it has a package one, unless `service_baseline` says otherwise. `needs` maps a package to
    the packages it needs: a dry run of removing them fails, as rpm and dpkg do, while one installed outside the removal
    needs one in it, or with `dry_run_error`. The `stale` baselines are older than BASELINE_MAX_AGE_DAYS.
    """
    state = {"installed": set(installed)}
    has_service_baseline = baseline is not None if service_baseline is None else service_baseline

    def run(cmd, ignore_status=False, **_):
        if cmd == LIST_PACKAGES:
            return MagicMock(stdout="".join(f"{package}\n" for package in sorted(state["installed"])))
        if cmd == "test -e /etc/debian_version":
            return MagicMock(stdout="", ok=debian)
        if cmd.startswith("rpm -q --whatprovides "):
            # The capabilities are named like the providing package: openssl-libs(x86-64)
            capabilities = shlex.split(cmd)[5:]
            return MagicMock(stdout="".join(f"{capability.split('(')[0]}.x86_64\n" for capability in capabilities))
        return MagicMock(stdout="", ok=True)

    def dry_run(cmd):
        if dry_run_error:
            return MagicMock(stdout="", stderr=dry_run_error, ok=False)
        removing = set(cmd.split()[3:])
        blocked = [
            (package, needed)
            for package, needed_ones in (needs or {}).items()
            if package in state["installed"] - removing
            for needed in sorted(needed_ones & removing)
        ]
        if debian:
            output = "".join(
                f"dpkg: dependency problems prevent removal of {needed}:\n"
                f" {package.split(':')[0]} depends on {needed.split(':')[0]} (>= 3.0.0).\n"
                for package, needed in blocked
            )
        else:
            output = "error: Failed dependencies:\n" + "".join(
                f"\t{needed.rsplit('.', 1)[0]}(x86-64) >= 1:3.0.7 is needed by (installed) "
                f"{package.rsplit('.', 1)[0]}-9.9p1-1.el9.x86_64\n"
                for package, needed in blocked
            )
        return MagicMock(stdout="", stderr=output if blocked else "", ok=not blocked)

    def sudo(cmd, **_):
        if cmd.startswith(DRY_RUNS):
            return dry_run(cmd)
        if cmd.startswith("find "):
            return MagicMock(stdout=f"{cmd.split()[1]}\n" if cmd.split()[1] in stale else "")
        if cmd == f"test -s {PACKAGE_BASELINE}":
            return MagicMock(stdout="", ok=baseline is not None)
        if cmd == f"test -s {SERVICE_BASELINE}":
            return MagicMock(stdout="", ok=has_service_baseline)
        if cmd == f"cat {PACKAGE_BASELINE}":
            return MagicMock(stdout="".join(f"{package}\n" for package in sorted(baseline)))
        packages = set(cmd.removesuffix("'").split()) - set(stuck)
        if " remove -y " in cmd or " purge -y " in cmd:
            state["installed"] -= packages
        elif " install -y " in cmd:
            state["installed"] |= (packages & (baseline or set())) | set(pulled_in)
        return MagicMock(stdout="", ok=True)

    host = PhysicalHost(name="scylla-db 1.2.3.4", remoter=MagicMock())
    host.remoter.run.side_effect = run
    host.remoter.sudo.side_effect = sudo
    host.log = MagicMock()
    return host, state


def test_the_baseline_is_recorded_aside_then_moved_in_place():
    host, _ = _host_with_packages(installed={"bash.x86_64"}, baseline=None)

    host.record_host_baseline()

    cmds = _sudo_cmds(host)
    assert cmds[0] == "mkdir -p /var/lib/scylla-cluster-tests"
    assert cmds[1].startswith("sh -c ") and cmds[1].endswith(f" > {SERVICE_BASELINE}.new'")
    assert cmds[2] == f"mv {SERVICE_BASELINE}.new {SERVICE_BASELINE}"
    assert cmds[3].startswith("sh -c ") and cmds[3].endswith(f" > {PACKAGE_BASELINE}.new'")
    assert cmds[4] == f"mv {PACKAGE_BASELINE}.new {PACKAGE_BASELINE}"


def test_a_baseline_left_by_a_run_without_cleanup_is_kept():
    """It holds the packages from before that run, which this run's cleanup restores."""
    host, _ = _host_with_packages(installed={"bash.x86_64", "scylla.x86_64"}, baseline={"bash.x86_64"})

    host.record_host_baseline()

    assert _sudo_cmds(host) == []


@pytest.mark.parametrize(
    "package_baseline, service_baseline, recorded",
    [
        pytest.param(None, True, PACKAGE_BASELINE, id="services-not-restored"),
        pytest.param({"bash.x86_64"}, False, SERVICE_BASELINE, id="packages-not-restored"),
    ],
)
def test_a_baseline_left_alone_is_kept_and_the_other_recorded(package_baseline, service_baseline, recorded):
    """The cleanup of an earlier run restored one baseline and not the other: the one it kept holds the host from
    before that run, and this run still needs the other, for its own cleanup to restore."""
    host, _ = _host_with_packages(
        installed={"bash.x86_64"}, baseline=package_baseline, service_baseline=service_baseline
    )

    host.record_host_baseline()

    cmds = _sudo_cmds(host)
    assert len(cmds) == 3
    assert cmds[1].endswith(f" > {recorded}.new'")
    assert cmds[2] == f"mv {recorded}.new {recorded}"


def test_no_baseline_leaves_the_packages_alone():
    host, state = _host_with_packages(installed={"bash.x86_64", "scylla.x86_64"}, baseline=None)

    host.restore_packages()

    assert _sudo_cmds(host) == []
    assert state["installed"] == {"bash.x86_64", "scylla.x86_64"}


def test_the_packages_of_the_baseline_are_restored():
    """A shared host: the run installed Scylla and hwloc, and removed abrt; openssh upgrades keep their name."""
    host, state = _host_with_packages(
        installed={"bash.x86_64", "openssh.x86_64", "scylla.x86_64", "hwloc.x86_64"},
        baseline={"bash.x86_64", "openssh.x86_64", "abrt.x86_64"},
    )

    host.restore_packages()

    assert state["installed"] == {"bash.x86_64", "openssh.x86_64", "abrt.x86_64"}
    cmds = _sudo_cmds(host)
    assert "yum remove -y --setopt=clean_requirements_on_remove=False hwloc.x86_64 scylla.x86_64'" in cmds[0]
    assert "yum install -y --setopt=install_weak_deps=False abrt.x86_64'" in cmds[1]
    assert cmds[2] == f"rm -f {PACKAGE_BASELINE}"


def test_the_baseline_is_kept_while_the_packages_differ():
    host, state = _host_with_packages(
        installed={"bash.x86_64"}, baseline={"bash.x86_64", "abrt.x86_64"}, stuck={"abrt.x86_64"}
    )

    host.restore_packages()

    assert f"rm -f {PACKAGE_BASELINE}" not in _sudo_cmds(host)
    assert "abrt.x86_64" in host.log.warning.call_args.args[1]


def test_debian_packages_are_purged_non_interactively():
    host, state = _host_with_packages(
        installed={"bash:amd64", "scylla-server:amd64"}, baseline={"bash:amd64"}, debian=True
    )

    host.restore_packages()

    assert state["installed"] == {"bash:amd64"}
    cmd = _sudo_cmds(host)[0]
    assert cmd.startswith("DEBIAN_FRONTEND=noninteractive bash -c ")
    assert "-o Acquire::http::Timeout=60" in cmd and "purge -y scylla-server:amd64" in cmd


def test_debian_packages_are_reinstalled_without_their_recommends():
    host, state = _host_with_packages(installed={"bash:amd64"}, baseline={"bash:amd64", "apport:amd64"}, debian=True)

    host.restore_packages()

    assert state["installed"] == {"bash:amd64", "apport:amd64"}
    assert "install -y --no-install-recommends apport:amd64" in _sudo_cmds(host)[0]


def test_what_a_reinstalled_package_needs_is_kept():
    """A shared host: the abrt and kexec-tools reinstalled are newer, and need kdump-utils, which the host did not have."""
    host, state = _host_with_packages(
        installed={"bash.x86_64", "scylla.x86_64"},
        baseline={"bash.x86_64", "kexec-tools.x86_64"},
        pulled_in={"kdump-utils.x86_64"},
    )

    host.restore_packages()

    assert state["installed"] == {"bash.x86_64", "kexec-tools.x86_64", "kdump-utils.x86_64"}
    assert _sudo_cmds(host)[-1] == f"rm -f {PACKAGE_BASELINE}"
    assert "kdump-utils.x86_64" in host.log.info.call_args_list[-2].args[1]


@pytest.mark.parametrize(
    "debian, openssh, openssl_libs, scylla",
    [
        pytest.param(False, "openssh-server.x86_64", "openssl-libs.x86_64", "scylla.x86_64", id="rpm"),
        pytest.param(True, "openssh-server:amd64", "libssl3:amd64", "scylla-server:amd64", id="debian"),
    ],
)
def test_a_package_an_upgraded_one_needs_is_kept(debian, openssh, openssl_libs, scylla):
    """SCT upgraded openssh-server, whose new version needs an openssl library the baseline did not have: removing the
    library would remove openssh-server with it, and stop sshd."""
    host, state = _host_with_packages(
        installed={"bash", openssh, openssl_libs, scylla},
        baseline={"bash", openssh},
        debian=debian,
        needs={openssh: {openssl_libs}},
    )

    host.restore_packages()

    assert state["installed"] == {"bash", openssh, openssl_libs}
    cmds = _sudo_cmds(host)
    assert f" {scylla}'" in cmds[0] and openssl_libs not in cmds[0]
    assert cmds[-1] == f"rm -f {PACKAGE_BASELINE}"
    assert openssl_libs in host.log.info.call_args_list[0].args[1]


def test_a_package_a_kept_one_needs_is_kept_too():
    host, state = _host_with_packages(
        installed={"openssh-server.x86_64", "openssl-libs.x86_64", "crypto-policies.x86_64", "scylla.x86_64"},
        baseline={"openssh-server.x86_64"},
        needs={"openssh-server.x86_64": {"openssl-libs.x86_64"}, "openssl-libs.x86_64": {"crypto-policies.x86_64"}},
    )

    host.restore_packages()

    assert state["installed"] == {"openssh-server.x86_64", "openssl-libs.x86_64", "crypto-policies.x86_64"}
    assert _sudo_cmds(host)[-1] == f"rm -f {PACKAGE_BASELINE}"


def test_a_dry_run_that_fails_otherwise_removes_nothing_and_keeps_the_baseline():
    host, state = _host_with_packages(
        installed={"bash.x86_64", "scylla.x86_64"},
        baseline={"bash.x86_64"},
        dry_run_error="error: can't create transaction lock on /var/lib/rpm/.rpm.lock (Resource temporarily unavailable)",
    )

    with pytest.raises(RuntimeError, match="transaction lock"):
        host.restore_packages()

    assert state["installed"] == {"bash.x86_64", "scylla.x86_64"}
    assert _sudo_cmds(host) == []


def test_a_stale_baseline_is_dropped_instead_of_restored():
    """Weeks after a cleanup that never restored it, the host's own updates since would be removed with it."""
    host, state = _host_with_packages(
        installed={"bash.x86_64", "kernel-6.9.x86_64"}, baseline={"bash.x86_64"}, stale={PACKAGE_BASELINE}
    )

    host.restore_packages()

    assert state["installed"] == {"bash.x86_64", "kernel-6.9.x86_64"}
    assert _sudo_cmds(host) == [f"rm -f {PACKAGE_BASELINE}"]
    assert PACKAGE_BASELINE in host.log.warning.call_args.args[1]


def test_a_stale_baseline_is_recorded_again():
    host, _ = _host_with_packages(installed={"bash.x86_64"}, baseline={"bash.x86_64"}, stale={PACKAGE_BASELINE})

    host.record_host_baseline()

    cmds = _sudo_cmds(host)
    assert cmds[0] == f"rm -f {PACKAGE_BASELINE}"
    assert cmds[-1] == f"mv {PACKAGE_BASELINE}.new {PACKAGE_BASELINE}"
    assert not any(SERVICE_BASELINE in cmd for cmd in cmds)


def test_a_package_the_removal_left_keeps_the_baseline():
    host, _ = _host_with_packages(
        installed={"bash.x86_64", "scylla.x86_64"}, baseline={"bash.x86_64"}, stuck={"scylla.x86_64"}
    )

    host.restore_packages()

    assert f"rm -f {PACKAGE_BASELINE}" not in _sudo_cmds(host)
    assert "scylla.x86_64" in host.log.warning.call_args.args[1]


def _host_with_services(running: set[str], baseline: set[str] | None, failing: set[str] = ()):
    """A host whose services start for real, except the `failing` ones."""
    state = {"running": set(running)}

    def run(cmd, **_):
        if cmd == LIST_RUNNING_SERVICES:
            return MagicMock(stdout="".join(f"{service}\n" for service in sorted(state["running"])))
        return MagicMock(stdout="", ok=True)

    def sudo(cmd, **_):
        if cmd == f"test -s {SERVICE_BASELINE}":
            return MagicMock(stdout="", ok=baseline is not None)
        if cmd == f"cat {SERVICE_BASELINE}":
            return MagicMock(stdout="".join(f"{service}\n" for service in sorted(baseline)))
        if cmd.startswith("systemctl start "):
            state["running"] |= set(cmd.split()[2:]) - set(failing)
        return MagicMock(stdout="", ok=True)

    host = PhysicalHost(name="scylla-db 1.2.3.4", remoter=MagicMock())
    host.remoter.run.side_effect = run
    host.remoter.sudo.side_effect = sudo
    host.log = MagicMock()
    return host, state


def test_a_service_a_package_replaced_is_started_again():
    """A shared host: scylla-kernel-conf replaced tuned, which stopped it; reinstalling tuned does not start it."""
    host, state = _host_with_services(
        running={"sshd.service", "docker.service"}, baseline={"sshd.service", "tuned.service"}
    )

    host.restore_services()

    assert state["running"] == {"sshd.service", "docker.service", "tuned.service"}
    assert _sudo_cmds(host) == [
        "systemctl start tuned.service",
        f"rm -f {SERVICE_BASELINE}",
        "rmdir --ignore-fail-on-non-empty /var/lib/scylla-cluster-tests",
    ]


def test_a_service_that_does_not_start_keeps_the_baseline():
    host, _ = _host_with_services(running=set(), baseline={"tuned.service"}, failing={"tuned.service"})

    host.restore_services()

    assert f"rm -f {SERVICE_BASELINE}" not in _sudo_cmds(host)
    assert "tuned.service" in host.log.warning.call_args.args[1]


def test_no_service_baseline_leaves_the_services_alone():
    host, _ = _host_with_services(running={"sshd.service"}, baseline=None)

    host.restore_services()

    assert _sudo_cmds(host) == []


def test_the_db_host_cleanup_stops_scylla_restores_packages_then_removes_leftovers(monkeypatch):
    host = _host_with_disk_state()
    calls = []
    monkeypatch.setattr(host, "_stop_stale_log_followers", lambda: calls.append("log followers"))
    monkeypatch.setattr(host, "restore_packages", lambda: calls.append("packages"))
    monkeypatch.setattr(host, "restore_services", lambda: calls.append("services"))
    monkeypatch.setattr(host, "remove_scylla_leftovers", lambda: calls.append("leftovers"))
    host.remoter.sudo.side_effect = lambda cmd, **_: calls.append(cmd)

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=True)

    assert calls == ["log followers", "systemctl stop scylla-server.service", "packages", "services", "leftovers"]


def test_a_loader_host_cleanup_skips_the_scylla_steps(monkeypatch):
    host = _host_with_disk_state()
    calls = []
    monkeypatch.setattr(host, "_stop_stale_log_followers", lambda: calls.append("log followers"))
    monkeypatch.setattr(host, "restore_packages", lambda: calls.append("packages"))
    monkeypatch.setattr(host, "restore_services", lambda: calls.append("services"))
    monkeypatch.setattr(host, "remove_scylla_leftovers", lambda: calls.append("leftovers"))

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=False)

    assert calls == ["log followers", "packages", "services"]
    host.remoter.sudo.assert_not_called()


def test_a_failed_package_restore_does_not_skip_the_leftovers(monkeypatch):
    host = _host_with_disk_state()
    calls = []
    monkeypatch.setattr(host, "restore_packages", MagicMock(side_effect=RuntimeError("yum: exited with 1")))
    monkeypatch.setattr(host, "restore_services", lambda: calls.append("services"))
    monkeypatch.setattr(host, "remove_scylla_leftovers", lambda: calls.append("leftovers"))

    host.clean_up_host(tunnel_ports=False, scylla_disk_setup=True)

    assert calls == ["services", "leftovers"]
    assert "the packages of the host, the next cleanup will" in host.log.warning.call_args.args[0]


def test_the_node_records_the_baseline_before_installing_anything(monkeypatch):
    """BaseNode.init() installs vector and rsync once SSH is up: the baseline must not have them."""
    calls = []
    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node.ssh_login_info = {"hostname": None}
    node.logdir = None
    node.log = MagicMock()
    node.test_config = MagicMock(REUSE_CLUSTER=False)
    monkeypatch.setattr(PhysicalMachineNode, "external_address", "1.2.3.4", raising=False)
    monkeypatch.setattr(PhysicalMachineNode, "__str__", lambda self: "db-node-1")
    monkeypatch.setattr(sdcm_cluster.RemoteCmdRunnerBase, "create_remoter", lambda **_: MagicMock())
    for method in ("resolve_ip_addresses", "wait_ssh_up", "_reconfigure_agent", "_init_remoter"):
        monkeypatch.setattr(node, method, lambda *_, **__: None)
    node.record_host_baseline = lambda: calls.append("baseline")
    node.configure_remote_logging = lambda: calls.append("vector")
    node.do_default_installations = lambda: calls.append("rsync")
    node.start_task_threads = MagicMock(side_effect=StopIteration)

    with pytest.raises(StopIteration):
        node.init()

    assert calls == ["baseline", "vector", "rsync"]


def test_a_baseline_that_fails_to_record_does_not_fail_the_node():
    node = PhysicalMachineNode.__new__(PhysicalMachineNode)
    node.log = MagicMock()
    node.record_host_baseline = MagicMock(side_effect=RuntimeError("rpm: exited with 1"))

    node.wait_for_cloud_init()

    node.record_host_baseline.assert_called_once()
    assert "leaves them as they are" in node.log.warning.call_args.args[0]


def _host_with_log_followers(leaders: str) -> PhysicalHost:
    host = PhysicalHost(name="scylla-db 1.2.3.4", remoter=MagicMock())
    host.remoter.run.return_value = MagicMock(stdout=leaders)
    host.log = MagicMock()
    return host


def test_log_followers_a_run_left_are_stopped_with_their_process_group():
    """A run on a shared host left `setsid bash /tmp/logger_cmd_93752.sh` and its sudo journalctl -f, in one process group."""
    host = _host_with_log_followers("1296707\n1306292\n")

    host._stop_stale_log_followers()

    assert host.remoter.run.call_args.args[0] == r"pgrep -f '^bash /tmp/logger_cmd_[0-9]+\.sh$'"
    assert [call.args[0] for call in host.remoter.sudo.call_args_list] == [
        "kill -9 -1296707 -1306292",
        REMOVE_LOG_FOLLOWER_SCRIPTS,
    ]


def test_without_log_followers_only_their_scripts_are_removed():
    """The PIDs in /tmp/logger_<pid>.pid are not used: one the follower had may belong to another process now."""
    host = _host_with_log_followers("")

    host._stop_stale_log_followers()

    assert [call.args[0] for call in host.remoter.sudo.call_args_list] == [REMOVE_LOG_FOLLOWER_SCRIPTS]
