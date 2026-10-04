"""Baremetal backend for SCT (experimental).

Provides cluster management for pre-provisioned physical machines.
This backend is experimental and not regularly tested in CI.
"""

import logging
import re
from typing import Optional, TypedDict

from sdcm import cluster
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.test_config import TestConfig
from sdcm.utils.ldap import LDAP_SSH_TUNNEL_LOCAL_PORT

LOGGER = logging.getLogger(__name__)

# scylla_setup binds /var/lib/scylla/coredump here, so a host's coredumps land on the scylla data disk
SCYLLA_COREDUMP_MOUNT_UNIT = "/etc/systemd/system/var-lib-systemd-coredump.mount"

BASE_NAME = "db-node"
LOADER_NAME = "loader-node"
MONITOR_NAME = "monitor-node"


class NodeInfo(TypedDict):
    public_ip: str
    private_ip: str


class NodeCredentialInformation(TypedDict):
    username: str
    node_list: list[NodeInfo]


class BareMetalCredentials(TypedDict):
    db_nodes: NodeCredentialInformation
    loader_nodes: NodeCredentialInformation
    monitor_nodes: NodeCredentialInformation


class NodeIpsNotConfiguredError(Exception):
    pass


class PhysicalHostCleanup:
    """Undo what a run leaves on a reused physical host that breaks the next run.

    A physical host outlives the run. `hydra clean-resources` cleans it up after the run (see
    sdcm.utils.resources_cleanup.clean_resources_baremetal), and PhysicalMachineNode repeats each step at the start of
    a run, right before what it protects, for a host whose cleanup did not run. Needs `remoter`, `log` and
    `test_config`.
    """

    def clean_up_host(self, tunnel_ports: bool, scylla_disk_setup: bool) -> None:
        """Run the cleanup steps asked for. A failing step is logged and does not skip the other."""
        if tunnel_ports:
            try:
                self._release_stale_tunnel_ports()
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Could not free the SSH tunnel ports, the next run will: %s", exc)
        if scylla_disk_setup:
            try:
                if disks := self._scylla_setup_disks():
                    self.remoter.sudo("systemctl stop scylla-server.service", ignore_status=True)
                    self._reset_scylla_disk_setup(disks)
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Could not reset the scylla disk setup, the next setup will: %s", exc)

    def _release_stale_tunnel_ports(self):
        """Kill the sshd sessions still holding the reverse-tunnel ports of an earlier run.

        A physical host outlives the run. The tunnel of an earlier run keeps its port on the host until sshd drops
        the session, which can take minutes after its runner is gone. The new tunnel cannot bind the port meanwhile,
        so the node ships its logs into the dead tunnel. Runs either at cleanup, once the run's tunnel containers are
        gone, or before a run creates its own tunnels, so any listener found is stale.

        NOTE: this only helps once the earlier runner is gone. A runner still alive restarts its tunnel container and
              races ours for the port, but it then still runs its test on these hosts, which no port handoff can fix.
        """
        ports = {
            self.test_config.SYSLOGNG_SSH_TUNNEL_LOCAL_PORT,
            self.test_config.VECTOR_SSH_TUNNEL_LOCAL_PORT,
            LDAP_SSH_TUNNEL_LOCAL_PORT,
        }
        result = self.remoter.run("sudo ss -Hltnp", ignore_status=True, verbose=False)
        pids = set()
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) < 4 or not fields[3].rpartition(":")[2].isdigit():
                continue
            if int(fields[3].rpartition(":")[2]) in ports:
                pids.update(re.findall(r'"sshd[^"]*",pid=(\d+)', line))
        if pids:
            self.log.warning("Killing sshd sessions %s holding stale SSH tunnel ports of an earlier run", sorted(pids))
            self.remoter.sudo(f"kill {' '.join(sorted(pids))}", ignore_status=True)

    def _reset_scylla_disk_setup(self, disks: list[str]) -> None:
        """Undo what an earlier scylla_setup built on `disks`, the disks this one is about to format.

        Touches nothing scylla_setup would not destroy anyway, and refuses when /var/lib/scylla or an md array
        involves any other device.
        """
        disk_names = {disk.removeprefix("/dev/") for disk in disks}
        arrays = self._md_arrays_on(disk_names)
        allowed_sources = set(disks) | {f"/dev/{array}" for array in arrays}

        # Check every disk before touching any, and let a failing probe stop the reset. lsblk MOUNTPOINT shows one
        # mount per device (and MOUNTPOINTS needs util-linux 2.37), so take all mounts, bind and stacked ones
        # included, from findmnt, and the devices on a disk (the disk and what is built on it: an md array, a
        # partition) from lsblk. /var/lib/scylla may only be mounted from the disks, and the disks only there.
        mounts = self._mounts()
        if foreign := sorted(
            source
            for source, targets in mounts.items()
            if "/var/lib/scylla" in targets and source not in allowed_sources
        ):
            raise cluster.NodeSetupFailed(
                node=self,
                error_msg=f"/var/lib/scylla is mounted from {', '.join(foreign)}, not from the disks for scylla_setup "
                f"({', '.join(disks)}): refusing to reset it",
            )
        # scylla_setup also binds /var/lib/scylla/coredump to /var/lib/systemd/coredump, with a mount unit of its own
        coredump_bind = self.remoter.run(f"test -e {SCYLLA_COREDUMP_MOUNT_UNIT}", ignore_status=True).ok
        scylla_mounts = {"/var/lib/scylla", "/var/lib/systemd/coredump"} if coredump_bind else {"/var/lib/scylla"}
        for disk in disks:
            devices = self.remoter.run(f"lsblk -nrp -o NAME {disk}").stdout.split()
            targets = set().union(*(mounts.get(device, set()) for device in devices))
            if mountpoints := sorted(targets - scylla_mounts):
                raise cluster.NodeSetupFailed(
                    node=self, error_msg=f"{disk} is mounted at {', '.join(mountpoints)}: refusing to wipe it"
                )
        self.log.info("Resetting the scylla disk setup of an earlier run on %s", ", ".join(disks))
        if coredump_bind:
            # The bind still holds the disk once /var/lib/scylla is unmounted, so it goes first. The next
            # scylla_setup recreates it.
            self.remoter.sudo("systemctl stop var-lib-systemd-coredump.mount", ignore_status=True)
            if any("/var/lib/systemd/coredump" in self._mounts().get(device, set()) for device in allowed_sources):
                self.remoter.sudo("umount /var/lib/systemd/coredump")
            self.remoter.sudo(
                f"rm -f {SCYLLA_COREDUMP_MOUNT_UNIT} /etc/systemd/system/*.wants/var-lib-systemd-coredump.mount"
            )
        self.remoter.sudo("systemctl stop var-lib-scylla.mount", ignore_status=True)
        # Stopping the unit unmounts it; only a mount made some other way (fstab, by hand) is still there
        if any("/var/lib/scylla" in targets for targets in self._mounts().values()):
            self.remoter.sudo("umount /var/lib/scylla")
        self.remoter.sudo(
            "rm -f /etc/systemd/system/var-lib-scylla.mount /etc/systemd/system/*.wants/var-lib-scylla.mount"
        )
        self.remoter.sudo(r"sed -i '\#[[:space:]]/var/lib/scylla[[:space:]]#d' /etc/fstab")
        self.remoter.sudo("systemctl daemon-reload")

        for array in arrays:
            self.remoter.sudo(f"mdadm --stop /dev/{array}")
        for disk in disks:
            self.remoter.sudo(f"mdadm --zero-superblock {disk}", ignore_status=True)
            self.remoter.sudo(f"wipefs -a {disk}")
        self.remoter.sudo("rm -f /etc/scylla.d/io.conf /etc/scylla.d/io_properties.yaml")

    def _mounts(self) -> dict[str, set[str]]:
        """Every mount on the host, as the targets each source device is mounted at."""
        mounts = {}
        for line in self.remoter.run("findmnt -rn -o SOURCE,TARGET").stdout.splitlines():
            source, _, target = line.partition(" ")
            # A bind mount of a directory has a source like /dev/nvme1n1[/subdir]
            mounts.setdefault(source.split("[", 1)[0], set()).add(target)
        return mounts

    def _md_arrays_on(self, disk_names: set[str]) -> list[str]:
        """The md arrays built from `disk_names`, refusing any that also uses another device."""
        arrays = []
        # No /proc/mdstat means the md driver is not loaded, so there is no array; any other failure stops the reset
        mdstat = self.remoter.run("if [ -e /proc/mdstat ]; then cat /proc/mdstat; fi").stdout
        for line in mdstat.splitlines():
            if not (match := re.match(r"(md\d+) : .*", line)):
                continue
            members = set(re.findall(r"(\w+)\[\d+\]", line))
            if not members & disk_names:
                continue
            if members - disk_names:
                raise cluster.NodeSetupFailed(
                    node=self,
                    error_msg=f"/dev/{match.group(1)} also uses {', '.join(sorted(members - disk_names))}, "
                    f"which are not disks for scylla_setup: refusing to stop it",
                )
            arrays.append(match.group(1))
        return arrays

    def _scylla_setup_disks(self) -> list[str]:
        """The disks scylla_setup set /var/lib/scylla up on, or none if it did not.

        Its var-lib-scylla.mount must be there, and /var/lib/scylla on a whole disk or on an md array of them. A
        host prepared by hand, e.g. on a partition, is left alone.
        """
        if not self.remoter.run("test -e /etc/systemd/system/var-lib-scylla.mount", ignore_status=True).ok:
            return []
        sources = [source for source, targets in self._mounts().items() if "/var/lib/scylla" in targets]
        if len(sources) != 1:
            return []
        source = sources[0]
        if match := re.fullmatch(r"/dev/(md\d+)", source):
            mdstat = self.remoter.run("if [ -e /proc/mdstat ]; then cat /proc/mdstat; fi").stdout
            line = next((line for line in mdstat.splitlines() if line.startswith(f"{match.group(1)} : ")), "")
            return sorted(f"/dev/{member}" for member in re.findall(r"(\w+)\[\d+\]", line))
        if self.remoter.run(f"lsblk -ndo TYPE {source}", ignore_status=True).stdout.strip() == "disk":
            return [source]
        return []


class PhysicalMachineNode(PhysicalHostCleanup, cluster.BaseNode):
    log = LOGGER

    def __init__(
        self,
        name,
        parent_cluster: "PhysicalMachineCluster",
        public_ip,
        private_ip,
        credentials,
        base_logdir=None,
        node_prefix=None,
        after_config=None,
        dc_idx=0,
        rack=0,
        node_index=0,
    ):
        self.node_index = node_index
        ssh_login_info = {
            "hostname": None,
            "user": getattr(parent_cluster, "ssh_username", credentials.name),
            "key_file": credentials.key_file,
        }
        self._public_ip = public_ip
        self._private_ip = private_ip
        super().__init__(
            name=name,
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            ssh_login_info=ssh_login_info,
            node_prefix=node_prefix,
            after_config=after_config,
            dc_idx=dc_idx,
            rack=rack,
        )

    def init(self):
        super().init()
        self.set_hostname()

    def _init_port_mapping(self):
        if self.test_config.IP_SSH_CONNECTIONS == "public":
            self._release_stale_tunnel_ports()
        super()._init_port_mapping()

    def wait_for_cloud_init(self):
        pass

    def _get_public_ip_address(self) -> Optional[str]:
        return self._public_ip

    @property
    def vm_region(self):
        return "baremetal"

    @property
    def region(self):
        return "baremetal"

    def scylla_setup(self, disks, devname: str):
        # A physical host is reused across runs. clean_scylla() removes the package and the data, but not what
        # scylla_setup built. With its var-lib-scylla.mount left in place, scylla_setup skips RAID setup, and with it
        # scylla_io_setup, and exits 1, and Scylla then fails with "Bad I/O Scheduler configuration" on the stub
        # io.conf the reinstall left behind. `hydra clean-resources` resets that after a run, but a host can still
        # arrive dirty: kept after a failure, or from a run whose cleanup did not run. So undo whatever is left first,
        # and let a failure of this setup fail.
        self._reset_scylla_disk_setup(disks)
        super().scylla_setup(disks, devname)

    def _get_private_ip_address(self) -> Optional[str]:
        return self._private_ip

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self.log.warning(
            "_set_keep_duration is not implemented for PhysicalMachineNode, since there's no tagging for baremetal nodes."
        )

    def set_hostname(self):
        # disabling since for baremetal we aren't going to fuss with their names, they are preconfigured
        pass

    def reboot(self, hard=True, verify_ssh=True):
        raise NotImplementedError("reboot not implemented")

    def restart(self):
        self.remoter.run("sudo reboot -h now", ignore_status=True)

    def destroy(self):
        self.stop_task_threads()  # For future implementation of destroy
        self.wait_till_tasks_threads_are_stopped()
        super().destroy()


class PhysicalHost(PhysicalHostCleanup):
    """A bare-metal host reached by SSH alone, for cleaning it up after its run, with no cluster around it."""

    def __init__(self, name: str, remoter):
        self.name = name
        self.remoter = remoter
        self.log = LOGGER
        self.test_config = TestConfig()

    def __str__(self):
        return self.name


class PhysicalMachineCluster(cluster.BaseCluster):
    def __init__(self, **kwargs):
        self.nodes = []
        self.credentials = kwargs.pop("credentials")
        n_nodes = kwargs.get("n_nodes")
        self._node_public_ips = kwargs.pop("public_ips", None) or []
        self._node_private_ips = kwargs.pop("private_ips", None) or []
        node_cnt = n_nodes[0] if isinstance(n_nodes, list) else n_nodes
        if len(self._node_public_ips) < node_cnt or len(self._node_private_ips) < node_cnt:
            raise NodeIpsNotConfiguredError("Physical hosts IPs are not configured!")
        super().__init__(**kwargs)

    @property
    def ssh_username(self) -> str:
        return self._ssh_username

    def _create_node(self, name, public_ip, private_ip, dc_idx, rack=0, node_index=0, after_config=None):
        node = PhysicalMachineNode(
            name,
            parent_cluster=self,
            public_ip=public_ip,
            private_ip=private_ip,
            credentials=self.credentials[0],
            base_logdir=self.logdir,
            node_prefix=self.node_prefix,
            dc_idx=dc_idx,
            rack=rack,
            node_index=node_index,
            after_config=after_config,
        )
        node.init()
        return node

    def _reuse_cluster_setup(self, node):
        node.run_startup_script()  # Reconfigure syslog-ng.

    @mark_new_nodes_as_running_nemesis
    def add_nodes(
        self,
        count,
        ec2_user_data="",
        dc_idx=0,
        rack=0,
        enable_auto_bootstrap=False,
        instance_type=None,
        after_config=None,
    ):
        assert instance_type is None, "baremetal can't provision different types"
        # Validate the whole request first: creating some of the nodes and then raising would
        # leave them on self.nodes, and _node_index advanced, while the caller gets an exception
        # instead of the list it needs to initialize them with.
        #
        # A host needs both of its addresses, and the constructor only checks the two lists
        # against the *initial* node count -- so count the pairs, not the public IPs, or a later
        # grow slips past this guard and dies on _node_private_ips[node_index].
        configured_hosts = min(len(self._node_public_ips), len(self._node_private_ips))
        if self._node_index + count > configured_hosts:
            raise NodeIpsNotConfiguredError(
                f"{self.node_prefix}: {count} node(s) requested from index {self._node_index}, but only "
                f"{configured_hosts} physical host(s) are configured with both a public and a private address"
            )

        added_nodes = []
        for _ in range(count):
            # BaseCluster._node_index persists across calls, as it does on AWS.  Counting from
            # zero per call instead would hand a second call the same name and the same physical
            # host as the first -- and restart the rack round-robin with it.
            node_index = self._node_index
            node_name = "%s-%s" % (self.node_prefix, node_index)
            # `rack is None` is how BaseCluster says "spread the nodes yourself", which it does
            # whenever simulated_racks is set.  A physical host has no availability zone to derive
            # a rack from -- without this every node lands in RACK0, and SCT enables
            # rf_rack_valid_keyspaces for every test in defaults/test_default.yaml, so any keyspace
            # with RF > 1 is then rejected as not RF-rack-valid.  Round-robin over racks_count, as
            # the cloud backends do; the rack reaches cassandra-rackdc.properties through
            # SnitchConfig, which BaseScyllaCluster.node_setup applies once simulated_racks > 1.
            node_rack = node_index % self.racks_count if rack is None else rack
            node = self._create_node(
                node_name,
                self._node_public_ips[node_index],
                self._node_private_ips[node_index],
                dc_idx=dc_idx,
                rack=node_rack,
                node_index=node_index,
                after_config=after_config,
            )
            self.nodes.append(node)
            added_nodes.append(node)
            self._node_index += 1
        # BaseCluster.add_nodes is documented to return the list of nodes, and both cloud backends
        # do; this one returned None, so any caller using the result (the nemesis add-node paths)
        # would fail on it.
        return added_nodes


class ScyllaPhysicalCluster(cluster.BaseScyllaCluster, PhysicalMachineCluster):
    def _reuse_cluster_setup(self, node):
        PhysicalMachineCluster._reuse_cluster_setup(self, node)

    def __init__(self, **kwargs):
        user_prefix = kwargs.pop("user_prefix")
        if username := kwargs.pop("ssh_username", None):
            self._ssh_username = username
        kwargs.update(
            dict(
                cluster_prefix=cluster.prepend_user_prefix(user_prefix, "db-cluster"),
                node_prefix=cluster.prepend_user_prefix(user_prefix, "db-node"),
                node_type="scylla-db",
            )
        )
        super().__init__(**kwargs)


class LoaderSetPhysical(cluster.BaseLoaderSet, PhysicalMachineCluster):
    def __init__(self, **kwargs):
        user_prefix = kwargs.pop("user_prefix")
        if username := kwargs.pop("ssh_username", None):
            self._ssh_username = username
        kwargs.update(
            dict(
                cluster_prefix=cluster.prepend_user_prefix(user_prefix, "loader-set"),
                node_prefix=cluster.prepend_user_prefix(user_prefix, "loader-node"),
                node_type="loader",
            )
        )
        cluster.BaseLoaderSet.__init__(self, kwargs["params"])
        PhysicalMachineCluster.__init__(self, **kwargs)

    @classmethod
    def _get_node_ips_param(cls, ip_type="public"):
        return cluster.BaseLoaderSet.get_node_ips_param(ip_type)


class MonitorSetPhysical(cluster.BaseMonitorSet, PhysicalMachineCluster):
    def __init__(self, **kwargs):
        user_prefix = kwargs.pop("user_prefix")
        if username := kwargs.pop("ssh_username", None):
            self._ssh_username = username
        kwargs.update(
            dict(
                cluster_prefix=cluster.prepend_user_prefix(user_prefix, "monitor-set"),
                node_prefix=cluster.prepend_user_prefix(user_prefix, "monitor-node"),
                node_type="monitor",
            )
        )
        cluster.BaseMonitorSet.__init__(self, targets=kwargs["targets"], params=kwargs["params"])
        kwargs.pop("targets")
        PhysicalMachineCluster.__init__(self, **kwargs)
