"""Baremetal backend for SCT (experimental).

Provides cluster management for pre-provisioned physical machines.
This backend is experimental and not regularly tested in CI.
"""

import logging
import re
import shlex
from typing import Optional, TypedDict

from sdcm import cluster
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.test_config import TestConfig
from sdcm.utils.apt import apt_cmd
from sdcm.utils.ldap import LDAP_SSH_TUNNEL_LOCAL_PORT
from sdcm.utils.remote_logger import journal_time
from sdcm.utils.rpm import rpm_cmd

LOGGER = logging.getLogger(__name__)

# scylla_setup binds /var/lib/scylla/coredump here, so a host's coredumps land on the scylla data disk
SCYLLA_COREDUMP_MOUNT_UNIT = "/etc/systemd/system/var-lib-systemd-coredump.mount"
# What an earlier Scylla install leaves on a host once its packages are removed: the repo SCT added, the config files
# it changed (kept by the package manager, e.g. as .rpmsave), what scylla_setup and SCT wrote next to them (unit
# drop-ins), and the files Scylla's python and scylla-housekeeping wrote at run time (bytecode caches, its uuid)
SCYLLA_CONFIG_LEFTOVERS = (
    "/etc/yum.repos.d/scylla*.repo",
    "/etc/zypp/repos.d/scylla*.repo",
    "/etc/apt/sources.list.d/scylla*.list",
    "/etc/scylla",
    "/etc/scylla.d",
    "/etc/sysconfig/scylla-server*",
    "/etc/default/scylla-server*",
    "/etc/systemd/system/scylla-*.service.d",
    "/opt/scylladb",
    "/var/lib/scylla-housekeeping",
)
# The data directory and the mount units scylla_setup made for it, unless it is still on a disk scylla_setup set up
SCYLLA_DATA_LEFTOVERS = (
    "/var/lib/scylla",
    "/etc/systemd/system/var-lib-scylla.mount",
    "/etc/systemd/system/*.wants/var-lib-scylla.mount",
    SCYLLA_COREDUMP_MOUNT_UNIT,
    "/etc/systemd/system/*.wants/var-lib-systemd-coredump.mount",
)

# The packages a host had, and the services it ran, before SCT first installed anything on it, which the cleanup after
# the run restores. They outlive a run whose cleanup did not run, so the next run keeps them, and its cleanup still
# restores the original state.
BASELINE_DIR = "/var/lib/scylla-cluster-tests"
PACKAGE_BASELINE = f"{BASELINE_DIR}/packages.baseline"
SERVICE_BASELINE = f"{BASELINE_DIR}/services.baseline"
# The installed packages, one per line as the package manager takes them: name.arch (rpm), name:arch (dpkg). The repo
# keys rpm lists as gpg-pubkey packages are not packages to restore.
LIST_PACKAGES = (
    "if [ -e /etc/debian_version ]; then "
    "dpkg-query -W -f '${db:Status-Abbrev} ${Package}:${Architecture}\\n' | awk '/^ii/ {print $2}'; "
    "else rpm -qa --qf '%{NAME}.%{ARCH}\\n' | grep -v '^gpg-pubkey[.]'; fi"
)
# The running services. Template instances (user@1001.service) belong to sessions and jobs, not to the host's setup.
LIST_RUNNING_SERVICES = (
    "systemctl list-units --type=service --state=running --no-legend --plain | awk '{print $1}' | grep -v @"
)

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
        """Run the cleanup steps asked for, and restore the host's packages. A failing step is logged and does not
        skip the others.

        `scylla_disk_setup` is for a DB host the run installed Scylla on: its disk setup is reset, and once its
        packages are restored, the files the Scylla install left are removed.
        """
        try:
            self._stop_stale_log_followers()
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Could not stop the log followers the run left, the next cleanup will: %s", exc)
        if tunnel_ports:
            try:
                self._release_stale_tunnel_ports()
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Could not free the SSH tunnel ports, the next run will: %s", exc)
        if scylla_disk_setup:
            try:
                # Before the disk reset, which unmounts the data directory, and the package restore, which removes it
                self.remoter.sudo("systemctl stop scylla-server.service", ignore_status=True)
                if disks := self._scylla_setup_disks():
                    self._reset_scylla_disk_setup(disks)
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Could not reset the scylla disk setup, the next setup will: %s", exc)
        try:
            self.restore_packages()
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Could not restore the packages of the host, the next cleanup will: %s", exc)
        try:
            self.restore_services()
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Could not restore the services of the host, the next cleanup will: %s", exc)
        if scylla_disk_setup:
            try:
                self.remove_scylla_leftovers()
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Could not remove the leftovers of the Scylla install, the next install will: %s", exc)

    def record_host_baseline(self) -> None:
        """Record the packages the host has and the services it runs, for the cleanup after the run to restore.

        A baseline already on the host is kept: it was recorded by an earlier run whose cleanup did not run, or did not
        restore all of it, and it holds the host from before that run. Each baseline is kept on its own, as a cleanup
        can restore one and not the other.
        """
        missing = []
        for baseline, list_cmd in ((SERVICE_BASELINE, LIST_RUNNING_SERVICES), (PACKAGE_BASELINE, LIST_PACKAGES)):
            if self._baseline_exists(baseline):
                self.log.info("Keeping the %s an earlier run left on the host, its cleanup restores it", baseline)
            else:
                missing.append((baseline, list_cmd))
        if not missing:
            return
        self.remoter.sudo(f"mkdir -p {BASELINE_DIR}")
        # Each written aside first: a baseline cut short by a failure would remove every package it misses
        for baseline, list_cmd in missing:
            self.remoter.sudo(f"sh -c {shlex.quote(f'{list_cmd} > {baseline}.new')}")
            self.remoter.sudo(f"mv {baseline}.new {baseline}")
        self.log.info(
            "Recorded %s, the cleanup after the run restores them", ", ".join(baseline for baseline, _ in missing)
        )

    def _baseline_exists(self, baseline: str) -> bool:
        # With sudo, as root wrote it: the user SSH logs in as may not be able to read it
        return self.remoter.sudo(f"test -s {baseline}", ignore_status=True).ok

    def restore_packages(self) -> None:
        """Restore the packages of the baseline: remove what was installed since, reinstall what was removed.

        Packages are compared by name, so an upgrade is kept: SCT upgrades openssh and openssl to fix sshd, and
        downgrading them could cut the host off. A reinstalled package can need a package the baseline does not
        have, as its newer version may: that one is kept. The baseline is removed once every package of it is
        installed and every package installed since is removed, and kept otherwise, for the next cleanup to retry.

        NOTE: a package something else installed on the host while the run was on it is removed too.
        """
        if not self._baseline_exists(PACKAGE_BASELINE):
            self.log.info("No package baseline on the host, its packages are left as they are")
            return
        baseline = set(self.remoter.sudo(f"cat {PACKAGE_BASELINE}").stdout.split())
        added = set(self.remoter.run(LIST_PACKAGES).stdout.split()) - baseline
        debian = self.remoter.run("test -e /etc/debian_version", ignore_status=True).ok
        if added:
            self.log.info("Removing the packages installed since the baseline: %s", " ".join(sorted(added)))
            self.remoter.sudo(self._package_cmd(debian, "remove", added), ignore_status=True, retry=3, timeout=900)
        before_reinstall = set(self.remoter.run(LIST_PACKAGES).stdout.split())
        if removed := baseline - before_reinstall:
            self.log.info("Reinstalling the packages removed since the baseline: %s", " ".join(sorted(removed)))
            self.remoter.sudo(self._package_cmd(debian, "install", removed), ignore_status=True, retry=3, timeout=900)
        installed = set(self.remoter.run(LIST_PACKAGES).stdout.split())
        if differ := sorted((baseline - installed) | (added & installed)):
            self.log.warning(
                "The packages still differ from the baseline, kept for the next cleanup: %s", " ".join(differ)
            )
            return
        if pulled_in := sorted(installed - before_reinstall - baseline):
            self.log.info("Keeping the packages the reinstalled ones need: %s", " ".join(pulled_in))
        self.remoter.sudo(f"rm -f {PACKAGE_BASELINE}")
        self.log.info("The packages of the host match the baseline again")

    def restore_services(self) -> None:
        """Start the services of the baseline that are stopped, e.g. one a package SCT installed replaced.

        Only starts: a service the baseline does not have may be another job's. The baseline is removed once each of
        its services runs, and kept otherwise, for the next cleanup to retry.
        """
        if not self._baseline_exists(SERVICE_BASELINE):
            self.log.info("No service baseline on the host, its services are left as they are")
            return
        baseline = set(self.remoter.sudo(f"cat {SERVICE_BASELINE}").stdout.split())
        if stopped := sorted(baseline - set(self.remoter.run(LIST_RUNNING_SERVICES).stdout.split())):
            self.log.info("Starting the services stopped since the baseline: %s", " ".join(stopped))
            self.remoter.sudo(f"systemctl start {' '.join(stopped)}", ignore_status=True)
        if stopped := sorted(baseline - set(self.remoter.run(LIST_RUNNING_SERVICES).stdout.split())):
            self.log.warning(
                "The services still differ from the baseline, kept for the next cleanup: %s", " ".join(stopped)
            )
            return
        self.remoter.sudo(f"rm -f {SERVICE_BASELINE}")
        # Gone once both baselines are: the package one may still be kept for the next cleanup
        self.remoter.sudo(f"rmdir --ignore-fail-on-non-empty {BASELINE_DIR}", ignore_status=True)
        self.log.info("The services of the host run as in the baseline again")

    @staticmethod
    def _package_cmd(debian: bool, action: str, packages: set[str]) -> str:
        names = " ".join(sorted(packages))
        # Removing a package removes no dependency it was installed with, which the baseline may have, and reinstalling
        # one installs no package it only recommends, which the baseline did not have
        if debian:
            subcommand = "purge -y" if action == "remove" else "install -y --no-install-recommends"
            return f"DEBIAN_FRONTEND=noninteractive {apt_cmd(f'{subcommand} {names}', lock_wait=True)}"
        setopt = "clean_requirements_on_remove" if action == "remove" else "install_weak_deps"
        return rpm_cmd("yum", f"{action} -y --setopt={setopt}=False {names}")

    def remove_scylla_leftovers(self):
        """Remove what an earlier Scylla install left on the host, so this install starts from a clean host.

        Removing the packages keeps the config files a run changed, and everything scylla_setup and SCT added. Without a
        data disk (scylla_setup_no_raid) the data directory survives too: clean_scylla_data() leaves its schema,
        system tables and caches. /var/lib/scylla still on a disk an earlier scylla_setup set up is left to the reset
        of that disk.
        """
        paths = list(SCYLLA_CONFIG_LEFTOVERS)
        if self._scylla_dir_mounted():
            self.log.info("/var/lib/scylla is on a disk an earlier scylla_setup set up, the disk reset resets it")
        else:
            paths += SCYLLA_DATA_LEFTOVERS
        # ls -d lists what exists, and drops the patterns that match nothing, with no error to log for each
        found = self.remoter.run(f"ls -d {' '.join(paths)} 2>/dev/null", ignore_status=True).stdout.split()
        if not found:
            self.log.info("No leftovers of an earlier Scylla install on the host")
            return
        self.log.info("Removing leftovers of an earlier Scylla install: %s", ", ".join(found))
        if "/var/lib/scylla" in found:
            self._undo_coredump_bind()
        self.remoter.sudo(f"rm -rf {' '.join(found)}")
        self.remoter.sudo("systemctl daemon-reload")
        # A Scylla unit that failed stays listed as failed once its package is gone, e.g. scylla-node-exporter
        self.remoter.sudo("systemctl reset-failed 'scylla-*'", ignore_status=True)

    def _undo_coredump_bind(self, data_sources: frozenset[str] = frozenset()) -> None:
        """Undo scylla_setup's bind of /var/lib/scylla/coredump to /var/lib/systemd/coredump, which keeps the data
        directory, and the disk under it, busy. The next scylla_setup recreates it.

        Stopping its unit unmounts it. A mount still there is unmounted only when it is that bind: of
        /var/lib/scylla/coredump, or from `data_sources`, the devices /var/lib/scylla is mounted from.
        """
        self.remoter.sudo("systemctl stop var-lib-systemd-coredump.mount", ignore_status=True)
        sources = self.remoter.run(
            "findmnt -rn -o SOURCE --mountpoint /var/lib/systemd/coredump", ignore_status=True
        ).stdout.split()
        # A bind of a directory has a source like /dev/nvme0n1p4[/var/lib/scylla/coredump]
        if any(
            source.endswith("[/var/lib/scylla/coredump]") or source.split("[", 1)[0] in data_sources
            for source in sources
        ):
            self.remoter.sudo("umount /var/lib/systemd/coredump")
        self.remoter.sudo(
            f"rm -f {SCYLLA_COREDUMP_MOUNT_UNIT} /etc/systemd/system/*.wants/var-lib-systemd-coredump.mount"
        )

    def _scylla_dir_mounted(self) -> bool:
        return any("/var/lib/scylla" in targets for targets in self._mounts().values())

    def _stop_stale_log_followers(self) -> None:
        """Stop the log followers a run left on the host, and remove their scripts.

        RemoteLogger starts each as `setsid bash /tmp/logger_cmd_<pid>.sh`, the leader of a process group with the
        `journalctl -f` it runs, and kills that group when the run stops it. A run that did not stop it leaves it
        following the journal of a host that outlives the run. The leaders are found by their command line, not by
        /tmp/logger_<pid>.pid: the PID in a file a run left may belong to another process by now.
        """
        leaders = self.remoter.run(
            r"pgrep -f '^bash /tmp/logger_cmd_[0-9]+\.sh$'", ignore_status=True, verbose=False
        ).stdout.split()
        if leaders := sorted(pid for pid in leaders if pid.isdigit()):
            self.log.warning("Stopping the log followers a run left on the host: process groups %s", ", ".join(leaders))
            self.remoter.sudo(f"kill -9 {' '.join(f'-{pid}' for pid in leaders)}", ignore_status=True)
        self.remoter.sudo("rm -f /tmp/logger_*.pid /tmp/logger_cmd_*.sh", ignore_status=True)

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
            # The bind still holds the disk once /var/lib/scylla is unmounted, so it goes first
            self._undo_coredump_bind(frozenset(allowed_sources))
        self.remoter.sudo("systemctl stop var-lib-scylla.mount", ignore_status=True)
        # Stopping the unit unmounts it; only a mount made some other way (fstab, by hand) is still there
        if self._scylla_dir_mounted():
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
    journal_outlives_run = True

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
        try:
            self.record_host_baseline()
        except Exception as exc:  # noqa: BLE001
            self.log.warning(
                "Could not record the packages and services of the host, the cleanup leaves them as they are: %s", exc
            )

    def start_task_threads(self):
        self.mark_run_start_in_journal()
        super().start_task_threads()

    def mark_run_start_in_journal(self):
        """Mark in the host's journal where this run starts, and read the journal from there.

        A physical host outlives the run, and so does its journal: SCT never reboots it, and even a volatile journal
        lasts until a reboot. Read from its beginning, it replays every earlier run on the host into this run's logs,
        where an earlier crash or backtrace raises its events again. The time is taken from the host's clock, before the
        mark is written, so the mark is the first entry read.
        """
        if self.journal_since:
            return
        test_id = self.test_config.test_id()
        result = self.remoter.run(
            f'date +%s; logger -t scylla-cluster-tests "SCT test {test_id} starts on this host"', ignore_status=True
        )
        epoch = next(iter(result.stdout.splitlines()), "").strip()
        if not epoch.isdigit():
            self.log.warning("Could not mark the start of the run in the journal, its logs include earlier runs")
            return
        self.journal_since = f"@{epoch}"
        self.log.info(
            "SCT test %s starts on this host at %s, its journal is read from there",
            test_id,
            journal_time(self.journal_since),
        )

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
        if disks:
            self._reset_scylla_disk_setup(disks)
        elif self._scylla_dir_mounted():
            raise cluster.NodeSetupFailed(
                node=self,
                error_msg="/var/lib/scylla is still on a disk an earlier run set up, and scylla_setup_no_raid keeps it "
                "on the root filesystem: reset the host with `hydra clean-resources` first",
            )
        super().scylla_setup(disks, devname)

    def clean_scylla(self):
        super().clean_scylla()
        self.remove_scylla_leftovers()

    def _get_private_ip_address(self) -> Optional[str]:
        return self._private_ip

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self.log.warning(
            "_set_keep_duration is not implemented for PhysicalMachineNode, since there's no tagging for baremetal nodes."
        )

    def set_hostname(self):
        # disabling since for baremetal we aren't going to fuss with their names, they are preconfigured
        pass

    @property
    def short_hostname(self):
        """The name this host's logs arrive under: BaseNode.system_log reads <logdir>/hosts/<it>/messages.log.

        vector files them under the hostname the journal reports. A physical host keeps its own (see set_hostname),
        often fully qualified, e.g. ip-10-4-13-234.eu-west-1.compute.internal, which `hostname -s` would cut short.
        """
        if not self.test_config.VECTOR_ADDRESS:
            return super().short_hostname
        if not self._short_hostname:
            try:
                self._short_hostname = self.remoter.run("hostname").stdout.strip()
            except Exception:  # noqa: BLE001
                return "no_booted_yet"
        return self._short_hostname

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
