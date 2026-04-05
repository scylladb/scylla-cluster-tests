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

import contextlib
import logging
import os
import tempfile
import threading
import time
from functools import cached_property
from typing import ContextManager, List

import yaml

from sdcm import wait
from sdcm.cassandra_exporter_setup import CassandraExporterSetup
from sdcm.exceptions import KillNemesis, NodeNotReady
from sdcm.node_exporter_setup import NodeExporterSetup
from sdcm.remote.remote_file import remote_file, dict_to_yaml_file, yaml_file_to_dict
from sdcm.sct_config import TestConfig
from sdcm.utils.common import raise_exception_in_thread
from sdcm.utils.decorators import log_run_info, optional_stage
from sdcm.utils.raft import NoRaft

LOGGER = logging.getLogger(__name__)

CASSANDRA_YAML_PATH = "/etc/cassandra/cassandra.yaml"
CASSANDRA_SERVICE_NAME = "cassandra"
CASSANDRA_ENV_SH_PATH = "/etc/cassandra/cassandra-env.sh"
CASSANDRA_RACKDC_PATH = "/etc/cassandra/cassandra-rackdc.properties"


def compute_jvm_heap_mb(total_ram_mb: int) -> tuple[int, int]:
    """Compute Cassandra JVM heap sizes following current Cassandra recommendations.

    Per the Apache Cassandra documentation:
    - Heap should be no less than 2 GB and no more than 50% of system RAM.
    - Heaps smaller than 12 GB should use ParNew/CMS; larger heaps may use G1GC.

    For small systems (< 4 GB RAM) we fall back to 50% of RAM with a 256 MB floor.

    Args:
        total_ram_mb: Total system RAM in megabytes.

    Returns:
        Tuple of (max_heap_mb, heap_new_mb).
    """
    half_ram = total_ram_mb // 2

    # 50% of RAM, but at least 2 GB when the system can afford it
    if total_ram_mb >= 4096:
        max_heap = max(half_ram, 2048)
    else:
        max_heap = half_ram

    # Floor: never go below 256 MB (tiny containers / CI)
    max_heap = max(max_heap, 256)

    heap_new = min(max_heap // 4, 800)
    return max_heap, heap_new


class CassandraNodeMixin:
    """Mixin for nodes running Cassandra instead of Scylla.

    Overrides Scylla-specific properties on BaseNode that would log
    errors or fail on a Cassandra node (no /etc/scylla, no scylla binary).
    """

    @property
    def smp(self):
        return ""

    @property
    def cpuset(self):
        return ""

    def _gen_nodetool_cmd(self, sub_cmd, args, options):
        """Use nodetool from PATH (Cassandra installs to /usr/bin or /opt/cassandra/bin)."""
        credentials = self.parent_cluster.get_db_auth()
        if credentials:
            options += "-u {} -pw '{}' ".format(*credentials)
        return f"nodetool {options} {sub_cmd} {args}"

    @contextlib.contextmanager
    def remote_scylla_yaml(self) -> ContextManager[dict]:
        """Read/write cassandra.yaml instead of scylla.yaml."""
        with remote_file(
            remoter=self.remoter,
            remote_path=CASSANDRA_YAML_PATH,
            serializer=dict_to_yaml_file,
            deserializer=yaml_file_to_dict,
            sudo=True,
        ) as cassandra_yaml:
            yield CassandraYamlAttrProxy(cassandra_yaml)

    def extract_seeds_from_scylla_yaml(self):
        """Extract seed IPs from cassandra.yaml."""
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix="sct"), "cassandra.yaml")
        wait.wait_for(
            func=self.remoter.receive_files,
            step=10,
            text="Waiting for copying cassandra.yaml",
            timeout=300,
            throw_exc=True,
            src=CASSANDRA_YAML_PATH,
            dst=yaml_dst_path,
        )
        with open(yaml_dst_path, encoding="utf-8") as yaml_stream:
            conf_dict = yaml.safe_load(yaml_stream)

        try:
            node_seeds = conf_dict["seed_provider"][0]["parameters"][0].get("seeds")
        except (KeyError, IndexError, TypeError) as details:
            self.log.debug("Loaded YAML data structure: %s", conf_dict)
            raise ValueError("Exception determining seed node ips from cassandra.yaml") from details

        if node_seeds:
            return [s.strip() for s in node_seeds.split(",")]
        return []

    @property
    def is_server_encrypt(self):
        result = self.remoter.run(f"grep '^server_encryption_options:' {CASSANDRA_YAML_PATH}", ignore_status=True)
        return "server_encryption_options" in result.stdout.lower()

    @cached_property
    def raft(self):
        return NoRaft(self)

    @cached_property
    def cql_address(self):
        return self.ip_address

    def wait_native_transport(self):
        """Cassandra has no Scylla REST API; check CQL port instead."""
        if not self.db_up():
            raise NodeNotReady(f"Node {self.name} CQL port not ready")

    def db_up(self):
        return self.is_port_used(port=self.CQL_PORT, service_name=CASSANDRA_SERVICE_NAME)

    def config_setup(self, append_scylla_args="", debug_install=False, **_):
        pass

    def get_scylla_binary_version(self):
        """Return Cassandra version without invoking /usr/bin/scylla (not present on Cassandra nodes)."""
        result = self.remoter.run(
            "dpkg-query --show --showformat '${Version}' cassandra",
            ignore_status=True,
        )
        if result.ok and result.stdout.strip():
            return result.stdout.strip()
        result = self.remoter.run("nodetool version", ignore_status=True)
        if result.ok:
            for line in result.stdout.splitlines():
                if "ReleaseVersion:" in line:
                    return line.split(":", 1)[1].strip()
        return None


class CassandraYamlAttrProxy:
    """Wraps a plain dict so attribute access works like on ScyllaYaml.

    ``report_scylla_yaml_to_argus`` calls ``scylla_yml.model_dump()``
    and accesses attributes like ``broadcast_rpc_address``.  This proxy
    makes a raw cassandra.yaml dict behave similarly.
    """

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError:
            return None

    def model_dump(self, **_kwargs):
        return dict(self._data)


class BaseCassandraCluster:
    """Backend-agnostic mixin for Cassandra clusters.

    Provides Cassandra-specific initialization, seed management no-ops,
    and node setup logic for cloud VM backends. Intended to be mixed in
    with a backend-specific cluster class (AWSCluster, GCECluster, DockerCluster).

    For Docker backend, node_setup() is overridden to no-op since the Docker
    image entrypoint handles installation. For cloud backends, node_setup()
    installs Cassandra via the official apt repository.
    """

    name: str
    nodes: List
    log: logging.Logger

    def __init__(self, *args, **kwargs):
        self.nemesis_termination_event = threading.Event()
        self.nemesis = []
        self.nemesis_threads = []
        self.nemesis_count = 0
        self.test_config = TestConfig()
        self._node_cycle = None
        self.vector_store_cluster = None
        self.params = kwargs.get("params", {})
        self.parallel_node_operations = False
        super().__init__(*args, **kwargs)

    @cached_property
    def parallel_startup(self):
        return False

    def get_scylla_args(self):
        return ""

    def update_seed_provider(self):
        pass

    def validate_seeds_on_all_nodes(self):
        pass

    def set_seeds(self, wait_for_timeout=300, first_only=False):
        """Mark nodes as seeds. For Cassandra, first node is always the seed."""
        if first_only:
            seed_nodes_addresses = [self.nodes[0].ip_address]
        else:
            seeds_num = self.params.get("seeds_num") or 1
            seed_nodes_addresses = [node.ip_address for node in self.nodes[:seeds_num]]

        for node in self.nodes:
            if node.ip_address in seed_nodes_addresses:
                node.set_seed_flag(True)

    @property
    def seed_nodes_addresses(self):
        seed_nodes_addresses = [node.ip_address for node in self.nodes if node.is_seed]
        assert seed_nodes_addresses, "We should have at least one selected seed by now"
        return seed_nodes_addresses

    @property
    def seed_nodes(self):
        seed_nodes = [node for node in self.nodes if node.is_seed]
        assert seed_nodes, "We should have at least one selected seed by now"
        return seed_nodes

    @optional_stage("nemesis")
    def add_nemesis(self, nemesis, tester_obj, hdr_tags: list[str] = None):
        for nem in nemesis:
            nemesis_obj = nem["nemesis"](
                tester_obj=tester_obj,
                termination_event=self.nemesis_termination_event,
                nemesis_selector=nem["nemesis_selector"],
                nemesis_seed=nem["nemesis_seed"],
            )
            if hdr_tags:
                nemesis_obj.hdr_tags = hdr_tags
            self.nemesis.append(nemesis_obj)
        self.nemesis_count = len(nemesis)

    def clean_nemesis(self):
        self.nemesis = []

    @optional_stage("nemesis")
    @log_run_info("Start nemesis threads on cluster")
    def start_nemesis(self, interval=None, cycles_count: int = -1):
        self.log.info("Clear _nemesis_termination_event")
        self.nemesis_termination_event.clear()
        for nemesis in self.nemesis:
            nemesis_thread = threading.Thread(
                target=nemesis.run, name="NemesisThread", args=(interval, cycles_count), daemon=True
            )
            nemesis_thread.start()
            self.nemesis_threads.append(nemesis_thread)

    @optional_stage("nemesis")
    @log_run_info("Stop nemesis threads on cluster")
    def stop_nemesis(self, timeout=10):
        if self.nemesis_termination_event.is_set():
            return
        self.log.info("Set _nemesis_termination_event")
        self.nemesis_termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            raise_exception_in_thread(nemesis_thread, KillNemesis)
            nemesis_thread.join(timeout)

    def start_kms_key_rotation_thread(self) -> None:
        pass

    def start_azure_kms_key_rotation_thread(self) -> None:
        pass

    def start_gcp_key_rotation_thread(self) -> None:
        pass

    def wait_total_space_used_per_node(self, size=None, keyspace="keyspace1"):
        pass

    @property
    def cassandra_version(self) -> str:
        return self.params.get("cassandra_version") or self.params.get("cassandra_oracle_version") or "4.1"

    def jdk_version(self) -> int:
        """Return required JDK major version for the configured Cassandra version.

        Cassandra 4.x needs JDK 11; Cassandra 5.x needs JDK 17.
        """
        major = int(self.cassandra_version.split(".")[0])
        return 17 if major >= 5 else 11

    def _setup_data_device(self, node):
        """Format and mount NVMe instance storage if available.

        Only formats and mounts — does NOT create symlinks yet.
        Symlinks are created after Cassandra is installed (so the
        cassandra user exists and apt's auto-start works normally).
        """
        result = node.remoter.run(
            "lsblk -dpno NAME,TYPE | awk '$2==\"disk\" && $1 ~ /nvme[1-9]/ {print $1; exit}'",
            ignore_status=True,
            verbose=False,
        )
        nvme_device = result.stdout.strip()
        if not nvme_device:
            self.log.info("No NVMe instance store found — Cassandra will use root EBS")
            return

        self.log.info("Setting up NVMe device %s", nvme_device)
        node.remoter.sudo("apt-get install -y xfsprogs", ignore_status=True, retry=3)
        # Ubuntu 22.04 cloud-init may auto-activate the ephemeral NVMe as
        # swap or auto-mount it, which makes mkfs.xfs fail with "Device or
        # resource busy". Release the device before formatting.
        node.remoter.sudo(f"swapoff {nvme_device}", ignore_status=True)
        node.remoter.sudo(f"umount {nvme_device}", ignore_status=True)
        node.remoter.sudo(f"mkfs.xfs -f {nvme_device}")
        node.remoter.sudo("mkdir -p /data")
        node.remoter.sudo(f"mount {nvme_device} /data")
        node.remoter.sudo(f"bash -c 'echo \"{nvme_device} /data xfs defaults,nofail 0 2\" >> /etc/fstab'")
        df_result = node.remoter.run("df -h /data", ignore_status=True, verbose=False)
        self.log.info("NVMe mounted: %s", df_result.stdout.strip())

    def _move_cassandra_to_nvme(self, node):
        """Move Cassandra data and logs to NVMe after install.

        Called after _install_cassandra (which stops Cassandra and wipes data).
        Moves directories to /data on NVMe and creates symlinks.
        Only runs if /data is mounted (i.e., NVMe was set up).
        """
        check = node.remoter.run("mountpoint -q /data", ignore_status=True, verbose=False)
        if not check.ok:
            return

        self.log.info("Moving Cassandra data and logs to NVMe (/data)")
        node.remoter.sudo("mkdir -p /data/cassandra /data/cassandra-logs")
        # Move existing data if any (after install+wipe, dirs are mostly empty)
        node.remoter.sudo("rsync -a /var/lib/cassandra/ /data/cassandra/ 2>/dev/null", ignore_status=True)
        node.remoter.sudo("rsync -a /var/log/cassandra/ /data/cassandra-logs/ 2>/dev/null", ignore_status=True)
        # Replace with symlinks
        node.remoter.sudo("rm -rf /var/lib/cassandra")
        node.remoter.sudo("ln -sf /data/cassandra /var/lib/cassandra")
        node.remoter.sudo("rm -rf /var/log/cassandra")
        node.remoter.sudo("ln -sf /data/cassandra-logs /var/log/cassandra")
        # Fix ownership (cassandra user exists after apt install)
        node.remoter.sudo("chown -R cassandra:cassandra /data/cassandra /data/cassandra-logs")

    def node_setup(self, node, verbose=False, timeout=3600):
        """Full Cassandra node setup for cloud VM backends.

        Installs JDK, adds the Apache Cassandra apt repository,
        installs Cassandra, configures cassandra.yaml and heap sizes.
        Cassandra itself is started in node_startup() so that
        wait_for_init_wrap serializes starts across nodes — non-seed
        Cassandra nodes cannot safely bootstrap in parallel against a
        seed that is still coming up.

        Docker backend overrides this to no-op since the image handles it.
        """
        node.wait_ssh_up(verbose=verbose)
        self._setup_data_device(node)
        NodeExporterSetup.install(node=node)
        self._install_jdk(node)
        self._add_cassandra_apt_repo(node)
        self._install_cassandra(node)
        self._move_cassandra_to_nvme(node)
        self._configure_cassandra_yaml(node)
        self._configure_cassandra_env(node)
        self._configure_rackdc(node)

    def check_node_db_up(self, node):
        """Check if Cassandra is up via nodetool status (runs on the node via SSH).

        BaseNode.db_up() connects from the SCT runner to the node's public IP,
        which may be blocked by AWS security groups. This method checks from
        the node itself via a remote command.
        """
        result = node.remoter.run(
            "nodetool status",
            ignore_status=True,
            verbose=False,
        )
        if not result.ok:
            self.log.debug("nodetool status failed (exit %s): %s", result.exited, result.stderr[:200])
            # Check if the CassandraDaemon JVM is actually running (systemctl
            # may report "active (exited)" even when the JVM is dead because
            # the init.d script uses a forking service type).
            svc_check = node.remoter.run(
                "systemctl is-active cassandra",
                ignore_status=True,
                verbose=False,
            )
            pid_check = node.remoter.run(
                "pgrep -f CassandraDaemon | head -1",
                ignore_status=True,
                verbose=False,
            )
            svc_state = svc_check.stdout.strip()
            jvm_pid = pid_check.stdout.strip()
            self.log.warning(
                "Cassandra service=%s, JVM pid=%s",
                svc_state,
                jvm_pid or "(not running)",
            )
            # Always dump system.log tail — even when active, shows if JMX/gossip is stuck
            log_tail = node.remoter.run(
                "sudo tail -30 /var/log/cassandra/system.log 2>/dev/null || echo 'no log file'",
                ignore_status=True,
                verbose=False,
            )
            self.log.warning("Cassandra system.log tail:\n%s", log_tail.stdout[-2000:])
            if not jvm_pid:
                df_result = node.remoter.run(
                    "df -h / /var/lib/cassandra 2>/dev/null", ignore_status=True, verbose=False
                )
                self.log.error("Cassandra JVM is NOT running. Disk space:\n%s", df_result.stdout)
                journal = node.remoter.run(
                    "sudo journalctl -u cassandra.service --no-pager -n 20",
                    ignore_status=True,
                    verbose=False,
                )
                self.log.error("Cassandra journal:\n%s", journal.stdout[-2000:])
            return False
        is_up = "UN " in result.stdout
        if not is_up:
            self.log.debug("nodetool status output (no UN found): %s", result.stdout[:200])
            return False
        # Verify CQL port is accepting connections. We avoid cqlsh here because
        # the bundled Cassandra driver (cassandra-driver-internal-only-3.25.0.zip)
        # crashes on Python 3.12 (Ubuntu 24.04), making cqlsh unusable as a check.
        cql_check = node.remoter.run(
            f"python3 -c \"import socket; socket.create_connection(('{node.ip_address}', 9042), 5).close()\"",
            ignore_status=True,
            verbose=False,
        )
        if not cql_check.ok:
            self.log.debug("CQL port 9042 not ready on %s: %s", node.name, cql_check.stderr[:200])
            return False
        self.log.info("Node %s is UN and CQL is ready", node.name)
        return True

    def node_startup(self, node, verbose=False, timeout=3600):
        """Start Cassandra, wait for UN, then install the exporter.

        wait_for_init_wrap invokes node_startup serially per node when
        parallel_startup is False (the default for Cassandra). We wait
        for UN here so the seed is fully in the ring before non-seeds
        begin bootstrap — Cassandra cannot resolve token ownership when
        two nodes try to join an unstable ring simultaneously.

        The cassandra_exporter systemd unit uses Requires=cassandra.service,
        so its install (which ends with systemctl start) must run after
        Cassandra is already started; otherwise the exporter start would
        itself trigger a parallel cassandra.service start.
        """
        self._start_cassandra_service(node)
        wait.wait_for(
            func=self.check_node_db_up,
            step=10,
            text=f"{node.name}: node_startup waiting for UN",
            timeout=timeout or 1200,
            throw_exc=True,
            node=node,
        )
        if self.params.get("install_cassandra_exporter"):
            CassandraExporterSetup.install(node=node)

    def _install_jdk(self, node):
        """Install the required JDK version and ensure it is the active alternative.

        Scylla AMIs may ship with a different Java version or have multiple
        JDKs installed.  We explicitly set update-alternatives so the
        ``java`` command in PATH (and systemd service contexts) points to
        the version Cassandra requires.
        """
        jdk = self.jdk_version()
        java_home = f"/usr/lib/jvm/java-{jdk}-openjdk-amd64"
        node.remoter.sudo("apt-get update", ignore_status=True, retry=3)
        node.remoter.sudo(
            f"apt-get install -y openjdk-{jdk}-jdk-headless",
            retry=3,
            timeout=300,
        )
        # Force the correct Java version as the system default
        node.remoter.sudo(
            f"update-alternatives --set java {java_home}/bin/java",
            ignore_status=True,
        )
        node.remoter.sudo(
            f"update-alternatives --set javac {java_home}/jre/../bin/javac 2>/dev/null "
            f"|| update-alternatives --set javac {java_home}/bin/javac",
            ignore_status=True,
        )
        version_check = node.remoter.run("java -version 2>&1", ignore_status=True, verbose=False)
        self.log.info("Java version after install: %s", version_check.stdout.strip() or version_check.stderr.strip())

    def _add_cassandra_apt_repo(self, node):
        """Add the official Apache Cassandra apt repository."""
        version = self.cassandra_version
        major = int(version.split(".")[0])
        minor = int(version.split(".")[1])
        dist_name = f"{major}{minor}x"  # e.g., "41x" for 4.1, "50x" for 5.0
        node.remoter.sudo(
            "bash -c 'curl -fsSL https://downloads.apache.org/cassandra/KEYS "
            "| tee /etc/apt/trusted.gpg.d/cassandra.asc > /dev/null'",
            retry=3,
        )
        node.remoter.sudo(
            f'bash -c \'echo "deb https://debian.cassandra.apache.org {dist_name} main" '
            f"| tee /etc/apt/sources.list.d/cassandra.sources.list'",
        )
        node.remoter.sudo("apt-get update", retry=3)

    def _install_cassandra(self, node):
        """Install Cassandra package and stop the auto-started service."""
        node.remoter.sudo("apt-get install -y cassandra", retry=5, timeout=600)
        # Cassandra auto-starts after install with default config. Stop it and
        # wipe data so we can reconfigure without cluster_name mismatch errors.
        # The init.d stop may leave the JVM running (systemd reports
        # "Unit process remains running after unit stopped"), so we must
        # force-kill and clean the PID file to avoid the next start being
        # a no-op.
        node.remoter.sudo("systemctl stop cassandra", ignore_status=True)
        node.remoter.sudo("pkill -9 -f CassandraDaemon", ignore_status=True)
        node.remoter.sudo("rm -f /var/run/cassandra/cassandra.pid", ignore_status=True)
        node.remoter.sudo("rm -rf /var/lib/cassandra/*", ignore_status=True)
        # Recreate required directories with correct ownership after wipe
        node.remoter.sudo(
            "mkdir -p /var/lib/cassandra/data /var/lib/cassandra/commitlog "
            "/var/lib/cassandra/saved_caches /var/lib/cassandra/hints "
            "/var/run/cassandra"
        )
        node.remoter.sudo("chown -R cassandra:cassandra /var/lib/cassandra /var/run/cassandra")

    def _configure_cassandra_yaml(self, node):
        """Render and upload cassandra.yaml for the given node."""
        seeds = ",".join(n.ip_address for n in self.nodes if n.is_seed) or node.ip_address
        num_tokens = self.params.get("cassandra_num_tokens") or 16

        yaml_updates = {
            "cluster_name": self.name,
            "num_tokens": num_tokens,
            "seed_provider": [
                {
                    "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                    "parameters": [{"seeds": seeds}],
                }
            ],
            "listen_address": node.ip_address,
            "rpc_address": "0.0.0.0",
            "broadcast_rpc_address": node.ip_address,
            "endpoint_snitch": "GossipingPropertyFileSnitch",
        }

        # Use sed to update individual keys in the existing cassandra.yaml.
        # Pattern ^#? handles entries that are commented out by default
        # (e.g., broadcast_rpc_address is "# broadcast_rpc_address: 1.2.3.4").
        # Use '|' as sed delimiter to avoid conflicts with '/' in values.
        for key, value in yaml_updates.items():
            if key == "seed_provider":
                node.remoter.sudo(f"sed -i 's|- seeds: .*|- seeds: \"{seeds}\"|' {CASSANDRA_YAML_PATH}")
            elif key == "cluster_name":
                # cluster_name may be quoted — handle both forms
                node.remoter.sudo(f"sed -i \"s|^cluster_name:.*|cluster_name: '{value}'|\" {CASSANDRA_YAML_PATH}")
            else:
                node.remoter.sudo(f"sed -Ei 's|^#? *{key}:.*|{key}: {value}|' {CASSANDRA_YAML_PATH}")

    def _configure_cassandra_env(self, node):
        """Set JAVA_HOME and JVM heap sizes in cassandra-env.sh."""
        jdk = self.jdk_version()
        java_home = f"/usr/lib/jvm/java-{jdk}-openjdk-amd64"
        # Inject JAVA_HOME so Cassandra uses the correct JDK even when
        # the system default (update-alternatives) points elsewhere.
        # Cassandra 4.1's cassandra-env.sh may not have a JAVA_HOME line,
        # so check first and either replace it or prepend a new line. Doing
        # this as two separate commands avoids shell-escaping headaches and
        # ensures sudo applies to the write (sed needs to create a temp file
        # in /etc/cassandra/).
        has_java_home = node.remoter.run(
            f"grep -q 'JAVA_HOME=' {CASSANDRA_ENV_SH_PATH}",
            ignore_status=True,
            verbose=False,
        )
        if has_java_home.ok:
            node.remoter.sudo(f"sed -Ei 's|^#? *JAVA_HOME=.*|JAVA_HOME=\"{java_home}\"|' {CASSANDRA_ENV_SH_PATH}")
        else:
            node.remoter.sudo(f"sed -i '1iJAVA_HOME=\"{java_home}\"' {CASSANDRA_ENV_SH_PATH}")

        result = node.remoter.run("grep MemTotal /proc/meminfo")
        total_kb = int(result.stdout.split()[1])
        total_mb = total_kb // 1024
        max_heap, heap_new = compute_jvm_heap_mb(total_mb)

        node.remoter.sudo(f"sed -Ei 's/^#?MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE=\"{max_heap}M\"/' {CASSANDRA_ENV_SH_PATH}")
        node.remoter.sudo(f"sed -Ei 's/^#?HEAP_NEWSIZE=.*/HEAP_NEWSIZE=\"{heap_new}M\"/' {CASSANDRA_ENV_SH_PATH}")

    def _configure_rackdc(self, node):
        """Write cassandra-rackdc.properties for GossipingPropertyFileSnitch."""
        dc = getattr(node, "dc_idx", 0)
        dc_name = f"datacenter{dc + 1}" if isinstance(dc, int) else str(dc)
        rack = getattr(node, "rack", 0)
        rack_name = f"rack{rack + 1}" if isinstance(rack, int) else str(rack)
        content = f"dc={dc_name}\\nrack={rack_name}"
        node.remoter.sudo(f"bash -c \"printf '{content}\\n' > {CASSANDRA_RACKDC_PATH}\"")

    def _start_cassandra_service(self, node):
        """Enable and start the Cassandra systemd service.

        The Cassandra Debian package uses a forking service type, so
        ``systemctl start`` returns 0 as soon as the init script exits —
        before the JVM is actually running.  We sleep briefly and then
        verify the CassandraDaemon process exists to catch silent JVM
        crashes early instead of waiting for the 1200-second nodetool
        timeout.
        """
        node.remoter.sudo("systemctl enable cassandra", ignore_status=True)
        node.remoter.sudo("systemctl start cassandra")

        time.sleep(10)

        proc_check = node.remoter.run(
            "pgrep -f CassandraDaemon",
            ignore_status=True,
            verbose=False,
        )
        if proc_check.ok:
            self.log.info("Cassandra JVM started (pid %s)", proc_check.stdout.strip().split()[0])
            return

        self.log.error("Cassandra JVM not running 10s after systemctl start — collecting diagnostics")
        for diag_cmd in [
            "systemctl status cassandra --no-pager -l",
            "sudo journalctl -u cassandra.service --no-pager -n 50",
            "sudo tail -50 /var/log/cassandra/system.log 2>/dev/null || echo 'no system.log'",
            "java -version 2>&1",
            f"grep JAVA_HOME {CASSANDRA_ENV_SH_PATH}",
            f"head -5 {CASSANDRA_ENV_SH_PATH}",
        ]:
            result = node.remoter.run(diag_cmd, ignore_status=True, verbose=False)
            self.log.error(
                "DIAG [%s]:\n%s%s",
                diag_cmd,
                result.stdout[-2000:],
                f"\nSTDERR: {result.stderr[-500:]}" if result.stderr else "",
            )

    def get_node_ips_param(self, public_ip=True):
        if self.test_config.MIXED_CLUSTER:
            return "oracle_db_nodes_public_ip" if public_ip else "oracle_db_nodes_private_ip"
        return "db_nodes_public_ip" if public_ip else "db_nodes_private_ip"

    @property
    def data_nodes(self):
        return list(self.nodes)

    @property
    def zero_nodes(self):
        return []

    def get_rack_nodes(self, rack: int) -> list:
        return sorted([node for node in self.nodes if node.rack == rack], key=lambda n: n.name)
