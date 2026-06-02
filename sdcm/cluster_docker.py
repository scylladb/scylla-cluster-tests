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
# Copyright (c) 2020 ScyllaDB

import os
import re
import logging
import shutil
import time
from typing import Optional, Union, Dict
from functools import cached_property

from sdcm import cluster
from sdcm.provision.helpers.certificate import (
    CA_CERT_FILE,
    JKS_TRUSTSTORE_FILE,
    TLSAssets,
    export_pem_cert_to_pkcs12_keystore,
    install_client_certificate,
)
from sdcm.remote import LOCALRUNNER
from sdcm.remote.docker_cmd_runner import DockerCmdRunner
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.utils.docker_utils import (
    get_docker_bridge_gateway,
    Container,
    ContainerManager,
    DockerException,
    _default_docker_client,
)
from sdcm.utils.health_checker import check_nodes_status
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.utils.net import get_my_public_ip
from sdcm.cluster_cassandra import BaseCassandraCluster, CassandraNodeMixin
from sdcm.utils.vector_store_utils import VectorStoreClusterMixin, VectorStoreNodeMixin

DEFAULT_SCYLLA_DB_IMAGE = "scylladb/scylla-nightly"
DEFAULT_SCYLLA_DB_IMAGE_TAG = "latest"
AIO_MAX_NR_RECOMMENDED_VALUE = 1048576

LOGGER = logging.getLogger(__name__)


class ScyllaDockerRequirementError(cluster.ScyllaRequirementError, DockerException):
    pass


class NodeContainerMixin:
    @cached_property
    def node_container_image_tag(self):
        return self.parent_cluster.source_image

    def node_container_image_dockerfile_args(self):
        return dict(path=self.parent_cluster.node_container_context_path)

    def node_container_image_build_args(self):
        return dict(
            buildargs={
                "SOURCE_IMAGE": self.parent_cluster.source_image,
            },
            labels=self.parent_cluster.tags,
        )

    def node_container_run_args(self, seed_ip):
        volumes = {
            "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
        }

        smp = 1
        if self.node_type == "db":
            scylla_args = self.parent_cluster.params.get("append_scylla_args")
            smp_match = re.search(r"--smp\s(\d+)", scylla_args)
            smp = int(smp_match.group(1)) if smp_match else 1

        return dict(
            name=self.name,
            image=self.node_container_image_tag,
            command=f'--seeds="{seed_ip}"' if seed_ip else None,
            volumes=volumes,
            network=self.parent_cluster.params.get("docker_network"),
            nano_cpus=smp * 10**9,
        )  # Same as `docker run --cpus=N ...' CLI command.


class DockerNode(cluster.BaseNode, NodeContainerMixin):
    def __init__(
        self,
        parent_cluster: "DockerCluster",
        container: Optional[Container] = None,
        node_prefix: str = "node",
        base_logdir: Optional[str] = None,
        ssh_login_info: Optional[dict] = None,
        node_index: int = 1,
        after_config=None,
    ) -> None:
        super().__init__(
            name=f"{node_prefix}-{node_index}",
            parent_cluster=parent_cluster,
            ssh_login_info=ssh_login_info,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            after_config=after_config,
        )
        self.node_index = node_index

        if container is not None:
            assert int(container.labels["NodeIndex"]) == node_index, "Container labeled with wrong index."
            self._containers["node"] = container

    def _init_remoter(self, ssh_login_info):
        self.remoter = DockerCmdRunner(self)

    def _init_port_mapping(self):
        pass

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

    def wait_for_cloud_init(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
        pass

    @property
    def network_interfaces(self):
        pass
        # TODO: it did not work, we can it check later
        # return [NetworkInterface(ipv4_public_address=self._get_public_ip_address(),
        #                          ipv6_public_addresses='',
        #                          ipv4_private_addresses=[self._get_private_ip_address()],
        #                          ipv6_private_address='',
        #                          dns_private_name='',
        #                          dns_public_name='',
        #                          device_index=0
        #                          )]

    @staticmethod
    def is_docker():
        return True

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    def _get_public_ip_address(self) -> Optional[str]:
        return ContainerManager.get_ip_address(self, "node")

    def _get_private_ip_address(self) -> Optional[str]:
        return self.public_ip_address

    def _refresh_instance_state(self):
        public_ipv4_addresses = [self._get_public_ip_address()]
        private_ipv4_addresses = [self._get_private_ip_address()]
        return public_ipv4_addresses, private_ipv4_addresses

    def _get_ipv6_ip_address(self):
        self.log.warning("We don't support IPv6 for Docker backend")
        return ""

    @cached_property
    def host_public_ip_address(self) -> str:
        return get_my_public_ip()

    def is_running(self):
        return ContainerManager.is_running(self, "node")

    def restart(self):
        ContainerManager.get_container(self, "node").restart()

    def get_service_status(self, service_name: str, timeout: int = 500, ignore_status=False):
        container_status = ContainerManager.get_container(self, "node").attrs["State"]["Status"]
        # return similar interface to what supervisorctl would return
        return type(
            "ServiceStatus",
            (),
            {
                "stdout": f"{service_name} {container_status}",
                "stderr": "",
                "ok": container_status in ["running", "active", "RUNNING"],
            },
        )

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=None):
        verify_up_timeout = verify_up_timeout or self.verify_up_timeout
        if verify_down:
            self.wait_db_down(timeout=timeout)

        container = ContainerManager.get_container(self, "node")
        if container.status != "running":
            self.log.info("Starting Docker container %s", container.name)
            container.start()
            ContainerManager.wait_for_status(self, "node", status="running")

        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

        self._restart_manager_agent_if_needed()

    @cluster.log_run_info
    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300, ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)

        container = ContainerManager.get_container(self, "node")
        self.log.info(f"Stopping Docker container {container.name}")
        # ignoring WARN messages upon stopping - https://github.com/scylladb/scylla-cluster-tests/issues/10633
        with DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line="WARN "):
            container.stop(timeout=timeout)

        if verify_down:
            self.wait_db_down(timeout=timeout)

    @cluster.log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    def restart_scylla_server(self, verify_up_before=False, verify_up_after=True, timeout=1800, verify_up_timeout=None):
        verify_up_timeout = verify_up_timeout or self.verify_up_timeout

        if verify_up_before:
            self.wait_db_up(timeout=verify_up_timeout)

        container = ContainerManager.get_container(self, "node")
        self.log.info(f"Restarting Docker container {container.name}")
        # ignoring WARN messages upon stopping - https://github.com/scylladb/scylla-cluster-tests/issues/10633
        with DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line="WARN "):
            container.restart(timeout=timeout)
            ContainerManager.wait_for_status(self, "node", status="running")

        if verify_up_after:
            self.wait_db_up(timeout=verify_up_timeout)

    @cluster.log_run_info
    def restart_scylla(self, verify_up_before=False, verify_up_after=True, timeout=1800) -> None:
        self.restart_scylla_server(verify_up_before=verify_up_before, verify_up_after=verify_up_after, timeout=timeout)

    def _restart_manager_agent_if_needed(self):
        """Restart the manager agent process if it was previously installed (e.g. after container restart by nemesis)."""
        exit_code, _ = ContainerManager.get_container(self, "node").exec_run("test -x /usr/bin/scylla-manager-agent")
        if exit_code != 0:
            return
        # Agent binary exists but process may be dead after container restart — re-launch it
        self.remoter.run(
            "sudo bash -c 'nohup /usr/bin/scylla-manager-agent > /var/log/scylla-manager-agent.log 2>&1 &'",
        )
        self.log.info("Scylla Manager agent restarted after container start")

    @property
    def image(self) -> str:
        return self.parent_cluster.source_image

    @property
    def vm_region(self):
        return "docker"

    @property
    def region(self):
        return "docker"

    def do_default_installations(self):
        self.install_sudo()
        self.install_package("tar")
        procps_package = "procps" if self.distro.is_debian_like else "procps-ng"
        self.install_package(procps_package)
        super().do_default_installations()

    def install_sudo(self, user: str = "scylla", verbose=False):
        """Install and configure passwordless sudo"""
        if self.distro.is_rhel_like:
            pkg_mgr = "microdnf"
        else:
            pkg_mgr = "apt"
            self.remoter.run("apt update", verbose=verbose, ignore_status=True, user="root")
        self.remoter.run(f"{pkg_mgr} install -y sudo", verbose=verbose, ignore_status=True, user="root")

        self.remoter.run("mkdir -p /etc/sudoers.d", user="root", ignore_status=True, verbose=verbose)
        sudoers_file = f"/etc/sudoers.d/{user}-nopasswd"
        sudoers_content = f"{user} ALL=(ALL) NOPASSWD: ALL"
        self.remoter.run(f"echo '{sudoers_content}' > {sudoers_file}", user="root", verbose=verbose, ignore_status=True)
        self.remoter.run(f"chmod 440 {sudoers_file}", user="root", verbose=verbose, ignore_status=True)

        verify_result = self.remoter.run("sudo -n true", ignore_status=True, verbose=verbose)
        if verify_result.ok:
            self.log.debug("Passwordless sudo configured successfully for user %s", user)
        else:
            self.log.warning("Passwordless sudo verification failed: %s", verify_result.stderr)

    def reload_config(self):
        """
        Docker-specific implementation of config reload.
        Attempts to send SIGHUP to Scylla process, falls back to container restart if signal fails.
        """
        try:
            result = self.remoter.run("ps -C scylla -o pid --no-headers", ignore_status=True, user="root")
            if result.ok and (pid := result.stdout.strip()):
                self.remoter.run(f"kill -s HUP {pid}", ignore_status=True, user="root")
        except Exception:  # noqa: BLE001
            self.restart_scylla_server(verify_up_before=True, verify_up_after=True)
        self.log.info("Scylla configuration have been reloaded")


class VectorStoreDockerNode(VectorStoreNodeMixin, DockerNode):
    """Docker node running Vector Store service"""

    def __init__(
        self,
        parent_cluster: "VectorStoreSetDocker",
        container: Optional[Container] = None,
        node_prefix: str = "vector",
        base_logdir: Optional[str] = None,
        ssh_login_info: Optional[dict] = None,
        node_index: int = 1,
        after_config=None,
    ) -> None:
        super().__init__(
            parent_cluster=parent_cluster,
            container=container,
            node_prefix=node_prefix,
            base_logdir=base_logdir,
            ssh_login_info=ssh_login_info,
            node_index=node_index,
            after_config=after_config,
        )

    def node_container_run_args(self, seed_ip=None):
        return self.vector_container_run_args(seed_ip)

    def vector_container_run_args(self, seed_ip=None):
        environment = {
            "VECTOR_STORE_URI": f"0.0.0.0:{self.parent_cluster.params.get('vector_store_port')}",
            "VECTOR_STORE_SCYLLADB_URI": self.scylla_uri,
        }

        if (threads := self.parent_cluster.params.get("vector_store_threads")) > 0:
            environment["VECTOR_STORE_THREADS"] = str(threads)
        ports = {f"{self.parent_cluster.params.get('vector_store_port')}/tcp": None}

        return dict(
            name=self.name,
            image=self.node_container_image_tag,
            environment=environment,
            ports=ports,
            network=self.parent_cluster.params.get("docker_network"),
        )


class DockerCluster(cluster.BaseCluster):
    node_container_user = "scylla-test"

    def __init__(
        self,
        docker_image: str = DEFAULT_SCYLLA_DB_IMAGE,
        docker_image_tag: str = DEFAULT_SCYLLA_DB_IMAGE_TAG,
        node_key_file: Optional[str] = None,
        cluster_prefix: str = "cluster",
        node_prefix: str = "node",
        node_type: Optional[str] = None,
        n_nodes: Union[list, int] = 3,
        params: dict = None,
    ) -> None:
        self.source_image = f"{docker_image}:{docker_image_tag}"
        self.node_container_key_file = node_key_file

        super().__init__(
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=[
                "localhost-dc",
            ],  # Multi DC is not supported currently.
            node_type=node_type,
        )

    @property
    def node_container_context_path(self):
        # scylla_linux_distro can be: centos or ubuntu-focal, hence we need to split it
        return os.path.join(
            os.path.dirname(__file__), "../docker/scylla-sct", self.params.get("scylla_linux_distro").split("-")[0]
        )

    def _create_node(self, node_index, container=None, after_config=None):
        node = DockerNode(
            parent_cluster=self,
            container=container,
            ssh_login_info=dict(hostname=None, user=self.node_container_user, key_file=self.node_container_key_file),
            base_logdir=self.logdir,
            node_prefix=self.node_prefix,
            node_index=node_index,
            after_config=after_config,
        )

        if container is None:
            ContainerManager.run_container(
                node, "node", seed_ip=self.nodes[0].public_ip_address if node_index else None
            )
            ContainerManager.wait_for_status(node, "node", status="running")

        node.init()

        return node

    def _get_new_node_indexes(self, count):
        """Return `count' indexes which will fill gaps or/and continue existent indexes in `self.nodes' list."""

        return sorted(set(range(len(self.nodes) + count)) - set(node.node_index for node in self.nodes))

    def _create_nodes(self, count, enable_auto_bootstrap=False):
        new_nodes = []
        for node_index in self._get_new_node_indexes(count):
            node = self._create_node(node_index)
            node.enable_auto_bootstrap = enable_auto_bootstrap
            self.nodes.append(node)
            new_nodes.append(node)
        return new_nodes

    def _get_nodes(self):
        containers = ContainerManager.get_containers_by_prefix(self.node_prefix)
        for node_index, container in sorted(((int(c.labels["NodeIndex"]), c) for c in containers), key=lambda x: x[0]):
            LOGGER.debug("Found container %s with name `%s' and index=%d", container, container.name, node_index)
            node = self._create_node(node_index, container)
            self.nodes.append(node)
        return self.nodes

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
        assert instance_type is None, "docker can't provision different instance types"
        return self._get_nodes() if self.test_config.REUSE_CLUSTER else self._create_nodes(count, enable_auto_bootstrap)


class ScyllaDockerCluster(cluster.BaseScyllaCluster, DockerCluster):
    def __init__(
        self,
        docker_image: str = DEFAULT_SCYLLA_DB_IMAGE,
        docker_image_tag: str = DEFAULT_SCYLLA_DB_IMAGE_TAG,
        node_key_file: Optional[str] = None,
        user_prefix: Optional[str] = None,
        n_nodes: Union[list, str] = 3,
        params: dict = None,
    ) -> None:
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "db-cluster")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "db-node")
        super().__init__(
            docker_image=docker_image,
            docker_image_tag=docker_image_tag,
            node_key_file=node_key_file,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            node_type="scylla-db",
            n_nodes=n_nodes,
            params=params,
        )

        self.vector_store_cluster = None

    def node_setup(self, node, verbose=False, timeout=3600):
        node.is_scylla_installed(raise_if_not_installed=True)
        self.check_aio_max_nr(node)
        if self.test_config.BACKTRACE_DECODING:
            node.install_scylla_debuginfo()

        if any([self.params.get("server_encrypt"), self.params.get("client_encrypt")]):
            self._generate_db_node_certs(node)
            install_client_certificate(node.remoter, node.ip_address, force=True)

        node.config_setup(append_scylla_args=self.get_scylla_args())
        node.restart_scylla(verify_up_before=True)

        if self.params.get("use_mgmt"):
            self.install_scylla_manager(node)

    def install_scylla_manager(self, node):
        """Install manager agent into running Scylla Docker container.

        Copies the agent binary from the scylladb/scylla-manager-agent image,
        writes config with auth_token, and starts the agent process.
        """
        mgmt_agent_image = "scylladb/scylla-manager-agent:latest"
        auth_token = self.test_config.test_id()
        manager_prometheus_port = self.params.get("manager_prometheus_port") or "56090"

        LOGGER.info("Installing manager agent on Docker node %s", node.name)

        node.remoter.run("sudo mkdir -p /etc/scylla-manager-agent", ignore_status=True)
        node.remoter.run(
            f"sudo bash -c 'cat > /etc/scylla-manager-agent/scylla-manager-agent.yaml << EOF\n"
            f"auth_token: {auth_token}\n"
            f'prometheus: ":{manager_prometheus_port}"\n'
            f"EOF'",
        )

        container = ContainerManager.get_container(node, "node")

        docker_client = _default_docker_client()

        tmp_container_name = f"mgmt-agent-copy-{node.node_index}"
        try:
            docker_client.containers.get(tmp_container_name).remove(force=True)
        except Exception:  # noqa: BLE001
            pass

        docker_client.images.pull(*mgmt_agent_image.split(":", maxsplit=1))
        tmp_container = docker_client.containers.create(
            mgmt_agent_image,
            name=tmp_container_name,
            command="sleep 1",
        )
        try:
            bits, _ = tmp_container.get_archive("/usr/bin/scylla-manager-agent")
            container.put_archive("/usr/bin/", bits)
        finally:
            tmp_container.remove(force=True)

        node.remoter.run("sudo chmod +x /usr/bin/scylla-manager-agent")

        node.remoter.run(
            "sudo bash -c 'nohup /usr/bin/scylla-manager-agent > /var/log/scylla-manager-agent.log 2>&1 &'",
        )

        node.wait_manager_agent_up(timeout=60)

    def _generate_db_node_certs(self, node):
        """Generate per-node SSL certificates for a Docker DB node."""
        node.create_node_certificate(
            cert_file=node.ssl_conf_dir / TLSAssets.DB_CERT,
            cert_key=node.ssl_conf_dir / TLSAssets.DB_KEY,
            csr_file=node.ssl_conf_dir / TLSAssets.DB_CSR,
        )
        node.create_node_certificate(
            node.ssl_conf_dir / TLSAssets.DB_CLIENT_FACING_CERT,
            node.ssl_conf_dir / TLSAssets.DB_CLIENT_FACING_KEY,
        )
        for src in (CA_CERT_FILE, JKS_TRUSTSTORE_FILE):
            shutil.copy(src, node.ssl_conf_dir)

    def node_startup(self, node, verbose=False, timeout=3600):
        if not ContainerManager.is_running(node, "node"):
            container = ContainerManager.get_container(node, "node")
            container.start()
        node.wait_db_up(verbose=verbose, timeout=timeout)
        for event in check_nodes_status(
            nodes_status=node.get_nodes_status(), current_node=node, removed_nodes_list=self.dead_nodes_ip_address_list
        ):
            event.publish()
        self.clean_replacement_node_options(node)

    @staticmethod
    def check_aio_max_nr(node: DockerNode, recommended_value: int = AIO_MAX_NR_RECOMMENDED_VALUE):
        """Verify that sysctl key `fs.aio-max-nr' set to recommended value.

        See https://github.com/scylladb/scylla/issues/5638 for details.
        """
        aio_max_nr = int(node.remoter.run("cat /proc/sys/fs/aio-max-nr").stdout)
        if aio_max_nr < recommended_value:
            raise ScyllaDockerRequirementError(
                f"{node}: value of sysctl key `fs.aio-max-nr' ({aio_max_nr}) "
                f"is less than recommended value ({recommended_value})"
            )

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, check_node_health=True):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)

    def get_scylla_args(self):
        append_scylla_args = (
            self.params.get("append_scylla_args_oracle")
            if self.name.find("oracle") > 0
            else self.params.get("append_scylla_args")
        )
        return re.sub(r"--blocked-reactor-notify-ms[ ]+[0-9]+", "", append_scylla_args)

    def destroy(self):
        if self.vector_store_cluster:
            self.log.info("Destroying vector store cluster...")
            try:
                self.vector_store_cluster.destroy()
            except Exception as e:  # noqa: BLE001
                self.log.warning("Failed destroy vector store cluster: %s", e)

        super().destroy()


DEFAULT_CASSANDRA_IMAGE = "cassandra"
DEFAULT_CASSANDRA_IMAGE_TAG = "4.1"


class CassandraDockerNode(CassandraNodeMixin, DockerNode):
    """A Cassandra node running in a Docker container.

    MRO puts CassandraNodeMixin before DockerNode/BaseNode, so the mixin's
    overrides win.  The explicit forwarding below makes that choice visible
    to static-analysis tools and prevents silent regressions if the
    inheritance order ever changes.
    """

    # --- Explicit MRO forwarding to CassandraNodeMixin ---
    remote_scylla_yaml = CassandraNodeMixin.remote_scylla_yaml
    smp = CassandraNodeMixin.smp
    cpuset = CassandraNodeMixin.cpuset
    _gen_nodetool_cmd = CassandraNodeMixin._gen_nodetool_cmd
    extract_seeds_from_scylla_yaml = CassandraNodeMixin.extract_seeds_from_scylla_yaml
    is_server_encrypt = CassandraNodeMixin.is_server_encrypt
    raft = CassandraNodeMixin.raft
    cql_address = CassandraNodeMixin.cql_address
    wait_native_transport = CassandraNodeMixin.wait_native_transport
    db_up = CassandraNodeMixin.db_up
    config_setup = CassandraNodeMixin.config_setup
    get_scylla_binary_version = CassandraNodeMixin.get_scylla_binary_version

    def node_container_run_args(self, seed_ip):
        env_vars = self.parent_cluster.cassandra_env_vars(self, seed_ip)
        return dict(
            name=self.name,
            image=self.node_container_image_tag,
            environment=env_vars,
            network=self.parent_cluster.params.get("docker_network"),
            nano_cpus=10**9,
        )


class CassandraDockerCluster(BaseCassandraCluster, DockerCluster):
    """Cassandra cluster on Docker backend for development and CI."""

    def __init__(
        self,
        docker_image: str = DEFAULT_CASSANDRA_IMAGE,
        docker_image_tag: str = DEFAULT_CASSANDRA_IMAGE_TAG,
        node_key_file: Optional[str] = None,
        user_prefix: Optional[str] = None,
        n_nodes: Union[list, str] = 1,
        params: dict = None,
    ) -> None:
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "cs-cluster")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "cs-node")
        super().__init__(
            docker_image=docker_image,
            docker_image_tag=docker_image_tag,
            node_key_file=node_key_file,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            node_type="cs-db",
            n_nodes=n_nodes,
            params=params,
        )

    def _create_node(self, node_index, container=None, after_config=None):
        node = CassandraDockerNode(
            parent_cluster=self,
            container=container,
            ssh_login_info=dict(hostname=None, user=self.node_container_user, key_file=self.node_container_key_file),
            base_logdir=self.logdir,
            node_prefix=self.node_prefix,
            node_index=node_index,
            after_config=after_config,
        )

        if container is None:
            ContainerManager.run_container(
                node, "node", seed_ip=self.nodes[0].public_ip_address if node_index else None
            )
            ContainerManager.wait_for_status(node, "node", status="running")

        node.init()

        return node

    def cassandra_env_vars(self, node, seed_ip) -> dict:
        """Compute Docker env vars for the Cassandra container.

        Args:
            node: The node being configured.
            seed_ip: IP address of the seed node, or None for the first node.

        Returns:
            Dict of environment variables for the Cassandra Docker container.
        """
        seeds = seed_ip or ""
        num_tokens = self.params.get("cassandra_num_tokens") or 16
        return {
            "CASSANDRA_CLUSTER_NAME": self.name,
            "CASSANDRA_SEEDS": seeds,
            "CASSANDRA_ENDPOINT_SNITCH": "SimpleSnitch",
            "CASSANDRA_NUM_TOKENS": str(num_tokens),
            "MAX_HEAP_SIZE": "512M",
            "HEAP_NEWSIZE": "64M",
        }

    def node_setup(self, node, verbose=False, timeout=3600):
        # Docker: image entrypoint handles Cassandra installation and config
        pass

    def node_startup(self, node, verbose=False, timeout=3600):
        # Docker: container start is the startup
        pass

    # Explicit MRO forwarding: BaseCassandraCluster provides the Cassandra-specific
    # implementation; this override makes the resolution explicit.
    def get_node_ips_param(self, public_ip=True):
        return BaseCassandraCluster.get_node_ips_param(self, public_ip=public_ip)

    def wait_for_nodes_up_and_normal(self, nodes=None, verification_node=None, iterations=60, sleep_time=3, timeout=0):
        # Docker Cassandra: CQL port check is sufficient
        pass

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, check_node_health=True):
        node_list = node_list if node_list else self.nodes
        for node in node_list:
            node.wait_db_up(timeout=timeout or 300)


class VectorStoreSetDocker(VectorStoreClusterMixin, DockerCluster):
    """Set of Vector Store nodes"""

    def __init__(self, params, vs_docker_image, vs_docker_image_tag, **kwargs):
        self.scylla_cluster = None

        kwargs["cluster_prefix"] = cluster.prepend_user_prefix(kwargs.get("cluster_prefix"), "vs-set")
        kwargs.setdefault("node_prefix", "vs-node")
        kwargs.setdefault("node_type", "vector-store")

        if not vs_docker_image_tag:
            vs_docker_image_tag = "latest"

        super().__init__(docker_image=vs_docker_image, docker_image_tag=vs_docker_image_tag, params=params, **kwargs)

    def _reconfigure_vector_store_nodes(self):
        """Update Vector Store nodes with Scylla info"""
        if not self.nodes:
            return

        self.log.info("Reconfiguring Vector Store with Scylla node information")
        for node in self.nodes:
            try:
                if ContainerManager.is_running(node, "node"):
                    self.log.debug("Stopping container %s for reconfiguration", node.name)
                    ContainerManager.get_container(node, "node").stop()
                ContainerManager.destroy_container(node, "node")
                ContainerManager.run_container(node, "node")
                ContainerManager.wait_for_status(node, "node", status="running")
                self.log.debug("Successfully reconfigured container %s", node.name)
            except Exception as e:  # noqa: BLE001
                self.log.error("Failed to reconfigure container %s: %s", node.name, e)
                raise

    def _create_node(self, node_index, container=None, after_config=None):
        node = VectorStoreDockerNode(
            parent_cluster=self,
            container=container,
            node_prefix=self.node_prefix,
            base_logdir=self.logdir,
            node_index=node_index,
        )

        if container is None:
            ContainerManager.run_container(node, "node")
            ContainerManager.wait_for_status(node, "node", status="running")
        node.init()
        return node

    def add_nodes(self, count, dc_idx=0, after_config=None, **kwargs):
        if count > 1:
            # TODO: implement once HA support is implemented for VS
            self.log.warning("Vector Store HA not implemented yet, creating only 1 node instead of %d", count)
            count = 1

        reuse_cluster = getattr(self.test_config, "REUSE_CLUSTER", "UNDEFINED")
        result = self._get_nodes() if reuse_cluster else self._create_nodes(count)
        return result


class DockerLoaderNode(cluster.BaseNode):
    """A lightweight loader node that runs locally on the host using LOCALRUNNER.

    Instead of spinning up a Scylla container just to serve as a Docker host for
    stress tool containers (docker-in-docker), this node runs directly on the host.
    Stress tool containers (RemoteDocker) are launched directly via the local Docker daemon.
    """

    log = LOGGER

    def __init__(
        self,
        parent_cluster: "LoaderSetDocker",
        node_prefix: str = "loader-node",
        base_logdir: Optional[str] = None,
        node_index: int = 1,
        ssh_login_info: Optional[dict] = None,
        after_config=None,
    ) -> None:
        super().__init__(
            name=f"{node_prefix}-{node_index}",
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            ssh_login_info=ssh_login_info,
            after_config=after_config,
        )
        self.node_index = node_index

    def wait_for_cloud_init(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
        pass

    @staticmethod
    def is_docker() -> bool:
        return False

    @cached_property
    def tags(self) -> dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    def _init_remoter(self, ssh_login_info):
        self.remoter = LOCALRUNNER

    def _init_port_mapping(self):
        pass

    def update_repo_cache(self):
        pass

    def _refresh_instance_state(self):
        return ["127.0.0.1"], ["127.0.0.1"]

    def refresh_ip_address(self):
        pass

    def start_journal_thread(self):
        pass

    def disable_daily_triggered_services(self):
        pass

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

    @property
    def vm_region(self):
        return "docker"

    @property
    def region(self):
        return "docker"


class LoaderSetDocker(cluster.BaseLoaderSet, DockerCluster):
    def __init__(
        self,
        docker_image: str = DEFAULT_SCYLLA_DB_IMAGE,
        docker_image_tag: str = DEFAULT_SCYLLA_DB_IMAGE_TAG,
        node_key_file: Optional[str] = None,
        user_prefix: Optional[str] = None,
        n_nodes: Union[list, str] = 3,
        params: dict = None,
    ) -> None:
        node_prefix = cluster.prepend_user_prefix(user_prefix, "loader-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "loader-set")

        cluster.BaseLoaderSet.__init__(self, params=params)
        DockerCluster.__init__(
            self,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            node_type="loader",
            n_nodes=n_nodes,
            params=params,
        )

    def _create_node(self, node_index, container=None, after_config=None):
        node = DockerLoaderNode(
            parent_cluster=self,
            base_logdir=self.logdir,
            node_prefix=self.node_prefix,
            node_index=node_index,
            ssh_login_info=None,
        )
        node.init()
        return node

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
        assert instance_type is None, "docker can provision different instance types"
        return self._create_nodes(count, enable_auto_bootstrap)

    def node_setup(self, node: DockerLoaderNode, verbose=False, timeout=3600, **kwargs):
        # No container setup needed - stress tools run directly on the host via LOCALRUNNER.
        # Docker is already available on the host machine.
        if self.params.get("client_encrypt"):
            self._generate_loader_certs(node)

    def _generate_loader_certs(self, node):
        """Generate SSL client certificates for a Docker loader node."""
        node.create_node_certificate(
            node.ssl_conf_dir / TLSAssets.CLIENT_CERT, node.ssl_conf_dir / TLSAssets.CLIENT_KEY
        )
        for src in (CA_CERT_FILE, JKS_TRUSTSTORE_FILE):
            shutil.copy(src, node.ssl_conf_dir)
        export_pem_cert_to_pkcs12_keystore(
            node.ssl_conf_dir / TLSAssets.CLIENT_CERT,
            node.ssl_conf_dir / TLSAssets.CLIENT_KEY,
            node.ssl_conf_dir / TLSAssets.PKCS12_KEYSTORE,
        )


class DockerMonitoringNode(cluster.BaseNode):
    log = LOGGER

    def __init__(
        self,
        parent_cluster: "MonitorSetDocker",
        node_prefix: str = "monitor-node",
        base_logdir: Optional[str] = None,
        node_index: int = 1,
        ssh_login_info: Optional[dict] = None,
        after_config=None,
    ) -> None:
        super().__init__(
            name=f"{node_prefix}-{node_index}",
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            ssh_login_info=ssh_login_info,
            after_config=after_config,
        )
        self.node_index = node_index

    def wait_for_cloud_init(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
        # backend does not use SSH, but LOCALRUNNER for commands execution
        pass

    @staticmethod
    def is_docker() -> bool:
        return True

    @cached_property
    def tags(self) -> dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    def _init_remoter(self, ssh_login_info):
        self.remoter = LOCALRUNNER

    def _init_port_mapping(self):
        pass

    def update_repo_cache(self):
        pass

    def _refresh_instance_state(self):
        return ["127.0.0.1"], ["127.0.0.1"]

    def refresh_ip_address(self):
        # IP is always localhost for local monitoring node
        pass

    @cached_property
    def grafana_address(self):
        # Grafana starts in port mapping mode under Docker and we can't use 127.0.0.1 because it'll be used by
        # RemoteWebDriver from RemoteWebDriverContainer.  We can use a gateway from a bridge network instead,
        # because port mapping works on the gateway address too.
        return get_docker_bridge_gateway(self.remoter)

    def start_journal_thread(self):
        # Hasn't it's own system logs, since running on the host
        pass

    def disable_daily_triggered_services(self):
        pass

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

    @property
    def vm_region(self):
        return "docker"

    @property
    def region(self):
        return "docker"


class MonitorSetDocker(cluster.BaseMonitorSet, DockerCluster):
    def __init__(
        self, targets: dict, user_prefix: Optional[str] = None, n_nodes: Union[list, int] = 3, params: dict = None
    ) -> None:
        node_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-set")

        cluster.BaseMonitorSet.__init__(self, targets=targets, params=params)
        DockerCluster.__init__(
            self,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            node_type="monitor",
            n_nodes=n_nodes,
            params=params,
        )
        self._manager_container = None
        self._manager_db_container = None
        self.manager_container_name = None

    def _create_node(self, node_index, container=None, after_config=None):
        node = DockerMonitoringNode(
            parent_cluster=self,
            base_logdir=self.logdir,
            node_prefix=self.node_prefix,
            node_index=node_index,
            ssh_login_info=None,
        )
        node.init()
        return node

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
        assert instance_type is None, "docker can provision different instance types"
        return self._create_nodes(count, enable_auto_bootstrap)

    @staticmethod
    def install_scylla_monitoring_prereqs(node):
        pass  # since running local, don't install anything, just the monitor

    def install_scylla_manager(self, node):
        """Start manager server + manager-db as Docker containers."""
        docker_client = _default_docker_client()
        docker_network = self.params.get("docker_network")
        user_prefix = self.params.get("user_prefix") or "sct"

        LOGGER.info("Starting Scylla Manager DB container")
        mgr_db_name = f"{user_prefix}-manager-db"
        try:
            docker_client.containers.get(mgr_db_name).remove(force=True)
        except Exception:  # noqa: BLE001
            pass

        mgr_db_image = self.params.get("docker_image") or "scylladb/scylla:latest"
        docker_client.images.pull(*mgr_db_image.split(":", maxsplit=1))
        self._manager_db_container = docker_client.containers.run(
            image=mgr_db_image,
            name=mgr_db_name,
            command="--smp 1 --memory 512M --developer-mode 1",
            network=docker_network,
            detach=True,
        )

        LOGGER.info("Waiting for manager DB to be ready")
        for _ in range(60):
            time.sleep(5)
            exit_code, _ = self._manager_db_container.exec_run("cqlsh -e 'SELECT now() FROM system.local'")
            if exit_code == 0:
                break
        else:
            raise RuntimeError("Manager DB did not become ready in time")

        LOGGER.info("Starting Scylla Manager server container")
        mgr_name = f"{user_prefix}-manager-server"
        try:
            docker_client.containers.get(mgr_name).remove(force=True)
        except Exception:  # noqa: BLE001
            pass

        mgmt_image = self.params.get("mgmt_docker_image") or "scylladb/scylla-manager:latest"
        docker_client.images.pull(*mgmt_image.split(":", maxsplit=1))
        self._manager_db_container.reload()
        mgr_db_ip = self._manager_db_container.attrs["NetworkSettings"]["Networks"][docker_network or "bridge"][
            "IPAddress"
        ]

        self._manager_container = docker_client.containers.run(
            image=mgmt_image,
            name=mgr_name,
            network=docker_network,
            environment={
                "SCYLLA_MANAGER_DB_HOSTS": mgr_db_ip,
            },
            detach=True,
        )

        LOGGER.info("Waiting for manager server to be ready")
        for _ in range(60):
            time.sleep(5)
            exit_code, output = self._manager_container.exec_run(
                'curl --silent --output /dev/null --write-out "%{http_code}" http://127.0.0.1:5080/ping'
            )
            if exit_code == 0 and b"204" in output:
                break
        else:
            raise RuntimeError("Manager server did not become ready in time")

        LOGGER.info("Scylla Manager is running in container %s", mgr_name)
        self.manager_container_name = mgr_name

    def get_backtraces(self):
        pass

    def destroy(self):
        for node in self.nodes:
            try:
                self.stop_scylla_monitoring(node)
                self.log.error("Stopping scylla monitoring succeeded")
            except Exception as exc:  # noqa: BLE001
                self.log.error(f"Stopping scylla monitoring failed with {exc!s}")
            node.destroy()

        if self._manager_container:
            try:
                self._manager_container.remove(force=True)
            except Exception:  # noqa: BLE001
                pass
        if self._manager_db_container:
            try:
                self._manager_db_container.remove(force=True)
            except Exception:  # noqa: BLE001
                pass
