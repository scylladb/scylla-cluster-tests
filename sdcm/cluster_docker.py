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
from typing import Optional, Union, Dict
from functools import cached_property


from sdcm import cluster
from sdcm.remote import LOCALRUNNER
from sdcm.remote.docker_cmd_runner import DockerCmdRunner
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.utils.docker_utils import get_docker_bridge_gateway, Container, ContainerManager, DockerException
from sdcm.utils.health_checker import check_nodes_status
from sdcm.utils.nemesis_utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.utils.net import get_my_public_ip
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
        return dict(buildargs={"SOURCE_IMAGE": self.parent_cluster.source_image, },
                    labels=self.parent_cluster.tags)

    def node_container_run_args(self, seed_ip):
        volumes = {
            '/var/run/docker.sock': {"bind": '/var/run/docker.sock', "mode": "rw"},
        }

        smp = 1
        if self.node_type == 'db':
            scylla_args = self.parent_cluster.params.get('append_scylla_args')
            smp_match = re.search(r'--smp\s(\d+)', scylla_args)
            smp = int(smp_match.group(1)) if smp_match else 1

        return dict(name=self.name,
                    image=self.node_container_image_tag,
                    command=f'--seeds="{seed_ip}"' if seed_ip else None,
                    volumes=volumes,
                    network=self.parent_cluster.params.get('docker_network'),
                    nano_cpus=smp*10**9)  # Same as `docker run --cpus=N ...' CLI command.


class DockerNode(cluster.BaseNode, NodeContainerMixin):
    def __init__(self,
                 parent_cluster: "DockerCluster",
                 container: Optional[Container] = None,
                 node_prefix: str = "node",
                 base_logdir: Optional[str] = None,
                 ssh_login_info: Optional[dict] = None,
                 node_index: int = 1) -> None:
        super().__init__(name=f"{node_prefix}-{node_index}",
                         parent_cluster=parent_cluster,
                         ssh_login_info=ssh_login_info,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix)
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
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

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
        container_status = ContainerManager.get_container(self, "node").attrs['State']['Status']
        # return similar interface to what supervisorctl would return
        return type('ServiceStatus', (), {
            'stdout': f"{service_name} {container_status}",
            'stderr': "",
            'ok': container_status in ['running', 'active', 'RUNNING']
        })

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=None):
        verify_up_timeout = verify_up_timeout or self.verify_up_timeout
        if verify_down:
            self.wait_db_down(timeout=timeout)

        container = ContainerManager.get_container(self, "node")
        if container.status != 'running':
            self.log.info("Starting Docker container %s", container.name)
            container.start()
            ContainerManager.wait_for_status(self, "node", status="running")

        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

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

    def install_sudo(self, user: str = 'scylla', verbose=False):
        """Install and configure passwordless sudo"""
        pkg_mgr = 'microdnf' if self.distro.is_rhel_like else 'apt'
        self.remoter.run(f'{pkg_mgr} install -y sudo', verbose=verbose, ignore_status=True, user='root')

        self.remoter.run("mkdir -p /etc/sudoers.d", user='root', ignore_status=True, verbose=verbose)
        sudoers_file = f"/etc/sudoers.d/{user}-nopasswd"
        sudoers_content = f"{user} ALL=(ALL) NOPASSWD: ALL"
        self.remoter.run(f"echo '{sudoers_content}' > {sudoers_file}", user='root', verbose=verbose, ignore_status=True)
        self.remoter.run(f"chmod 440 {sudoers_file}", user='root', verbose=verbose, ignore_status=True)

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
            result = self.remoter.run("ps -C scylla -o pid --no-headers", ignore_status=True, user='root')
            if result.ok and (pid := result.stdout.strip()):
                self.remoter.run(f"kill -s HUP {pid}", ignore_status=True, user='root')
        except Exception:  # noqa: BLE001
            self.restart_scylla_server(verify_up_before=True, verify_up_after=True)
        self.log.info("Scylla configuration have been reloaded")


class VectorStoreDockerNode(VectorStoreNodeMixin, DockerNode):
    """Docker node running Vector Store service"""

    def __init__(self,
                 parent_cluster: "VectorStoreSetDocker",
                 container: Optional[Container] = None,
                 node_prefix: str = "vector",
                 base_logdir: Optional[str] = None,
                 ssh_login_info: Optional[dict] = None,
                 node_index: int = 1) -> None:
        super().__init__(parent_cluster=parent_cluster,
                         container=container,
                         node_prefix=node_prefix,
                         base_logdir=base_logdir,
                         ssh_login_info=ssh_login_info,
                         node_index=node_index)

    def node_container_run_args(self, seed_ip=None):
        return self.vector_container_run_args(seed_ip)

    def vector_container_run_args(self, seed_ip=None):
        environment = {
            'VECTOR_STORE_URI': f"0.0.0.0:{self.parent_cluster.params.get('vector_store_port')}",
            'VECTOR_STORE_SCYLLADB_URI': self.scylla_uri,
        }

        if (threads := self.parent_cluster.params.get('vector_store_threads')) > 0:
            environment['VECTOR_STORE_THREADS'] = str(threads)
        ports = {f"{self.parent_cluster.params.get('vector_store_port')}/tcp": None}

        return dict(
            name=self.name,
            image=self.node_container_image_tag,
            environment=environment,
            ports=ports,
            network=self.parent_cluster.params.get('docker_network'))


class DockerCluster(cluster.BaseCluster):
    node_container_user = "scylla-test"

    def __init__(self,
                 docker_image: str = DEFAULT_SCYLLA_DB_IMAGE,
                 docker_image_tag: str = DEFAULT_SCYLLA_DB_IMAGE_TAG,
                 node_key_file: Optional[str] = None,
                 cluster_prefix: str = "cluster",
                 node_prefix: str = "node",
                 node_type: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: dict = None) -> None:
        self.source_image = f"{docker_image}:{docker_image_tag}"
        self.node_container_key_file = node_key_file

        super().__init__(cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=["localhost-dc", ],  # Multi DC is not supported currently.
                         node_type=node_type)

    @property
    def node_container_context_path(self):
        # scylla_linux_distro can be: centos or ubuntu-focal, hence we need to split it
        return os.path.join(os.path.dirname(__file__), '../docker/scylla-sct',
                            self.params.get("scylla_linux_distro").split('-')[0])

    def _create_node(self, node_index, container=None):
        node = DockerNode(parent_cluster=self,
                          container=container,
                          ssh_login_info=dict(hostname=None,
                                              user=self.node_container_user,
                                              key_file=self.node_container_key_file),
                          base_logdir=self.logdir,
                          node_prefix=self.node_prefix,
                          node_index=node_index)

        if container is None:
            ContainerManager.run_container(
                node, "node", seed_ip=self.nodes[0].public_ip_address if node_index else None)
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
    def add_nodes(self, count, ec2_user_data="", dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        assert instance_type is None, "docker can't provision different instance types"
        return self._get_nodes() if self.test_config.REUSE_CLUSTER else self._create_nodes(count, enable_auto_bootstrap)


class ScyllaDockerCluster(cluster.BaseScyllaCluster, DockerCluster):
    def __init__(self,
                 docker_image: str = DEFAULT_SCYLLA_DB_IMAGE,
                 docker_image_tag: str = DEFAULT_SCYLLA_DB_IMAGE_TAG,
                 node_key_file: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, str] = 3,
                 params: dict = None) -> None:
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')
        super().__init__(docker_image=docker_image,
                         docker_image_tag=docker_image_tag,
                         node_key_file=node_key_file,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         node_type="scylla-db",
                         n_nodes=n_nodes,
                         params=params)

        self.vector_store_cluster = None

    def node_setup(self, node, verbose=False, timeout=3600):
        node.is_scylla_installed(raise_if_not_installed=True)
        self.check_aio_max_nr(node)
        if self.test_config.BACKTRACE_DECODING:
            node.install_scylla_debuginfo()

        node.config_setup(append_scylla_args=self.get_scylla_args())
        node.restart_scylla(verify_up_before=True)

    def node_startup(self, node, verbose=False, timeout=3600):
        if not ContainerManager.is_running(node, "node"):
            container = ContainerManager.get_container(node, "node")
            container.start()
        node.wait_db_up(verbose=verbose, timeout=timeout)
        for event in check_nodes_status(nodes_status=node.get_nodes_status(),
                                        current_node=node,
                                        removed_nodes_list=self.dead_nodes_ip_address_list):
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
                f"is less than recommended value ({recommended_value})")

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, check_node_health=True):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)

    def get_scylla_args(self):

        append_scylla_args = self.params.get('append_scylla_args_oracle') if self.name.find('oracle') > 0 else \
            self.params.get('append_scylla_args')
        return re.sub(r'--blocked-reactor-notify-ms[ ]+[0-9]+', '', append_scylla_args)

    def destroy(self):
        if self.vector_store_cluster:
            self.log.info("Destroying vector store cluster...")
            try:
                self.vector_store_cluster.destroy()
            except Exception as e:  # noqa: BLE001
                self.log.warning("Failed destroy vector store cluster: %s", e)

        super().destroy()


class VectorStoreSetDocker(VectorStoreClusterMixin, DockerCluster):
    """Set of Vector Store nodes"""

    def __init__(self,
                 params,
                 vs_docker_image,
                 vs_docker_image_tag,
                 **kwargs):
        self.scylla_cluster = None

        kwargs['cluster_prefix'] = cluster.prepend_user_prefix(kwargs.get('cluster_prefix'), 'vs-set')
        kwargs.setdefault('node_prefix', 'vs-node')
        kwargs.setdefault('node_type', 'vs')

        if not vs_docker_image_tag:
            vs_docker_image_tag = 'latest'

        super().__init__(docker_image=vs_docker_image,
                         docker_image_tag=vs_docker_image_tag,
                         params=params, **kwargs)

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

    def _create_node(self, node_index, container=None):
        node = VectorStoreDockerNode(
            parent_cluster=self,
            container=container,
            node_prefix=self.node_prefix,
            base_logdir=self.logdir,
            node_index=node_index)

        if container is None:
            ContainerManager.run_container(node, "node")
            ContainerManager.wait_for_status(node, "node", status="running")

        node.init()
        return node

    def add_nodes(self, count, dc_idx=0, **kwargs):
        if count > 1:
            # TODO: implement once HA support is implemented for VS
            self.log.warning("Vector Store HA not implemented yet, creating only 1 node instead of %d", count)
            count = 1

        reuse_cluster = getattr(self.test_config, 'REUSE_CLUSTER', 'UNDEFINED')
        result = self._get_nodes() if reuse_cluster else self._create_nodes(count)
        return result


class LoaderSetDocker(cluster.BaseLoaderSet, DockerCluster):
    def __init__(self,
                 docker_image: str = DEFAULT_SCYLLA_DB_IMAGE,
                 docker_image_tag: str = DEFAULT_SCYLLA_DB_IMAGE_TAG,
                 node_key_file: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, str] = 3,
                 params: dict = None) -> None:
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')

        cluster.BaseLoaderSet.__init__(self,
                                       params=params)
        DockerCluster.__init__(self,
                               docker_image=docker_image,
                               docker_image_tag=docker_image_tag,
                               node_key_file=node_key_file,
                               cluster_prefix=cluster_prefix,
                               node_prefix=node_prefix,
                               node_type="loader",
                               n_nodes=n_nodes,
                               params=params)

    def node_setup(self, node: DockerNode, verbose=False, **kwargs):
        node.remoter.sudo("apt update", verbose=True, ignore_status=True)
        node.remoter.sudo("apt install -y openjdk-8-jre", verbose=True, ignore_status=True)
        node.remoter.sudo("ln -sf /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/bin/java* /etc/alternatives/java",
                          verbose=True, ignore_status=True)

        self._install_docker_cli(node, verbose=verbose)
        if self.params.get('client_encrypt'):
            node.config_client_encrypt()

    def _install_docker_cli(self, node, verbose=False):
        result = node.remoter.run("docker --version", ignore_status=True, verbose=False)
        if result.ok:
            self.log.debug("Docker CLI already installed on loader node: %s", result.stdout.strip())
            return

        self.log.debug("Installing Docker CLI on loader node")

        commands = (
            [
                'apt update',
                'apt install -y gnupg2 software-properties-common lsb-release',
                'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -',
                'add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"',
                'apt update',
                'apt install -y docker-ce-cli'
            ]
            if node.distro.is_debian_like
            else [
                'curl -L https://download.docker.com/linux/centos/docker-ce.repo -o /etc/yum.repos.d/docker-ce.repo',
                'microdnf -y update',
                'microdnf -y install docker-ce-cli'
            ])

        for cmd in commands:
            result = node.remoter.run(cmd, timeout=300, verbose=verbose, ignore_status=True, retry=3, user='root')
            if not result.ok:
                raise RuntimeError(f"Command {cmd} failed with error: {result.stderr.strip()}")

        verify = node.remoter.run("docker --version", ignore_status=True)
        if verify.ok:
            self.log.info("Docker CLI installed successfully: %s", verify.stdout.strip())
        else:
            raise RuntimeError("Docker CLI installation verification failed")


class DockerMonitoringNode(cluster.BaseNode):
    log = LOGGER

    def __init__(self,
                 parent_cluster: "MonitorSetDocker",
                 node_prefix: str = "monitor-node",
                 base_logdir: Optional[str] = None,
                 node_index: int = 1,
                 ssh_login_info: Optional[dict] = None) -> None:
        super().__init__(name=f"{node_prefix}-{node_index}",
                         parent_cluster=parent_cluster,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix,
                         ssh_login_info=ssh_login_info)
        self.node_index = node_index

    def wait_for_cloud_init(self):
        pass

    @staticmethod
    def is_docker() -> bool:
        return True

    @cached_property
    def tags(self) -> dict[str, str]:
        return {**super().tags, "NodeIndex": str(self.node_index), }

    def _init_remoter(self, ssh_login_info):
        self.remoter = LOCALRUNNER

    def _init_port_mapping(self):
        pass

    def update_repo_cache(self):
        pass

    def _refresh_instance_state(self):
        return ["127.0.0.1"], ["127.0.0.1"]

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
    def __init__(self,
                 targets: dict,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: dict = None) -> None:
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')

        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        DockerCluster.__init__(self,
                               cluster_prefix=cluster_prefix,
                               node_prefix=node_prefix,
                               node_type="monitor",
                               n_nodes=n_nodes,
                               params=params)

    def _create_node(self, node_index, container=None):
        node = DockerMonitoringNode(parent_cluster=self,
                                    base_logdir=self.logdir,
                                    node_prefix=self.node_prefix,
                                    node_index=node_index,
                                    ssh_login_info=dict(hostname=None,
                                                        user=self.node_container_user,
                                                        key_file=self.node_container_key_file)
                                    )
        node.init()
        return node

    def add_nodes(self, count, ec2_user_data="", dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        assert instance_type is None, "docker can provision different instance types"
        return self._create_nodes(count, enable_auto_bootstrap)

    @staticmethod
    def install_scylla_monitoring_prereqs(node):
        pass  # since running local, don't install anything, just the monitor

    def get_backtraces(self):
        pass

    def destroy(self):
        for node in self.nodes:
            try:
                self.stop_scylla_monitoring(node)
                self.log.error("Stopping scylla monitoring succeeded")
            except Exception as exc:  # noqa: BLE001
                self.log.error(f"Stopping scylla monitoring failed with {str(exc)}")
            try:
                node.remoter.sudo(f"rm -rf '{self.monitor_install_path_base}'")
                self.log.error("Cleaning up scylla monitoring succeeded")
            except Exception as exc:  # noqa: BLE001
                self.log.error(f"Cleaning up scylla monitoring failed with {str(exc)}")
            node.destroy()
