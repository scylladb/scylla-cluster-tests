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

# pylint: disable=too-many-arguments; looks like we need to increase DESIGN.max_args to 10 in our pylintrc
# pylint: disable=invalid-overridden-method; pylint doesn't know that cached_property is property

import os
import re
import logging
import pathlib
from typing import Optional, Union, Dict

from sdcm import cluster
from sdcm.remote import LOCALRUNNER
from sdcm.utils.docker_utils import get_docker_bridge_gateway, Container, ContainerManager, DockerException
from sdcm.utils.decorators import cached_property
from sdcm.utils.health_checker import check_nodes_status

DEFAULT_SCYLLA_DB_IMAGE = "scylladb/scylla-nightly"
DEFAULT_SCYLLA_DB_IMAGE_TAG = "latest"
AIO_MAX_NR_RECOMMENDED_VALUE = 1048576

LOGGER = logging.getLogger(__name__)


class ScyllaDockerRequirementError(cluster.ScyllaRequirementError, DockerException):
    pass


class NodeContainerMixin:
    @cached_property
    def node_container_image_tag(self):
        return self.parent_cluster.node_container_image_tag

    def node_container_image_dockerfile_args(self):
        return dict(path=self.parent_cluster.node_container_context_path)

    def node_container_image_build_args(self):
        return dict(buildargs={"SOURCE_IMAGE": self.parent_cluster.source_image, },
                    labels=self.parent_cluster.tags)

    def node_container_run_args(self, seed_ip):
        return dict(name=self.name,
                    image=self.node_container_image_tag,
                    command=f'--seeds="{seed_ip}"' if seed_ip else None,
                    nano_cpus=10**9)  # Same as `docker run --cpus=1 ...' CLI command.


class DockerNode(cluster.BaseNode, NodeContainerMixin):  # pylint: disable=abstract-method
    def __init__(self,  # pylint: disable=too-many-arguments
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

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    @cached_property
    def public_ip_address(self):
        return ContainerManager.get_ip_address(self, "node")

    @cached_property
    def private_ip_address(self):
        return self.public_ip_address

    def is_running(self):
        return ContainerManager.is_running(self, "node")

    def restart(self):
        ContainerManager.get_container(self, "node").restart()

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        self.remoter.run('sudo supervisorctl start scylla', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

    @cluster.log_run_info
    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300, ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        self.remoter.run('sudo supervisorctl stop scylla', timeout=timeout)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    @cluster.log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    def restart_scylla_server(self, verify_up_before=False, verify_up_after=True, timeout=300, ignore_status=False):
        if verify_up_before:
            self.wait_db_up(timeout=timeout)
        self.remoter.run("sudo supervisorctl restart scylla", timeout=timeout)
        if verify_up_after:
            self.wait_db_up(timeout=timeout)

    @cluster.log_run_info
    def restart_scylla(self, verify_up_before=False, verify_up_after=True, timeout=300):
        self.restart_scylla_server(verify_up_before=verify_up_before, verify_up_after=verify_up_after, timeout=timeout)

    @property
    def image(self) -> str:
        return self.parent_cluster.source_image


class DockerCluster(cluster.BaseCluster):  # pylint: disable=abstract-method
    node_container_context_path = os.path.join(os.path.dirname(__file__), '../docker/scylla-sct')
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
        self.node_container_image_tag = f"scylla-sct:{node_type}-{str(cluster.Setup.test_id())[:8]}"
        self.node_container_key_file = node_key_file

        super(DockerCluster, self).__init__(cluster_prefix=cluster_prefix,
                                            node_prefix=node_prefix,
                                            n_nodes=n_nodes,
                                            params=params,
                                            region_names=["localhost-dc", ],  # Multi DC is not supported currently.
                                            node_type=node_type)

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
            ContainerManager.build_container_image(node, "node")

        ContainerManager.run_container(node, "node", seed_ip=self.nodes[0].public_ip_address if node_index else None)
        ContainerManager.wait_for_status(node, "node", status="running")
        ContainerManager.ssh_copy_id(node, "node", self.node_container_user, self.node_container_key_file)

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
        for node_index, container in sorted((int(c.labels["NodeIndex"]), c) for c in containers):
            LOGGER.debug("Found container %s with name `%s' and index=%d", container, container.name, node_index)
            node = self._create_node(node_index, container)
            self.nodes.append(node)
        return self.nodes

    def add_nodes(self, count, ec2_user_data="", dc_idx=0, enable_auto_bootstrap=False):
        return self._get_nodes() if cluster.Setup.REUSE_CLUSTER else self._create_nodes(count, enable_auto_bootstrap)


class ScyllaDockerCluster(cluster.BaseScyllaCluster, DockerCluster):  # pylint: disable=abstract-method
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

    def node_setup(self, node, verbose=False, timeout=3600):
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = ','.join(self.seed_nodes_ips)

        node.wait_ssh_up(verbose=verbose)

        if not node.is_scylla_installed():
            raise Exception(f"There is no pre-installed ScyllaDB")

        self.check_aio_max_nr(node)

        if cluster.Setup.BACKTRACE_DECODING:
            node.install_scylla_debuginfo()

        self.node_config_setup(node, seed_address, endpoint_snitch)

        node.stop_scylla_server(verify_down=False)
        node.remoter.run('sudo rm -Rf /var/lib/scylla/data/*')  # Clear data folder to drop wrong cluster name data.
        node.start_scylla_server(verify_up=False)

        node.wait_db_up(verbose=verbose, timeout=timeout)
        nodes_status = node.get_nodes_status()
        check_nodes_status(nodes_status=nodes_status, current_node=node)
        self.clean_replacement_node_ip(node)

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
    def wait_for_init(self, node_list=None, verbose=False, timeout=None):  # pylint: disable=unused-argument,arguments-differ
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)

    def get_scylla_args(self):
        # pylint: disable=no-member
        append_scylla_args = self.params.get('append_scylla_args_oracle') if self.name.find('oracle') > 0 else \
            self.params.get('append_scylla_args')
        return re.sub(r'--blocked-reactor-notify-ms[ ]+[0-9]+', '', append_scylla_args)


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

    def node_setup(self, node, verbose=False, db_node_address=None, **kwargs):
        self.install_gemini(node=node)
        if self.params.get('client_encrypt'):
            node.config_client_encrypt()


class DockerMonitoringNode(cluster.BaseNode):  # pylint: disable=abstract-method,too-many-instance-attributes
    log = LOGGER

    def __init__(self,
                 parent_cluster: "MonitorSetDocker",
                 node_prefix: str = "monitor-node",
                 base_logdir: Optional[str] = None,
                 node_index: int = 1) -> None:
        super().__init__(name=f"{node_prefix}-{node_index}",
                         parent_cluster=parent_cluster,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix)
        self.node_index = node_index

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _init_remoter(self, ssh_login_info):  # pylint: disable=no-self-use
        self.remoter = LOCALRUNNER

    def _init_port_mapping(self):  # pylint: disable=no-self-use
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
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


class MonitorSetDocker(cluster.BaseMonitorSet, DockerCluster):  # pylint: disable=abstract-method
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
                                    node_index=node_index)
        node.init()
        return node

    def add_nodes(self, count, ec2_user_data="", dc_idx=0, enable_auto_bootstrap=False):
        return self._create_nodes(count, enable_auto_bootstrap)

    @staticmethod
    def install_scylla_monitoring_prereqs(node):  # pylint: disable=invalid-name
        pass  # since running local, don't install anything, just the monitor

    @staticmethod
    def get_monitor_install_path_base(node):
        base_dir = os.environ.get("_SCT_BASE_DIR", None)
        logdir = pathlib.Path(node.logdir)
        logdir = pathlib.Path(base_dir).joinpath(*logdir.parts[2:]) if base_dir else logdir
        return str(logdir)

    def get_backtraces(self):
        pass

    def destroy(self):
        for node in self.nodes:
            try:
                self.stop_scylla_monitoring(node)
                self.log.error(f"Stopping scylla monitoring succeeded")
            except Exception as exc:  # pylint: disable=broad-except
                self.log.error(f"Stopping scylla monitoring failed with {str(exc)}")
            try:
                node.remoter.run(f"sudo rm -rf '{self._monitor_install_path_base}'")
                self.log.error(f"Cleaning up scylla monitoring succeeded")
            except Exception as exc:  # pylint: disable=broad-except
                self.log.error(f"Cleaning up scylla monitoring failed with {str(exc)}")
            node.destroy()
