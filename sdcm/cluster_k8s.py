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

# pylint: disable=too-many-arguments

import os
import logging
from typing import Optional, Union, List, Dict
from textwrap import dedent
from functools import cached_property

from sdcm import cluster, cluster_gce, cluster_docker
from sdcm.remote import LOCALRUNNER
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.sct_config import sct_abs_path
from sdcm.sct_events import TestFrameworkEvent
from sdcm.utils.k8s import KubernetesOps
from sdcm.utils.common import get_free_port, wait_for_port
from sdcm.utils.docker_utils import ContainerManager
from sdcm.utils.remote_logger import get_system_logging_thread


KUBECTL_PROXY_PORT = 8001
KUBECTL_PROXY_CONTAINER = "auto_ssh:kubectl_proxy"
SCYLLA_OPERATOR_CONFIG = sct_abs_path("sdcm/k8s_configs/operator.yaml")
SCYLLA_CLUSTER_CONFIG = sct_abs_path("sdcm/k8s_configs/cluster.yaml")
IPTABLES_BIN = "iptables"

LOGGER = logging.getLogger(__name__)


class MinikubeOps:
    @classmethod
    def setup_minikube(cls, node: cluster.BaseNode, kubectl_version: str, minikube_version: str) -> None:
        LOGGER.debug("Prepare %s to run Minikube cluster", node)
        if node.distro.is_ubuntu18:
            cls.setup_minikube_ubuntu18(node, kubectl_version, minikube_version)
        else:
            raise ValueError(f"{node.distro} is not supported by SCT for running Minikube")

    @staticmethod
    def setup_minikube_ubuntu18(node: cluster.BaseNode, kubectl_version: str, minikube_version: str) -> None:
        minikube_setup_script = dedent(f"""
            # Make sure that cloud-init finished running.
            until [ -f /var/lib/cloud/instance/boot-finished ]; do sleep 1; done

            # Disable apt-key warnings and set non-interactive frontend.
            export APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
            export DEBIAN_FRONTEND=noninteractive

            apt-get -qq update
            apt-get -qq install --no-install-recommends apt-transport-https conntrack

            # Install and configure Docker.
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
            add-apt-repository \\"deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable\\"
            apt-get -qq install --no-install-recommends docker-ce docker-ce-cli containerd.io
            usermod -a -G docker {node.ssh_login_info["user"]}
            echo 1048576 > /proc/sys/fs/aio-max-nr

            # Download kubectl binary.
            curl -fsSLo /usr/local/bin/kubectl \
                https://storage.googleapis.com/kubernetes-release/release/v{kubectl_version}/bin/linux/amd64/kubectl
            chmod +x /usr/local/bin/kubectl

            # Download Minikube binary.
            curl -fsSLo /usr/local/bin/minikube \
                https://storage.googleapis.com/minikube/releases/v{minikube_version}/minikube-linux-amd64
            chmod +x /usr/local/bin/minikube

            # Install Helm 3.
            snap install helm --classic
            ln -s /snap/bin/helm /usr/local/bin/helm
        """)
        node.remoter.run(f'sudo bash -cxe "{minikube_setup_script}"')

        # Reconnect to update user groups.
        node.remoter.reconnect()

    @staticmethod
    def start_minikube(node: cluster.BaseNode) -> None:
        LOGGER.debug("Start Minikube cluster")
        minikube_start_script = dedent(f"""
            sysctl fs.protected_regular=0
            minikube start --driver=none --extra-config=apiserver.service-node-port-range=1-65535
            chown -R $USER $HOME/.kube $HOME/.minikube
        """)
        node.remoter.run(f'sudo -E bash -cxe "{minikube_start_script}"')

    @staticmethod
    def get_kubectl_proxy(node: cluster.BaseNode) -> [str, int]:
        LOGGER.debug("Stop any other process listening on kubectl proxy port")
        node.remoter.run(f"sudo fuser -v4k {KUBECTL_PROXY_PORT}/tcp", ignore_status=True)

        LOGGER.debug("Start kubectl proxy in detached mode")
        node.remoter.run(
            "setsid kubectl proxy --disable-filter --accept-hosts '.*' > proxy.log 2>&1 < /dev/null & sleep 1")

        LOGGER.debug("Start auto_ssh for kubectl proxy")
        ContainerManager.run_container(node, KUBECTL_PROXY_CONTAINER,
                                       local_port=KUBECTL_PROXY_PORT,
                                       remote_port=get_free_port(),
                                       ssh_mode="-L")

        host = "127.0.0.1"
        port = int(ContainerManager.get_environ(node, KUBECTL_PROXY_CONTAINER)["SSH_TUNNEL_REMOTE"])

        LOGGER.debug("Waiting for port %s:%s is accepting connections", host, port)
        wait_for_port(host, port)

        return host, port


class KubernetesCluster:  # pylint: disable=too-few-public-methods
    datacenter = ()

    @staticmethod
    def k8s_server_url():
        return None


class MinikubeCluster(KubernetesCluster):
    @property
    def minikube_version(self):
        try:
            return self._minikube_version
        except AttributeError:
            raise ValueError("You should set `minikube_version' first.") from None

    @minikube_version.setter
    def minikube_version(self, value):
        self._minikube_version = value

    @cached_property
    def local_kubectl_version(self):  # pylint: disable=no-self-use
        # Example of kubectl command output:
        #   $ kubectl version --client --short
        #   Client Version: v1.18.5
        return LOCALRUNNER.run("kubectl version --client --short").stdout.rsplit(None, 1)[-1][1:]

    @cached_property
    def k8s_server_url(self):
        host, port = MinikubeOps.get_kubectl_proxy(self.nodes[-1])
        return f"http://{host}:{port}"

    def node_setup(self, node, verbose=False, timeout=3600):
        node.wait_ssh_up(verbose=verbose, timeout=timeout)

        if not cluster.Setup.REUSE_CLUSTER:
            MinikubeOps.setup_minikube(node=node,
                                       kubectl_version=self.local_kubectl_version,
                                       minikube_version=self.minikube_version)
            MinikubeOps.start_minikube(node)


class GceMinikubeCluster(MinikubeCluster, cluster_gce.GCECluster):
    def __init__(self, minikube_version, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,  # pylint: disable=too-many-arguments
                 gce_instance_type="n1-highmem-8", gce_image_username="centos", user_prefix=None, params=None,
                 gce_datacenter=None):
        # pylint: disable=too-many-locals
        self.minikube_version = minikube_version

        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "k8s")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "k8s-minikube")
        super().__init__(gce_image=gce_image,
                         gce_image_type=gce_image_type,
                         gce_image_size=gce_image_size,
                         gce_n_local_ssd=0,
                         gce_network=gce_network,
                         gce_instance_type=gce_instance_type,
                         gce_image_username=gce_image_username,
                         services=services,
                         credentials=credentials,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=1,
                         add_disks=None,
                         params=params,
                         gce_region_names=gce_datacenter,
                         node_type="scylla-db")

    @cluster.wait_for_init_wrap
    def wait_for_init(self):
        for node in self.nodes:
            node.remoter.reconnect()  # Reconnect to update user groups in main thread too.

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Not implemented yet.")  # TODO: add implementation of this method


class BasePodContainer(cluster.BaseNode):
    def __init__(self, name: str, parent_cluster: "PodCluster", node_prefix: str = "node", node_index: int = 1,
                 base_logdir: Optional[str] = None, dc_idx: int = 0):
        self.node_index = node_index
        super().__init__(name=name,
                         parent_cluster=parent_cluster,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix,
                         dc_idx=dc_idx)

    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _init_remoter(self, ssh_login_info):
        self.remoter = KubernetesCmdRunner(pod=self.name,
                                           container=self.parent_cluster.container,
                                           namespace=self.parent_cluster.namespace,
                                           k8s_server_url=self.parent_cluster.k8s_cluster.k8s_server_url)

        # TODO: refactor our commands to use sudo in more organized way and remove `sudo' dependency
        # TODO: remove `net-tools' dependency by using `ss' instead of `netstat'
        # TODO: remove `file' dependency by using `readelf' instead of `file'
        self.remoter.run("yum install -y sudo net-tools file && yum clean all")

    def _init_port_mapping(self):
        pass

    @property
    def system_log(self):
        return os.path.join(self.logdir, "system.log")

    @property
    def region(self):
        return self.parent_cluster.k8s_cluster.datacenter[0]  # TODO: find the node and return it's region.

    def start_journal_thread(self):
        self._journal_thread = get_system_logging_thread(logs_transport="kubectl",
                                                         node=self,
                                                         target_log_file=self.system_log)
        if self._journal_thread:
            self.log.info("Use %s as logging daemon", type(self._journal_thread).__name__)
            self._journal_thread.start()
        else:
            TestFrameworkEvent(source=self.__class__.__name__,
                               source_method="start_journal_thread",
                               message="Got no logging daemon by unknown reason").publish()

    def check_spot_termination(self):
        pass

    @property
    def scylla_listen_address(self):
        return self._pod_status.pod_ip

    @property
    def _pod_status(self):
        pods = KubernetesOps.list_pods(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                       field_selector=f"metadata.name={self.name}")
        return pods[0].status if pods else None

    @property
    def _cluster_ip_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}")
        return services[0] if services else None

    @property
    def _loadbalancer_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}-loadbalancer")
        return services[0] if services else None

    @property
    def _container_status(self):
        pod_status = self._pod_status
        if pod_status:
            return next((x for x in pod_status.container_statuses if x.name == self.parent_cluster.container), None)
        return None

    def _refresh_instance_state(self):
        return ([self._cluster_ip_service.spec.cluster_ip, ],
                [self._cluster_ip_service.spec.cluster_ip, self._pod_status.pod_ip, ], )

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        self.remoter.run("supervisorctl start scylla", timeout=timeout)
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
        self.remoter.run("supervisorctl restart scylla", timeout=timeout)
        if verify_up_after:
            self.wait_db_up(timeout=timeout)

    @cluster.log_run_info
    def restart_scylla(self, verify_up_before=False, verify_up_after=True, timeout=300):
        self.restart_scylla_server(verify_up_before=verify_up_before, verify_up_after=verify_up_after, timeout=timeout)

    @property
    def image(self) -> str:
        return self._container_status.image

    def iptables_node_redirect_rules(self, dest_ip: str) -> str:
        to_ip = self._cluster_ip_service.spec.cluster_ip
        ports = self._loadbalancer_service.spec.ports
        return "\n".join(iptables_port_redirect_rule(to_ip, p.target_port, dest_ip, p.node_port) for p in ports)

    @property
    def ipv6_ip_address(self):
        raise NotImplementedError()

    def restart(self):
        raise NotImplementedError("Not implemented yet")  # TODO: implement this method.


class PodCluster(cluster.BaseCluster):
    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 namespace: str = "default",
                 container: Optional[str] = None,
                 cluster_uuid: Optional[str] = None,
                 cluster_prefix: str = "cluster",
                 node_prefix: str = "node",
                 node_type: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None) -> None:
        self.k8s_cluster = k8s_cluster
        self.namespace = namespace
        self.container = container

        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=k8s_cluster.datacenter,
                         node_type=node_type)

    @cached_property
    def _k8s_core_v1_api(self):
        return KubernetesOps.core_v1_api(self.k8s_cluster)

    def _create_node(self, node_index: int, pod_name: str) -> BasePodContainer:
        node = BasePodContainer(parent_cluster=self,
                                name=pod_name,
                                base_logdir=self.logdir,
                                node_prefix=self.node_prefix,
                                node_index=node_index)

        node.init()

        return node

    def add_nodes(self, count: int, ec2_user_data: str = "", dc_idx: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:
        pods = KubernetesOps.list_pods(self, namespace=self.namespace)

        self.log.debug("Numbers of pods: %s", len(pods))
        assert count == len(pods), "You can't alter number of pods here"

        nodes = []
        for node_index, pod in enumerate(pods):
            node = self._create_node(node_index, pod.metadata.name)
            nodes.append(node)
            self.nodes.append(node)

        return nodes

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Derived class must implement 'node_setup' method!")

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Derived class must implement 'get_node_ips_param' method!")

    def wait_for_init(self):
        raise NotImplementedError("Derived class must implement 'wait_for_init' method!")


class ScyllaPodCluster(cluster.BaseScyllaCluster, PodCluster):
    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None) -> None:
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')

        super().__init__(k8s_cluster=k8s_cluster,
                         namespace="scylla",
                         container="scylla",
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         node_type="scylla-db",
                         n_nodes=n_nodes,
                         params=params)

    wait_for_init = cluster_docker.ScyllaDockerCluster.wait_for_init
    get_scylla_args = cluster_docker.ScyllaDockerCluster.get_scylla_args


def iptables_port_redirect_rule(to_ip, to_port, dest_ip, dest_port):
    return f"sudo {IPTABLES_BIN} -t nat -A OUTPUT -d {to_ip} -p tcp --dport {to_port} " \
           f"-j DNAT --to-destination {dest_ip}:{dest_port}"
