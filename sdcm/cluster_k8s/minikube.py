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

import logging
from typing import Optional, List
from textwrap import dedent
from functools import cached_property

from invoke.exceptions import UnexpectedExit

from sdcm import sct_abs_path, cluster, cluster_gce
from sdcm.remote import LOCALRUNNER
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.cluster_k8s import KubernetesCluster, BaseScyllaPodContainer, ScyllaPodCluster
from sdcm.cluster_k8s.iptables import IptablesPodPortsRedirectMixin, IptablesClusterOpsMixin
from sdcm.cluster_gce import MonitorSetGCE
from sdcm.utils.k8s import KubernetesOps, K8S_CONFIGS_PATH_IN_CONTAINER
from sdcm.utils.common import get_free_port, wait_for_port
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import ContainerManager


SCYLLA_CLUSTER_CONFIG = f"{K8S_CONFIGS_PATH_IN_CONTAINER}/minikube-cluster-chart-values.yaml"
KUBECTL_PROXY_PORT = 8001
KUBECTL_PROXY_CONTAINER = "auto_ssh:kubectl_proxy"
SCYLLA_POD_EXPOSED_PORTS = [3000, 9042, 9180, ]

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

            mkdir -p /var/lib/scylla/coredumps

            cat <<EOF > /etc/sysctl.d/99-sct-minikube.conf
            fs.aio-max-nr=1048576
            net.ipv4.ip_forward=1
            net.ipv4.conf.all.forwarding=1
            kernel.core_pattern=/var/lib/scylla/coredumps/%h-%P-%u-%g-%s-%t.core
            EOF
            sysctl --system

            # Download kubectl binary.
            curl -fsSLo /usr/local/bin/kubectl \
                https://storage.googleapis.com/kubernetes-release/release/v{kubectl_version}/bin/linux/amd64/kubectl
            chmod +x /usr/local/bin/kubectl

            # Download Minikube binary.
            curl -fsSLo /usr/local/bin/minikube \
                https://storage.googleapis.com/minikube/releases/v{minikube_version}/minikube-linux-amd64
            chmod +x /usr/local/bin/minikube
        """)
        node.remoter.run(f'sudo bash -cxe "{minikube_setup_script}"', change_context=True)

    @staticmethod
    def start_minikube(node: cluster.BaseNode) -> None:
        LOGGER.debug("Start Minikube cluster")
        minikube_start_script = dedent(f"""
            sysctl fs.protected_regular=0
            minikube start --driver=none --extra-config=apiserver.service-node-port-range=1-65535
            ip link set docker0 promisc on
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

    @cached_property
    def hydra_dest_ip(self) -> Optional[str]:
        return self.nodes[-1].external_address

    @cached_property
    def nodes_dest_ip(self) -> Optional[str]:
        return self.nodes[-1].ip_address

    @cached_property
    def remoter(self) -> KubernetesCmdRunner:
        return self.nodes[-1].remoter

    def docker_pull(self, image):
        LOGGER.info("Pull `%s' to Minikube' Docker environment", image)
        self.remoter.run(f"docker pull -q {image}")


class GceMinikubeCluster(MinikubeCluster, cluster_gce.GCECluster):
    def __init__(self, minikube_version, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,  # pylint: disable=too-many-arguments
                 gce_instance_type="n1-highmem-8", gce_image_username="centos", user_prefix=None, params=None,
                 gce_datacenter=None):
        # pylint: disable=too-many-locals
        self.minikube_version = minikube_version

        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "k8s-minikube")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "node")
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
            node.remoter._reconnect()  # Reconnect to update user groups in main thread too.

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Not implemented yet.")  # TODO: add implementation of this method

    def destroy(self) -> None:
        super().destroy()
        self.stop_k8s_task_threads()


class MinikubeScyllaPodContainer(BaseScyllaPodContainer, IptablesPodPortsRedirectMixin):
    parent_cluster: 'MinikubeScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @cached_property
    def host_remoter(self):
        return self.parent_cluster.k8s_cluster.remoter

    @property
    def docker_id(self):
        return self.host_remoter.run(
            f'docker ps -q --filter "label=io.kubernetes.pod.name={self.name}" '
            f'--filter "label=io.kubernetes.container.name=scylla"').stdout.strip()

    def restart(self):
        self.host_remoter.run(f"docker restart {self.docker_id}")

    @cached_property
    def node_type(self) -> 'str':
        return 'db'

    @cached_property
    def hydra_dest_ip(self):
        return self.parent_cluster.k8s_cluster.hydra_dest_ip

    @cached_property
    def nodes_dest_ip(self):
        return self.parent_cluster.k8s_cluster.nodes_dest_ip

    def drain_k8s_node(self):
        self.log.debug('Node draining is not possible on minikube')

    def destroy(self):
        self.parent_cluster.update_nodes_iptables_redirect_rules(command="D", nodes=[self, ])
        KubernetesOps.unexpose_pod_ports(
            self.parent_cluster.k8s_cluster, self.name, namespace=self.parent_cluster.namespace)
        super().destroy()


class MinikubeScyllaPodCluster(ScyllaPodCluster, IptablesClusterOpsMixin):
    k8s_cluster: MinikubeCluster
    PodContainerClass = MinikubeScyllaPodContainer

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[MinikubeScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap)
        for node in new_nodes:
            KubernetesOps.expose_pod_ports(self.k8s_cluster, node.name,
                                           ports=SCYLLA_POD_EXPOSED_PORTS,
                                           labels=f"statefulset.kubernetes.io/pod-name={node.name}",
                                           selector=f"statefulset.kubernetes.io/pod-name={node.name}",
                                           namespace=self.namespace)

        self.add_hydra_iptables_rules(nodes=new_nodes)
        self.update_nodes_iptables_redirect_rules(nodes=new_nodes)

        return new_nodes

    def upgrade_scylla_cluster(self, new_version: str) -> None:
        self.k8s_cluster.docker_pull(f"{self.params.get('docker_image')}:{new_version}")
        return super().upgrade_scylla_cluster(new_version)

    @retrying(n=20, sleep_time=60, allowed_exceptions=(cluster.ClusterNodesNotReady, UnexpectedExit),
              message="Waiting for nodes to join the cluster")
    def wait_for_nodes_up_and_normal(self, nodes, verification_node=None):
        super().wait_for_nodes_up_and_normal(nodes, verification_node)


class MonitorSetMinikube(MonitorSetGCE):
    def install_scylla_manager(self, node, auth_token):
        pass
