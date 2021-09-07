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
import abc
import getpass
import logging
from typing import Tuple, Optional, List, Callable
from textwrap import dedent
from functools import cached_property

from invoke.exceptions import UnexpectedExit

from sdcm import cluster, cluster_gce
from sdcm.cluster import LocalNode
from sdcm.remote import LOCALRUNNER
from sdcm.remote.base import CommandRunner
from sdcm.cluster_k8s import (
    CloudK8sNodePool,
    KubernetesCluster,
    BaseScyllaPodContainer,
    ScyllaPodCluster,
    COMMON_CONTAINERS_RESOURCES,
    OPERATOR_CONTAINERS_RESOURCES,
)
from sdcm.cluster_k8s.iptables import IptablesPodPortsRedirectMixin, IptablesClusterOpsMixin
from sdcm.cluster_gce import MonitorSetGCE
from sdcm.utils.k8s import TokenUpdateThread, HelmValues
from sdcm.utils.common import get_free_port, wait_for_port
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import ContainerManager, docker_hub_login
from sdcm.wait import wait_for

KUBECTL_PROXY_PORT = 8001
KUBECTL_PROXY_CONTAINER = "auto_ssh:kubectl_proxy"
SCYLLA_POD_EXPOSED_PORTS = [3000, 9042, 9180, ]
POOL_LABEL_NAME = 'minimal-k8s-nodepool'

LOGGER = logging.getLogger(__name__)


class MinimalK8SNodePool(CloudK8sNodePool):
    k8s_cluster: 'LocalKindCluster'

    def deploy(self) -> None:
        self.is_deployed = True

    def undeploy(self):
        pass

    def resize(self, num_nodes: int):
        pass

    @cached_property
    def cpu_and_memory_capacity(self) -> Tuple[float, float]:
        return (
            1 + COMMON_CONTAINERS_RESOURCES['cpu'] + OPERATOR_CONTAINERS_RESOURCES['cpu'],
            2.5 + COMMON_CONTAINERS_RESOURCES['memory'] + OPERATOR_CONTAINERS_RESOURCES['memory'],
        )


class MinimalK8SOps:
    @classmethod
    def setup_prerequisites(cls, node: cluster.BaseNode) -> None:
        if node.distro.is_ubuntu or node.distro.is_debian:
            cls.setup_prerequisites_ubuntu(node)
        else:
            raise ValueError(f"{node.distro} is not supported")

    @staticmethod
    def setup_prerequisites_ubuntu(node: cluster.BaseNode) -> None:
        script = dedent("""
            # Make sure that cloud-init finished running.
            # until [ -f /var/lib/cloud/instance/boot-finished ]; do sleep 1; done

            # Disable apt-key warnings and set non-interactive frontend.
            export APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
            export DEBIAN_FRONTEND=noninteractive

            apt-get -qq update
            apt-get -qq install --no-install-recommends apt-transport-https conntrack

            mkdir -p /var/lib/scylla/coredumps

            # Create /etc/ssh/ssh_host_ecdsa_key if it is absent
            ls /etc/ssh/ssh_host_ecdsa_key || ssh-keygen -f /etc/ssh/ssh_host_ecdsa_key -t ecdsa -q -N ''

            cat <<EOF > /etc/sysctl.d/99-sct-minikube.conf
            fs.aio-max-nr=1048576
            net.ipv4.ip_forward=1
            net.ipv4.conf.all.forwarding=1
            kernel.core_pattern=/var/lib/scylla/coredumps/%h-%P-%u-%g-%s-%t.core
            EOF
            sysctl --system
            """)
        node.remoter.sudo(f'bash -cxe "{script}"')

    @classmethod
    def setup_docker(cls, node: cluster.BaseNode, target_user: str = None) -> None:
        if node.distro.is_ubuntu or node.distro.is_debian:
            cls.setup_docker_ubuntu(node, target_user=target_user)
        else:
            raise ValueError(f"{node.distro} is not supported")
        docker_hub_login(remoter=node.remoter)

    @staticmethod
    def setup_docker_ubuntu(node: cluster.BaseNode, target_user: str = None) -> None:
        script = dedent(f"""
            # Install and configure Docker.
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
            add-apt-repository \\"deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable\\"
            apt-get -qq install --no-install-recommends docker-ce docker-ce-cli containerd.io
            {f'usermod -a -G docker {target_user}' if target_user else ''}
            """)
        node.remoter.sudo(f'bash -cxe "{script}"')

    @classmethod
    def setup_kubectl(cls, node: cluster.BaseNode, kubectl_version: str) -> None:
        if node.distro.is_ubuntu or node.distro.is_debian:
            cls.setup_kubectl_ubuntu(node, kubectl_version=kubectl_version)
        else:
            raise ValueError(f"{node.distro} is not supported")

    @staticmethod
    def setup_kubectl_ubuntu(node: cluster.BaseNode, kubectl_version: str) -> None:
        script = dedent(f"""
            # Download kubectl binary.
            curl -fsSLo /usr/local/bin/kubectl \
                https://storage.googleapis.com/kubernetes-release/release/v{kubectl_version}/bin/linux/amd64/kubectl
            chmod +x /usr/local/bin/kubectl
            """)
        node.remoter.sudo(f'bash -cxe "{script}"')

    @staticmethod
    def get_local_kubectl_proxy() -> [str, int]:
        LOGGER.info("Stop any other process listening on kubectl proxy port")
        LOCALRUNNER.sudo(f"fuser -v4k {KUBECTL_PROXY_PORT}/tcp", ignore_status=True)

        LOGGER.info("Start kubectl proxy in detached mode")
        proxy_port = get_free_port(address='127.0.0.1')
        LOCALRUNNER.run(
            f"setsid kubectl proxy --disable-filter --address '127.0.0.1' --port {proxy_port} "
            "--accept-hosts '.*' > proxy.log 2>&1 < /dev/null & sleep 1")

        def get_proxy_ip_port():
            return LOCALRUNNER.run("grep -P '^Starting' proxy.log | grep -oP '127.0.0.1:[0-9]+'").stdout

        ip_port = wait_for(get_proxy_ip_port, timeout=15, throw_exc=True)
        return ip_port.strip().split(':')

    @staticmethod
    def get_kubectl_proxy(node: cluster.BaseNode) -> [str, int]:
        LOGGER.info("Stop any other process listening on kubectl proxy port")
        node.remoter.sudo(f"fuser -v4k {KUBECTL_PROXY_PORT}/tcp", ignore_status=True)

        LOGGER.info("Start kubectl proxy in detached mode")
        node.remoter.run("kill -9 $(ps aux | grep 'kubectl proxy' | awk '{print $2}')", ignore_status=True)
        node.remoter.run(
            "setsid kubectl proxy --disable-filter --accept-hosts '.*' > proxy.log 2>&1 < /dev/null & sleep 1")

        LOGGER.info("Start auto_ssh for kubectl proxy")
        ContainerManager.run_container(node, KUBECTL_PROXY_CONTAINER,
                                       local_port=KUBECTL_PROXY_PORT,
                                       remote_port=get_free_port(),
                                       ssh_mode="-L")

        host = "127.0.0.1"
        port = int(ContainerManager.get_environ(node, KUBECTL_PROXY_CONTAINER)["SSH_TUNNEL_REMOTE"])

        LOGGER.info("Waiting for port %s:%s is accepting connections", host, port)
        wait_for_port(host, port)

        return host, port


class KindK8sMixin:
    docker_pull: Callable
    host_node: 'BaseNode'
    params: dict
    scylla_image: Optional[str]
    software_version: str
    _target_user: str
    _create_kubectl_config_cmd: str = '/var/tmp/kind export kubeconfig'

    @property
    def is_k8s_software_installed(self) -> bool:
        return self.host_node.remoter.run('ls /var/tmp/kind', ignore_status=True).ok

    @property
    def is_k8s_software_running(self) -> bool:
        return self.host_node.remoter.run('/var/tmp/kind get kubeconfig', ignore_status=True).ok

    def setup_k8s_software(self):
        script = dedent(f"""
        # Download kubectl binary.
        curl -fsSLo /var/tmp/kind \
            https://kind.sigs.k8s.io/dl/v{self.software_version}/kind-linux-amd64
        chmod +x /var/tmp/kind
        """)
        self.host_node.remoter.sudo(f'bash -cxe "{script}"')

    def start_k8s_software(self) -> None:
        LOGGER.info("Start Kind cluster")
        script_start_part = f"""
        sysctl fs.protected_regular=0
        ip link set docker0 promisc on
        /var/tmp/kind delete cluster || true
        cat >/tmp/kind.cluster.yaml <<- EndOfSpec
        kind: Cluster
        apiVersion: kind.x-k8s.io/v1alpha4
        # patch the generated kubeadm config with some extra settings
        networking:
          podSubnet: 10.244.0.0/16
          serviceSubnet: 10.96.0.0/16
        kubeadmConfigPatches:
        - |
          apiVersion: kubelet.config.k8s.io/v1beta1
          kind: KubeletConfiguration
          evictionHard:
            nodefs.available: 0%
        nodes:
          - role: control-plane
          - role: worker
            labels:
              {POOL_LABEL_NAME}: {self.AUXILIARY_POOL_NAME}
          - role: worker
            labels:
              {POOL_LABEL_NAME}: {self.AUXILIARY_POOL_NAME}
        """
        scylla_node_definition = f"""
          - role: worker
            labels:
              {POOL_LABEL_NAME}: {self.SCYLLA_POOL_NAME}
        """
        for _ in range(self.params.get("n_db_nodes") + 1):
            script_start_part += scylla_node_definition
        script_end_part = """
        EndOfSpec
        /var/tmp/kind delete cluster || true
        /var/tmp/kind create cluster --config /tmp/kind.cluster.yaml
        SERVICE_GATEWAY=`docker inspect kind-control-plane \
            -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}'`
        ip ro add 10.96.0.0/16 via $SERVICE_GATEWAY || ip ro change 10.96.0.0/16 via $SERVICE_GATEWAY
        ip ro add 10.224.0.0/16 via $SERVICE_GATEWAY || ip ro change 10.224.0.0/16 via $SERVICE_GATEWAY
        """
        script = dedent(script_start_part + script_end_part)
        self.host_node.remoter.run(f"sudo -E bash -cxe '{script}'")

    def stop_k8s_software(self):
        self.host_node.remoter.run('/var/tmp/kind delete cluster', ignore_status=True)

    def on_deploy_completed(self):
        if self.scylla_image:
            self.docker_pull(self.scylla_image)
            self.host_node.remoter.run(f"/var/tmp/kind load docker-image {self.scylla_image}", ignore_status=True)


class MinikubeK8sMixin:
    docker_pull: Callable
    host_node: 'BaseNode'
    params: dict
    scylla_image: Optional[str]
    software_version: str
    _target_user: str
    _create_kubectl_config_cmd = '/var/tmp/minikube update-context'

    @property
    def is_k8s_software_installed(self) -> bool:
        return self.host_node.remoter.run('ls /var/tmp/minikube', ignore_status=True).ok

    @property
    def is_k8s_software_running(self) -> bool:
        return self.host_node.remoter.run('/var/tmp/minikube status', ignore_status=True).ok

    def setup_k8s_software(self):
        script = dedent(f"""
            # Download Minikube binary.
            curl -fsSLo /var/tmp/minikube \
                https://storage.googleapis.com/minikube/releases/v{self.software_version}/minikube-linux-amd64
            chmod +x /var/tmp/minikube
        """)
        self.host_node.remoter.sudo(f'bash -cxe "{script}"')

    def start_k8s_software(self):
        LOGGER.info("Start Minikube cluster")
        target_user = self._target_user if self._target_user else 'root'
        script = dedent(f"""
            sysctl fs.protected_regular=0
            ip link set docker0 promisc on
            HOME=/root
            [ -z "$KUBECONFIG" ] && export KUBECONFIG=~{target_user}/.kube/config
            export MINIKUBE_HOME=$(dirname $(realpath -m $KUBECONFIG))/.kube/.minikube
            # export MINIKUBE_HOME=~{target_user}/.kube/.minikube
            rm -rf /root/.minikube; ln -s $MINIKUBE_HOME /root/.minikube || true
            {f'rm -rf ~{target_user}/.minikube; '
             f'ln -s $MINIKUBE_HOME ~{target_user}/.minikube || true' if target_user != 'root' else ''}
            /var/tmp/minikube delete || true
            /var/tmp/minikube start --driver=none --extra-config=apiserver.service-node-port-range=1-65535
            chmod 777 -R $MINIKUBE_HOME
        """)
        self.host_node.remoter.run(f"sudo -E bash -cxe '{script}'")

    def stop_k8s_software(self):
        self.host_node.remoter.run('/var/tmp/minikube delete', ignore_status=True)

    def on_deploy_completed(self):
        if self.scylla_image:
            self.docker_pull(self.scylla_image)
            self.host_node.remoter.run(f'/var/tmp/minikube image load {self.scylla_image}', ignore_status=True)


class MinimalClusterBase(KubernetesCluster, metaclass=abc.ABCMeta):  # pylint: disable=too-many-public-methods
    POOL_LABEL_NAME = POOL_LABEL_NAME

    # pylint: disable=too-many-arguments
    def __init__(self, mini_k8s_version, params: dict, user_prefix: str = '', region_name: str = None,
                 cluster_uuid: str = None, **_):
        self.software_version = mini_k8s_version
        super().__init__(params=params, user_prefix=user_prefix, region_name=region_name, cluster_uuid=cluster_uuid)

    @property
    def is_kubectl_installed(self) -> None:
        return self.host_node.remoter.run('ls /usr/local/bin/kubectl || ls /usr/bin/kubectl', ignore_status=True).ok

    @property
    def is_docker_installed(self) -> None:
        return self.host_node.remoter.run('ls /usr/local/bin/docker || ls /usr/bin/docker', ignore_status=True).ok

    def setup_prerequisites(self):
        LOGGER.info("Install prerequisites to %s", self.host_node)
        MinimalK8SOps.setup_prerequisites(node=self.host_node)

    def setup_docker(self):
        LOGGER.info("Install docker to %s", self.host_node)
        MinimalK8SOps.setup_docker(node=self.host_node, target_user=self._target_user)

    def setup_kubectl(self):
        LOGGER.info("Install kubectl to %s", self.host_node)
        MinimalK8SOps.setup_kubectl(node=self.host_node, kubectl_version=self.local_kubectl_version)

    def get_scylla_cluster_helm_values(self, cpu_limit, memory_limit, pool_name: str = None) -> HelmValues:
        values = super().get_scylla_cluster_helm_values(cpu_limit, memory_limit, pool_name)
        values.delete('racks.[0].storage.storageClassName')
        values.set('cpuset', False)
        values.set('developerMode', False)
        values.set('hostNetworking', False)
        return values

    def create_kubectl_config(self):
        """
        Kubectl config is being created when minikube cluster is started
        """
        LOGGER.info("Creating kubectl config")
        if self._target_user:
            self.host_node.remoter.run(self._create_kubectl_config_cmd)
            # Create config for target user in it's default place
            self.host_node.remoter.run(f'KUBECONFIG="" {self._create_kubectl_config_cmd}')
        self.host_node.remoter.sudo(f'bash -cxe " KUBECONFIG=\'\' {self._create_kubectl_config_cmd}"')

    def create_token_update_thread(self) -> TokenUpdateThread:
        """
        No token update thread required
        """

    @property
    def software_version(self):
        try:
            return self._mini_k8s_version
        except AttributeError:
            raise ValueError("You should set `software_version' first.") from None

    @software_version.setter
    def software_version(self, value):
        self._mini_k8s_version = value

    @cached_property
    def local_kubectl_version(self):  # pylint: disable=no-self-use
        # Example of kubectl command output:
        #   $ kubectl version --client --short
        #   Client Version: v1.18.5
        return LOCALRUNNER.run("kubectl version --client --short").stdout.rsplit(None, 1)[-1][1:]

    def docker_pull(self, image):
        LOGGER.info("Pull `%s' to docker environment", image)
        self.remoter.run(f"docker pull -q {image}")

    @property
    def remoter(self) -> CommandRunner:
        return self.host_node.remoter

    def deploy_scylla_manager(self, pool_name: str = None) -> None:
        self.deploy_minio_s3_backend()
        super().deploy_scylla_manager(pool_name=pool_name)

    @property
    @abc.abstractmethod
    def _create_kubectl_config_cmd(self):
        pass

    @property
    @abc.abstractmethod
    def _target_user(self) -> Optional[str]:
        pass

    @property
    @abc.abstractmethod
    def k8s_server_url(self):
        pass

    @property
    @abc.abstractmethod
    def host_node(self) -> 'BaseNode':
        """
        Host where minikube is running
        """

    @property
    @abc.abstractmethod
    def is_k8s_software_installed(self) -> bool:
        """
        Is kind/k3d/minikube installed ?
        """

    @property
    @abc.abstractmethod
    def is_k8s_software_running(self) -> bool:
        """
        Is kind/k3d/minikube running ?
        """

    @property
    @abc.abstractmethod
    def setup_k8s_software(self):
        """
        install kind/k3d/minikube
        """

    @property
    @abc.abstractmethod
    def start_k8s_software(self):
        """
        Start kind/k3d/minikube
        """

    @property
    @abc.abstractmethod
    def stop_k8s_software(self):
        """
        Stop kind/k3d/minikube
        """

    def on_deploy_completed(self):
        """
        Hook that is executed just before completing deployment
        """

    @property
    def scylla_image(self):
        docker_repo = self.params.get('docker_image')
        scylla_version = self.params.get('scylla_version')
        if not scylla_version or not docker_repo:
            return ""
        return f"{docker_repo}:{scylla_version}"

    def deploy(self):
        self.setup_prerequisites()
        if not self.is_docker_installed:
            self.setup_docker()
        if not self.is_kubectl_installed:
            self.setup_kubectl()
        if not self.is_k8s_software_installed:
            self.setup_k8s_software()
        if not self.is_k8s_software_running:
            if self.test_config.REUSE_CLUSTER:
                raise RuntimeError("SCT_REUSE_CLUSTER is set, but target host is not ready")
            self.start_k8s_software()
        elif not self.test_config.REUSE_CLUSTER:
            self.stop_k8s_software()
            self.start_k8s_software()
        self.create_kubectl_config()
        self.on_deploy_completed()


class LocalMinimalClusterBase(MinimalClusterBase):
    """
    This class represents minimal (minikube, k3d or kind) k8s cluster running locally
    """

    def __init__(self, software_version, user_prefix=None, params=None):
        self.software_version = software_version
        self.node_prefix = cluster.prepend_user_prefix(user_prefix, "node")
        super().__init__(
            mini_k8s_version=software_version,
            user_prefix=user_prefix,
            params=params)

    # pylint: disable=invalid-overridden-method
    @cached_property
    def host_node(self):
        node = LocalNode(
            name=f"{self.node_prefix}-1",
            parent_cluster=self,
            base_logdir=self.logdir,
            dc_idx=0,
            rack=1
        )
        node.init()
        return node

    @cached_property
    def k8s_server_url(self):
        host, port = MinimalK8SOps.get_local_kubectl_proxy()
        return f"http://{host}:{port}"

    def deploy_node_pool(self, pool, wait_till_ready=True) -> None:
        pass

    @property
    @abc.abstractmethod
    def _create_kubectl_config_cmd(self):
        pass

    @property
    def _target_user(self) -> str:
        return getpass.getuser()

    def upgrade_kubernetes_platform(self):
        pass


class LocalMinikubeCluster(MinikubeK8sMixin, LocalMinimalClusterBase):
    pass


class LocalKindCluster(KindK8sMixin, LocalMinimalClusterBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allowed_labels_on_scylla_node = [
            ('k8s-app', 'kindnet'),
            ('k8s-app', 'kube-proxy'),
            ('scylla/cluster', self.k8s_scylla_cluster_name),
        ]


class RemoteMinimalClusterBase(MinimalClusterBase, metaclass=abc.ABCMeta):
    # pylint: disable=invalid-overridden-method
    @cached_property
    def host_node(self):
        return self.nodes[-1]

    # pylint: disable=invalid-overridden-method
    @cached_property
    def k8s_server_url(self):
        host, port = MinimalK8SOps.get_kubectl_proxy(self.host_node)
        return f"http://{host}:{port}"

    @cached_property
    def hydra_dest_ip(self) -> Optional[str]:
        return self.host_node.external_address

    @cached_property
    def nodes_dest_ip(self) -> Optional[str]:
        return self.host_node.ip_address

    def deploy(self):
        pass

    def create_kubectl_config(self):
        pass

    def create_token_update_thread(self):
        pass

    def deploy_node_pool(self, pool, wait_till_ready=True) -> None:
        raise NotImplementedError("Not supported on minimal k8s")


class GceMinikubeCluster(MinikubeK8sMixin, RemoteMinimalClusterBase, cluster_gce.GCECluster):
    def __init__(self, mini_k8s_version, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,  # pylint: disable=too-many-arguments
                 gce_instance_type="n1-highmem-8", gce_image_username="centos", user_prefix=None, params=None,
                 gce_datacenter=None):
        # pylint: disable=too-many-locals
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "k8s-minikube")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "node")
        # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
        super().__init__(mini_k8s_version=mini_k8s_version,
                         gce_image=gce_image,
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

    # pylint: disable=unused-argument,arguments-differ
    @cluster.wait_for_init_wrap
    def wait_for_init(self, *_, node_list=None, verbose=False, timeout=None, **__):
        for node in self.nodes:
            # Reconnect to update user groups in main thread too.
            node.remoter._reconnect()  # pylint: disable=protected-access
        super().wait_for_init()

    def destroy(self) -> None:
        super().destroy()
        self.stop_k8s_task_threads()

    def deploy_node_pool(self, pool, wait_till_ready=True) -> None:
        pass

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Not implemented yet")

    def get_node_ips_param(self, public_ip=True):
        pass


class LocalMinimalScyllaPodContainer(BaseScyllaPodContainer):  # pylint: disable=abstract-method
    public_ip_via_service: bool = False
    parent_cluster: 'LocalMinimalScyllaPodCluster'

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


# pylint: disable=too-many-ancestors
class RemoteMinimalScyllaPodContainer(LocalMinimalScyllaPodContainer, IptablesPodPortsRedirectMixin):  # pylint: disable=abstract-method
    parent_cluster: 'RemoteMinimalScyllaPodCluster'
    public_ip_via_service: bool = False

    @cached_property
    def hydra_dest_ip(self):
        return self.parent_cluster.k8s_cluster.hydra_dest_ip

    @cached_property
    def nodes_dest_ip(self):
        return self.parent_cluster.k8s_cluster.nodes_dest_ip


# pylint: disable=too-many-ancestors
class LocalMinimalScyllaPodCluster(ScyllaPodCluster):
    """
    This class represents scylla cluster hosted on locally running minimal k8s clusters, such as: k3d, minikube or kind
    """
    k8s_cluster: MinimalClusterBase
    PodContainerClass = LocalMinimalScyllaPodContainer

    @cached_property
    def node_terminate_methods(self) -> List[str]:
        if isinstance(self.k8s_cluster, MinikubeK8sMixin):
            return []
        elif isinstance(self.k8s_cluster, KindK8sMixin):
            return [
                'drain_k8s_node',
                # NOTE: enable below methods when it's support fully implemented
                # https://trello.com/c/LrAObHPC/3119-fix-gce-node-termination-nemesis-on-k8s
                # https://github.com/scylladb/scylla-operator/issues/524
                # https://github.com/scylladb/scylla-operator/issues/507
                # 'terminate_k8s_host',
                # 'terminate_k8s_node',
            ]
        return []

    @retrying(n=20, sleep_time=60, allowed_exceptions=(cluster.ClusterNodesNotReady, UnexpectedExit),
              message="Waiting for nodes to join the cluster")
    def wait_for_nodes_up_and_normal(self, nodes=None, verification_node=None):
        super().wait_for_nodes_up_and_normal(nodes, verification_node)

    @cluster.wait_for_init_wrap
    def wait_for_init(self, *_, node_list=None, verbose=False, timeout=None, **__):  # pylint: disable=unused-argument
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)

    def upgrade_scylla_cluster(self, new_version: str) -> None:
        self.k8s_cluster.docker_pull(f"{self.params.get('docker_image')}:{new_version}")
        return super().upgrade_scylla_cluster(new_version)

    @staticmethod
    def fstrim_scylla_disks():
        LOGGER.warning("'k8s-gce-minikube' doesn't support running 'fstrim' command. Ignoring.")


# pylint: disable=too-many-ancestors
class RemoteMinimalScyllaPodCluster(LocalMinimalScyllaPodCluster, IptablesClusterOpsMixin):
    """
    This class represents scylla cluster hosted on remotely running minimal k8s clusters, such as: k3d, minikube or kind
    """
    PodContainerClass = RemoteMinimalScyllaPodContainer

    # pylint: disable=too-many-arguments
    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[RemoteMinimalScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap)
        self.add_hydra_iptables_rules(nodes=new_nodes)
        self.update_nodes_iptables_redirect_rules(nodes=new_nodes)

        return new_nodes

    def terminate_node(self, node: RemoteMinimalScyllaPodContainer, scylla_shards=""):
        super().terminate_node(node=node, scylla_shards=scylla_shards)
        self.update_nodes_iptables_redirect_rules(command="D", nodes=[self, ])

    @retrying(n=20, sleep_time=60, allowed_exceptions=(cluster.ClusterNodesNotReady, UnexpectedExit),
              message="Waiting for nodes to join the cluster")
    def wait_for_nodes_up_and_normal(self, nodes=None, verification_node=None):
        super().wait_for_nodes_up_and_normal(nodes, verification_node)

    @cached_property
    def hydra_dest_ip(self) -> Optional[str]:
        return self.nodes[-1].external_address

    @cached_property
    def nodes_dest_ip(self) -> Optional[str]:
        return self.nodes[-1].ip_address


class MonitorSetMinikube(MonitorSetGCE):
    def install_scylla_manager(self, node):
        pass
