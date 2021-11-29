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

from sdcm import cluster
from sdcm.cluster import LocalK8SHostNode
from sdcm.remote import LOCALRUNNER
from sdcm.remote.base import CommandRunner
from sdcm.cluster_k8s import (
    CloudK8sNodePool,
    KubernetesCluster,
    BaseScyllaPodContainer,
    ScyllaPodCluster,
    COMMON_CONTAINERS_RESOURCES,
    OPERATOR_CONTAINERS_RESOURCES,
    SCYLLA_MANAGER_AGENT_RESOURCES,
)
from sdcm.utils.k8s import TokenUpdateThread, HelmValues
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import docker_hub_login


LOGGER = logging.getLogger(__name__)
POOL_LABEL_NAME = 'minimal-k8s-nodepool'


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
            1 + COMMON_CONTAINERS_RESOURCES['cpu']
            + OPERATOR_CONTAINERS_RESOURCES['cpu']
            + SCYLLA_MANAGER_AGENT_RESOURCES['cpu'],
            2.5 + COMMON_CONTAINERS_RESOURCES['memory']
            + OPERATOR_CONTAINERS_RESOURCES['memory']
            + SCYLLA_MANAGER_AGENT_RESOURCES['memory'],
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

            cat <<EOF > /etc/sysctl.d/99-sct-local-k8s.conf
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
        """Kubectl config gets created when K8S cluster is started"""
        LOGGER.info("Creating kubectl config")
        if self._target_user:
            self.host_node.remoter.run(self._create_kubectl_config_cmd)
            # Create config for target user in it's default place
            self.host_node.remoter.run(f'KUBECONFIG="" {self._create_kubectl_config_cmd}')
        self.host_node.remoter.sudo(f'bash -cxe " KUBECONFIG=\'\' {self._create_kubectl_config_cmd}"')

    def create_token_update_thread(self) -> TokenUpdateThread:
        """No token update thread required"""

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
    def host_node(self) -> 'BaseNode':
        """
        Host where kind/k3d/minikube is running
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

    @abc.abstractmethod
    def setup_k8s_software(self):
        """
        install kind/k3d/minikube
        """

    @abc.abstractmethod
    def start_k8s_software(self):
        """
        Start kind/k3d/minikube
        """

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
    """Represents minimal (minikube, k3d or kind) k8s cluster running locally"""

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
        node = LocalK8SHostNode(
            name=f"{self.node_prefix}-1",
            parent_cluster=self,
            base_logdir=self.logdir,
            dc_idx=0,
            rack=1
        )
        node.init()
        return node

    def deploy_node_pool(self, pool, wait_till_ready=True) -> None:
        self._add_pool(pool)

    @property
    @abc.abstractmethod
    def _create_kubectl_config_cmd(self):
        pass

    @property
    def _target_user(self) -> str:
        return getpass.getuser()

    def upgrade_kubernetes_platform(self):
        return ""


class LocalKindCluster(LocalMinimalClusterBase):
    docker_pull: Callable
    host_node: 'BaseNode'
    scylla_image: Optional[str]
    software_version: str
    _target_user: str
    _create_kubectl_config_cmd: str = '/var/tmp/kind export kubeconfig'

    @cached_property
    def allowed_labels_on_scylla_node(self) -> list:
        return [
            ('k8s-app', 'kindnet'),
            ('k8s-app', 'kube-proxy'),
            ('scylla/cluster', self.k8s_scylla_cluster_name),
        ]

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
        images_to_cache = []
        if self.scylla_image:
            images_to_cache.append(self.scylla_image)
        if self.params.get("use_mgmt") and self.params.get("mgmt_docker_image"):
            images_to_cache.append(self.params.get("mgmt_docker_image"))
        if self.params.get("scylla_mgmt_agent_version"):
            images_to_cache.append(
                "scylladb/scylla-manager-agent:" + self.params.get("scylla_mgmt_agent_version"))

        try:
            images_to_cache.append(self.get_operator_image())
        except ValueError as exc:
            LOGGER.warning("scylla-operator image won't be cached. Error: %s", str(exc))

        for image in images_to_cache:
            self.docker_pull(image)
            self.host_node.remoter.run(
                f"/var/tmp/kind load docker-image {image}", ignore_status=True)


class LocalMinimalScyllaPodContainer(BaseScyllaPodContainer):
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

    def terminate_k8s_host(self):
        raise NotImplementedError("Not supported on local K8S backends")


# pylint: disable=too-many-ancestors
class LocalMinimalScyllaPodCluster(ScyllaPodCluster):
    """Represents scylla cluster hosted on locally running minimal k8s clusters such as k3d, minikube or kind"""
    k8s_cluster: MinimalClusterBase
    PodContainerClass = LocalMinimalScyllaPodContainer

    @cached_property
    def node_terminate_methods(self) -> List[str]:
        if isinstance(self.k8s_cluster, LocalKindCluster):
            # NOTE: local K8S installation supports only 'drain_k8s_node' method.
            #       'terminate_k8s_host' and 'terminate_k8s_node' are not applicable to it.
            return ['drain_k8s_node']
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
        LOGGER.warning("Local K8S backends don't support running 'fstrim' command. Ignoring.")
