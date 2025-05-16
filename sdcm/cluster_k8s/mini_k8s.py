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
import os
import re
from typing import Tuple, Optional, Callable
from textwrap import dedent
from functools import cached_property
import yaml

from invoke.exceptions import UnexpectedExit

from sdcm import (
    cluster,
    sct_abs_path,
)
from sdcm.cluster import LocalK8SHostNode
from sdcm.remote import LOCALRUNNER
from sdcm.remote.base import CommandRunner
from sdcm.cluster_k8s import (
    CloudK8sNodePool,
    KubernetesCluster,
    BaseScyllaPodContainer,
    ScyllaPodCluster,
    COMMON_CONTAINERS_RESOURCES,
    INGRESS_CONTROLLER_CONFIG_PATH,
    K8S_LOCAL_VOLUME_PROVISIONER_VERSION,
    LOCAL_MINIO_DIR,
    LOCAL_PROVISIONER_FILE,
    OPERATOR_CONTAINERS_RESOURCES,
    SCYLLA_MANAGER_AGENT_RESOURCES,
    SCYLLA_MANAGER_AGENT_VERSION_IN_SCYLLA_MANAGER,
)
from sdcm.utils.k8s import TokenUpdateThread, HelmValues
from sdcm.utils.k8s.chaos_mesh import ChaosMesh
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import docker_hub_login
from sdcm.utils import version_utils


SRC_APISERVER_AUDIT_POLICY = sct_abs_path("sdcm/k8s_configs/local-kind/audit-policy.yaml")
DST_APISERVER_AUDIT_POLICY = "/etc/kubernetes/policies/audit-policy.yaml"
DST_APISERVER_AUDIT_LOG = "/var/log/kubernetes/kube-apiserver-audit.log"

CNI_CALICO_CONFIG = sct_abs_path("sdcm/k8s_configs/cni-calico.yaml")
CNI_CALICO_VERSION = "v3.24.5"
HELM_VERSION = "v3.12.1"
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
        cpu_per_member = 1
        # NOTE: Setting '1' to 'memory_for_cpu_multiplier' we will get failure incresing CPUs
        #       Setting '2' to 'memory_for_cpu_multiplier' we will be able to add 1 CPU per member
        #       And so on... Useful for tests with change of CPU for Scylla pods.
        memory_for_cpu_multiplier = 2
        memory_for_cpu = memory_for_cpu_multiplier * cpu_per_member
        memory_base = 1.5
        return (
            cpu_per_member
            + COMMON_CONTAINERS_RESOURCES['cpu']
            + OPERATOR_CONTAINERS_RESOURCES['cpu']
            + SCYLLA_MANAGER_AGENT_RESOURCES['cpu'],
            memory_base + memory_for_cpu
            + COMMON_CONTAINERS_RESOURCES['memory']
            + OPERATOR_CONTAINERS_RESOURCES['memory']
            + SCYLLA_MANAGER_AGENT_RESOURCES['memory'],
        )


class MinimalK8SOps:
    @classmethod
    def setup_prerequisites(cls, node: cluster.BaseNode) -> None:
        if node.distro.is_debian_like:
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
            fs.inotify.max_user_instances=8192
            net.ipv4.ip_forward=1
            net.ipv4.conf.all.forwarding=1
            kernel.core_pattern=/var/lib/scylla/coredumps/%h-%P-%u-%g-%s-%t.core
            EOF
            sysctl --system
            """)
        node.remoter.sudo(f'bash -cxe "{script}"')
        node.remoter.sudo(
            'bash -cxe \"helm version'
            f' || (curl --silent --location "https://get.helm.sh/helm-{HELM_VERSION}-linux-amd64.tar.gz"'
            '  | tar xz -C /tmp && mv /tmp/linux-amd64/helm /usr/local/bin)'
            '\"')

        # NOTE: if running in Hydra then it must have '/dev' mount from host as 'rw'
        for i in range(7, 41):
            cmd = f"if ! [ -e /dev/loop{i} ]; then mknod -m 660 /dev/loop{i} b 7 {i}; fi"
            node.remoter.sudo(f'bash -cxe "{cmd}"')

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


class MinimalClusterBase(KubernetesCluster, metaclass=abc.ABCMeta):
    POOL_LABEL_NAME = POOL_LABEL_NAME

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
        self.log.info("Install prerequisites to %s", self.host_node)
        MinimalK8SOps.setup_prerequisites(node=self.host_node)

    def setup_docker(self):
        self.log.info("Install docker to %s", self.host_node)
        MinimalK8SOps.setup_docker(node=self.host_node, target_user=self._target_user)

    def setup_kubectl(self):
        self.log.info("Install kubectl to %s", self.host_node)
        MinimalK8SOps.setup_kubectl(node=self.host_node, kubectl_version=self.local_kubectl_version)

    def get_scylla_cluster_helm_values(self, cpu_limit, memory_limit, pool_name: str = None,
                                       cluster_name: str = None) -> HelmValues:
        values = super().get_scylla_cluster_helm_values(
            cpu_limit=cpu_limit, memory_limit=memory_limit,
            pool_name=pool_name, cluster_name=cluster_name)
        values.set('cpuset', False)
        values.set('developerMode', False)
        values.set('hostNetworking', False)
        return values

    def create_kubectl_config(self):
        """Kubectl config gets created when K8S cluster is started"""
        self.log.info("Creating kubectl config")
        default_kube_config_dir_path = os.path.expanduser("~/.kube")
        kube_paths = {self.kube_config_dir_path, default_kube_config_dir_path}
        for kube_path in kube_paths:
            if not os.path.exists(kube_path):
                os.makedirs(kube_path, exist_ok=True)
            if self._target_user:
                self.host_node.remoter.sudo(
                    f'bash -cxe "chown -R {self._target_user}:{self._target_user} {kube_path}"')
        # NOTE: export KinD's cluster kubeconfig to 2 places for the following reasons:
        #       - The non-default path will be used only by test.
        #       - The default path may be used by user anytime for any K8S cluster
        #         including the KinD one in parallel to a test run.
        #       As a result a user won't need to bother himself about
        #       the 'current context' running KinD tests locally.
        self.host_node.remoter.run(f'KUBECONFIG="{self.kube_config_path}" {self._create_kubectl_config_cmd}')
        self.host_node.remoter.run(f'KUBECONFIG="" {self._create_kubectl_config_cmd}')

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
    def local_kubectl_version(self):
        # Example of kubectl command output:
        #   $ kubectl version --client --short
        #   Client Version: v1.18.5
        return LOCALRUNNER.run("kubectl version --client --short").stdout.rsplit(None, 1)[-1][1:]

    def docker_pull(self, image):
        self.log.info("Pull `%s' to docker environment", image)
        self.remoter.run(f"docker pull -q {image}")

    def docker_tag(self, src, dst):
        self.log.info("Retag `%s' image as '%s'", src, dst)
        self.remoter.run(f"docker tag {src} {dst}")

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
    def host_node(self) -> 'BaseNode':  # noqa: F821
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

    @cached_property
    def minio_images(self):
        with open(LOCAL_MINIO_DIR + '/values.yaml', mode='r', encoding='utf8') as minio_config_stream:
            minio_config = yaml.safe_load(minio_config_stream)
            return [
                f"{minio_config['image']['repository']}:{minio_config['image']['tag']}",
                f"{minio_config['mcImage']['repository']}:{minio_config['mcImage']['tag']}",
            ]

    @cached_property
    def static_local_volume_provisioner_image(self):
        with open(LOCAL_PROVISIONER_FILE, mode='r', encoding='utf8') as provisioner_config_stream:
            for doc in yaml.safe_load_all(provisioner_config_stream):
                if doc["kind"] != "DaemonSet":
                    continue
                try:
                    return doc["spec"]["template"]["spec"]["containers"][0]["image"]
                except Exception as exc:  # noqa: BLE001
                    self.log.warning(
                        "Could not read the static local volume provisioner image: %s", exc)
        return ""

    @cached_property
    def dynamic_local_volume_provisioner_image(self):
        return f"scylladb/k8s-local-volume-provisioner:{K8S_LOCAL_VOLUME_PROVISIONER_VERSION}"

    @cached_property
    def cert_manager_images(self):
        base_repo, tag = "quay.io/jetstack", f"v{self.params.get('k8s_cert_manager_version')}"
        return [
            f"{base_repo}/cert-manager-controller:{tag}",
            f"{base_repo}/cert-manager-cainjector:{tag}",
            f"{base_repo}/cert-manager-webhook:{tag}",
        ]

    @cached_property
    def ingress_controller_images(self):
        ingress_images = set()
        for root, _, subfiles in os.walk(INGRESS_CONTROLLER_CONFIG_PATH):
            for subfile in subfiles:
                if not subfile.endswith('yaml'):
                    continue
                with open(os.path.join(root, subfile), mode='r', encoding='utf8') as file_stream:
                    for doc in yaml.safe_load_all(file_stream):
                        if doc["kind"] != "Deployment":
                            continue
                        for container in doc["spec"]["template"]["spec"]["containers"]:
                            try:
                                ingress_images.add(container["image"])
                            except Exception as exc:  # noqa: BLE001
                                self.log.warning(
                                    "Could not read the ingress controller related image: %s", exc)
        return ingress_images

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
        self.start_scylla_pods_ip_change_tracker_thread()
        if self.test_config.REUSE_CLUSTER:
            return
        self.on_deploy_completed()


class LocalMinimalClusterBase(MinimalClusterBase):
    """Represents minimal (minikube, k3d or kind) k8s cluster running locally"""

    def __init__(self, software_version, user_prefix=None, params=None):
        self.software_version = software_version
        self.node_prefix = cluster.prepend_user_prefix(user_prefix, "node")
        super().__init__(
            mini_k8s_version=software_version,
            user_prefix=user_prefix,
            region_name="local-dc-1",
            params=params)

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

    def upgrade_kubernetes_platform(self, pod_objects: list[cluster.BaseNode],
                                    use_additional_scylla_nodepool: bool) -> (str, CloudK8sNodePool):
        return ""


class LocalKindCluster(LocalMinimalClusterBase):
    docker_pull: Callable
    docker_tag: Callable
    host_node: 'BaseNode'  # noqa: F821
    scylla_image: Optional[str]
    software_version: str
    _target_user: str
    _create_kubectl_config_cmd: str = '/var/tmp/kind export kubeconfig'

    @cached_property
    def allowed_labels_on_scylla_node(self) -> list:
        allowed_labels_on_scylla_node = [
            ('k8s-app', 'kindnet'),
            ('k8s-app', 'kube-proxy'),
            ('k8s-app', 'calico-node'),
            ('app', 'static-local-volume-provisioner'),
            ('scylla/cluster', self.k8s_scylla_cluster_name),
        ]
        if self.params.get('k8s_use_chaos_mesh'):
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/component', 'chaos-daemon'))
        if self.params.get("k8s_local_volume_provisioner_type") != 'static':
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/name', 'local-csi-driver'))
        return allowed_labels_on_scylla_node

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
        self.log.info("Start Kind cluster")
        audit_log_path_option = ""
        if self.params.get("k8s_log_api_calls"):
            audit_log_path_option = f"audit-log-path: {DST_APISERVER_AUDIT_LOG}"
        pod_subnet, service_subnet = '10.16.0.0/16', '10.19.0.0/16'
        script_start_part = f"""
        sysctl fs.protected_regular=0
        ip link set docker0 promisc on
        /var/tmp/kind delete cluster || true
        cat >/tmp/kind.cluster.yaml <<- EndOfSpec
        kind: Cluster
        apiVersion: kind.x-k8s.io/v1alpha4
        networking:
          podSubnet: {pod_subnet}
          serviceSubnet: {service_subnet}
          disableDefaultCNI: true
        kubeadmConfigPatches:
        - |
          apiVersion: kubelet.config.k8s.io/v1beta1
          kind: KubeletConfiguration
          evictionHard:
            nodefs.available: 0%
        nodes:
          - role: control-plane
            kubeadmConfigPatches:
            - |
              kind: ClusterConfiguration
              apiServer:
                extraArgs:
                  event-ttl: 24h
                  {audit_log_path_option}
                  audit-log-maxsize: "100"
                  audit-policy-file: {DST_APISERVER_AUDIT_POLICY}
                extraVolumes:
                  - name: audit-policies
                    hostPath: /etc/kubernetes/policies
                    mountPath: /etc/kubernetes/policies
                    readOnly: true
                    pathType: "DirectoryOrCreate"
                  - name: "audit-logs"
                    hostPath: "/var/log/kubernetes"
                    mountPath: "/var/log/kubernetes"
                    readOnly: false
                    pathType: DirectoryOrCreate
            extraMounts:
            - hostPath: {SRC_APISERVER_AUDIT_POLICY}
              containerPath: {DST_APISERVER_AUDIT_POLICY}
              readOnly: true

        """
        for node_pool_type, node_num in (
                (self.AUXILIARY_POOL_NAME, self.params.get("k8s_n_auxiliary_nodes") or 2),
                (self.SCYLLA_POOL_NAME, self.params.get("n_db_nodes")),
                (self.LOADER_POOL_NAME, self.params.get("n_loaders")),
                (self.MONITORING_POOL_NAME, self.params.get("k8s_n_monitor_nodes"))):
            for _ in range(node_num):
                script_start_part += f"""
          - role: worker
            labels:
              {POOL_LABEL_NAME}: {node_pool_type}
                """
        script_end_part = f"""
        EndOfSpec
        /var/tmp/kind delete cluster || true
        /var/tmp/kind create cluster --config /tmp/kind.cluster.yaml
        SERVICE_GATEWAY=`docker inspect kind-control-plane \
            -f '{{{{range.NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}'`
        ip ro add {service_subnet} via $SERVICE_GATEWAY || ip ro change {service_subnet} via $SERVICE_GATEWAY
        """
        script = dedent(script_start_part + script_end_part)
        self.host_node.remoter.run(f"sudo -E bash -cxe '{script}'")

    def setup_pod_network_connectivity(self):
        target_route_regex = re.compile(
            r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\/\d{1,2} via \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} .*")
        route_lines = self.host_node.remoter.run(
            "docker exec kind-control-plane /bin/bash -c 'ip r'").stdout.split("\n")
        for route_line in route_lines:
            if not target_route_regex.match(route_line):
                continue
            route_parts = route_line.split()
            route_cmd = "ip ro add {0} via {1} || ip ro change {0} via {1}".format(route_parts[0], route_parts[2])
            self.host_node.remoter.run(f"sudo bash -c '{route_cmd}'")

    def stop_k8s_software(self):
        self.host_node.remoter.run('/var/tmp/kind delete cluster', ignore_status=True)

    def load_images(self, images_list: [str]):
        for image in images_list:
            self.docker_pull(image)
            self.host_node.remoter.run(
                f"/var/tmp/kind load docker-image {image}", ignore_status=True)

    def on_deploy_completed(self):
        images_to_cache, images_to_retag, new_scylla_image_tag = [], {}, ""

        # first setup CNI plugin, otherwise everything else might get broken
        cni_images_to_cache = []
        for image_repo in ('kube-controllers', 'cni', 'node'):
            cni_images_to_cache.append(f"calico/{image_repo}:{CNI_CALICO_VERSION}")

        if not self.params.get('reuse_cluster'):
            self.load_images(cni_images_to_cache)

        self.apply_file(CNI_CALICO_CONFIG, environ={
            "SCT_K8S_CNI_CALICO_VERSION": CNI_CALICO_VERSION,
        })

        images_to_cache.extend(self.cert_manager_images)
        if self.params.get("k8s_local_volume_provisioner_type") != 'static':
            images_to_cache.append(self.dynamic_local_volume_provisioner_image)
        elif provisioner_image := self.static_local_volume_provisioner_image:
            images_to_cache.append(provisioner_image)
        if self.params.get("k8s_use_chaos_mesh"):
            chaos_mesh_version = ChaosMesh.VERSION
            if not chaos_mesh_version.startswith("v"):
                chaos_mesh_version = f"v{chaos_mesh_version}"
            for image_suffix in ("daemon", "mesh"):
                images_to_cache.append(f"ghcr.io/chaos-mesh/chaos-{image_suffix}:{chaos_mesh_version}")
        if self.scylla_image:
            scylla_image_repo, scylla_image_tag = self.scylla_image.split(":")
            if not version_utils.SEMVER_REGEX.match(scylla_image_tag):
                try:
                    new_scylla_image_tag = version_utils.transform_non_semver_scylla_version_to_semver(
                        scylla_image_tag)
                    images_to_retag[self.scylla_image] = f"{scylla_image_repo}:{new_scylla_image_tag}"
                except ValueError as exc:
                    self.log.warning(
                        "Failed to transform non-semver scylla version '%s' to a semver-like one:\n%s",
                        scylla_image_tag, str(exc))

            images_to_cache.append(self.scylla_image)
        if self.params.get("use_mgmt"):
            images_to_cache.extend(self.minio_images)
            images_to_cache.append(
                f"scylladb/scylla-manager-agent:{SCYLLA_MANAGER_AGENT_VERSION_IN_SCYLLA_MANAGER}")
            if self.params.get("mgmt_docker_image"):
                images_to_cache.append(self.params.get("mgmt_docker_image"))
        if self.params.get("scylla_mgmt_agent_version"):
            images_to_cache.append(
                "scylladb/scylla-manager-agent:" + self.params.get("scylla_mgmt_agent_version"))
        if self.params.get("k8s_enable_sni"):
            images_to_cache.extend(self.ingress_controller_images)

        # TODO: enable caching of the 'scylla-operator' image when the following bug gets fixed:
        #       https://github.com/scylladb/scylla-operator/issues/1448
        # try:
        #     images_to_cache.append(self.get_operator_image())
        # except ValueError as exc:
        #     self.log.warning("scylla-operator image won't be cached. Error: %s", str(exc))

        if not self.params.get('reuse_cluster'):
            self.load_images(images_to_cache)

        if new_scylla_image_tag:
            self.params['scylla_version'] = new_scylla_image_tag
        for src_image, dst_image in images_to_retag.items():
            self.docker_tag(src_image, dst_image)
            self.host_node.remoter.run(
                f"/var/tmp/kind load docker-image {dst_image}", ignore_status=True)

        self.setup_pod_network_connectivity()

    def install_static_local_volume_provisioner(
            self, node_pools: list[CloudK8sNodePool] | CloudK8sNodePool) -> None:
        if not isinstance(node_pools, list):
            node_pools = [node_pools]
        pool_names = ",".join([current_pool.name for current_pool in node_pools])

        # NOTE: create static dirs on the KinD Scylla K8S nodes
        #       which will be used by the static local volume provisioner.
        node_names = self.kubectl(
            f"get nodes -l '{POOL_LABEL_NAME} in ({pool_names})' "
            "--no-headers -o custom-columns=:.metadata.name").stdout.split()
        for node_name in node_names:
            path = f"/mnt/raid-disks/disk0/pv-on-{node_name}"
            self.host_node.remoter.run(
                f"docker exec {node_name} /bin/bash "
                f" -c \"mkdir -p {path} && mount --bind {path}{{,}}\"")
        super().install_static_local_volume_provisioner(node_pools=node_pools)

    def gather_k8s_logs(self) -> None:
        if self.params.get("k8s_log_api_calls"):
            # NOTE: export K8S API server log files to the SCT log dir
            src_container_path, log_prefix = DST_APISERVER_AUDIT_LOG.rsplit('/', maxsplit=1)
            log_prefix = log_prefix.split(".")[0]
            dst_subdir = "kube-apiserver"
            try:
                self.host_node.remoter.run(
                    f"docker cp kind-control-plane:{src_container_path} {self.logdir} "
                    f"&& mkdir -p {self.logdir}/{dst_subdir} "
                    f"&& mv {self.logdir}/*/{log_prefix}* {self.logdir}/{dst_subdir}")
            except Exception as exc:  # noqa: BLE001
                self.log.warning(
                    "Failed to copy K8S apiserver audit logs located at '%s'. Exception: \n%s",
                    src_container_path, exc)
        super().gather_k8s_logs()


class LocalMinimalScyllaPodContainer(BaseScyllaPodContainer):
    parent_cluster: 'LocalMinimalScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @cached_property
    def host_remoter(self):
        return self.k8s_cluster.remoter

    @property
    def docker_id(self):
        return self.host_remoter.run(
            f'docker ps -q --filter "label=io.kubernetes.pod.name={self.name}" '
            f'--filter "label=io.kubernetes.container.name=scylla"').stdout.strip()

    def restart(self):
        self.host_remoter.run(f"docker restart {self.docker_id}")

    def terminate_k8s_node(self):
        raise NotImplementedError("Not supported on local K8S backends")

    def terminate_k8s_host(self):
        raise NotImplementedError("Not supported on local K8S backends")


class LocalMinimalScyllaPodCluster(ScyllaPodCluster):
    """Represents scylla cluster hosted on locally running minimal k8s clusters such as k3d, minikube or kind"""
    PodContainerClass = LocalMinimalScyllaPodContainer

    def wait_for_nodes_up_and_normal(self, nodes=None, verification_node=None, iterations=20, sleep_time=60, timeout=0):
        @retrying(n=iterations, sleep_time=sleep_time,
                  allowed_exceptions=(cluster.ClusterNodesNotReady, UnexpectedExit),
                  message="Waiting for nodes to join the cluster", timeout=timeout)
        def _wait_for_nodes_up_and_normal(self):
            super().check_nodes_up_and_normal(nodes=nodes, verification_node=verification_node)

        _wait_for_nodes_up_and_normal(self)

    @cluster.wait_for_init_wrap
    def wait_for_init(self, *_, node_list=None, verbose=False, timeout=None, **__):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)

    def upgrade_scylla_cluster(self, new_version: str) -> None:
        self.k8s_clusters[0].docker_pull(f"{self.params.get('docker_image')}:{new_version}")
        return super().upgrade_scylla_cluster(new_version)

    @staticmethod
    def fstrim_scylla_disks():
        LOGGER.warning("Local K8S backends don't support running 'fstrim' command. Ignoring.")
