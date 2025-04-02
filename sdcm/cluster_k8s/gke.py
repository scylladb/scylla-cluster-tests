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

from textwrap import dedent
from typing import List, Dict, ParamSpec, TypeVar
from functools import cached_property
from collections.abc import Callable

import tempfile
import json
import yaml
import tenacity
from google.cloud import compute_v1

from sdcm import sct_abs_path, cluster
from sdcm.wait import exponential_retry
from sdcm.utils.common import list_instances_gce, gce_meta_to_dict
from sdcm.utils.k8s import ApiCallRateLimiter, TokenUpdateThread
from sdcm.utils.gce_utils import (
    GcloudContainerMixin,
    GcloudContextManager,
    get_gce_compute_instances_client,
    wait_for_extended_operation,
    gce_public_addresses,
    gce_private_addresses,
)
from sdcm.utils.ci_tools import get_test_name
from sdcm.cluster_k8s import KubernetesCluster, ScyllaPodCluster, BaseScyllaPodContainer, CloudK8sNodePool
from sdcm.cluster_gce import MonitorSetGCE
from sdcm.keystore import KeyStore
from sdcm.remote import LOCALRUNNER


GKE_API_CALL_RATE_LIMIT = 5  # ops/s
GKE_API_CALL_QUEUE_SIZE = 1000  # ops
GKE_URLLIB_RETRY = 5  # How many times api request is retried before reporting failure
GKE_URLLIB_BACKOFF_FACTOR = 0.1

P = ParamSpec("P")
R = TypeVar("R")


def init_k8s_gke_cluster(gce_datacenter: str, availability_zone: str, params: dict,
                         cluster_uuid: str = None):
    """Dedicated for the usage by the 'Tester' class which orchestrates all the resources creation.

    Return the 'k8s_cluster' object back to the 'Tester' as soon as possible to be able to trigger
    logs gathering operations in case resources provisioning and/or setup fail.
    The 'Tester' should then call the 'deploy_k8s_gke_cluster' function
    providing initialized 'k8s_cluster' object.
    """
    return GkeCluster(
        gke_cluster_version=params.get("gke_cluster_version"),
        gke_k8s_release_channel=params.get("gke_k8s_release_channel"),
        gce_disk_size=params.get("root_disk_size_db"),
        gce_disk_type=params.get("gce_root_disk_type_db"),
        gce_network=params.get("gce_network"),
        gce_service=get_gce_compute_instances_client(),
        gce_instance_type=params.get('k8s_instance_type_auxiliary'),
        user_prefix=params.get("user_prefix"),
        params=params,
        gce_datacenter=[gce_datacenter],
        availability_zone=availability_zone,
        cluster_uuid=cluster_uuid,
        n_nodes=params.get('k8s_n_auxiliary_nodes'),
    )


def deploy_k8s_gke_cluster(k8s_cluster) -> None:
    """Dedicated for the usage by the 'Tester' class which orchestrates all the resources creation.

    This function creates all the needed node pools and other ecosystem workloads
    returning the 'k8s_cluster' object ready to provision Scylla and loaders pods.
    """
    params = k8s_cluster.params
    k8s_cluster.deploy()

    # NOTE: between GKE cluster creation and addition of new node pools we need
    # several minutes gap to avoid "repair" status of a cluster when API server goes down.
    # So, deploy apps specific to default-pool in between above mentioned deployment steps.
    k8s_cluster.set_nodeselector_for_deployments(
        pool_name=k8s_cluster.AUXILIARY_POOL_NAME, namespace="kube-system")
    k8s_cluster.deploy_cert_manager(pool_name=k8s_cluster.AUXILIARY_POOL_NAME)
    if params.get("k8s_enable_sni"):
        k8s_cluster.deploy_ingress_controller(pool_name=k8s_cluster.AUXILIARY_POOL_NAME)
    k8s_cluster.deploy_scylla_operator()
    if params.get("k8s_use_chaos_mesh"):
        k8s_cluster.chaos_mesh.initialize()
    if params.get('use_mgmt'):
        # NOTE: deploy scylla-manager only in the first dc/region
        if params.gce_datacenters.index(k8s_cluster.region_name) == 0:
            k8s_cluster.deploy_scylla_manager(pool_name=k8s_cluster.AUXILIARY_POOL_NAME)

    # TODO: add support for different loaders amount in different K8S clusters
    if params.get("n_loaders"):
        loader_pool = GkeNodePool(
            name=k8s_cluster.LOADER_POOL_NAME,
            instance_type=params.get("gce_instance_type_loader"),
            num_nodes=params.get("n_loaders"),
            k8s_cluster=k8s_cluster)
        k8s_cluster.deploy_node_pool(loader_pool, wait_till_ready=False)

    if params.get('k8s_deploy_monitoring'):
        monitor_pool = GkeNodePool(
            name=k8s_cluster.MONITORING_POOL_NAME,
            local_ssd_count=params.get("gce_n_local_ssd_disk_monitor"),
            disk_size=params.get("root_disk_size_monitor"),
            disk_type=params.get("gce_root_disk_type_monitor"),
            instance_type=params.get("k8s_instance_type_monitor") or params.get("gce_instance_type_monitor"),
            num_nodes=params.get("k8s_n_monitor_nodes") or params.get("n_monitor_nodes"),
            k8s_cluster=k8s_cluster)
        k8s_cluster.deploy_node_pool(monitor_pool, wait_till_ready=False)

    # TODO: add support for different DB nodes amount in different K8S clusters
    scylla_pool = GkeNodePool(
        name=k8s_cluster.SCYLLA_POOL_NAME,
        local_ssd_count=params.get("gce_n_local_ssd_disk_db"),
        disk_size=params.get("root_disk_size_db"),
        disk_type=params.get("gce_root_disk_type_db"),
        instance_type=params.get("gce_instance_type_db"),
        num_nodes=params.get("n_db_nodes"),
        taints=["role=scylla-clusters:NoSchedule"],
        k8s_cluster=k8s_cluster)
    k8s_cluster.deploy_node_pool(scylla_pool, wait_till_ready=False)

    k8s_cluster.wait_all_node_pools_to_be_ready()
    k8s_cluster.prepare_k8s_scylla_nodes(node_pools=scylla_pool)
    return k8s_cluster


class GkeNodePool(CloudK8sNodePool):
    k8s_cluster: 'GkeCluster'

    def __init__(  # noqa: PLR0913
            self,
            k8s_cluster: 'KubernetesCluster',
            name: str,
            num_nodes: int,
            instance_type: str,
            disk_size: int = None,
            disk_type: str = None,
            image_type: str = "UBUNTU_CONTAINERD",
            labels: dict = None,
            taints: list = None,
            local_ssd_count: int = None,
            gce_project: str = None,
            is_deployed: bool = False
    ):
        super().__init__(
            k8s_cluster=k8s_cluster,
            name=name,
            num_nodes=num_nodes,
            disk_size=disk_size,
            disk_type=disk_type,
            image_type=image_type,
            instance_type=instance_type,
            labels=labels,
            taints=taints,
            is_deployed=is_deployed,
        )
        self.local_ssd_count = local_ssd_count
        self.gce_project = self.k8s_cluster.gce_project if gce_project is None else gce_project
        self.gce_region = self.k8s_cluster.gce_region
        self.gce_zone = self.k8s_cluster.gce_zone

    @property
    def _deploy_cmd(self) -> str:
        # NOTE: '/tmp/system_config.yaml' file gets created on the gcloud container start up.
        cmd = [f"container --project {self.gce_project} node-pools create {self.name}",
               f"--region {self.gce_region}",
               f"--node-locations {self.gce_zone}",
               f"--cluster {self.k8s_cluster.short_cluster_name}",
               f"--num-nodes {self.num_nodes}",
               f"--machine-type {self.instance_type}",
               f"--image-type {self.image_type}",
               "--system-config-from-file /tmp/system_config.yaml",
               ]
        if not self.k8s_cluster.gke_k8s_release_channel:
            # NOTE: only static K8S release channel supports disabling of autoupgrade
            cmd.append("--no-enable-autoupgrade")
            cmd.append("--no-enable-autorepair")
        if self.disk_type:
            cmd.append(f"--disk-type {self.disk_type}")
        if self.disk_size:
            cmd.append(f"--disk-size {self.disk_size}")
        if self.local_ssd_count:
            cmd.append(f"--local-nvme-ssd-block count=\"{self.local_ssd_count}\"")
        if self.taints:
            cmd.append(f"--node-taints {' '.join(self.taints)}")
        if self.tags:
            cmd.append(f"--metadata {','.join(f'{key}={value}' for key, value in self.tags.items())}")
        return ' '.join(cmd)

    def deploy(self) -> None:
        self.k8s_cluster.gcloud.run(self._deploy_cmd)
        self.is_deployed = True

    def resize(self, num_nodes: int):
        self.k8s_cluster.gcloud.run(
            f"container clusters resize {self.k8s_cluster.short_cluster_name} --project {self.gce_project} "
            f"--region {self.gce_region} "
            f"--node-pool {self.name} --num-nodes {num_nodes} --quiet")
        self.num_nodes = int(num_nodes)
        self.wait_for_nodes_readiness()

    def undeploy(self):
        self.k8s_cluster.gcloud.run(
            f"container --project {self.gce_project} node-pools delete {self.name} "
            f"--cluster {self.k8s_cluster.short_cluster_name} "
            f"--region {self.gce_region} --quiet")

    @property
    def instance_group_name(self) -> str:
        try:
            group_link = yaml.safe_load(
                self.k8s_cluster.gcloud.run(
                    f'container node-pools describe {self.name} '
                    f'--region {self.gce_region} '
                    f'--project {self.gce_project} '
                    f'--cluster {self.k8s_cluster.short_cluster_name}')
            ).get('instanceGroupUrls')[0]
            return group_link.split('/')[-1]
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Can't get instance group name due to the: {exc}") from exc

    def remove_instance(self, instance_name: str):
        self.k8s_cluster.gcloud.run(
            f'compute instance-groups managed delete-instances {self.instance_group_name} '
            f'--region={self.gce_region} '
            f'--node-locations={self.gce_zone} '
            f'--instances={instance_name}')


class GcloudTokenUpdateThread(TokenUpdateThread):
    def __init__(self, gcloud, kubectl_token_path: str, token_min_duration: int = 60):
        self._gcloud = gcloud
        self._token_min_duration = token_min_duration
        super().__init__(kubectl_token_path=kubectl_token_path)

    def get_token(self) -> str:
        return self._gcloud.run(f'config config-helper --min-expiry={self._token_min_duration * 60} --format=json')


class GcloudException(Exception):
    ...


class GkeCluster(KubernetesCluster):
    AUXILIARY_POOL_NAME = 'default-pool'  # This is default pool that is deployed with the cluster
    POOL_LABEL_NAME = 'cloud.google.com/gke-nodepool'
    IS_NODE_TUNING_SUPPORTED = True
    NODE_PREPARE_FILE = sct_abs_path("sdcm/k8s_configs/gke/scylla-node-prepare.yaml")
    NODE_CONFIG_CRD_FILE = sct_abs_path("sdcm/k8s_configs/gke/node-config-crd.yaml")
    TOKEN_UPDATE_NEEDED = False
    pools: Dict[str, GkeNodePool]

    def __init__(self,  # noqa: PLR0913
                 gke_cluster_version,
                 gke_k8s_release_channel,
                 gce_disk_size,
                 gce_disk_type,
                 gce_network,
                 gce_service: tuple[compute_v1.InstancesClient, dict],
                 gce_instance_type='n2-standard-2',
                 user_prefix=None,
                 params=None,
                 gce_datacenter=None,
                 availability_zone=None,
                 cluster_uuid=None,
                 n_nodes=2,
                 ):
        self.gke_cluster_version = gke_cluster_version
        self.gke_k8s_release_channel = gke_k8s_release_channel.strip()
        self.gce_disk_type = gce_disk_type
        self.gce_disk_size = gce_disk_size
        self.gce_network = gce_network
        self.gce_service, info = gce_service
        self.gce_instance_type = gce_instance_type
        self.n_nodes = n_nodes
        self.gce_project = info['project_id']
        self.gce_user = info['client_email']
        dc_parts = gce_datacenter[0].split("-")[:3]
        self.gce_region = "-".join(dc_parts[:2])
        self.gce_zone = f"{self.gce_region}-"
        self.gce_zone += availability_zone or (dc_parts[2] if len(dc_parts) == 3 else 'b')
        super().__init__(
            params=params,
            cluster_uuid=cluster_uuid,
            user_prefix=user_prefix,
            region_name=self.gce_region,
        )
        self.gke_cluster_created = False
        self._authenticate_in_gcloud()
        self.api_call_rate_limiter = ApiCallRateLimiter(
            rate_limit=GKE_API_CALL_RATE_LIMIT,
            queue_size=GKE_API_CALL_QUEUE_SIZE,
            urllib_retry=GKE_URLLIB_RETRY,
            urllib_backoff_factor=GKE_URLLIB_BACKOFF_FACTOR,
        )
        self.api_call_rate_limiter.start()

    @staticmethod
    def _authenticate_in_gcloud():
        credentials = KeyStore().get_gcp_credentials()
        with tempfile.NamedTemporaryFile(mode='w+', delete=True, encoding='utf-8') as tmp_gcloud_creds_file:
            tmp_gcloud_creds_file.write(json.dumps(credentials))
            tmp_gcloud_creds_file.flush()
            auth_cmd = (
                f"gcloud auth activate-service-account {credentials['client_email']}"
                f" --key-file {tmp_gcloud_creds_file.name} --project {credentials['project_id']}"
            )
            auth_result = LOCALRUNNER.run(auth_cmd)
            if auth_result.exited:
                raise GcloudException(auth_result.stdout.strip())

    @cached_property
    def allowed_labels_on_scylla_node(self) -> list:
        allowed_labels_on_scylla_node = [
            ('app', 'node-setup'),
            ('app', 'static-local-volume-provisioner'),
            ('k8s-app', 'fluentbit-gke'),
            ('k8s-app', 'gke-metrics-agent'),
            ('component', 'kube-proxy'),
            ('k8s-app', 'gcp-compute-persistent-disk-csi-driver'),
        ]
        if self.tenants_number > 1:
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/name', 'scylla'))
            allowed_labels_on_scylla_node.append(('app', 'scylla'))
        else:
            allowed_labels_on_scylla_node.append(('scylla/cluster', self.k8s_scylla_cluster_name))
        if self.is_performance_tuning_enabled:
            # NOTE: add performance tuning related pods only if we expect it to be.
            #       When we have tuning disabled it must not exist.
            allowed_labels_on_scylla_node.extend(self.perf_pods_labels)
        if self.params.get('k8s_use_chaos_mesh'):
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/component', 'chaos-daemon'))
        if self.params.get("k8s_local_volume_provisioner_type") != 'static':
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/name', 'local-csi-driver'))
        return allowed_labels_on_scylla_node

    def __str__(self):
        return f"{type(self).__name__} {self.name} | Region: {self.gce_region} | Version: {self.gke_cluster_version}"

    def deploy(self):
        self.log.info(
            "Create GKE cluster `%s' with %d node(s) in %s",
            self.short_cluster_name, self.n_nodes, self.AUXILIARY_POOL_NAME)
        tags = ",".join(f"{key}={value}" for key, value in self.tags.items())
        with self.gcloud as gcloud:
            # NOTE: only static K8S release channel supports disabling of autoupgrade
            gcloud.run(f"container --project {self.gce_project} clusters create {self.short_cluster_name}"
                       f" --no-enable-basic-auth"
                       f" --region {self.gce_region}"
                       f" --node-locations {self.gce_zone}"
                       f" --cluster-version {self.gke_cluster_version}"
                       f"{' --release-channel ' + self.gke_k8s_release_channel if self.gke_k8s_release_channel else ''}"
                       f" --network {self.gce_network}"
                       f" --num-nodes {self.n_nodes}"
                       f" --machine-type {self.gce_instance_type}"
                       f" --image-type ubuntu_containerd"
                       f" --disk-type {self.gce_disk_type}"
                       f" --disk-size {self.gce_disk_size}"
                       f" --logging=SYSTEM,WORKLOAD --monitoring=SYSTEM"
                       f"{'' if self.gke_k8s_release_channel else ' --no-enable-autoupgrade'}"
                       f"{'' if self.gke_k8s_release_channel else ' --no-enable-autorepair'}"
                       f" --metadata {tags}")
            self.patch_kubectl_config()
            self.deploy_node_pool(GkeNodePool(
                name=self.AUXILIARY_POOL_NAME,
                num_nodes=self.n_nodes,
                disk_size=self.gce_disk_size,
                disk_type=self.gce_disk_type,
                k8s_cluster=self,
                instance_type=self.gce_instance_type,
                is_deployed=True
            ))

        self.log.info("Setup RBAC for GKE cluster `%s'", self.name)
        self.kubectl("create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin "
                     f"--user {self.gce_user}")

    @cached_property
    def gcloud(self) -> GcloudContextManager:
        return type("GcloudCmdRunner", (GcloudContainerMixin, ), {
            "_containers": {},
            "tags": {},
            "name": f"gcloud_cmd_runner-{self.gce_zone}",
            "kube_config_path": self.kube_config_path,
        })().gcloud

    def deploy_node_pool(self, pool: GkeNodePool, wait_till_ready=True) -> None:
        self._add_pool(pool)
        if pool.is_deployed:
            return
        self.log.info(
            "Create %s pool with %d node(s) in GKE cluster `%s'",
            pool.name, pool.num_nodes, self.name)
        if wait_till_ready:
            with self.api_call_rate_limiter.pause:
                pool.deploy_and_wait_till_ready()
                self.api_call_rate_limiter.wait_till_api_become_stable(self)
        else:
            pool.deploy()

    def wait_all_node_pools_to_be_ready(self):
        with self.api_call_rate_limiter.pause:
            super().wait_all_node_pools_to_be_ready()
            self.api_call_rate_limiter.wait_till_api_become_stable(self)

    def resize_node_pool(self, name: str, num_nodes: int) -> None:
        with self.api_call_rate_limiter.pause:
            self.pools[name].resize(num_nodes)
            self.api_call_rate_limiter.wait_till_api_become_stable(self)

    def create_token_update_thread(self):
        return GcloudTokenUpdateThread(self.gcloud, self.kubectl_token_path)

    def create_kubectl_config(self):
        self.gcloud.run(
            f"container clusters get-credentials {self.short_cluster_name}"
            f" --region {self.gce_region}")

    def destroy(self):
        self.api_call_rate_limiter.stop()

    def deploy_scylla_manager(self, pool_name: str = None) -> None:
        self.deploy_minio_s3_backend()
        super().deploy_scylla_manager(pool_name=pool_name)

    def upgrade_kubernetes_platform(self, pod_objects: list[cluster.BaseNode],
                                    use_additional_scylla_nodepool: bool) -> (str, CloudK8sNodePool):
        # NOTE: 'self.gke_cluster_version' can be like 1.21.3-gke.N or 1.21
        upgrade_version = f"1.{int(self.gke_cluster_version.split('.')[1]) + 1}"

        with self.gcloud as gcloud:
            # Upgrade control plane (API, scheduler, manager and so on ...)
            self.log.info("Upgrading K8S control plane to the '%s' version", upgrade_version)
            gcloud.run(f"container clusters upgrade {self.short_cluster_name} "
                       f"--master --quiet --project {self.gce_project} "
                       f"--region {self.gce_region} "
                       f"--cluster-version {upgrade_version}")
            self.gke_cluster_version = upgrade_version

            # Upgrade scylla-related node pools
            for node_pool, need_upgrade in (
                    (self.AUXILIARY_POOL_NAME, True),
                    (self.SCYLLA_POOL_NAME, not use_additional_scylla_nodepool)):
                if not need_upgrade:
                    continue
                self.log.info("Upgrading '%s' node pool to the '%s' version", node_pool, upgrade_version)
                # NOTE: one node upgrade takes about 10 minutes if no load and preloaded data exist
                gcloud.run(f"container clusters upgrade {self.short_cluster_name} "
                           f"--quiet --project {self.gce_project} "
                           f"--region {self.gce_region} "
                           f"--node-pool={node_pool}")

        if use_additional_scylla_nodepool:
            # Create new node pool
            new_scylla_pool_name = f"{self.SCYLLA_POOL_NAME}-new"
            new_scylla_pool = GkeNodePool(
                name=new_scylla_pool_name,
                local_ssd_count=self.params.get("gce_n_local_ssd_disk_db"),
                disk_size=self.params.get("root_disk_size_db"),
                disk_type=self.params.get("gce_root_disk_type_db"),
                instance_type=self.params.get("gce_instance_type_db"),
                num_nodes=self.params.get("n_db_nodes"),
                k8s_cluster=self)
            self.deploy_node_pool(new_scylla_pool, wait_till_ready=True)

            # Prepare new nodes for Scylla pods hosting
            self.prepare_k8s_scylla_nodes(
                node_pools=[self.pools[self.SCYLLA_POOL_NAME], new_scylla_pool])

            # Move Scylla pods to the new nodes
            self.move_pods_to_new_node_pool(
                pod_objects=pod_objects,
                node_pool_name=new_scylla_pool_name,
                pod_readiness_timeout_minutes=120)

            # Delete old node pool
            self.pools[self.SCYLLA_POOL_NAME].undeploy()

            return upgrade_version, new_scylla_pool
        else:
            return upgrade_version, self.pools[self.SCYLLA_POOL_NAME]


class GkeScyllaPodContainer(BaseScyllaPodContainer):
    parent_cluster: 'GkeScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @property
    def gce_node_ips(self):
        gce_node = self.k8s_node
        return gce_public_addresses(gce_node), gce_private_addresses(gce_node)

    @cached_property
    def hydra_dest_ip(self) -> str:
        if self.test_config.IP_SSH_CONNECTIONS == "public" or self.test_config.INTRA_NODE_COMM_PUBLIC:
            return self.gce_node_ips[0][0]
        return self.gce_node_ips[1][0]

    @cached_property
    def nodes_dest_ip(self) -> str:
        if self.test_config.INTRA_NODE_COMM_PUBLIC:
            return self.gce_node_ips[0][0]
        return self.gce_node_ips[1][0]

    @property
    def k8s_node(self):
        k8s_cluster = self.k8s_cluster
        return k8s_cluster.gce_service.get(project=k8s_cluster.gce_project,
                                           zone=k8s_cluster.gce_zone,
                                           instance=self.node_name)

    def refresh_network_interfaces_info(self):
        pass

    def terminate_k8s_host(self):
        self.k8s_cluster.log.info(
            'terminate_k8s_host: GCE instance of kubernetes node will be terminated, '
            'the following is affected :\n' + dedent('''
            GCE instance  X  <-
            K8s node      X
            Scylla Pod    X
            Scylla node   X
            '''))
        self._instance_wait_safe(self._destroy)
        self.wait_for_k8s_node_readiness()

    def _destroy(self):
        if self.k8s_node:
            instances_client, _ = get_gce_compute_instances_client()
            operation = instances_client.delete(instance=self.k8s_node.name,
                                                project=self.k8s_cluster.gce_project,
                                                zone=self.k8s_cluster.gce_zone)
            wait_for_extended_operation(operation, "wait for k8s_node deletion")

    def _instance_wait_safe(self, instance_method: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return exponential_retry(func=lambda: instance_method(*args, **kwargs), logger=self.log)
        except tenacity.RetryError:
            raise cluster.NodeError(
                f"Timeout while running '{instance_method.__name__}' method on GCE instance '{self.k8s_node.id}'"
            ) from None

    def terminate_k8s_node(self):
        """
        Delete kubernetes node, which will terminate scylla node that is running on it
        """

        # There is a bug in GCE, it keeps instance running when kubernetes node is deleted via kubectl
        # As result GKE infrastructure does not allow you to add a node to the cluster
        # In order to fix that we have to delete instance manually and add a node to the cluster

        self.k8s_cluster.log.info('terminate_k8s_node: kubernetes node will be deleted, the following is affected :\n' + dedent('''
            GKE instance    X  <-
            K8s node        X  <-
            Scylla Pod      X
            Scylla node     X
            '''))
        node_name = self.node_name
        super().terminate_k8s_node()

        # Removing GKE instance and adding one node back to the cluster
        # TBD: Remove below lines when https://issuetracker.google.com/issues/178302655 is fixed
        self.parent_cluster.node_pool.remove_instance(instance_name=node_name)
        self.k8s_cluster.resize_node_pool(
            self.parent_cluster.node_pool.name,
            self.parent_cluster.node_pool.num_nodes,
        )


class GkeScyllaPodCluster(ScyllaPodCluster):
    node_pool: 'GkeNodePool'
    PodContainerClass = GkeScyllaPodContainer

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = None,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False,
                  instance_type=None) -> List[GkeScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap,
                                      instance_type=instance_type)
        return new_nodes


class MonitorSetGKE(MonitorSetGCE):
    DB_NODES_IP_ADDRESS = 'ip_address'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.json_file_params_for_replace = {
            "$test_name": f"{get_test_name()}--{self.targets['db_cluster'].scylla_cluster_name}",
        }
        self.tags["monitorid"] = self.monitor_id

    def install_scylla_manager(self, node):
        pass

    # NOTE: setting and filtering of the "monitorid" tag is needed for the multi-tenant setup.
    def _get_instances(self, dc_idx):
        if not self.monitor_id:
            raise ValueError("'monitor_id' must exist")
        instances_by_nodetype = list_instances_gce(
            tags_dict={'MonitorId': self.monitor_id, 'NodeType': self.node_type})
        instances_by_zone = self._get_instances_by_prefix(dc_idx)
        instances = []
        ip_addresses = gce_public_addresses if self._node_public_ips else gce_private_addresses
        for node_zone in instances_by_zone:
            # Filter nodes by zone and by ip addresses
            if not ip_addresses(node_zone):
                continue
            for node_nodetype in instances_by_nodetype:
                if node_zone.id == node_nodetype.id:
                    instances.append(node_zone)

        def sort_by_index(node):
            metadata = gce_meta_to_dict(node.metadata)
            return metadata.get('NodeIndex', 0)

        instances = sorted(instances, key=sort_by_index)
        return instances
