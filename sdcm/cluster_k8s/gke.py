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
from textwrap import dedent
from typing import List, Dict, ParamSpec, TypeVar
from functools import cached_property
from collections.abc import Callable

import yaml
import tenacity

from sdcm import sct_abs_path, cluster
from sdcm.wait import exponential_retry
from sdcm.utils.k8s import ApiCallRateLimiter, TokenUpdateThread
from sdcm.utils.gce_utils import GcloudContextManager
from sdcm.cluster_k8s import KubernetesCluster, ScyllaPodCluster, BaseScyllaPodContainer, CloudK8sNodePool
from sdcm.cluster_k8s.iptables import IptablesPodIpRedirectMixin, IptablesClusterOpsMixin
from sdcm.cluster_gce import MonitorSetGCE


GKE_API_CALL_RATE_LIMIT = 5  # ops/s
GKE_API_CALL_QUEUE_SIZE = 1000  # ops
GKE_URLLIB_RETRY = 5  # How many times api request is retried before reporting failure
GKE_URLLIB_BACKOFF_FACTOR = 0.1

LOADER_CLUSTER_CONFIG = sct_abs_path("sdcm/k8s_configs/gke-loaders.yaml")
LOGGER = logging.getLogger(__name__)

P = ParamSpec("P")  # pylint: disable=invalid-name
R = TypeVar("R")  # pylint: disable=invalid-name


class GkeNodePool(CloudK8sNodePool):
    k8s_cluster: 'GkeCluster'

    # pylint: disable=too-many-arguments
    def __init__(
            self,
            k8s_cluster: 'KubernetesCluster',
            name: str,
            num_nodes: int,
            instance_type: str,
            disk_size: int = None,
            disk_type: str = None,
            image_type: str = "UBUNTU_CONTAINERD",
            labels: dict = None,
            local_ssd_count: int = None,
            gce_project: str = None,
            gce_zone: str = None,
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
            is_deployed=is_deployed,
        )
        self.local_ssd_count = local_ssd_count
        self.gce_project = self.k8s_cluster.gce_project if gce_project is None else gce_project
        self.gce_zone = self.k8s_cluster.gce_zone if gce_zone is None else gce_zone

    @property
    def _deploy_cmd(self) -> str:
        # NOTE: '/tmp/system_config.yaml' file gets created on the gcloud container start up.
        cmd = [f"beta container --project {self.gce_project} node-pools create {self.name}",
               f"--zone {self.gce_zone}",
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
            # NOTE: Commands to be used:
            # Stable API: --local-ssd-count 3
            # Beta API  : --ephemeral-storage="local-ssd-count=3"
            cmd.append(f"--ephemeral-storage=\"local-ssd-count={self.local_ssd_count}\"")
        if self.tags:
            cmd.append(f"--metadata {','.join(f'{key}={value}' for key, value in self.tags.items())}")
        return ' '.join(cmd)

    def deploy(self) -> None:
        self.k8s_cluster.gcloud.run(self._deploy_cmd)
        self.is_deployed = True

    def resize(self, num_nodes: int):
        self.k8s_cluster.gcloud.run(
            f"container clusters resize {self.k8s_cluster.short_cluster_name} --project {self.gce_project} "
            f"--zone {self.gce_zone} --node-pool {self.name} --num-nodes {num_nodes} --quiet")
        self.num_nodes = int(num_nodes)
        self.wait_for_nodes_readiness()

    def undeploy(self):
        pass

    @property
    def instance_group_name(self) -> str:
        try:
            group_link = yaml.safe_load(
                self.k8s_cluster.gcloud.run(
                    f'container node-pools describe {self.name} '
                    f'--zone {self.gce_zone} --project {self.gce_project} '
                    f'--cluster {self.k8s_cluster.short_cluster_name}')
            ).get('instanceGroupUrls')[0]
            return group_link.split('/')[-1]
        except Exception as exc:
            raise RuntimeError(f"Can't get instance group name due to the: {exc}") from exc

    def remove_instance(self, instance_name: str):
        self.k8s_cluster.gcloud.run(
            f'compute instance-groups managed delete-instances {self.instance_group_name} '
            f'--zone={self.gce_zone} --instances={instance_name}')


class GcloudTokenUpdateThread(TokenUpdateThread):
    def __init__(self, gcloud, kubectl_token_path: str, token_min_duration: int = 60):
        self._gcloud = gcloud
        self._token_min_duration = token_min_duration
        super().__init__(kubectl_token_path=kubectl_token_path)

    def get_token(self) -> str:
        return self._gcloud.run(f'config config-helper --min-expiry={self._token_min_duration * 60} --format=json')


# pylint: disable=too-many-instance-attributes
class GkeCluster(KubernetesCluster):
    AUXILIARY_POOL_NAME = 'default-pool'  # This is default pool that is deployed with the cluster
    POOL_LABEL_NAME = 'cloud.google.com/gke-nodepool'
    IS_NODE_TUNING_SUPPORTED = True
    pools: Dict[str, GkeNodePool]

    # pylint: disable=too-many-arguments
    def __init__(self,
                 gke_cluster_version,
                 gke_k8s_release_channel,
                 gce_disk_size,
                 gce_disk_type,
                 gce_network,
                 services,
                 gce_instance_type='n1-standard-2',
                 user_prefix=None,
                 params=None,
                 gce_datacenter=None,
                 cluster_uuid=None,
                 n_nodes=2,
                 ):
        super().__init__(
            params=params,
            cluster_uuid=cluster_uuid,
            user_prefix=user_prefix
        )
        self.gke_cluster_version = gke_cluster_version
        self.gke_k8s_release_channel = gke_k8s_release_channel.strip()
        self.gce_disk_type = gce_disk_type
        self.gce_disk_size = gce_disk_size
        self.gce_network = gce_network
        self.gce_services = services
        self.gce_instance_type = gce_instance_type
        self.n_nodes = n_nodes
        self.gce_project = services[0].project
        self.gce_user = services[0].key
        self.gce_zone = gce_datacenter[0]
        self.gke_cluster_created = False
        self.api_call_rate_limiter = ApiCallRateLimiter(
            rate_limit=GKE_API_CALL_RATE_LIMIT,
            queue_size=GKE_API_CALL_QUEUE_SIZE,
            urllib_retry=GKE_URLLIB_RETRY,
            urllib_backoff_factor=GKE_URLLIB_BACKOFF_FACTOR,
        )
        self.api_call_rate_limiter.start()

    @cached_property
    def allowed_labels_on_scylla_node(self) -> list:
        allowed_labels_on_scylla_node = [
            ('app', 'xfs-formatter'),
            ('app', 'local-volume-provisioner'),
            ('k8s-app', 'fluentbit-gke'),
            ('k8s-app', 'gke-metrics-agent'),
            ('component', 'kube-proxy'),
            ('k8s-app', 'gcp-compute-persistent-disk-csi-driver'),
            ('scylla/cluster', self.k8s_scylla_cluster_name),
        ]
        if self.is_performance_tuning_enabled:
            # NOTE: add performance tuning related pods only if we expect it to be.
            #       When we have tuning disabled it must not exist.
            allowed_labels_on_scylla_node.extend(self.perf_pods_labels)
        return allowed_labels_on_scylla_node

    def __str__(self):
        return f"{type(self).__name__} {self.name} | Zone: {self.gce_zone} | Version: {self.gke_cluster_version}"

    def deploy(self):
        LOGGER.info("Create GKE cluster `%s' with %d node(s) in %s",
                    self.short_cluster_name, self.n_nodes, self.AUXILIARY_POOL_NAME)
        tags = ",".join(f"{key}={value}" for key, value in self.tags.items())
        with self.gcloud as gcloud:
            # NOTE: only static K8S release channel supports disabling of autoupgrade
            gcloud.run(f"container --project {self.gce_project} clusters create {self.short_cluster_name}"
                       f" --no-enable-basic-auth"
                       f" --zone {self.gce_zone}"
                       f" --cluster-version {self.gke_cluster_version}"
                       f"{' --release-channel ' + self.gke_k8s_release_channel if self.gke_k8s_release_channel else ''}"
                       f" --network {self.gce_network}"
                       f" --num-nodes {self.n_nodes}"
                       f" --machine-type {self.gce_instance_type}"
                       f" --image-type UBUNTU"
                       f" --disk-type {self.gce_disk_type}"
                       f" --disk-size {self.gce_disk_size}"
                       f" --enable-stackdriver-kubernetes"
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

        LOGGER.info("Setup RBAC for GKE cluster `%s'", self.name)
        self.kubectl("create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin "
                     f"--user {self.gce_user}")

    @cached_property
    def gcloud(self) -> GcloudContextManager:  # pylint: disable=no-self-use
        return self.test_config.tester_obj().localhost.gcloud

    def deploy_node_pool(self, pool: GkeNodePool, wait_till_ready=True) -> None:
        self._add_pool(pool)
        if pool.is_deployed:
            return
        LOGGER.info("Create %s pool with %d node(s) in GKE cluster `%s'", pool.name, pool.num_nodes, self.name)
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

    def get_instance_group_name_for_pool(self, pool_name: str, default=None) -> str:
        try:
            group_link = yaml.safe_load(
                self.gcloud.run(
                    f'container node-pools describe {pool_name} '
                    f'--zone {self.gce_zone} --project {self.gce_project} '
                    f'--cluster {self.short_cluster_name}')
            ).get('instanceGroupUrls')[0]
            return group_link.split('/')[-1]
        except Exception as exc:
            if default is not None:
                return default
            raise RuntimeError(f"Can't get instance group name due to the: {exc}") from exc

    def delete_instance_that_belong_to_instance_group(self, group_name: str, instance_name: str):
        self.gcloud.run(f'compute instance-groups managed delete-instances {group_name} '
                        f'--zone={self.gce_zone} --instances={instance_name}')

    def create_token_update_thread(self):
        return GcloudTokenUpdateThread(self.gcloud, self.kubectl_token_path)

    def create_kubectl_config(self):
        self.gcloud.run(f"container clusters get-credentials {self.short_cluster_name} --zone {self.gce_zone}")

    def destroy(self):
        self.api_call_rate_limiter.stop()
        self.stop_token_update_thread()

    def deploy_scylla_manager(self, pool_name: str = None) -> None:
        self.deploy_minio_s3_backend()
        super().deploy_scylla_manager(pool_name=pool_name)

    # NOTE: blocked by https://github.com/scylladb/scylla-operator/issues/760
    def upgrade_kubernetes_platform(self) -> str:
        # NOTE: 'self.gke_cluster_version' can be like 1.21.3-gke.N or 1.21
        upgrade_version = f"1.{int(self.gke_cluster_version.split('.')[1]) + 1}"

        with self.gcloud as gcloud:
            # Upgrade control plane (API, scheduler, manager and so on ...)
            LOGGER.info("Upgrading K8S control plane to the '%s' version", upgrade_version)
            gcloud.run(f"container clusters upgrade {self.short_cluster_name} "
                       f"--master --quiet --project {self.gce_project} --zone {self.gce_zone} "
                       f"--cluster-version {upgrade_version}")

            # Upgrade scylla-related node pools
            for node_pool in (self.AUXILIARY_POOL_NAME, self.SCYLLA_POOL_NAME):
                LOGGER.info("Upgrading '%s' node pool to the '%s' version",
                            node_pool, upgrade_version)
                # NOTE: one node upgrade takes about 10 minutes
                gcloud.run(f"container clusters upgrade {self.short_cluster_name} "
                           f"--quiet --project {self.gce_project} --zone {self.gce_zone} "
                           f"--node-pool={node_pool}")
        return upgrade_version


class GkeScyllaPodContainer(BaseScyllaPodContainer, IptablesPodIpRedirectMixin):
    parent_cluster: 'GkeScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @property
    def gce_node_ips(self):
        gce_node = self.k8s_node
        return gce_node.public_ips, gce_node.private_ips

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
        return self.parent_cluster.k8s_cluster.gce_services[0].ex_get_node(name=self.node_name)

    def terminate_k8s_host(self):
        self.log.info('terminate_k8s_host: GCE instance of kubernetes node will be terminated, '
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
            self.k8s_node.destroy()

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

        self.log.info('terminate_k8s_node: kubernetes node will be deleted, the following is affected :\n' + dedent('''
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
        self.parent_cluster.k8s_cluster.resize_node_pool(
            self.parent_cluster.node_pool.name,
            self.parent_cluster.node_pool.num_nodes,
        )


class GkeScyllaPodCluster(ScyllaPodCluster, IptablesClusterOpsMixin):
    NODE_PREPARE_FILE = sct_abs_path("sdcm/k8s_configs/gke/scylla-node-prepare.yaml")
    node_terminate_methods = [
        'drain_k8s_node',
        # NOTE: uncomment below when following scylla-operator bug is fixed:
        #       https://github.com/scylladb/scylla-operator/issues/643
        #       Also, need to add check that there are no PV duplicates
        # 'terminate_k8s_host',
        # 'terminate_k8s_node',
    ]

    k8s_cluster: 'GkeCluster'
    node_pool: 'GkeNodePool'
    PodContainerClass = GkeScyllaPodContainer

    # pylint: disable=too-many-arguments
    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[GkeScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap)

        self.add_hydra_iptables_rules(nodes=new_nodes)
        self.update_nodes_iptables_redirect_rules(nodes=new_nodes, loaders=False)

        return new_nodes


class MonitorSetGKE(MonitorSetGCE):
    DB_NODES_IP_ADDRESS = 'public_ip_address'

    def install_scylla_manager(self, node):
        pass
