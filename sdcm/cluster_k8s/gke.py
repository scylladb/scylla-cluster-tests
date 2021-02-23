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
import logging
import time
from textwrap import dedent
from typing import List, Dict
from functools import cached_property

import yaml

from sdcm import sct_abs_path, cluster
from sdcm.utils.k8s import ApiCallRateLimiter, K8S_CONFIGS_PATH_IN_CONTAINER
from sdcm.utils.common import shorten_cluster_name
from sdcm.utils.gce_utils import GcloudContextManager, GcloudTokenUpdateThread
from sdcm.wait import wait_for
from sdcm.cluster_k8s import KubernetesCluster, ScyllaPodCluster, BaseScyllaPodContainer
from sdcm.cluster_k8s.iptables import IptablesPodIpRedirectMixin, IptablesClusterOpsMixin
from sdcm.cluster_gce import MonitorSetGCE


GKE_API_CALL_RATE_LIMIT = 0.5  # ops/s
GKE_API_CALL_QUEUE_SIZE = 1000  # ops
GKE_URLLIB_RETRY = 6  # How many times api request is retried before reporting failure

SCYLLA_CLUSTER_CONFIG = f"{K8S_CONFIGS_PATH_IN_CONTAINER}/gke-cluster-chart-values.yaml"
LOADER_CLUSTER_CONFIG = sct_abs_path("sdcm/k8s_configs/gke-loaders.yaml")
CPU_POLICY_DAEMONSET = sct_abs_path("sdcm/k8s_configs/cpu-policy-daemonset.yaml")
RAID_DAEMONSET = sct_abs_path("sdcm/k8s_configs/raid-daemonset.yaml")

SCYLLA_POD_POOL_NAME = 'default-pool'
OPERATOR_POD_POOL_NAME = 'operator-pool'

LOGGER = logging.getLogger(__name__)


class GkeCluster(KubernetesCluster, cluster.BaseCluster):

    def __init__(self,
                 gke_cluster_version,
                 gce_image_type,
                 gce_image_size,
                 gce_network,
                 services,
                 credentials,
                 gce_n_local_ssd=0,
                 gce_instance_type="n1-highmem-8",
                 n_nodes=3,
                 user_prefix=None,
                 params=None,
                 gce_datacenter=None):
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "k8s-gke")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "node")
        self._pools_info: Dict[str: int] = {}
        self._gcloud_token_thread = None
        self.gke_cluster_version = gke_cluster_version
        self.gce_image_type = gce_image_type
        self.gce_image_size = gce_image_size
        self.gce_network = gce_network
        self.gce_services = services
        self.credentials = credentials
        self.gce_instance_type = gce_instance_type
        self.gce_n_local_ssd = int(gce_n_local_ssd) if gce_n_local_ssd else 0

        self.gce_project = services[0].project
        self.gce_user = services[0].key
        self.gce_zone = gce_datacenter[0]
        self.gke_cluster_created = False

        self.api_call_rate_limiter = ApiCallRateLimiter(
            rate_limit=GKE_API_CALL_RATE_LIMIT,
            queue_size=GKE_API_CALL_QUEUE_SIZE,
            urllib_retry=GKE_URLLIB_RETRY
        )
        self.api_call_rate_limiter.start()

        super().__init__(cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=gce_datacenter,
                         node_type="scylla-db")

    @cached_property
    def gke_cluster_name(self):
        return shorten_cluster_name(self.name, 40)

    def __str__(self):
        return f"{type(self).__name__} {self.name} | Zone: {self.gce_zone} | Version: {self.gke_cluster_version}"

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, rack=0, enable_auto_bootstrap=False):
        if not self.gke_cluster_created:
            self.setup_gke_cluster(num_nodes=count)
            self.gke_cluster_created = True
        else:
            raise NotImplementedError

    @property
    def gcloud(self) -> GcloudContextManager:
        return cluster.Setup.tester_obj().localhost.gcloud

    def setup_gke_cluster(self, num_nodes: int) -> None:
        LOGGER.info("Create GKE cluster `%s' with %d node(s) in %s and 1 node in %s",
                    self.gke_cluster_name, num_nodes, SCYLLA_POD_POOL_NAME, OPERATOR_POD_POOL_NAME)
        tags = ",".join(f"{key}={value}" for key, value in self.tags.items())
        with self.gcloud as gcloud:
            gcloud.run(f"container --project {self.gce_project} clusters create {self.gke_cluster_name}"
                       f" --zone {self.gce_zone}"
                       f" --cluster-version {self.gke_cluster_version}"
                       f" --username admin"
                       f" --network {self.gce_network}"
                       f" --num-nodes {num_nodes}"
                       f" --machine-type {self.gce_instance_type}"
                       f" --image-type UBUNTU"
                       f" --disk-type {self.gce_image_type}"
                       f" --disk-size {self.gce_image_size}"
                       f" --local-ssd-count {self.gce_n_local_ssd}"
                       f" --node-taints role=scylla-clusters:NoSchedule"
                       f" --enable-stackdriver-kubernetes"
                       f" --no-enable-autoupgrade"
                       f" --no-enable-autorepair"
                       f" --metadata {tags}")
            self.set_nodes_in_pool(SCYLLA_POD_POOL_NAME, num_nodes)
            gcloud.run(f"container --project {self.gce_project} node-pools create {OPERATOR_POD_POOL_NAME}"
                       f" --zone {self.gce_zone}"
                       f" --cluster {self.gke_cluster_name}"
                       f" --num-nodes 1"
                       f" --machine-type n1-standard-4"
                       f" --image-type UBUNTU"
                       f" --disk-type pd-ssd"
                       f" --disk-size 20"
                       f" --no-enable-autoupgrade"
                       f" --no-enable-autorepair")
            self.set_nodes_in_pool(OPERATOR_POD_POOL_NAME, 1)

            LOGGER.info("Get credentials for GKE cluster `%s'", self.name)
            gcloud.run(f"container clusters get-credentials {self.gke_cluster_name} --zone {self.gce_zone}")
        self.start_gcloud_token_update_thread()
        self.patch_kube_config()

        LOGGER.info("Setup RBAC for GKE cluster `%s'", self.name)
        self.kubectl(f"create clusterrolebinding cluster-admin-binding"
                     f" --clusterrole cluster-admin"
                     f" --user {self.gce_user}")

        LOGGER.info("Install RAID DaemonSet to GKE cluster `%s'", self.name)
        self.apply_file(RAID_DAEMONSET, envsubst=False)

        LOGGER.info("Install CPU policy DaemonSet to GKE cluster `%s'", self.name)
        self.apply_file(CPU_POLICY_DAEMONSET, envsubst=False)

        LOGGER.info("Install local volume provisioner to GKE cluster `%s'", self.name)
        self.helm(f"install local-provisioner {K8S_CONFIGS_PATH_IN_CONTAINER}/provisioner")

    def get_nodes_in_pool(self, pool_name: str) -> int:
        return self._pools_info.get(pool_name, 0)

    def set_nodes_in_pool(self, pool_name: str, num: int):
        self._pools_info[pool_name] = num

    def add_gke_pool(self, name: str, num_nodes: int, instance_type: str) -> None:
        LOGGER.info("Create %s pool with %d node(s) in GKE cluster `%s'", name, num_nodes, self.name)
        with self.api_call_rate_limiter.pause:
            self.gcloud.run(f"container --project {self.gce_project} node-pools create {name}"
                            f" --zone {self.gce_zone}"
                            f" --cluster {self.gke_cluster_name}"
                            f" --num-nodes {num_nodes}"
                            f" --machine-type {instance_type}"
                            f" --image-type UBUNTU"
                            f" --node-taints role=sct-loaders:NoSchedule"
                            f" --no-enable-autoupgrade"
                            f" --no-enable-autorepair")
            self.api_call_rate_limiter.wait_till_api_become_not_operational(self)
            self.api_call_rate_limiter.wait_till_api_become_stable(self)
            self.kubectl_no_wait('wait --timeout=15m --all --for=condition=Ready node')
            self.set_nodes_in_pool(name, num_nodes)

    def resize_gke_pool(self, name: str, num_nodes: int) -> None:
        LOGGER.info("Resize %s pool with %d node(s) in GKE cluster `%s'", name, num_nodes, self.name)
        with self.api_call_rate_limiter.pause:
            self.gcloud.run(f"container clusters resize {self.gke_cluster_name} --project {self.gce_project} "
                            f"--zone {self.gce_zone} --node-pool {name} --num-nodes {num_nodes} --quiet")
            self.api_call_rate_limiter.wait_till_api_become_not_operational(self)
            self.api_call_rate_limiter.wait_till_api_become_stable(self)
            self.kubectl_no_wait('wait --timeout=15m --all --for=condition=Ready node')
            self.set_nodes_in_pool(name, num_nodes)

    def get_instance_group_name_for_pool(self, pool_name: str, default=None) -> str:
        try:
            group_link = yaml.load(
                self.gcloud.run(
                    f'container node-pools describe {pool_name} '
                    f'--zone {self.gce_zone} --project {self.gce_project} '
                    f'--cluster {self.gke_cluster_name}')
            ).get('instanceGroupUrls')[0]
            return group_link.split('/')[-1]
        except Exception as exc:
            if default is not None:
                return default
            raise RuntimeError(f"Can't get instance group name due to the: {exc}")

    def delete_instance_that_belong_to_instance_group(self, group_name: str, instance_name: str):
        self.gcloud.run(f'compute instance-groups managed delete-instances {group_name} '
                        f'--zone={self.gce_zone} --instances={instance_name}')

    def get_kubectl_config_for_user(self, config, username):
        for user in config["users"]:
            if user["name"] == username:
                return user["user"]["auth-provider"]["config"]
        return None

    @cached_property
    def gcloud_token_path(self):
        return os.path.join(self.logdir, 'gcloud.output')

    def start_gcloud_token_update_thread(self):
        self._gcloud_token_thread = GcloudTokenUpdateThread(self.gcloud, self.gcloud_token_path)
        self._gcloud_token_thread.start()
        # Wait till GcloudTokenUpdateThread get tokens and dump them to gcloud_token_path
        wait_for(os.path.exists, timeout=30, step=5, text="Wait for gcloud token", throw_exc=True,
                 path=self.gcloud_token_path)

    def patch_kube_config(self) -> None:
        # It assumes that config is already created by gcloud
        # It patches kube config so that instead of running gcloud each time
        # we will get it's output from the cache file located at gcloud_token_path
        # To keep this cache file updated we run GcloudTokenUpdateThread thread
        kube_config_path = os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))
        user_name = f"gke_{self.gce_project}_{self.gce_zone}_{self.gke_cluster_name}"
        LOGGER.debug("Patch %s to use dockerized gcloud for auth against GKE cluster `%s'", kube_config_path, self.name)

        with open(kube_config_path) as kube_config:
            data = yaml.safe_load(kube_config)
        user_config = self.get_kubectl_config_for_user(data, user_name)

        if user_config is None:
            raise RuntimeError(f"Unable to find configuration for `{user_name}' in ~/.kube/config")

        user_config["cmd-args"] = self.gcloud_token_path
        user_config["cmd-path"] = "cat"

        with open(kube_config_path, "w") as kube_config:
            yaml.safe_dump(data, kube_config)

        self.log.debug(f'Patched kubectl config at {kube_config_path} '
                       f'with static gcloud config from {self.gcloud_token_path}')

    @cluster.wait_for_init_wrap
    def wait_for_init(self):
        LOGGER.info("--- List of nodes in GKE cluster `%s': ---\n%s\n", self.name, self.kubectl("get nodes").stdout)
        LOGGER.info("--- List of pods in GKE cluster `%s': ---\n%s\n", self.name, self.kubectl("get pods -A").stdout)

        LOGGER.info("Wait for readiness of all pods in default namespace...")
        self.kubectl("wait --timeout=15m --all --for=condition=Ready pod", timeout=15*60+10)

    def destroy(self):
        self.api_call_rate_limiter.stop()
        if self._gcloud_token_thread:
            self._gcloud_token_thread.stop()


class GkeScyllaPodContainer(BaseScyllaPodContainer, IptablesPodIpRedirectMixin):
    parent_cluster: 'GkeScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @cached_property
    def gce_node_ips(self):
        gce_node = self.k8s_node
        return gce_node.public_ips, gce_node.private_ips

    @cached_property
    def hydra_dest_ip(self) -> str:
        if cluster.IP_SSH_CONNECTIONS == "public" or cluster.Setup.INTRA_NODE_COMM_PUBLIC:
            return self.gce_node_ips[0][0]
        return self.gce_node_ips[1][0]

    @cached_property
    def nodes_dest_ip(self) -> str:
        if cluster.Setup.INTRA_NODE_COMM_PUBLIC:
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

    def _instance_wait_safe(self, instance_method, *args, **kwargs):
        """
        Wrapper around GCE instance methods that is safer to use.

        Let's try a method, and if it fails, let's retry using an exponential
        backoff algorithm, similar to what Amazon recommends for it's own
        service [1].

        :see: [1] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        threshold = 300
        ok = False
        retries = 0
        max_retries = 9
        while not ok and retries <= max_retries:
            try:
                return instance_method(*args, **kwargs)
            except Exception as details:  # pylint: disable=broad-except
                self.log.error('Call to method %s (retries: %s) failed: %s',
                               instance_method, retries, details)
                time.sleep(min((2 ** retries) * 2, threshold))
                retries += 1

        if not ok:
            raise cluster.NodeError('GCE instance %s method call error after '
                                    'exponential backoff wait' % self.k8s_node.id)

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
        group_name = self.parent_cluster.k8s_cluster.get_instance_group_name_for_pool(SCYLLA_POD_POOL_NAME)
        super().terminate_k8s_node()

        # Removing GKE instance and adding one node back to the cluster
        # TBD: Remove below lines when https://issuetracker.google.com/issues/178302655 is fixed

        self.parent_cluster.k8s_cluster.delete_instance_that_belong_to_instance_group(group_name, self.node_name)
        self.parent_cluster.k8s_cluster.resize_gke_pool(
            SCYLLA_POD_POOL_NAME,
            self.parent_cluster.k8s_cluster.get_nodes_in_pool(SCYLLA_POD_POOL_NAME)
        )


class GkeScyllaPodCluster(ScyllaPodCluster, IptablesClusterOpsMixin):
    k8s_cluster: 'GkeCluster'
    PodContainerClass = GkeScyllaPodContainer

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
    def install_scylla_manager(self, node, auth_token):
        pass
