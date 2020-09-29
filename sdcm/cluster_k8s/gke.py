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
from typing import Callable, List
from functools import cached_property

import yaml

from sdcm import cluster
from sdcm.sct_config import sct_abs_path
from sdcm.cluster_k8s import KubernetesCluster, ScyllaPodCluster, BasePodContainer
from sdcm.cluster_k8s.iptables import IptablesPodIpRedirectMixin, IptablesClusterOpsMixin


SCYLLA_CLUSTER_CONFIG = sct_abs_path("sdcm/k8s_configs/cluster-gke.yaml")
CPU_POLICY_DAEMONSET = sct_abs_path("sdcm/k8s_configs/cpu-policy-daemonset.yaml")
RAID_DAEMONSET = sct_abs_path("sdcm/k8s_configs/raid-daemonset.yaml")

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

        super().__init__(cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=gce_datacenter,
                         node_type="scylla-db")

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if not self.gke_cluster_created:
            self.setup_gke_cluster(num_nodes=count)
            self.gke_cluster_created = True
        else:
            raise NotImplementedError

    @cached_property
    def gcloud(self) -> Callable[[str], str]:
        return cluster.Setup.tester_obj().localhost.gcloud

    @cached_property
    def gcloud_container_id(self) -> str:
        return cluster.Setup.tester_obj().localhost.gcloud_container_id

    def setup_gke_cluster(self, num_nodes: int) -> None:
        LOGGER.info("Create GKE cluster `%s' with %d node(s) in default-pool and 1 node in operator-pool",
                    self.name, num_nodes)
        tags = ",".join(f"{key}={value}" for key, value in self.tags.items())
        self.gcloud(f"container --project {self.gce_project} clusters create {self.name} --username admin "
                    f"--zone {self.gce_zone} --cluster-version {self.gke_cluster_version} "
                    f"--machine-type {self.gce_instance_type} --num-nodes {num_nodes} "
                    f"--disk-type {self.gce_image_type} --disk-size {self.gce_image_size} "
                    f"--local-ssd-count {self.gce_n_local_ssd} --node-taints role=scylla-clusters:NoSchedule "
                    f"--image-type UBUNTU --enable-stackdriver-kubernetes --no-enable-autoupgrade "
                    f"--no-enable-autorepair --metadata {tags} --network {self.gce_network}")
        self.gcloud(f"container --project {self.gce_project} node-pools create operator-pool "
                    f"--cluster {self.name} --zone {self.gce_zone} --machine-type n1-standard-4 --num-nodes 1 "
                    f"--disk-type pd-ssd --disk-size 20 --image-type UBUNTU --no-enable-autoupgrade "
                    f"--no-enable-autorepair")

        LOGGER.info("Get credentials for GKE cluster `%s'", self.name)
        self.gcloud(f"container clusters get-credentials {self.name} --zone {self.gce_zone}")
        self.patch_kube_config()

        LOGGER.info("Setup RBAC for GKE cluster `%s'", self.name)
        self.kubectl(
            f"create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin --user {self.gce_user}")

        LOGGER.info("Install RAID DaemonSet to GKE cluster `%s'", self.name)
        self.apply_file(RAID_DAEMONSET, envsubst=False)

        LOGGER.info("Install CPU policy DaemonSet to GKE cluster `%s'", self.name)
        self.apply_file(CPU_POLICY_DAEMONSET, envsubst=False)

        LOGGER.info("Install local volume provisioner to GKE cluster `%s'", self.name)
        self.helm(f"install local-provisioner provisioner")

    def patch_kube_config(self) -> None:
        kube_config_path = os.path.expanduser("~/.kube/config")

        LOGGER.debug("Patch %s to use dockerized gcloud for auth against GKE cluster `%s'", kube_config_path, self.name)
        with open(kube_config_path) as kube_config:
            data = yaml.safe_load(kube_config)

        user_name = f"gke_{self.gce_project}_{self.gce_zone}_{self.name}"
        for user in data["users"]:
            if user["name"] == user_name:
                config = user["user"]["auth-provider"]["config"]
                break
        else:
            raise RuntimeError(f"Unable to find configuration for `{user_name}' in ~/.kube/config")

        config["cmd-args"] = f"exec {self.gcloud_container_id} gcloud config config-helper --format=json"
        config["cmd-path"] = "/usr/bin/docker"

        with open(kube_config_path, "w") as kube_config:
            yaml.safe_dump(data, kube_config)

    @cluster.wait_for_init_wrap
    def wait_for_init(self):
        pass

    def destroy(self):
        pass


class GkeScyllaPodContainer(BasePodContainer, IptablesPodIpRedirectMixin):
    @cached_property
    def gce_node_ips(self):
        gce_node_name = self._pod.spec.node_name
        gce_node = self.parent_cluster.k8s_cluster.gce_services[0].ex_get_node(name=gce_node_name)
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


class GkeScyllaPodCluster(ScyllaPodCluster, IptablesClusterOpsMixin):
    PodContainerClass = GkeScyllaPodContainer

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[GkeScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      enable_auto_bootstrap=enable_auto_bootstrap)

        self.add_hydra_iptables_rules(nodes=new_nodes)
        self.update_nodes_iptables_redirect_rules(nodes=new_nodes)

        return new_nodes
