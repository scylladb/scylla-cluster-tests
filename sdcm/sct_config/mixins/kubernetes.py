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

"""Kubernetes backends (EKS/GKE/kind) configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class KubernetesConfigMixin(BaseModel):
    """Kubernetes backends (EKS/GKE/kind).

    Scylla Operator deployments: EKS, GKE and local kind clusters.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Kubernetes backends (EKS/GKE/kind)"

    eks_admin_arn: StringOrList = SctField(
        description="ARN(s) of the IAM user or role to be granted cluster admin access",
    )
    eks_cluster_version: String = SctField(
        description="Kubernetes version for the EKS control plane, e.g. '1.30'.",
    )
    eks_nodegroup_role_arn: String = SctField(
        description="ARN of the IAM role for EKS node groups",
    )
    eks_role_arn: String = SctField(
        description="ARN of the IAM role for EKS",
    )
    eks_service_ipv4_cidr: String = SctField(
        description="CIDR block EKS allocates Kubernetes service IPs from, e.g. '10.100.0.0/16'.",
    )
    eks_vpc_cni_version: String = SctField(
        description="Version of the EKS VPC CNI networking plugin to install.",
    )
    # k8s-gke options
    gke_cluster_version: String = SctField(
        description="Specifies the version of the GKE cluster to be used.",
    )
    gke_k8s_release_channel: String = SctField(
        description="K8S release channel name to be used. Expected values are: 'rapid', 'regular', 'stable' and '' (static / No channel).",
    )
    k8s_cert_manager_version: String = SctField(
        description="Specifies the version of the cert-manager to be used in K8S.",
    )
    k8s_connection_bundle_file: String = SctField(
        description="Serverless configuration bundle file.",
    )
    k8s_db_node_service_type: String = SctField(
        description="Defines the type of the K8S 'Service' objects type used for ScyllaDB pods. Empty value means 'do not set and allow scylla-operator to choose'.",
    )
    k8s_db_node_to_client_broadcast_ip_type: String = SctField(
        description="Defines the source of the IP address to be used for the 'broadcast_rpc_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.",
    )
    k8s_db_node_to_node_broadcast_ip_type: String = SctField(
        description="Defines the source of the IP address to be used for the 'broadcast_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.",
    )
    k8s_deploy_monitoring: Boolean = SctField(
        description="Determines if monitoring should be deployed alongside the Scylla cluster.",
    )
    k8s_enable_alternator: Boolean = SctField(
        description="Defines whether we enable the alternator feature using scylla-operator or not.",
    )
    k8s_enable_performance_tuning: Boolean = SctField(
        description="Define whether performance tuning must run or not.",
    )
    k8s_enable_sni: Boolean = SctField(
        description="Defines whether we install SNI and use it or not (serverless feature).",
    )
    k8s_enable_tls: Boolean = SctField(
        description="Defines whether to enable the operator serverless options.",
    )
    k8s_functional_test_dataset: String = SctField(
        description="Defines whether dataset uses for pre-fill cluster in functional test. Defined in sdcm.utils.sstable.load_inventory. Expected values: BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA",
    )
    k8s_instance_type_auxiliary: String = SctField(
        description="Instance type for the nodes of the K8S auxiliary/default node pool.",
    )
    k8s_instance_type_monitor: String = SctField(
        description="Instance type for the nodes of the K8S monitoring node pool.",
    )
    k8s_loader_cluster_name: String = SctField(
        description="Specifies the name of the loader cluster.",
    )
    k8s_loader_run_type: String = SctField(
        description="Defines how the loader pods must run. It may be either 'static' (default, run stress command on the constantly existing idle pod having reserved resources, perf-oriented) or 'dynamic' (run stress command in a separate pod as main thread and get logs in a separate retryable API call not having resource reservations).",
    )
    k8s_local_volume_provisioner_type: String = SctField(
        description="Defines the type of the K8S local volume provisioner to be deployed. It may be either 'static' or 'dynamic'. Details about 'dynamic': 'dynamic': https://github.com/scylladb/k8s-local-volume-provisioner; 'static': sdcm/k8s_configs/static-local-volume-provisioner.yaml",
    )
    k8s_log_api_calls: Boolean = SctField(
        description="Defines whether the K8S API server logging must be enabled and its logs gathered. Be aware that it may be a really huge set of data.",
    )
    k8s_minio_storage_size: String = SctField(
        description="Specifies the storage size for MinIO deployment in K8S.",
    )
    k8s_n_auxiliary_nodes: int = SctField(
        description="Number of nodes in the auxiliary pool.",
    )
    k8s_n_loader_pods_per_cluster: int = SctField(
        description="Number of loader pods per loader cluster.",
    )
    k8s_n_monitor_nodes: int = SctField(
        description="Number of nodes in the monitoring pool that will be used for scylla-operator's deployed monitoring pods.",
    )
    k8s_n_scylla_pods_per_cluster: int = SctField(
        description="Number of Scylla pods per cluster.",
    )
    k8s_scylla_cluster_name: String = SctField(
        description="Specifies the name of the Scylla cluster to be deployed in K8S.",
    )
    k8s_scylla_cpu_limit: String = SctField(
        description="The CPU limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '500m' or '2'",
    )
    k8s_scylla_disk_class: String = SctField(
        description="Specifies the disk class for Scylla pods.",
    )
    k8s_scylla_disk_gi: int = SctField(
        description="Specifies the disk size in GiB for Scylla pods.",
    )
    k8s_scylla_memory_limit: String = SctField(
        description="The memory limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '16384Mi'",
    )
    k8s_scylla_operator_chart_version: String = SctField(
        description="Version of 'scylla-operator' Helm chart to use. If not set then latest one will be used.",
    )
    k8s_scylla_operator_docker_image: String = SctField(
        description="Docker image to be used for installation of Scylla operator.",
    )
    k8s_scylla_operator_helm_repo: String = SctField(
        description="Link to the Helm repository where to get 'scylla-operator' charts from.",
    )
    k8s_scylla_operator_upgrade_chart_version: String = SctField(
        description="Version of 'scylla-operator' Helm chart to use for upgrade.",
    )
    k8s_scylla_operator_upgrade_docker_image: String = SctField(
        description="Docker image to be used for upgrade of Scylla operator.",
    )
    k8s_scylla_operator_upgrade_helm_repo: String = SctField(
        description="Link to the Helm repository where to get 'scylla-operator' charts for upgrade.",
    )
    k8s_scylla_utils_docker_image: String = SctField(
        description="Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when 'k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.",
    )
    k8s_use_chaos_mesh: Boolean = SctField(
        description="Enables chaos-mesh for K8S testing.",
    )
    mini_k8s_version: String = SctField(
        description="Specifies the version of the mini K8S cluster to be used.",
    )
