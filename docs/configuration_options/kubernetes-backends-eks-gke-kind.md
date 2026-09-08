# Kubernetes backends (EKS/GKE/kind)

[← All configuration options](configuration_options.md)

Scylla Operator deployments: EKS, GKE and local kind clusters.

**44 options.** Jump to: [eks_admin_arn](#eks_admin_arn) · [eks_cluster_version](#eks_cluster_version) · [eks_nodegroup_role_arn](#eks_nodegroup_role_arn) · [eks_role_arn](#eks_role_arn) · [eks_service_ipv4_cidr](#eks_service_ipv4_cidr) · [eks_vpc_cni_version](#eks_vpc_cni_version) · [gke_cluster_version](#gke_cluster_version) · [gke_k8s_release_channel](#gke_k8s_release_channel) · [k8s_cert_manager_version](#k8s_cert_manager_version) · [k8s_connection_bundle_file](#k8s_connection_bundle_file) · [k8s_db_node_service_type](#k8s_db_node_service_type) · [k8s_db_node_to_client_broadcast_ip_type](#k8s_db_node_to_client_broadcast_ip_type) · [k8s_db_node_to_node_broadcast_ip_type](#k8s_db_node_to_node_broadcast_ip_type) · [k8s_deploy_monitoring](#k8s_deploy_monitoring) · [k8s_enable_alternator](#k8s_enable_alternator) · [k8s_enable_performance_tuning](#k8s_enable_performance_tuning) · [k8s_enable_sni](#k8s_enable_sni) · [k8s_enable_tls](#k8s_enable_tls) · [k8s_functional_test_dataset](#k8s_functional_test_dataset) · [k8s_instance_type_auxiliary](#k8s_instance_type_auxiliary) · [k8s_instance_type_monitor](#k8s_instance_type_monitor) · [k8s_loader_cluster_name](#k8s_loader_cluster_name) · [k8s_loader_run_type](#k8s_loader_run_type) · [k8s_local_volume_provisioner_type](#k8s_local_volume_provisioner_type) · [k8s_log_api_calls](#k8s_log_api_calls) · [k8s_minio_storage_size](#k8s_minio_storage_size) · [k8s_n_auxiliary_nodes](#k8s_n_auxiliary_nodes) · [k8s_n_loader_pods_per_cluster](#k8s_n_loader_pods_per_cluster) · [k8s_n_monitor_nodes](#k8s_n_monitor_nodes) · [k8s_n_scylla_pods_per_cluster](#k8s_n_scylla_pods_per_cluster) · [k8s_scylla_cluster_name](#k8s_scylla_cluster_name) · [k8s_scylla_cpu_limit](#k8s_scylla_cpu_limit) · [k8s_scylla_disk_class](#k8s_scylla_disk_class) · [k8s_scylla_disk_gi](#k8s_scylla_disk_gi) · [k8s_scylla_memory_limit](#k8s_scylla_memory_limit) · [k8s_scylla_operator_chart_version](#k8s_scylla_operator_chart_version) · [k8s_scylla_operator_docker_image](#k8s_scylla_operator_docker_image) · [k8s_scylla_operator_helm_repo](#k8s_scylla_operator_helm_repo) · [k8s_scylla_operator_upgrade_chart_version](#k8s_scylla_operator_upgrade_chart_version) · [k8s_scylla_operator_upgrade_docker_image](#k8s_scylla_operator_upgrade_docker_image) · [k8s_scylla_operator_upgrade_helm_repo](#k8s_scylla_operator_upgrade_helm_repo) · [k8s_scylla_utils_docker_image](#k8s_scylla_utils_docker_image) · [k8s_use_chaos_mesh](#k8s_use_chaos_mesh) · [mini_k8s_version](#mini_k8s_version)


## **eks_admin_arn** / SCT_EKS_ADMIN_ARN

ARN(s) of the IAM user or role to be granted cluster admin access

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `['arn:aws:iam::797456418907:role/DeveloperAccessRole', 'arn:aws:iam::797456418907:role/DevOpsAccessRole']`: k8s-eks


## **eks_cluster_version** / SCT_EKS_CLUSTER_VERSION

Kubernetes version for the EKS control plane, e.g. '1.30'.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `1.32`: k8s-eks


## **eks_nodegroup_role_arn** / SCT_EKS_NODEGROUP_ROLE_ARN

ARN of the IAM role for EKS node groups

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `arn:aws:iam::797456418907:role/helm-test-worker-nodes-NodeInstanceRole-6ACHDYEKNN3I`: k8s-eks


## **eks_role_arn** / SCT_EKS_ROLE_ARN

ARN of the IAM role for EKS

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `arn:aws:iam::797456418907:role/eksServicePolicy`: k8s-eks


## **eks_service_ipv4_cidr** / SCT_EKS_SERVICE_IPV4_CIDR

CIDR block EKS allocates Kubernetes service IPs from, e.g. '10.100.0.0/16'.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `172.20.0.0/16`: k8s-eks


## **eks_vpc_cni_version** / SCT_EKS_VPC_CNI_VERSION

Version of the EKS VPC CNI networking plugin to install.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `v1.19.2-eksbuild.5`: k8s-eks


## **gke_cluster_version** / SCT_GKE_CLUSTER_VERSION

Specifies the version of the GKE cluster to be used.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `1.31`: k8s-gke


## **gke_k8s_release_channel** / SCT_GKE_K8S_RELEASE_CHANNEL

K8S release channel name to be used. Expected values are: 'rapid', 'regular', 'stable' and '' (static / No channel).

**default:** N/A

**type:** str (appendable)


## **k8s_cert_manager_version** / SCT_K8S_CERT_MANAGER_VERSION

Specifies the version of the cert-manager to be used in K8S.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `1.19.1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **k8s_connection_bundle_file** / SCT_K8S_CONNECTION_BUNDLE_FILE

Serverless configuration bundle file.

**default:** N/A

**type:** str (appendable)


## **k8s_db_node_service_type** / SCT_K8S_DB_NODE_SERVICE_TYPE

Defines the type of the K8S 'Service' objects type used for ScyllaDB pods. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** str (appendable)


## **k8s_db_node_to_client_broadcast_ip_type** / SCT_K8S_DB_NODE_TO_CLIENT_BROADCAST_IP_TYPE

Defines the source of the IP address to be used for the 'broadcast_rpc_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** str (appendable)


## **k8s_db_node_to_node_broadcast_ip_type** / SCT_K8S_DB_NODE_TO_NODE_BROADCAST_IP_TYPE

Defines the source of the IP address to be used for the 'broadcast_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** str (appendable)


## **k8s_deploy_monitoring** / SCT_K8S_DEPLOY_MONITORING

Determines if monitoring should be deployed alongside the Scylla cluster.

**default:** False

**type:** bool


## **k8s_enable_alternator** / SCT_K8S_ENABLE_ALTERNATOR

Defines whether we enable the alternator feature using scylla-operator or not.

**default:** N/A

**type:** bool


## **k8s_enable_performance_tuning** / SCT_K8S_ENABLE_PERFORMANCE_TUNING

Define whether performance tuning must run or not.

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce
- `True`: k8s-gke, k8s-eks


## **k8s_enable_sni** / SCT_K8S_ENABLE_SNI

Defines whether we install SNI and use it or not (serverless feature).

**default:** N/A

**type:** bool


## **k8s_enable_tls** / SCT_K8S_ENABLE_TLS

Defines whether to enable the operator serverless options.

**default:** N/A

**type:** bool


## **k8s_functional_test_dataset** / SCT_K8S_FUNCTIONAL_TEST_DATASET

Defines whether dataset uses for pre-fill cluster in functional test. Defined in sdcm.utils.sstable.load_inventory. Expected values: BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA

**default:** N/A

**type:** str (appendable)


## **k8s_instance_type_auxiliary** / SCT_K8S_INSTANCE_TYPE_AUXILIARY

Instance type for the nodes of the K8S auxiliary/default node pool.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `n2-standard-2`: k8s-gke
- `t3.large`: k8s-eks


## **k8s_instance_type_monitor** / SCT_K8S_INSTANCE_TYPE_MONITOR

Instance type for the nodes of the K8S monitoring node pool.

**default:** N/A

**type:** str (appendable)


## **k8s_loader_cluster_name** / SCT_K8S_LOADER_CLUSTER_NAME

Specifies the name of the loader cluster.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `sct-loaders`: k8s-gke, k8s-eks


## **k8s_loader_run_type** / SCT_K8S_LOADER_RUN_TYPE

Defines how the loader pods must run. It may be either 'static' (default, run stress command on the constantly existing idle pod having reserved resources, perf-oriented) or 'dynamic' (run stress command in a separate pod as main thread and get logs in a separate retryable API call not having resource reservations).

**default:** dynamic

**type:** str (appendable)


## **k8s_local_volume_provisioner_type** / SCT_K8S_LOCAL_VOLUME_PROVISIONER_TYPE

Defines the type of the K8S local volume provisioner to be deployed. It may be either 'static' or 'dynamic'. Details about 'dynamic': 'dynamic': https://github.com/scylladb/k8s-local-volume-provisioner; 'static': sdcm/k8s_configs/static-local-volume-provisioner.yaml

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `dynamic`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **k8s_log_api_calls** / SCT_K8S_LOG_API_CALLS

Defines whether the K8S API server logging must be enabled and its logs gathered. Be aware that it may be a really huge set of data.

**default:** False

**type:** bool


## **k8s_minio_storage_size** / SCT_K8S_MINIO_STORAGE_SIZE

Specifies the storage size for MinIO deployment in K8S.

**default:** 10Gi

**type:** str (appendable)

**backend overrides:**
- `20Gi`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce
- `60Gi`: k8s-gke, k8s-eks


## **k8s_n_auxiliary_nodes** / SCT_K8S_N_AUXILIARY_NODES

Number of nodes in the auxiliary pool.

**default:** N/A

**type:** int

**backend overrides:**
- `2`: k8s-gke
- `3`: k8s-eks


## **k8s_n_loader_pods_per_cluster** / SCT_K8S_N_LOADER_PODS_PER_CLUSTER

Number of loader pods per loader cluster.

**default:** N/A

**type:** int


## **k8s_n_monitor_nodes** / SCT_K8S_N_MONITOR_NODES

Number of nodes in the monitoring pool that will be used for scylla-operator's deployed monitoring pods.

**default:** N/A

**type:** int

**backend overrides:**
- `1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce
- `0`: k8s-gke, k8s-eks


## **k8s_n_scylla_pods_per_cluster** / SCT_K8S_N_SCYLLA_PODS_PER_CLUSTER

Number of Scylla pods per cluster.

**default:** 3

**type:** int


## **k8s_scylla_cluster_name** / SCT_K8S_SCYLLA_CLUSTER_NAME

Specifies the name of the Scylla cluster to be deployed in K8S.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `sct-cluster`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **k8s_scylla_cpu_limit** / SCT_K8S_SCYLLA_CPU_LIMIT

The CPU limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '500m' or '2'

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_disk_class** / SCT_K8S_SCYLLA_DISK_CLASS

Specifies the disk class for Scylla pods.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-local-xfs`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **k8s_scylla_disk_gi** / SCT_K8S_SCYLLA_DISK_GI

Specifies the disk size in GiB for Scylla pods.

**default:** N/A

**type:** int

**backend overrides:**
- `10`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce
- `1100`: k8s-gke
- `3490`: k8s-eks


## **k8s_scylla_memory_limit** / SCT_K8S_SCYLLA_MEMORY_LIMIT

The memory limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '16384Mi'

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_chart_version** / SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION

Version of 'scylla-operator' Helm chart to use. If not set then latest one will be used.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `latest`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **k8s_scylla_operator_docker_image** / SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE

Docker image to be used for installation of Scylla operator.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_helm_repo** / SCT_K8S_SCYLLA_OPERATOR_HELM_REPO

Link to the Helm repository where to get 'scylla-operator' charts from.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://storage.googleapis.com/scylla-operator-charts/latest`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **k8s_scylla_operator_upgrade_chart_version** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION

Version of 'scylla-operator' Helm chart to use for upgrade.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_upgrade_docker_image** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE

Docker image to be used for upgrade of Scylla operator.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_upgrade_helm_repo** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO

Link to the Helm repository where to get 'scylla-operator' charts for upgrade.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_utils_docker_image** / SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE

Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when [`k8s_enable_performance_tuning`](#k8s_enable_performance_tuning) is defined to 'True'. If not set then the default from operator will be used.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb/scylla-enterprise:2021.1.6`: k8s-gke


## **k8s_use_chaos_mesh** / SCT_K8S_USE_CHAOS_MESH

Enables chaos-mesh for K8S testing.

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **mini_k8s_version** / SCT_MINI_K8S_VERSION

Specifies the version of the mini K8S cluster to be used.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `0.20.0`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce
