# scylla-cluster-tests configuration options

#### Appending with environment variables or with config files
* **strings:** can be appended with adding `++` at the beginning of the string:
`export SCT_APPEND_SCYLLA_ARGS="++ --overprovisioned 1"`
* **list:** can be appended by adding `++` as the first item of the list
`export SCT_SCYLLA_D_OVERRIDES_FILES='["++", "extra_file/scylla.d/io.conf"]'`

## **perf_extra_jobs_to_compare** / SCT_PERF_EXTRA_JOBS_TO_COMPARE

Jobs to compare performance results with, for example if running in staging,<br>we still can compare with official jobs

**default:** N/A

**type:** str | list[str] (appendable)


## **perf_simple_query_extra_command** / SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND

Extra command line options to pass to perf_simple_query

**default:** N/A

**type:** str (appendable)


## **config_files** / SCT_CONFIG_FILES

a list of config files that would be used

**default:** N/A

**type:** str | list[str]


## **cluster_backend** / SCT_CLUSTER_BACKEND

backend that will be used, aws/gce/azure/docker/xcloud

**default:** N/A

**type:** str


## **test_method** / SCT_TEST_METHOD

class.method used to run the test. Filled automatically with run-test sct command.

**default:** N/A

**type:** str


## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **cloud_credentials_path** / SCT_CLOUD_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** str (appendable)


## **cloud_prom_bearer_token** / SCT_CLOUD_PROM_BEARER_TOKEN

scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_path** / SCT_CLOUD_PROM_PATH

scylla cloud promproxy path to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_host** / SCT_CLOUD_PROM_HOST

scylla cloud promproxy hostname to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] (appendable)


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'

**default:** N/A

**type:** str


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** str


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** str


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically lookup AMIs for formal versions.<br>WARNING: can't be used together with 'ami_id_db_oracle'

**default:** 2024.1

**type:** str


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-jammy

**type:** str


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** N/A

**type:** str | list[str] (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_repo_loader** / SCT_SCYLLA_REPO_LOADER

Url to the repo of scylla version to install c-s for loader

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


## **manager_version** / SCT_MANAGER_VERSION

Branch of scylla manager server and agent to install. Options in defaults/manager_versions.yaml

**default:** 3.8

**type:** str


## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Branch of scylla manager server and agent to upgrade to. Options in defaults/manager_versions.yaml

**default:** N/A

**type:** str


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Branch of scylla db enterprise to install. Options in defaults/manager_versions.yaml

**default:** 2025

**type:** str


## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION



**default:** 3.8.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** str (appendable)


## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** str (appendable)


## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.13

**type:** str (appendable)


## **user_prefix** / SCT_USER_PREFIX

the prefix of the name of the cloud instances, defaults to username

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


## **sct_public_ip** / SCT_SCT_PUBLIC_IP

Override the default hostname address of the sct test runner,<br>for the monitoring of the Nemesis.<br>can only work out of the box in AWS

**default:** N/A

**type:** str (appendable)


## **sct_ngrok_name** / SCT_SCT_NGROK_NAME

Override the default hostname address of the sct test runner, using ngrok server, see readme for more instructions

**default:** N/A

**type:** str (appendable)


## **backtrace_decoding_disable_regex** / SCT_BACKTRACE_DECODING_DISABLE_REGEX

Regex pattern to disable backtrace decoding for specific event types. If an event type matches<br>this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests<br>by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.

**default:** N/A

**type:** str (appendable)


## **reuse_cluster** / SCT_REUSE_CLUSTER

If reuse_cluster is set it should hold test_id of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`

**default:** N/A

**type:** str (appendable)


## **test_id** / SCT_TEST_ID

Set the test_id of the run manually. Use only from the env before running Hydra

**default:** N/A

**type:** str (appendable)


## **billing_project** / SCT_BILLING_PROJECT

Billing project for the test run. Used for cost tracking and reporting

**default:** N/A

**type:** str (appendable)


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str | list[str] (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Email subject postfix

**default:** N/A

**type:** str (appendable)


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

unlock specified experimental features

**default:** N/A

**type:** str | list[str] (appendable)


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str (appendable)


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Next syntax supporting:<br>- nemesis_class_name: "NemesisName"  Run one nemesis in single thread<br>- nemesis_class_name: "<NemesisName>:<num>" Run <NemesisName> in <num><br>parallel threads on different nodes. Ex.: "ChaosMonkey:2"<br>- nemesis_class_name: "<NemesisName1>:<num1> <NemesisName2>:<num2>" Run<br><NemesisName1> in <num1> parallel threads and <NemesisName2> in <num2><br>parallel threads. Ex.: "DisruptiveMonkey:1 NonDisruptiveMonkey:2"

**default:** NoOpMonkey

**type:** str | list[str] (appendable)


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** str (appendable)


## **stress_cmd** / SCT_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Url of the schema/configuration the gemini tool would use

**default:** N/A

**type:** str (appendable)


## **gemini_cmd** / SCT_GEMINI_CMD

gemini command to run (for now used only in GeminiTest)

**default:** N/A

**type:** str (appendable)


## **instance_type_loader** / SCT_INSTANCE_TYPE_LOADER

AWS image type of the loader node

**default:** N/A

**type:** str (appendable)


## **instance_type_monitor** / SCT_INSTANCE_TYPE_MONITOR

AWS image type of the monitor node

**default:** N/A

**type:** str (appendable)


## **instance_type_db** / SCT_INSTANCE_TYPE_DB

AWS image type of the db node

**default:** N/A

**type:** str (appendable)


## **instance_type_db_oracle** / SCT_INSTANCE_TYPE_DB_ORACLE

AWS image type of the oracle node

**default:** N/A

**type:** str (appendable)


## **instance_type_runner** / SCT_INSTANCE_TYPE_RUNNER

instance type of the sct-runner node

**default:** N/A

**type:** str (appendable)


## **region_name** / SCT_REGION_NAME

AWS regions to use

**default:** N/A

**type:** str | list[str]


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_cassandra** / SCT_AMI_ID_DB_CASSANDRA

AMS AMI id to use for cassandra node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_oracle** / SCT_AMI_ID_DB_ORACLE

AMS AMI id to use for oracle node

**default:** N/A

**type:** str (appendable)


## **ami_id_vector_store** / SCT_AMI_ID_VECTOR_STORE

AMS AMI id to use for vector store node

**default:** N/A

**type:** str (appendable)


## **instance_type_vector_store** / SCT_INSTANCE_TYPE_VECTOR_STORE

AWS/GCP cloud provider instance type for Vector Store nodes

**default:** N/A

**type:** str (appendable)


## **ami_db_scylla_user** / SCT_AMI_DB_SCYLLA_USER



**default:** N/A

**type:** str (appendable)


## **ami_monitor_user** / SCT_AMI_MONITOR_USER



**default:** N/A

**type:** str (appendable)


## **ami_loader_user** / SCT_AMI_LOADER_USER



**default:** N/A

**type:** str (appendable)


## **ami_db_cassandra_user** / SCT_AMI_DB_CASSANDRA_USER



**default:** N/A

**type:** str (appendable)


## **ami_vector_store_user** / SCT_AMI_VECTOR_STORE_USER



**default:** N/A

**type:** str (appendable)


## **aws_instance_profile_name_db** / SCT_AWS_INSTANCE_PROFILE_NAME_DB

This is the name of the instance profile to set on all db instances

**default:** N/A

**type:** str (appendable)


## **aws_instance_profile_name_loader** / SCT_AWS_INSTANCE_PROFILE_NAME_LOADER

This is the name of the instance profile to set on all loader instances

**default:** N/A

**type:** str (appendable)


## **backup_bucket_backend** / SCT_BACKUP_BUCKET_BACKEND

the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')

**default:** N/A

**type:** str (appendable)


## **backup_bucket_location** / SCT_BACKUP_BUCKET_LOCATION

the bucket name to be used for backup (e.g., 'manager-backup-tests')

**default:** N/A

**type:** str | list[str] (appendable)


## **backup_bucket_region** / SCT_BACKUP_BUCKET_REGION

the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')

**default:** N/A

**type:** str (appendable)


## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** N/A

**type:** str | list[str] (appendable)


## **gce_project** / SCT_GCE_PROJECT

gcp project name to use

**default:** N/A

**type:** str (appendable)


## **gce_datacenter** / SCT_GCE_DATACENTER

Supported: us-east1 - means that the zone will be selected automatically or you can mention the zone explicitly, for example: us-east1-b

**default:** N/A

**type:** str


## **gce_network** / SCT_GCE_NETWORK

gce network to use

**default:** N/A

**type:** str (appendable)


## **gce_image_db** / SCT_GCE_IMAGE_DB

gce image to use for db nodes

**default:** N/A

**type:** str (appendable)


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR

gce image to use for monitor nodes

**default:** N/A

**type:** str (appendable)


## **gce_image_loader** / SCT_GCE_IMAGE_LOADER

Google Compute Engine image to use for loader nodes

**default:** N/A

**type:** str (appendable)


## **gce_image_username** / SCT_GCE_IMAGE_USERNAME

Username for the Google Compute Engine image

**default:** N/A

**type:** str (appendable)


## **gce_instance_type_loader** / SCT_GCE_INSTANCE_TYPE_LOADER

Instance type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_loader** / SCT_GCE_ROOT_DISK_TYPE_LOADER

Root disk type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_instance_type_monitor** / SCT_GCE_INSTANCE_TYPE_MONITOR

Instance type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_monitor** / SCT_GCE_ROOT_DISK_TYPE_MONITOR

Root disk type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** str | list[str] (appendable)


## **bisect_start_date** / SCT_BISECT_START_DATE

Start date for bisecting test runs to find regressions

**default:** N/A

**type:** str (appendable)


## **bisect_end_date** / SCT_BISECT_END_DATE

End date for bisecting test runs to find regressions

**default:** N/A

**type:** str (appendable)


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero token node

**default:** i4i.large

**type:** str (appendable)


## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)


## **gce_instance_type_db** / SCT_GCE_INSTANCE_TYPE_DB

Instance type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB

Root disk type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **azure_region_name** / SCT_AZURE_REGION_NAME

Azure region(s) where the resources will be deployed. Supports single or multiple regions.

**default:** N/A

**type:** str | list[str]


## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER

The Azure virtual machine size to be used for loader nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR

The Azure virtual machine size to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB

The Azure virtual machine size to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE

The Azure virtual machine size to be used for Oracle database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_image_db** / SCT_AZURE_IMAGE_DB

The Azure image to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR

The Azure image to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)


## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)


## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME

The username for the Azure image.

**default:** N/A

**type:** str (appendable)


## **eks_service_ipv4_cidr** / SCT_EKS_SERVICE_IPV4_CIDR

EKS service IPv4 CIDR block

**default:** N/A

**type:** str (appendable)


## **eks_vpc_cni_version** / SCT_EKS_VPC_CNI_VERSION

EKS VPC CNI plugin version

**default:** N/A

**type:** str (appendable)


## **eks_role_arn** / SCT_EKS_ROLE_ARN

ARN of the IAM role for EKS

**default:** N/A

**type:** str (appendable)


## **eks_admin_arn** / SCT_EKS_ADMIN_ARN

ARN(s) of the IAM user or role to be granted cluster admin access

**default:** N/A

**type:** str | list[str] (appendable)


## **eks_cluster_version** / SCT_EKS_CLUSTER_VERSION

EKS cluster Kubernetes version

**default:** N/A

**type:** str (appendable)


## **eks_nodegroup_role_arn** / SCT_EKS_NODEGROUP_ROLE_ARN

ARN of the IAM role for EKS node groups

**default:** N/A

**type:** str (appendable)


## **gke_cluster_version** / SCT_GKE_CLUSTER_VERSION

Specifies the version of the GKE cluster to be used.

**default:** N/A

**type:** str (appendable)


## **gke_k8s_release_channel** / SCT_GKE_K8S_RELEASE_CHANNEL

K8S release channel name to be used. Expected values are: 'rapid', 'regular', 'stable' and '' (static / No channel).

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_utils_docker_image** / SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE

Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when 'k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.

**default:** N/A

**type:** str (appendable)


## **k8s_local_volume_provisioner_type** / SCT_K8S_LOCAL_VOLUME_PROVISIONER_TYPE

Defines the type of the K8S local volume provisioner to be deployed. It may be either 'static' or 'dynamic'. Details about 'dynamic': 'dynamic': https://github.com/scylladb/k8s-local-volume-provisioner; 'static': sdcm/k8s_configs/static-local-volume-provisioner.yaml

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_docker_image** / SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE

Docker image to be used for installation of Scylla operator.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_upgrade_docker_image** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE

Docker image to be used for upgrade of Scylla operator.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_helm_repo** / SCT_K8S_SCYLLA_OPERATOR_HELM_REPO

Link to the Helm repository where to get 'scylla-operator' charts from.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_upgrade_helm_repo** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO

Link to the Helm repository where to get 'scylla-operator' charts for upgrade.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_chart_version** / SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION

Version of 'scylla-operator' Helm chart to use. If not set then latest one will be used.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_upgrade_chart_version** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION

Version of 'scylla-operator' Helm chart to use for upgrade.

**default:** N/A

**type:** str (appendable)


## **k8s_functional_test_dataset** / SCT_K8S_FUNCTIONAL_TEST_DATASET

Defines whether dataset uses for pre-fill cluster in functional test. Defined in sdcm.utils.sstable.load_inventory. Expected values: BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_cpu_limit** / SCT_K8S_SCYLLA_CPU_LIMIT

The CPU limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '500m' or '2'

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_memory_limit** / SCT_K8S_SCYLLA_MEMORY_LIMIT

The memory limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '16384Mi'

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_cluster_name** / SCT_K8S_SCYLLA_CLUSTER_NAME

Specifies the name of the Scylla cluster to be deployed in K8S.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_disk_class** / SCT_K8S_SCYLLA_DISK_CLASS

Specifies the disk class for Scylla pods.

**default:** N/A

**type:** str (appendable)


## **k8s_loader_cluster_name** / SCT_K8S_LOADER_CLUSTER_NAME

Specifies the name of the loader cluster.

**default:** N/A

**type:** str (appendable)


## **k8s_loader_run_type** / SCT_K8S_LOADER_RUN_TYPE

Defines how the loader pods must run. It may be either 'static' (default, run stress command on the constantly existing idle pod having reserved resources, perf-oriented) or 'dynamic' (run stress command in a separate pod as main thread and get logs in a separate retryable API call not having resource reservations).

**default:** dynamic

**type:** str (appendable)


## **k8s_instance_type_auxiliary** / SCT_K8S_INSTANCE_TYPE_AUXILIARY

Instance type for the nodes of the K8S auxiliary/default node pool.

**default:** N/A

**type:** str (appendable)


## **k8s_instance_type_monitor** / SCT_K8S_INSTANCE_TYPE_MONITOR

Instance type for the nodes of the K8S monitoring node pool.

**default:** N/A

**type:** str (appendable)


## **mini_k8s_version** / SCT_MINI_K8S_VERSION

Specifies the version of the mini K8S cluster to be used.

**default:** N/A

**type:** str (appendable)


## **k8s_cert_manager_version** / SCT_K8S_CERT_MANAGER_VERSION

Specifies the version of the cert-manager to be used in K8S.

**default:** N/A

**type:** str (appendable)


## **k8s_minio_storage_size** / SCT_K8S_MINIO_STORAGE_SIZE

Specifies the storage size for MinIO deployment in K8S.

**default:** 10Gi

**type:** str (appendable)


## **k8s_connection_bundle_file** / SCT_K8S_CONNECTION_BUNDLE_FILE

Serverless configuration bundle file.

**default:** N/A

**type:** str (appendable)


## **k8s_db_node_service_type** / SCT_K8S_DB_NODE_SERVICE_TYPE

Defines the type of the K8S 'Service' objects type used for ScyllaDB pods. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** str (appendable)


## **k8s_db_node_to_node_broadcast_ip_type** / SCT_K8S_DB_NODE_TO_NODE_BROADCAST_IP_TYPE

Defines the source of the IP address to be used for the 'broadcast_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** str (appendable)


## **k8s_db_node_to_client_broadcast_ip_type** / SCT_K8S_DB_NODE_TO_CLIENT_BROADCAST_IP_TYPE

Defines the source of the IP address to be used for the 'broadcast_rpc_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** str (appendable)


## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.8.0

**type:** str (appendable)


## **docker_image** / SCT_DOCKER_IMAGE

Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version

**default:** N/A

**type:** str (appendable)


## **docker_network** / SCT_DOCKER_NETWORK

Local docker network to use, if there's need to have db cluster connect to other services running in docker

**default:** N/A

**type:** str (appendable)


## **vector_store_docker_image** / SCT_VECTOR_STORE_DOCKER_IMAGE

Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version

**default:** scylladb/vector-store

**type:** str (appendable)


## **vector_store_version** / SCT_VECTOR_STORE_VERSION

Vector Store version / docker image tag

**default:** N/A

**type:** str (appendable)


## **s3_baremetal_config** / SCT_S3_BAREMETAL_CONFIG

Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.

**default:** N/A

**type:** str (appendable)


## **db_nodes_private_ip** / SCT_DB_NODES_PRIVATE_IP

Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] (appendable)


## **db_nodes_public_ip** / SCT_DB_NODES_PUBLIC_IP

Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] (appendable)


## **loaders_private_ip** / SCT_LOADERS_PRIVATE_IP

Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] (appendable)


## **loaders_public_ip** / SCT_LOADERS_PUBLIC_IP

Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] (appendable)


## **monitor_nodes_private_ip** / SCT_MONITOR_NODES_PRIVATE_IP

Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] (appendable)


## **monitor_nodes_public_ip** / SCT_MONITOR_NODES_PUBLIC_IP

Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] (appendable)


## **run_full_partition_scan** / SCT_RUN_FULL_PARTITION_SCAN

Enable or disable running full partition scans during tests

**default:** N/A

**type:** str (appendable)


## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Enable or disable tombstone garbage collection verification during tests

**default:** N/A

**type:** str (appendable)


## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keyspace to be pre-created before running workload

**default:** N/A

**type:** str | list[str] (appendable)


## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** str | list[str] (appendable)


## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Compaction strategy to use for pre-created schema

**default:** SizeTieredCompactionStrategy

**type:** str (appendable)


## **data_validation** / SCT_DATA_VALIDATION

Specify the type of data validation to perform

**default:** N/A

**type:** str (appendable)


## **stress_read_cmd** / SCT_STRESS_READ_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** str (appendable)


## **stress_cmd_w** / SCT_STRESS_CMD_W

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_r** / SCT_STRESS_CMD_R

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_m** / SCT_STRESS_CMD_M

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_read_disk** / SCT_STRESS_CMD_READ_DISK

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARMUP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE



**default:** N/A

**type:** str | list[str] (appendable)


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** N/A

**type:** str | list[str] (appendable)


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** N/A

**type:** str | list[str] (appendable)


## **cs_duration** / SCT_CS_DURATION



**default:** 50m

**type:** str (appendable)


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str | list[str] (appendable)


## **sstable_file** / SCT_SSTABLE_FILE



**default:** N/A

**type:** str (appendable)


## **sstable_url** / SCT_SSTABLE_URL



**default:** N/A

**type:** str (appendable)


## **sstable_md5** / SCT_SSTABLE_MD5



**default:** N/A

**type:** str (appendable)


## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO

URL to the Scylla repository for new versions.

**default:** N/A

**type:** str (appendable)


## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** str (appendable)


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

The target version to upgrade Scylla to.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** str (appendable)


## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** str | list[str] (appendable)


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** str | list[str] (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

**type:** str | list[str] (appendable)


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** str | list[str] (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla option: internode_compression.

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** str (appendable)


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** str (appendable)


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade (prepare stage)

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str | list[str] (appendable)


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str | list[str] (appendable)


## **jepsen_scylla_repo** / SCT_JEPSEN_SCYLLA_REPO

Link to the git repository with Jepsen Scylla tests

**default:** https://github.com/jepsen-io/scylla.git

**type:** str (appendable)


## **jepsen_test_cmd** / SCT_JEPSEN_TEST_CMD

Jepsen test command (e.g., 'test-all')

**default:** ['test-all -w cas-register --concurrency 10n', 'test-all -w counter --concurrency 10n', 'test-all -w cmap --concurrency 10n', 'test-all -w cset --concurrency 10n', 'test-all -w write-isolation --concurrency 10n', 'test-all -w list-append --concurrency 10n', 'test-all -w wr-register --concurrency 10n']

**type:** str | list[str] (appendable)


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str | list[str] (appendable)


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** str | list[str] (appendable)


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** str (appendable)


## **stress_step_duration** / SCT_STRESS_STEP_DURATION

Duration of time for stress round

**default:** 15m

**type:** str (appendable)


## **custom_es_index** / SCT_CUSTOM_ES_INDEX

Use custom ES index for storing test results

**default:** N/A

**type:** str (appendable)


## **xcloud_credentials_path** / SCT_XCLOUD_CREDENTIALS_PATH

Path to Scylla Cloud credentials file, if stored locally

**default:** N/A

**type:** str (appendable)


## **xcloud_env** / SCT_XCLOUD_ENV

Scylla Cloud environment (e.g., lab).

**default:** N/A

**type:** str (appendable)


## **xcloud_provider** / SCT_XCLOUD_PROVIDER

Cloud provider for Scylla Cloud deployment (aws, gce)

**default:** N/A

**type:** str (appendable)


## **argus_email_report_template** / SCT_ARGUS_EMAIL_REPORT_TEMPLATE

Path to the email report template used for sending argus email reports

**default:** email_report_template_basic.yaml

**type:** str (appendable)
