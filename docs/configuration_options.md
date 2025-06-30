# scylla-cluster-tests configuration options

#### Appending with environment variables or with config files
* **strings:** can be appended with adding `++` at the beginning of the string:
`export SCT_APPEND_SCYLLA_ARGS="++ --overprovisioned 1"`
* **list:** can be appended by adding `++` as the first item of the list
`export SCT_SCYLLA_D_OVERRIDES_FILES='["++", "extra_file/scylla.d/io.conf"]'`

## **config_files** / SCT_CONFIG_FILES

a list of config files that would be used

**default:** N/A

**type:** [str, list[str]]


## **cluster_backend** / SCT_CLUSTER_BACKEND

backend that will be used, aws/gce/docker

**default:** N/A

**type:** [str, NoneType]


## **test_method** / SCT_TEST_METHOD

class.method used to run the test. Filled automatically with run-test sct command.

**default:** N/A

**type:** [str, NoneType]


## **test_duration** / SCT_TEST_DURATION

Test duration (min). Parameter used to keep instances produced by tests<br>and for jenkins pipeline timeout and TimoutThread.

**default:** 60

**type:** int


## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** [str, NoneType]


## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from stress_cmd parameters<br>and is used in test duration calculation

**default:** N/A

**type:** int


## **n_db_nodes** / SCT_N_DB_NODES

Number list of database nodes in multiple data centers.

**default:** N/A

**type:** [int, list[int]]


## **n_test_oracle_db_nodes** / SCT_N_TEST_ORACLE_DB_NODES

Number list of oracle test nodes in multiple data centers.

**default:** 1

**type:** [int, list[int]]


## **n_loaders** / SCT_N_LOADERS

Number list of loader nodes in multiple data centers

**default:** N/A

**type:** [int, list[int]]


## **n_monitor_nodes** / SCT_N_MONITORS_NODES

Number list of monitor nodes in multiple data centers

**default:** 1

**type:** [int, list[int]]


## **intra_node_comm_public** / SCT_INTRA_NODE_COMM_PUBLIC

If True, all communication between nodes are via public addresses

**default:** N/A

**type:** bool


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** [str, NoneType]


## **user_credentials_path** / SCT_USER_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** str


## **cloud_credentials_path** / SCT_CLOUD_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** [str, NoneType]


## **cloud_cluster_id** / SCT_CLOUD_CLUSTER_ID

scylla cloud cluster id

**default:** N/A

**type:** int


## **cloud_prom_bearer_token** / SCT_CLOUD_PROM_BEARER_TOKEN

scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance

**default:** N/A

**type:** [str, NoneType]


## **cloud_prom_path** / SCT_CLOUD_PROM_PATH

scylla cloud promproxy path to federate monitoring data into our monitoring instance

**default:** N/A

**type:** [str, NoneType]


## **cloud_prom_host** / SCT_CLOUD_PROM_HOST

scylla cloud promproxy hostname to federate monitoring data into our monitoring instance

**default:** N/A

**type:** [str, NoneType]


## **ip_ssh_connections** / SCT_IP_SSH_CONNECTIONS

Type of IP used to connect to machine instances.<br>This depends on whether you are running your tests from a machine inside<br>your cloud provider, where it makes sense to use 'private', or outside (use 'public')<br><br>Default: Use public IPs to connect to instances (public)<br>Use private IPs to connect to instances (private)<br>Use IPv6 IPs to connect to instances (ipv6)

**default:** private

**type:** [str, NoneType]


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** [str, NoneType]


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3']

**type:** [str, list[str]]


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** [str, NoneType]


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** [str, NoneType]


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'

**default:** N/A

**type:** [str, NoneType]


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** [str, NoneType]


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** [str, NoneType]


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically lookup AMIs for formal versions.<br>WARNING: can't be used together with 'ami_id_db_oracle'

**default:** 2022.1.14

**type:** [str, NoneType]


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** [str, NoneType]


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-jammy

**type:** [str, NoneType]


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** N/A

**type:** [str, list[str]] (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** [str, NoneType]


## **scylla_repo_loader** / SCT_SCYLLA_REPO_LOADER

Url to the repo of scylla version to install c-s for loader

**default:** https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-5.2.list

**type:** [str, NoneType]


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** [str, NoneType]


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** [str, NoneType]


## **manager_version** / SCT_MANAGER_VERSION

Branch of scylla manager server and agent to install. Options in defaults/manager_versions.yaml

**default:** 3.5

**type:** [str, NoneType]


## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Branch of scylla manager server and agent to upgrade to. Options in defaults/manager_versions.yaml

**default:** N/A

**type:** [str, NoneType]


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Branch of scylla db enterprise to install. Options in defaults/manager_versions.yaml

**default:** 2024

**type:** [str, NoneType]


## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION



**default:** 3.5.0

**type:** [str, NoneType]


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** [str, NoneType]


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** N/A

**type:** bool


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool


## **manager_prometheus_port** / SCT_MANAGER_PROMETHEUS_PORT

Port to be used by the manager to contact Prometheus

**default:** 5090

**type:** int


## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** [str, NoneType]


## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** [str, NoneType]


## **use_ldap** / SCT_USE_LDAP

When defined true, LDAP is going to be used.

**default:** N/A

**type:** bool


## **use_ldap_authorization** / SCT_USE_LDAP_AUTHORIZATION

When defined true, will create a docker container with LDAP and configure scylla.yaml to use it

**default:** N/A

**type:** bool


## **use_ldap_authentication** / SCT_USE_LDAP_AUTHENTICATION

When defined true, will create a docker container with LDAP and configure scylla.yaml to use it

**default:** N/A

**type:** bool


## **prepare_saslauthd** / SCT_PREPARE_SASLAUTHD

When defined true, will install and start saslauthd service

**default:** N/A

**type:** bool


## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** [str, NoneType]


## **parallel_node_operations** / SCT_PARALLEL_NODE_OPERATIONS

When defined true, will run node operations in parallel. Supported operations: startup

**default:** True

**type:** bool


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** [str, NoneType]


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.10

**type:** [str, NoneType]


## **user_prefix** / SCT_USER_PREFIX

the prefix of the name of the cloud instances, defaults to username

**default:** N/A

**type:** [str, NoneType]


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** [str, NoneType]


## **sct_public_ip** / SCT_SCT_PUBLIC_IP

Override the default hostname address of the sct test runner,<br>for the monitoring of the Nemesis.<br>can only work out of the box in AWS

**default:** N/A

**type:** [str, NoneType]


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** N/A

**type:** bool


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** N/A

**type:** bool


## **sct_ngrok_name** / SCT_NGROK_NAME

Override the default hostname address of the sct test runner, using ngrok server, see readme for more instructions

**default:** N/A

**type:** [str, NoneType]


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** bool


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** bool


## **instance_provision** / SCT_INSTANCE_PROVISION

instance_provision: spot|on_demand|spot_fleet

**default:** spot

**type:** Literal[spot, on_demand, spot_fleet, spot_low_price]


## **instance_provision_fallback_on_demand** / SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND

instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected 'instance_provision' type creation failed. Expected values: true|false (default - false

**default:** N/A

**type:** bool


## **reuse_cluster** / SCT_REUSE_CLUSTER

If reuse_cluster is set it should hold test_id of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`

**default:** N/A

**type:** [str, NoneType]


## **test_id** / SCT_TEST_ID

Set the test_id of the run manually. Use only from the env before running Hydra

**default:** N/A

**type:** [str, NoneType]


## **db_nodes_shards_selection** / SCT_NODES_SHARDS_SELECTION

How to select number of shards of Scylla. Expected values: default/random.<br>Default value: 'default'.<br>In case of random option - Scylla will start with different (random) shards on every node of the cluster

**default:** default

**type:** Literal[default, random]


## **seeds_selector** / SCT_SEEDS_SELECTOR

How to select the seeds. Expected values: random/first/all

**default:** all

**type:** Literal[random, first, all]


## **seeds_num** / SCT_SEEDS_NUM

Number of seeds to select

**default:** 1

**type:** int


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** [str, list[str]]


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Email subject postfix

**default:** N/A

**type:** [str, NoneType]


## **enable_test_profiling** / SCT_ENABLE_TEST_PROFILING

Turn on sct profiling

**default:** N/A

**type:** bool


## **ssh_transport** / SSH_TRANSPORT

Set type of ssh library to use. Could be 'fabric' (default) or 'libssh2'

**default:** libssh2

**type:** Literal[libssh2, fabric]


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

unlock specified experimental features

**default:** N/A

**type:** [str, list[str]]


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** N/A

**type:** bool


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** N/A

**type:** bool


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** [str, NoneType]


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** N/A

**type:** int


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal[PasswordAuthenticator, AllowAllAuthenticator, com.scylladb.auth.SaslauthdAuthenticator]


## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** [str, NoneType]


## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** [str, NoneType]


## **authorizer** / SCT_AUTHORIZER

which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer

**default:** N/A

**type:** Literal[AllowAllAuthorizer, CassandraAuthorizer]


## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** bool


## **service_level_shares** / SCT_SERVICE_LEVEL_SHARES

List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]

**default:** [1000]

**type:** list


## **alternator_port** / SCT_ALTERNATOR_PORT

Port to configure for alternator in scylla.yaml

**default:** N/A

**type:** int


## **dynamodb_primarykey_type** / SCT_DYNAMODB_PRIMARYKEY_TYPE

Type of dynamodb table to create with range key or not, can be: HASH,HASH_AND_RANGE

**default:** HASH

**type:** [str, NoneType]


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** [str, NoneType]


## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** N/A

**type:** bool


## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** N/A

**type:** bool


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** [str, NoneType]


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** [str, NoneType]


## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** N/A

**type:** bool


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** [str, NoneType]


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** [str, NoneType]


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** N/A

**type:** [str, NoneType]


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** [str, NoneType]


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Next syntax supporting:<br>- nemesis_class_name: "NemesisName"  Run one nemesis in single thread<br>- nemesis_class_name: "<NemesisName>:<num>" Run <NemesisName> in <num><br>parallel threads on different nodes. Ex.: "ChaosMonkey:2"<br>- nemesis_class_name: "<NemesisName1>:<num1> <NemesisName2>:<num2>" Run<br><NemesisName1> in <num1> parallel threads and <NemesisName2> in <num2><br>parallel threads. Ex.: "DisruptiveMonkey:1 NonDisruptiveMonkey:2"

**default:** NoOpMonkey

**type:** [str, NoneType]


## **nemesis_interval** / SCT_NEMESIS_INTERVAL

Nemesis sleep interval to use if None provided specifically in the test

**default:** 5

**type:** int


## **nemesis_sequence_sleep_between_ops** / SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS

Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests

**default:** N/A

**type:** int


## **nemesis_during_prepare** / SCT_NEMESIS_DURING_PREPARE

Run nemesis during prepare stage of the test

**default:** True

**type:** bool


## **nemesis_seed** / SCT_NEMESIS_SEED

A seed number in order to repeat nemesis sequence as part of SisyphusMonkey

**default:** N/A

**type:** int


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** N/A

**type:** int


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** N/A

**type:** bool


## **stress_cmd** / SCT_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. multiple commands can passed as a list

**default:** N/A

**type:** [str, list[str]]


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Url of the schema/configuration the gemini tool would use

**default:** N/A

**type:** [str, NoneType]


## **gemini_cmd** / SCT_GEMINI_CMD

gemini command to run (for now used only in GeminiTest)

**default:** N/A

**type:** [str, NoneType]


## **gemini_seed** / SCT_GEMINI_SEED

Seed number for gemini command

**default:** N/A

**type:** int


## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']

**default:** N/A

**type:** list


## **instance_type_loader** / SCT_INSTANCE_TYPE_LOADER

AWS image type of the loader node

**default:** N/A

**type:** [str, NoneType]


## **instance_type_monitor** / SCT_INSTANCE_TYPE_MONITOR

AWS image type of the monitor node

**default:** N/A

**type:** [str, NoneType]


## **instance_type_db** / SCT_INSTANCE_TYPE_DB

AWS image type of the db node

**default:** N/A

**type:** [str, NoneType]


## **instance_type_db_oracle** / SCT_INSTANCE_TYPE_DB_ORACLE

AWS image type of the oracle node

**default:** N/A

**type:** [str, NoneType]


## **instance_type_runner** / SCT_INSTANCE_TYPE_RUNNER

instance type of the sct-runner node

**default:** N/A

**type:** [str, NoneType]


## **region_name** / SCT_REGION_NAME

AWS regions to use

**default:** N/A

**type:** [str, list[str]]


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** N/A

**type:** bool


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** [str, NoneType]


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** [str, NoneType]


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** [str, NoneType]


## **ami_id_db_cassandra** / SCT_AMI_ID_DB_CASSANDRA

AMS AMI id to use for cassandra node

**default:** N/A

**type:** [str, NoneType]


## **ami_id_db_oracle** / SCT_AMI_ID_DB_ORACLE

AMS AMI id to use for oracle node

**default:** N/A

**type:** [str, NoneType]


## **root_disk_size_db** / SCT_ROOT_DISK_SIZE_DB



**default:** N/A

**type:** int


## **root_disk_size_monitor** / SCT_ROOT_DISK_SIZE_MONITOR



**default:** N/A

**type:** int


## **root_disk_size_loader** / SCT_ROOT_DISK_SIZE_LOADER



**default:** N/A

**type:** int


## **root_disk_size_runner** / SCT_ROOT_DISK_SIZE_RUNNER

root disk size in Gb for sct-runner

**default:** N/A

**type:** int


## **ami_db_scylla_user** / SCT_AMI_DB_SCYLLA_USER



**default:** N/A

**type:** [str, NoneType]


## **ami_monitor_user** / SCT_AMI_MONITOR_USER



**default:** N/A

**type:** [str, NoneType]


## **ami_loader_user** / SCT_AMI_LOADER_USER



**default:** N/A

**type:** [str, NoneType]


## **ami_db_cassandra_user** / SCT_AMI_DB_CASSANDRA_USER



**default:** N/A

**type:** [str, NoneType]


## **spot_max_price** / SCT_SPOT_MAX_PRICE

The max percentage of the on demand price we set for spot/fleet instances

**default:** N/A

**type:** float


## **extra_network_interface** / SCT_EXTRA_NETWORK_INTERFACE

if true, create extra network interface on each node

**default:** N/A

**type:** bool


## **aws_instance_profile_name_db** / SCT_AWS_INSTANCE_PROFILE_NAME_DB

This is the name of the instance profile to set on all db instances

**default:** N/A

**type:** [str, NoneType]


## **aws_instance_profile_name_loader** / SCT_AWS_INSTANCE_PROFILE_NAME_LOADER

This is the name of the instance profile to set on all loader instances

**default:** N/A

**type:** [str, NoneType]


## **backup_bucket_backend** / SCT_BACKUP_BUCKET_BACKEND

the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')

**default:** N/A

**type:** [str, NoneType]


## **backup_bucket_location** / SCT_BACKUP_BUCKET_LOCATION

the bucket name to be used for backup (e.g., 'manager-backup-tests')

**default:** N/A

**type:** [str, list[str]]


## **backup_bucket_region** / SCT_BACKUP_BUCKET_REGION

the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')

**default:** N/A

**type:** [str, NoneType]


## **use_prepared_loaders** / SCT_USE_PREPARED_LOADERS

If True, we use prepared VMs for loader (instead of using docker images)

**default:** N/A

**type:** bool


## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** N/A

**type:** [str, list[str]]


## **gce_project** / SCT_GCE_PROJECT

gcp project name to use

**default:** N/A

**type:** [str, NoneType]


## **gce_datacenter** / SCT_GCE_DATACENTER

Supported: us-east1 - means that the zone will be selected automatically or you can mention the zone explicitly, for example: us-east1-b

**default:** N/A

**type:** [str, NoneType]


## **gce_network** / SCT_GCE_NETWORK

gce network to use

**default:** N/A

**type:** [str, NoneType]


## **gce_image_db** / SCT_GCE_IMAGE_DB

gce image to use for db nodes

**default:** N/A

**type:** [str, NoneType]


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR

gce image to use for monitor nodes

**default:** N/A

**type:** [str, NoneType]


## **scylla_network_config** / SCT_SCYLLA_NETWORK_CONFIG

Configure Scylla networking with single or multiple NIC/IP combinations.<br>It must be defined for listen_address and rpc_address. For each address mandatory parameters are:<br>- address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication<br>- ip_type: ipv4 or ipv6<br>- public: false or true<br>- nic: number of NIC. 0, 1<br>Supported for AWS only meanwhile

**default:** N/A

**type:** list


## **gce_image_loader** / SCT_GCE_IMAGE_LOADER

Google Compute Engine image to use for loader nodes

**default:** N/A

**type:** [str, NoneType]


## **gce_image_username** / SCT_GCE_IMAGE_USERNAME

Username for the Google Compute Engine image

**default:** N/A

**type:** [str, NoneType]


## **gce_instance_type_loader** / SCT_GCE_INSTANCE_TYPE_LOADER

Instance type for loader nodes in Google Compute Engine

**default:** N/A

**type:** [str, NoneType]


## **gce_root_disk_type_loader** / SCT_GCE_ROOT_DISK_TYPE_LOADER

Root disk type for loader nodes in Google Compute Engine

**default:** N/A

**type:** [str, NoneType]


## **gce_n_local_ssd_disk_loader** / SCT_GCE_N_LOCAL_SSD_DISK_LOADER

Number of local SSD disks for loader nodes in Google Compute Engine

**default:** N/A

**type:** int


## **gce_instance_type_monitor** / SCT_GCE_INSTANCE_TYPE_MONITOR

Instance type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** [str, NoneType]


## **gce_root_disk_type_monitor** / SCT_GCE_ROOT_DISK_TYPE_MONITOR

Root disk type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** [str, NoneType]


## **validate_large_collections** / SCT_VALIDATE_LARGE_COLLECTIONS

Flag to validate large collections in the database

**default:** N/A

**type:** bool


## **run_commit_log_check_thread** / SCT_RUN_COMMIT_LOG_CHECK_THREAD

Flag to run a thread that checks commit logs

**default:** True

**type:** bool


## **teardown_validators** / SCT_TEARDOWN_VALIDATORS

Validators to use during teardown phase

**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}}

**type:** [dict, str]


## **use_capacity_reservation** / SCT_USE_CAPACITY_RESERVATION

Flag to use capacity reservation for instances

**default:** N/A

**type:** bool


## **use_dedicated_host** / SCT_USE_DEDICATED_HOST

Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)

**default:** N/A

**type:** bool


## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** [str, list[str]]


## **post_behavior_dedicated_host** / SCT_POST_BEHAVIOR_DEDICATED_HOST

Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.<br><br>'destroy' - Destroy hosts (default)<br>'keep' - Keep hosts allocated

**default:** N/A

**type:** Literal[keep, destroy]


## **bisect_start_date** / SCT_BISECT_START_DATE

Start date for bisecting test runs to find regressions

**default:** N/A

**type:** [str, NoneType]


## **bisect_end_date** / SCT_BISECT_END_DATE

End date for bisecting test runs to find regressions

**default:** N/A

**type:** [str, NoneType]


## **kafka_backend** / SCT_KAFKA_BACKEND

Type of Kafka backend to use

**default:** N/A

**type:** Union[Literal['localstack', 'vm', 'msk'], NoneType]


## **kafka_connectors** / SCT_KAFKA_CONNECTORS

Kafka connectors to use

**default:** N/A

**type:** list[sdcm.kafka.kafka_config.SctKafkaConfiguration]


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario

**default:** N/A

**type:** [dict, str]


## **use_zero_nodes** / SCT_USE_ZERO_NODES

If True, enable support in SCT of zero nodes (configuration, nemesis)

**default:** N/A

**type:** bool


## **n_db_zero_token_nodes** / SCT_N_DB_ZERO_TOKEN_NODES

Number of zero token nodes in cluster. Value should be set as '0 1 1' for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions

**default:** N/A

**type:** [int, list[int]]


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero token node

**default:** i4i.large

**type:** [str, NoneType]


## **sct_aws_account_id** / SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** [str, NoneType]


## **latency_decorator_error_thresholds** / SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS

Error thresholds for latency decorator. Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}

**default:** {'write': {'default': {'P90 write': {'fixed_limit': 5}, 'P99 write': {'fixed_limit': 10}}}, 'read': {'default': {'P90 read': {'fixed_limit': 5}, 'P99 read': {'fixed_limit': 10}}}, 'mixed': {'default': {'P90 write': {'fixed_limit': 5}, 'P90 read': {'fixed_limit': 5}, 'P99 write': {'fixed_limit': 10}, 'P99 read': {'fixed_limit': 10}}}}

**type:** [dict, str]


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** [str, NoneType]


## **adaptive_timeout_store_metrics** / SCT_ADAPTIVE_TIMEOUT_STORE_METRICS

Store adaptive timeout metrics in Argus. Disabled for performance tests only.

**default:** True

**type:** bool


## **gce_n_local_ssd_disk_monitor** / SCT_GCE_N_LOCAL_SSD_DISK_MONITOR

Number of local SSD disks for monitor nodes in Google Compute Engine

**default:** N/A

**type:** int


## **gce_instance_type_db** / SCT_GCE_INSTANCE_TYPE_DB

Instance type for database nodes in Google Compute Engine

**default:** N/A

**type:** [str, NoneType]


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB

Root disk type for database nodes in Google Compute Engine

**default:** N/A

**type:** [str, NoneType]


## **gce_n_local_ssd_disk_db** / SCT_GCE_N_LOCAL_SSD_DISK_DB

Number of local SSD disks for database nodes in Google Compute Engine

**default:** N/A

**type:** int


## **gce_pd_standard_disk_size_db** / SCT_GCE_PD_STANDARD_DISK_SIZE_DB

The size of the standard persistent disk in GB used for GCE database nodes

**default:** N/A

**type:** int


## **gce_pd_ssd_disk_size_db** / SCT_GCE_PD_SSD_DISK_SIZE_DB



**default:** N/A

**type:** int


## **gce_setup_hybrid_raid** / SCT_GCE_SETUP_HYBRID_RAID

If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data

**default:** N/A

**type:** bool


## **gce_pd_ssd_disk_size_loader** / SCT_GCE_PD_SSD_DISK_SIZE_LOADER



**default:** N/A

**type:** int


## **gce_pd_ssd_disk_size_monitor** / SCT_GCE_SSD_DISK_SIZE_MONITOR



**default:** N/A

**type:** int


## **azure_region_name** / SCT_AZURE_REGION_NAME

Azure region(s) where the resources will be deployed. Supports single or multiple regions.

**default:** N/A

**type:** [str, list[str]]


## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER

The Azure virtual machine size to be used for loader nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR

The Azure virtual machine size to be used for monitor nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB

The Azure virtual machine size to be used for database nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE

The Azure virtual machine size to be used for Oracle database nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_image_db** / SCT_AZURE_IMAGE_DB

The Azure image to be used for database nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR

The Azure image to be used for monitor nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** [str, NoneType]


## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME

The username for the Azure image.

**default:** N/A

**type:** [str, NoneType]


## **eks_service_ipv4_cidr** / SCT_EKS_SERVICE_IPV4_CIDR

EKS service IPv4 CIDR block

**default:** N/A

**type:** [str, NoneType]


## **eks_vpc_cni_version** / SCT_EKS_VPC_CNI_VERSION

EKS VPC CNI plugin version

**default:** N/A

**type:** [str, NoneType]


## **eks_role_arn** / SCT_EKS_ROLE_ARN

ARN of the IAM role for EKS

**default:** N/A

**type:** [str, NoneType]


## **eks_cluster_version** / SCT_EKS_CLUSTER_VERSION

EKS cluster Kubernetes version

**default:** N/A

**type:** [str, NoneType]


## **eks_nodegroup_role_arn** / SCT_EKS_NODEGROUP_ROLE_ARN

ARN of the IAM role for EKS node groups

**default:** N/A

**type:** [str, NoneType]


## **gke_cluster_version** / SCT_GKE_CLUSTER_VERSION

Specifies the version of the GKE cluster to be used.

**default:** N/A

**type:** [str, NoneType]


## **gke_k8s_release_channel** / SCT_GKE_K8S_RELEASE_CHANNEL

K8S release channel name to be used. Expected values are: 'rapid', 'regular', 'stable' and '' (static / No channel).

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_utils_docker_image** / SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE

Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when 'k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.

**default:** N/A

**type:** [str, NoneType]


## **k8s_enable_performance_tuning** / SCT_K8S_ENABLE_PERFORMANCE_TUNING

Define whether performance tuning must run or not.

**default:** N/A

**type:** bool


## **k8s_deploy_monitoring** / SCT_K8S_DEPLOY_MONITORING

Determines if monitoring should be deployed alongside the Scylla cluster.

**default:** N/A

**type:** bool


## **k8s_local_volume_provisioner_type** / SCT_K8S_LOCAL_VOLUME_PROVISIONER_TYPE

Defines the type of the K8S local volume provisioner to be deployed. It may be either 'static' or 'dynamic'. Details about 'dynamic': 'dynamic': https://github.com/scylladb/k8s-local-volume-provisioner; 'static': sdcm/k8s_configs/static-local-volume-provisioner.yaml

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_operator_docker_image** / SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE

Docker image to be used for installation of Scylla operator.

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_operator_upgrade_docker_image** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE

Docker image to be used for upgrade of Scylla operator.

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_operator_helm_repo** / SCT_K8S_SCYLLA_OPERATOR_HELM_REPO

Link to the Helm repository where to get 'scylla-operator' charts from.

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_operator_upgrade_helm_repo** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO

Link to the Helm repository where to get 'scylla-operator' charts for upgrade.

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_operator_chart_version** / SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION

Version of 'scylla-operator' Helm chart to use. If not set then latest one will be used.

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_operator_upgrade_chart_version** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION

Version of 'scylla-operator' Helm chart to use for upgrade.

**default:** N/A

**type:** [str, NoneType]


## **k8s_functional_test_dataset** / SCT_K8S_FUNCTIONAL_TEST_DATASET

Defines whether dataset uses for pre-fill cluster in functional test. Defined in sdcm.utils.sstable.load_inventory. Expected values: BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_cpu_limit** / SCT_K8S_SCYLLA_CPU_LIMIT

The CPU limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '500m' or '2'

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_memory_limit** / SCT_K8S_SCYLLA_MEMORY_LIMIT

The memory limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '16384Mi'

**default:** N/A

**type:** [str, NoneType]


## **k8s_scylla_cluster_name** / SCT_K8S_SCYLLA_CLUSTER_NAME

Specifies the name of the Scylla cluster to be deployed in K8S.

**default:** N/A

**type:** [str, NoneType]


## **k8s_n_scylla_pods_per_cluster** / K8S_N_SCYLLA_PODS_PER_CLUSTER

Number of Scylla pods per cluster.

**default:** 3

**type:** int


## **k8s_scylla_disk_gi** / SCT_K8S_SCYLLA_DISK_GI

Specifies the disk size in GiB for Scylla pods.

**default:** N/A

**type:** int


## **k8s_scylla_disk_class** / SCT_K8S_SCYLLA_DISK_CLASS

Specifies the disk class for Scylla pods.

**default:** N/A

**type:** [str, NoneType]


## **k8s_loader_cluster_name** / SCT_K8S_LOADER_CLUSTER_NAME

Specifies the name of the loader cluster.

**default:** N/A

**type:** [str, NoneType]


## **k8s_n_loader_pods_per_cluster** / SCT_K8S_N_LOADER_PODS_PER_CLUSTER

Number of loader pods per loader cluster.

**default:** N/A

**type:** int


## **k8s_loader_run_type** / SCT_K8S_LOADER_RUN_TYPE

Defines how the loader pods must run. It may be either 'static' (default, run stress command on the constantly existing idle pod having reserved resources, perf-oriented) or 'dynamic' (run stress command in a separate pod as main thread and get logs in a separate retryable API call not having resource reservations).

**default:** dynamic

**type:** [str, NoneType]


## **k8s_instance_type_auxiliary** / SCT_K8S_INSTANCE_TYPE_AUXILIARY

Instance type for the nodes of the K8S auxiliary/default node pool.

**default:** N/A

**type:** [str, NoneType]


## **k8s_instance_type_monitor** / SCT_K8S_INSTANCE_TYPE_MONITOR

Instance type for the nodes of the K8S monitoring node pool.

**default:** N/A

**type:** [str, NoneType]


## **mini_k8s_version** / SCT_MINI_K8S_VERSION

Specifies the version of the mini K8S cluster to be used.

**default:** N/A

**type:** [str, NoneType]


## **k8s_cert_manager_version** / SCT_K8S_CERT_MANAGER_VERSION

Specifies the version of the cert-manager to be used in K8S.

**default:** N/A

**type:** [str, NoneType]


## **k8s_minio_storage_size** / SCT_K8S_MINIO_STORAGE_SIZE

Specifies the storage size for MinIO deployment in K8S.

**default:** 10Gi

**type:** [str, NoneType]


## **k8s_log_api_calls** / SCT_K8S_LOG_API_CALLS

Defines whether the K8S API server logging must be enabled and its logs gathered. Be aware that it may be a really huge set of data.

**default:** N/A

**type:** bool


## **k8s_tenants_num** / SCT_TENANTS_NUM

Number of Scylla clusters to create in the K8S cluster.

**default:** 1

**type:** int


## **k8s_enable_tls** / SCT_K8S_ENABLE_TLS

Defines whether to enable the operator serverless options.

**default:** N/A

**type:** bool


## **k8s_enable_sni** / SCT_K8S_ENABLE_SNI

Defines whether we install SNI and use it or not (serverless feature).

**default:** N/A

**type:** bool


## **k8s_enable_alternator** / SCT_K8S_ENABLE_ALTERNATOR

Defines whether we enable the alternator feature using scylla-operator or not.

**default:** N/A

**type:** bool


## **k8s_connection_bundle_file** / SCT_K8S_CONNECTION_BUNDLE_FILE

Serverless configuration bundle file.

**default:** N/A

**type:** [str, NoneType]


## **k8s_db_node_service_type** / SCT_K8S_DB_NODE_SERVICE_TYPE

Defines the type of the K8S 'Service' objects type used for ScyllaDB pods. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** [str, NoneType]


## **k8s_db_node_to_node_broadcast_ip_type** / SCT_K8S_DB_NODE_TO_NODE_BROADCAST_IP_TYPE

Defines the source of the IP address to be used for the 'broadcast_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** [str, NoneType]


## **k8s_db_node_to_client_broadcast_ip_type** / SCT_K8S_DB_NODE_TO_CLIENT_BROADCAST_IP_TYPE

Defines the source of the IP address to be used for the 'broadcast_rpc_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.

**default:** N/A

**type:** [str, NoneType]


## **k8s_use_chaos_mesh** / SCT_K8S_USE_CHAOS_MESH

Enables chaos-mesh for K8S testing.

**default:** N/A

**type:** bool


## **k8s_n_auxiliary_nodes** / SCT_K8S_N_AUXILIARY_NODES

Number of nodes in the auxiliary pool.

**default:** N/A

**type:** int


## **k8s_n_monitor_nodes** / SCT_K8S_N_MONITOR_NODES

Number of nodes in the monitoring pool that will be used for scylla-operator's deployed monitoring pods.

**default:** N/A

**type:** int


## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.5.0

**type:** [str, NoneType]


## **docker_image** / SCT_DOCKER_IMAGE

Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version

**default:** N/A

**type:** [str, NoneType]


## **docker_network** / SCT_DOCKER_NETWORK

Local docker network to use, if there's need to have db cluster connect to other services running in docker

**default:** N/A

**type:** [str, NoneType]


## **s3_baremetal_config** / SCT_S3_BAREMETAL_CONFIG

Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.

**default:** N/A

**type:** [str, NoneType]


## **db_nodes_private_ip** / SCT_DB_NODES_PRIVATE_IP

Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** [str, list[str]]


## **db_nodes_public_ip** / SCT_DB_NODES_PUBLIC_IP

Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** [str, list[str]]


## **loaders_private_ip** / SCT_LOADERS_PRIVATE_IP

Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** [str, list[str]]


## **loaders_public_ip** / SCT_LOADERS_PUBLIC_IP

Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** [str, list[str]]


## **monitor_nodes_private_ip** / SCT_MONITOR_NODES_PRIVATE_IP

Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** [str, list[str]]


## **monitor_nodes_public_ip** / SCT_MONITOR_NODES_PUBLIC_IP

Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** [str, list[str]]


## **cassandra_stress_population_size** / SCT_CASSANDRA_STRESS_POPULATION_SIZE

The total population size over which the Cassandra stress tests are run.

**default:** 1000000

**type:** int


## **cassandra_stress_threads** / SCT_CASSANDRA_STRESS_THREADS

The number of threads used by Cassandra stress tests.

**default:** 1000

**type:** int


## **add_node_cnt** / SCT_ADD_NODE_CNT

The number of nodes to add during the test.

**default:** 1

**type:** int


## **stress_multiplier** / SCT_STRESS_MULTIPLIER

Multiplier for stress command intensity

**default:** 1

**type:** int


## **stress_multiplier_w** / SCT_STRESS_MULTIPLIER_W

Write stress command intensity multiplier

**default:** 1

**type:** int


## **stress_multiplier_r** / SCT_STRESS_MULTIPLIER_R

Read stress command intensity multiplier

**default:** 1

**type:** int


## **stress_multiplier_m** / SCT_STRESS_MULTIPLIER_M

Mixed operations stress command intensity multiplier

**default:** 1

**type:** int


## **run_fullscan** / SCT_RUN_FULLSCAN

Enable or disable running full scans during tests

**default:** N/A

**type:** list


## **run_full_partition_scan** / SCT_run_full_partition_scan

Enable or disable running full partition scans during tests

**default:** N/A

**type:** [str, NoneType]


## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Enable or disable tombstone garbage collection verification during tests

**default:** N/A

**type:** [str, NoneType]


## **keyspace_num** / SCT_KEYSPACE_NUM

Number of keyspaces to use in the test

**default:** 1

**type:** int


## **round_robin** / SCT_ROUND_ROBIN

Enable or disable round robin selection of nodes for operations

**default:** N/A

**type:** bool


## **batch_size** / SCT_BATCH_SIZE

Batch size for operations

**default:** 1

**type:** int


## **pre_create_schema** / SCT_PRE_CREATE_SCHEMA

Enable or disable pre-creation of schema before running workload

**default:** N/A

**type:** bool


## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keyspace to be pre-created before running workload

**default:** N/A

**type:** [str, list[str]]


## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** [str, list[str]]


## **prepare_wait_no_compactions_timeout** / SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT

Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load

**default:** N/A

**type:** int


## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Compaction strategy to use for pre-created schema

**default:** SizeTieredCompactionStrategy

**type:** [str, NoneType]


## **sstable_size** / SSTABLE_SIZE

Configure sstable size for pre-create-schema mode

**default:** N/A

**type:** int


## **cluster_health_check** / SCT_CLUSTER_HEALTH_CHECK

Enable or disable starting cluster health checker for all nodes

**default:** True

**type:** bool


## **data_validation** / SCT_DATA_VALIDATION

Specify the type of data validation to perform

**default:** N/A

**type:** [str, NoneType]


## **stress_read_cmd** / SCT_STRESS_READ_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** [str, list[str]]


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** [str, list[str]]


## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

Number of user profile tables to create for the test

**default:** 1

**type:** int


## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** N/A

**type:** bool


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** [str, NoneType]


## **mgmt_restore_params** / SCT_MGMT_RESTORE_PARAMS

Manager restore operation specific parameters: batch_size, parallel. For example, {'batch_size': 100, 'parallel': 10}

**default:** N/A

**type:** [dict, str, pydantic.main.BaseModel]


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** [dict, str, pydantic.main.BaseModel]


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** [str, NoneType]


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** [str, NoneType]


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** [str, NoneType]


## **mgmt_prepare_snapshot_size** / SCT_MGMT_PREPARE_SNAPSHOT_SIZE

Size of backup snapshot in Gb to be prepared for backup

**default:** N/A

**type:** int


## **mgmt_snapshots_preparer_params** / SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS

Custom parameters of c-s write operation used in snapshots preparer

**default:** {'cs_cmd_template': "cassandra-stress {operation} cl={cl} n={num_of_rows} -schema 'keyspace={ks_name} replication(strategy={replication},replication_factor={rf}) compaction(strategy={compaction})' -mode cql3 native -rate threads={threads_num} -col 'size=FIXED({col_size}) n=FIXED({col_n})' -pop seq={sequence_start}..{sequence_end}", 'operation': 'write', 'cl': 'QUORUM', 'replication': 'NetworkTopologyStrategy', 'rf': 3, 'compaction': 'IncrementalCompactionStrategy', 'threads_num': 500, 'col_size': 1024, 'col_n': 1, 'ks_name': '', 'num_of_rows': '', 'sequence_start': '', 'sequence_end': ''}

**type:** [dict, str]


## **stress_cmd_w** / SCT_STRESS_CMD_W

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_r** / SCT_STRESS_CMD_R

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_m** / SCT_STRESS_CMD_M

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE



**default:** N/A

**type:** [str, NoneType]


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** N/A

**type:** [str, list[str]]


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** N/A

**type:** [str, list[str]]


## **cs_duration** / SCT_CS_DURATION



**default:** 50m

**type:** [str, NoneType]


## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** bool


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** [str, list[str]]


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** [dict, str]


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Example: {'read': ['100000', '150000'], 'mixed': ['300']}

**default:** N/A

**type:** [dict, str]


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** [dict, str]


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** [str, NoneType]


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** [str, NoneType]


## **skip_download** / SCT_SKIP_DOWNLOAD



**default:** N/A

**type:** bool


## **sstable_file** / SCT_SSTABLE_FILE



**default:** N/A

**type:** [str, NoneType]


## **sstable_url** / SCT_SSTABLE_URL



**default:** N/A

**type:** [str, NoneType]


## **sstable_md5** / SCT_SSTABLE_MD5



**default:** N/A

**type:** [str, NoneType]


## **flush_times** / SCT_FLUSH_TIMES



**default:** N/A

**type:** int


## **flush_period** / SCT_FLUSH_PERIOD



**default:** N/A

**type:** int


## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO

URL to the Scylla repository for new versions.

**default:** N/A

**type:** [str, NoneType]


## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** [str, NoneType]


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

The target version to upgrade Scylla to.

**default:** N/A

**type:** [str, NoneType]


## **disable_raft** / SCT_DISABLE_RAFT

Flag to disable Raft consensus for LWT operations.

**default:** True

**type:** bool


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade

**default:** N/A

**type:** bool


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** N/A

**type:** bool


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** [str, NoneType]


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default.

**default:** True

**type:** bool


## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** [str, list[str]]


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** [str, list[str]]


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

**type:** [str, list[str]]


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** [str, list[str]]


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** [str, list[str]]


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** [str, list[str]]


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** [str, NoneType]


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** N/A

**type:** bool


## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** syslog-ng

**type:** Literal[ssh, docker, syslog-ng, vector]


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** N/A

**type:** bool


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** N/A

**type:** bool


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal[destroy, keep, keep-on-failure]


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal[destroy, keep, keep-on-failure]


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal[destroy, keep, keep-on-failure]


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal[destroy, keep, keep-on-failure]


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla option: internode_compression.

**default:** N/A

**type:** [str, NoneType]


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** [str, NoneType]


## **jmx_heap_memory** / SCT_JMX_HEAP_MEMORY

The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).

**default:** N/A

**type:** int


## **loader_swap_size** / SCT_LOADER_SWAP_SIZE

The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **monitor_swap_size** / SCT_MONITOR_SWAP_SIZE

The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **store_perf_results** / SCT_STORE_PERF_RESULTS

A flag that indicates whether or not to gather the prometheus stats at the end of the run. Intended to be used in performance testing

**default:** N/A

**type:** bool


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** [str, NoneType]


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** N/A

**type:** bool


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** [str, NoneType]


## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** N/A

**type:** bool


## **stop_test_on_stress_failure** / SCT_STOP_TEST_ON_STRESS_FAILURE

If set to True the test will be stopped immediately when stress command failed.<br>When set to False the test will continue to run even when there are errors in the<br>stress process

**default:** True

**type:** bool


## **stress_cdc_log_reader_batching_enable** / SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE

retrieving data from multiple streams in one poll

**default:** True

**type:** bool


## **use_legacy_cluster_init** / SCT_USE_LEGACY_CLUSTER_INIT

Use legacy cluster initialization with autobootsrap disabled and parallel node setup

**default:** N/A

**type:** bool


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** [str, NoneType]


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Try all availability zones one by one in order to maximize the chances of getting the requested instance capacity.

**default:** N/A

**type:** bool


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** int


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** bool


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade (prepare stage)

**default:** N/A

**type:** [str, NoneType]


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** [str, NoneType]


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** [str, NoneType]


## **jepsen_scylla_repo** / SCT_JEPSEN_SCYLLA_REPO

Link to the git repository with Jepsen Scylla tests

**default:** https://github.com/jepsen-io/scylla.git

**type:** [str, NoneType]


## **jepsen_test_cmd** / SCT_JEPSEN_TEST_CMD

Jepsen test command (e.g., 'test-all')

**default:** ['test-all -w cas-register --concurrency 10n', 'test-all -w counter --concurrency 10n', 'test-all -w cmap --concurrency 10n', 'test-all -w cset --concurrency 10n', 'test-all -w write-isolation --concurrency 10n', 'test-all -w list-append --concurrency 10n', 'test-all -w wr-register --concurrency 10n']

**type:** [str, list[str]]


## **jepsen_test_count** / SCT_JEPSEN_TEST_COUNT

Possible number of reruns of single Jepsen test command

**default:** 1

**type:** int


## **jepsen_test_run_policy** / SCT_JEPSEN_TEST_RUN_POLICY

Jepsen test run policy (i.e., what we want to consider as passed for a single test)<br><br>'most' - most test runs are passed<br>'any'  - one pass is enough<br>'all'  - all test runs should pass

**default:** all

**type:** Literal[most, any, all]


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** [str, list[str]]


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** N/A

**type:** bool


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Limit number events in email reports

**default:** 10

**type:** int


## **data_volume_disk_num** / SCT_DATA_VOLUME_DISK_NUM

Number of additional data volumes attached to instances<br>if data_volume_disk_num > 0, then data volumes (ebs on aws) will be<br>used for scylla data directory

**default:** N/A

**type:** int


## **data_volume_disk_type** / SCT_DATA_VOLUME_DISK_TYPE

Type of additional volumes: gp2|gp3|io2|io3

**default:** N/A

**type:** Literal[gp2, gp3, io2, io3, ]


## **data_volume_disk_size** / SCT_DATA_VOLUME_DISK_SIZE

Size of additional volume in GB

**default:** N/A

**type:** int


## **data_volume_disk_iops** / SCT_DATA_VOLUME_DISK_IOPS

Number of iops for ebs type io2|io3|gp3

**default:** N/A

**type:** int


## **run_db_node_benchmarks** / SCT_RUN_DB_NODE_BENCHMARKS

Flag for running db node benchmarks before the tests

**default:** N/A

**type:** bool


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** [str, list[str]]


## **nemesis_exclude_disabled** / SCT_NEMESIS_EXCLUDE_DISABLED

nemesis_exclude_disabled determines whether 'disabled' nemeses are filtered out from list<br>or are allowed to be used. This allows to easily disable too 'risky' or 'extreme' nemeses by default,<br>for all longevities. For example: it is unwanted to run the ToggleGcModeMonkey in standard longevities<br>that runs a stress with data validation.

**default:** True

**type:** bool


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 6

**type:** int


## **raid_level** / SCT_RAID_LEVEL

Number of of raid level: 0 - RAID0, 5 - RAID5

**default:** N/A

**type:** int


## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** N/A

**type:** bool


## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** N/A

**type:** [dict, str]


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** bool


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** [str, NoneType]


## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.<br>For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}

**default:** N/A

**type:** [dict, str]


## **num_loaders_step** / SCT_NUM_LOADERS_STEP

Number of loaders which should be added per step

**default:** N/A

**type:** int


## **stress_threads_start_num** / SCT_STRESS_THREADS_START_NUM

Number of threads for c-s command

**default:** N/A

**type:** int


## **num_threads_step** / SCT_NUM_THREADS_STEP

Number of threads which should be added on per step

**default:** N/A

**type:** int


## **stress_step_duration** / SCT_STRESS_STEP_DURATION

Duration of time for stress round

**default:** 15m

**type:** [str, NoneType]


## **max_deviation** / SCT_MAX_DEVIATION

Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one

**default:** N/A

**type:** float


## **n_stress_process** / SCT_N_STRESS_PROCESS

Number of stress processes per loader

**default:** N/A

**type:** int


## **stress_process_step** / SCT_STRESS_PROCESS_STEP

add/remove num of process on each round

**default:** N/A

**type:** int


## **use_hdrhistogram** / SCT_USE_HDRHISTOGRAM

Enable hdr histogram logging for cs

**default:** N/A

**type:** bool


## **stop_on_hw_perf_failure** / SCT_STOP_ON_HW_PERF_FAILURE

Stop sct performance test if hardware performance test failed<br><br>Hardware performance tests runs on each node with sysbench and cassandra-fio tools.<br>Results stored in ES. HW perf tests run during cluster setups and not affect<br>SCT Performance tests. Results calculated as average among all results for certain<br>instance type or among all nodes during single run.<br>if results for a single node is not in margin 0.01 of<br>average result for all nodes, hw test considered as Failed.<br>If stop_on_hw_perf_failure is True, then sct performance test will be terminated<br>after hw perf tests detect node with hw results not in margin with average<br>If stop_on_hw_perf_failure is False, then sct performance test will be run<br>even after hw perf tests detect node with hw results not in margin with average

**default:** N/A

**type:** bool


## **custom_es_index** / SCT_CUSTOM_ES_INDEX

Use custom ES index for storing test results

**default:** N/A

**type:** [str, NoneType]


## **simulated_regions** / SCT_SIMULATED_REGIONS

Number of simulated regions for the test

**default:** N/A

**type:** int


## **simulated_racks** / SCT_SIMULATED_RACKS

Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.<br>Provide number of racks to simulate.

**default:** 3

**type:** int


## **rack_aware_loader** / SCT_RACK_AWARE_LOADER

When enabled, loaders will look for nodes on the same rack.

**default:** N/A

**type:** bool


## **use_dns_names** / SCT_USE_DNS_NAMES

Use dns names instead of ip addresses for nodes in cluster

**default:** N/A

**type:** bool
