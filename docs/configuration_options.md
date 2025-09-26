<<<<<<< HEAD
# scylla-cluster-tests configuration options

#### Appending with environment variables or with config files
* **strings:** can be appended with adding `++` at the beginning of the string:
`export SCT_APPEND_SCYLLA_ARGS="++ --overprovisioned 1"`
* **list:** can be appended by adding `++` as the first item of the list
`export SCT_SCYLLA_D_OVERRIDES_FILES='["++", "extra_file/scylla.d/io.conf"]'`

## **config_files** / SCT_CONFIG_FILES

a list of config files that would be used

**default:** N/A

**type:** str_or_list_or_eval


## **cluster_backend** / SCT_CLUSTER_BACKEND

backend that will be used, aws/gce/azure/docker/xcloud

**default:** N/A

**type:** str


## **test_method** / SCT_TEST_METHOD

class.method used to run the test. Filled automatically with run-test sct command.

**default:** N/A

**type:** str


## **test_duration** / SCT_TEST_DURATION

Test duration (min). Parameter used to keep instances produced by tests<br>and for jenkins pipeline timeout and TimoutThread.

**default:** 60

**type:** int


## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from stress_cmd parameters<br>and is used in test duration calculation

**default:** N/A

**type:** int


## **alternator_stress_rate** / SCT_ALTERNATOR_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing.

**default:** N/A

**type:** int


## **alternator_write_always_lwt_stress_rate** / SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.

**default:** N/A

**type:** int


## **n_db_nodes** / SCT_N_DB_NODES

Number list of database data nodes in multiple data centers. To use with<br>multi data centers and zero nodes, dc with zero-nodes only should be set as 0,<br>ex. "3 3 0".

**default:** N/A

**type:** int_or_space_separated_ints


## **n_test_oracle_db_nodes** / SCT_N_TEST_ORACLE_DB_NODES

Number list of oracle test nodes in multiple data centers.

**default:** 1

**type:** int_or_space_separated_ints


## **n_loaders** / SCT_N_LOADERS

Number list of loader nodes in multiple data centers

**default:** N/A

**type:** int_or_space_separated_ints


## **n_monitor_nodes** / SCT_N_MONITORS_NODES

Number list of monitor nodes in multiple data centers

**default:** 1

**type:** int_or_space_separated_ints


## **intra_node_comm_public** / SCT_INTRA_NODE_COMM_PUBLIC

If True, all communication between nodes are via public addresses

**default:** N/A

**type:** boolean


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **user_credentials_path** / SCT_USER_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** str (appendable)


## **cloud_credentials_path** / SCT_CLOUD_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** str (appendable)


## **cloud_cluster_id** / SCT_CLOUD_CLUSTER_ID

scylla cloud cluster id

**default:** N/A

**type:** int


## **cloud_cluster_name** / SCT_CLOUD_CLUSTER_NAME

scylla cloud cluster name

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


## **ip_ssh_connections** / SCT_IP_SSH_CONNECTIONS

Type of IP used to connect to machine instances.<br>This depends on whether you are running your tests from a machine inside<br>your cloud provider, where it makes sense to use 'private', or outside (use 'public')<br><br>Default: Use public IPs to connect to instances (public)<br>Use private IPs to connect to instances (private)<br>Use IPv6 IPs to connect to instances (ipv6)

**default:** private

**type:** str (appendable)


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3']

**type:** str_or_list (appendable)


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root priviledge

**default:** N/A

**type:** boolean


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

**default:** 2022.1.14

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

List of distro features relevant to SCT test. Example: 'fips'.

**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for managment tests

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

**default:** 3.6

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



**default:** 3.6.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF condition>

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str (appendable)


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str (appendable)


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** N/A

**type:** boolean


## **use_ldap** / SCT_USE_LDAP

When defined true, LDAP is going to be used.

**default:** N/A

**type:** boolean


## **use_ldap_authorization** / SCT_USE_LDAP_AUTHORIZATION

When defined true, will create a docker container with LDAP and configure scylla.yaml to use it

**default:** N/A

**type:** boolean


## **use_ldap_authentication** / SCT_USE_LDAP_AUTHENTICATION

When defined true, will create a docker container with LDAP and configure scylla.yaml to use it

**default:** N/A

**type:** boolean


## **prepare_saslauthd** / SCT_PREPARE_SASLAUTHD

When defined true, will install and start saslauthd service

**default:** N/A

**type:** boolean


## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** str (appendable)


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** boolean


## **parallel_node_operations** / SCT_PARALLEL_NODE_OPERATIONS

When defined true, will run node operations in parallel. Supported operations: startup

**default:** True

**type:** boolean


## **manager_prometheus_port** / SCT_MANAGER_PROMETHEUS_PORT

Port to be used by the manager to contact Prometheus

**default:** 5090

**type:** int


## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** str (appendable)


## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.11

**type:** str (appendable)


## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

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


## **sct_ngrok_name** / SCT_NGROK_NAME

Override the default hostname address of the sct test runner,<br>using ngrok server, see readme for more instructions

**default:** N/A

**type:** str (appendable)


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** boolean


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** boolean


## **instance_provision** / SCT_INSTANCE_PROVISION

instance_provision: spot|on_demand|spot_fleet

**default:** spot

**type:** str (appendable)


## **instance_provision_fallback_on_demand** / SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND

instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected 'instance_provision' type creation failed. Expected values: true|false (default - false

**default:** N/A

**type:** boolean


## **reuse_cluster** / SCT_REUSE_CLUSTER

If reuse_cluster is set it should hold test_id of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`

**default:** N/A

**type:** str (appendable)


## **test_id** / SCT_TEST_ID

Set the test_id of the run manually. Use only from the env before running Hydra

**default:** N/A

**type:** str (appendable)


## **db_nodes_shards_selection** / SCT_NODES_SHARDS_SELECTION

How to select number of shards of Scylla. Expected values: default/random.<br>Default value: 'default'.<br>In case of random option - Scylla will start with different (random) shards on every node of the cluster

**default:** default

**type:** str (appendable)


## **seeds_selector** / SCT_SEEDS_SELECTOR

How to select the seeds. Expected values: random/first/all

**default:** all

**type:** str (appendable)


## **seeds_num** / SCT_SEEDS_NUM

Number of seeds to select

**default:** 1

**type:** int


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str_or_list (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Email subject postfix

**default:** N/A

**type:** str (appendable)


## **enable_test_profiling** / SCT_ENABLE_TEST_PROFILING

Turn on sct profiling

**default:** N/A

**type:** boolean


## **ssh_transport** / SSH_TRANSPORT

Set type of ssh library to use. Could be 'fabric' (default) or 'libssh2'

**default:** libssh2

**type:** str (appendable)


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

unlock specified experimental features

**default:** N/A

**type:** list


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** N/A

**type:** boolean


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** N/A

**type:** boolean


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** boolean


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** N/A

**type:** boolean


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** N/A

**type:** boolean


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** str (appendable)


## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authorizer** / SCT_AUTHORIZER

which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer

**default:** N/A

**type:** str (appendable)


## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** boolean


## **service_level_shares** / SCT_SERVICE_LEVEL_SHARES

List if service level shares - how many server levels to create and test. Uses in SLA test.list of int, like: [100, 200]

**default:** [1000]

**type:** list


## **alternator_port** / SCT_ALTERNATOR_PORT

Port to configure for alternator in scylla.yaml

**default:** N/A

**type:** int


## **dynamodb_primarykey_type** / SCT_DYNAMODB_PRIMARYKEY_TYPE

Type of dynamodb table to create with range key or not, can be:<br>HASH,HASH_AND_RANGE

**default:** HASH

**type:** str (appendable)


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** N/A

**type:** boolean


## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** N/A

**type:** boolean


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** N/A

**type:** boolean


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str (appendable)


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** N/A

**type:** dict_or_str


## **append_scylla_node_exporter_args** / SCT_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Next syntax supporting:<br>- nemesis_class_name: "NemesisName"  Run one nemesis in single thread<br>- nemesis_class_name: "<NemesisName>:<num>" Run <NemesisName> in <num><br>parallel threads on different nodes. Ex.: "ChaosMonkey:2"<br>- nemesis_class_name: "<NemesisName1>:<num1> <NemesisName2>:<num2>" Run<br><NemesisName1> in <num1> parallel threads and <NemesisName2> in <num2><br>parallel threads. Ex.: "ScyllaOperatorBasicOperationsMonkey:1 NonDisruptiveMonkey:2"

**default:** NoOpMonkey

**type:** _str


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

**type:** boolean


## **nemesis_seed** / SCT_NEMESIS_SEED

A seed number in order to repeat nemesis sequence as part of SisyphusMonkey.<br>Can provide a list of seeds for multiple nemesis

**default:** N/A

**type:** int_or_space_separated_ints


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** _str


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int_or_space_separated_ints


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** N/A

**type:** int


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** N/A

**type:** boolean


## **stress_cmd** / SCT_STRESS_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Url of the schema/configuration the gemini tool would use

**default:** N/A

**type:** str (appendable)


## **gemini_cmd** / SCT_GEMINI_CMD

gemini command to run (for now used only in GeminiTest)

**default:** N/A

**type:** str (appendable)


## **gemini_seed** / SCT_GEMINI_SEED

Seed number for gemini command

**default:** N/A

**type:** int


## **gemini_log_cql_statements** / SCT_GEMINI_LOG_CQL_STATEMENTS

Log CQL statements to file

**default:** N/A

**type:** boolean


## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example:<br>["cdc={'enabled': true}"]<br>["cdc={'enabled': true}", "compaction={'class': 'IncrementalCompactionStrategy'}"]

**default:** N/A

**type:** list


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

**type:** str_or_list_or_eval


## **security_group_ids** / SCT_SECURITY_GROUP_IDS

AWS security groups ids to use

**default:** N/A

**type:** str_or_list (appendable)


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** N/A

**type:** boolean


## **subnet_id** / SCT_SUBNET_ID

AWS subnet ids to use

**default:** N/A

**type:** str_or_list (appendable)


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

AMI ID for Vector Store nodes

**default:** N/A

**type:** str (appendable)


## **instance_type_vector_store** / SCT_INSTANCE_TYPE_VECTOR_STORE

EC2 instance type for Vector Store nodes

**default:** N/A

**type:** str (appendable)


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


## **extra_network_interface** / SCT_EXTRA_NETWORK_INTERFACE

if true, create extra network interface on each node

**default:** N/A

**type:** boolean


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

**type:** str_or_list (appendable)


## **use_prepared_loaders** / SCT_USE_PREPARED_LOADERS

If True, we use prepared VMs for loader (instead of using docker images)

**default:** N/A

**type:** boolean


## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **gce_project** / SCT_GCE_PROJECT

gcp project name to use

**default:** N/A

**type:** str (appendable)


## **gce_datacenter** / SCT_GCE_DATACENTER

Supported: us-east1 - means that the zone will be selected automatically or you can mention the zone explicitly, for example: us-east1-b

**default:** N/A

**type:** str_or_list_or_eval


## **gce_network** / SCT_GCE_NETWORK



**default:** N/A

**type:** str (appendable)


## **gce_image_db** / SCT_GCE_IMAGE_DB



**default:** N/A

**type:** str (appendable)


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR



**default:** N/A

**type:** str (appendable)


## **gce_image_loader** / SCT_GCE_IMAGE_LOADER



**default:** N/A

**type:** str (appendable)


## **gce_image_username** / SCT_GCE_IMAGE_USERNAME



**default:** N/A

**type:** str (appendable)


## **gce_instance_type_loader** / SCT_GCE_INSTANCE_TYPE_LOADER



**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_loader** / SCT_GCE_ROOT_DISK_TYPE_LOADER



**default:** N/A

**type:** str (appendable)


## **gce_n_local_ssd_disk_loader** / SCT_GCE_N_LOCAL_SSD_DISK_LOADER



**default:** N/A

**type:** int


## **gce_instance_type_monitor** / SCT_GCE_INSTANCE_TYPE_MONITOR



**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_monitor** / SCT_GCE_ROOT_DISK_TYPE_MONITOR



**default:** N/A

**type:** str (appendable)


## **gce_n_local_ssd_disk_monitor** / SCT_GCE_N_LOCAL_SSD_DISK_MONITOR



**default:** N/A

**type:** int


## **gce_instance_type_db** / SCT_GCE_INSTANCE_TYPE_DB



**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB



**default:** N/A

**type:** str (appendable)


## **gce_n_local_ssd_disk_db** / SCT_GCE_N_LOCAL_SSD_DISK_DB



**default:** N/A

**type:** int


## **gce_pd_standard_disk_size_db** / SCT_GCE_PD_STANDARD_DISK_SIZE_DB



**default:** N/A

**type:** int


## **gce_pd_ssd_disk_size_db** / SCT_GCE_PD_SSD_DISK_SIZE_DB



**default:** N/A

**type:** int


## **gce_setup_hybrid_raid** / SCT_GCE_SETUP_HYBRID_RAID

If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data

**default:** N/A

**type:** boolean


## **gce_pd_ssd_disk_size_loader** / SCT_GCE_PD_SSD_DISK_SIZE_LOADER



**default:** N/A

**type:** int


## **gce_pd_ssd_disk_size_monitor** / SCT_GCE_SSD_DISK_SIZE_MONITOR



**default:** N/A

**type:** int


## **azure_region_name** / SCT_AZURE_REGION_NAME

Supported: eastus

**default:** N/A

**type:** str_or_list_or_eval


## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER



**default:** N/A

**type:** str (appendable)


## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR



**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB



**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE



**default:** N/A

**type:** str (appendable)


## **azure_image_db** / SCT_AZURE_IMAGE_DB



**default:** N/A

**type:** str (appendable)


## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR



**default:** N/A

**type:** str (appendable)


## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER



**default:** N/A

**type:** str (appendable)


## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME



**default:** N/A

**type:** str (appendable)


## **eks_service_ipv4_cidr** / SCT_EKS_SERVICE_IPV4_CIDR



**default:** N/A

**type:** str (appendable)


## **eks_vpc_cni_version** / SCT_EKS_VPC_CNI_VERSION



**default:** N/A

**type:** str (appendable)


## **eks_role_arn** / SCT_EKS_ROLE_ARN



**default:** N/A

**type:** str (appendable)


## **eks_cluster_version** / SCT_EKS_CLUSTER_VERSION



**default:** N/A

**type:** str (appendable)


## **eks_nodegroup_role_arn** / SCT_EKS_NODEGROUP_ROLE_ARN



**default:** N/A

**type:** str (appendable)


## **gke_cluster_version** / SCT_GKE_CLUSTER_VERSION



**default:** N/A

**type:** str (appendable)


## **gke_k8s_release_channel** / SCT_GKE_K8S_RELEASE_CHANNEL

K8S release channel name to be used. Expected values are: 'rapid', 'regular', 'stable' and '' (static / No channel).

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_utils_docker_image** / SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE

Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.

**default:** N/A

**type:** str (appendable)


## **k8s_enable_performance_tuning** / SCT_K8S_ENABLE_PERFORMANCE_TUNING

Define whether performance tuning must run or not.

**default:** N/A

**type:** boolean


## **k8s_deploy_monitoring** / SCT_K8S_DEPLOY_MONITORING



**default:** N/A

**type:** boolean


## **k8s_local_volume_provisioner_type** / SCT_K8S_LOCAL_VOLUME_PROVISIONER_TYPE

Defines the type of the K8S local volume provisioner to be deployed. It may be either 'static' or 'dynamic'. Details about 'dynamic': 'dynamic': https://github.com/scylladb/k8s-local-volume-provisioner; 'static': sdcm/k8s_configs/static-local-volume-provisioner.yaml

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_docker_image** / SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE

Docker image to be used for installation of scylla operator.

**default:** N/A

**type:** str (appendable)


## **k8s_scylla_operator_upgrade_docker_image** / SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE

Docker image to be used for upgrade of scylla operator.

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



**default:** N/A

**type:** str (appendable)


## **k8s_n_scylla_pods_per_cluster** / K8S_N_SCYLLA_PODS_PER_CLUSTER

Number of loader pods per loader cluster.

**default:** 3

**type:** int_or_space_separated_ints


## **k8s_scylla_disk_gi** / SCT_K8S_SCYLLA_DISK_GI



**default:** N/A

**type:** int


## **k8s_scylla_disk_class** / SCT_K8S_SCYLLA_DISK_CLASS



**default:** N/A

**type:** str (appendable)


## **k8s_loader_cluster_name** / SCT_K8S_LOADER_CLUSTER_NAME



**default:** N/A

**type:** str (appendable)


## **k8s_n_loader_pods_per_cluster** / SCT_K8S_N_LOADER_PODS_PER_CLUSTER

Number of loader pods per loader cluster.

**default:** N/A

**type:** int_or_space_separated_ints


## **k8s_loader_run_type** / SCT_K8S_LOADER_RUN_TYPE

Defines how the loader pods must run. It may be either 'static' (default, run stress command on the constantly existing idle pod having reserved resources, perf-oriented) or 'dynamic' (run stress commad in a separate pod as main thread and get logs in a searate retryable API call not having resource reservations).

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



**default:** N/A

**type:** str (appendable)


## **k8s_cert_manager_version** / SCT_K8S_CERT_MANAGER_VERSION



**default:** N/A

**type:** str (appendable)


## **k8s_minio_storage_size** / SCT_K8S_MINIO_STORAGE_SIZE



**default:** 10Gi

**type:** str (appendable)


## **k8s_log_api_calls** / SCT_K8S_LOG_API_CALLS

Defines whether the K8S API server logging must be enabled and it's logs gathered. Be aware that it may be really huge set of data.

**default:** N/A

**type:** boolean


## **k8s_tenants_num** / SCT_TENANTS_NUM

Number of Scylla clusters to create in the K8S cluster.

**default:** 1

**type:** int


## **k8s_enable_tls** / SCT_K8S_ENABLE_TLS

Defines whether we enable the scylla operator TLS feature or not.

**default:** N/A

**type:** boolean


## **k8s_enable_sni** / SCT_K8S_ENABLE_SNI

Defines whether we install SNI and use it or not (serverless feature).

**default:** N/A

**type:** boolean


## **k8s_enable_alternator** / SCT_K8S_ENABLE_ALTERNATOR

Defines whether we enable the alternator feature using scylla-operator or not.

**default:** N/A

**type:** boolean


## **k8s_connection_bundle_file** / SCT_K8S_CONNECTION_BUNDLE_FILE

Serverless configuration bundle file

**default:** N/A

**type:** _file


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


## **k8s_use_chaos_mesh** / SCT_K8S_USE_CHAOS_MESH

enables chaos-mesh for k8s testing

**default:** N/A

**type:** boolean


## **k8s_n_auxiliary_nodes** / SCT_K8S_N_AUXILIARY_NODES

Number of nodes in auxiliary pool

**default:** N/A

**type:** int


## **k8s_n_monitor_nodes** / SCT_K8S_N_MONITOR_NODES

Number of nodes in monitoring pool that will be used for scylla-operator's deployed monitoring pods.

**default:** N/A

**type:** int


## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.6.0

**type:** str (appendable)


## **docker_image** / SCT_DOCKER_IMAGE

Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version

**default:** N/A

**type:** str (appendable)


## **docker_network** / SCT_DOCKER_NETWORK

local docker network to use, if there's need to have db cluster connect to other services running in docker

**default:** N/A

**type:** str (appendable)


## **vector_store_docker_image** / SCT_VECTOR_STORE_DOCKER_IMAGE

Vector Store docker image repo

**default:** scylladb/vector-store

**type:** str (appendable)


## **vector_store_version** / SCT_VECTOR_STORE_VERSION

Vector Store version / docker image tag

**default:** N/A

**type:** str (appendable)


## **s3_baremetal_config** / SCT_S3_BAREMETAL_CONFIG



**default:** N/A

**type:** str (appendable)


## **db_nodes_private_ip** / SCT_DB_NODES_PRIVATE_IP



**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **db_nodes_public_ip** / SCT_DB_NODES_PUBLIC_IP



**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **loaders_private_ip** / SCT_LOADERS_PRIVATE_IP



**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **loaders_public_ip** / SCT_LOADERS_PUBLIC_IP



**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **monitor_nodes_private_ip** / SCT_MONITOR_NODES_PRIVATE_IP



**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **monitor_nodes_public_ip** / SCT_MONITOR_NODES_PUBLIC_IP



**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **cassandra_stress_population_size** / SCT_CASSANDRA_STRESS_POPULATION_SIZE



**default:** 1000000

**type:** int


## **cassandra_stress_threads** / SCT_CASSANDRA_STRESS_THREADS



**default:** 1000

**type:** int


## **add_node_cnt** / SCT_ADD_NODE_CNT



**default:** 1

**type:** int


## **stress_multiplier** / SCT_STRESS_MULTIPLIER

Number of cassandra-stress processes

**default:** 1

**type:** int


## **stress_multiplier_w** / SCT_STRESS_MULTIPLIER_W

Number of cassandra-stress processes for write workload

**default:** 1

**type:** int


## **stress_multiplier_r** / SCT_STRESS_MULTIPLIER_R

Number of cassandra-stress processes for read workload

**default:** 1

**type:** int


## **stress_multiplier_m** / SCT_STRESS_MULTIPLIER_M

Number of cassandra-stress processes for mixed workload

**default:** 1

**type:** int


## **run_fullscan** / SCT_RUN_FULLSCAN



**default:** N/A

**type:** list


## **run_full_partition_scan** / SCT_run_full_partition_scan

Runs a background thread that issues reversed-queries on a table random partition by an interval

**default:** N/A

**type:** str (appendable)


## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Runs a background thread that verifies Tombstones GC on a table by an interval

**default:** N/A

**type:** str (appendable)


## **keyspace_num** / SCT_KEYSPACE_NUM



**default:** 1

**type:** int


## **round_robin** / SCT_ROUND_ROBIN



**default:** N/A

**type:** boolean


## **batch_size** / SCT_BATCH_SIZE



**default:** 1

**type:** int


## **pre_create_schema** / SCT_PRE_CREATE_SCHEMA



**default:** N/A

**type:** boolean


## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keysapce to be pre-create before running workload

**default:** N/A

**type:** str_or_list (appendable)


## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** str_or_list (appendable)


## **prepare_wait_no_compactions_timeout** / SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT

At the end of prepare stage, run major compaction and wait for this time (in minutes) for compaction to finish. (relevant only to longevity_test.py), Should be use only for when facing issue like compaction is affect the test or load

**default:** N/A

**type:** int


## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Choose a specific compaction strategy to pre-create schema with.

**default:** SizeTieredCompactionStrategy

**type:** str (appendable)


## **sstable_size** / SSTABLE_SIZE

Configure sstable size for the usage of pre-create-schema mode

**default:** N/A

**type:** int


## **cluster_health_check** / SCT_CLUSTER_HEALTH_CHECK

When true, start cluster health checker for all nodes

**default:** True

**type:** boolean


## **data_validation** / SCT_DATA_VALIDATION

A group of sub-parameters: validate_partitions, table_name, primary_key_column,<br>partition_range_with_data_validation, max_partitions_in_test_table.<br>1. validate_partitions - when true, validating the same number of rows-per-partition before/after a Nemesis.<br>2. table_name - table name to check for the validate_partitions check.<br>3. primary_key_column - primary key of the table to check for the validate_partitions check<br>4. partition_range_with_data_validation - Relevant for scylla-bench. A range (min - max) of PK values<br>for partitions to be validated by reads and not to be deleted during test. Example: 0-250.<br>5. max_partitions_in_test_table - Relevant for scylla-bench. Max partition keys (partition-count)<br>in the scylla_bench.test table.

**default:** N/A

**type:** str (appendable)


## **stress_read_cmd** / SCT_STRESS_READ_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

number of tables to create for template user c-s

**default:** 1

**type:** int


## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** N/A

**type:** boolean


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc.For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** dict_or_str_or_pydantic


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** boolean


## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** str (appendable)


## **mgmt_prepare_snapshot_size** / SCT_MGMT_PREPARE_SNAPSHOT_SIZE

Size of backup snapshot in Gb to be prepared for backup

**default:** N/A

**type:** int


## **mgmt_snapshots_preparer_params** / SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS

Custom parameters of c-s write operation used in snapshots preparer

**default:** {'cs_cmd_template': "cassandra-stress {operation} cl={cl} n={num_of_rows} -schema 'keyspace={ks_name} replication(strategy={replication},replication_factor={rf}) compaction(strategy={compaction})' -mode cql3 native -rate threads={threads_num} -col 'size=FIXED({col_size}) n=FIXED({col_n})' -pop seq={sequence_start}..{sequence_end}", 'operation': 'write', 'cl': 'QUORUM', 'replication': 'NetworkTopologyStrategy', 'rf': 3, 'compaction': 'IncrementalCompactionStrategy', 'threads_num': 500, 'col_size': 1024, 'col_n': 1, 'ks_name': '', 'num_of_rows': '', 'sequence_start': '', 'sequence_end': ''}

**type:** dict_or_str


## **one_one_restore_cluster_bootstrap_duration** / SCT_ONE_ONE_RESTORE_CLUSTER_BOOTSTRAP_DURATION

Time in seconds it took Siren to bootstrap 1-1-restore cluster

**default:** N/A

**type:** int


## **stress_cmd_w** / SCT_STRESS_CMD_W

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_r** / SCT_STRESS_CMD_R

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_m** / SCT_STRESS_CMD_M

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARM_UP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE



**default:** N/A

**type:** str (appendable)


## **perf_extra_jobs_to_compare** / SCT_PERF_EXTRA_JOBS_TO_COMPARE

jobs to compare performance results with, for example if running in staging, we still can compare with official jobs

**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **perf_simple_query_extra_command** / SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND

extra command line options to pass to perf_simple_query

**default:** N/A

**type:** str (appendable)


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** N/A

**type:** str_or_list (appendable)


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** N/A

**type:** str_or_list (appendable)


## **cs_duration** / SCT_CS_DURATION



**default:** 50m

**type:** str (appendable)


## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** boolean


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict_or_str


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Example: {'read': ['100000', '150000'], 'mixed': ['300']}

**default:** N/A

**type:** dict_or_str


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict_or_str


## **skip_download** / SCT_SKIP_DOWNLOAD



**default:** N/A

**type:** boolean


## **sstable_file** / SCT_SSTABLE_FILE



**default:** N/A

**type:** str (appendable)


## **sstable_url** / SCT_SSTABLE_URL



**default:** N/A

**type:** str (appendable)


## **sstable_md5** / SCT_SSTABLE_MD5



**default:** N/A

**type:** str (appendable)


## **flush_times** / SCT_FLUSH_TIMES



**default:** N/A

**type:** int


## **flush_period** / SCT_FLUSH_PERIOD



**default:** N/A

**type:** int


## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO



**default:** N/A

**type:** str (appendable)


## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** str (appendable)


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

Assign target upgrade version, use for decide if the truncate entries test should be run. This test should be performed in case the target upgrade version >= 3.1

**default:** N/A

**type:** str (appendable)


## **disable_raft** / SCT_DISABLE_RAFT

As for now, raft will be enable by default in all [upgrade] tests, so this flag will allow usto still run [upgrade] test without raft enabled (or disabling raft), so we will have bettercoverage

**default:** True

**type:** boolean


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test,the tablets feature will only be enabled after the upgrade

**default:** N/A

**type:** boolean


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** N/A

**type:** boolean


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES



**default:** N/A

**type:** str (appendable)


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default

**default:** True

**type:** boolean


## **stress_cmd_1** / SCT_STRESS_CMD_1

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str_or_list (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str_or_list (appendable)


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when AWS KMS or Azure KMS service is configured to be used. NOTE: Be aware that Azure Key rotations cost $1/rotation.

**default:** N/A

**type:** int


## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable only to Azure backend. In case of AWS backend its KMS keys will always be rotated as of now.

**default:** N/A

**type:** boolean


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed, we enable kms by default since if we use scylla 2023.1.3 and up

**default:** N/A

**type:** boolean


## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** syslog-ng

**type:** str (appendable)


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** N/A

**type:** boolean


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** N/A

**type:** boolean


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** str (appendable)


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** str (appendable)


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** str (appendable)


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** str (appendable)


## **internode_compression** / SCT_INTERNODE_COMPRESSION

scylla option: internode_compression

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

scylla sub option of server_encryption_options: internode_encryption

**default:** all

**type:** str (appendable)


## **jmx_heap_memory** / SCT_JMX_HEAP_MEMORY

The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB)

**default:** N/A

**type:** int


## **store_perf_results** / SCT_STORE_PERF_RESULTS

A flag that indicates whether or not to gather the prometheus stats at the end of the run.<br>Intended to be used in performance testing

**default:** N/A

**type:** boolean


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** N/A

**type:** boolean


## **force_run_iotune** / SCT_FORCE_RUN_IOTUNE

Force running iotune on the DB nodes, regdless if image has predefined values

**default:** N/A

**type:** boolean


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node , -keyspace, -table, parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** N/A

**type:** boolean


## **stop_test_on_stress_failure** / SCT_STOP_TEST_ON_STRESS_FAILURE

If set to True the test will be stopped immediately when stress command failed.<br>When set to False the test will continue to run even when there are errors in the<br>stress process

**default:** True

**type:** boolean


## **stress_cdc_log_reader_batching_enable** / SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE

retrieving data from multiple streams in one poll

**default:** True

**type:** boolean


## **use_legacy_cluster_init** / SCT_USE_LEGACY_CLUSTER_INIT

Use legacy cluster initialization with autobootsrap disabled and parallel node setup

**default:** N/A

**type:** boolean


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** str (appendable)


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Try all availability zones one by one in order to maximize the chances of getting<br>the requested instance capacity.

**default:** N/A

**type:** boolean


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** str (appendable)


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** boolean


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade (preapre stage)

**default:** N/A

**type:** str (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str (appendable)


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str (appendable)


## **jepsen_scylla_repo** / SCT_JEPSEN_SCYLLA_REPO

Link to the git repository with Jepsen Scylla tests

**default:** https://github.com/jepsen-io/scylla.git

**type:** str (appendable)


## **jepsen_test_cmd** / SCT_JEPSEN_TEST_CMD

Jepsen test command (e.g., 'test-all')

**default:** ['test-all -w cas-register --concurrency 10n', 'test-all -w counter --concurrency 10n', 'test-all -w cmap --concurrency 10n', 'test-all -w cset --concurrency 10n', 'test-all -w write-isolation --concurrency 10n', 'test-all -w list-append --concurrency 10n', 'test-all -w wr-register --concurrency 10n']

**type:** str_or_list (appendable)


## **jepsen_test_count** / SCT_JEPSEN_TEST_COUNT

possible number of reruns of single Jepsen test command

**default:** 1

**type:** int


## **jepsen_test_run_policy** / SCT_JEPSEN_TEST_RUN_POLICY

Jepsen test run policy (i.e., what we want to consider as passed for a single test)<br><br>'most' - most test runs are passed<br>'any'  - one pass is enough<br>'all'  - all test runs should pass

**default:** all

**type:** str (appendable)


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str_or_list (appendable)


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** N/A

**type:** boolean


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Limit number events in email reports

**default:** 10

**type:** int


## **data_volume_disk_num** / SCT_DATA_VOLUME_DISK_NUM

Number of additional data volumes attached to instances<br>if data_volume_disk_num > 0, then data volumes (ebs on aws) will be<br>used for scylla data directory

**default:** N/A

**type:** int


## **data_volume_disk_type** / SCT_DATA_VOLUME_DISK_TYPE

Type of addtitional volumes: gp2|gp3|io2|io3

**default:** N/A

**type:** str (appendable)


## **data_volume_disk_size** / SCT_DATA_VOLUME_DISK_SIZE

Size of additional volume in GB

**default:** N/A

**type:** int


## **data_volume_disk_iops** / SCT_DATA_VOLUME_DISK_IOPS

Number of iops for ebs type io2|io3|gp3

**default:** N/A

**type:** int


## **data_volume_disk_throughput** / SCT_DATA_VOLUME_DISK_THROUGHPUT

Throughput in MiB/sec for ebs type gp3. Min is 125. Max is 1000.

**default:** N/A

**type:** int


## **run_db_node_benchmarks** / SCT_RUN_DB_NODE_BENCHMARKS

Flag for running db node benchmarks before the tests

**default:** N/A

**type:** boolean


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of logical expression based on "nemesis properties" and filters IN all the nemesis that has<br>example of logical expression:<br>```yaml<br>nemesis_selector: "disruptive and not sla" # simple one<br>nemesis_selector: "disruptive and not (sla or limited or manager_operation or config_changes)" # complex one<br>```

**default:** N/A

**type:** str_or_list (appendable)


## **nemesis_exclude_disabled** / SCT_NEMESIS_EXCLUDE_DISABLED

nemesis_exclude_disabled determines whether 'disabled' nemeses are filtered out from list<br>or are allowed to be used. This allows to easily disable too 'risky' or 'extreme' nemeses by default,<br>for all longevities. For example: it is unwanted to run the ToggleGcModeMonkey in standard longevities<br>that runs a stress with data validation.

**default:** True

**type:** boolean


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 6

**type:** int


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** N/A

**type:** int


## **raid_level** / SCT_RAID_LEVEL

Number of of raid level: 0 - RAID0, 5 - RAID5

**default:** N/A

**type:** int


## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** N/A

**type:** boolean


## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** N/A

**type:** dict_or_str


## **scylla_network_config** / SCT_SCYLLA_NETWORK_CONFIG

Configure Scylla networking with single or multiple NIC/IP combinations.<br>It must be defined for listen_address and rpc_address. For each address mandatory parameters are:<br>- address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication<br>- ip_type: ipv4 or ipv6<br>- public: false or true<br>- nic: number of NIC. 0, 1<br>Supported for AWS only meanwhile

**default:** N/A

**type:** list


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** boolean


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for<br>performance gradual throughtput grow tests

**default:** N/A

**type:** str (appendable)


## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.

**default:** N/A

**type:** dict


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

**type:** str (appendable)


## **max_deviation** / SCT_MAX_DEVIATION

Max relative difference between best and current throughput,<br>if current throughput larger then best on max_rel_diff, it become new best one

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

**type:** boolean


## **stop_on_hw_perf_failure** / SCT_STOP_ON_HW_PERF_FAILURE

Stop sct performance test if hardware performance test failed<br><br>Hardware performance tests runs on each node with sysbench and cassandra-fio tools.<br>Results stored in ES. HW perf tests run during cluster setups and not affect<br>SCT Performance tests. Results calculated as average among all results for certain<br>instance type or among all nodes during single run.<br>if results for a single node is not in margin 0.01 of<br>average result for all nodes, hw test considered as Failed.<br>If stop_on_hw_perf_failure is True, then sct performance test will be terminated<br>after hw perf tests detect node with hw results not in margin with average<br>If stop_on_hw_perf_failure is False, then sct performance test will be run<br>even after hw perf tests detect node with hw results not in margin with average

**default:** N/A

**type:** boolean


## **custom_es_index** / SCT_CUSTOM_ES_INDEX

Use custom ES index for storing test results

**default:** N/A

**type:** str (appendable)


## **simulated_regions** / SCT_SIMULATED_REGIONS

Defines how many regions must be simulated on the Scylla config side. If set then<br>nodes will be provisioned only using the very first real region defined in the configuration.

**default:** N/A

**type:** int


## **simulated_racks** / SCT_SIMULATED_RACKS

Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.<br>Provide number of racks to simulate.

**default:** 3

**type:** int


## **rack_aware_loader** / SCT_RACK_AWARE_LOADER

When enabled, loaders will look for nodes on the same rack.

**default:** N/A

**type:** boolean


## **capacity_errors_check_mode** / SCT_CAPACITY_ERRORS_CHECK_MODE

how to check if to continue test execution when capacity errors are detected.<br>per-initial_config - check if cluster layout is same as initial configuration, if not stop test execution<br>disabled - continue test execution even if capacity errors are detected

**default:** N/A

**type:** str (appendable)


## **use_dns_names** / SCT_USE_DNS_NAMES

Use dns names instead of ip addresses for nodes in cluster

**default:** N/A

**type:** boolean


## **validate_large_collections** / SCT_VALIDATE_LARGE_COLLECTIONS

Enable validation for large cells in system table and logs

**default:** N/A

**type:** boolean


## **run_commit_log_check_thread** / SCT_RUN_COMMIT_LOG_CHECK_THREAD

Run commit log check thread if commitlog_use_hard_size_limit is True

**default:** True

**type:** boolean


## **teardown_validators** / SCT_TEARDOWN_VALIDATORS

Configuration for additional validations executed after the test

**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}}

**type:** dict_or_str


## **use_capacity_reservation** / SCT_USE_CAPACITY_RESERVATION

reserves instances capacity for whole duration of the test run (AWS only).<br>Fallbacks to next availabilit zone if capacity is not available

**default:** N/A

**type:** boolean


## **use_dedicated_host** / SCT_USE_DEDICATED_HOST

Allocates dedicated hosts for the instances for the entire duration of the test run (AWS only)

**default:** N/A

**type:** boolean


## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

list of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **post_behavior_dedicated_host** / SCT_POST_BEHAVIOR_DEDICATED_HOST

Failure/post test behavior, i.e. what to do with the dedicate hosts at the end of the test.<br><br>'destroy' - Destroy hosts (default)<br>'keep' - Keep hosts allocated

**default:** N/A

**type:** str (appendable)


## **bisect_start_date** / SCT_BISECT_START_DATE

Scylla build date from which bisecting should start.<br>Setting this date enables bisection. Format: YYYY-MM-DD

**default:** N/A

**type:** str (appendable)


## **bisect_end_date** / SCT_BISECT_END_DATE

Scylla build date until which bisecting should run. Format: YYYY-MM-DD

**default:** N/A

**type:** str (appendable)


## **kafka_backend** / SCT_KAFKA_BACKEND

Enable validation for large cells in system table and logs

**default:** N/A

**type:** str (appendable)


## **kafka_connectors** / SCT_KAFKA_CONNECTORS

configuration for setup up kafka connectors

**default:** N/A

**type:** str_or_list_or_eval (appendable)


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Run scylla-doctor in artifact tests

**default:** True

**type:** boolean


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario

**default:** N/A

**type:** dict_or_str


## **use_zero_nodes** / SCT_USE_ZERO_NODES

If True, enable support in sct of zero nodes(configuration, nemesis)

**default:** N/A

**type:** boolean


## **n_db_zero_token_nodes** / SCT_N_DB_ZERO_TOKEN_NODES

Number of zero token nodes in cluster. Value should be set as "0 1 1"<br>for multidc configuration in same manner as 'n_db_nodes' and should be equal<br>number of regions

**default:** N/A

**type:** int_or_space_separated_ints


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero token node

**default:** i4i.large

**type:** str (appendable)


## **sct_aws_account_id** / SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **latency_decorator_error_thresholds** / SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS

Error thresholds for latency decorator. Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}

**default:** {'write': {'default': {'P90 write': {'fixed_limit': 5}, 'P99 write': {'fixed_limit': 10}}}, 'read': {'default': {'P90 read': {'fixed_limit': 5}, 'P99 read': {'fixed_limit': 10}}}, 'mixed': {'default': {'P90 write': {'fixed_limit': 5}, 'P90 read': {'fixed_limit': 5}, 'P99 write': {'fixed_limit': 10}, 'P99 read': {'fixed_limit': 10}}}}

**type:** dict_or_str


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset.Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true).If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)


## **adaptive_timeout_store_metrics** / SCT_ADAPTIVE_TIMEOUT_STORE_METRICS

Store adaptive timeout metrics in Argus. Disabled for performance tests only.

**default:** True

**type:** boolean


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


## **xcloud_replication_factor** / SCT_XCLOUD_REPLICATION_FACTOR

Replication factor for Scylla Cloud cluster (default: 3)

**default:** N/A

**type:** int


## **xcloud_vpc_peering** / SCT_XCLOUD_VPC_PEERING

Dictionary of VPC peering parameters for private connectivity between<br>SCT infrastructure and Scylla Cloud. The following parameters are used:<br>enabled: bool - indicates whether VPC peering is to be used<br>cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)<br>cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)

**default:** N/A

**type:** dict_or_str


## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** N/A

**type:** int


## **vector_store_port** / SCT_VECTOR_STORE_PORT

Vector Store API port

**default:** 6080

**type:** int


## **vector_store_scylla_port** / SCT_VECTOR_STORE_SCYLLA_PORT

ScyllaDB connection port for Vector Store

**default:** 9042

**type:** int


## **vector_store_threads** / SCT_VECTOR_STORE_THREADS

Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)

**default:** N/A

**type:** int
=======
# scylla-cluster-tests configuration options
| Parameter | Description  | Default | Override environment<br>variable
| :-------  | :----------  | :------ | :-------------------------------
| **<a href="#user-content-config_files" name="config_files">config_files</a>**  | a list of config files that would be used | N/A | SCT_CONFIG_FILES
| **<a href="#user-content-cluster_backend" name="cluster_backend">cluster_backend</a>**  | backend that will be used, aws/gce/docker | N/A | SCT_CLUSTER_BACKEND
| **<a href="#user-content-test_duration" name="test_duration">test_duration</a>**  | Test duration (min). Parameter used to keep instances produced by tests<br>and for jenkins pipeline timeout and TimoutThread. | 60 | SCT_TEST_DURATION
| **<a href="#user-content-n_db_nodes" name="n_db_nodes">n_db_nodes</a>**  | Number list of database nodes in multiple data centers. | N/A | SCT_N_DB_NODES
| **<a href="#user-content-n_test_oracle_db_nodes" name="n_test_oracle_db_nodes">n_test_oracle_db_nodes</a>**  | Number list of oracle test nodes in multiple data centers. | 1 | SCT_N_TEST_ORACLE_DB_NODES
| **<a href="#user-content-n_loaders" name="n_loaders">n_loaders</a>**  | Number list of loader nodes in multiple data centers | N/A | SCT_N_LOADERS
| **<a href="#user-content-n_monitor_nodes" name="n_monitor_nodes">n_monitor_nodes</a>**  | Number list of monitor nodes in multiple data centers | N/A | SCT_N_MONITORS_NODES
| **<a href="#user-content-intra_node_comm_public" name="intra_node_comm_public">intra_node_comm_public</a>**  | If True, all communication between nodes are via public addresses | N/A | SCT_INTRA_NODE_COMM_PUBLIC
| **<a href="#user-content-endpoint_snitch" name="endpoint_snitch">endpoint_snitch</a>**  | The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch' | N/A | SCT_ENDPOINT_SNITCH
| **<a href="#user-content-user_credentials_path" name="user_credentials_path">user_credentials_path</a>**  | Path to your user credentials. qa key are downloaded automatically from S3 bucket | N/A | SCT_USER_CREDENTIALS_PATH
| **<a href="#user-content-cloud_credentials_path" name="cloud_credentials_path">cloud_credentials_path</a>**  | Path to your user credentials. qa key are downloaded automatically from S3 bucket | ~/.ssh/support | SCT_CLOUD_CREDENTIALS_PATH
| **<a href="#user-content-cloud_cluster_id" name="cloud_cluster_id">cloud_cluster_id</a>**  | scylla cloud cluster id | N/A | SCT_CLOUD_CLUSTER_ID
| **<a href="#user-content-cloud_prom_bearer_token" name="cloud_prom_bearer_token">cloud_prom_bearer_token</a>**  | scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance | N/A | SCT_CLOUD_PROM_BEARER_TOKEN
| **<a href="#user-content-cloud_prom_path" name="cloud_prom_path">cloud_prom_path</a>**  | scylla cloud promproxy path to federate monitoring data into our monitoring instance | N/A | SCT_CLOUD_PROM_PATH
| **<a href="#user-content-cloud_prom_host" name="cloud_prom_host">cloud_prom_host</a>**  | scylla cloud promproxy hostname to federate monitoring data into our monitoring instance | N/A | SCT_CLOUD_PROM_HOST
| **<a href="#user-content-ip_ssh_connections" name="ip_ssh_connections">ip_ssh_connections</a>**  | Type of IP used to connect to machine instances.<br>This depends on whether you are running your tests from a machine inside<br>your cloud provider, where it makes sense to use 'private', or outside (use 'public')<br><br>Default: Use public IPs to connect to instances (public)<br>Use private IPs to connect to instances (private)<br>Use IPv6 IPs to connect to instances (ipv6) | private | SCT_IP_SSH_CONNECTIONS
| **<a href="#user-content-scylla_repo" name="scylla_repo">scylla_repo</a>**  | Url to the repo of scylla version to install scylla | N/A | SCT_SCYLLA_REPO
| **<a href="#user-content-unified_package" name="unified_package">unified_package</a>**  | Url to the unified package of scylla version to install scylla | N/A | SCT_UNIFIED_PACKAGE
| **<a href="#user-content-nonroot_offline_install" name="nonroot_offline_install">nonroot_offline_install</a>**  | Install Scylla without required root priviledge | N/A | SCT_NONROOT_OFFLINE_INSTALL
| **<a href="#user-content-scylla_version" name="scylla_version">scylla_version</a>**  | Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla' | N/A | SCT_SCYLLA_VERSION
| **<a href="#user-content-oracle_scylla_version" name="oracle_scylla_version">oracle_scylla_version</a>**  | Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically lookup AMIs for formal versions.<br>WARNING: can't be used together with 'ami_id_db_oracle' | N/A | SCT_ORACLE_SCYLLA_VERSION
| **<a href="#user-content-scylla_linux_distro" name="scylla_linux_distro">scylla_linux_distro</a>**  | The distro name and family name to use [centos/ubuntu-xenial/debian-jessie] | centos | SCT_SCYLLA_LINUX_DISTRO
| **<a href="#user-content-scylla_linux_distro_loader" name="scylla_linux_distro_loader">scylla_linux_distro_loader</a>**  | The distro name and family name to use [centos/ubuntu-xenial/debian-jessie] | centos | SCT_SCYLLA_LINUX_DISTRO_LOADER
| **<a href="#user-content-scylla_repo_m" name="scylla_repo_m">scylla_repo_m</a>**  | Url to the repo of scylla version to install scylla from for managment tests | http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2020.1.repo | SCT_SCYLLA_REPO_M
| **<a href="#user-content-scylla_repo_loader" name="scylla_repo_loader">scylla_repo_loader</a>**  | Url to the repo of scylla version to install c-s for loader | N/A | SCT_SCYLLA_REPO_LOADER
| **<a href="#user-content-scylla_mgmt_repo" name="scylla_mgmt_repo">scylla_mgmt_repo</a>**  | Url to the repo of scylla manager version to install for management tests | http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylladb-manager-2.2.repo | SCT_SCYLLA_MGMT_REPO
| **<a href="#user-content-scylla_mgmt_agent_repo" name="scylla_mgmt_agent_repo">scylla_mgmt_agent_repo</a>**  | Url to the repo of scylla manager agent version to install for management tests | N/A | SCT_SCYLLA_MGMT_AGENT_REPO
| **<a href="#user-content-scylla_mgmt_agent_version" name="scylla_mgmt_agent_version">scylla_mgmt_agent_version</a>**  |  | N/A | SCT_SCYLLA_MGMT_AGENT_VERSION
| **<a href="#user-content-scylla_mgmt_pkg" name="scylla_mgmt_pkg">scylla_mgmt_pkg</a>**  | Url to the scylla manager packages to install for management tests | N/A | SCT_SCYLLA_MGMT_PKG
| **<a href="#user-content-stress_cmd_lwt_i" name="stress_cmd_lwt_i">stress_cmd_lwt_i</a>**  | Stress command for LWT performance test for INSERT baseline | N/A | SCT_STRESS_CMD_LWT_I
| **<a href="#user-content-stress_cmd_lwt_d" name="stress_cmd_lwt_d">stress_cmd_lwt_d</a>**  | Stress command for LWT performance test for DELETE baseline | N/A | SCT_STRESS_CMD_LWT_D
| **<a href="#user-content-stress_cmd_lwt_u" name="stress_cmd_lwt_u">stress_cmd_lwt_u</a>**  | Stress command for LWT performance test for UPDATE baseline | N/A | SCT_STRESS_CMD_LWT_U
| **<a href="#user-content-stress_cmd_lwt_ine" name="stress_cmd_lwt_ine">stress_cmd_lwt_ine</a>**  | Stress command for LWT performance test for INSERT with IF NOT EXISTS | N/A | SCT_STRESS_CMD_LWT_INE
| **<a href="#user-content-stress_cmd_lwt_uc" name="stress_cmd_lwt_uc">stress_cmd_lwt_uc</a>**  | Stress command for LWT performance test for UPDATE with IF <condition> | N/A | SCT_STRESS_CMD_LWT_UC
| **<a href="#user-content-stress_cmd_lwt_ue" name="stress_cmd_lwt_ue">stress_cmd_lwt_ue</a>**  | Stress command for LWT performance test for UPDATE with IF EXISTS | N/A | SCT_STRESS_CMD_LWT_UE
| **<a href="#user-content-stress_cmd_lwt_de" name="stress_cmd_lwt_de">stress_cmd_lwt_de</a>**  | Stress command for LWT performance test for DELETE with IF EXISTS | N/A | SCT_STRESS_CMD_LWT_DE
| **<a href="#user-content-stress_cmd_lwt_dc" name="stress_cmd_lwt_dc">stress_cmd_lwt_dc</a>**  | Stress command for LWT performance test for DELETE with IF condition> | N/A | SCT_STRESS_CMD_LWT_DC
| **<a href="#user-content-stress_cmd_lwt_mixed" name="stress_cmd_lwt_mixed">stress_cmd_lwt_mixed</a>**  | Stress command for LWT performance test for mixed lwt load | N/A | SCT_STRESS_CMD_LWT_MIXED
| **<a href="#user-content-stress_cmd_lwt_mixed_baseline" name="stress_cmd_lwt_mixed_baseline">stress_cmd_lwt_mixed_baseline</a>**  | Stress command for LWT performance test for mixed lwt load baseline | N/A | SCT_STRESS_CMD_LWT_MIXED_BASELINE
| **<a href="#user-content-use_cloud_manager" name="use_cloud_manager">use_cloud_manager</a>**  | When define true, will install scylla cloud manager | N/A | SCT_USE_CLOUD_MANAGER
| **<a href="#user-content-use_ldap_authorization" name="use_ldap_authorization">use_ldap_authorization</a>**  | When defined true, will create a docker container with LDAP and configure scylla.yaml to use it | N/A | SCT_USE_LDAP_AUTHORIZATION
| **<a href="#user-content-use_mgmt" name="use_mgmt">use_mgmt</a>**  | When define true, will install scylla management | N/A | SCT_USE_MGMT
| **<a href="#user-content-manager_prometheus_port" name="manager_prometheus_port">manager_prometheus_port</a>**  | Port to be used by the manager to contact Prometheus | 5090 | SCT_MANAGER_PROMETHEUS_PORT
| **<a href="#user-content-target_scylla_mgmt_server_repo" name="target_scylla_mgmt_server_repo">target_scylla_mgmt_server_repo</a>**  | Url to the repo of scylla manager version used to upgrade the manager server | N/A | SCT_TARGET_SCYLLA_MGMT_SERVER_REPO
| **<a href="#user-content-target_scylla_mgmt_agent_repo" name="target_scylla_mgmt_agent_repo">target_scylla_mgmt_agent_repo</a>**  | Url to the repo of scylla manager version used to upgrade the manager agents | N/A | SCT_TARGET_SCYLLA_MGMT_AGENT_REPO
| **<a href="#user-content-update_db_packages" name="update_db_packages">update_db_packages</a>**  | A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami) | N/A | SCT_UPDATE_DB_PACKAGES
| **<a href="#user-content-monitor_branch" name="monitor_branch">monitor_branch</a>**  | The port of scylla management | branch-3.6 | SCT_MONITOR_BRANCH
| **<a href="#user-content-db_type" name="db_type">db_type</a>**  | Db type to install into db nodes, scylla/cassandra | scylla | SCT_DB_TYPE
| **<a href="#user-content-user_prefix" name="user_prefix">user_prefix</a>**  | the prefix of the name of the cloud instances, defaults to username | N/A | SCT_USER_PREFIX
| **<a href="#user-content-ami_id_db_scylla_desc" name="ami_id_db_scylla_desc">ami_id_db_scylla_desc</a>**  | version name to report stats to Elasticsearch and tagged on cloud instances | N/A | SCT_AMI_ID_DB_SCYLLA_DESC
| **<a href="#user-content-sct_public_ip" name="sct_public_ip">sct_public_ip</a>**  | Override the default hostname address of the sct test runner,<br>for the monitoring of the Nemesis.<br>can only work out of the box in AWS | N/A | SCT_SCT_PUBLIC_IP
| **<a href="#user-content-sct_ngrok_name" name="sct_ngrok_name">sct_ngrok_name</a>**  | Override the default hostname address of the sct test runner,<br>using ngrok server, see readme for more instructions | N/A | SCT_NGROK_NAME
| **<a href="#user-content-backtrace_decoding" name="backtrace_decoding">backtrace_decoding</a>**  | If True, all backtraces found in db nodes would be decoded automatically | True | SCT_BACKTRACE_DECODING
| **<a href="#user-content-instance_provision" name="instance_provision">instance_provision</a>**  | instance_provision: spot|on_demand|spot_fleet | spot | SCT_INSTANCE_PROVISION
| **<a href="#user-content-instance_provision_fallback_on_demand" name="instance_provision_fallback_on_demand">instance_provision_fallback_on_demand</a>**  | instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected 'instance_provision' type creation failed. Expected values: true|false (default - false | N/A | SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND
| **<a href="#user-content-reuse_cluster" name="reuse_cluster">reuse_cluster</a>**  | If reuse_cluster is set it should hold test_id of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71` | N/A | SCT_REUSE_CLUSTER
| **<a href="#user-content-test_id" name="test_id">test_id</a>**  | test id to filter by | N/A | SCT_TEST_ID
| **<a href="#user-content-seeds_selector" name="seeds_selector">seeds_selector</a>**  | How to select the seeds. Expected values: reflector/random/first | first | SCT_SEEDS_SELECTOR
| **<a href="#user-content-seeds_num" name="seeds_num">seeds_num</a>**  | Number of seeds to select | 1 | SCT_SEEDS_NUM
| **<a href="#user-content-send_email" name="send_email">send_email</a>**  | If true would send email out of the performance regression test | N/A | SCT_SEND_EMAIL
| **<a href="#user-content-email_recipients" name="email_recipients">email_recipients</a>**  | list of email of send the performance regression test to | ['qa@scylladb.com'] | SCT_EMAIL_RECIPIENTS
| **<a href="#user-content-email_subject_postfix" name="email_subject_postfix">email_subject_postfix</a>**  | Email subject postfix | N/A | SCT_EMAIL_SUBJECT_POSTFIX
| **<a href="#user-content-enable_test_profiling" name="enable_test_profiling">enable_test_profiling</a>**  | Turn on sct profiling | N/A | SCT_ENABLE_TEST_PROFILING
| **<a href="#user-content-ssh_transport" name="ssh_transport">ssh_transport</a>**  | Set type of ssh library to use. Could be 'fabric' (default) or 'libssh2' | fabric | SSH_TRANSPORT
| **<a href="#user-content-bench_run" name="bench_run">bench_run</a>**  | If true would kill the scylla-bench thread in the test teardown | N/A | SCT_BENCH_RUN
| **<a href="#user-content-fullscan" name="fullscan">fullscan</a>**  | If true would kill the fullscan thread in the test teardown | N/A | SCT_FULLSCAN
| **<a href="#user-content-experimental" name="experimental">experimental</a>**  | when enabled scylla will use it's experimental features | True | SCT_EXPERIMENTAL
| **<a href="#user-content-server_encrypt" name="server_encrypt">server_encrypt</a>**  | when enable scylla will use encryption on the server side | N/A | SCT_SERVER_ENCRYPT
| **<a href="#user-content-client_encrypt" name="client_encrypt">client_encrypt</a>**  | when enable scylla will use encryption on the client side | N/A | SCT_CLIENT_ENCRYPT
| **<a href="#user-content-hinted_handoff" name="hinted_handoff">hinted_handoff</a>**  | when enable or disable scylla hinted handoff (enabled/disabled) | enabled | SCT_HINTED_HANDOFF
| **<a href="#user-content-authenticator" name="authenticator">authenticator</a>**  | which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator | N/A | SCT_AUTHENTICATOR
| **<a href="#user-content-authenticator_user" name="authenticator_user">authenticator_user</a>**  | the username if PasswordAuthenticator is used | N/A | SCT_AUTHENTICATOR_USER
| **<a href="#user-content-authenticator_password" name="authenticator_password">authenticator_password</a>**  | the password if PasswordAuthenticator is used | N/A | SCT_AUTHENTICATOR_PASSWORD
| **<a href="#user-content-authorizer" name="authorizer">authorizer</a>**  | which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer | N/A | SCT_AUTHORIZER
| **<a href="#user-content-system_auth_rf" name="system_auth_rf">system_auth_rf</a>**  | Replication factor will be set to system_auth | 3 | SCT_SYSTEM_AUTH_RF
| **<a href="#user-content-alternator_port" name="alternator_port">alternator_port</a>**  | Port to configure for alternator in scylla.yaml | N/A | SCT_ALTERNATOR_PORT
| **<a href="#user-content-dynamodb_primarykey_type" name="dynamodb_primarykey_type">dynamodb_primarykey_type</a>**  | Type of dynamodb table to create with range key or not, can be:<br>HASH,HASH_AND_RANGE | HASH | SCT_DYNAMODB_PRIMARYKEY_TYPE
| **<a href="#user-content-alternator_write_isolation" name="alternator_write_isolation">alternator_write_isolation</a>**  | Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details | N/A | SCT_ALTERNATOR_WRITE_ISOLATION
| **<a href="#user-content-alternator_use_dns_routing" name="alternator_use_dns_routing">alternator_use_dns_routing</a>**  | If true, spawn a docker with a dns server for the ycsb loader to point to | N/A | SCT_ALTERNATOR_USE_DNS_ROUTING
| **<a href="#user-content-alternator_enforce_authorization" name="alternator_enforce_authorization">alternator_enforce_authorization</a>**  | If true, enable the authorization check in dynamodb api (alternator) | N/A | SCT_ALTERNATOR_ENFORCE_AUTHORIZATION
| **<a href="#user-content-alternator_access_key_id" name="alternator_access_key_id">alternator_access_key_id</a>**  | the aws_access_key_id that would be used for alternator | N/A | SCT_ALTERNATOR_ACCESS_KEY_ID
| **<a href="#user-content-alternator_secret_access_key" name="alternator_secret_access_key">alternator_secret_access_key</a>**  | the aws_secret_access_key that would be used for alternator | N/A | SCT_ALTERNATOR_SECRET_ACCESS_KEY
| **<a href="#user-content-region_aware_loader" name="region_aware_loader">region_aware_loader</a>**  | When in multi region mode, run stress on loader that is located in the same region as db node | N/A | SCT_REGION_AWARE_LOADER
| **<a href="#user-content-append_scylla_args" name="append_scylla_args">append_scylla_args</a>**  | More arguments to append to scylla command line | --blocked-reactor-notify-ms 500 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1 | SCT_APPEND_SCYLLA_ARGS
| **<a href="#user-content-append_scylla_args_oracle" name="append_scylla_args_oracle">append_scylla_args_oracle</a>**  | More arguments to append to oracle command line | N/A | SCT_APPEND_SCYLLA_ARGS_ORACLE
| **<a href="#user-content-append_scylla_yaml" name="append_scylla_yaml">append_scylla_yaml</a>**  | More configuration to append to /etc/scylla/scylla.yaml | N/A | SCT_APPEND_SCYLLA_YAML
| **<a href="#user-content-nemesis_class_name" name="nemesis_class_name">nemesis_class_name</a>**  | Nemesis class to use (possible types in sdcm.nemesis).<br>Next syntax supporting:<br>- nemesis_class_name: "NemesisName"  Run one nemesis in single thread<br>- nemesis_class_name: "<NemesisName>:<num>" Run <NemesisName> in <num><br>parallel threads on different nodes. Ex.: "ChaosMonkey:2"<br>- nemesis_class_name: "<NemesisName1>:<num1> <NemesisName2>:<num2>" Run<br><NemesisName1> in <num1> parallel threads and <NemesisName2> in <num2><br>parallel threads. Ex.: "DisruptiveMonkey:1 NonDisruptiveMonkey:2" | NoOpMonkey | SCT_NEMESIS_CLASS_NAME
| **<a href="#user-content-nemesis_interval" name="nemesis_interval">nemesis_interval</a>**  | Nemesis sleep interval to use if None provided specifically in the test | 5 | SCT_NEMESIS_INTERVAL
| **<a href="#user-content-nemesis_sequence_sleep_between_ops" name="nemesis_sequence_sleep_between_ops">nemesis_sequence_sleep_between_ops</a>**  | Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests | N/A | SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS
| **<a href="#user-content-nemesis_during_prepare" name="nemesis_during_prepare">nemesis_during_prepare</a>**  | Run nemesis during prepare stage of the test | True | SCT_NEMESIS_DURING_PREPARE
| **<a href="#user-content-nemesis_seed" name="nemesis_seed">nemesis_seed</a>**  | A seed number in order to repeat nemesis sequence as part of SisyphusMonkey | N/A | SCT_NEMESIS_SEED
| **<a href="#user-content-nemesis_add_node_cnt" name="nemesis_add_node_cnt">nemesis_add_node_cnt</a>**  | Add/remove nodes during GrowShrinkCluster nemesis | 1 | SCT_NEMESIS_ADD_NODE_CNT
| **<a href="#user-content-cluster_target_size" name="cluster_target_size">cluster_target_size</a>**  | Used for scale test: max size of the cluster | N/A | SCT_CLUSTER_TARGET_SIZE
| **<a href="#user-content-space_node_threshold" name="space_node_threshold">space_node_threshold</a>**  | Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140 | N/A | SCT_SPACE_NODE_THRESHOLD
| **<a href="#user-content-nemesis_filter_seeds" name="nemesis_filter_seeds">nemesis_filter_seeds</a>**  | If true runs the nemesis only on non seed nodes | True | SCT_NEMESIS_FILTER_SEEDS
| **<a href="#user-content-stress_cmd" name="stress_cmd">stress_cmd</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD
| **<a href="#user-content-gemini_version" name="gemini_version">gemini_version</a>**  | Version of download of the binaries of gemini tool | 0.9.2 | SCT_GEMINI_VERSION
| **<a href="#user-content-gemini_schema_url" name="gemini_schema_url">gemini_schema_url</a>**  | Url of the schema/configuration the gemini tool would use | N/A | SCT_GEMINI_SCHEMA_URL
| **<a href="#user-content-gemini_cmd" name="gemini_cmd">gemini_cmd</a>**  | gemini command to run (for now used only in GeminiTest) | N/A | SCT_GEMINI_CMD
| **<a href="#user-content-gemini_seed" name="gemini_seed">gemini_seed</a>**  | Seed number for gemini command | N/A | SCT_GEMINI_SEED
| **<a href="#user-content-gemini_table_options" name="gemini_table_options">gemini_table_options</a>**  | table options for created table. example:<br>["cdc={'enabled': true}"]<br>["cdc={'enabled': true}", "compaction={'class': 'IncrementalCompactionStrategy'}"] | N/A | SCT_GEMINI_TABLE_OPTIONS
| **<a href="#user-content-instance_type_loader" name="instance_type_loader">instance_type_loader</a>**  | AWS image type of the loader node | N/A | SCT_INSTANCE_TYPE_LOADER
| **<a href="#user-content-instance_type_monitor" name="instance_type_monitor">instance_type_monitor</a>**  | AWS image type of the monitor node | N/A | SCT_INSTANCE_TYPE_MONITOR
| **<a href="#user-content-instance_type_db" name="instance_type_db">instance_type_db</a>**  | AWS image type of the db node | N/A | SCT_INSTANCE_TYPE_DB
| **<a href="#user-content-instance_type_db_oracle" name="instance_type_db_oracle">instance_type_db_oracle</a>**  | AWS image type of the oracle node | N/A | SCT_INSTANCE_TYPE_DB_ORACLE
| **<a href="#user-content-region_name" name="region_name">region_name</a>**  | AWS regions to use | N/A | SCT_REGION_NAME
| **<a href="#user-content-security_group_ids" name="security_group_ids">security_group_ids</a>**  | AWS security groups ids to use | N/A | SCT_SECURITY_GROUP_IDS
| **<a href="#user-content-subnet_id" name="subnet_id">subnet_id</a>**  | AWS subnet ids to use | N/A | SCT_SUBNET_ID
| **<a href="#user-content-ami_id_db_scylla" name="ami_id_db_scylla">ami_id_db_scylla</a>**  | AMS AMI id to use for scylla db node | N/A | SCT_AMI_ID_DB_SCYLLA
| **<a href="#user-content-ami_id_loader" name="ami_id_loader">ami_id_loader</a>**  | AMS AMI id to use for loader node | N/A | SCT_AMI_ID_LOADER
| **<a href="#user-content-ami_id_monitor" name="ami_id_monitor">ami_id_monitor</a>**  | AMS AMI id to use for monitor node | N/A | SCT_AMI_ID_MONITOR
| **<a href="#user-content-ami_id_db_cassandra" name="ami_id_db_cassandra">ami_id_db_cassandra</a>**  | AMS AMI id to use for cassandra node | N/A | SCT_AMI_ID_DB_CASSANDRA
| **<a href="#user-content-ami_id_db_oracle" name="ami_id_db_oracle">ami_id_db_oracle</a>**  | AMS AMI id to use for oracle node | N/A | SCT_AMI_ID_DB_ORACLE
| **<a href="#user-content-aws_root_disk_size_db" name="aws_root_disk_size_db">aws_root_disk_size_db</a>**  |  | N/A | SCT_AWS_ROOT_DISK_SIZE_DB
| **<a href="#user-content-aws_root_disk_size_monitor" name="aws_root_disk_size_monitor">aws_root_disk_size_monitor</a>**  |  | N/A | SCT_AWS_ROOT_DISK_SIZE_MONITOR
| **<a href="#user-content-aws_root_disk_size_loader" name="aws_root_disk_size_loader">aws_root_disk_size_loader</a>**  |  | N/A | SCT_AWS_ROOT_DISK_SIZE_LOADER
| **<a href="#user-content-ami_db_scylla_user" name="ami_db_scylla_user">ami_db_scylla_user</a>**  |  | N/A | SCT_AMI_DB_SCYLLA_USER
| **<a href="#user-content-ami_monitor_user" name="ami_monitor_user">ami_monitor_user</a>**  |  | N/A | SCT_AMI_MONITOR_USER
| **<a href="#user-content-ami_loader_user" name="ami_loader_user">ami_loader_user</a>**  |  | N/A | SCT_AMI_LOADER_USER
| **<a href="#user-content-ami_db_cassandra_user" name="ami_db_cassandra_user">ami_db_cassandra_user</a>**  |  | N/A | SCT_AMI_DB_CASSANDRA_USER
| **<a href="#user-content-spot_max_price" name="spot_max_price">spot_max_price</a>**  | The max percentage of the on demand price we set for spot/fleet instances | N/A | SCT_SPOT_MAX_PRICE
| **<a href="#user-content-extra_network_interface" name="extra_network_interface">extra_network_interface</a>**  | if true, create extra network interface on each node | N/A | SCT_EXTRA_NETWORK_INTERFACE
| **<a href="#user-content-aws_instance_profile_name" name="aws_instance_profile_name">aws_instance_profile_name</a>**  | This is the name of the instance profile to set on all instances | N/A | SCT_AWS_INSTANCE_PROFILE_NAME
| **<a href="#user-content-backup_bucket_location" name="backup_bucket_location">backup_bucket_location</a>**  | This is the bucket name to be used for backup with its region<br>(e.g. backup_bucket_location: 'manager-backup-tests') | N/A | SCT_BACKUP_BUCKET_LOCATION
| **<a href="#user-content-tag_ami_with_result" name="tag_ami_with_result">tag_ami_with_result</a>**  | If True, would tag the ami with the test final result | N/A | SCT_TAG_AMI_WITH_RESULT
| **<a href="#user-content-gce_datacenter" name="gce_datacenter">gce_datacenter</a>**  | Supported: us-east1 - means that the zone will be selected automatically or you can mention the zone explicitly, for example: us-east1-b | N/A | SCT_GCE_DATACENTER
| **<a href="#user-content-gce_network" name="gce_network">gce_network</a>**  |  | N/A | SCT_GCE_NETWORK
| **<a href="#user-content-gce_image" name="gce_image">gce_image</a>**  | GCE image to use for all node types: db, loader and monitor | N/A | SCT_GCE_IMAGE
| **<a href="#user-content-gce_image_db" name="gce_image_db">gce_image_db</a>**  |  | N/A | SCT_GCE_IMAGE_DB
| **<a href="#user-content-gce_image_monitor" name="gce_image_monitor">gce_image_monitor</a>**  |  | N/A | SCT_GCE_IMAGE_MONITOR
| **<a href="#user-content-gce_image_loader" name="gce_image_loader">gce_image_loader</a>**  |  | N/A | SCT_GCE_IMAGE_LOADER
| **<a href="#user-content-gce_image_username" name="gce_image_username">gce_image_username</a>**  |  | N/A | SCT_GCE_IMAGE_USERNAME
| **<a href="#user-content-gce_instance_type_loader" name="gce_instance_type_loader">gce_instance_type_loader</a>**  |  | N/A | SCT_GCE_INSTANCE_TYPE_LOADER
| **<a href="#user-content-gce_root_disk_type_loader" name="gce_root_disk_type_loader">gce_root_disk_type_loader</a>**  |  | N/A | SCT_GCE_ROOT_DISK_TYPE_LOADER
| **<a href="#user-content-gce_n_local_ssd_disk_loader" name="gce_n_local_ssd_disk_loader">gce_n_local_ssd_disk_loader</a>**  |  | N/A | SCT_GCE_N_LOCAL_SSD_DISK_LOADER
| **<a href="#user-content-gce_instance_type_monitor" name="gce_instance_type_monitor">gce_instance_type_monitor</a>**  |  | N/A | SCT_GCE_INSTANCE_TYPE_MONITOR
| **<a href="#user-content-gce_root_disk_type_monitor" name="gce_root_disk_type_monitor">gce_root_disk_type_monitor</a>**  |  | N/A | SCT_GCE_ROOT_DISK_TYPE_MONITOR
| **<a href="#user-content-gce_root_disk_size_monitor" name="gce_root_disk_size_monitor">gce_root_disk_size_monitor</a>**  |  | N/A | SCT_GCE_ROOT_DISK_SIZE_MONITOR
| **<a href="#user-content-gce_n_local_ssd_disk_monitor" name="gce_n_local_ssd_disk_monitor">gce_n_local_ssd_disk_monitor</a>**  |  | N/A | SCT_GCE_N_LOCAL_SSD_DISK_MONITOR
| **<a href="#user-content-gce_instance_type_db" name="gce_instance_type_db">gce_instance_type_db</a>**  |  | N/A | SCT_GCE_INSTANCE_TYPE_DB
| **<a href="#user-content-gce_root_disk_type_db" name="gce_root_disk_type_db">gce_root_disk_type_db</a>**  |  | N/A | SCT_GCE_ROOT_DISK_TYPE_DB
| **<a href="#user-content-gce_root_disk_size_db" name="gce_root_disk_size_db">gce_root_disk_size_db</a>**  |  | N/A | SCT_GCE_ROOT_DISK_SIZE_DB
| **<a href="#user-content-gce_n_local_ssd_disk_db" name="gce_n_local_ssd_disk_db">gce_n_local_ssd_disk_db</a>**  |  | N/A | SCT_GCE_N_LOCAL_SSD_DISK_DB
| **<a href="#user-content-gce_pd_standard_disk_size_db" name="gce_pd_standard_disk_size_db">gce_pd_standard_disk_size_db</a>**  |  | N/A | SCT_GCE_PD_STANDARD_DISK_SIZE_DB
| **<a href="#user-content-gce_pd_ssd_disk_size_db" name="gce_pd_ssd_disk_size_db">gce_pd_ssd_disk_size_db</a>**  |  | N/A | SCT_GCE_PD_SSD_DISK_SIZE_DB
| **<a href="#user-content-gce_pd_ssd_disk_size_loader" name="gce_pd_ssd_disk_size_loader">gce_pd_ssd_disk_size_loader</a>**  |  | N/A | SCT_GCE_PD_SSD_DISK_SIZE_LOADER
| **<a href="#user-content-gce_pd_ssd_disk_size_monitor" name="gce_pd_ssd_disk_size_monitor">gce_pd_ssd_disk_size_monitor</a>**  |  | N/A | SCT_GCE_SSD_DISK_SIZE_MONITOR
| **<a href="#user-content-gce_image_minikube" name="gce_image_minikube">gce_image_minikube</a>**  |  | N/A | SCT_GCE_IMAGE_MINIKUBE
| **<a href="#user-content-gce_instance_type_minikube" name="gce_instance_type_minikube">gce_instance_type_minikube</a>**  |  | N/A | SCT_GCE_INSTANCE_TYPE_MINIKUBE
| **<a href="#user-content-gce_root_disk_type_minikube" name="gce_root_disk_type_minikube">gce_root_disk_type_minikube</a>**  |  | N/A | SCT_GCE_ROOT_DISK_TYPE_MINIKUBE
| **<a href="#user-content-gce_root_disk_size_minikube" name="gce_root_disk_size_minikube">gce_root_disk_size_minikube</a>**  |  | N/A | SCT_GCE_ROOT_DISK_SIZE_MINIKUBE
| **<a href="#user-content-gke_cluster_version" name="gke_cluster_version">gke_cluster_version</a>**  |  | N/A | SCT_GKE_CLUSTER_VERSION
| **<a href="#user-content-k8s_deploy_monitoring" name="k8s_deploy_monitoring">k8s_deploy_monitoring</a>**  |  | N/A | SCT_K8S_DEPLOY_MONITORING
| **<a href="#user-content-k8s_scylla_operator_docker_image" name="k8s_scylla_operator_docker_image">k8s_scylla_operator_docker_image</a>**  |  | N/A | SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE
| **<a href="#user-content-k8s_scylla_operator_helm_repo" name="k8s_scylla_operator_helm_repo">k8s_scylla_operator_helm_repo</a>**  |  | N/A | SCT_K8S_SCYLLA_OPERATOR_HELM_REPO
| **<a href="#user-content-k8s_scylla_operator_chart_version" name="k8s_scylla_operator_chart_version">k8s_scylla_operator_chart_version</a>**  |  | N/A | SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION
| **<a href="#user-content-k8s_scylla_datacenter" name="k8s_scylla_datacenter">k8s_scylla_datacenter</a>**  |  | N/A | SCT_K8S_SCYLLA_DATACENTER
| **<a href="#user-content-k8s_scylla_rack" name="k8s_scylla_rack">k8s_scylla_rack</a>**  |  | N/A | SCT_K8S_SCYLLA_RACK
| **<a href="#user-content-k8s_scylla_cluster_name" name="k8s_scylla_cluster_name">k8s_scylla_cluster_name</a>**  |  | N/A | SCT_K8S_SCYLLA_CLUSTER_NAME
| **<a href="#user-content-k8s_scylla_disk_gi" name="k8s_scylla_disk_gi">k8s_scylla_disk_gi</a>**  |  | N/A | SCT_K8S_SCYLLA_DISK_GI
| **<a href="#user-content-k8s_loader_cluster_name" name="k8s_loader_cluster_name">k8s_loader_cluster_name</a>**  |  | N/A | SCT_K8S_LOADER_CLUSTER_NAME
| **<a href="#user-content-minikube_version" name="minikube_version">minikube_version</a>**  |  | N/A | SCT_MINIKUBE_VERSION
| **<a href="#user-content-k8s_cert_manager_version" name="k8s_cert_manager_version">k8s_cert_manager_version</a>**  |  | N/A | SCT_K8S_CERT_MANAGER_VERSION
| **<a href="#user-content-mgmt_docker_image" name="mgmt_docker_image">mgmt_docker_image</a>**  | Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1' | N/A | SCT_MGMT_DOCKER_IMAGE
| **<a href="#user-content-docker_image" name="docker_image">docker_image</a>**  | Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version | N/A | SCT_DOCKER_IMAGE
| **<a href="#user-content-db_nodes_private_ip" name="db_nodes_private_ip">db_nodes_private_ip</a>**  |  | N/A | SCT_DB_NODES_PRIVATE_IP
| **<a href="#user-content-db_nodes_public_ip" name="db_nodes_public_ip">db_nodes_public_ip</a>**  |  | N/A | SCT_DB_NODES_PUBLIC_IP
| **<a href="#user-content-loaders_private_ip" name="loaders_private_ip">loaders_private_ip</a>**  |  | N/A | SCT_LOADERS_PRIVATE_IP
| **<a href="#user-content-loaders_public_ip" name="loaders_public_ip">loaders_public_ip</a>**  |  | N/A | SCT_LOADERS_PUBLIC_IP
| **<a href="#user-content-monitor_nodes_private_ip" name="monitor_nodes_private_ip">monitor_nodes_private_ip</a>**  |  | N/A | SCT_MONITOR_NODES_PRIVATE_IP
| **<a href="#user-content-monitor_nodes_public_ip" name="monitor_nodes_public_ip">monitor_nodes_public_ip</a>**  |  | N/A | SCT_MONITOR_NODES_PUBLIC_IP
| **<a href="#user-content-cassandra_stress_population_size" name="cassandra_stress_population_size">cassandra_stress_population_size</a>**  |  | 1000000 | SCT_CASSANDRA_STRESS_POPULATION_SIZE
| **<a href="#user-content-cassandra_stress_threads" name="cassandra_stress_threads">cassandra_stress_threads</a>**  |  | 1000 | SCT_CASSANDRA_STRESS_THREADS
| **<a href="#user-content-add_node_cnt" name="add_node_cnt">add_node_cnt</a>**  |  | 1 | SCT_ADD_NODE_CNT
| **<a href="#user-content-stress_multiplier" name="stress_multiplier">stress_multiplier</a>**  |  | 1 | SCT_STRESS_MULTIPLIER
| **<a href="#user-content-run_fullscan" name="run_fullscan">run_fullscan</a>**  |  | N/A | SCT_RUN_FULLSCAN
| **<a href="#user-content-keyspace_num" name="keyspace_num">keyspace_num</a>**  |  | 1 | SCT_KEYSPACE_NUM
| **<a href="#user-content-round_robin" name="round_robin">round_robin</a>**  |  | N/A | SCT_ROUND_ROBIN
| **<a href="#user-content-batch_size" name="batch_size">batch_size</a>**  |  | 1 | SCT_BATCH_SIZE
| **<a href="#user-content-pre_create_schema" name="pre_create_schema">pre_create_schema</a>**  |  | N/A | SCT_PRE_CREATE_SCHEMA
| **<a href="#user-content-pre_create_keyspace" name="pre_create_keyspace">pre_create_keyspace</a>**  | Command to create keysapce to be pre-create before running workload | N/A | SCT_PRE_CREATE_KEYSPACE
| **<a href="#user-content-post_prepare_cql_cmds" name="post_prepare_cql_cmds">post_prepare_cql_cmds</a>**  | CQL Commands to run after prepare stage finished (relevant only to longevity_test.py) | N/A | SCT_POST_PREPARE_CQL_CMDS
| **<a href="#user-content-prepare_wait_no_compactions_timeout" name="prepare_wait_no_compactions_timeout">prepare_wait_no_compactions_timeout</a>**  | At the end of prepare stage, run major compaction and wait for this time (in minutes) for compaction to finish. (relevant only to longevity_test.py), Should be use only for when facing issue like compaction is affect the test or load | N/A | SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT
| **<a href="#user-content-compaction_strategy" name="compaction_strategy">compaction_strategy</a>**  | Choose a specific compaction strategy to pre-create schema with. | SizeTieredCompactionStrategy | SCT_COMPACTION_STRATEGY
| **<a href="#user-content-sstable_size" name="sstable_size">sstable_size</a>**  | Configure sstable size for the usage of pre-create-schema mode | N/A | SSTABLE_SIZE
| **<a href="#user-content-cluster_health_check" name="cluster_health_check">cluster_health_check</a>**  | When true, start cluster health checker for all nodes | True | SCT_CLUSTER_HEALTH_CHECK
| **<a href="#user-content-validate_partitions" name="validate_partitions">validate_partitions</a>**  | when true, log of the partitions before and after the nemesis run is compacted | N/A | SCT_VALIDATE_PARTITIONS
| **<a href="#user-content-table_name" name="table_name">table_name</a>**  | table name to check for the validate_partitions check | N/A | SCT_TABLE_NAME
| **<a href="#user-content-primary_key_column" name="primary_key_column">primary_key_column</a>**  | primary key of the table to check for the validate_partitions check | N/A | SCT_PRIMARY_KEY_COLUMN
| **<a href="#user-content-stress_read_cmd" name="stress_read_cmd">stress_read_cmd</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_READ_CMD
| **<a href="#user-content-prepare_verify_cmd" name="prepare_verify_cmd">prepare_verify_cmd</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_PREPARE_VERIFY_CMD
| **<a href="#user-content-user_profile_table_count" name="user_profile_table_count">user_profile_table_count</a>**  | number of tables to create for template user c-s | 1 | SCT_USER_PROFILE_TABLE_COUNT
| **<a href="#user-content-scylla_mgmt_upgrade_to_repo" name="scylla_mgmt_upgrade_to_repo">scylla_mgmt_upgrade_to_repo</a>**  | Url to the repo of scylla manager version to upgrade to for management tests | N/A | SCT_SCYLLA_MGMT_UPGRADE_TO_REPO
| **<a href="#user-content-partition_range_with_data_validation" name="partition_range_with_data_validation">partition_range_with_data_validation</a>**  | Relevant for scylla-bench. Hold range (min - max) of PKs values for partitions that data was<br>written with validate data and will be validate during the read.<br>Example: 0-250.<br>Optional parameter for DeleteByPartitionsMonkey and DeleteByRowsRangeMonkey | N/A | SCT_PARTITION_RANGE_WITH_DATA_VALIDATION
| **<a href="#user-content-max_partitions_in_test_table" name="max_partitions_in_test_table">max_partitions_in_test_table</a>**  | Relevant for scylla-bench. MAX partition keys (partition-count) in the scylla_bench.test table.<br>Mandatory parameter for DeleteByPartitionsMonkey and DeleteByRowsRangeMonkey | N/A | SCT_MAX_PARTITIONS_IN_TEST_TABLE
| **<a href="#user-content-stress_cmd_w" name="stress_cmd_w">stress_cmd_w</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_W
| **<a href="#user-content-stress_cmd_r" name="stress_cmd_r">stress_cmd_r</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_R
| **<a href="#user-content-stress_cmd_m" name="stress_cmd_m">stress_cmd_m</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_M
| **<a href="#user-content-prepare_write_cmd" name="prepare_write_cmd">prepare_write_cmd</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_PREPARE_WRITE_CMD
| **<a href="#user-content-stress_cmd_no_mv" name="stress_cmd_no_mv">stress_cmd_no_mv</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_NO_MV
| **<a href="#user-content-stress_cmd_no_mv_profile" name="stress_cmd_no_mv_profile">stress_cmd_no_mv_profile</a>**  |  | N/A | SCT_STRESS_CMD_NO_MV_PROFILE
| **<a href="#user-content-cs_user_profiles" name="cs_user_profiles">cs_user_profiles</a>**  |  | N/A | SCT_CS_USER_PROFILES
| **<a href="#user-content-cs_duration" name="cs_duration">cs_duration</a>**  |  | 50m | SCT_CS_DURATION
| **<a href="#user-content-stress_cmd_mv" name="stress_cmd_mv">stress_cmd_mv</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_MV
| **<a href="#user-content-prepare_stress_cmd" name="prepare_stress_cmd">prepare_stress_cmd</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_PREPARE_STRESS_CMD
| **<a href="#user-content-skip_download" name="skip_download">skip_download</a>**  |  | N/A | SCT_SKIP_DOWNLOAD
| **<a href="#user-content-sstable_file" name="sstable_file">sstable_file</a>**  |  | N/A | SCT_SSTABLE_FILE
| **<a href="#user-content-sstable_url" name="sstable_url">sstable_url</a>**  |  | N/A | SCT_SSTABLE_URL
| **<a href="#user-content-sstable_md5" name="sstable_md5">sstable_md5</a>**  |  | N/A | SCT_SSTABLE_MD5
| **<a href="#user-content-flush_times" name="flush_times">flush_times</a>**  |  | N/A | SCT_FLUSH_TIMES
| **<a href="#user-content-flush_period" name="flush_period">flush_period</a>**  |  | N/A | SCT_FLUSH_PERIOD
| **<a href="#user-content-new_scylla_repo" name="new_scylla_repo">new_scylla_repo</a>**  |  | N/A | SCT_NEW_SCYLLA_REPO
| **<a href="#user-content-new_version" name="new_version">new_version</a>**  | Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1 | N/A | SCT_NEW_VERSION
| **<a href="#user-content-target_upgrade_version" name="target_upgrade_version">target_upgrade_version</a>**  | Assign target upgrade version, use for decide if the truncate entries test should be run. This test should be performed in case the target upgrade version >= 3.1 | N/A | SCT_TAGRET_UPGRADE_VERSION
| **<a href="#user-content-upgrade_node_packages" name="upgrade_node_packages">upgrade_node_packages</a>**  |  | N/A | SCT_UPGRADE_NODE_PACKAGES
| **<a href="#user-content-test_sst3" name="test_sst3">test_sst3</a>**  |  | N/A | SCT_TEST_SST3
| **<a href="#user-content-test_upgrade_from_installed_3_1_0" name="test_upgrade_from_installed_3_1_0">test_upgrade_from_installed_3_1_0</a>**  | Enable an option for installed 3.1.0 for work around a scylla issue if it's true | N/A | SCT_TEST_UPGRADE_FROM_INSTALLED_3_1_0
| **<a href="#user-content-authorization_in_upgrade" name="authorization_in_upgrade">authorization_in_upgrade</a>**  | Which Authorization to enable after upgrade | N/A | SCT_AUTHORIZATION_IN_UPGRADE
| **<a href="#user-content-remove_authorization_in_rollback" name="remove_authorization_in_rollback">remove_authorization_in_rollback</a>**  | Disable Authorization after rollback to old Scylla | N/A | SCT_REMOVE_AUTHORIZATION_IN_ROLLBACK
| **<a href="#user-content-new_introduced_pkgs" name="new_introduced_pkgs">new_introduced_pkgs</a>**  |  | N/A | SCT_NEW_INTRODUCED_PKGS
| **<a href="#user-content-recover_system_tables" name="recover_system_tables">recover_system_tables</a>**  |  | N/A | SCT_RECOVER_SYSTEM_TABLES
| **<a href="#user-content-stress_cmd_1" name="stress_cmd_1">stress_cmd_1</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_1
| **<a href="#user-content-stress_cmd_complex_prepare" name="stress_cmd_complex_prepare">stress_cmd_complex_prepare</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_COMPLEX_PREPARE
| **<a href="#user-content-prepare_write_stress" name="prepare_write_stress">prepare_write_stress</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_PREPARE_WRITE_STRESS
| **<a href="#user-content-stress_cmd_read_10m" name="stress_cmd_read_10m">stress_cmd_read_10m</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_READ_10M
| **<a href="#user-content-stress_cmd_read_cl_one" name="stress_cmd_read_cl_one">stress_cmd_read_cl_one</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure. | N/A | SCT_STRESS_CMD_READ_CL_ONE
| **<a href="#user-content-stress_cmd_read_60m" name="stress_cmd_read_60m">stress_cmd_read_60m</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_READ_60M
| **<a href="#user-content-stress_cmd_complex_verify_read" name="stress_cmd_complex_verify_read">stress_cmd_complex_verify_read</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_COMPLEX_VERIFY_READ
| **<a href="#user-content-stress_cmd_complex_verify_more" name="stress_cmd_complex_verify_more">stress_cmd_complex_verify_more</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_COMPLEX_VERIFY_MORE
| **<a href="#user-content-write_stress_during_entire_test" name="write_stress_during_entire_test">write_stress_during_entire_test</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_WRITE_STRESS_DURING_ENTIRE_TEST
| **<a href="#user-content-verify_data_after_entire_test" name="verify_data_after_entire_test">verify_data_after_entire_test</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure. | N/A | SCT_VERIFY_DATA_AFTER_ENTIRE_TEST
| **<a href="#user-content-stress_cmd_read_cl_quorum" name="stress_cmd_read_cl_quorum">stress_cmd_read_cl_quorum</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_READ_CL_QUORUM
| **<a href="#user-content-verify_stress_after_cluster_upgrade" name="verify_stress_after_cluster_upgrade">verify_stress_after_cluster_upgrade</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE
| **<a href="#user-content-stress_cmd_complex_verify_delete" name="stress_cmd_complex_verify_delete">stress_cmd_complex_verify_delete</a>**  | cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | N/A | SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE
| **<a href="#user-content-scylla_encryption_options" name="scylla_encryption_options">scylla_encryption_options</a>**  | options will be used for enable encryption at-rest for tables | N/A | SCT_SCYLLA_ENCRYPTION_OPTIONS
| **<a href="#user-content-logs_transport" name="logs_transport">logs_transport</a>**  | How to transport logs: rsyslog, ssh or docker | rsyslog | SCT_LOGS_TRANSPORT
| **<a href="#user-content-collect_logs" name="collect_logs">collect_logs</a>**  | Collect logs from instances and sct runner | N/A | SCT_COLLECT_LOGS
| **<a href="#user-content-execute_post_behavior" name="execute_post_behavior">execute_post_behavior</a>**  | Run post behavior actions in sct teardown step | N/A | SCT_EXECUTE_POST_BEHAVIOR
| **<a href="#user-content-post_behavior_db_nodes" name="post_behavior_db_nodes">post_behavior_db_nodes</a>**  | Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed | keep-on-failure | SCT_POST_BEHAVIOR_DB_NODES
| **<a href="#user-content-post_behavior_loader_nodes" name="post_behavior_loader_nodes">post_behavior_loader_nodes</a>**  | Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed | destroy | SCT_POST_BEHAVIOR_LOADER_NODES
| **<a href="#user-content-post_behavior_monitor_nodes" name="post_behavior_monitor_nodes">post_behavior_monitor_nodes</a>**  | Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed | keep-on-failure | SCT_POST_BEHAVIOR_MONITOR_NODES
| **<a href="#user-content-workaround_kernel_bug_for_iotune" name="workaround_kernel_bug_for_iotune">workaround_kernel_bug_for_iotune</a>**  | Workaround a known kernel bug which causes iotune to fail in scylla_io_setup, only effect GCE backend | N/A | SCT_WORKAROUND_KERNEL_BUG_FOR_IOTUNE
| **<a href="#user-content-internode_compression" name="internode_compression">internode_compression</a>**  | scylla option: internode_compression | N/A | SCT_INTERNODE_COMPRESSION
| **<a href="#user-content-internode_encryption" name="internode_encryption">internode_encryption</a>**  | scylla sub option of server_encryption_options: internode_encryption | all | SCT_INTERNODE_ENCRYPTION
| **<a href="#user-content-jmx_heap_memory" name="jmx_heap_memory">jmx_heap_memory</a>**  | The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB) | N/A | SCT_JMX_HEAP_MEMORY
| **<a href="#user-content-loader_swap_size" name="loader_swap_size">loader_swap_size</a>**  | The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB | 1024 | SCT_LOADER_SWAP_SIZE
| **<a href="#user-content-monitor_swap_size" name="monitor_swap_size">monitor_swap_size</a>**  | The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB | 8192 | SCT_MONITOR_SWAP_SIZE
| **<a href="#user-content-store_perf_results" name="store_perf_results">store_perf_results</a>**  | A flag that indicates whether or not to gather the prometheus stats at the end of the run.<br>Intended to be used in performance testing | N/A | SCT_STORE_PERF_RESULTS
| **<a href="#user-content-append_scylla_setup_args" name="append_scylla_setup_args">append_scylla_setup_args</a>**  | More arguments to append to scylla_setup command line | N/A | SCT_APPEND_SCYLLA_SETUP_ARGS
| **<a href="#user-content-use_preinstalled_scylla" name="use_preinstalled_scylla">use_preinstalled_scylla</a>**  | Don't install/update ScyllaDB on DB nodes | N/A | SCT_USE_PREINSTALLED_SCYLLA
| **<a href="#user-content-stress_cdclog_reader_cmd" name="stress_cdclog_reader_cmd">stress_cdclog_reader_cmd</a>**  | cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node , -keyspace, -table, parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list | cdc-stressor -stream-query-round-duration 30s | SCT_STRESS_CDCLOG_READER_CMD
| **<a href="#user-content-store_cdclog_reader_stats_in_es" name="store_cdclog_reader_stats_in_es">store_cdclog_reader_stats_in_es</a>**  | Add cdclog reader stats to ES for future performance result calculating | N/A | SCT_STORE_CDCLOG_READER_STATS_IN_ES
| **<a href="#user-content-stop_test_on_stress_failure" name="stop_test_on_stress_failure">stop_test_on_stress_failure</a>**  | If set to True the test will be stopped immediately when stress command failed.<br>When set to False the test will continue to run even when there are errors in the<br>stress process | True | SCT_STOP_TEST_ON_STRESS_FAILURE
| **<a href="#user-content-stress_cdc_log_reader_batching_enable" name="stress_cdc_log_reader_batching_enable">stress_cdc_log_reader_batching_enable</a>**  | retrieving data from multiple streams in one poll | True | SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE
| **<a href="#user-content-use_legacy_cluster_init" name="use_legacy_cluster_init">use_legacy_cluster_init</a>**  | Use legacy cluster initialization with autobootsrap disabled and parallel node setup | N/A | SCT_USE_LEGACY_CLUSTER_INIT
| **<a href="#user-content-availability_zone" name="availability_zone">availability_zone</a>**  | Availability zone to use. Same for multi-region scenario. | N/A | SCT_AVAILABILITY_ZONE
| **<a href="#user-content-num_nodes_to_rollback" name="num_nodes_to_rollback">num_nodes_to_rollback</a>**  | Number of nodes to upgrade and rollback in test_generic_cluster_upgrade | N/A | SCT_NUM_NODES_TO_ROLLBACK
| **<a href="#user-content-upgrade_sstables" name="upgrade_sstables">upgrade_sstables</a>**  | Whether to upgrade sstables as part of upgrade_node or not | N/A | SCT_UPGRADE_SSTABLES
| **<a href="#user-content-stress_before_upgrade" name="stress_before_upgrade">stress_before_upgrade</a>**  | Stress command to be run before upgrade (preapre stage) | N/A | SCT_STRESS_BEFORE_UPGRADE
| **<a href="#user-content-stress_during_entire_upgrade" name="stress_during_entire_upgrade">stress_during_entire_upgrade</a>**  | Stress command to be run during the upgrade - user should take care for suitable duration | N/A | SCT_STRESS_DURING_ENTIRE_UPGRADE
| **<a href="#user-content-stress_after_cluster_upgrade" name="stress_after_cluster_upgrade">stress_after_cluster_upgrade</a>**  | Stress command to be run after full upgrade - usually used to read the dataset for verification | N/A | SCT_STRESS_AFTER_CLUSTER_UPGRADE
| **<a href="#user-content-jepsen_scylla_repo" name="jepsen_scylla_repo">jepsen_scylla_repo</a>**  | Link to the git repository with Jepsen Scylla tests | https://github.com/jepsen-io/scylla.git | SCT_JEPSEN_SCYLLA_REPO
| **<a href="#user-content-jepsen_test_cmd" name="jepsen_test_cmd">jepsen_test_cmd</a>**  | Jepsen test command (e.g., 'test-all') | N/A | SCT_JEPSEN_TEST_CMD
| **<a href="#user-content-max_events_severities" name="max_events_severities">max_events_severities</a>**  | Limit severity level for event types | N/A | SCT_MAX_EVENTS_SEVERITIES
| **<a href="#user-content-scylla_rsyslog_setup" name="scylla_rsyslog_setup">scylla_rsyslog_setup</a>**  | Configure rsyslog on scylla nodes to send logs to monitoring nodes | False | SCT_SCYLLA_RSYSLOG_SETUP
| **<a href="#user-content-cdc_replication_rounds_num" name="cdc_replication_rounds_num">cdc_replication_rounds_num</a>**  | Number of rounds for cdc replication longevity tests | False | SCT_CDC_REPLICATION_ROUNDS_NUM
>>>>>>> 4e53319da (test(cdc replication): Enable cdc replication tests in regular run)
