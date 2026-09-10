# scylla-cluster-tests configuration options

Every option can be set in a config file, or as an environment variable named
`SCT_<OPTION>` (upper-cased). Options are grouped by what they configure: cross-cutting
concerns first, then one page per backend, then one per test type.

#### Appending with environment variables or with config files
* **strings:** can be appended with adding `++` at the beginning of the string:
       `export SCT_APPEND_SCYLLA_ARGS="++ --overprovisioned 1"`
* **list:** can be appended by adding `++` as the first item of the list
       `export SCT_SCYLLA_D_OVERRIDES_FILES='["++", "extra_file/scylla.d/io.conf"]'`

#### Nested (dict/list) options
* A single sub-key of a dict/list option can be set on its own, without
       quoting the whole value, using either dot-notation or double-underscore
       notation: `SCT_STRESS_IMAGE.ycsb=...` or `SCT_STRESS_IMAGE__ycsb=...`.
* `__` is the bash-exportable form (dots are invalid in bash variable names),
       so prefer it with plain `export`, e.g. `export SCT_STRESS_IMAGE__ycsb=...`.
* Sub-keys containing `-` (e.g. `cassandra-stress`) still require the dot form,
       set via `env 'SCT_STRESS_IMAGE.cassandra-stress=...' ...`, since `-` is not
       a valid bash identifier character either.
* **Case matters:** the `__` form lower-cases the sub-key (e.g.
       `SCT_INSTANCE_TYPE_DB__ARCH` becomes sub-key `arch`), while the `.` form
       preserves case verbatim (`SCT_INSTANCE_TYPE_DB.ARCH` stays `ARCH`). This
       matters for sub-keys consumed by case-sensitive lookups -- prefer
       uppercase sub-keys with `__` (they'll be lowered, matching the common
       convention) rather than the case-preserving dot form.

<<<<<<< HEAD
#### Options by group
The options below are grouped by domain -- cross-cutting concerns first, then one
section per backend, then one per test type. Each group mirrors a mixin module under
`sdcm/sct_config/mixins/`.

# General and provisioning


## **adaptive_timeout_multipliers** / SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS

Optional dict of adaptive-timeout multipliers keyed by operation name (from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). If the current operation key is absent, multiplier 1.0 is used.<br>YAML example:<br>adaptive_timeout_multipliers:<br>  decommission: 4<br>  new_node: 2<br>Environment variable examples:<br>SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS="{'decommission': 4, 'new_node': 2}"<br>Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4

**default:** {}

**type:** sdcm.sct_config.AdaptiveTimeoutMultipliers


## **adaptive_timeout_store_metrics** / SCT_ADAPTIVE_TIMEOUT_STORE_METRICS

Store adaptive timeout metrics in Argus. Disabled for performance tests only.

**default:** True

**type:** bool


## **add_node_cnt** / SCT_ADD_NODE_CNT

The number of nodes to add during the test.

**default:** 1

**type:** int


## **agent** / SCT_AGENT

Configuration for SCT agent - a lightweight service for remote command execution.                 When enabled, replaces SSH-based command execution with RESTful API calls for DB nodes.<br>Configuration options:<br>- enabled: bool - enable agent (required)<br>- port: int - agent HTTP API port (default: 16000)<br>- binary_url: str - URL to download agent binary<br>- max_concurrent_jobs: int - max concurrent jobs per agent (default: 10)<br>- log_level: str - logging level (default: info)<br>- tls: bool - enable TLS for agent communication (default: false)

**default:** {'enabled': False, 'port': 16000, 'binary_url': '', 'max_concurrent_jobs': 10, 'log_level': 'info', 'tls': False}

**type:** dict | YAML/JSON string → dict


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `a`: aws, oci, aws-siren, k8s-local-kind-aws
- `c`: k8s-gke
- `a,b`: k8s-eks


## **billing_project** / SCT_BILLING_PROJECT

Billing project for the test run. Used for cost tracking and reporting

**default:** N/A

**type:** str (appendable)


## **bisect_end_date** / SCT_BISECT_END_DATE

End date for bisecting test runs to find regressions

**default:** N/A

**type:** str (appendable)


## **bisect_start_date** / SCT_BISECT_START_DATE

Start date for bisecting test runs to find regressions

**default:** N/A

**type:** str (appendable)


## **cluster_backend** / SCT_CLUSTER_BACKEND

backend that will be used, aws/gce/azure/oci/docker/xcloud

**default:** N/A

**type:** str


## **cluster_health_check** / SCT_CLUSTER_HEALTH_CHECK

Enable or disable starting cluster health checker for all nodes

**default:** True

**type:** bool


## **cluster_health_check_parallel_workers** / SCT_CLUSTER_HEALTH_CHECK_PARALLEL_WORKERS

Number of parallel workers for health checks. Values above 10 are not recommended (diminishing returns, risk of API rate limiting). Default: 5.

**default:** 5

**type:** int


## **config_files** / SCT_CONFIG_FILES

a list of config files that would be used

**default:** N/A

**type:** str | list[str] → list[str]


## **data_volume_disk_iops** / SCT_DATA_VOLUME_DISK_IOPS

Number of iops for ebs type io2|io3|gp3

**default:** 0

**type:** int

**backend overrides:**
- `10000`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks


## **data_volume_disk_num** / SCT_DATA_VOLUME_DISK_NUM

Number of additional data volumes attached to instances<br>if data_volume_disk_num > 0, then data volumes (ebs on aws) will be<br>used for scylla data directory

**default:** 0

**type:** int


## **data_volume_disk_size** / SCT_DATA_VOLUME_DISK_SIZE

Size of additional volume in GB

**default:** 0

**type:** int

**backend overrides:**
- `500`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **data_volume_disk_throughput** / SCT_DATA_VOLUME_DISK_THROUGHPUT

Throughput in MiB/sec for ebs type gp3. Min is 125. Max is 1000.

**default:** N/A

**type:** int


## **data_volume_disk_type** / SCT_DATA_VOLUME_DISK_TYPE

Type of additional volumes. AWS: gp2|gp3|io2|io3. OCI: lower_cost|balanced|higher_performance|ultra

**default:** N/A

**type:** Literal['gp2', 'gp3', 'io2', 'io3', '', 'lower_cost', 'balanced', 'higher_performance', 'ultra']

**backend overrides:**
- `gp2`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks
- `ultra`: oci


## **db_nodes_shards_selection** / SCT_DB_NODES_SHARDS_SELECTION

How to select number of shards of Scylla. Expected values: default/random.<br>Default value: 'default'.<br>In case of random option - Scylla will start with different (random) shards on every node of the cluster

**default:** default

**type:** Literal['default', 'random']


## **fallback_to_next_availability_zone** / SCT_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

On capacity errors, automatically retry provisioning in the next available AZ in the same region. Backend-agnostic parameter; supersedes `aws_fallback_to_next_availability_zone`.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **fallback_to_next_region** / SCT_FALLBACK_TO_NEXT_REGION

On capacity errors, after all AZs/zones in the configured region are exhausted, relocate to the next eligible region: a single-region cluster moves as a whole, while in a multi-region test only the exhausted datacenter is relocated (to a region no other datacenter occupies) and the cluster is retried. On AWS the target region should be VPC-peered with the runner region with infra-prepared and AMI available; on GCE the global VPC and global images make any supported region eligible. Only applies during initial setup. Supported backends: AWS, GCE.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **force_run_iotune** / SCT_FORCE_RUN_IOTUNE

Force running iotune on the DB nodes, regardless if image has predefined values

**default:** N/A

**type:** bool


<<<<<<< HEAD
## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] → list[str] (appendable)


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'

**default:** N/A

**type:** str

**backend overrides:**
- `6.2.3`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** str


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** str


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically looks up cloud images for formal versions.<br>WARNING: can't be used together with 'ami_id_db_oracle' and 'oci_image_db_oracle'

**default:** 2026.1

**type:** str


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str

**backend overrides:**
- `centos`: docker


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-jammy

**type:** str


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

unlock specified experimental features

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** False

**type:** bool


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** False

**type:** bool


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** 0

**type:** int


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal['PasswordAuthenticator', 'AllowAllAuthenticator', 'com.scylladb.auth.SaslauthdAuthenticator']


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

**type:** Literal['AllowAllAuthorizer', 'CassandraAuthorizer']


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

Type of dynamodb table to create with range key or not

**default:** HASH

**type:** Literal['HASH', 'HASH_AND_RANGE']


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** True

**type:** bool


## **alternator_loadbalancing** / SCT_ALTERNATOR_LOADBALANCING

If true, enable native load balancing for alternator

**default:** False

**type:** bool


## **alternator_test_table** / SCT_ALTERNATOR_TEST_TABLE

Dictionary of a test alternator table features:<br>name: str - the name of the table<br>lsi_name: str - the name of the local secondary index to create with a table<br>gsi_name: str - the name of the global secondary index to create with a table<br>tags: dict - the tags to apply to the created table<br>items: int - expected number of items in the table after prepare

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** False

**type:** bool


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_trust_all_certificates** / SCT_ALTERNATOR_TRUST_ALL_CERTIFICATES

If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)

**default:** True

**type:** bool


## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** False

**type:** bool


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)

**backend overrides:**
- `--abort-on-lsa-bad-alloc 1 --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str (appendable)


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** {'rf_rack_valid_keyspaces': True}

**type:** dict | str | pydantic.main.BaseModel


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


# Authentication, encryption and credentials


## **user_credentials_path** / SCT_USER_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** str

**backend overrides:**
- `~/.ssh/scylla_test_id_ed25519`: aws, gce, azure, oci, docker, baremetal, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


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

**type:** str (appendable)


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** False

**type:** bool


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** False

**type:** bool


## **keystore_backend** / SCT_KEYSTORE_BACKEND

Credential storage backend for KeyStore: 'secretsmanager' (default) or 's3' (legacy)

**default:** secretsmanager

**type:** Literal['s3', 'secretsmanager']


## **keystore_sm_prefix** / SCT_KEYSTORE_SM_PREFIX

AWS Secrets Manager secret name prefix when keystore_backend=secretsmanager (default: 'sct/')

**default:** sct/

**type:** str (appendable)


## **keystore_sm_region** / SCT_KEYSTORE_SM_REGION

AWS region holding the KeyStore secrets when keystore_backend=secretsmanager (default: 'us-east-1')

**default:** us-east-1

**type:** str (appendable)


# Nemesis (chaos testing)


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Supported syntax:<br>- nemesis_class_name: "NemesisName"<br>Run one nemesis in a single thread.<br>- nemesis_class_name: ["NemesisA", "NemesisB"]<br>Run NemesisA and NemesisB each in their own thread.<br>- nemesis_class_name: ["SisyphusMonkey", "SisyphusMonkey"]<br>Run two SisyphusMonkey threads in parallel.<br>Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and<br>space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no<br>longer supported. Use an explicit YAML list instead.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


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

**type:** int | list[int] | space-separated ints → list[int]


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** str (appendable)


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** 0

**type:** int


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** False

**type:** bool


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 2

**type:** int


# Stress commands and load generation


## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from stress_cmd parameters<br>and is used in test duration calculation

**default:** 0

**type:** int


## **alternator_stress_rate** / SCT_ALTERNATOR_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing.

**default:** N/A

**type:** int


## **alternator_write_always_lwt_stress_rate** / SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.

**default:** N/A

**type:** int


## **stress_cmd** / SCT_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.<br>Local files are uploaded to the loader via send_files and mounted into the Gemini Docker<br>container via --schema.<br>Remote URLs are downloaded on the loader node with curl and then mounted the same way.

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

**type:** bool


## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']

**default:** N/A

**type:** list


## **run_gemini_in_rolling_upgrade** / SCT_RUN_GEMINI_IN_ROLLING_UPGRADE

Enable running Gemini workload during rolling upgrade test. Default is false.

**default:** False

**type:** bool


## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** False

**type:** bool


## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** {}

**type:** dict | YAML/JSON string → dict


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** str (appendable)


## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.<br>Also used as a fallback source for keyspace/table in gradual performance tests when<br>perf_stress_keyspace/perf_stress_table are not set.<br>For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}

**default:** {}

**type:** dict | YAML/JSON string → dict


## **c_s_driver_version** / SCT_C_S_DRIVER_VERSION

cassandra-stress driver version to use: 3|4|random

**default:** 3

**type:** Literal['3', '4', 'random']


# Monitoring, events and reporting


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.16

**type:** str (appendable)

**backend overrides:**
- `N/A`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **sct_ngrok_name** / SCT_SCT_NGROK_NAME

Override the default hostname address of the sct test runner, using ngrok server, see readme for more instructions

**default:** N/A

**type:** str (appendable)


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** bool

**backend overrides:**
- `False`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **backtrace_stall_decoding** / SCT_BACKTRACE_STALL_DECODING

If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during<br>backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.

**default:** True

**type:** bool


## **backtrace_decoding_disable_regex** / SCT_BACKTRACE_DECODING_DISABLE_REGEX

Regex pattern to disable backtrace decoding for specific event types. If an event type matches<br>this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests<br>by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.

**default:** N/A

**type:** str (appendable)


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** bool

**backend overrides:**
- `False`: docker


## **enable_kernel_panic_checker** / SCT_ENABLE_KERNEL_PANIC_CHECKER

Enable kernel panic detection by monitoring cloud instance console output for panic indicators. When enabled, a background thread monitors each node's console output for kernel panic patterns.

**default:** True

**type:** bool


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str | list[str] → list[str] (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Email subject postfix

**default:** N/A

**type:** str (appendable)


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** False

**type:** bool


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Limit number events in email reports

**default:** 10

**type:** int


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** bool


## **argus_use_ssh_tunnel** / SCT_ARGUS_USE_SSH_TUNNEL

Enable SSH tunnel support in the Argus client connection

**default:** True

**type:** bool


## **download_from_s3** / SCT_DOWNLOAD_FROM_S3

Destination-source map of dirs/buckets to download from S3 before starting the test

**default:** []

**type:** list


## **argus_email_report_template** / SCT_ARGUS_EMAIL_REPORT_TEMPLATE

Path to the email report template used for sending argus email reports

**default:** email_report_template_basic.yaml

**type:** str (appendable)


# Scylla Manager


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


## **manager_version** / SCT_MANAGER_VERSION

Version of Scylla Manager server and agent to install

**default:** 3.12

**type:** str


## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Version of Scylla Manager server and agent to upgrade to

**default:** N/A

**type:** str


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Version of ScyllaDB to install as Manager backend

**default:** 2025.4

**type:** str


## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION

Version of Scylla Manager agent to install for management tests

**default:** 3.12.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** False

**type:** bool


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool


## **agent** / SCT_AGENT

Configuration for SCT agent - a lightweight service for remote command execution.                 When enabled, replaces SSH-based command execution with RESTful API calls for DB nodes.<br>Configuration options:<br>- enabled: bool - enable agent (required)<br>- port: int - agent HTTP API port (default: 16000)<br>- binary_url: str - URL to download agent binary<br>- max_concurrent_jobs: int - max concurrent jobs per agent (default: 10)<br>- log_level: str - logging level (default: info)<br>- tls: bool - enable TLS for agent communication (default: false)

**default:** {'enabled': False, 'port': 16000, 'binary_url': '', 'max_concurrent_jobs': 10, 'log_level': 'info', 'tls': False}

**type:** dict | YAML/JSON string → dict


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


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


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

**type:** dict | YAML/JSON string → dict


# Vector Store


## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** 0

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

**default:** 0

**type:** int


# AWS backend


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] → list[str] (appendable)


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'

**default:** N/A

**type:** str

**backend overrides:**
- `6.2.3`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** str


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Format version of the user-data to use for scylla images,<br>default to what tagged on the image used

**default:** N/A

**type:** str


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically looks up cloud images for formal versions.<br>WARNING: can't be used together with the backend's oracle image param<br>('ami_id_db_oracle', 'gce_image_db_oracle', 'azure_image_db_oracle' or 'oci_image_db_oracle')

**default:** 2026.1

**type:** str


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str

**backend overrides:**
- `centos`: docker


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-jammy

**type:** str


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

unlock specified experimental features

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** False

**type:** bool


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** False

**type:** bool


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** 0

**type:** int


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal['PasswordAuthenticator', 'AllowAllAuthenticator', 'com.scylladb.auth.SaslauthdAuthenticator']


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

**type:** Literal['AllowAllAuthorizer', 'CassandraAuthorizer']


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

Type of dynamodb table to create with range key or not

**default:** HASH

**type:** Literal['HASH', 'HASH_AND_RANGE']


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** True

**type:** bool


## **alternator_loadbalancing** / SCT_ALTERNATOR_LOADBALANCING

If true, enable native load balancing for alternator

**default:** False

**type:** bool


## **alternator_test_table** / SCT_ALTERNATOR_TEST_TABLE

Dictionary of a test alternator table features:<br>name: str - the name of the table<br>lsi_name: str - the name of the local secondary index to create with a table<br>gsi_name: str - the name of the global secondary index to create with a table<br>tags: dict - the tags to apply to the created table<br>items: int - expected number of items in the table after prepare

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** False

**type:** bool


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_trust_all_certificates** / SCT_ALTERNATOR_TRUST_ALL_CERTIFICATES

If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)

**default:** True

**type:** bool


## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** False

**type:** bool


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)

**backend overrides:**
- `--abort-on-lsa-bad-alloc 1 --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str (appendable)


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** {'rf_rack_valid_keyspaces': True}

**type:** dict | str | pydantic.main.BaseModel


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


# Authentication, encryption and credentials


## **user_credentials_path** / SCT_USER_CREDENTIALS_PATH

Path to your user credentials. qa key are downloaded automatically from S3 bucket

**default:** N/A

**type:** str

**backend overrides:**
- `~/.ssh/scylla_test_id_ed25519`: aws, gce, azure, oci, docker, baremetal, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


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

**type:** str (appendable)


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** False

**type:** bool


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** False

**type:** bool


## **keystore_backend** / SCT_KEYSTORE_BACKEND

Credential storage backend for KeyStore: 'secretsmanager' (default) or 's3' (legacy)

**default:** secretsmanager

**type:** Literal['s3', 'secretsmanager']


## **keystore_sm_prefix** / SCT_KEYSTORE_SM_PREFIX

AWS Secrets Manager secret name prefix when keystore_backend=secretsmanager (default: 'sct/')

**default:** sct/

**type:** str (appendable)


## **keystore_sm_region** / SCT_KEYSTORE_SM_REGION

AWS region holding the KeyStore secrets when keystore_backend=secretsmanager (default: 'us-east-1')

**default:** us-east-1

**type:** str (appendable)


# Nemesis (chaos testing)


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Supported syntax:<br>- nemesis_class_name: "NemesisName"<br>Run one nemesis in a single thread.<br>- nemesis_class_name: ["NemesisA", "NemesisB"]<br>Run NemesisA and NemesisB each in their own thread.<br>- nemesis_class_name: ["SisyphusMonkey", "SisyphusMonkey"]<br>Run two SisyphusMonkey threads in parallel.<br>Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and<br>space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no<br>longer supported. Use an explicit YAML list instead.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


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

**type:** int | list[int] | space-separated ints → list[int]


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** str (appendable)


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** 0

**type:** int


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** False

**type:** bool


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 2

**type:** int


# Stress commands and load generation


## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from stress_cmd parameters<br>and is used in test duration calculation

**default:** 0

**type:** int


## **alternator_stress_rate** / SCT_ALTERNATOR_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing.

**default:** N/A

**type:** int


## **alternator_write_always_lwt_stress_rate** / SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.

**default:** N/A

**type:** int


## **stress_cmd** / SCT_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.<br>Local files are uploaded to the loader via send_files and mounted into the Gemini Docker<br>container via --schema.<br>Remote URLs are downloaded on the loader node with curl and then mounted the same way.

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

**type:** bool


## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']

**default:** N/A

**type:** list


## **run_gemini_in_rolling_upgrade** / SCT_RUN_GEMINI_IN_ROLLING_UPGRADE

Enable running Gemini workload during rolling upgrade test. Default is false.

**default:** False

**type:** bool


## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** False

**type:** bool


## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** {}

**type:** dict | YAML/JSON string → dict


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** str (appendable)


## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.<br>Also used as a fallback source for keyspace/table in gradual performance tests when<br>perf_stress_keyspace/perf_stress_table are not set.<br>For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}

**default:** {}

**type:** dict | YAML/JSON string → dict


## **c_s_driver_version** / SCT_C_S_DRIVER_VERSION

cassandra-stress driver version to use: 3|4|random

**default:** 3

**type:** Literal['3', '4', 'random']


# Monitoring, events and reporting


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.16

**type:** str (appendable)

**backend overrides:**
- `N/A`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **sct_ngrok_name** / SCT_SCT_NGROK_NAME

Override the default hostname address of the sct test runner, using ngrok server, see readme for more instructions

**default:** N/A

**type:** str (appendable)


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** bool

**backend overrides:**
- `False`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **backtrace_stall_decoding** / SCT_BACKTRACE_STALL_DECODING

If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during<br>backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.

**default:** True

**type:** bool


## **backtrace_decoding_disable_regex** / SCT_BACKTRACE_DECODING_DISABLE_REGEX

Regex pattern to disable backtrace decoding for specific event types. If an event type matches<br>this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests<br>by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.

**default:** N/A

**type:** str (appendable)


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** bool

**backend overrides:**
- `False`: docker


## **enable_kernel_panic_checker** / SCT_ENABLE_KERNEL_PANIC_CHECKER

Enable kernel panic detection by monitoring cloud instance console output for panic indicators. When enabled, a background thread monitors each node's console output for kernel panic patterns.

**default:** True

**type:** bool


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str | list[str] → list[str] (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Email subject postfix

**default:** N/A

**type:** str (appendable)


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** False

**type:** bool


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Limit number events in email reports

**default:** 10

**type:** int


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** bool


## **argus_use_ssh_tunnel** / SCT_ARGUS_USE_SSH_TUNNEL

Enable SSH tunnel support in the Argus client connection

**default:** True

**type:** bool


## **download_from_s3** / SCT_DOWNLOAD_FROM_S3

Destination-source map of dirs/buckets to download from S3 before starting the test

**default:** []

**type:** list


## **argus_email_report_template** / SCT_ARGUS_EMAIL_REPORT_TEMPLATE

Path to the email report template used for sending argus email reports

**default:** email_report_template_basic.yaml

**type:** str (appendable)


# Scylla Manager


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


## **manager_version** / SCT_MANAGER_VERSION

Version of Scylla Manager server and agent to install

**default:** 3.12

**type:** str


## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Version of Scylla Manager server and agent to upgrade to

**default:** N/A

**type:** str


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Version of ScyllaDB to install as Manager backend

**default:** 2025.4

**type:** str


## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION

Version of Scylla Manager agent to install for management tests

**default:** 3.12.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** False

**type:** bool


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool


## **agent** / SCT_AGENT

Configuration for SCT agent - a lightweight service for remote command execution.                 When enabled, replaces SSH-based command execution with RESTful API calls for DB nodes.<br>Configuration options:<br>- enabled: bool - enable agent (required)<br>- port: int - agent HTTP API port (default: 16000)<br>- binary_url: str - URL to download agent binary<br>- max_concurrent_jobs: int - max concurrent jobs per agent (default: 10)<br>- log_level: str - logging level (default: info)<br>- tls: bool - enable TLS for agent communication (default: false)

**default:** {'enabled': False, 'port': 16000, 'binary_url': '', 'max_concurrent_jobs': 10, 'log_level': 'info', 'tls': False}

**type:** dict | YAML/JSON string → dict


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


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


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

**type:** dict | YAML/JSON string → dict


# Vector Store


## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** 0

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

**default:** 0

**type:** int


# AWS backend


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


=======
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **instance_provision** / SCT_INSTANCE_PROVISION

instance_provision: spot|on_demand|spot_fleet

**default:** spot

**type:** Literal['spot', 'on_demand', 'spot_fleet', 'spot_low_price']

**backend overrides:**
- `on_demand`: oci, k8s-gke, k8s-eks


## **instance_provision_fallback_on_demand** / SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND

instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected 'instance_provision' type creation failed. Expected values: true|false (default - false

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks


<<<<<<< HEAD
## **sizing_db** / SCT_SIZING_DB

Cloud-agnostic instance sizing constraints for db nodes

**default:** N/A

**type:** dict


## **sizing_db_oracle** / SCT_SIZING_DB_ORACLE

Cloud-agnostic instance sizing constraints for db_oracle nodes

**default:** {'vcpu': 8, 'memory': '>=60'}

**type:** dict


## **sizing_loader** / SCT_SIZING_LOADER

Cloud-agnostic instance sizing constraints for loader nodes

**default:** {'vcpu': 4, 'memory': '>=8'}

**type:** dict


## **sizing_monitor** / SCT_SIZING_MONITOR

Cloud-agnostic instance sizing constraints for monitor nodes

**default:** {'vcpu': 2, 'memory': '>=8'}

**type:** dict


## **instance_type_loader** / SCT_INSTANCE_TYPE_LOADER

AWS image type of the loader node

**default:** N/A

**type:** str (appendable)


## **instance_type_monitor** / SCT_INSTANCE_TYPE_MONITOR

AWS image type of the monitor node

**default:** N/A

**type:** str (appendable)


||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **sizing_db** / SCT_SIZING_DB

Cloud-agnostic instance sizing constraints for db nodes

**default:** N/A

**type:** dict


## **sizing_db_oracle** / SCT_SIZING_DB_ORACLE

Cloud-agnostic instance sizing constraints for db_oracle nodes

**default:** {'vcpu': 8, 'memory': '>=60'}

**type:** dict


## **sizing_loader** / SCT_SIZING_LOADER

Cloud-agnostic instance sizing constraints for loader nodes. Loaders default to Arm. A stress tool whose loader image is published for linux/amd64 only (cassandra-harry, hydra-kcl, ndbench, nosqlbench, and the alternator DNS sidecar used by YCSB when alternator_use_dns_routing is set) sets arch to x86_64 for you. Set arch here to pick the architecture yourself

**default:** {'vcpu': 4, 'memory': '>=8'}

**type:** dict


## **sizing_monitor** / SCT_SIZING_MONITOR

Cloud-agnostic instance sizing constraints for monitor nodes

**default:** {'vcpu': 2, 'memory': '>=8'}

**type:** dict


## **instance_type_loader** / SCT_INSTANCE_TYPE_LOADER

AWS image type of the loader node

**default:** N/A

**type:** str (appendable)


## **instance_type_monitor** / SCT_INSTANCE_TYPE_MONITOR

AWS image type of the monitor node

**default:** N/A

**type:** str (appendable)


=======
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **instance_type_db** / SCT_INSTANCE_TYPE_DB

AWS image type of the db node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `i4i.4xlarge`: k8s-eks


## **instance_type_db_oracle** / SCT_INSTANCE_TYPE_DB_ORACLE

AWS image type of the oracle node

**default:** N/A

**type:** str (appendable)


## **instance_type_db_target** / SCT_INSTANCE_TYPE_DB_TARGET

Target AWS instance type for platform migration (e.g., i8g.2xlarge for ARM)

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


## **instance_type_runner** / SCT_INSTANCE_TYPE_RUNNER

instance type of the sct-runner node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `c6i.2xlarge`: k8s-local-kind-aws
- `e2-standard-8`: k8s-local-kind-gce


<<<<<<< HEAD
## **region_name** / SCT_REGION_NAME

AWS regions to use

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eu-west-1']`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** False

**type:** bool


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


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


||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **region_name** / SCT_REGION_NAME

AWS regions to use

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eu-west-1']`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** False

**type:** bool


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/{arch}/hvm/ebs-gp3/ami-id`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


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


=======
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **instance_type_vector_store** / SCT_INSTANCE_TYPE_VECTOR_STORE

AWS/GCP cloud provider instance type for Vector Store nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `t4g.medium`: aws, aws-siren, k8s-local-kind-aws, k8s-eks
- `e2-medium`: gce, gce-siren, k8s-gke


## **intra_node_comm_public** / SCT_INTRA_NODE_COMM_PUBLIC

If True, all communication between nodes are via public addresses

**default:** N/A

**type:** bool


## **ip_ssh_connections** / SCT_IP_SSH_CONNECTIONS

Type of IP used to connect to machine instances.<br>This depends on whether you are running your tests from a machine inside<br>your cloud provider, where it makes sense to use 'private', or outside (use 'public')<br><br>Default: Use public IPs to connect to instances (public)<br>Use private IPs to connect to instances (private)<br>Use IPv6 IPs to connect to instances (ipv6)

**default:** private

**type:** Literal['public', 'private', 'ipv6']


## **keystore_backend** / SCT_KEYSTORE_BACKEND

Credential storage backend for KeyStore: 'secretsmanager' (default) or 's3' (legacy)

**default:** secretsmanager

**type:** Literal['s3', 'secretsmanager']


## **keystore_sm_prefix** / SCT_KEYSTORE_SM_PREFIX

AWS Secrets Manager secret name prefix when keystore_backend=secretsmanager (default: 'sct/')

**default:** sct/

**type:** str (appendable)


## **keystore_sm_region** / SCT_KEYSTORE_SM_REGION

AWS region holding the KeyStore secrets when keystore_backend=secretsmanager (default: 'us-east-1')

**default:** us-east-1

**type:** str (appendable)


## **latency_decorator_error_thresholds** / SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS

Error thresholds for latency decorator. Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}

**default:** {'write': {'default': {'P90 write': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}}}, 'read': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'read_disk_only': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'mixed': {'default': {'P90 write': {'fixed_limit': None}, 'P90 read': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}, 'P99 read': {'fixed_limit': 10}}}}

**type:** dict | YAML/JSON string → dict


## **n_db_nodes** / SCT_N_DB_NODES

Number list of database nodes in multiple data centers.

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]

**backend overrides:**
- `4`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks
- `3`: xcloud


## **n_db_zero_token_nodes** / SCT_N_DB_ZERO_TOKEN_NODES

Number of zero token nodes in cluster. Value should be set as '0 1 1' for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions

**default:** 0

**type:** int | list[int] | space-separated ints → list[int]


## **n_loaders** / SCT_N_LOADERS

Number list of loader nodes in multiple data centers

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]

**backend overrides:**
- `1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks, xcloud


## **n_monitor_nodes** / SCT_N_MONITOR_NODES

Number list of monitor nodes in multiple data centers

**default:** 1

**type:** int | list[int] | space-separated ints → list[int]

**backend overrides:**
- `0`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **parallel_node_operations** / SCT_PARALLEL_NODE_OPERATIONS

When defined true, will run node operations in parallel. Supported operations: startup

**default:** True

**type:** bool


## **pre_filter_unavailable_availability_zones** / SCT_PRE_FILTER_UNAVAILABLE_AVAILABILITY_ZONES

Filter availability zones upfront to only those that support all required instance types. Replaces invalid AZs with valid alternatives in the same region before any provisioning attempt. Supported backends: AWS, GCE.

**default:** True

**type:** bool


## **pre_flight_capacity_probe** / SCT_PRE_FLIGHT_CAPACITY_PROBE

Before provisioning, probe capacity by launching and terminating one on-demand instance per dynamic type (`instance_type_db_target`, `nemesis_grow_shrink_instance_type`) in the chosen AZ. On capacity errors, raise to trigger AZ/region fallback. Costs ~1 min per type. AWS-only.

**default:** False

**type:** bool


## **raid_level** / SCT_RAID_LEVEL

Number of of raid level: 0 - RAID0, 5 - RAID5

**default:** 0

**type:** int


## **region_name** / SCT_REGION_NAME

Cloud region(s) to run in. A space-separated list or YAML list provisions a multi-region cluster, one entry per datacenter. Despite the AWS-sounding default, this is the generic region option; GCE uses 'gce_datacenter' and Azure uses 'azure_region_name'.

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eu-west-1']`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **reuse_cluster** / SCT_REUSE_CLUSTER

If reuse_cluster is set it should hold test_id of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`

**default:** N/A

**type:** str (appendable)


## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **sct_public_ip** / SCT_SCT_PUBLIC_IP

Override the default hostname address of the sct test runner,<br>for the monitoring of the Nemesis.<br>can only work out of the box in AWS

**default:** N/A

**type:** str (appendable)


## **seeds_num** / SCT_SEEDS_NUM

Number of seeds to select

**default:** 1

**type:** int


## **seeds_selector** / SCT_SEEDS_SELECTOR

How to select the seeds. Expected values: random/first/all

**default:** all

**type:** Literal['random', 'first', 'all']


## **simulated_racks** / SCT_SIMULATED_RACKS

Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.<br>Provide number of racks to simulate. Takes effect only with more than one DB node: a<br>single-node cluster stays in one rack and `endpoint_snitch` is left alone. On the docker<br>backend the rack is passed to the image entrypoint as `--dc/--rack`, which requires Scylla<br>>= 2026.1; an older image fails the configuration, so set 1 to opt out.

**default:** 3

**type:** int

**backend overrides:**
- `0`: xcloud


## **simulated_regions** / SCT_SIMULATED_REGIONS

Number of simulated regions for the test

**default:** 0

**type:** Literal[0, 2, 3, 4, 5]


## **sizing_db** / SCT_SIZING_DB

Cloud-agnostic instance sizing constraints for db nodes

**default:** N/A

**type:** dict


## **sizing_db_oracle** / SCT_SIZING_DB_ORACLE

Cloud-agnostic instance sizing constraints for db_oracle nodes

**default:** {'vcpu': 8, 'memory': '>=60'}

**type:** dict


## **sizing_loader** / SCT_SIZING_LOADER

Cloud-agnostic instance sizing constraints for loader nodes

**default:** {'vcpu': 4, 'memory': '>=8'}

**type:** dict


## **sizing_monitor** / SCT_SIZING_MONITOR

Cloud-agnostic instance sizing constraints for monitor nodes

**default:** {'vcpu': 2, 'memory': '>=8'}

**type:** dict


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario

**default:** {}

**type:** dict | YAML/JSON string → dict


## **ssh_transport** / SCT_SSH_TRANSPORT

Set type of ssh library to use. Could be 'libssh2' (default) or 'fabric'

**default:** libssh2

**type:** Literal['libssh2', 'fabric']


## **test_duration** / SCT_TEST_DURATION

Test duration (min). Parameter used to keep instances produced by tests<br>and for jenkins pipeline timeout and TimoutThread.

**default:** 60

**type:** int


## **test_id** / SCT_TEST_ID

Set the test_id of the run manually. Use only from the env before running Hydra

**default:** N/A

**type:** str (appendable)


## **test_metadata** / SCT_TEST_METADATA

Structured metadata for test documentation and labeling. Validated by pydantic model. Flows to Argus.

**default:** N/A

**type:** sdcm.test_metadata.TestMetadata


## **test_method** / SCT_TEST_METHOD

class.method used to run the test. Filled automatically with run-test sct command.

**default:** N/A

**type:** str


## **use_dns_names** / SCT_USE_DNS_NAMES

Use dns names instead of ip addresses for nodes in cluster

**default:** False

**type:** bool


## **use_legacy_cluster_init** / SCT_USE_LEGACY_CLUSTER_INIT

Use legacy cluster initialization with autobootsrap disabled and parallel node setup

**default:** False

**type:** bool


## **use_zero_nodes** / SCT_USE_ZERO_NODES

If True, enable support in SCT of zero nodes (configuration, nemesis)

**default:** False

**type:** bool


## **user_credentials_path** / SCT_USER_CREDENTIALS_PATH

Path to the SSH private key SCT uses to reach the nodes it provisions. The QA key is fetched automatically from the KeyStore, so this rarely needs setting by hand.

**default:** N/A

**type:** str

**backend overrides:**
- `~/.ssh/scylla_test_id_ed25519`: aws, gce, azure, oci, docker, baremetal, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_prefix** / SCT_USER_PREFIX

the prefix of the name of the cloud instances, defaults to username

**default:** N/A

**type:** str (appendable)


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero-token DB nodes -- nodes that join the ring for reads/writes but own no token range. Falls back to 'instance_type_db' when unset.

**default:** N/A

**type:** str (appendable)


# Scylla installation and configuration


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)

**backend overrides:**
- `--abort-on-lsa-bad-alloc 1 --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** {'rf_rack_valid_keyspaces': True}

**type:** dict | str | pydantic.main.BaseModel


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal['PasswordAuthenticator', 'AllowAllAuthenticator', 'com.scylladb.auth.SaslauthdAuthenticator']


## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authorizer** / SCT_AUTHORIZER

which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer

**default:** N/A

**type:** Literal['AllowAllAuthorizer', 'CassandraAuthorizer']


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** False

**type:** bool


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** False

**type:** bool


## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.

**default:** True

**type:** bool


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** False

**type:** bool


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

Scylla experimental features to enable in scylla.yaml, as a list of feature names (e.g. 'udf', 'alternator-streams').

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla `internode_compression` in scylla.yaml: which inter-node traffic to compress -- 'all', 'dc' (between datacenters only) or 'none'.

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** str (appendable)


## **jmx_heap_memory** / SCT_JMX_HEAP_MEMORY

The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).

**default:** N/A

**type:** int


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws, gce, azure, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** str (appendable)


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


## **prepare_saslauthd** / SCT_PREPARE_SASLAUTHD

When defined true, will install and start saslauthd service

**default:** N/A

**type:** bool


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] → list[str] (appendable)


## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

Distro and family for the DB node image, e.g. 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str

**backend overrides:**
- `centos`: docker


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

Distro and family for the loader node image. Independent of the DB nodes, so loaders can run a different distro.

**default:** ubuntu-jammy

**type:** str


## **scylla_network_config** / SCT_SCYLLA_NETWORK_CONFIG

Configure Scylla networking with single or multiple NIC/IP combinations.<br>It must be defined for listen_address and rpc_address. For each address mandatory parameters are:<br>- address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication<br>- ip_type: ipv4 or ipv6<br>- public: false or true<br>- nic: number of NIC. 0, 1<br>Supported for AWS and GCE meanwhile

**default:** N/A

**type:** list

**backend overrides:**
- `[{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}]`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'

**default:** N/A

**type:** str

**backend overrides:**
- `6.2.3`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** False

**type:** bool


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** False

**type:** bool


## **service_level_shares** / SCT_SERVICE_LEVEL_SHARES

List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]

**default:** [1000]

**type:** list


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **use_ldap** / SCT_USE_LDAP

When defined true, LDAP is going to be used.

**default:** N/A

**type:** bool


## **use_ldap_authentication** / SCT_USE_LDAP_AUTHENTICATION

Authenticate Scylla users against LDAP: starts an LDAP container and sets scylla.yaml to use it for authentication (who you are).

**default:** N/A

**type:** bool


## **use_ldap_authorization** / SCT_USE_LDAP_AUTHORIZATION

Authorize Scylla users through LDAP group membership: starts an LDAP container and sets scylla.yaml to use it for authorization (what you may do).

**default:** N/A

**type:** bool


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

user-data format version to send to the DB node images. Defaults to whatever the image is tagged with; set it only to override that.

**default:** N/A

**type:** str


# Nemesis (chaos testing)


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Supported syntax:<br>- nemesis_class_name: "NemesisName"<br>Run one nemesis in a single thread.<br>- nemesis_class_name: ["NemesisA", "NemesisB"]<br>Run NemesisA and NemesisB each in their own thread.<br>- nemesis_class_name: ["SisyphusMonkey", "SisyphusMonkey"]<br>Run two SisyphusMonkey threads in parallel.<br>Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and<br>space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no<br>longer supported. Use an explicit YAML list instead.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** 0

**type:** int


## **nemesis_during_prepare** / SCT_NEMESIS_DURING_PREPARE

Run nemesis during prepare stage of the test

**default:** True

**type:** bool


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** False

**type:** bool


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** str (appendable)


## **nemesis_interval** / SCT_NEMESIS_INTERVAL

Nemesis sleep interval to use if None provided specifically in the test

**default:** 5

**type:** int


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 2

**type:** int


## **nemesis_seed** / SCT_NEMESIS_SEED

A seed number in order to repeat nemesis sequence as part of SisyphusMonkey

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **nemesis_sequence_sleep_between_ops** / SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS

Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests

**default:** N/A

**type:** int


## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** bool


# Stress commands and load generation


## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** False

**type:** bool


## **alternator_stress_rate** / SCT_ALTERNATOR_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing.

**default:** N/A

**type:** int


## **alternator_write_always_lwt_stress_rate** / SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.

**default:** N/A

**type:** int


## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** False

**type:** bool


## **batch_size** / SCT_BATCH_SIZE

Number of rows per batch for the stress commands that write in batches.

**default:** 1

**type:** int


## **c_s_driver_version** / SCT_C_S_DRIVER_VERSION

cassandra-stress driver version to use: 3|4|random

**default:** 3

**type:** Literal['3', '4', 'random']


## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** bool


## **cs_duration** / SCT_CS_DURATION

Duration passed to cassandra-stress, e.g. '50m'. Overrides any duration in the command itself.

**default:** 50m

**type:** str (appendable)


## **cs_extra_jvm_opts** / SCT_CS_EXTRA_JVM_OPTS

Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' (requires Java 21+, which cassandra-stress 3.20.6+ ships with).

**default:** N/A

**type:** str (appendable)


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** str (appendable)


## **cs_safepoint_logging** / SCT_CS_SAFEPOINT_LOGGING

Enable JVM safepoint logging (-Xlog:safepoint) for the cassandra-stress loaders. The log is written on the loader host, pulled into the loader log directory and collected into the run log archive. Use it to tell a loader JVM pause (including non-GC safepoints) apart from a server-side or network stall behind a latency-step failure. Not supported for k8s backends and prepared loaders.

**default:** False

**type:** bool


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **effective_compression_ratio** / SCT_EFFECTIVE_COMPRESSION_RATIO

Effective compression ratio used for Jinja stress command templating. Defined as on_disk_bytes / logical_uncompressed_bytes. This estimates how much disk space Scylla uses after compression relative to the logical uncompressed dataset size. For example, 1.0 means no effective compression and 0.68 means the data is expected to occupy about 68% of its logical uncompressed size on disk. Used together with the effective_disk_size_bytes template variable to calculate row counts that fill a target fraction of available disk capacity. You can estimate this ratio from Grafana in Keyspace -> Compression metrics; a compression value of 0% corresponds to effective_compression_ratio=1.0. Must be in range (0, 1.0].

**default:** 1.0

**type:** float


## **gemini_cmd** / SCT_GEMINI_CMD

gemini command to run (for now used only in GeminiTest)

**default:** N/A

**type:** str (appendable)


## **gemini_log_cql_statements** / SCT_GEMINI_LOG_CQL_STATEMENTS

Log CQL statements to file

**default:** N/A

**type:** bool


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.<br>Local files are uploaded to the loader via send_files and mounted into the Gemini Docker<br>container via --schema.<br>Remote URLs are downloaded on the loader node with curl and then mounted the same way.

**default:** N/A

**type:** str (appendable)


## **gemini_seed** / SCT_GEMINI_SEED

Seed number for gemini command

**default:** N/A

**type:** int


## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']

**default:** N/A

**type:** list


## **keyspace_num** / SCT_KEYSPACE_NUM

Number of keyspaces to use in the test

**default:** 1

**type:** int


## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.<br>Also used as a fallback source for keyspace/table in gradual performance tests when<br>perf_stress_keyspace/perf_stress_table are not set.<br>For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}

**default:** {}

**type:** dict | YAML/JSON string → dict


## **loader_swap_size** / SCT_LOADER_SWAP_SIZE

The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

Stress command(s) run in the prepare phase, alongside 'prepare_write_cmd'. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

Stress command(s) that verify the pre-loaded dataset before the test proper. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_wait_no_compactions_timeout** / SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT

Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load

**default:** N/A

**type:** int


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

Stress command(s) that pre-load the dataset before the test's own load starts. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **rack_aware_loader** / SCT_RACK_AWARE_LOADER

When enabled, loaders will look for nodes on the same rack.

**default:** False

**type:** bool


## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** False

**type:** bool


## **round_robin** / SCT_ROUND_ROBIN

Enable or disable round robin selection of nodes for operations

**default:** False

**type:** bool


## **stop_test_on_stress_failure** / SCT_STOP_TEST_ON_STRESS_FAILURE

If set to True the test will be stopped immediately when stress command failed.<br>When set to False the test will continue to run even when there are errors in the<br>stress process

**default:** True

**type:** bool


## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** False

**type:** bool


## **stress_before_migration** / SCT_STRESS_BEFORE_MIGRATION

Stress command to write data for post-migration validation

**default:** N/A

**type:** str (appendable)


## **stress_cdc_log_reader_batching_enable** / SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE

retrieving data from multiple streams in one poll

**default:** True

**type:** bool


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


## **stress_cmd** / SCT_STRESS_CMD

The test's main stress command(s). Everything except '-node' can be set; SCT fills in the node list. Accepts a single command or a list, one per loader thread.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARMUP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

Stress command(s) that delete rows in the complex-schema data validation flow. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_m** / SCT_STRESS_CMD_M

Mixed read/write stress command(s). See 'stress_cmd' for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

Stress command(s) for the leg of the test that runs with materialized views. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

Stress command(s) for the leg of the test that runs without materialized views, so the MV overhead can be compared. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE

cassandra-stress user profile (YAML) for the no-materialized-view leg of the test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_r** / SCT_STRESS_CMD_R

Read-only stress command(s). See 'stress_cmd' for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_disk** / SCT_STRESS_CMD_READ_DISK

Read stress command(s) sized to miss the cache and read from disk. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_w** / SCT_STRESS_CMD_W

Write-only stress command(s). See 'stress_cmd' for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from stress_cmd parameters<br>and is used in test duration calculation

**default:** 0

**type:** int


## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** {}

**type:** dict | YAML/JSON string → dict


## **stress_multiplier** / SCT_STRESS_MULTIPLIER

Multiplier for stress command intensity

**default:** 1

**type:** int


## **stress_multiplier_m** / SCT_STRESS_MULTIPLIER_M

Mixed operations stress command intensity multiplier

**default:** 1

**type:** int


## **stress_multiplier_r** / SCT_STRESS_MULTIPLIER_R

Multiplies the thread count of every read stress command, to scale read load without editing each command.

**default:** 1

**type:** int


## **stress_multiplier_w** / SCT_STRESS_MULTIPLIER_W

Multiplies the thread count of every write stress command, to scale write load without editing each command.

**default:** 1

**type:** int


## **stress_read_cmd** / SCT_STRESS_READ_CMD

Read stress command(s) run in the verification phase. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_template_context** / SCT_STRESS_TEMPLATE_CONTEXT

Shared runtime-only Jinja variables for stress command templating. Entries are resolved in declaration order and may reference earlier context entries as well as built-in stress template variables such as effective_disk_size_bytes and db_node_count_per_dc. These values are available to stress commands rendered by SCT, but are not evaluated during config load or validation.

**default:** {}

**type:** dict | YAML/JSON string → dict


## **use_prepared_loaders** / SCT_USE_PREPARED_LOADERS

If True, we use prepared VMs for loader (instead of using docker images)

**default:** N/A

**type:** bool


## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

Number of user profile tables to create for the test

**default:** 1

**type:** int


# Monitoring, events and reporting


## **argus_email_report_template** / SCT_ARGUS_EMAIL_REPORT_TEMPLATE

Path to the email report template used for sending argus email reports

**default:** email_report_template_basic.yaml

**type:** str (appendable)


## **argus_use_ssh_tunnel** / SCT_ARGUS_USE_SSH_TUNNEL

Enable SSH tunnel support in the Argus client connection

**default:** True

**type:** bool


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** bool

**backend overrides:**
- `False`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **backtrace_decoding_disable_regex** / SCT_BACKTRACE_DECODING_DISABLE_REGEX

Regex pattern to disable backtrace decoding for specific event types. If an event type matches<br>this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests<br>by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.

**default:** N/A

**type:** str (appendable)


## **backtrace_stall_decoding** / SCT_BACKTRACE_STALL_DECODING

If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during<br>backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.

**default:** True

**type:** bool


## **download_from_s3** / SCT_DOWNLOAD_FROM_S3

Destination-source map of dirs/buckets to download from S3 before starting the test

**default:** []

**type:** list


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str | list[str] → list[str] (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Text appended to the subject of the test result email, to tell similar runs apart.

**default:** N/A

**type:** str (appendable)


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** bool


## **enable_kernel_panic_checker** / SCT_ENABLE_KERNEL_PANIC_CHECKER

Enable kernel panic detection by monitoring cloud instance console output for panic indicators. When enabled, a background thread monitors each node's console output for kernel panic patterns.

**default:** True

**type:** bool


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Maximum number of events of each severity to include in the email report.

**default:** 10

**type:** int


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.16

**type:** str (appendable)

**backend overrides:**
- `N/A`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **monitor_swap_size** / SCT_MONITOR_SWAP_SIZE

The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** bool

**backend overrides:**
- `False`: docker


## **sct_ngrok_name** / SCT_SCT_NGROK_NAME

DEPRECATED (see SCT-954, unused for years): expose the SCT runner under this ngrok hostname instead of its own address.

**default:** N/A

**type:** str (appendable)


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** False

**type:** bool


# Logs, diagnostics and teardown


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** False

**type:** bool


## **collect_nvme_diagnostics** / SCT_COLLECT_NVME_DIAGNOSTICS

Collect NVMe SMART logs, error logs, and self-test results from DB nodes during test teardown. Requires nvme-cli to be installed on the nodes. Skipped gracefully on backends without NVMe devices.

**default:** False

**type:** bool


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** False

**type:** bool


## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** vector

**type:** Literal['ssh', 'docker', 'syslog-ng', 'vector']

**backend overrides:**
- `docker`: docker


## **nvme_self_test_type** / SCT_NVME_SELF_TEST_TYPE

NVMe device self-test type to run: 1 (short, ~2 min) or 2 (extended, may take hours). Only used when collect_nvme_diagnostics is enabled.

**default:** 1

**type:** int


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_dedicated_host** / SCT_POST_BEHAVIOR_DEDICATED_HOST

Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.<br><br>'destroy' - Destroy hosts (default)<br>'keep' - Keep hosts allocated

**default:** N/A

**type:** Literal['keep', 'destroy']

**backend overrides:**
- `destroy`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **post_behavior_emr_cluster** / SCT_POST_BEHAVIOR_EMR_CLUSTER

Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.<br><br>'destroy' - Destroy EMR cluster (default)<br>'keep' - Keep EMR cluster running<br>'keep-on-failure' - Keep EMR cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_vector_store_nodes** / SCT_POST_BEHAVIOR_VECTOR_STORE_NODES

Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


## **run_scylla_doctor_only** / SCT_RUN_SCYLLA_DOCTOR_ONLY

When true, the artifact test runs only the Scylla Doctor validation<br>(install, collect vitals, analyze, verify) and skips all other artifact checks<br>such as stop/start, cassandra-stress, etc. Useful for fast SD<br>release gating. Implies run_scylla_doctor=true.

**default:** False

**type:** bool


## **scylla_doctor_edition** / SCT_SCYLLA_DOCTOR_EDITION

Scylla Doctor edition to use. Allowed values: 'basic', 'full'.<br>'basic' fetches the free/open-source edition via HTTP.<br>'full' fetches the full/enterprise edition from a private S3 bucket.

**default:** basic

**type:** Literal['basic', 'full']


## **scylla_doctor_full_tarball_url** / SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL

Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the<br>standard version-based S3 lookup and downloads SD directly from this URL.<br>Use for testing unofficial or pre-release SD versions.<br>Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'

**default:** N/A

**type:** str (appendable)


## **scylla_doctor_version** / SCT_SCYLLA_DOCTOR_VERSION

Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')<br>to hardcode the version, or leave empty to use the latest available version. For stability,<br>artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.

**default:** 1.13

**type:** str (appendable)


## **teardown_validators** / SCT_TEARDOWN_VALIDATORS

Validators to use during teardown phase

**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}, 'nvme': {'enabled': False}}

**type:** dict | YAML/JSON string → dict


## **use_scylla_doctor_on_failure** / SCT_USE_SCYLLA_DOCTOR_ON_FAILURE

Run scylla-doctor on test failure to collect additional diagnostics

**default:** True

**type:** bool


# Scylla Manager


## **backup_bucket_backend** / SCT_BACKUP_BUCKET_BACKEND

the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `s3`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-gke, k8s-eks
- `gcs`: gce, gce-siren
- `azure`: azure


## **backup_bucket_location** / SCT_BACKUP_BUCKET_LOCATION

the bucket name to be used for backup (e.g., 'manager-backup-tests')

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `manager-backup-tests-{region}`: aws, aws-siren, k8s-eks
- `manager-backup-tests-sct-project-1-us-east1`: gce, gce-siren
- `manager-backup-tests-us-east-1`: azure
- `minio-bucket`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke


## **backup_bucket_region** / SCT_BACKUP_BUCKET_REGION

the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')

**default:** N/A

**type:** str (appendable)


## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


## **manager_prometheus_port** / SCT_MANAGER_PROMETHEUS_PORT

Port to be used by the manager to contact Prometheus

**default:** 5090

**type:** int


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Version of ScyllaDB to install as Manager backend

**default:** 2025.4

**type:** str


## **manager_version** / SCT_MANAGER_VERSION

Version of Scylla Manager server and agent to install

**default:** 3.12

**type:** str


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.12.0

**type:** str (appendable)


## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** str (appendable)


## **mgmt_prepare_snapshot_size** / SCT_MGMT_PREPARE_SNAPSHOT_SIZE

Size of backup snapshot in Gb to be prepared for backup

**default:** N/A

**type:** int


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


## **mgmt_snapshots_preparer_params** / SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS

Custom parameters of c-s write operation used in snapshots preparer

**default:** {'cs_cmd_template': "cassandra-stress {operation} cl={cl} n={num_of_rows} -schema 'keyspace={ks_name} replication(strategy={replication},replication_factor={rf}) compaction(strategy={compaction})' -mode cql3 native -rate threads={threads_num} -col 'size=FIXED({col_size}) n=FIXED({col_n})' -pop seq={sequence_start}..{sequence_end}", 'operation': 'write', 'cl': 'QUORUM', 'replication': 'NetworkTopologyStrategy', 'rf': 3, 'compaction': 'IncrementalCompactionStrategy', 'threads_num': 500, 'col_size': 1024, 'col_n': 1, 'ks_name': '', 'num_of_rows': '', 'sequence_start': '', 'sequence_end': ''}

**type:** dict | YAML/JSON string → dict


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


<<<<<<< HEAD
## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR
||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **gce_image_db_oracle** / SCT_GCE_IMAGE_DB_ORACLE

GCE image to use for oracle (2nd ref cluster) DB node(s). If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR
=======
## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION

Version of Scylla Manager agent to install for management tests

**default:** 3.12.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)

<<<<<<< HEAD
**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2604-lts-amd64`: gce, gce-siren, k8s-gke
||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2604-lts-{arch}`: gce, gce-siren, k8s-gke
=======

## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Version of Scylla Manager server and agent to upgrade to

**default:** N/A

**type:** str
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)


## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** str (appendable)


## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** str (appendable)


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** False

**type:** bool


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool


# Auxiliary DB cluster (oracle / Cassandra)


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

<<<<<<< HEAD
**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}}
||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}, 'nvme': {'enabled': False}}
=======
**default:** --enable-cache false

**type:** str (appendable)


## **cassandra_broadcast_rpc_public** / SCT_CASSANDRA_BROADCAST_RPC_PUBLIC

When True, set broadcast_rpc_address to the public IP of the node in cassandra.yaml, so clients outside the VPC (e.g. sct-runner driver connection that reads system.peers) can reach the nodes. Defaults to False (private IP, matches intra-VPC behavior).

**default:** N/A

**type:** bool


## **cassandra_num_tokens** / SCT_CASSANDRA_NUM_TOKENS

num_tokens value to configure in cassandra.yaml.

**default:** 16

**type:** int


## **cassandra_oracle_version** / SCT_CASSANDRA_ORACLE_VERSION

Cassandra version for the oracle cluster, i.e. '4.1' or '5.0'

**default:** N/A

**type:** str (appendable)


## **cassandra_version** / SCT_CASSANDRA_VERSION

Cassandra version / docker image tag, i.e. '4.1' or '5.0'

**default:** 4.1

**type:** str (appendable)


## **docker_image_cassandra** / SCT_DOCKER_IMAGE_CASSANDRA

Cassandra docker image repo, i.e. 'cassandra'. Used when db_type is 'cassandra'.

**default:** cassandra

**type:** str (appendable)


## **install_cassandra_exporter** / SCT_INSTALL_CASSANDRA_EXPORTER

Install Criteo cassandra_exporter on Cassandra nodes for Prometheus metrics collection. The exporter connects to JMX (port 7199) and exposes metrics on port 8080.

**default:** True

**type:** bool


## **n_test_oracle_db_nodes** / SCT_N_TEST_ORACLE_DB_NODES

Number list of oracle test nodes in multiple data centers.

**default:** 1

**type:** int | list[int] | space-separated ints → list[int]


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically looks up cloud images for formal versions.<br>WARNING: can't be used together with the backend's oracle image param<br>('ami_id_db_oracle', 'gce_image_db_oracle', 'azure_image_db_oracle' or 'oci_image_db_oracle')

**default:** 2026.1

**type:** str


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Same as 'user_data_format_version', but for the auxiliary oracle cluster's images.

**default:** N/A

**type:** str


# Alternator (DynamoDB API)


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** False

**type:** bool


## **alternator_loadbalancing** / SCT_ALTERNATOR_LOADBALANCING

If true, enable native load balancing for alternator

**default:** False

**type:** bool


## **alternator_port** / SCT_ALTERNATOR_PORT

Port to configure for alternator in scylla.yaml

**default:** N/A

**type:** int


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_test_table** / SCT_ALTERNATOR_TEST_TABLE

Dictionary of a test alternator table features:<br>name: str - the name of the table<br>lsi_name: str - the name of the local secondary index to create with a table<br>gsi_name: str - the name of the global secondary index to create with a table<br>tags: dict - the tags to apply to the created table<br>items: int - expected number of items in the table after prepare

**default:** N/A
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)

**type:** dict | YAML/JSON string → dict


## **alternator_trust_all_certificates** / SCT_ALTERNATOR_TRUST_ALL_CERTIFICATES

If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)

**default:** True

**type:** bool


## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** True

**type:** bool


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


## **dynamodb_primarykey_type** / SCT_DYNAMODB_PRIMARYKEY_TYPE

Type of dynamodb table to create with range key or not

**default:** HASH

**type:** Literal['HASH', 'HASH_AND_RANGE']


# Vector Store


## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** 0

**type:** int


## **vector_store_docker_image** / SCT_VECTOR_STORE_DOCKER_IMAGE

Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version

**default:** scylladb/vector-store

**type:** str (appendable)


## **vector_store_port** / SCT_VECTOR_STORE_PORT

TCP port the Vector Store service listens on for its API.

**default:** 6080

**type:** int


## **vector_store_scylla_port** / SCT_VECTOR_STORE_SCYLLA_PORT

ScyllaDB connection port for Vector Store

**default:** 9042

**type:** int


## **vector_store_threads** / SCT_VECTOR_STORE_THREADS

Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)

**default:** 0

**type:** int


## **vector_store_version** / SCT_VECTOR_STORE_VERSION

Vector Store version / docker image tag

**default:** N/A

**type:** str (appendable)


# Kafka / CDC connectors


## **kafka_backend** / SCT_KAFKA_BACKEND

Type of Kafka backend to use

**default:** N/A

**type:** Literal['localstack', 'vm', 'msk']


## **kafka_connectors** / SCT_KAFKA_CONNECTORS

Kafka Connect connector definitions to deploy, as a list of config dicts -- typically the Scylla CDC source connector.

**default:** []

**type:** list[sdcm.kafka.kafka_config.SctKafkaConfiguration]


# AWS backend


## **ami_db_cassandra_user** / SCT_AMI_DB_CASSANDRA_USER

SSH login user baked into the Cassandra AMI, for the auxiliary cluster.

**default:** N/A

**type:** str (appendable)


## **ami_db_scylla_user** / SCT_AMI_DB_SCYLLA_USER

SSH login user baked into the DB node AMI (e.g. 'centos', 'ubuntu').

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_db_cassandra** / SCT_AMI_ID_DB_CASSANDRA

AMS AMI id to use for cassandra node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_oracle** / SCT_AMI_ID_DB_ORACLE

AMS AMI id to use for oracle node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_vector_store** / SCT_AMI_ID_VECTOR_STORE

AMS AMI id to use for vector store node

**default:** N/A

**type:** str (appendable)


## **ami_loader_user** / SCT_AMI_LOADER_USER

SSH login user baked into the loader AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_monitor_user** / SCT_AMI_MONITOR_USER

SSH login user baked into the monitoring node AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_vector_store_user** / SCT_AMI_VECTOR_STORE_USER

SSH login user baked into the Vector Store AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `[]`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Deprecated alias of `fallback_to_next_availability_zone`. Kept for backward compatibility.

**default:** False

**type:** bool


## **aws_instance_profile_name_db** / SCT_AWS_INSTANCE_PROFILE_NAME_DB

This is the name of the instance profile to set on all db instances

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-scylla-manager-backup-instance-profile`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_instance_profile_name_loader** / SCT_AWS_INSTANCE_PROFILE_NAME_LOADER

This is the name of the instance profile to set on all loader instances

**default:** N/A

**type:** str (appendable)


## **extra_network_interface** / SCT_EXTRA_NETWORK_INTERFACE

if true, create extra network interface on each node

**default:** N/A

**type:** bool


## **root_disk_size_db** / SCT_ROOT_DISK_SIZE_DB

Root (boot) disk size in GB for the DB nodes.

**default:** N/A

**type:** int

**backend overrides:**
- `30`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks
- `50`: gce, gce-siren, k8s-gke


## **root_disk_size_loader** / SCT_ROOT_DISK_SIZE_LOADER

Root (boot) disk size in GB for the loader nodes.

**default:** N/A

**type:** int

**backend overrides:**
- `20`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **root_disk_size_monitor** / SCT_ROOT_DISK_SIZE_MONITOR

Root (boot) disk size in GB for the monitoring node.

**default:** N/A

**type:** int

**backend overrides:**
- `50`: aws, gce, azure, oci, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **root_disk_size_runner** / SCT_ROOT_DISK_SIZE_RUNNER

root disk size in Gb for sct-runner

**default:** N/A

**type:** int

**backend overrides:**
- `140`: k8s-local-kind-aws, k8s-local-kind-gce


## **spot_max_price** / SCT_SPOT_MAX_PRICE

The max percentage of the on demand price we set for spot/fleet instances

**default:** N/A

**type:** float


## **use_capacity_reservation** / SCT_USE_CAPACITY_RESERVATION

Flag to use capacity reservation for instances

**default:** False

**type:** bool


## **use_dedicated_host** / SCT_USE_DEDICATED_HOST

Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)

**default:** False

**type:** bool


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** False

**type:** bool


<<<<<<< HEAD
## **emr_release_label** / SCT_EMR_RELEASE_LABEL

EMR release version (e.g., 'emr-7.8.0'). When set, an EMR cluster is provisioned alongside the Scylla cluster.

**default:** N/A

**type:** str (appendable)


## **emr_instance_type_master** / SCT_EMR_INSTANCE_TYPE_MASTER

Instance type for EMR master node (e.g., 'm5.xlarge')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_core** / SCT_EMR_INSTANCE_TYPE_CORE

Instance type for EMR core nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_count_core** / SCT_EMR_INSTANCE_COUNT_CORE

Number of EMR core nodes

**default:** N/A

**type:** int

**backend overrides:**
- `2`: aws


## **emr_instance_type_task** / SCT_EMR_INSTANCE_TYPE_TASK

Instance type for EMR task nodes (optional, uses Spot instances)

**default:** N/A

**type:** str (appendable)


## **emr_instance_count_task** / SCT_EMR_INSTANCE_COUNT_TASK

Number of EMR task nodes

**default:** N/A

**type:** int

**backend overrides:**
- `0`: aws


## **emr_spot_bid_percentage** / SCT_EMR_SPOT_BID_PERCENTAGE

Max Spot price as percentage of On-Demand for EMR task nodes (default: 100)

**default:** N/A

**type:** int

**backend overrides:**
- `100`: aws


## **emr_applications** / SCT_EMR_APPLICATIONS

List of EMR applications to install (default: ['Spark'])

**default:** N/A

**type:** list

**backend overrides:**
- `['Spark']`: aws


## **emr_spark_migrator_jar_path** / SCT_EMR_SPARK_MIGRATOR_JAR_PATH

S3 path or local path to the spark-migrator JAR file

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_release** / SCT_EMR_SPARK_MIGRATOR_RELEASE

scylla-migrator release tag (e.g., 'v1.1.2'). When set, JAR is auto-downloaded from GitHub releases and uploaded to S3. Takes precedence over emr_spark_migrator_jar_path.

**default:** N/A

**type:** str (appendable)


## **emr_log_uri** / SCT_EMR_LOG_URI

S3 URI for EMR cluster logs (e.g., 's3://sct-emr-spark-migrator-{region}/logs/')

**default:** N/A

**type:** str (appendable)


## **emr_keep_alive** / SCT_EMR_KEEP_ALIVE

Whether EMR cluster stays alive after job completion (default: true for reuse during testing)

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws


## **emr_install_spark4_via_bootstrap** / SCT_EMR_INSTALL_SPARK4_VIA_BOOTSTRAP

Legacy fallback: install Spark 4.x via an EMR bootstrap action and submit the migrator through script-runner.jar (for emr-7.x releases). Default value is false - i.e. deployment of native Spark on an `emr-spark-8.x` release label.

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: aws


## **migrator_source_hosts** / SCT_MIGRATOR_SOURCE_HOSTS

CQL contact-point IPs for the source Cassandra/Scylla cluster. Mutually exclusive with migrator_source_test_id.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **migrator_source_keyspace** / SCT_MIGRATOR_SOURCE_KEYSPACE

Keyspace to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_table** / SCT_MIGRATOR_SOURCE_TABLE

Table to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_test_id** / SCT_MIGRATOR_SOURCE_TEST_ID

SCT test_id of a running source cluster. When set, source host IPs are auto-discovered via EC2 tags (NodeType=cs-db). Mutually exclusive with migrator_source_hosts.

**default:** N/A

**type:** str (appendable)


## **migrator_target_keyspace** / SCT_MIGRATOR_TARGET_KEYSPACE

Keyspace to migrate into on the target Scylla cluster. Defaults to migrator_source_keyspace.

**default:** N/A

**type:** str (appendable)


## **migrator_target_table** / SCT_MIGRATOR_TARGET_TABLE

Table to migrate into on the target Scylla cluster. Defaults to migrator_source_table.

**default:** N/A

**type:** str (appendable)


## **migrator_run_validator** / SCT_MIGRATOR_RUN_VALIDATOR

Run the spark-migrator validator after migration to do a row-by-row comparison

**default:** N/A

**type:** bool


## **migrator_step_timeout_minutes** / SCT_MIGRATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator migration EMR step. Default 360.

**default:** N/A

**type:** int

**backend overrides:**
- `360`: aws


## **validator_step_timeout_minutes** / SCT_VALIDATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator validator EMR step. Default 60.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


## **scylla_doctor_version** / SCT_SCYLLA_DOCTOR_VERSION

Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')<br>to hardcode the version, or leave empty to use the latest available version. For stability,<br>artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.

**default:** 1.12

**type:** str (appendable)


## **scylla_doctor_full_tarball_url** / SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL

Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the<br>standard version-based S3 lookup and downloads SD directly from this URL.<br>Use for testing unofficial or pre-release SD versions.<br>Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'

**default:** N/A

**type:** str (appendable)


## **run_scylla_doctor_only** / SCT_RUN_SCYLLA_DOCTOR_ONLY

When true, the artifact test runs only the Scylla Doctor validation<br>(install, collect vitals, analyze, verify) and skips all other artifact checks<br>such as stop/start, cassandra-stress, housekeeping, etc. Useful for fast SD<br>release gating. Implies run_scylla_doctor=true.

**default:** False

**type:** bool


## **scylla_doctor_edition** / SCT_SCYLLA_DOCTOR_EDITION

Scylla Doctor edition to use. Allowed values: 'basic', 'full'.<br>'basic' fetches the free/open-source edition via HTTP.<br>'full' fetches the full/enterprise edition from a private S3 bucket.

**default:** basic

**type:** Literal['basic', 'full']


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario

**default:** {}

**type:** dict | YAML/JSON string → dict


## **use_zero_nodes** / SCT_USE_ZERO_NODES

If True, enable support in SCT of zero nodes (configuration, nemesis)

**default:** False

**type:** bool


## **n_db_zero_token_nodes** / SCT_N_DB_ZERO_TOKEN_NODES

Number of zero token nodes in cluster. Value should be set as '0 1 1' for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions

**default:** 0

**type:** int | list[int] | space-separated ints → list[int]


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero token node

**default:** N/A

**type:** str (appendable)


## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **latency_decorator_error_thresholds** / SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS

Error thresholds for latency decorator. Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}

**default:** {'write': {'default': {'P90 write': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}}}, 'read': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'read_disk_only': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'mixed': {'default': {'P90 write': {'fixed_limit': None}, 'P90 read': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}, 'P99 read': {'fixed_limit': 10}}}}

**type:** dict | YAML/JSON string → dict


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)


## **adaptive_timeout_store_metrics** / SCT_ADAPTIVE_TIMEOUT_STORE_METRICS

Store adaptive timeout metrics in Argus. Disabled for performance tests only.

**default:** True

**type:** bool


## **adaptive_timeout_multipliers** / SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS

Optional dict of adaptive-timeout multipliers keyed by operation name (from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). If the current operation key is absent, multiplier 1.0 is used.<br>YAML example:<br>adaptive_timeout_multipliers:<br>  decommission: 4<br>  new_node: 2<br>Environment variable examples:<br>SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS="{'decommission': 4, 'new_node': 2}"<br>Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4

**default:** {}

**type:** sdcm.sct_config.AdaptiveTimeoutMultipliers
||||||| parent of 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)
## **emr_release_label** / SCT_EMR_RELEASE_LABEL

EMR release version (e.g., 'emr-7.8.0'). When set, an EMR cluster is provisioned alongside the Scylla cluster.

**default:** N/A

**type:** str (appendable)


## **emr_instance_type_master** / SCT_EMR_INSTANCE_TYPE_MASTER

Instance type for EMR master node (e.g., 'm5.xlarge')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_core** / SCT_EMR_INSTANCE_TYPE_CORE

Instance type for EMR core nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_count_core** / SCT_EMR_INSTANCE_COUNT_CORE

Number of EMR core nodes

**default:** N/A

**type:** int

**backend overrides:**
- `2`: aws


## **emr_instance_type_task** / SCT_EMR_INSTANCE_TYPE_TASK

Instance type for EMR task nodes (optional, uses Spot instances)

**default:** N/A

**type:** str (appendable)


## **emr_instance_count_task** / SCT_EMR_INSTANCE_COUNT_TASK

Number of EMR task nodes

**default:** N/A

**type:** int

**backend overrides:**
- `0`: aws


## **emr_spot_bid_percentage** / SCT_EMR_SPOT_BID_PERCENTAGE

Max Spot price as percentage of On-Demand for EMR task nodes (default: 100)

**default:** N/A

**type:** int

**backend overrides:**
- `100`: aws


## **emr_applications** / SCT_EMR_APPLICATIONS

List of EMR applications to install (default: ['Spark'])

**default:** N/A

**type:** list

**backend overrides:**
- `['Spark']`: aws


## **emr_spark_migrator_jar_path** / SCT_EMR_SPARK_MIGRATOR_JAR_PATH

S3 path or local path to the spark-migrator JAR file

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_release** / SCT_EMR_SPARK_MIGRATOR_RELEASE

scylla-migrator release tag (e.g., 'v1.1.2'). When set, JAR is auto-downloaded from GitHub releases and uploaded to S3. Takes precedence over emr_spark_migrator_jar_path.

**default:** N/A

**type:** str (appendable)


## **emr_log_uri** / SCT_EMR_LOG_URI

S3 URI for EMR cluster logs (e.g., 's3://sct-emr-spark-migrator-{region}/logs/')

**default:** N/A

**type:** str (appendable)


## **emr_keep_alive** / SCT_EMR_KEEP_ALIVE

Whether EMR cluster stays alive after job completion (default: true for reuse during testing)

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws


## **emr_install_spark4_via_bootstrap** / SCT_EMR_INSTALL_SPARK4_VIA_BOOTSTRAP

Legacy fallback: install Spark 4.x via an EMR bootstrap action and submit the migrator through script-runner.jar (for emr-7.x releases). Default value is false - i.e. deployment of native Spark on an `emr-spark-8.x` release label.

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: aws


## **migrator_source_hosts** / SCT_MIGRATOR_SOURCE_HOSTS

CQL contact-point IPs for the source Cassandra/Scylla cluster. Mutually exclusive with migrator_source_test_id.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **migrator_source_keyspace** / SCT_MIGRATOR_SOURCE_KEYSPACE

Keyspace to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_table** / SCT_MIGRATOR_SOURCE_TABLE

Table to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_test_id** / SCT_MIGRATOR_SOURCE_TEST_ID

SCT test_id of a running source cluster. When set, source host IPs are auto-discovered via EC2 tags (NodeType=cs-db). Mutually exclusive with migrator_source_hosts.

**default:** N/A

**type:** str (appendable)


## **migrator_target_keyspace** / SCT_MIGRATOR_TARGET_KEYSPACE

Keyspace to migrate into on the target Scylla cluster. Defaults to migrator_source_keyspace.

**default:** N/A

**type:** str (appendable)


## **migrator_target_table** / SCT_MIGRATOR_TARGET_TABLE

Table to migrate into on the target Scylla cluster. Defaults to migrator_source_table.

**default:** N/A

**type:** str (appendable)


## **migrator_run_validator** / SCT_MIGRATOR_RUN_VALIDATOR

Run the spark-migrator validator after migration to do a row-by-row comparison

**default:** N/A

**type:** bool


## **migrator_step_timeout_minutes** / SCT_MIGRATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator migration EMR step. Default 360.

**default:** N/A

**type:** int

**backend overrides:**
- `360`: aws


## **validator_step_timeout_minutes** / SCT_VALIDATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator validator EMR step. Default 60.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


## **scylla_doctor_version** / SCT_SCYLLA_DOCTOR_VERSION

Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')<br>to hardcode the version, or leave empty to use the latest available version. For stability,<br>artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.

**default:** 1.13

**type:** str (appendable)


## **scylla_doctor_full_tarball_url** / SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL

Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the<br>standard version-based S3 lookup and downloads SD directly from this URL.<br>Use for testing unofficial or pre-release SD versions.<br>Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'

**default:** N/A

**type:** str (appendable)


## **run_scylla_doctor_only** / SCT_RUN_SCYLLA_DOCTOR_ONLY

When true, the artifact test runs only the Scylla Doctor validation<br>(install, collect vitals, analyze, verify) and skips all other artifact checks<br>such as stop/start, cassandra-stress, etc. Useful for fast SD<br>release gating. Implies run_scylla_doctor=true.

**default:** False

**type:** bool


## **scylla_doctor_edition** / SCT_SCYLLA_DOCTOR_EDITION

Scylla Doctor edition to use. Allowed values: 'basic', 'full'.<br>'basic' fetches the free/open-source edition via HTTP.<br>'full' fetches the full/enterprise edition from a private S3 bucket.

**default:** basic

**type:** Literal['basic', 'full']


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario

**default:** {}

**type:** dict | YAML/JSON string → dict


## **use_zero_nodes** / SCT_USE_ZERO_NODES

If True, enable support in SCT of zero nodes (configuration, nemesis)

**default:** False

**type:** bool


## **n_db_zero_token_nodes** / SCT_N_DB_ZERO_TOKEN_NODES

Number of zero token nodes in cluster. Value should be set as '0 1 1' for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions

**default:** 0

**type:** int | list[int] | space-separated ints → list[int]


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero token node

**default:** N/A

**type:** str (appendable)


## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **latency_decorator_error_thresholds** / SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS

Error thresholds for latency decorator. Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}

**default:** {'write': {'default': {'P90 write': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}}}, 'read': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'read_disk_only': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'mixed': {'default': {'P90 write': {'fixed_limit': None}, 'P90 read': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}, 'P99 read': {'fixed_limit': 10}}}}

**type:** dict | YAML/JSON string → dict


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)


## **adaptive_timeout_store_metrics** / SCT_ADAPTIVE_TIMEOUT_STORE_METRICS

Store adaptive timeout metrics in Argus. Disabled for performance tests only.

**default:** True

**type:** bool


## **adaptive_timeout_multipliers** / SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS

Optional dict of adaptive-timeout multipliers keyed by operation name (from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). If the current operation key is absent, multiplier 1.0 is used.<br>YAML example:<br>adaptive_timeout_multipliers:<br>  decommission: 4<br>  new_node: 2<br>Environment variable examples:<br>SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS="{'decommission': 4, 'new_node': 2}"<br>Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4

**default:** {}

**type:** sdcm.sct_config.AdaptiveTimeoutMultipliers
=======
# GCE backend
>>>>>>> 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)


## **gce_datacenter** / SCT_GCE_DATACENTER

Supported regions: us-east1, us-east4, us-west1, us-central1. Specifying just the region (e.g., us-east1) means the zone will be selected automatically, or you can mention the zone explicitly (e.g., us-east1-b)

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `us-east1`: gce, gce-siren, k8s-gke


## **gce_image_db** / SCT_GCE_IMAGE_DB

gce image to use for db nodes

**default:** N/A

**type:** str (appendable)


## **gce_image_db_oracle** / SCT_GCE_IMAGE_DB_ORACLE

GCE image to use for oracle (2nd ref cluster) DB node(s). If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **gce_image_loader** / SCT_GCE_IMAGE_LOADER

Google Compute Engine image to use for loader nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2604-lts-amd64`: gce, gce-siren, k8s-gke


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR

gce image to use for monitor nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: gce, gce-siren, k8s-gke


## **gce_image_username** / SCT_GCE_IMAGE_USERNAME

Username for the Google Compute Engine image

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylla-test`: gce, gce-siren, k8s-gke


## **gce_instance_type_db** / SCT_GCE_INSTANCE_TYPE_DB

Instance type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `n2-standard-8`: k8s-gke


<<<<<<< HEAD
## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB
||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **gce_instance_type_db_oracle** / SCT_GCE_INSTANCE_TYPE_DB_ORACLE

Instance type for the oracle (2nd ref cluster) DB nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB
=======
## **gce_instance_type_db_oracle** / SCT_GCE_INSTANCE_TYPE_DB_ORACLE

Instance type for the oracle (2nd ref cluster) DB nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_instance_type_loader** / SCT_GCE_INSTANCE_TYPE_LOADER
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)

Instance type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `e2-standard-4`: k8s-gke


## **gce_instance_type_monitor** / SCT_GCE_INSTANCE_TYPE_MONITOR

Instance type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `e2-medium`: k8s-gke


## **gce_n_local_ssd_disk_db** / SCT_GCE_N_LOCAL_SSD_DISK_DB

Number of local SSD disks for database nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `4`: gce, gce-siren, k8s-gke


## **gce_n_local_ssd_disk_loader** / SCT_GCE_N_LOCAL_SSD_DISK_LOADER

Number of local SSD disks for loader nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_n_local_ssd_disk_monitor** / SCT_GCE_N_LOCAL_SSD_DISK_MONITOR

Number of local SSD disks for monitor nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_network** / SCT_GCE_NETWORK

GCP VPC network the instances are attached to.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-vpc`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_db** / SCT_GCE_PD_SSD_DISK_SIZE_DB

Size in GB of the persistent SSD disk attached to each DB node.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_loader** / SCT_GCE_PD_SSD_DISK_SIZE_LOADER

Size in GB of the persistent SSD disk attached to each loader.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_monitor** / SCT_GCE_PD_SSD_DISK_SIZE_MONITOR

Size in GB of the persistent SSD disk attached to the monitoring node.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_standard_disk_size_db** / SCT_GCE_PD_STANDARD_DISK_SIZE_DB

The size of the standard persistent disk in GB used for GCE database nodes

**default:** 0

**type:** int


## **gce_project** / SCT_GCE_PROJECT

GCP project that owns the provisioned resources.

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB

Root disk type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-ssd`: gce, gce-siren, k8s-gke


## **gce_root_disk_type_loader** / SCT_GCE_ROOT_DISK_TYPE_LOADER

Root disk type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-standard`: gce, gce-siren, k8s-gke


## **gce_root_disk_type_monitor** / SCT_GCE_ROOT_DISK_TYPE_MONITOR

Root disk type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-standard`: gce, gce-siren, k8s-gke


## **gce_setup_hybrid_raid** / SCT_GCE_SETUP_HYBRID_RAID

If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: gce, gce-siren, k8s-gke


# Azure backend


## **azure_image_db** / SCT_AZURE_IMAGE_DB

The Azure image to be used for database nodes.

**default:** N/A

**type:** str (appendable)


<<<<<<< HEAD
||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **azure_image_db_oracle** / SCT_AZURE_IMAGE_DB_ORACLE

The Azure image to be used for oracle (2nd ref cluster) DB nodes. If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


=======
## **azure_image_db_oracle** / SCT_AZURE_IMAGE_DB_ORACLE

The Azure image to be used for oracle (2nd ref cluster) DB nodes. If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-26_04-lts:server:latest`: azure


>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR

The Azure image to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-24_04-lts:server:latest`: azure


<<<<<<< HEAD
## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-26_04-lts:server:latest`: azure


||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-26_04-lts:{arch_sku}:latest`: azure


=======
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME

The username for the Azure image.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: azure


## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB

The Azure virtual machine size to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE

The Azure virtual machine size to be used for Oracle database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER

The Azure virtual machine size to be used for loader nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR

The Azure virtual machine size to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)


## **azure_provision_stuck_vm_recreate_attempts** / SCT_AZURE_PROVISION_STUCK_VM_RECREATE_ATTEMPTS

How many times to recreate a stuck Azure VM (full node: VM, NIC and public IP) onto<br>fresh capacity before giving up with a non-retryable error.

**default:** N/A

**type:** int

**backend overrides:**
- `3`: azure


## **azure_provision_stuck_vm_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TIMEOUT

Seconds to wait for an Azure VM to reach the 'Succeeded' provisioning state before<br>treating it as stuck (accepted by Azure but never started by the host - SCT-434) and<br>recreating it. Detection is gated on the polled instanceView provisioning state.

**default:** N/A

**type:** int

**backend overrides:**
- `900`: azure


## **azure_provision_stuck_vm_total_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TOTAL_TIMEOUT

Total timeout (seconds) for the whole stuck-VM recovery attempts.<br>Recovery stops with a non-retryable error when either this timeout or<br>'azure_provision_stuck_vm_recreate_attempts' is exhausted. This way a degraded Azure<br>region cannot keep provisioning running until the CI stage times out SCT.<br>This value must be at least 'azure_provision_stuck_vm_timeout', otherwise SCT may<br>give up during the initial wait without making even one recreate attempt.

**default:** N/A

**type:** int

**backend overrides:**
- `4500`: azure


## **azure_region_name** / SCT_AZURE_REGION_NAME

Azure region(s) where the resources will be deployed. Supports single or multiple regions.

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eastus']`: azure


# OCI backend


## **oci_image_db** / SCT_OCI_IMAGE_DB

Oracle Cloud image to use for DB node(s)

**default:** N/A

**type:** str (appendable)


## **oci_image_db_oracle** / SCT_OCI_IMAGE_DB_ORACLE

Oracle Cloud image to use for oracle (2nd ref cluster) DB node(s). If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **oci_image_loader** / SCT_OCI_IMAGE_LOADER

Oracle Cloud image to use for the loader node(s). Empty value results into latest ubuntu image

**default:** N/A

**type:** str (appendable)


## **oci_image_monitor** / SCT_OCI_IMAGE_MONITOR

Oracle Cloud image to use for the monitor node. Empty value results into latest ubuntu image

**default:** N/A

**type:** str (appendable)


## **oci_image_username** / SCT_OCI_IMAGE_USERNAME

Username used in the Oracle Cloud images utilized by the DB node(s)

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: oci


## **oci_instance_type_db** / SCT_OCI_INSTANCE_TYPE_DB

Oracle Cloud instance shape to use for DB node(s). Usage of flex shapes allows setting of the ocpus, memory and nvme disks. Format is following: <shape-name>:<ocpus>:<ram>:<nvmes> . For DenseIO shapes it makes sense to specify only 'ocpus' part, because ram and amount of NVMe disks will be fixed based on the OCPUs count.

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_db_oracle** / SCT_OCI_INSTANCE_TYPE_DB_ORACLE

Oracle Cloud instance shape to use for 'oracle' (2nd ref cluster) ScylladbDB cluster

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_loader** / SCT_OCI_INSTANCE_TYPE_LOADER

Oracle Cloud instance shape to use for loader node(s). Usage of flex shapes allows setting of the ocpus, memory. Format is following: <shape-name>:<ocpus>:<ram>

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_monitor** / SCT_OCI_INSTANCE_TYPE_MONITOR

Oracle Cloud instance shape to use for monitor node. Usage of flex shapes allows setting of the ocpus, memory. Format is following: <shape-name>:<ocpus>:<ram>

**default:** N/A

**type:** str (appendable)


## **oci_region_name** / SCT_OCI_REGION_NAME

OCI region where the resources will be deployed

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['us-phoenix-1']`: oci


# Kubernetes backends (EKS/GKE/kind)


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

Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when 'k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.

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


# Docker backend


## **docker_image** / SCT_DOCKER_IMAGE

Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version

**default:** N/A

**type:** str (appendable)


## **docker_network** / SCT_DOCKER_NETWORK

Local docker network to use, if there's need to have db cluster connect to other services running in docker

**default:** N/A

**type:** str (appendable)


# Baremetal backend


## **db_nodes_private_ip** / SCT_DB_NODES_PRIVATE_IP

Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **db_nodes_public_ip** / SCT_DB_NODES_PUBLIC_IP

Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **loaders_private_ip** / SCT_LOADERS_PRIVATE_IP

Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **loaders_public_ip** / SCT_LOADERS_PUBLIC_IP

Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_nodes_private_ip** / SCT_MONITOR_NODES_PRIVATE_IP

Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_nodes_public_ip** / SCT_MONITOR_NODES_PUBLIC_IP

Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **s3_baremetal_config** / SCT_S3_BAREMETAL_CONFIG

Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.

**default:** N/A

**type:** str (appendable)


# Scylla Cloud (xcloud) backend


## **cloud_cluster_id** / SCT_CLOUD_CLUSTER_ID

ID of an existing Scylla Cloud cluster to run against, instead of provisioning a new one.

**default:** N/A

**type:** int


## **cloud_credentials_path** / SCT_CLOUD_CREDENTIALS_PATH

Path to the SSH private key for nodes in a Scylla Cloud (siren) cluster, which SCT does not provision itself.

**default:** N/A

**type:** str (appendable)


## **cloud_prom_bearer_token** / SCT_CLOUD_PROM_BEARER_TOKEN

scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_host** / SCT_CLOUD_PROM_HOST

scylla cloud promproxy hostname to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_path** / SCT_CLOUD_PROM_PATH

scylla cloud promproxy path to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **xcloud_availability_zones** / SCT_XCLOUD_AVAILABILITY_ZONES

Comma-separated availability zones for Scylla Cloud DB placement.<br>AWS values are AZ IDs (e.g., 'use1-az1,use1-az2,use1-az3'); GCE values are zone names<br>(e.g., 'us-east1-b,us-east1-c'). When set, SCT sends 'availabilityZoneIdsOverride' and forces placement.<br>Provide one zone per DB node, or provide a shorter list to cycle round-robin (node count must divide evenly).<br>Repeat the same zone to keep all nodes in one AZ. Leave empty (default) to let Scylla Cloud choose placement<br>(multi-AZ spread). Cannot be used with 'xcloud_scaling_config'.

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


## **xcloud_replication_factor** / SCT_XCLOUD_REPLICATION_FACTOR

Replication factor for Scylla Cloud cluster

**default:** N/A

**type:** int


## **xcloud_scaling_config** / SCT_XCLOUD_SCALING_CONFIG

Scaling policy configuration. The payload should follow the following structure:<br><br>{<br>"InstanceFamilies": ["i8g"],<br>"Mode": "xcloud",<br>"Policies": {<br>"Storage": {"Min": 0, "TargetUtilization": 0.8},<br>"VCPU": {"Min": 0}<br>}<br>}<br><br>- InstanceFamilies(list): instance families to use for scaling (e.g., ["i4i", "i8g"])<br>- Mode(str): scaling mode, always "xcloud"<br>- Policies(dict): scaling policies with the following keys:<br>- Storage(dict):<br>- Min(int): minimum storage in TB to maintain<br>- TargetUtilization(float): target storage utilization from 0.7 to 0.9 with 0.05 step<br>- VCPU(dict):<br>- Min(int): minimum number of virtual CPUs to maintain<br><br>For more details, see `scaling` parameter description in Cloud REST API documentation:<br>https://cloud.docs.scylladb.com/stable/api.html#tag/Cluster/operation/createCluster

**default:** N/A

**type:** dict

**backend overrides:**
- `{}`: xcloud


## **xcloud_vpc_peering** / SCT_XCLOUD_VPC_PEERING

Dictionary of VPC peering parameters for private connectivity between<br>SCT infrastructure and Scylla Cloud. The following parameters are used:<br>enabled: bool - indicates whether VPC peering is to be used<br>cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)<br>cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)

**default:** N/A

<<<<<<< HEAD
**type:** str | list[str] → list[str] (appendable)


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

Number of user profile tables to create for the test

**default:** 1

**type:** int


## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** False

**type:** bool


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


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

**type:** dict | YAML/JSON string → dict


## **stress_cmd_w** / SCT_STRESS_CMD_W

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_r** / SCT_STRESS_CMD_R

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_m** / SCT_STRESS_CMD_M

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_disk** / SCT_STRESS_CMD_READ_DISK

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARMUP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_before_migration** / SCT_STRESS_BEFORE_MIGRATION

Stress command to write data for post-migration validation

**default:** N/A

**type:** str (appendable)


## **verify_stress_after_migration** / SCT_VERIFY_STRESS_AFTER_MIGRATION

Stress command to verify data after migration

**default:** N/A

**type:** str (appendable)


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE



**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **cs_duration** / SCT_CS_DURATION



**default:** 50m

**type:** str (appendable)


## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** bool


## **cs_extra_jvm_opts** / SCT_CS_EXTRA_JVM_OPTS

Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' (requires Java 21+, which cassandra-stress 3.20.6+ ships with).

**default:** N/A

**type:** str (appendable)


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Supports three formats: 1) String/int list (cassandra-stress): {'read': ['100000', '150000'], 'mixed': [100, 200]} 2) Dict list (latte/multi-param): {'read': [{'threads': 10, 'concurrency': 128, 'rate': '100000'}, ...]} Dict format allows specifying threads, concurrency, and rate per step. Integers are automatically converted to strings for backward compatibility.

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **skip_download** / SCT_SKIP_DOWNLOAD



**default:** False

**type:** bool


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


## **disable_raft** / SCT_DISABLE_RAFT

Flag to disable Raft consensus for LWT operations.

**default:** True

**type:** bool


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade

**default:** False

**type:** bool


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** False

**type:** bool


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default.

**default:** True

**type:** bool


## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int
||||||| parent of 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)
**type:** str | list[str] → list[str] (appendable)


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

Number of user profile tables to create for the test

**default:** 1

**type:** int


## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** False

**type:** bool


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


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

**type:** dict | YAML/JSON string → dict


## **stress_cmd_w** / SCT_STRESS_CMD_W

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_r** / SCT_STRESS_CMD_R

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_m** / SCT_STRESS_CMD_M

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_disk** / SCT_STRESS_CMD_READ_DISK

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARMUP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **effective_compression_ratio** / SCT_EFFECTIVE_COMPRESSION_RATIO

Effective compression ratio used for Jinja stress command templating. Defined as on_disk_bytes / logical_uncompressed_bytes. This estimates how much disk space Scylla uses after compression relative to the logical uncompressed dataset size. For example, 1.0 means no effective compression and 0.68 means the data is expected to occupy about 68% of its logical uncompressed size on disk. Used together with the effective_disk_size_bytes template variable to calculate row counts that fill a target fraction of available disk capacity. You can estimate this ratio from Grafana in Keyspace -> Compression metrics; a compression value of 0% corresponds to effective_compression_ratio=1.0. Must be in range (0, 1.0].

**default:** 1.0

**type:** float


## **stress_template_context** / SCT_STRESS_TEMPLATE_CONTEXT

Shared runtime-only Jinja variables for stress command templating. Entries are resolved in declaration order and may reference earlier context entries as well as built-in stress template variables such as effective_disk_size_bytes and db_node_count_per_dc. These values are available to stress commands rendered by SCT, but are not evaluated during config load or validation.

**default:** {}

**type:** dict | YAML/JSON string → dict


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_before_migration** / SCT_STRESS_BEFORE_MIGRATION

Stress command to write data for post-migration validation

**default:** N/A

**type:** str (appendable)


## **verify_stress_after_migration** / SCT_VERIFY_STRESS_AFTER_MIGRATION

Stress command to verify data after migration

**default:** N/A

**type:** str (appendable)


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE



**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **cs_duration** / SCT_CS_DURATION



**default:** 50m

**type:** str (appendable)


## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** bool


## **cs_extra_jvm_opts** / SCT_CS_EXTRA_JVM_OPTS

Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' (requires Java 21+, which cassandra-stress 3.20.6+ ships with).

**default:** N/A

**type:** str (appendable)


## **cs_safepoint_logging** / SCT_CS_SAFEPOINT_LOGGING

Enable JVM safepoint logging (-Xlog:safepoint) for the cassandra-stress loaders. The log is written on the loader host, pulled into the loader log directory and collected into the run log archive. Use it to tell a loader JVM pause (including non-GC safepoints) apart from a server-side or network stall behind a latency-step failure. Not supported for k8s backends and prepared loaders.

**default:** False

**type:** bool


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Supports three formats: 1) String/int list (cassandra-stress): {'read': ['100000', '150000'], 'mixed': [100, 200]} 2) Dict list (latte/multi-param): {'read': [{'threads': 10, 'concurrency': 128, 'rate': '100000'}, ...]} Dict format allows specifying threads, concurrency, and rate per step. Integers are automatically converted to strings for backward compatibility.

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_write_preload_data** / SCT_PERF_GRADUAL_WRITE_PRELOAD_DATA

If true, preload data (via prepare_write_cmd) before test_write_gradual_increase_load. Needed for LWT conditional-update workloads (e.g. UPDATE ... IF <cond>) that require existing rows to have a chance of applying; not needed for INSERT-based write workloads on a fresh table.

**default:** False

**type:** bool


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **skip_download** / SCT_SKIP_DOWNLOAD



**default:** False

**type:** bool


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


## **disable_raft** / SCT_DISABLE_RAFT

Flag to disable Raft consensus for LWT operations.

**default:** True

**type:** bool


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade

**default:** False

**type:** bool


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** False

**type:** bool


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default.

**default:** True

**type:** bool


## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

cassandra-stress commands.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int
=======
**type:** dict
>>>>>>> 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)

**backend overrides:**
- `{'enabled': True, 'cidr_pool_base': '172.31.0.0/16', 'cidr_subnet_size': 24}`: xcloud


# Minicloud


## **minicloud_container_cpus** / SCT_MINICLOUD_CONTAINER_CPUS

Cap the minicloud container's CPU allowance, in docker --cpus form (e.g. '8' or '7.5'). Empty means no limit

**default:** N/A

**type:** str (appendable)


## **minicloud_container_memory** / SCT_MINICLOUD_CONTAINER_MEMORY

Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker limit, so the container can consume the whole host. Setting it also makes this, rather than the host's free memory, the budget the preflight guest-memory gate measures against

**default:** N/A

**type:** str (appendable)


## **minicloud_container_name** / SCT_MINICLOUD_CONTAINER_NAME

Name of the minicloud docker container. Change it to run two emulators on one host — a second run under the same name force-removes the first one's container

**default:** minicloud

**type:** str (appendable)


## **minicloud_docker_image** / SCT_MINICLOUD_DOCKER_IMAGE

Explicit minicloud image override. Empty means the renovate-managed default from defaults/docker_images/minicloud/ (exposed as stress_image.minicloud)

**default:** N/A

**type:** str (appendable)


## **minicloud_endpoint_url** / SCT_MINICLOUD_ENDPOINT_URL

EC2 API endpoint URL for minicloud. When set, SCT adapts for minicloud limitations (no spot, no EIP, graceful TerminateInstances). Example: http://localhost:5000

**default:** N/A

**type:** str


<<<<<<< HEAD
## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** vector

**type:** Literal['ssh', 'docker', 'syslog-ng', 'vector']

**backend overrides:**
- `docker`: docker


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** False

**type:** bool


## **use_scylla_doctor_on_failure** / SCT_USE_SCYLLA_DOCTOR_ON_FAILURE

Run scylla-doctor on test failure to collect additional diagnostics

**default:** True

**type:** bool


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** False

**type:** bool


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_vector_store_nodes** / SCT_POST_BEHAVIOR_VECTOR_STORE_NODES

Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_emr_cluster** / SCT_POST_BEHAVIOR_EMR_CLUSTER

Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.<br><br>'destroy' - Destroy EMR cluster (default)<br>'keep' - Keep EMR cluster running<br>'keep-on-failure' - Keep EMR cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla option: internode_compression.

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** str (appendable)


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


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** False

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

**default:** False

**type:** bool


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `a`: aws, oci, aws-siren, k8s-local-kind-aws
- `c`: k8s-gke
- `a,b`: k8s-eks


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Deprecated alias of `fallback_to_next_availability_zone`. Kept for backward compatibility.

**default:** False

**type:** bool


## **fallback_to_next_availability_zone** / SCT_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

On capacity errors, automatically retry provisioning in the next available AZ in the same region. Backend-agnostic parameter; supersedes `aws_fallback_to_next_availability_zone`.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **pre_filter_unavailable_availability_zones** / SCT_PRE_FILTER_UNAVAILABLE_AVAILABILITY_ZONES

Filter availability zones upfront to only those that support all required instance types. Replaces invalid AZs with valid alternatives in the same region before any provisioning attempt. Supported backends: AWS, GCE.

**default:** True

**type:** bool


## **pre_flight_capacity_probe** / SCT_PRE_FLIGHT_CAPACITY_PROBE

Before provisioning, probe capacity by launching and terminating one on-demand instance per dynamic type (`instance_type_db_target`, `nemesis_grow_shrink_instance_type`) in the chosen AZ. On capacity errors, raise to trigger AZ/region fallback. Costs ~1 min per type. AWS-only.

**default:** False

**type:** bool


## **fallback_to_next_region** / SCT_FALLBACK_TO_NEXT_REGION

On capacity errors, after all AZs/zones in the configured region are exhausted, relocate to the next eligible region: a single-region cluster moves as a whole, while in a multi-region test only the exhausted datacenter is relocated (to a region no other datacenter occupies) and the cluster is retried. On AWS the target region should be VPC-peered with the runner region with infra-prepared and AMI available; on GCE the global VPC and global images make any supported region eligible. Only applies during initial setup. Supported backends: AWS, GCE.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** int


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** bool


## **enable_truncate_checks_on_node_upgrade** / SCT_ENABLE_TRUNCATE_CHECKS_ON_NODE_UPGRADE

Enables or disables truncate checks on each node upgrade and rollback

**default:** True

**type:** bool


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade starts (preload/validation stage). This workload runs before any nodes are upgraded and can use CL=ALL for data validation.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **large_partition_stress_during_upgrade** / SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE

Stress command to be run during rolling upgrade while nodes are being upgraded. This workload cannot use CL=ALL as not all nodes may be available during the upgrade.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **minicloud_docker_image** / SCT_MINICLOUD_DOCKER_IMAGE

Explicit minicloud image override. Empty means the renovate-managed default from defaults/docker_images/minicloud/ (exposed as stress_image.minicloud)

**default:** N/A

**type:** str (appendable)


## **minicloud_lightweight** / SCT_MINICLOUD_LIGHTWEIGHT

Enable lightweight mode for minicloud deployments

**default:** True

**type:** bool


## **minicloud_lightweight_memory** / SCT_MINICLOUD_LIGHTWEIGHT_MEMORY

Memory allocation for lightweight minicloud deployments

**default:** 4GiB

**type:** str (appendable)


## **minicloud_lightweight_vcpus** / SCT_MINICLOUD_LIGHTWEIGHT_VCPUS

vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this multiplies with minicloud_lightweight_memory across every guest in the test — raise it only on a host with cores to spare

**default:** 1

**type:** int


## **minicloud_container_memory** / SCT_MINICLOUD_CONTAINER_MEMORY

Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker limit, so the container can consume the whole host. Setting it also makes this, rather than the host's free memory, the budget the preflight guest-memory gate measures against

**default:** N/A

**type:** str (appendable)


## **minicloud_container_cpus** / SCT_MINICLOUD_CONTAINER_CPUS

Cap the minicloud container's CPU allowance, in docker --cpus form (e.g. '8' or '7.5'). Empty means no limit

**default:** N/A

**type:** str (appendable)


## **minicloud_state_dir** / SCT_MINICLOUD_STATE_DIR

Where minicloud keeps its image cache, per-instance disks and minicloud.log — tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace

**default:** N/A

**type:** str (appendable)


## **minicloud_container_name** / SCT_MINICLOUD_CONTAINER_NAME

Name of the minicloud docker container. Change it to run two emulators on one host — a second run under the same name force-removes the first one's container

**default:** minicloud

**type:** str (appendable)


## **minicloud_keep_alive** / SCT_MINICLOUD_KEEP_ALIVE

Leave the minicloud container running after the test instead of tearing it down (CI sets this so separate provision/test/collect/clean stages reach the same container)

**default:** False

**type:** bool


## **minicloud_skip_memory_check** / SCT_MINICLOUD_SKIP_MEMORY_CHECK

Skip the conservative host-memory preflight gate — for development machines whose owner knows the workload's real footprint; an oversized test then dies mid-run as a container OOM kill (exit 137)

**default:** False

**type:** bool


## **minicloud_s3_passthrough_buckets** / SCT_MINICLOUD_S3_PASSTHROUGH_BUCKETS

S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). Backend-independent: GCE runs reach S3 for the same content

**default:** scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com

**type:** str | list[str] → list[str] (appendable)


## **minicloud_regions** / SCT_MINICLOUD_REGIONS

Narrow the AWS regions minicloud prepares (default: every SCT-supported region; each costs ~2s at start-up)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **minicloud_gcs_bucket** / SCT_MINICLOUD_GCS_BUCKET

GCS bucket for minicloud GCE image staging. Empty means derive <project>-minicloud-staging and create it on demand

**default:** N/A

**type:** str (appendable)


## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** vector

**type:** Literal['ssh', 'docker', 'syslog-ng', 'vector']

**backend overrides:**
- `docker`: docker


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** False

**type:** bool


## **collect_nvme_diagnostics** / SCT_COLLECT_NVME_DIAGNOSTICS

Collect NVMe SMART logs, error logs, and self-test results from DB nodes during test teardown. Requires nvme-cli to be installed on the nodes. Skipped gracefully on backends without NVMe devices.

**default:** False

**type:** bool


## **nvme_self_test_type** / SCT_NVME_SELF_TEST_TYPE

NVMe device self-test type to run: 1 (short, ~2 min) or 2 (extended, may take hours). Only used when collect_nvme_diagnostics is enabled. Honored only on controllers that advertise Device Self-test support (Identify Controller OACS bit 4); unsupported controllers are skipped without issuing the command. This has no effect on AWS: neither instance-store (Nitro SSD) nor EBS implements Device Self-test, so on AWS the diagnostics rely on SMART counters and the error log instead.

**default:** 1

**type:** int


## **use_scylla_doctor_on_failure** / SCT_USE_SCYLLA_DOCTOR_ON_FAILURE

Run scylla-doctor on test failure to collect additional diagnostics

**default:** True

**type:** bool


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** False

**type:** bool


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_vector_store_nodes** / SCT_POST_BEHAVIOR_VECTOR_STORE_NODES

Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_emr_cluster** / SCT_POST_BEHAVIOR_EMR_CLUSTER

Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.<br><br>'destroy' - Destroy EMR cluster (default)<br>'keep' - Keep EMR cluster running<br>'keep-on-failure' - Keep EMR cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla option: internode_compression.

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** str (appendable)


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


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** False

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

**default:** False

**type:** bool


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `a`: aws, oci, aws-siren, k8s-local-kind-aws
- `c`: k8s-gke
- `a,b`: k8s-eks


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Deprecated alias of `fallback_to_next_availability_zone`. Kept for backward compatibility.

**default:** False

**type:** bool


## **fallback_to_next_availability_zone** / SCT_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

On capacity errors, automatically retry provisioning in the next available AZ in the same region. Backend-agnostic parameter; supersedes `aws_fallback_to_next_availability_zone`.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **pre_filter_unavailable_availability_zones** / SCT_PRE_FILTER_UNAVAILABLE_AVAILABILITY_ZONES

Filter availability zones upfront to only those that support all required instance types. Replaces invalid AZs with valid alternatives in the same region before any provisioning attempt. Supported backends: AWS, GCE.

**default:** True

**type:** bool


## **pre_flight_capacity_probe** / SCT_PRE_FLIGHT_CAPACITY_PROBE

Before provisioning, probe capacity by launching and terminating one on-demand instance per dynamic type (`instance_type_db_target`, `nemesis_grow_shrink_instance_type`) in the chosen AZ. On capacity errors, raise to trigger AZ/region fallback. Costs ~1 min per type. AWS-only.

**default:** False

**type:** bool


## **fallback_to_next_region** / SCT_FALLBACK_TO_NEXT_REGION

On capacity errors, after all AZs/zones in the configured region are exhausted, relocate to the next eligible region: a single-region cluster moves as a whole, while in a multi-region test only the exhausted datacenter is relocated (to a region no other datacenter occupies) and the cluster is retried. On AWS the target region should be VPC-peered with the runner region with infra-prepared and AMI available; on GCE the global VPC and global images make any supported region eligible. Only applies during initial setup. Supported backends: AWS, GCE.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** int


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** bool


## **enable_truncate_checks_on_node_upgrade** / SCT_ENABLE_TRUNCATE_CHECKS_ON_NODE_UPGRADE

Enables or disables truncate checks on each node upgrade and rollback

**default:** True

**type:** bool


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade starts (preload/validation stage). This workload runs before any nodes are upgraded and can use CL=ALL for data validation.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **large_partition_stress_during_upgrade** / SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE

Stress command to be run during rolling upgrade while nodes are being upgraded. This workload cannot use CL=ALL as not all nodes may be available during the upgrade.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


=======
## **minicloud_gcs_bucket** / SCT_MINICLOUD_GCS_BUCKET

GCS bucket for minicloud GCE image staging. Empty means derive <project>-minicloud-staging and create it on demand

**default:** N/A

**type:** str (appendable)


## **minicloud_keep_alive** / SCT_MINICLOUD_KEEP_ALIVE

Leave the minicloud container running after the test instead of tearing it down (CI sets this so separate provision/test/collect/clean stages reach the same container)

**default:** False

**type:** bool


## **minicloud_lightweight** / SCT_MINICLOUD_LIGHTWEIGHT

Enable lightweight mode for minicloud deployments

**default:** True

**type:** bool


## **minicloud_lightweight_memory** / SCT_MINICLOUD_LIGHTWEIGHT_MEMORY

Memory allocation for lightweight minicloud deployments

**default:** 4GiB

**type:** str (appendable)


## **minicloud_lightweight_vcpus** / SCT_MINICLOUD_LIGHTWEIGHT_VCPUS

vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this multiplies with minicloud_lightweight_memory across every guest in the test — raise it only on a host with cores to spare

**default:** 1

**type:** int


## **minicloud_regions** / SCT_MINICLOUD_REGIONS

Narrow the AWS regions minicloud prepares (default: every SCT-supported region; each costs ~2s at start-up)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **minicloud_s3_passthrough_buckets** / SCT_MINICLOUD_S3_PASSTHROUGH_BUCKETS

S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). Backend-independent: GCE runs reach S3 for the same content

**default:** scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com

**type:** str | list[str] → list[str] (appendable)


## **minicloud_skip_memory_check** / SCT_MINICLOUD_SKIP_MEMORY_CHECK

Skip the conservative host-memory preflight gate — for development machines whose owner knows the workload's real footprint; an oversized test then dies mid-run as a container OOM kill (exit 137)

**default:** False

**type:** bool


## **minicloud_state_dir** / SCT_MINICLOUD_STATE_DIR

Where minicloud keeps its image cache, per-instance disks and minicloud.log — tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace

**default:** N/A

**type:** str (appendable)


>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
# Longevity tests


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Compaction strategy to use for pre-created schema

**default:** IncrementalCompactionStrategy

**type:** str (appendable)


## **data_validation** / SCT_DATA_VALIDATION

Specify the type of data validation to perform

**default:** N/A

**type:** str (appendable)


## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keyspace to be pre-created before running workload

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **pre_create_schema** / SCT_PRE_CREATE_SCHEMA

Enable or disable pre-creation of schema before running workload

**default:** False

**type:** bool


## **run_commit_log_check_thread** / SCT_RUN_COMMIT_LOG_CHECK_THREAD

Flag to run a thread that checks commit logs

**default:** True

**type:** bool

**backend overrides:**
- `False`: xcloud


## **run_full_partition_scan** / SCT_RUN_FULL_PARTITION_SCAN

Enable or disable running full partition scans during tests

**default:** N/A

**type:** str (appendable)


## **run_fullscan** / SCT_RUN_FULLSCAN

Enable or disable running full scans during tests

**default:** []

**type:** list


## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Enable or disable tombstone garbage collection verification during tests

**default:** N/A

**type:** str (appendable)


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** 0

**type:** int


## **sstable_size** / SCT_SSTABLE_SIZE

Configure sstable size for pre-create-schema mode

**default:** N/A

**type:** int


## **validate_large_collections** / SCT_VALIDATE_LARGE_COLLECTIONS

Flag to validate large collections in the database

**default:** False

**type:** bool


# Performance regression tests


## **max_deviation** / SCT_MAX_DEVIATION

Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one

**default:** N/A

**type:** float


## **n_stress_process** / SCT_N_STRESS_PROCESS

Number of stress processes per loader

**default:** N/A

**type:** int


## **num_loaders_step** / SCT_NUM_LOADERS_STEP

Number of loaders which should be added per step

**default:** N/A

**type:** int


## **num_threads_step** / SCT_NUM_THREADS_STEP

Number of threads which should be added on per step

**default:** N/A

**type:** int


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Supports three formats: 1) String/int list (cassandra-stress): {'read': ['100000', '150000'], 'mixed': [100, 200]} 2) Dict list (latte/multi-param): {'read': [{'threads': 10, 'concurrency': 128, 'rate': '100000'}, ...]} Dict format allows specifying threads, concurrency, and rate per step. Integers are automatically converted to strings for backward compatibility.

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_write_preload_data** / SCT_PERF_GRADUAL_WRITE_PRELOAD_DATA

If true, preload data (via prepare_write_cmd) before test_write_gradual_increase_load. Needed for LWT conditional-update workloads (e.g. UPDATE ... IF <cond>) that require existing rows to have a chance of applying; not needed for INSERT-based write workloads on a fresh table.

**default:** False

**type:** bool


## **perf_simple_query_extra_command** / SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND

Extra command line options to pass to perf_simple_query

**default:** N/A

**type:** str (appendable)


## **perf_stress_keyspace** / SCT_PERF_STRESS_KEYSPACE

Keyspace name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'keyspace' key in latte_schema_parameters.

**default:** N/A

**type:** str (appendable)


## **perf_stress_table** / SCT_PERF_STRESS_TABLE

Table name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'table' key in latte_schema_parameters.

**default:** N/A

**type:** str (appendable)


## **run_db_node_benchmarks** / SCT_RUN_DB_NODE_BENCHMARKS

Flag for running db node benchmarks before the tests

**default:** False

**type:** bool


## **stop_on_hw_perf_failure** / SCT_STOP_ON_HW_PERF_FAILURE

Stop sct performance test if hardware performance test failed<br><br>Hardware performance tests runs on each node with sysbench and cassandra-fio tools.<br>Results stored in ES. HW perf tests run during cluster setups and not affect<br>SCT Performance tests. Results calculated as average among all results for certain<br>instance type or among all nodes during single run.<br>if results for a single node is not in margin 0.01 of<br>average result for all nodes, hw test considered as Failed.<br>If stop_on_hw_perf_failure is True, then sct performance test will be terminated<br>after hw perf tests detect node with hw results not in margin with average<br>If stop_on_hw_perf_failure is False, then sct performance test will be run<br>even after hw perf tests detect node with hw results not in margin with average

**default:** False

**type:** bool


## **stress_process_step** / SCT_STRESS_PROCESS_STEP

add/remove num of process on each round

**default:** N/A

**type:** int


## **stress_step_duration** / SCT_STRESS_STEP_DURATION

Duration of time for stress round

**default:** 15m

**type:** str (appendable)


## **stress_threads_start_num** / SCT_STRESS_THREADS_START_NUM

Number of threads for c-s command

**default:** N/A

**type:** int


## **use_hdrhistogram** / SCT_USE_HDRHISTOGRAM

Enable hdr histogram logging for cs

**default:** False

**type:** bool


# Upgrade tests


<<<<<<< HEAD
## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO

URL to the Scylla repository for new versions.

**default:** N/A

<<<<<<< HEAD
**type:** int

||||||| parent of 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)
**type:** int

**backend overrides:**
- `0`: xcloud

=======
**type:** str (appendable)

>>>>>>> 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)

## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** str (appendable)


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

The target version to upgrade Scylla to.

**default:** N/A

**type:** str (appendable)


||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
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


=======
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
## **disable_raft** / SCT_DISABLE_RAFT

Flag to disable Raft consensus for LWT operations.

**default:** True

**type:** bool


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade

**default:** False

**type:** bool


## **enable_truncate_checks_on_node_upgrade** / SCT_ENABLE_TRUNCATE_CHECKS_ON_NODE_UPGRADE

Enables or disables truncate checks on each node upgrade and rollback

**default:** True

**type:** bool


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** False

**type:** bool


## **large_partition_stress_during_upgrade** / SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE

Stress command to be run during rolling upgrade while nodes are being upgraded. This workload cannot use CL=ALL as not all nodes may be available during the upgrade.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO

URL to the Scylla repository for new versions.

**default:** N/A

**type:** str (appendable)


## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** str (appendable)


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** int


## **run_gemini_in_rolling_upgrade** / SCT_RUN_GEMINI_IN_ROLLING_UPGRADE

Enable running Gemini workload during rolling upgrade test. Default is false.

**default:** False

**type:** bool


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade starts (preload/validation stage). This workload runs before any nodes are upgraded and can use CL=ALL for data validation.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

The target version to upgrade Scylla to.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default.

**default:** True

**type:** bool


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** bool


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

Stress command(s) run after every node has been upgraded, to verify the upgraded cluster. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_migration** / SCT_VERIFY_STRESS_AFTER_MIGRATION

Stress command to verify data after migration

**default:** N/A

**type:** str (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

<<<<<<< HEAD
**type:** int

<<<<<<< HEAD

## **xcloud_vpc_peering** / SCT_XCLOUD_VPC_PEERING

Dictionary of VPC peering parameters for private connectivity between<br>SCT infrastructure and Scylla Cloud. The following parameters are used:<br>enabled: bool - indicates whether VPC peering is to be used<br>cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)<br>cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)

**default:** N/A

**type:** dict

||||||| parent of 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)

## **xcloud_availability_zones** / SCT_XCLOUD_AVAILABILITY_ZONES

Comma-separated availability zones for Scylla Cloud DB placement.<br>AWS values are AZ IDs (e.g., 'use1-az1,use1-az2,use1-az3'); GCE values are zone names<br>(e.g., 'us-east1-b,us-east1-c'). When set, SCT sends 'availabilityZoneIdsOverride' and forces placement.<br>Provide one zone per DB node, or provide a shorter list to cycle round-robin (node count must divide evenly).<br>Repeat the same zone to keep all nodes in one AZ. Leave empty (default) to let Scylla Cloud choose placement<br>(multi-AZ spread). Cannot be used with 'xcloud_scaling_config'.

**default:** N/A

**type:** str (appendable)


## **xcloud_vpc_peering** / SCT_XCLOUD_VPC_PEERING

Dictionary of VPC peering parameters for private connectivity between<br>SCT infrastructure and Scylla Cloud. The following parameters are used:<br>enabled: bool - indicates whether VPC peering is to be used<br>cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)<br>cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)

**default:** N/A

**type:** dict

=======
>>>>>>> 3bd271237 (refactor(sct_config): split field definitions into 25 domain mixins)
**backend overrides:**
- `60`: aws, gce, azure, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.

**default:** True

**type:** bool


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** False

**type:** bool
||||||| parent of 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)
**type:** int

**backend overrides:**
- `60`: aws, gce, azure, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.

**default:** True

**type:** bool


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** False

**type:** bool
=======
**type:** str | list[str] → list[str] (appendable)
>>>>>>> 538efdcf7 (refactor(sct_config): regroup options by what they configure, and document all 523)


# Grow cluster tests


## **cassandra_stress_population_size** / SCT_CASSANDRA_STRESS_POPULATION_SIZE

The total population size over which the Cassandra stress tests are run.

**default:** 1000000

**type:** int


## **cassandra_stress_threads** / SCT_CASSANDRA_STRESS_THREADS

The number of threads used by Cassandra stress tests.

**default:** 1000

**type:** int


# Refresh (sstable loading) tests


## **flush_period** / SCT_FLUSH_PERIOD

Seconds to wait between the flushes controlled by 'flush_times'.

**default:** N/A

**type:** int


## **flush_times** / SCT_FLUSH_TIMES

How many times to flush the memtable to disk during the refresh test.

**default:** N/A

**type:** int


## **skip_download** / SCT_SKIP_DOWNLOAD

Skip downloading the SSTable archive and reuse a copy already on the node.

**default:** False

**type:** bool


## **sstable_file** / SCT_SSTABLE_FILE

Local path of the SSTable archive to load with 'nodetool refresh'.

**default:** N/A

**type:** str (appendable)


## **sstable_md5** / SCT_SSTABLE_MD5

Expected MD5 of the downloaded SSTable archive, used to verify the download.

**default:** N/A

**type:** str (appendable)


## **sstable_url** / SCT_SSTABLE_URL

URL the SSTable archive is downloaded from when it is not already on the node.

**default:** N/A

**type:** str (appendable)


# Jepsen tests


## **jepsen_scylla_repo** / SCT_JEPSEN_SCYLLA_REPO

Link to the git repository with Jepsen Scylla tests

**default:** https://github.com/jepsen-io/scylla.git

**type:** str (appendable)


## **jepsen_test_cmd** / SCT_JEPSEN_TEST_CMD

Jepsen test command (e.g., 'test-all')

**default:** ['test-all -w cas-register --concurrency 10n', 'test-all -w counter --concurrency 10n', 'test-all -w cmap --concurrency 10n', 'test-all -w cset --concurrency 10n', 'test-all -w write-isolation --concurrency 10n', 'test-all -w list-append --concurrency 10n', 'test-all -w wr-register --concurrency 10n']

**type:** str | list[str] → list[str] (appendable)


## **jepsen_test_count** / SCT_JEPSEN_TEST_COUNT

Possible number of reruns of single Jepsen test command

**default:** 1

**type:** int


## **jepsen_test_run_policy** / SCT_JEPSEN_TEST_RUN_POLICY

Jepsen test run policy (i.e., what we want to consider as passed for a single test)<br><br>'most' - most test runs are passed<br>'any'  - one pass is enough<br>'all'  - all test runs should pass

**default:** all

**type:** Literal['most', 'any', 'all']


# Amazon EMR (spark-migrator)


## **emr_applications** / SCT_EMR_APPLICATIONS

List of EMR applications to install (default: ['Spark'])

**default:** N/A

**type:** list

**backend overrides:**
- `['Spark']`: aws


## **emr_install_spark4_via_bootstrap** / SCT_EMR_INSTALL_SPARK4_VIA_BOOTSTRAP

Legacy fallback: install Spark 4.x via an EMR bootstrap action and submit the migrator through script-runner.jar (for emr-7.x releases). Default value is false - i.e. deployment of native Spark on an `emr-spark-8.x` release label.

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: aws


## **emr_instance_count_core** / SCT_EMR_INSTANCE_COUNT_CORE

How many EMR core nodes to launch.

**default:** N/A

**type:** int

**backend overrides:**
- `2`: aws


## **emr_instance_count_task** / SCT_EMR_INSTANCE_COUNT_TASK

How many EMR task nodes to launch (compute only, no HDFS).

**default:** N/A

**type:** int

**backend overrides:**
- `0`: aws


## **emr_instance_type_core** / SCT_EMR_INSTANCE_TYPE_CORE

EC2 instance type for the EMR core nodes (they run both compute and HDFS).

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_master** / SCT_EMR_INSTANCE_TYPE_MASTER

Instance type for EMR master node (e.g., 'm5.xlarge')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_task** / SCT_EMR_INSTANCE_TYPE_TASK

Instance type for EMR task nodes (optional, uses Spot instances)

**default:** N/A

**type:** str (appendable)


## **emr_keep_alive** / SCT_EMR_KEEP_ALIVE

Whether EMR cluster stays alive after job completion (default: true for reuse during testing)

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws


## **emr_log_uri** / SCT_EMR_LOG_URI

S3 URI for EMR cluster logs (e.g., 's3://sct-emr-spark-migrator-{region}/logs/')

**default:** N/A

**type:** str (appendable)


## **emr_release_label** / SCT_EMR_RELEASE_LABEL

EMR release version (e.g., 'emr-7.8.0'). When set, an EMR cluster is provisioned alongside the Scylla cluster.

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_jar_path** / SCT_EMR_SPARK_MIGRATOR_JAR_PATH

S3 path or local path to the spark-migrator JAR file

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_release** / SCT_EMR_SPARK_MIGRATOR_RELEASE

scylla-migrator release tag (e.g., 'v1.1.2'). When set, JAR is auto-downloaded from GitHub releases and uploaded to S3. Takes precedence over emr_spark_migrator_jar_path.

**default:** N/A

**type:** str (appendable)


## **emr_spot_bid_percentage** / SCT_EMR_SPOT_BID_PERCENTAGE

Max Spot price as percentage of On-Demand for EMR task nodes (default: 100)

**default:** N/A

**type:** int

**backend overrides:**
- `100`: aws


# Spark migrator (Cassandra to Scylla)


## **migrator_run_validator** / SCT_MIGRATOR_RUN_VALIDATOR

Run the spark-migrator validator after migration to do a row-by-row comparison

**default:** N/A

**type:** bool


## **migrator_source_hosts** / SCT_MIGRATOR_SOURCE_HOSTS

CQL contact-point IPs for the source Cassandra/Scylla cluster. Mutually exclusive with migrator_source_test_id.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **migrator_source_keyspace** / SCT_MIGRATOR_SOURCE_KEYSPACE

Keyspace to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_table** / SCT_MIGRATOR_SOURCE_TABLE

Table to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_test_id** / SCT_MIGRATOR_SOURCE_TEST_ID

SCT test_id of a running source cluster. When set, source host IPs are auto-discovered via EC2 tags (NodeType=cs-db). Mutually exclusive with migrator_source_hosts.

**default:** N/A

**type:** str (appendable)


## **migrator_step_timeout_minutes** / SCT_MIGRATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator migration EMR step. Default 360.

**default:** N/A

**type:** int

**backend overrides:**
- `360`: aws


## **migrator_target_keyspace** / SCT_MIGRATOR_TARGET_KEYSPACE

Keyspace to migrate into on the target Scylla cluster. Defaults to migrator_source_keyspace.

**default:** N/A

**type:** str (appendable)


## **migrator_target_table** / SCT_MIGRATOR_TARGET_TABLE

Table to migrate into on the target Scylla cluster. Defaults to migrator_source_table.

**default:** N/A

**type:** str (appendable)


## **validator_step_timeout_minutes** / SCT_VALIDATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator validator EMR step. Default 60.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws
||||||| parent of 905145b03 (docs(sct_config): split the option reference by group, and finish the regrouping)
#### Options by group
The options below are grouped by domain -- cross-cutting concerns first, then one
section per backend, then one per test type. Each group mirrors a mixin module under
`sdcm/sct_config/mixins/`.

# General and provisioning


## **adaptive_timeout_multipliers** / SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS

Optional dict of adaptive-timeout multipliers keyed by operation name (from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). If the current operation key is absent, multiplier 1.0 is used.<br>YAML example:<br>adaptive_timeout_multipliers:<br>  decommission: 4<br>  new_node: 2<br>Environment variable examples:<br>SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS="{'decommission': 4, 'new_node': 2}"<br>Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4

**default:** {}

**type:** sdcm.sct_config.AdaptiveTimeoutMultipliers


## **adaptive_timeout_store_metrics** / SCT_ADAPTIVE_TIMEOUT_STORE_METRICS

Store adaptive timeout metrics in Argus. Disabled for performance tests only.

**default:** True

**type:** bool


## **add_node_cnt** / SCT_ADD_NODE_CNT

The number of nodes to add during the test.

**default:** 1

**type:** int


## **agent** / SCT_AGENT

Configuration for SCT agent - a lightweight service for remote command execution.                 When enabled, replaces SSH-based command execution with RESTful API calls for DB nodes.<br>Configuration options:<br>- enabled: bool - enable agent (required)<br>- port: int - agent HTTP API port (default: 16000)<br>- binary_url: str - URL to download agent binary<br>- max_concurrent_jobs: int - max concurrent jobs per agent (default: 10)<br>- log_level: str - logging level (default: info)<br>- tls: bool - enable TLS for agent communication (default: false)

**default:** {'enabled': False, 'port': 16000, 'binary_url': '', 'max_concurrent_jobs': 10, 'log_level': 'info', 'tls': False}

**type:** dict | YAML/JSON string → dict


## **availability_zone** / SCT_AVAILABILITY_ZONE

Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).<br>"Same for multi-region scenario.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `a`: aws, oci, aws-siren, k8s-local-kind-aws
- `c`: k8s-gke
- `a,b`: k8s-eks


## **billing_project** / SCT_BILLING_PROJECT

Billing project for the test run. Used for cost tracking and reporting

**default:** N/A

**type:** str (appendable)


## **bisect_end_date** / SCT_BISECT_END_DATE

End date for bisecting test runs to find regressions

**default:** N/A

**type:** str (appendable)


## **bisect_start_date** / SCT_BISECT_START_DATE

Start date for bisecting test runs to find regressions

**default:** N/A

**type:** str (appendable)


## **cluster_backend** / SCT_CLUSTER_BACKEND

backend that will be used, aws/gce/azure/oci/docker/xcloud

**default:** N/A

**type:** str


## **cluster_health_check** / SCT_CLUSTER_HEALTH_CHECK

Enable or disable starting cluster health checker for all nodes

**default:** True

**type:** bool


## **cluster_health_check_parallel_workers** / SCT_CLUSTER_HEALTH_CHECK_PARALLEL_WORKERS

Number of parallel workers for health checks. Values above 10 are not recommended (diminishing returns, risk of API rate limiting). Default: 5.

**default:** 5

**type:** int


## **config_files** / SCT_CONFIG_FILES

a list of config files that would be used

**default:** N/A

**type:** str | list[str] → list[str]


## **data_volume_disk_iops** / SCT_DATA_VOLUME_DISK_IOPS

Number of iops for ebs type io2|io3|gp3

**default:** 0

**type:** int

**backend overrides:**
- `10000`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks


## **data_volume_disk_num** / SCT_DATA_VOLUME_DISK_NUM

Number of additional data volumes attached to instances<br>if data_volume_disk_num > 0, then data volumes (ebs on aws) will be<br>used for scylla data directory

**default:** 0

**type:** int


## **data_volume_disk_size** / SCT_DATA_VOLUME_DISK_SIZE

Size of additional volume in GB

**default:** 0

**type:** int

**backend overrides:**
- `500`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **data_volume_disk_throughput** / SCT_DATA_VOLUME_DISK_THROUGHPUT

Throughput in MiB/sec for ebs type gp3. Min is 125. Max is 1000.

**default:** N/A

**type:** int


## **data_volume_disk_type** / SCT_DATA_VOLUME_DISK_TYPE

Type of additional volumes. AWS: gp2|gp3|io2|io3. OCI: lower_cost|balanced|higher_performance|ultra

**default:** N/A

**type:** Literal['gp2', 'gp3', 'io2', 'io3', '', 'lower_cost', 'balanced', 'higher_performance', 'ultra']

**backend overrides:**
- `gp2`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks
- `ultra`: oci


## **db_nodes_shards_selection** / SCT_DB_NODES_SHARDS_SELECTION

How to select number of shards of Scylla. Expected values: default/random.<br>Default value: 'default'.<br>In case of random option - Scylla will start with different (random) shards on every node of the cluster

**default:** default

**type:** Literal['default', 'random']


## **fallback_to_next_availability_zone** / SCT_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

On capacity errors, automatically retry provisioning in the next available AZ in the same region. Backend-agnostic parameter; supersedes `aws_fallback_to_next_availability_zone`.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **fallback_to_next_region** / SCT_FALLBACK_TO_NEXT_REGION

On capacity errors, after all AZs/zones in the configured region are exhausted, relocate to the next eligible region: a single-region cluster moves as a whole, while in a multi-region test only the exhausted datacenter is relocated (to a region no other datacenter occupies) and the cluster is retried. On AWS the target region should be VPC-peered with the runner region with infra-prepared and AMI available; on GCE the global VPC and global images make any supported region eligible. Only applies during initial setup. Supported backends: AWS, GCE.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **force_run_iotune** / SCT_FORCE_RUN_IOTUNE

Force running iotune on the DB nodes, regardless if image has predefined values

**default:** N/A

**type:** bool


## **instance_provision** / SCT_INSTANCE_PROVISION

instance_provision: spot|on_demand|spot_fleet

**default:** spot

**type:** Literal['spot', 'on_demand', 'spot_fleet', 'spot_low_price']

**backend overrides:**
- `on_demand`: oci, k8s-gke, k8s-eks


## **instance_provision_fallback_on_demand** / SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND

instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected 'instance_provision' type creation failed. Expected values: true|false (default - false

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks


## **instance_type_db** / SCT_INSTANCE_TYPE_DB

AWS image type of the db node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `i4i.4xlarge`: k8s-eks


## **instance_type_db_oracle** / SCT_INSTANCE_TYPE_DB_ORACLE

AWS image type of the oracle node

**default:** N/A

**type:** str (appendable)


## **instance_type_db_target** / SCT_INSTANCE_TYPE_DB_TARGET

Target AWS instance type for platform migration (e.g., i8g.2xlarge for ARM)

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


## **instance_type_runner** / SCT_INSTANCE_TYPE_RUNNER

instance type of the sct-runner node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `c6i.2xlarge`: k8s-local-kind-aws
- `e2-standard-8`: k8s-local-kind-gce


## **instance_type_vector_store** / SCT_INSTANCE_TYPE_VECTOR_STORE

AWS/GCP cloud provider instance type for Vector Store nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `t4g.medium`: aws, aws-siren, k8s-local-kind-aws, k8s-eks
- `e2-medium`: gce, gce-siren, k8s-gke


## **intra_node_comm_public** / SCT_INTRA_NODE_COMM_PUBLIC

If True, all communication between nodes are via public addresses

**default:** N/A

**type:** bool


## **ip_ssh_connections** / SCT_IP_SSH_CONNECTIONS

Type of IP used to connect to machine instances.<br>This depends on whether you are running your tests from a machine inside<br>your cloud provider, where it makes sense to use 'private', or outside (use 'public')<br><br>Default: Use public IPs to connect to instances (public)<br>Use private IPs to connect to instances (private)<br>Use IPv6 IPs to connect to instances (ipv6)

**default:** private

**type:** Literal['public', 'private', 'ipv6']


## **keystore_backend** / SCT_KEYSTORE_BACKEND

Credential storage backend for KeyStore: 'secretsmanager' (default) or 's3' (legacy)

**default:** secretsmanager

**type:** Literal['s3', 'secretsmanager']


## **keystore_sm_prefix** / SCT_KEYSTORE_SM_PREFIX

AWS Secrets Manager secret name prefix when keystore_backend=secretsmanager (default: 'sct/')

**default:** sct/

**type:** str (appendable)


## **keystore_sm_region** / SCT_KEYSTORE_SM_REGION

AWS region holding the KeyStore secrets when keystore_backend=secretsmanager (default: 'us-east-1')

**default:** us-east-1

**type:** str (appendable)


## **latency_decorator_error_thresholds** / SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS

Error thresholds for latency decorator. Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}

**default:** {'write': {'default': {'P90 write': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}}}, 'read': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'read_disk_only': {'default': {'P90 read': {'fixed_limit': None}, 'P99 read': {'fixed_limit': 10}}}, 'mixed': {'default': {'P90 write': {'fixed_limit': None}, 'P90 read': {'fixed_limit': None}, 'P99 write': {'fixed_limit': 10}, 'P99 read': {'fixed_limit': 10}}}}

**type:** dict | YAML/JSON string → dict


## **n_db_nodes** / SCT_N_DB_NODES

Number list of database nodes in multiple data centers.

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]

**backend overrides:**
- `4`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks
- `3`: xcloud


## **n_db_zero_token_nodes** / SCT_N_DB_ZERO_TOKEN_NODES

Number of zero token nodes in cluster. Value should be set as '0 1 1' for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions

**default:** 0

**type:** int | list[int] | space-separated ints → list[int]


## **n_loaders** / SCT_N_LOADERS

Number list of loader nodes in multiple data centers

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]

**backend overrides:**
- `1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks, xcloud


## **n_monitor_nodes** / SCT_N_MONITOR_NODES

Number list of monitor nodes in multiple data centers

**default:** 1

**type:** int | list[int] | space-separated ints → list[int]

**backend overrides:**
- `0`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **parallel_node_operations** / SCT_PARALLEL_NODE_OPERATIONS

When defined true, will run node operations in parallel. Supported operations: startup

**default:** True

**type:** bool


## **pre_filter_unavailable_availability_zones** / SCT_PRE_FILTER_UNAVAILABLE_AVAILABILITY_ZONES

Filter availability zones upfront to only those that support all required instance types. Replaces invalid AZs with valid alternatives in the same region before any provisioning attempt. Supported backends: AWS, GCE.

**default:** True

**type:** bool


## **pre_flight_capacity_probe** / SCT_PRE_FLIGHT_CAPACITY_PROBE

Before provisioning, probe capacity by launching and terminating one on-demand instance per dynamic type (`instance_type_db_target`, `nemesis_grow_shrink_instance_type`) in the chosen AZ. On capacity errors, raise to trigger AZ/region fallback. Costs ~1 min per type. AWS-only.

**default:** False

**type:** bool


## **raid_level** / SCT_RAID_LEVEL

Number of of raid level: 0 - RAID0, 5 - RAID5

**default:** 0

**type:** int


## **region_name** / SCT_REGION_NAME

Cloud region(s) to run in. A space-separated list or YAML list provisions a multi-region cluster, one entry per datacenter. Despite the AWS-sounding default, this is the generic region option; GCE uses 'gce_datacenter' and Azure uses 'azure_region_name'.

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eu-west-1']`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **reuse_cluster** / SCT_REUSE_CLUSTER

If reuse_cluster is set it should hold test_id of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`

**default:** N/A

**type:** str (appendable)


## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **sct_public_ip** / SCT_SCT_PUBLIC_IP

Override the default hostname address of the sct test runner,<br>for the monitoring of the Nemesis.<br>can only work out of the box in AWS

**default:** N/A

**type:** str (appendable)


## **seeds_num** / SCT_SEEDS_NUM

Number of seeds to select

**default:** 1

**type:** int


## **seeds_selector** / SCT_SEEDS_SELECTOR

How to select the seeds. Expected values: random/first/all

**default:** all

**type:** Literal['random', 'first', 'all']


## **simulated_racks** / SCT_SIMULATED_RACKS

Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.<br>Provide number of racks to simulate. Takes effect only with more than one DB node: a<br>single-node cluster stays in one rack and `endpoint_snitch` is left alone. On the docker<br>backend the rack is passed to the image entrypoint as `--dc/--rack`, which requires Scylla<br>>= 2026.1; an older image fails the configuration, so set 1 to opt out.

**default:** 3

**type:** int

**backend overrides:**
- `0`: xcloud


## **simulated_regions** / SCT_SIMULATED_REGIONS

Number of simulated regions for the test

**default:** 0

**type:** Literal[0, 2, 3, 4, 5]


## **sizing_db** / SCT_SIZING_DB

Cloud-agnostic instance sizing constraints for db nodes

**default:** N/A

**type:** dict


## **sizing_db_oracle** / SCT_SIZING_DB_ORACLE

Cloud-agnostic instance sizing constraints for db_oracle nodes

**default:** {'vcpu': 8, 'memory': '>=60'}

**type:** dict


## **sizing_loader** / SCT_SIZING_LOADER

Cloud-agnostic instance sizing constraints for loader nodes

**default:** {'vcpu': 4, 'memory': '>=8'}

**type:** dict


## **sizing_monitor** / SCT_SIZING_MONITOR

Cloud-agnostic instance sizing constraints for monitor nodes

**default:** {'vcpu': 2, 'memory': '>=8'}

**type:** dict


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario

**default:** {}

**type:** dict | YAML/JSON string → dict


## **ssh_transport** / SCT_SSH_TRANSPORT

Set type of ssh library to use. Could be 'libssh2' (default) or 'fabric'

**default:** libssh2

**type:** Literal['libssh2', 'fabric']


## **test_duration** / SCT_TEST_DURATION

Test duration (min). Parameter used to keep instances produced by tests<br>and for jenkins pipeline timeout and TimoutThread.

**default:** 60

**type:** int


## **test_id** / SCT_TEST_ID

Set the test_id of the run manually. Use only from the env before running Hydra

**default:** N/A

**type:** str (appendable)


## **test_metadata** / SCT_TEST_METADATA

Structured metadata for test documentation and labeling. Validated by pydantic model. Flows to Argus.

**default:** N/A

**type:** sdcm.test_metadata.TestMetadata


## **test_method** / SCT_TEST_METHOD

class.method used to run the test. Filled automatically with run-test sct command.

**default:** N/A

**type:** str


## **use_dns_names** / SCT_USE_DNS_NAMES

Use dns names instead of ip addresses for nodes in cluster

**default:** False

**type:** bool


## **use_legacy_cluster_init** / SCT_USE_LEGACY_CLUSTER_INIT

Use legacy cluster initialization with autobootsrap disabled and parallel node setup

**default:** False

**type:** bool


## **use_zero_nodes** / SCT_USE_ZERO_NODES

If True, enable support in SCT of zero nodes (configuration, nemesis)

**default:** False

**type:** bool


## **user_credentials_path** / SCT_USER_CREDENTIALS_PATH

Path to the SSH private key SCT uses to reach the nodes it provisions. The QA key is fetched automatically from the KeyStore, so this rarely needs setting by hand.

**default:** N/A

**type:** str

**backend overrides:**
- `~/.ssh/scylla_test_id_ed25519`: aws, gce, azure, oci, docker, baremetal, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_prefix** / SCT_USER_PREFIX

the prefix of the name of the cloud instances, defaults to username

**default:** N/A

**type:** str (appendable)


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero-token DB nodes -- nodes that join the ring for reads/writes but own no token range. Falls back to 'instance_type_db' when unset.

**default:** N/A

**type:** str (appendable)


# Scylla installation and configuration


## **append_scylla_args** / SCT_APPEND_SCYLLA_ARGS

More arguments to append to scylla command line

**default:** --blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1

**type:** str (appendable)

**backend overrides:**
- `--abort-on-lsa-bad-alloc 1 --abort-on-internal-error 1 --abort-on-ebadf 1 --enable-sstable-key-validation 1`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **append_scylla_node_exporter_args** / SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS

More arguments to append to scylla-node-exporter command line

**default:** N/A

**type:** str (appendable)


## **append_scylla_setup_args** / SCT_APPEND_SCYLLA_SETUP_ARGS

More arguments to append to scylla_setup command line

**default:** N/A

**type:** str (appendable)


## **append_scylla_yaml** / SCT_APPEND_SCYLLA_YAML

More configuration to append to /etc/scylla/scylla.yaml

**default:** {'rf_rack_valid_keyspaces': True}

**type:** dict | str | pydantic.main.BaseModel


## **assert_linux_distro_features** / SCT_ASSERT_LINUX_DISTRO_FEATURES

List of distro features relevant to SCT test. Example: 'fips'.<br>This is used to assert that the distro features are supported by the scylla version being tested.<br>If the feature is not supported, the test will fail.

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **authenticator** / SCT_AUTHENTICATOR

which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator

**default:** N/A

**type:** Literal['PasswordAuthenticator', 'AllowAllAuthenticator', 'com.scylladb.auth.SaslauthdAuthenticator']


## **authenticator_password** / SCT_AUTHENTICATOR_PASSWORD

the password if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authenticator_user** / SCT_AUTHENTICATOR_USER

the username if PasswordAuthenticator is used

**default:** N/A

**type:** str (appendable)


## **authorizer** / SCT_AUTHORIZER

which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer

**default:** N/A

**type:** Literal['AllowAllAuthorizer', 'CassandraAuthorizer']


## **client_encrypt** / SCT_CLIENT_ENCRYPT

when enable scylla will use encryption on the client side

**default:** False

**type:** bool


## **client_encrypt_mtls** / SCT_CLIENT_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled

**default:** False

**type:** bool


## **db_type** / SCT_DB_TYPE

Db type to install into db nodes, scylla/cassandra

**default:** scylla

**type:** str (appendable)


## **enable_kms_key_rotation** / SCT_ENABLE_KMS_KEY_ROTATION

Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.

**default:** True

**type:** bool


## **endpoint_snitch** / SCT_ENDPOINT_SNITCH

The snitch class scylla would use<br><br>'GossipingPropertyFileSnitch' - default<br>'Ec2MultiRegionSnitch' - default on aws backend<br>'GoogleCloudSnitch'

**default:** N/A

**type:** str (appendable)


## **enterprise_disable_kms** / SCT_ENTERPRISE_DISABLE_KMS

An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up

**default:** False

**type:** bool


## **experimental_features** / SCT_EXPERIMENTAL_FEATURES

Scylla experimental features to enable in scylla.yaml, as a list of feature names (e.g. 'udf', 'alternator-streams').

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **hinted_handoff** / SCT_HINTED_HANDOFF

when enable or disable scylla hinted handoff (enabled/disabled)

**default:** disabled

**type:** str (appendable)


## **install_mode** / SCT_INSTALL_MODE

Scylla install mode, repo/offline/web

**default:** repo

**type:** str


## **internode_compression** / SCT_INTERNODE_COMPRESSION

Scylla `internode_compression` in scylla.yaml: which inter-node traffic to compress -- 'all', 'dc' (between datacenters only) or 'none'.

**default:** N/A

**type:** str (appendable)


## **internode_encryption** / SCT_INTERNODE_ENCRYPTION

Scylla sub option of server_encryption_options: internode_encryption.

**default:** all

**type:** str (appendable)


## **jmx_heap_memory** / SCT_JMX_HEAP_MEMORY

The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).

**default:** N/A

**type:** int


## **kms_key_rotation_interval** / SCT_KMS_KEY_ROTATION_INTERVAL

The time interval in minutes which gets waited before the KMS key rotation happens. Applied when the AWS KMS service is configured to be used.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws, gce, azure, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **ldap_server_type** / SCT_LDAP_SERVER_TYPE

This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]

**default:** N/A

**type:** str (appendable)


## **nonroot_offline_install** / SCT_NONROOT_OFFLINE_INSTALL

Install Scylla without required root privilege

**default:** N/A

**type:** bool


## **peer_verification** / SCT_PEER_VERIFICATION

enable peer verification for encrypted communication

**default:** True

**type:** bool


## **prepare_saslauthd** / SCT_PREPARE_SASLAUTHD

When defined true, will install and start saslauthd service

**default:** N/A

**type:** bool


## **scylla_apt_keys** / SCT_SCYLLA_APT_KEYS

APT keys for ScyllaDB repos

**default:** ['17723034C56D4B19', '5E08FBD8B5D6EC9C', 'D0A112E067426AB2', '491C93B9DE7496A7', 'A43E06657BAC99E3', 'C503C686B007F39E']

**type:** str | list[str] → list[str] (appendable)


## **scylla_d_overrides_files** / SCT_SCYLLA_D_OVERRIDES_FILES

list of files that should upload to /etc/scylla.d/ directory to override scylla config files

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **scylla_encryption_options** / SCT_SCYLLA_ENCRYPTION_OPTIONS

options will be used for enable encryption at-rest for tables

**default:** N/A

**type:** str (appendable)


## **scylla_linux_distro** / SCT_SCYLLA_LINUX_DISTRO

Distro and family for the DB node image, e.g. 'ubuntu-jammy' or 'debian-bookworm'.

**default:** ubuntu-focal

**type:** str

**backend overrides:**
- `centos`: docker


## **scylla_linux_distro_loader** / SCT_SCYLLA_LINUX_DISTRO_LOADER

Distro and family for the loader node image. Independent of the DB nodes, so loaders can run a different distro.

**default:** ubuntu-jammy

**type:** str


## **scylla_network_config** / SCT_SCYLLA_NETWORK_CONFIG

Configure Scylla networking with single or multiple NIC/IP combinations.<br>It must be defined for listen_address and rpc_address. For each address mandatory parameters are:<br>- address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication<br>- ip_type: ipv4 or ipv6<br>- public: false or true<br>- nic: number of NIC. 0, 1<br>Supported for AWS and GCE meanwhile

**default:** N/A

**type:** list

**backend overrides:**
- `[{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}, {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}]`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **scylla_repo** / SCT_SCYLLA_REPO

Url to the repo of scylla version to install scylla. Can provide specific version after a colon e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`

**default:** N/A

**type:** str (appendable)


## **scylla_version** / SCT_SCYLLA_VERSION

Version of scylla to install, ex. '2.3.1'<br>Automatically lookup AMIs and repo links for formal versions.<br>WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'

**default:** N/A

**type:** str

**backend overrides:**
- `6.2.3`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **server_encrypt** / SCT_SERVER_ENCRYPT

when enable scylla will use encryption on the server side

**default:** False

**type:** bool


## **server_encrypt_mtls** / SCT_SERVER_ENCRYPT_MTLS

when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled

**default:** False

**type:** bool


## **service_level_shares** / SCT_SERVICE_LEVEL_SHARES

List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]

**default:** [1000]

**type:** list


## **unified_package** / SCT_UNIFIED_PACKAGE

Url to the unified package of scylla version to install scylla

**default:** N/A

**type:** str (appendable)


## **update_db_packages** / SCT_UPDATE_DB_PACKAGES

A local directory of rpms to install a custom version on top of<br>the scylla installed (or from repo or from ami)

**default:** N/A

**type:** str (appendable)


## **use_ldap** / SCT_USE_LDAP

When defined true, LDAP is going to be used.

**default:** N/A

**type:** bool


## **use_ldap_authentication** / SCT_USE_LDAP_AUTHENTICATION

Authenticate Scylla users against LDAP: starts an LDAP container and sets scylla.yaml to use it for authentication (who you are).

**default:** N/A

**type:** bool


## **use_ldap_authorization** / SCT_USE_LDAP_AUTHORIZATION

Authorize Scylla users through LDAP group membership: starts an LDAP container and sets scylla.yaml to use it for authorization (what you may do).

**default:** N/A

**type:** bool


## **use_preinstalled_scylla** / SCT_USE_PREINSTALLED_SCYLLA

Don't install/update ScyllaDB on DB nodes

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks


## **user_data_format_version** / SCT_USER_DATA_FORMAT_VERSION

user-data format version to send to the DB node images. Defaults to whatever the image is tagged with; set it only to override that.

**default:** N/A

**type:** str


# Nemesis (chaos testing)


## **nemesis_add_node_cnt** / SCT_NEMESIS_ADD_NODE_CNT

Add/remove nodes during GrowShrinkCluster nemesis

**default:** 3

**type:** int


## **nemesis_class_name** / SCT_NEMESIS_CLASS_NAME

Nemesis class to use (possible types in sdcm.nemesis).<br>Supported syntax:<br>- nemesis_class_name: "NemesisName"<br>Run one nemesis in a single thread.<br>- nemesis_class_name: ["NemesisA", "NemesisB"]<br>Run NemesisA and NemesisB each in their own thread.<br>- nemesis_class_name: ["SisyphusMonkey", "SisyphusMonkey"]<br>Run two SisyphusMonkey threads in parallel.<br>Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and<br>space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no<br>longer supported. Use an explicit YAML list instead.

**default:** NoOpMonkey

**type:** str | list[str] → list[str] (appendable)


## **nemesis_double_load_during_grow_shrink_duration** / SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION

After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.

**default:** 0

**type:** int


## **nemesis_during_prepare** / SCT_NEMESIS_DURING_PREPARE

Run nemesis during prepare stage of the test

**default:** True

**type:** bool


## **nemesis_filter_seeds** / SCT_NEMESIS_FILTER_SEEDS

If true runs the nemesis only on non seed nodes

**default:** False

**type:** bool


## **nemesis_grow_shrink_instance_type** / SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE

Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis

**default:** N/A

**type:** str (appendable)


## **nemesis_interval** / SCT_NEMESIS_INTERVAL

Nemesis sleep interval to use if None provided specifically in the test

**default:** 5

**type:** int


## **nemesis_multiply_factor** / SCT_NEMESIS_MULTIPLY_FACTOR

Multiply the list of nemesis to execute by the specified factor

**default:** 2

**type:** int


## **nemesis_seed** / SCT_NEMESIS_SEED

A seed number in order to repeat nemesis sequence as part of SisyphusMonkey

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **nemesis_selector** / SCT_NEMESIS_SELECTOR

nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has<br>ALL the properties in that list which are set to true (the intersection of all properties).<br>(In other words filters out all nemesis that doesn't ONE of these properties set to true)<br>IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **nemesis_sequence_sleep_between_ops** / SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS

Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests

**default:** N/A

**type:** int


## **sla** / SCT_SLA

run SLA nemeses if the test is SLA only

**default:** N/A

**type:** bool


# Stress commands and load generation


## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** False

**type:** bool


## **alternator_stress_rate** / SCT_ALTERNATOR_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing.

**default:** N/A

**type:** int


## **alternator_write_always_lwt_stress_rate** / SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.

**default:** N/A

**type:** int


## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** False

**type:** bool


## **batch_size** / SCT_BATCH_SIZE

Number of rows per batch for the stress commands that write in batches.

**default:** 1

**type:** int


## **c_s_driver_version** / SCT_C_S_DRIVER_VERSION

cassandra-stress driver version to use: 3|4|random

**default:** 3

**type:** Literal['3', '4', 'random']


## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** bool


## **cs_duration** / SCT_CS_DURATION

Duration passed to cassandra-stress, e.g. '50m'. Overrides any duration in the command itself.

**default:** 50m

**type:** str (appendable)


## **cs_extra_jvm_opts** / SCT_CS_EXTRA_JVM_OPTS

Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' (requires Java 21+, which cassandra-stress 3.20.6+ ships with).

**default:** N/A

**type:** str (appendable)


## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** str (appendable)


## **cs_safepoint_logging** / SCT_CS_SAFEPOINT_LOGGING

Enable JVM safepoint logging (-Xlog:safepoint) for the cassandra-stress loaders. The log is written on the loader host, pulled into the loader log directory and collected into the run log archive. Use it to tell a loader JVM pause (including non-GC safepoints) apart from a server-side or network stall behind a latency-step failure. Not supported for k8s backends and prepared loaders.

**default:** False

**type:** bool


## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **effective_compression_ratio** / SCT_EFFECTIVE_COMPRESSION_RATIO

Effective compression ratio used for Jinja stress command templating. Defined as on_disk_bytes / logical_uncompressed_bytes. This estimates how much disk space Scylla uses after compression relative to the logical uncompressed dataset size. For example, 1.0 means no effective compression and 0.68 means the data is expected to occupy about 68% of its logical uncompressed size on disk. Used together with the effective_disk_size_bytes template variable to calculate row counts that fill a target fraction of available disk capacity. You can estimate this ratio from Grafana in Keyspace -> Compression metrics; a compression value of 0% corresponds to effective_compression_ratio=1.0. Must be in range (0, 1.0].

**default:** 1.0

**type:** float


## **gemini_cmd** / SCT_GEMINI_CMD

gemini command to run (for now used only in GeminiTest)

**default:** N/A

**type:** str (appendable)


## **gemini_log_cql_statements** / SCT_GEMINI_LOG_CQL_STATEMENTS

Log CQL statements to file

**default:** N/A

**type:** bool


## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.<br>Local files are uploaded to the loader via send_files and mounted into the Gemini Docker<br>container via --schema.<br>Remote URLs are downloaded on the loader node with curl and then mounted the same way.

**default:** N/A

**type:** str (appendable)


## **gemini_seed** / SCT_GEMINI_SEED

Seed number for gemini command

**default:** N/A

**type:** int


## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']

**default:** N/A

**type:** list


## **keyspace_num** / SCT_KEYSPACE_NUM

Number of keyspaces to use in the test

**default:** 1

**type:** int


## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.<br>Also used as a fallback source for keyspace/table in gradual performance tests when<br>perf_stress_keyspace/perf_stress_table are not set.<br>For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}

**default:** {}

**type:** dict | YAML/JSON string → dict


## **loader_swap_size** / SCT_LOADER_SWAP_SIZE

The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** []

**type:** str | list[str] → list[str] (appendable)


## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

Stress command(s) run in the prepare phase, alongside 'prepare_write_cmd'. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

Stress command(s) that verify the pre-loaded dataset before the test proper. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_wait_no_compactions_timeout** / SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT

Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load

**default:** N/A

**type:** int


## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

Stress command(s) that pre-load the dataset before the test's own load starts. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **rack_aware_loader** / SCT_RACK_AWARE_LOADER

When enabled, loaders will look for nodes on the same rack.

**default:** False

**type:** bool


## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** False

**type:** bool


## **round_robin** / SCT_ROUND_ROBIN

Enable or disable round robin selection of nodes for operations

**default:** False

**type:** bool


## **stop_test_on_stress_failure** / SCT_STOP_TEST_ON_STRESS_FAILURE

If set to True the test will be stopped immediately when stress command failed.<br>When set to False the test will continue to run even when there are errors in the<br>stress process

**default:** True

**type:** bool


## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** False

**type:** bool


## **stress_before_migration** / SCT_STRESS_BEFORE_MIGRATION

Stress command to write data for post-migration validation

**default:** N/A

**type:** str (appendable)


## **stress_cdc_log_reader_batching_enable** / SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE

retrieving data from multiple streams in one poll

**default:** True

**type:** bool


## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


## **stress_cmd** / SCT_STRESS_CMD

The test's main stress command(s). Everything except '-node' can be set; SCT fills in the node list. Accepts a single command or a list, one per loader thread.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARMUP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

Stress command(s) that delete rows in the complex-schema data validation flow. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_m** / SCT_STRESS_CMD_M

Mixed read/write stress command(s). See 'stress_cmd' for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_mv** / SCT_STRESS_CMD_MV

Stress command(s) for the leg of the test that runs with materialized views. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

Stress command(s) for the leg of the test that runs without materialized views, so the MV overhead can be compared. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE

cassandra-stress user profile (YAML) for the no-materialized-view leg of the test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_r** / SCT_STRESS_CMD_R

Read-only stress command(s). See 'stress_cmd' for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_read_disk** / SCT_STRESS_CMD_READ_DISK

Read stress command(s) sized to miss the cache and read from disk. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_cmd_w** / SCT_STRESS_CMD_W

Write-only stress command(s). See 'stress_cmd' for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from stress_cmd parameters<br>and is used in test duration calculation

**default:** 0

**type:** int


## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** {}

**type:** dict | YAML/JSON string → dict


## **stress_multiplier** / SCT_STRESS_MULTIPLIER

Multiplier for stress command intensity

**default:** 1

**type:** int


## **stress_multiplier_m** / SCT_STRESS_MULTIPLIER_M

Mixed operations stress command intensity multiplier

**default:** 1

**type:** int


## **stress_multiplier_r** / SCT_STRESS_MULTIPLIER_R

Multiplies the thread count of every read stress command, to scale read load without editing each command.

**default:** 1

**type:** int


## **stress_multiplier_w** / SCT_STRESS_MULTIPLIER_W

Multiplies the thread count of every write stress command, to scale write load without editing each command.

**default:** 1

**type:** int


## **stress_read_cmd** / SCT_STRESS_READ_CMD

Read stress command(s) run in the verification phase. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_template_context** / SCT_STRESS_TEMPLATE_CONTEXT

Shared runtime-only Jinja variables for stress command templating. Entries are resolved in declaration order and may reference earlier context entries as well as built-in stress template variables such as effective_disk_size_bytes and db_node_count_per_dc. These values are available to stress commands rendered by SCT, but are not evaluated during config load or validation.

**default:** {}

**type:** dict | YAML/JSON string → dict


## **use_prepared_loaders** / SCT_USE_PREPARED_LOADERS

If True, we use prepared VMs for loader (instead of using docker images)

**default:** N/A

**type:** bool


## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

Number of user profile tables to create for the test

**default:** 1

**type:** int


# Monitoring, events and reporting


## **argus_email_report_template** / SCT_ARGUS_EMAIL_REPORT_TEMPLATE

Path to the email report template used for sending argus email reports

**default:** email_report_template_basic.yaml

**type:** str (appendable)


## **argus_use_ssh_tunnel** / SCT_ARGUS_USE_SSH_TUNNEL

Enable SSH tunnel support in the Argus client connection

**default:** True

**type:** bool


## **backtrace_decoding** / SCT_BACKTRACE_DECODING

If True, all backtraces found in db nodes would be decoded automatically

**default:** True

**type:** bool

**backend overrides:**
- `False`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce


## **backtrace_decoding_disable_regex** / SCT_BACKTRACE_DECODING_DISABLE_REGEX

Regex pattern to disable backtrace decoding for specific event types. If an event type matches<br>this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests<br>by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.

**default:** N/A

**type:** str (appendable)


## **backtrace_stall_decoding** / SCT_BACKTRACE_STALL_DECODING

If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during<br>backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.

**default:** True

**type:** bool


## **download_from_s3** / SCT_DOWNLOAD_FROM_S3

Destination-source map of dirs/buckets to download from S3 before starting the test

**default:** []

**type:** list


## **email_recipients** / SCT_EMAIL_RECIPIENTS

list of email of send the performance regression test to

**default:** ['qa@scylladb.com']

**type:** str | list[str] → list[str] (appendable)


## **email_subject_postfix** / SCT_EMAIL_SUBJECT_POSTFIX

Text appended to the subject of the test result email, to tell similar runs apart.

**default:** N/A

**type:** str (appendable)


## **enable_argus** / SCT_ENABLE_ARGUS

Control reporting to argus

**default:** True

**type:** bool


## **enable_kernel_panic_checker** / SCT_ENABLE_KERNEL_PANIC_CHECKER

Enable kernel panic detection by monitoring cloud instance console output for panic indicators. When enabled, a background thread monitors each node's console output for kernel panic patterns.

**default:** True

**type:** bool


## **events_limit_in_email** / SCT_EVENTS_LIMIT_IN_EMAIL

Maximum number of events of each severity to include in the email report.

**default:** 10

**type:** int


## **max_events_severities** / SCT_MAX_EVENTS_SEVERITIES

Limit severity level for event types

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_branch** / SCT_MONITOR_BRANCH

The port of scylla management

**default:** branch-4.16

**type:** str (appendable)

**backend overrides:**
- `N/A`: aws, gce, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **monitor_swap_size** / SCT_MONITOR_SWAP_SIZE

The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


## **print_kernel_callstack** / SCT_PRINT_KERNEL_CALLSTACK

Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message<br>that it failed to.

**default:** True

**type:** bool

**backend overrides:**
- `False`: docker


## **sct_ngrok_name** / SCT_SCT_NGROK_NAME

DEPRECATED (see SCT-954, unused for years): expose the SCT runner under this ngrok hostname instead of its own address.

**default:** N/A

**type:** str (appendable)


## **scylla_rsyslog_setup** / SCT_SCYLLA_RSYSLOG_SETUP

Configure rsyslog on Scylla nodes to send logs to monitoring nodes

**default:** False

**type:** bool


# Logs, diagnostics and teardown


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** False

**type:** bool


## **collect_nvme_diagnostics** / SCT_COLLECT_NVME_DIAGNOSTICS

Collect NVMe SMART logs, error logs, and self-test results from DB nodes during test teardown. Requires nvme-cli to be installed on the nodes. Skipped gracefully on backends without NVMe devices.

**default:** False

**type:** bool


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** False

**type:** bool


## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** vector

**type:** Literal['ssh', 'docker', 'syslog-ng', 'vector']

**backend overrides:**
- `docker`: docker


## **nvme_self_test_type** / SCT_NVME_SELF_TEST_TYPE

NVMe device self-test type to run: 1 (short, ~2 min) or 2 (extended, may take hours). Only used when collect_nvme_diagnostics is enabled.

**default:** 1

**type:** int


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_dedicated_host** / SCT_POST_BEHAVIOR_DEDICATED_HOST

Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.<br><br>'destroy' - Destroy hosts (default)<br>'keep' - Keep hosts allocated

**default:** N/A

**type:** Literal['keep', 'destroy']

**backend overrides:**
- `destroy`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **post_behavior_emr_cluster** / SCT_POST_BEHAVIOR_EMR_CLUSTER

Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.<br><br>'destroy' - Destroy EMR cluster (default)<br>'keep' - Keep EMR cluster running<br>'keep-on-failure' - Keep EMR cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_vector_store_nodes** / SCT_POST_BEHAVIOR_VECTOR_STORE_NODES

Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


## **run_scylla_doctor_only** / SCT_RUN_SCYLLA_DOCTOR_ONLY

When true, the artifact test runs only the Scylla Doctor validation<br>(install, collect vitals, analyze, verify) and skips all other artifact checks<br>such as stop/start, cassandra-stress, etc. Useful for fast SD<br>release gating. Implies run_scylla_doctor=true.

**default:** False

**type:** bool


## **scylla_doctor_edition** / SCT_SCYLLA_DOCTOR_EDITION

Scylla Doctor edition to use. Allowed values: 'basic', 'full'.<br>'basic' fetches the free/open-source edition via HTTP.<br>'full' fetches the full/enterprise edition from a private S3 bucket.

**default:** basic

**type:** Literal['basic', 'full']


## **scylla_doctor_full_tarball_url** / SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL

Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the<br>standard version-based S3 lookup and downloads SD directly from this URL.<br>Use for testing unofficial or pre-release SD versions.<br>Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'

**default:** N/A

**type:** str (appendable)


## **scylla_doctor_version** / SCT_SCYLLA_DOCTOR_VERSION

Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')<br>to hardcode the version, or leave empty to use the latest available version. For stability,<br>artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.

**default:** 1.13

**type:** str (appendable)


## **teardown_validators** / SCT_TEARDOWN_VALIDATORS

Validators to use during teardown phase

**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}, 'nvme': {'enabled': False}}

**type:** dict | YAML/JSON string → dict


## **use_scylla_doctor_on_failure** / SCT_USE_SCYLLA_DOCTOR_ON_FAILURE

Run scylla-doctor on test failure to collect additional diagnostics

**default:** True

**type:** bool


# Scylla Manager


## **backup_bucket_backend** / SCT_BACKUP_BUCKET_BACKEND

the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `s3`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-gke, k8s-eks
- `gcs`: gce, gce-siren
- `azure`: azure


## **backup_bucket_location** / SCT_BACKUP_BUCKET_LOCATION

the bucket name to be used for backup (e.g., 'manager-backup-tests')

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `manager-backup-tests-{region}`: aws, aws-siren, k8s-eks
- `manager-backup-tests-sct-project-1-us-east1`: gce, gce-siren
- `manager-backup-tests-us-east-1`: azure
- `minio-bucket`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke


## **backup_bucket_region** / SCT_BACKUP_BUCKET_REGION

the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')

**default:** N/A

**type:** str (appendable)


## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


## **manager_prometheus_port** / SCT_MANAGER_PROMETHEUS_PORT

Port to be used by the manager to contact Prometheus

**default:** 5090

**type:** int


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Version of ScyllaDB to install as Manager backend

**default:** 2025.4

**type:** str


## **manager_version** / SCT_MANAGER_VERSION

Version of Scylla Manager server and agent to install

**default:** 3.12

**type:** str


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.12.0

**type:** str (appendable)


## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** str (appendable)


## **mgmt_prepare_snapshot_size** / SCT_MGMT_PREPARE_SNAPSHOT_SIZE

Size of backup snapshot in Gb to be prepared for backup

**default:** N/A

**type:** int


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


## **mgmt_snapshots_preparer_params** / SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS

Custom parameters of c-s write operation used in snapshots preparer

**default:** {'cs_cmd_template': "cassandra-stress {operation} cl={cl} n={num_of_rows} -schema 'keyspace={ks_name} replication(strategy={replication},replication_factor={rf}) compaction(strategy={compaction})' -mode cql3 native -rate threads={threads_num} -col 'size=FIXED({col_size}) n=FIXED({col_n})' -pop seq={sequence_start}..{sequence_end}", 'operation': 'write', 'cl': 'QUORUM', 'replication': 'NetworkTopologyStrategy', 'rf': 3, 'compaction': 'IncrementalCompactionStrategy', 'threads_num': 500, 'col_size': 1024, 'col_n': 1, 'ks_name': '', 'num_of_rows': '', 'sequence_start': '', 'sequence_end': ''}

**type:** dict | YAML/JSON string → dict


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION

Version of Scylla Manager agent to install for management tests

**default:** 3.12.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)


## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Version of Scylla Manager server and agent to upgrade to

**default:** N/A

**type:** str


## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** str (appendable)


## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** str (appendable)


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** False

**type:** bool


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool


# Auxiliary DB cluster (oracle / Cassandra)


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str (appendable)


## **cassandra_broadcast_rpc_public** / SCT_CASSANDRA_BROADCAST_RPC_PUBLIC

When True, set broadcast_rpc_address to the public IP of the node in cassandra.yaml, so clients outside the VPC (e.g. sct-runner driver connection that reads system.peers) can reach the nodes. Defaults to False (private IP, matches intra-VPC behavior).

**default:** N/A

**type:** bool


## **cassandra_num_tokens** / SCT_CASSANDRA_NUM_TOKENS

num_tokens value to configure in cassandra.yaml.

**default:** 16

**type:** int


## **cassandra_oracle_version** / SCT_CASSANDRA_ORACLE_VERSION

Cassandra version for the oracle cluster, i.e. '4.1' or '5.0'

**default:** N/A

**type:** str (appendable)


## **cassandra_version** / SCT_CASSANDRA_VERSION

Cassandra version / docker image tag, i.e. '4.1' or '5.0'

**default:** 4.1

**type:** str (appendable)


## **docker_image_cassandra** / SCT_DOCKER_IMAGE_CASSANDRA

Cassandra docker image repo, i.e. 'cassandra'. Used when db_type is 'cassandra'.

**default:** cassandra

**type:** str (appendable)


## **install_cassandra_exporter** / SCT_INSTALL_CASSANDRA_EXPORTER

Install Criteo cassandra_exporter on Cassandra nodes for Prometheus metrics collection. The exporter connects to JMX (port 7199) and exposes metrics on port 8080.

**default:** True

**type:** bool


## **n_test_oracle_db_nodes** / SCT_N_TEST_ORACLE_DB_NODES

Number list of oracle test nodes in multiple data centers.

**default:** 1

**type:** int | list[int] | space-separated ints → list[int]


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically looks up cloud images for formal versions.<br>WARNING: can't be used together with the backend's oracle image param<br>('ami_id_db_oracle', 'gce_image_db_oracle', 'azure_image_db_oracle' or 'oci_image_db_oracle')

**default:** 2026.1

**type:** str


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Same as 'user_data_format_version', but for the auxiliary oracle cluster's images.

**default:** N/A

**type:** str


# Alternator (DynamoDB API)


## **alternator_access_key_id** / SCT_ALTERNATOR_ACCESS_KEY_ID

the aws_access_key_id that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_enforce_authorization** / SCT_ALTERNATOR_ENFORCE_AUTHORIZATION

If true, enable the authorization check in dynamodb api (alternator)

**default:** False

**type:** bool


## **alternator_loadbalancing** / SCT_ALTERNATOR_LOADBALANCING

If true, enable native load balancing for alternator

**default:** False

**type:** bool


## **alternator_port** / SCT_ALTERNATOR_PORT

Port to configure for alternator in scylla.yaml

**default:** N/A

**type:** int


## **alternator_secret_access_key** / SCT_ALTERNATOR_SECRET_ACCESS_KEY

the aws_secret_access_key that would be used for alternator

**default:** N/A

**type:** str (appendable)


## **alternator_test_table** / SCT_ALTERNATOR_TEST_TABLE

Dictionary of a test alternator table features:<br>name: str - the name of the table<br>lsi_name: str - the name of the local secondary index to create with a table<br>gsi_name: str - the name of the global secondary index to create with a table<br>tags: dict - the tags to apply to the created table<br>items: int - expected number of items in the table after prepare

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **alternator_trust_all_certificates** / SCT_ALTERNATOR_TRUST_ALL_CERTIFICATES

If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)

**default:** True

**type:** bool


## **alternator_use_dns_routing** / SCT_ALTERNATOR_USE_DNS_ROUTING

If true, spawn a docker with a dns server for the ycsb loader to point to

**default:** True

**type:** bool


## **alternator_write_isolation** / SCT_ALTERNATOR_WRITE_ISOLATION

Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details

**default:** N/A

**type:** str (appendable)


## **dynamodb_primarykey_type** / SCT_DYNAMODB_PRIMARYKEY_TYPE

Type of dynamodb table to create with range key or not

**default:** HASH

**type:** Literal['HASH', 'HASH_AND_RANGE']


# Vector Store


## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** 0

**type:** int


## **vector_store_docker_image** / SCT_VECTOR_STORE_DOCKER_IMAGE

Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version

**default:** scylladb/vector-store

**type:** str (appendable)


## **vector_store_port** / SCT_VECTOR_STORE_PORT

TCP port the Vector Store service listens on for its API.

**default:** 6080

**type:** int


## **vector_store_scylla_port** / SCT_VECTOR_STORE_SCYLLA_PORT

ScyllaDB connection port for Vector Store

**default:** 9042

**type:** int


## **vector_store_threads** / SCT_VECTOR_STORE_THREADS

Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)

**default:** 0

**type:** int


## **vector_store_version** / SCT_VECTOR_STORE_VERSION

Vector Store version / docker image tag

**default:** N/A

**type:** str (appendable)


# Kafka / CDC connectors


## **kafka_backend** / SCT_KAFKA_BACKEND

Type of Kafka backend to use

**default:** N/A

**type:** Literal['localstack', 'vm', 'msk']


## **kafka_connectors** / SCT_KAFKA_CONNECTORS

Kafka Connect connector definitions to deploy, as a list of config dicts -- typically the Scylla CDC source connector.

**default:** []

**type:** list[sdcm.kafka.kafka_config.SctKafkaConfiguration]


# AWS backend


## **ami_db_cassandra_user** / SCT_AMI_DB_CASSANDRA_USER

SSH login user baked into the Cassandra AMI, for the auxiliary cluster.

**default:** N/A

**type:** str (appendable)


## **ami_db_scylla_user** / SCT_AMI_DB_SCYLLA_USER

SSH login user baked into the DB node AMI (e.g. 'centos', 'ubuntu').

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_db_cassandra** / SCT_AMI_ID_DB_CASSANDRA

AMS AMI id to use for cassandra node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_oracle** / SCT_AMI_ID_DB_ORACLE

AMS AMI id to use for oracle node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_vector_store** / SCT_AMI_ID_VECTOR_STORE

AMS AMI id to use for vector store node

**default:** N/A

**type:** str (appendable)


## **ami_loader_user** / SCT_AMI_LOADER_USER

SSH login user baked into the loader AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_monitor_user** / SCT_AMI_MONITOR_USER

SSH login user baked into the monitoring node AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_vector_store_user** / SCT_AMI_VECTOR_STORE_USER

SSH login user baked into the Vector Store AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `[]`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Deprecated alias of `fallback_to_next_availability_zone`. Kept for backward compatibility.

**default:** False

**type:** bool


## **aws_instance_profile_name_db** / SCT_AWS_INSTANCE_PROFILE_NAME_DB

This is the name of the instance profile to set on all db instances

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-scylla-manager-backup-instance-profile`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_instance_profile_name_loader** / SCT_AWS_INSTANCE_PROFILE_NAME_LOADER

This is the name of the instance profile to set on all loader instances

**default:** N/A

**type:** str (appendable)


## **extra_network_interface** / SCT_EXTRA_NETWORK_INTERFACE

if true, create extra network interface on each node

**default:** N/A

**type:** bool


## **root_disk_size_db** / SCT_ROOT_DISK_SIZE_DB

Root (boot) disk size in GB for the DB nodes.

**default:** N/A

**type:** int

**backend overrides:**
- `30`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks
- `50`: gce, gce-siren, k8s-gke


## **root_disk_size_loader** / SCT_ROOT_DISK_SIZE_LOADER

Root (boot) disk size in GB for the loader nodes.

**default:** N/A

**type:** int

**backend overrides:**
- `20`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **root_disk_size_monitor** / SCT_ROOT_DISK_SIZE_MONITOR

Root (boot) disk size in GB for the monitoring node.

**default:** N/A

**type:** int

**backend overrides:**
- `50`: aws, gce, azure, oci, aws-siren, gce-siren, k8s-local-kind-aws, k8s-gke, k8s-eks


## **root_disk_size_runner** / SCT_ROOT_DISK_SIZE_RUNNER

root disk size in Gb for sct-runner

**default:** N/A

**type:** int

**backend overrides:**
- `140`: k8s-local-kind-aws, k8s-local-kind-gce


## **spot_max_price** / SCT_SPOT_MAX_PRICE

The max percentage of the on demand price we set for spot/fleet instances

**default:** N/A

**type:** float


## **use_capacity_reservation** / SCT_USE_CAPACITY_RESERVATION

Flag to use capacity reservation for instances

**default:** False

**type:** bool


## **use_dedicated_host** / SCT_USE_DEDICATED_HOST

Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)

**default:** False

**type:** bool


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** False

**type:** bool


# GCE backend


## **gce_datacenter** / SCT_GCE_DATACENTER

Supported regions: us-east1, us-east4, us-west1, us-central1. Specifying just the region (e.g., us-east1) means the zone will be selected automatically, or you can mention the zone explicitly (e.g., us-east1-b)

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `us-east1`: gce, gce-siren, k8s-gke


## **gce_image_db** / SCT_GCE_IMAGE_DB

gce image to use for db nodes

**default:** N/A

**type:** str (appendable)


## **gce_image_db_oracle** / SCT_GCE_IMAGE_DB_ORACLE

GCE image to use for oracle (2nd ref cluster) DB node(s). If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **gce_image_loader** / SCT_GCE_IMAGE_LOADER

Google Compute Engine image to use for loader nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2604-lts-amd64`: gce, gce-siren, k8s-gke


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR

gce image to use for monitor nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: gce, gce-siren, k8s-gke


## **gce_image_username** / SCT_GCE_IMAGE_USERNAME

Username for the Google Compute Engine image

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylla-test`: gce, gce-siren, k8s-gke


## **gce_instance_type_db** / SCT_GCE_INSTANCE_TYPE_DB

Instance type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `n2-standard-8`: k8s-gke


## **gce_instance_type_db_oracle** / SCT_GCE_INSTANCE_TYPE_DB_ORACLE

Instance type for the oracle (2nd ref cluster) DB nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_instance_type_loader** / SCT_GCE_INSTANCE_TYPE_LOADER

Instance type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `e2-standard-4`: k8s-gke


## **gce_instance_type_monitor** / SCT_GCE_INSTANCE_TYPE_MONITOR

Instance type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `e2-medium`: k8s-gke


## **gce_n_local_ssd_disk_db** / SCT_GCE_N_LOCAL_SSD_DISK_DB

Number of local SSD disks for database nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `4`: gce, gce-siren, k8s-gke


## **gce_n_local_ssd_disk_loader** / SCT_GCE_N_LOCAL_SSD_DISK_LOADER

Number of local SSD disks for loader nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_n_local_ssd_disk_monitor** / SCT_GCE_N_LOCAL_SSD_DISK_MONITOR

Number of local SSD disks for monitor nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_network** / SCT_GCE_NETWORK

GCP VPC network the instances are attached to.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-vpc`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_db** / SCT_GCE_PD_SSD_DISK_SIZE_DB

Size in GB of the persistent SSD disk attached to each DB node.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_loader** / SCT_GCE_PD_SSD_DISK_SIZE_LOADER

Size in GB of the persistent SSD disk attached to each loader.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_monitor** / SCT_GCE_PD_SSD_DISK_SIZE_MONITOR

Size in GB of the persistent SSD disk attached to the monitoring node.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_standard_disk_size_db** / SCT_GCE_PD_STANDARD_DISK_SIZE_DB

The size of the standard persistent disk in GB used for GCE database nodes

**default:** 0

**type:** int


## **gce_project** / SCT_GCE_PROJECT

GCP project that owns the provisioned resources.

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB

Root disk type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-ssd`: gce, gce-siren, k8s-gke


## **gce_root_disk_type_loader** / SCT_GCE_ROOT_DISK_TYPE_LOADER

Root disk type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-standard`: gce, gce-siren, k8s-gke


## **gce_root_disk_type_monitor** / SCT_GCE_ROOT_DISK_TYPE_MONITOR

Root disk type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-standard`: gce, gce-siren, k8s-gke


## **gce_setup_hybrid_raid** / SCT_GCE_SETUP_HYBRID_RAID

If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: gce, gce-siren, k8s-gke


# Azure backend


## **azure_image_db** / SCT_AZURE_IMAGE_DB

The Azure image to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_image_db_oracle** / SCT_AZURE_IMAGE_DB_ORACLE

The Azure image to be used for oracle (2nd ref cluster) DB nodes. If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-26_04-lts:server:latest`: azure


## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR

The Azure image to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-24_04-lts:server:latest`: azure


## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME

The username for the Azure image.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: azure


## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB

The Azure virtual machine size to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE

The Azure virtual machine size to be used for Oracle database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER

The Azure virtual machine size to be used for loader nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR

The Azure virtual machine size to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)


## **azure_provision_stuck_vm_recreate_attempts** / SCT_AZURE_PROVISION_STUCK_VM_RECREATE_ATTEMPTS

How many times to recreate a stuck Azure VM (full node: VM, NIC and public IP) onto<br>fresh capacity before giving up with a non-retryable error.

**default:** N/A

**type:** int

**backend overrides:**
- `3`: azure


## **azure_provision_stuck_vm_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TIMEOUT

Seconds to wait for an Azure VM to reach the 'Succeeded' provisioning state before<br>treating it as stuck (accepted by Azure but never started by the host - SCT-434) and<br>recreating it. Detection is gated on the polled instanceView provisioning state.

**default:** N/A

**type:** int

**backend overrides:**
- `900`: azure


## **azure_provision_stuck_vm_total_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TOTAL_TIMEOUT

Total timeout (seconds) for the whole stuck-VM recovery attempts.<br>Recovery stops with a non-retryable error when either this timeout or<br>'azure_provision_stuck_vm_recreate_attempts' is exhausted. This way a degraded Azure<br>region cannot keep provisioning running until the CI stage times out SCT.<br>This value must be at least 'azure_provision_stuck_vm_timeout', otherwise SCT may<br>give up during the initial wait without making even one recreate attempt.

**default:** N/A

**type:** int

**backend overrides:**
- `4500`: azure


## **azure_region_name** / SCT_AZURE_REGION_NAME

Azure region(s) where the resources will be deployed. Supports single or multiple regions.

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eastus']`: azure


# OCI backend


## **oci_image_db** / SCT_OCI_IMAGE_DB

Oracle Cloud image to use for DB node(s)

**default:** N/A

**type:** str (appendable)


## **oci_image_db_oracle** / SCT_OCI_IMAGE_DB_ORACLE

Oracle Cloud image to use for oracle (2nd ref cluster) DB node(s). If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **oci_image_loader** / SCT_OCI_IMAGE_LOADER

Oracle Cloud image to use for the loader node(s). Empty value results into latest ubuntu image

**default:** N/A

**type:** str (appendable)


## **oci_image_monitor** / SCT_OCI_IMAGE_MONITOR

Oracle Cloud image to use for the monitor node. Empty value results into latest ubuntu image

**default:** N/A

**type:** str (appendable)


## **oci_image_username** / SCT_OCI_IMAGE_USERNAME

Username used in the Oracle Cloud images utilized by the DB node(s)

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: oci


## **oci_instance_type_db** / SCT_OCI_INSTANCE_TYPE_DB

Oracle Cloud instance shape to use for DB node(s). Usage of flex shapes allows setting of the ocpus, memory and nvme disks. Format is following: <shape-name>:<ocpus>:<ram>:<nvmes> . For DenseIO shapes it makes sense to specify only 'ocpus' part, because ram and amount of NVMe disks will be fixed based on the OCPUs count.

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_db_oracle** / SCT_OCI_INSTANCE_TYPE_DB_ORACLE

Oracle Cloud instance shape to use for 'oracle' (2nd ref cluster) ScylladbDB cluster

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_loader** / SCT_OCI_INSTANCE_TYPE_LOADER

Oracle Cloud instance shape to use for loader node(s). Usage of flex shapes allows setting of the ocpus, memory. Format is following: <shape-name>:<ocpus>:<ram>

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_monitor** / SCT_OCI_INSTANCE_TYPE_MONITOR

Oracle Cloud instance shape to use for monitor node. Usage of flex shapes allows setting of the ocpus, memory. Format is following: <shape-name>:<ocpus>:<ram>

**default:** N/A

**type:** str (appendable)


## **oci_region_name** / SCT_OCI_REGION_NAME

OCI region where the resources will be deployed

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['us-phoenix-1']`: oci


# Kubernetes backends (EKS/GKE/kind)


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

Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when 'k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.

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


# Docker backend


## **docker_image** / SCT_DOCKER_IMAGE

Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version

**default:** N/A

**type:** str (appendable)


## **docker_network** / SCT_DOCKER_NETWORK

Local docker network to use, if there's need to have db cluster connect to other services running in docker

**default:** N/A

**type:** str (appendable)


# Baremetal backend


## **db_nodes_private_ip** / SCT_DB_NODES_PRIVATE_IP

Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **db_nodes_public_ip** / SCT_DB_NODES_PUBLIC_IP

Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **loaders_private_ip** / SCT_LOADERS_PRIVATE_IP

Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **loaders_public_ip** / SCT_LOADERS_PUBLIC_IP

Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_nodes_private_ip** / SCT_MONITOR_NODES_PRIVATE_IP

Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **monitor_nodes_public_ip** / SCT_MONITOR_NODES_PUBLIC_IP

Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **s3_baremetal_config** / SCT_S3_BAREMETAL_CONFIG

Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.

**default:** N/A

**type:** str (appendable)


# Scylla Cloud (xcloud) backend


## **cloud_cluster_id** / SCT_CLOUD_CLUSTER_ID

ID of an existing Scylla Cloud cluster to run against, instead of provisioning a new one.

**default:** N/A

**type:** int


## **cloud_credentials_path** / SCT_CLOUD_CREDENTIALS_PATH

Path to the SSH private key for nodes in a Scylla Cloud (siren) cluster, which SCT does not provision itself.

**default:** N/A

**type:** str (appendable)


## **cloud_prom_bearer_token** / SCT_CLOUD_PROM_BEARER_TOKEN

scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_host** / SCT_CLOUD_PROM_HOST

scylla cloud promproxy hostname to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_path** / SCT_CLOUD_PROM_PATH

scylla cloud promproxy path to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **xcloud_availability_zones** / SCT_XCLOUD_AVAILABILITY_ZONES

Comma-separated availability zones for Scylla Cloud DB placement.<br>AWS values are AZ IDs (e.g., 'use1-az1,use1-az2,use1-az3'); GCE values are zone names<br>(e.g., 'us-east1-b,us-east1-c'). When set, SCT sends 'availabilityZoneIdsOverride' and forces placement.<br>Provide one zone per DB node, or provide a shorter list to cycle round-robin (node count must divide evenly).<br>Repeat the same zone to keep all nodes in one AZ. Leave empty (default) to let Scylla Cloud choose placement<br>(multi-AZ spread). Cannot be used with 'xcloud_scaling_config'.

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


## **xcloud_replication_factor** / SCT_XCLOUD_REPLICATION_FACTOR

Replication factor for Scylla Cloud cluster

**default:** N/A

**type:** int


## **xcloud_scaling_config** / SCT_XCLOUD_SCALING_CONFIG

Scaling policy configuration. The payload should follow the following structure:<br><br>{<br>"InstanceFamilies": ["i8g"],<br>"Mode": "xcloud",<br>"Policies": {<br>"Storage": {"Min": 0, "TargetUtilization": 0.8},<br>"VCPU": {"Min": 0}<br>}<br>}<br><br>- InstanceFamilies(list): instance families to use for scaling (e.g., ["i4i", "i8g"])<br>- Mode(str): scaling mode, always "xcloud"<br>- Policies(dict): scaling policies with the following keys:<br>- Storage(dict):<br>- Min(int): minimum storage in TB to maintain<br>- TargetUtilization(float): target storage utilization from 0.7 to 0.9 with 0.05 step<br>- VCPU(dict):<br>- Min(int): minimum number of virtual CPUs to maintain<br><br>For more details, see `scaling` parameter description in Cloud REST API documentation:<br>https://cloud.docs.scylladb.com/stable/api.html#tag/Cluster/operation/createCluster

**default:** N/A

**type:** dict

**backend overrides:**
- `{}`: xcloud


## **xcloud_vpc_peering** / SCT_XCLOUD_VPC_PEERING

Dictionary of VPC peering parameters for private connectivity between<br>SCT infrastructure and Scylla Cloud. The following parameters are used:<br>enabled: bool - indicates whether VPC peering is to be used<br>cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)<br>cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)

**default:** N/A

**type:** dict

**backend overrides:**
- `{'enabled': True, 'cidr_pool_base': '172.31.0.0/16', 'cidr_subnet_size': 24}`: xcloud


# Minicloud


## **minicloud_container_cpus** / SCT_MINICLOUD_CONTAINER_CPUS

Cap the minicloud container's CPU allowance, in docker --cpus form (e.g. '8' or '7.5'). Empty means no limit

**default:** N/A

**type:** str (appendable)


## **minicloud_container_memory** / SCT_MINICLOUD_CONTAINER_MEMORY

Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker limit, so the container can consume the whole host. Setting it also makes this, rather than the host's free memory, the budget the preflight guest-memory gate measures against

**default:** N/A

**type:** str (appendable)


## **minicloud_container_name** / SCT_MINICLOUD_CONTAINER_NAME

Name of the minicloud docker container. Change it to run two emulators on one host — a second run under the same name force-removes the first one's container

**default:** minicloud

**type:** str (appendable)


## **minicloud_docker_image** / SCT_MINICLOUD_DOCKER_IMAGE

Explicit minicloud image override. Empty means the renovate-managed default from defaults/docker_images/minicloud/ (exposed as stress_image.minicloud)

**default:** N/A

**type:** str (appendable)


## **minicloud_endpoint_url** / SCT_MINICLOUD_ENDPOINT_URL

EC2 API endpoint URL for minicloud. When set, SCT adapts for minicloud limitations (no spot, no EIP, graceful TerminateInstances). Example: http://localhost:5000

**default:** N/A

**type:** str


## **minicloud_gcs_bucket** / SCT_MINICLOUD_GCS_BUCKET

GCS bucket for minicloud GCE image staging. Empty means derive <project>-minicloud-staging and create it on demand

**default:** N/A

**type:** str (appendable)


## **minicloud_keep_alive** / SCT_MINICLOUD_KEEP_ALIVE

Leave the minicloud container running after the test instead of tearing it down (CI sets this so separate provision/test/collect/clean stages reach the same container)

**default:** False

**type:** bool


## **minicloud_lightweight** / SCT_MINICLOUD_LIGHTWEIGHT

Enable lightweight mode for minicloud deployments

**default:** True

**type:** bool


## **minicloud_lightweight_memory** / SCT_MINICLOUD_LIGHTWEIGHT_MEMORY

Memory allocation for lightweight minicloud deployments

**default:** 4GiB

**type:** str (appendable)


## **minicloud_lightweight_vcpus** / SCT_MINICLOUD_LIGHTWEIGHT_VCPUS

vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this multiplies with minicloud_lightweight_memory across every guest in the test — raise it only on a host with cores to spare

**default:** 1

**type:** int


## **minicloud_regions** / SCT_MINICLOUD_REGIONS

Narrow the AWS regions minicloud prepares (default: every SCT-supported region; each costs ~2s at start-up)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **minicloud_s3_passthrough_buckets** / SCT_MINICLOUD_S3_PASSTHROUGH_BUCKETS

S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). Backend-independent: GCE runs reach S3 for the same content

**default:** scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com

**type:** str | list[str] → list[str] (appendable)


## **minicloud_skip_memory_check** / SCT_MINICLOUD_SKIP_MEMORY_CHECK

Skip the conservative host-memory preflight gate — for development machines whose owner knows the workload's real footprint; an oversized test then dies mid-run as a container OOM kill (exit 137)

**default:** False

**type:** bool


## **minicloud_state_dir** / SCT_MINICLOUD_STATE_DIR

Where minicloud keeps its image cache, per-instance disks and minicloud.log — tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace

**default:** N/A

**type:** str (appendable)


# Longevity tests


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Compaction strategy to use for pre-created schema

**default:** IncrementalCompactionStrategy

**type:** str (appendable)


## **data_validation** / SCT_DATA_VALIDATION

Specify the type of data validation to perform

**default:** N/A

**type:** str (appendable)


## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keyspace to be pre-created before running workload

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **pre_create_schema** / SCT_PRE_CREATE_SCHEMA

Enable or disable pre-creation of schema before running workload

**default:** False

**type:** bool


## **run_commit_log_check_thread** / SCT_RUN_COMMIT_LOG_CHECK_THREAD

Flag to run a thread that checks commit logs

**default:** True

**type:** bool

**backend overrides:**
- `False`: xcloud


## **run_full_partition_scan** / SCT_RUN_FULL_PARTITION_SCAN

Enable or disable running full partition scans during tests

**default:** N/A

**type:** str (appendable)


## **run_fullscan** / SCT_RUN_FULLSCAN

Enable or disable running full scans during tests

**default:** []

**type:** list


## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Enable or disable tombstone garbage collection verification during tests

**default:** N/A

**type:** str (appendable)


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** 0

**type:** int


## **sstable_size** / SCT_SSTABLE_SIZE

Configure sstable size for pre-create-schema mode

**default:** N/A

**type:** int


## **validate_large_collections** / SCT_VALIDATE_LARGE_COLLECTIONS

Flag to validate large collections in the database

**default:** False

**type:** bool


# Performance regression tests


## **max_deviation** / SCT_MAX_DEVIATION

Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one

**default:** N/A

**type:** float


## **n_stress_process** / SCT_N_STRESS_PROCESS

Number of stress processes per loader

**default:** N/A

**type:** int


## **num_loaders_step** / SCT_NUM_LOADERS_STEP

Number of loaders which should be added per step

**default:** N/A

**type:** int


## **num_threads_step** / SCT_NUM_THREADS_STEP

Number of threads which should be added on per step

**default:** N/A

**type:** int


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Supports three formats: 1) String/int list (cassandra-stress): {'read': ['100000', '150000'], 'mixed': [100, 200]} 2) Dict list (latte/multi-param): {'read': [{'threads': 10, 'concurrency': 128, 'rate': '100000'}, ...]} Dict format allows specifying threads, concurrency, and rate per step. Integers are automatically converted to strings for backward compatibility.

**default:** N/A

**type:** dict | YAML/JSON string → dict


## **perf_gradual_write_preload_data** / SCT_PERF_GRADUAL_WRITE_PRELOAD_DATA

If true, preload data (via prepare_write_cmd) before test_write_gradual_increase_load. Needed for LWT conditional-update workloads (e.g. UPDATE ... IF <cond>) that require existing rows to have a chance of applying; not needed for INSERT-based write workloads on a fresh table.

**default:** False

**type:** bool


## **perf_simple_query_extra_command** / SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND

Extra command line options to pass to perf_simple_query

**default:** N/A

**type:** str (appendable)


## **perf_stress_keyspace** / SCT_PERF_STRESS_KEYSPACE

Keyspace name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'keyspace' key in latte_schema_parameters.

**default:** N/A

**type:** str (appendable)


## **perf_stress_table** / SCT_PERF_STRESS_TABLE

Table name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'table' key in latte_schema_parameters.

**default:** N/A

**type:** str (appendable)


## **run_db_node_benchmarks** / SCT_RUN_DB_NODE_BENCHMARKS

Flag for running db node benchmarks before the tests

**default:** False

**type:** bool


## **stop_on_hw_perf_failure** / SCT_STOP_ON_HW_PERF_FAILURE

Stop sct performance test if hardware performance test failed<br><br>Hardware performance tests runs on each node with sysbench and cassandra-fio tools.<br>Results stored in ES. HW perf tests run during cluster setups and not affect<br>SCT Performance tests. Results calculated as average among all results for certain<br>instance type or among all nodes during single run.<br>if results for a single node is not in margin 0.01 of<br>average result for all nodes, hw test considered as Failed.<br>If stop_on_hw_perf_failure is True, then sct performance test will be terminated<br>after hw perf tests detect node with hw results not in margin with average<br>If stop_on_hw_perf_failure is False, then sct performance test will be run<br>even after hw perf tests detect node with hw results not in margin with average

**default:** False

**type:** bool


## **stress_process_step** / SCT_STRESS_PROCESS_STEP

add/remove num of process on each round

**default:** N/A

**type:** int


## **stress_step_duration** / SCT_STRESS_STEP_DURATION

Duration of time for stress round

**default:** 15m

**type:** str (appendable)


## **stress_threads_start_num** / SCT_STRESS_THREADS_START_NUM

Number of threads for c-s command

**default:** N/A

**type:** int


## **use_hdrhistogram** / SCT_USE_HDRHISTOGRAM

Enable hdr histogram logging for cs

**default:** False

**type:** bool


# Upgrade tests


## **disable_raft** / SCT_DISABLE_RAFT

Flag to disable Raft consensus for LWT operations.

**default:** True

**type:** bool


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade

**default:** False

**type:** bool


## **enable_truncate_checks_on_node_upgrade** / SCT_ENABLE_TRUNCATE_CHECKS_ON_NODE_UPGRADE

Enables or disables truncate checks on each node upgrade and rollback

**default:** True

**type:** bool


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** False

**type:** bool


## **large_partition_stress_during_upgrade** / SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE

Stress command to be run during rolling upgrade while nodes are being upgraded. This workload cannot use CL=ALL as not all nodes may be available during the upgrade.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO

URL to the Scylla repository for new versions.

**default:** N/A

**type:** str (appendable)


## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** str (appendable)


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** int


## **run_gemini_in_rolling_upgrade** / SCT_RUN_GEMINI_IN_ROLLING_UPGRADE

Enable running Gemini workload during rolling upgrade test. Default is false.

**default:** False

**type:** bool


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade starts (preload/validation stage). This workload runs before any nodes are upgraded and can use CL=ALL for data validation.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

The target version to upgrade Scylla to.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default.

**default:** True

**type:** bool


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** bool


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

Stress command(s) run after every node has been upgraded, to verify the upgraded cluster. See 'stress_cmd' for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_migration** / SCT_VERIFY_STRESS_AFTER_MIGRATION

Stress command to verify data after migration

**default:** N/A

**type:** str (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


# Grow cluster tests


## **cassandra_stress_population_size** / SCT_CASSANDRA_STRESS_POPULATION_SIZE

The total population size over which the Cassandra stress tests are run.

**default:** 1000000

**type:** int


## **cassandra_stress_threads** / SCT_CASSANDRA_STRESS_THREADS

The number of threads used by Cassandra stress tests.

**default:** 1000

**type:** int


# Refresh (sstable loading) tests


## **flush_period** / SCT_FLUSH_PERIOD

Seconds to wait between the flushes controlled by 'flush_times'.

**default:** N/A

**type:** int


## **flush_times** / SCT_FLUSH_TIMES

How many times to flush the memtable to disk during the refresh test.

**default:** N/A

**type:** int


## **skip_download** / SCT_SKIP_DOWNLOAD

Skip downloading the SSTable archive and reuse a copy already on the node.

**default:** False

**type:** bool


## **sstable_file** / SCT_SSTABLE_FILE

Local path of the SSTable archive to load with 'nodetool refresh'.

**default:** N/A

**type:** str (appendable)


## **sstable_md5** / SCT_SSTABLE_MD5

Expected MD5 of the downloaded SSTable archive, used to verify the download.

**default:** N/A

**type:** str (appendable)


## **sstable_url** / SCT_SSTABLE_URL

URL the SSTable archive is downloaded from when it is not already on the node.

**default:** N/A

**type:** str (appendable)


# Jepsen tests


## **jepsen_scylla_repo** / SCT_JEPSEN_SCYLLA_REPO

Link to the git repository with Jepsen Scylla tests

**default:** https://github.com/jepsen-io/scylla.git

**type:** str (appendable)


## **jepsen_test_cmd** / SCT_JEPSEN_TEST_CMD

Jepsen test command (e.g., 'test-all')

**default:** ['test-all -w cas-register --concurrency 10n', 'test-all -w counter --concurrency 10n', 'test-all -w cmap --concurrency 10n', 'test-all -w cset --concurrency 10n', 'test-all -w write-isolation --concurrency 10n', 'test-all -w list-append --concurrency 10n', 'test-all -w wr-register --concurrency 10n']

**type:** str | list[str] → list[str] (appendable)


## **jepsen_test_count** / SCT_JEPSEN_TEST_COUNT

Possible number of reruns of single Jepsen test command

**default:** 1

**type:** int


## **jepsen_test_run_policy** / SCT_JEPSEN_TEST_RUN_POLICY

Jepsen test run policy (i.e., what we want to consider as passed for a single test)<br><br>'most' - most test runs are passed<br>'any'  - one pass is enough<br>'all'  - all test runs should pass

**default:** all

**type:** Literal['most', 'any', 'all']


# Amazon EMR (spark-migrator)


## **emr_applications** / SCT_EMR_APPLICATIONS

List of EMR applications to install (default: ['Spark'])

**default:** N/A

**type:** list

**backend overrides:**
- `['Spark']`: aws


## **emr_install_spark4_via_bootstrap** / SCT_EMR_INSTALL_SPARK4_VIA_BOOTSTRAP

Legacy fallback: install Spark 4.x via an EMR bootstrap action and submit the migrator through script-runner.jar (for emr-7.x releases). Default value is false - i.e. deployment of native Spark on an `emr-spark-8.x` release label.

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: aws


## **emr_instance_count_core** / SCT_EMR_INSTANCE_COUNT_CORE

How many EMR core nodes to launch.

**default:** N/A

**type:** int

**backend overrides:**
- `2`: aws


## **emr_instance_count_task** / SCT_EMR_INSTANCE_COUNT_TASK

How many EMR task nodes to launch (compute only, no HDFS).

**default:** N/A

**type:** int

**backend overrides:**
- `0`: aws


## **emr_instance_type_core** / SCT_EMR_INSTANCE_TYPE_CORE

EC2 instance type for the EMR core nodes (they run both compute and HDFS).

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_master** / SCT_EMR_INSTANCE_TYPE_MASTER

Instance type for EMR master node (e.g., 'm5.xlarge')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_task** / SCT_EMR_INSTANCE_TYPE_TASK

Instance type for EMR task nodes (optional, uses Spot instances)

**default:** N/A

**type:** str (appendable)


## **emr_keep_alive** / SCT_EMR_KEEP_ALIVE

Whether EMR cluster stays alive after job completion (default: true for reuse during testing)

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws


## **emr_log_uri** / SCT_EMR_LOG_URI

S3 URI for EMR cluster logs (e.g., 's3://sct-emr-spark-migrator-{region}/logs/')

**default:** N/A

**type:** str (appendable)


## **emr_release_label** / SCT_EMR_RELEASE_LABEL

EMR release version (e.g., 'emr-7.8.0'). When set, an EMR cluster is provisioned alongside the Scylla cluster.

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_jar_path** / SCT_EMR_SPARK_MIGRATOR_JAR_PATH

S3 path or local path to the spark-migrator JAR file

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_release** / SCT_EMR_SPARK_MIGRATOR_RELEASE

scylla-migrator release tag (e.g., 'v1.1.2'). When set, JAR is auto-downloaded from GitHub releases and uploaded to S3. Takes precedence over emr_spark_migrator_jar_path.

**default:** N/A

**type:** str (appendable)


## **emr_spot_bid_percentage** / SCT_EMR_SPOT_BID_PERCENTAGE

Max Spot price as percentage of On-Demand for EMR task nodes (default: 100)

**default:** N/A

**type:** int

**backend overrides:**
- `100`: aws


# Spark migrator (Cassandra to Scylla)


## **migrator_run_validator** / SCT_MIGRATOR_RUN_VALIDATOR

Run the spark-migrator validator after migration to do a row-by-row comparison

**default:** N/A

**type:** bool


## **migrator_source_hosts** / SCT_MIGRATOR_SOURCE_HOSTS

CQL contact-point IPs for the source Cassandra/Scylla cluster. Mutually exclusive with migrator_source_test_id.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **migrator_source_keyspace** / SCT_MIGRATOR_SOURCE_KEYSPACE

Keyspace to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_table** / SCT_MIGRATOR_SOURCE_TABLE

Table to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


## **migrator_source_test_id** / SCT_MIGRATOR_SOURCE_TEST_ID

SCT test_id of a running source cluster. When set, source host IPs are auto-discovered via EC2 tags (NodeType=cs-db). Mutually exclusive with migrator_source_hosts.

**default:** N/A

**type:** str (appendable)


## **migrator_step_timeout_minutes** / SCT_MIGRATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator migration EMR step. Default 360.

**default:** N/A

**type:** int

**backend overrides:**
- `360`: aws


## **migrator_target_keyspace** / SCT_MIGRATOR_TARGET_KEYSPACE

Keyspace to migrate into on the target Scylla cluster. Defaults to migrator_source_keyspace.

**default:** N/A

**type:** str (appendable)


## **migrator_target_table** / SCT_MIGRATOR_TARGET_TABLE

Table to migrate into on the target Scylla cluster. Defaults to migrator_source_table.

**default:** N/A

**type:** str (appendable)


## **validator_step_timeout_minutes** / SCT_VALIDATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator validator EMR step. Default 60.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws
=======
## Groups

523 options across 29 groups.

| Group | Options | What it covers |
|---|---:|---|
| [General and provisioning](configuration_options/general-and-provisioning.md) | 71 | Cluster topology, region/AZ placement, instance provisioning, credentials and test-level plumbing. Options here apply to every... |
| [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) | 44 | Which Scylla to install and how it is configured: repos, versions, distro, `scylla.yaml`/command-line options, experimental... |
| [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) | 12 | Which disruptions run, how often, and how targets are selected. |
| [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) | 74 | The load applied to the cluster: stress tool command lines, loader-side settings and stress duration. **Which option belongs... |
| [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) | 17 | The monitoring stack, event severities, Argus reporting and email reports. |
| [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) | 13 | How logs and diagnostics are collected, and what happens to the resources when the test ends. |
| [Scylla Doctor](configuration_options/scylla-doctor.md) | 6 | The scylla-doctor diagnostic tool. It is both a subject under test (the artifact tests run it and assert on its findings) and... |
| [Scylla Manager](configuration_options/scylla-manager.md) | 26 | Scylla Manager server and agent: versions, repos and backup/restore settings. |
| [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) | 10 | A second database cluster used for comparison or migration testing -- the 'oracle' cluster in Gemini runs, or a Cassandra... |
| [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) | 10 | Scylla's DynamoDB-compatible API: the endpoint, write isolation, load-balancing and the credentials the tests use against it. |
| [Vector Store](configuration_options/vector-store.md) | 6 | The Vector Store service under test alongside Scylla. |
| [Kafka / CDC connectors](configuration_options/kafka-cdc-connectors.md) | 2 | Kafka deployment and connector configuration for CDC testing. |
| [AWS backend](configuration_options/aws-backend.md) | 22 | AWS-specific provisioning: AMIs, EC2 instance and disk settings, placement groups, capacity reservations and dedicated hosts. |
| [GCE backend](configuration_options/gce-backend.md) | 23 | Google Compute Engine provisioning. |
| [Azure backend](configuration_options/azure-backend.md) | 13 | Microsoft Azure provisioning. |
| [OCI backend](configuration_options/oci-backend.md) | 10 | Oracle Cloud Infrastructure provisioning. |
| [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) | 44 | Scylla Operator deployments: EKS, GKE and local kind clusters. |
| [Docker backend](configuration_options/docker-backend.md) | 2 | Running the cluster as local Docker containers. |
| [Baremetal backend](configuration_options/baremetal-backend.md) | 7 | Running against pre-existing hosts that SCT does not provision. |
| [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) | 12 | Clusters provisioned through the Scylla Cloud API, including the legacy siren `cloud_*` options. |
| [Minicloud](configuration_options/minicloud.md) | 14 | Minicloud is an AWS-API-compatible environment rather than a cloud of its own: it runs with `cluster_backend: aws` and an... |
| [Longevity tests](configuration_options/longevity-tests.md) | 13 | Options specific to long-running longevity test scenarios. |
| [Performance regression tests](configuration_options/performance-regression-tests.md) | 18 | Throughput/latency measurement runs, including gradual-throughput steps and HDR histogram settings. |
| [Upgrade tests](configuration_options/upgrade-tests.md) | 20 | Rolling upgrade and rollback scenarios: target versions and the load applied across the upgrade. |
| [Grow cluster tests](configuration_options/grow-cluster-tests.md) | 2 | Scaling the cluster up and down during a test. |
| [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) | 6 | Loading pre-built SSTables into a running cluster via nodetool refresh. |
| [Jepsen tests](configuration_options/jepsen-tests.md) | 4 | Jepsen consistency test runs. |
| [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) | 13 | The EMR cluster that runs the Spark migrator job. |
| [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) | 9 | The migration job itself: source and target keyspaces/tables and validation. |

## All options, alphabetically

| Option | Environment variable | Group |
|---|---|---|
| [`adaptive_timeout_multipliers`](configuration_options/general-and-provisioning.md#adaptive_timeout_multipliers) | `SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`adaptive_timeout_store_metrics`](configuration_options/general-and-provisioning.md#adaptive_timeout_store_metrics) | `SCT_ADAPTIVE_TIMEOUT_STORE_METRICS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`add_cs_user_profiles_extra_tables`](configuration_options/stress-commands-and-load-generation.md#add_cs_user_profiles_extra_tables) | `SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`add_node_cnt`](configuration_options/general-and-provisioning.md#add_node_cnt) | `SCT_ADD_NODE_CNT` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`agent`](configuration_options/general-and-provisioning.md#agent) | `SCT_AGENT` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`alternator_access_key_id`](configuration_options/alternator-dynamodb-api.md#alternator_access_key_id) | `SCT_ALTERNATOR_ACCESS_KEY_ID` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_enforce_authorization`](configuration_options/alternator-dynamodb-api.md#alternator_enforce_authorization) | `SCT_ALTERNATOR_ENFORCE_AUTHORIZATION` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_loadbalancing`](configuration_options/alternator-dynamodb-api.md#alternator_loadbalancing) | `SCT_ALTERNATOR_LOADBALANCING` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_port`](configuration_options/alternator-dynamodb-api.md#alternator_port) | `SCT_ALTERNATOR_PORT` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_secret_access_key`](configuration_options/alternator-dynamodb-api.md#alternator_secret_access_key) | `SCT_ALTERNATOR_SECRET_ACCESS_KEY` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_stress_rate`](configuration_options/stress-commands-and-load-generation.md#alternator_stress_rate) | `SCT_ALTERNATOR_STRESS_RATE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`alternator_test_table`](configuration_options/alternator-dynamodb-api.md#alternator_test_table) | `SCT_ALTERNATOR_TEST_TABLE` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_trust_all_certificates`](configuration_options/alternator-dynamodb-api.md#alternator_trust_all_certificates) | `SCT_ALTERNATOR_TRUST_ALL_CERTIFICATES` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_use_dns_routing`](configuration_options/alternator-dynamodb-api.md#alternator_use_dns_routing) | `SCT_ALTERNATOR_USE_DNS_ROUTING` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`alternator_write_always_lwt_stress_rate`](configuration_options/stress-commands-and-load-generation.md#alternator_write_always_lwt_stress_rate) | `SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`alternator_write_isolation`](configuration_options/alternator-dynamodb-api.md#alternator_write_isolation) | `SCT_ALTERNATOR_WRITE_ISOLATION` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`ami_db_cassandra_user`](configuration_options/aws-backend.md#ami_db_cassandra_user) | `SCT_AMI_DB_CASSANDRA_USER` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_db_scylla_user`](configuration_options/aws-backend.md#ami_db_scylla_user) | `SCT_AMI_DB_SCYLLA_USER` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_db_cassandra`](configuration_options/aws-backend.md#ami_id_db_cassandra) | `SCT_AMI_ID_DB_CASSANDRA` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_db_oracle`](configuration_options/aws-backend.md#ami_id_db_oracle) | `SCT_AMI_ID_DB_ORACLE` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_db_scylla`](configuration_options/aws-backend.md#ami_id_db_scylla) | `SCT_AMI_ID_DB_SCYLLA` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_db_scylla_desc`](configuration_options/aws-backend.md#ami_id_db_scylla_desc) | `SCT_AMI_ID_DB_SCYLLA_DESC` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_loader`](configuration_options/aws-backend.md#ami_id_loader) | `SCT_AMI_ID_LOADER` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_monitor`](configuration_options/aws-backend.md#ami_id_monitor) | `SCT_AMI_ID_MONITOR` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_id_vector_store`](configuration_options/aws-backend.md#ami_id_vector_store) | `SCT_AMI_ID_VECTOR_STORE` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_loader_user`](configuration_options/aws-backend.md#ami_loader_user) | `SCT_AMI_LOADER_USER` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_monitor_user`](configuration_options/aws-backend.md#ami_monitor_user) | `SCT_AMI_MONITOR_USER` | [AWS backend](configuration_options/aws-backend.md) |
| [`ami_vector_store_user`](configuration_options/aws-backend.md#ami_vector_store_user) | `SCT_AMI_VECTOR_STORE_USER` | [AWS backend](configuration_options/aws-backend.md) |
| [`append_scylla_args`](configuration_options/scylla-installation-and-configuration.md#append_scylla_args) | `SCT_APPEND_SCYLLA_ARGS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`append_scylla_args_oracle`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#append_scylla_args_oracle) | `SCT_APPEND_SCYLLA_ARGS_ORACLE` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`append_scylla_node_exporter_args`](configuration_options/scylla-installation-and-configuration.md#append_scylla_node_exporter_args) | `SCT_APPEND_SCYLLA_NODE_EXPORTER_ARGS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`append_scylla_setup_args`](configuration_options/scylla-installation-and-configuration.md#append_scylla_setup_args) | `SCT_APPEND_SCYLLA_SETUP_ARGS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`append_scylla_yaml`](configuration_options/scylla-installation-and-configuration.md#append_scylla_yaml) | `SCT_APPEND_SCYLLA_YAML` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`argus_email_report_template`](configuration_options/monitoring-events-and-reporting.md#argus_email_report_template) | `SCT_ARGUS_EMAIL_REPORT_TEMPLATE` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`argus_use_ssh_tunnel`](configuration_options/monitoring-events-and-reporting.md#argus_use_ssh_tunnel) | `SCT_ARGUS_USE_SSH_TUNNEL` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`assert_linux_distro_features`](configuration_options/scylla-installation-and-configuration.md#assert_linux_distro_features) | `SCT_ASSERT_LINUX_DISTRO_FEATURES` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`authenticator`](configuration_options/scylla-installation-and-configuration.md#authenticator) | `SCT_AUTHENTICATOR` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`authenticator_password`](configuration_options/scylla-installation-and-configuration.md#authenticator_password) | `SCT_AUTHENTICATOR_PASSWORD` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`authenticator_user`](configuration_options/scylla-installation-and-configuration.md#authenticator_user) | `SCT_AUTHENTICATOR_USER` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`authorizer`](configuration_options/scylla-installation-and-configuration.md#authorizer) | `SCT_AUTHORIZER` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`availability_zone`](configuration_options/general-and-provisioning.md#availability_zone) | `SCT_AVAILABILITY_ZONE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`aws_dedicated_host_ids`](configuration_options/aws-backend.md#aws_dedicated_host_ids) | `SCT_AWS_DEDICATED_HOST_IDS` | [AWS backend](configuration_options/aws-backend.md) |
| [`aws_fallback_to_next_availability_zone`](configuration_options/aws-backend.md#aws_fallback_to_next_availability_zone) | `SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE` | [AWS backend](configuration_options/aws-backend.md) |
| [`aws_instance_profile_name_db`](configuration_options/aws-backend.md#aws_instance_profile_name_db) | `SCT_AWS_INSTANCE_PROFILE_NAME_DB` | [AWS backend](configuration_options/aws-backend.md) |
| [`aws_instance_profile_name_loader`](configuration_options/aws-backend.md#aws_instance_profile_name_loader) | `SCT_AWS_INSTANCE_PROFILE_NAME_LOADER` | [AWS backend](configuration_options/aws-backend.md) |
| [`azure_image_db`](configuration_options/azure-backend.md#azure_image_db) | `SCT_AZURE_IMAGE_DB` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_image_db_oracle`](configuration_options/azure-backend.md#azure_image_db_oracle) | `SCT_AZURE_IMAGE_DB_ORACLE` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_image_loader`](configuration_options/azure-backend.md#azure_image_loader) | `SCT_AZURE_IMAGE_LOADER` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_image_monitor`](configuration_options/azure-backend.md#azure_image_monitor) | `SCT_AZURE_IMAGE_MONITOR` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_image_username`](configuration_options/azure-backend.md#azure_image_username) | `SCT_AZURE_IMAGE_USERNAME` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_instance_type_db`](configuration_options/azure-backend.md#azure_instance_type_db) | `SCT_AZURE_INSTANCE_TYPE_DB` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_instance_type_db_oracle`](configuration_options/azure-backend.md#azure_instance_type_db_oracle) | `SCT_AZURE_INSTANCE_TYPE_DB_ORACLE` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_instance_type_loader`](configuration_options/azure-backend.md#azure_instance_type_loader) | `SCT_AZURE_INSTANCE_TYPE_LOADER` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_instance_type_monitor`](configuration_options/azure-backend.md#azure_instance_type_monitor) | `SCT_AZURE_INSTANCE_TYPE_MONITOR` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_provision_stuck_vm_recreate_attempts`](configuration_options/azure-backend.md#azure_provision_stuck_vm_recreate_attempts) | `SCT_AZURE_PROVISION_STUCK_VM_RECREATE_ATTEMPTS` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_provision_stuck_vm_timeout`](configuration_options/azure-backend.md#azure_provision_stuck_vm_timeout) | `SCT_AZURE_PROVISION_STUCK_VM_TIMEOUT` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_provision_stuck_vm_total_timeout`](configuration_options/azure-backend.md#azure_provision_stuck_vm_total_timeout) | `SCT_AZURE_PROVISION_STUCK_VM_TOTAL_TIMEOUT` | [Azure backend](configuration_options/azure-backend.md) |
| [`azure_region_name`](configuration_options/azure-backend.md#azure_region_name) | `SCT_AZURE_REGION_NAME` | [Azure backend](configuration_options/azure-backend.md) |
| [`backtrace_decoding`](configuration_options/monitoring-events-and-reporting.md#backtrace_decoding) | `SCT_BACKTRACE_DECODING` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`backtrace_decoding_disable_regex`](configuration_options/monitoring-events-and-reporting.md#backtrace_decoding_disable_regex) | `SCT_BACKTRACE_DECODING_DISABLE_REGEX` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`backtrace_stall_decoding`](configuration_options/monitoring-events-and-reporting.md#backtrace_stall_decoding) | `SCT_BACKTRACE_STALL_DECODING` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`backup_bucket_backend`](configuration_options/scylla-manager.md#backup_bucket_backend) | `SCT_BACKUP_BUCKET_BACKEND` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`backup_bucket_location`](configuration_options/scylla-manager.md#backup_bucket_location) | `SCT_BACKUP_BUCKET_LOCATION` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`backup_bucket_region`](configuration_options/scylla-manager.md#backup_bucket_region) | `SCT_BACKUP_BUCKET_REGION` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`bare_loaders`](configuration_options/stress-commands-and-load-generation.md#bare_loaders) | `SCT_BARE_LOADERS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`batch_size`](configuration_options/stress-commands-and-load-generation.md#batch_size) | `SCT_BATCH_SIZE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`billing_project`](configuration_options/general-and-provisioning.md#billing_project) | `SCT_BILLING_PROJECT` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`bisect_end_date`](configuration_options/general-and-provisioning.md#bisect_end_date) | `SCT_BISECT_END_DATE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`bisect_start_date`](configuration_options/general-and-provisioning.md#bisect_start_date) | `SCT_BISECT_START_DATE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`c_s_driver_version`](configuration_options/stress-commands-and-load-generation.md#c_s_driver_version) | `SCT_C_S_DRIVER_VERSION` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`cassandra_broadcast_rpc_public`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#cassandra_broadcast_rpc_public) | `SCT_CASSANDRA_BROADCAST_RPC_PUBLIC` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`cassandra_num_tokens`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#cassandra_num_tokens) | `SCT_CASSANDRA_NUM_TOKENS` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`cassandra_oracle_version`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#cassandra_oracle_version) | `SCT_CASSANDRA_ORACLE_VERSION` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`cassandra_stress_population_size`](configuration_options/grow-cluster-tests.md#cassandra_stress_population_size) | `SCT_CASSANDRA_STRESS_POPULATION_SIZE` | [Grow cluster tests](configuration_options/grow-cluster-tests.md) |
| [`cassandra_stress_threads`](configuration_options/grow-cluster-tests.md#cassandra_stress_threads) | `SCT_CASSANDRA_STRESS_THREADS` | [Grow cluster tests](configuration_options/grow-cluster-tests.md) |
| [`cassandra_version`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#cassandra_version) | `SCT_CASSANDRA_VERSION` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`client_encrypt`](configuration_options/scylla-installation-and-configuration.md#client_encrypt) | `SCT_CLIENT_ENCRYPT` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`client_encrypt_mtls`](configuration_options/scylla-installation-and-configuration.md#client_encrypt_mtls) | `SCT_CLIENT_ENCRYPT_MTLS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`cloud_cluster_id`](configuration_options/scylla-cloud-xcloud-backend.md#cloud_cluster_id) | `SCT_CLOUD_CLUSTER_ID` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`cloud_credentials_path`](configuration_options/scylla-cloud-xcloud-backend.md#cloud_credentials_path) | `SCT_CLOUD_CREDENTIALS_PATH` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`cloud_prom_bearer_token`](configuration_options/scylla-cloud-xcloud-backend.md#cloud_prom_bearer_token) | `SCT_CLOUD_PROM_BEARER_TOKEN` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`cloud_prom_host`](configuration_options/scylla-cloud-xcloud-backend.md#cloud_prom_host) | `SCT_CLOUD_PROM_HOST` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`cloud_prom_path`](configuration_options/scylla-cloud-xcloud-backend.md#cloud_prom_path) | `SCT_CLOUD_PROM_PATH` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`cluster_backend`](configuration_options/general-and-provisioning.md#cluster_backend) | `SCT_CLUSTER_BACKEND` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`cluster_health_check`](configuration_options/general-and-provisioning.md#cluster_health_check) | `SCT_CLUSTER_HEALTH_CHECK` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`cluster_health_check_parallel_workers`](configuration_options/general-and-provisioning.md#cluster_health_check_parallel_workers) | `SCT_CLUSTER_HEALTH_CHECK_PARALLEL_WORKERS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`cluster_target_size`](configuration_options/longevity-tests.md#cluster_target_size) | `SCT_CLUSTER_TARGET_SIZE` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`collect_logs`](configuration_options/logs-diagnostics-and-teardown.md#collect_logs) | `SCT_COLLECT_LOGS` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`collect_nvme_diagnostics`](configuration_options/logs-diagnostics-and-teardown.md#collect_nvme_diagnostics) | `SCT_COLLECT_NVME_DIAGNOSTICS` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`compaction_strategy`](configuration_options/longevity-tests.md#compaction_strategy) | `SCT_COMPACTION_STRATEGY` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`config_files`](configuration_options/general-and-provisioning.md#config_files) | `SCT_CONFIG_FILES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`cs_debug`](configuration_options/stress-commands-and-load-generation.md#cs_debug) | `SCT_CS_DEBUG` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`cs_duration`](configuration_options/stress-commands-and-load-generation.md#cs_duration) | `SCT_CS_DURATION` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`cs_extra_jvm_opts`](configuration_options/stress-commands-and-load-generation.md#cs_extra_jvm_opts) | `SCT_CS_EXTRA_JVM_OPTS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`cs_populating_distribution`](configuration_options/stress-commands-and-load-generation.md#cs_populating_distribution) | `SCT_CS_POPULATING_DISTRIBUTION` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`cs_safepoint_logging`](configuration_options/stress-commands-and-load-generation.md#cs_safepoint_logging) | `SCT_CS_SAFEPOINT_LOGGING` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`cs_user_profiles`](configuration_options/stress-commands-and-load-generation.md#cs_user_profiles) | `SCT_CS_USER_PROFILES` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`data_validation`](configuration_options/longevity-tests.md#data_validation) | `SCT_DATA_VALIDATION` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`data_volume_disk_iops`](configuration_options/general-and-provisioning.md#data_volume_disk_iops) | `SCT_DATA_VOLUME_DISK_IOPS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`data_volume_disk_num`](configuration_options/general-and-provisioning.md#data_volume_disk_num) | `SCT_DATA_VOLUME_DISK_NUM` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`data_volume_disk_size`](configuration_options/general-and-provisioning.md#data_volume_disk_size) | `SCT_DATA_VOLUME_DISK_SIZE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`data_volume_disk_throughput`](configuration_options/general-and-provisioning.md#data_volume_disk_throughput) | `SCT_DATA_VOLUME_DISK_THROUGHPUT` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`data_volume_disk_type`](configuration_options/general-and-provisioning.md#data_volume_disk_type) | `SCT_DATA_VOLUME_DISK_TYPE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`db_nodes_private_ip`](configuration_options/baremetal-backend.md#db_nodes_private_ip) | `SCT_DB_NODES_PRIVATE_IP` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`db_nodes_public_ip`](configuration_options/baremetal-backend.md#db_nodes_public_ip) | `SCT_DB_NODES_PUBLIC_IP` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`db_nodes_shards_selection`](configuration_options/general-and-provisioning.md#db_nodes_shards_selection) | `SCT_DB_NODES_SHARDS_SELECTION` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`db_type`](configuration_options/scylla-installation-and-configuration.md#db_type) | `SCT_DB_TYPE` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`disable_raft`](configuration_options/upgrade-tests.md#disable_raft) | `SCT_DISABLE_RAFT` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`docker_image`](configuration_options/docker-backend.md#docker_image) | `SCT_DOCKER_IMAGE` | [Docker backend](configuration_options/docker-backend.md) |
| [`docker_image_cassandra`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#docker_image_cassandra) | `SCT_DOCKER_IMAGE_CASSANDRA` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`docker_network`](configuration_options/docker-backend.md#docker_network) | `SCT_DOCKER_NETWORK` | [Docker backend](configuration_options/docker-backend.md) |
| [`download_from_s3`](configuration_options/monitoring-events-and-reporting.md#download_from_s3) | `SCT_DOWNLOAD_FROM_S3` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`dynamodb_primarykey_type`](configuration_options/alternator-dynamodb-api.md#dynamodb_primarykey_type) | `SCT_DYNAMODB_PRIMARYKEY_TYPE` | [Alternator (DynamoDB API)](configuration_options/alternator-dynamodb-api.md) |
| [`effective_compression_ratio`](configuration_options/stress-commands-and-load-generation.md#effective_compression_ratio) | `SCT_EFFECTIVE_COMPRESSION_RATIO` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`eks_admin_arn`](configuration_options/kubernetes-backends-eks-gke-kind.md#eks_admin_arn) | `SCT_EKS_ADMIN_ARN` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`eks_cluster_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#eks_cluster_version) | `SCT_EKS_CLUSTER_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`eks_nodegroup_role_arn`](configuration_options/kubernetes-backends-eks-gke-kind.md#eks_nodegroup_role_arn) | `SCT_EKS_NODEGROUP_ROLE_ARN` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`eks_role_arn`](configuration_options/kubernetes-backends-eks-gke-kind.md#eks_role_arn) | `SCT_EKS_ROLE_ARN` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`eks_service_ipv4_cidr`](configuration_options/kubernetes-backends-eks-gke-kind.md#eks_service_ipv4_cidr) | `SCT_EKS_SERVICE_IPV4_CIDR` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`eks_vpc_cni_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#eks_vpc_cni_version) | `SCT_EKS_VPC_CNI_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`email_recipients`](configuration_options/monitoring-events-and-reporting.md#email_recipients) | `SCT_EMAIL_RECIPIENTS` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`email_subject_postfix`](configuration_options/monitoring-events-and-reporting.md#email_subject_postfix) | `SCT_EMAIL_SUBJECT_POSTFIX` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`emr_applications`](configuration_options/amazon-emr-spark-migrator.md#emr_applications) | `SCT_EMR_APPLICATIONS` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_install_spark4_via_bootstrap`](configuration_options/amazon-emr-spark-migrator.md#emr_install_spark4_via_bootstrap) | `SCT_EMR_INSTALL_SPARK4_VIA_BOOTSTRAP` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_instance_count_core`](configuration_options/amazon-emr-spark-migrator.md#emr_instance_count_core) | `SCT_EMR_INSTANCE_COUNT_CORE` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_instance_count_task`](configuration_options/amazon-emr-spark-migrator.md#emr_instance_count_task) | `SCT_EMR_INSTANCE_COUNT_TASK` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_instance_type_core`](configuration_options/amazon-emr-spark-migrator.md#emr_instance_type_core) | `SCT_EMR_INSTANCE_TYPE_CORE` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_instance_type_master`](configuration_options/amazon-emr-spark-migrator.md#emr_instance_type_master) | `SCT_EMR_INSTANCE_TYPE_MASTER` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_instance_type_task`](configuration_options/amazon-emr-spark-migrator.md#emr_instance_type_task) | `SCT_EMR_INSTANCE_TYPE_TASK` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_keep_alive`](configuration_options/amazon-emr-spark-migrator.md#emr_keep_alive) | `SCT_EMR_KEEP_ALIVE` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_log_uri`](configuration_options/amazon-emr-spark-migrator.md#emr_log_uri) | `SCT_EMR_LOG_URI` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_release_label`](configuration_options/amazon-emr-spark-migrator.md#emr_release_label) | `SCT_EMR_RELEASE_LABEL` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_spark_migrator_jar_path`](configuration_options/amazon-emr-spark-migrator.md#emr_spark_migrator_jar_path) | `SCT_EMR_SPARK_MIGRATOR_JAR_PATH` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_spark_migrator_release`](configuration_options/amazon-emr-spark-migrator.md#emr_spark_migrator_release) | `SCT_EMR_SPARK_MIGRATOR_RELEASE` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`emr_spot_bid_percentage`](configuration_options/amazon-emr-spark-migrator.md#emr_spot_bid_percentage) | `SCT_EMR_SPOT_BID_PERCENTAGE` | [Amazon EMR (spark-migrator)](configuration_options/amazon-emr-spark-migrator.md) |
| [`enable_argus`](configuration_options/monitoring-events-and-reporting.md#enable_argus) | `SCT_ENABLE_ARGUS` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`enable_kernel_panic_checker`](configuration_options/monitoring-events-and-reporting.md#enable_kernel_panic_checker) | `SCT_ENABLE_KERNEL_PANIC_CHECKER` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`enable_kms_key_rotation`](configuration_options/scylla-installation-and-configuration.md#enable_kms_key_rotation) | `SCT_ENABLE_KMS_KEY_ROTATION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`enable_tablets_on_upgrade`](configuration_options/upgrade-tests.md#enable_tablets_on_upgrade) | `SCT_ENABLE_TABLETS_ON_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`enable_truncate_checks_on_node_upgrade`](configuration_options/upgrade-tests.md#enable_truncate_checks_on_node_upgrade) | `SCT_ENABLE_TRUNCATE_CHECKS_ON_NODE_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`enable_views_with_tablets_on_upgrade`](configuration_options/upgrade-tests.md#enable_views_with_tablets_on_upgrade) | `SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`endpoint_snitch`](configuration_options/scylla-installation-and-configuration.md#endpoint_snitch) | `SCT_ENDPOINT_SNITCH` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`enterprise_disable_kms`](configuration_options/scylla-installation-and-configuration.md#enterprise_disable_kms) | `SCT_ENTERPRISE_DISABLE_KMS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`events_limit_in_email`](configuration_options/monitoring-events-and-reporting.md#events_limit_in_email) | `SCT_EVENTS_LIMIT_IN_EMAIL` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`execute_post_behavior`](configuration_options/logs-diagnostics-and-teardown.md#execute_post_behavior) | `SCT_EXECUTE_POST_BEHAVIOR` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`experimental_features`](configuration_options/scylla-installation-and-configuration.md#experimental_features) | `SCT_EXPERIMENTAL_FEATURES` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`extra_network_interface`](configuration_options/aws-backend.md#extra_network_interface) | `SCT_EXTRA_NETWORK_INTERFACE` | [AWS backend](configuration_options/aws-backend.md) |
| [`fallback_to_next_availability_zone`](configuration_options/general-and-provisioning.md#fallback_to_next_availability_zone) | `SCT_FALLBACK_TO_NEXT_AVAILABILITY_ZONE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`fallback_to_next_region`](configuration_options/general-and-provisioning.md#fallback_to_next_region) | `SCT_FALLBACK_TO_NEXT_REGION` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`flush_period`](configuration_options/refresh-sstable-loading-tests.md#flush_period) | `SCT_FLUSH_PERIOD` | [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) |
| [`flush_times`](configuration_options/refresh-sstable-loading-tests.md#flush_times) | `SCT_FLUSH_TIMES` | [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) |
| [`force_run_iotune`](configuration_options/general-and-provisioning.md#force_run_iotune) | `SCT_FORCE_RUN_IOTUNE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`gce_datacenter`](configuration_options/gce-backend.md#gce_datacenter) | `SCT_GCE_DATACENTER` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_image_db`](configuration_options/gce-backend.md#gce_image_db) | `SCT_GCE_IMAGE_DB` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_image_db_oracle`](configuration_options/gce-backend.md#gce_image_db_oracle) | `SCT_GCE_IMAGE_DB_ORACLE` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_image_loader`](configuration_options/gce-backend.md#gce_image_loader) | `SCT_GCE_IMAGE_LOADER` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_image_monitor`](configuration_options/gce-backend.md#gce_image_monitor) | `SCT_GCE_IMAGE_MONITOR` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_image_username`](configuration_options/gce-backend.md#gce_image_username) | `SCT_GCE_IMAGE_USERNAME` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_instance_type_db`](configuration_options/gce-backend.md#gce_instance_type_db) | `SCT_GCE_INSTANCE_TYPE_DB` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_instance_type_db_oracle`](configuration_options/gce-backend.md#gce_instance_type_db_oracle) | `SCT_GCE_INSTANCE_TYPE_DB_ORACLE` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_instance_type_loader`](configuration_options/gce-backend.md#gce_instance_type_loader) | `SCT_GCE_INSTANCE_TYPE_LOADER` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_instance_type_monitor`](configuration_options/gce-backend.md#gce_instance_type_monitor) | `SCT_GCE_INSTANCE_TYPE_MONITOR` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_n_local_ssd_disk_db`](configuration_options/gce-backend.md#gce_n_local_ssd_disk_db) | `SCT_GCE_N_LOCAL_SSD_DISK_DB` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_n_local_ssd_disk_loader`](configuration_options/gce-backend.md#gce_n_local_ssd_disk_loader) | `SCT_GCE_N_LOCAL_SSD_DISK_LOADER` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_n_local_ssd_disk_monitor`](configuration_options/gce-backend.md#gce_n_local_ssd_disk_monitor) | `SCT_GCE_N_LOCAL_SSD_DISK_MONITOR` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_network`](configuration_options/gce-backend.md#gce_network) | `SCT_GCE_NETWORK` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_pd_ssd_disk_size_db`](configuration_options/gce-backend.md#gce_pd_ssd_disk_size_db) | `SCT_GCE_PD_SSD_DISK_SIZE_DB` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_pd_ssd_disk_size_loader`](configuration_options/gce-backend.md#gce_pd_ssd_disk_size_loader) | `SCT_GCE_PD_SSD_DISK_SIZE_LOADER` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_pd_ssd_disk_size_monitor`](configuration_options/gce-backend.md#gce_pd_ssd_disk_size_monitor) | `SCT_GCE_PD_SSD_DISK_SIZE_MONITOR` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_pd_standard_disk_size_db`](configuration_options/gce-backend.md#gce_pd_standard_disk_size_db) | `SCT_GCE_PD_STANDARD_DISK_SIZE_DB` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_project`](configuration_options/gce-backend.md#gce_project) | `SCT_GCE_PROJECT` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_root_disk_type_db`](configuration_options/gce-backend.md#gce_root_disk_type_db) | `SCT_GCE_ROOT_DISK_TYPE_DB` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_root_disk_type_loader`](configuration_options/gce-backend.md#gce_root_disk_type_loader) | `SCT_GCE_ROOT_DISK_TYPE_LOADER` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_root_disk_type_monitor`](configuration_options/gce-backend.md#gce_root_disk_type_monitor) | `SCT_GCE_ROOT_DISK_TYPE_MONITOR` | [GCE backend](configuration_options/gce-backend.md) |
| [`gce_setup_hybrid_raid`](configuration_options/gce-backend.md#gce_setup_hybrid_raid) | `SCT_GCE_SETUP_HYBRID_RAID` | [GCE backend](configuration_options/gce-backend.md) |
| [`gemini_cmd`](configuration_options/stress-commands-and-load-generation.md#gemini_cmd) | `SCT_GEMINI_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`gemini_log_cql_statements`](configuration_options/stress-commands-and-load-generation.md#gemini_log_cql_statements) | `SCT_GEMINI_LOG_CQL_STATEMENTS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`gemini_schema_url`](configuration_options/stress-commands-and-load-generation.md#gemini_schema_url) | `SCT_GEMINI_SCHEMA_URL` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`gemini_seed`](configuration_options/stress-commands-and-load-generation.md#gemini_seed) | `SCT_GEMINI_SEED` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`gemini_table_options`](configuration_options/stress-commands-and-load-generation.md#gemini_table_options) | `SCT_GEMINI_TABLE_OPTIONS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`gke_cluster_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#gke_cluster_version) | `SCT_GKE_CLUSTER_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`gke_k8s_release_channel`](configuration_options/kubernetes-backends-eks-gke-kind.md#gke_k8s_release_channel) | `SCT_GKE_K8S_RELEASE_CHANNEL` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`hinted_handoff`](configuration_options/scylla-installation-and-configuration.md#hinted_handoff) | `SCT_HINTED_HANDOFF` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`install_cassandra_exporter`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#install_cassandra_exporter) | `SCT_INSTALL_CASSANDRA_EXPORTER` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`install_mode`](configuration_options/scylla-installation-and-configuration.md#install_mode) | `SCT_INSTALL_MODE` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`instance_provision`](configuration_options/general-and-provisioning.md#instance_provision) | `SCT_INSTANCE_PROVISION` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_provision_fallback_on_demand`](configuration_options/general-and-provisioning.md#instance_provision_fallback_on_demand) | `SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_db`](configuration_options/general-and-provisioning.md#instance_type_db) | `SCT_INSTANCE_TYPE_DB` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_db_oracle`](configuration_options/general-and-provisioning.md#instance_type_db_oracle) | `SCT_INSTANCE_TYPE_DB_ORACLE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_db_target`](configuration_options/general-and-provisioning.md#instance_type_db_target) | `SCT_INSTANCE_TYPE_DB_TARGET` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_loader`](configuration_options/general-and-provisioning.md#instance_type_loader) | `SCT_INSTANCE_TYPE_LOADER` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_monitor`](configuration_options/general-and-provisioning.md#instance_type_monitor) | `SCT_INSTANCE_TYPE_MONITOR` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_runner`](configuration_options/general-and-provisioning.md#instance_type_runner) | `SCT_INSTANCE_TYPE_RUNNER` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`instance_type_vector_store`](configuration_options/general-and-provisioning.md#instance_type_vector_store) | `SCT_INSTANCE_TYPE_VECTOR_STORE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`internode_compression`](configuration_options/scylla-installation-and-configuration.md#internode_compression) | `SCT_INTERNODE_COMPRESSION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`internode_encryption`](configuration_options/scylla-installation-and-configuration.md#internode_encryption) | `SCT_INTERNODE_ENCRYPTION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`intra_node_comm_public`](configuration_options/general-and-provisioning.md#intra_node_comm_public) | `SCT_INTRA_NODE_COMM_PUBLIC` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`ip_ssh_connections`](configuration_options/general-and-provisioning.md#ip_ssh_connections) | `SCT_IP_SSH_CONNECTIONS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`jepsen_scylla_repo`](configuration_options/jepsen-tests.md#jepsen_scylla_repo) | `SCT_JEPSEN_SCYLLA_REPO` | [Jepsen tests](configuration_options/jepsen-tests.md) |
| [`jepsen_test_cmd`](configuration_options/jepsen-tests.md#jepsen_test_cmd) | `SCT_JEPSEN_TEST_CMD` | [Jepsen tests](configuration_options/jepsen-tests.md) |
| [`jepsen_test_count`](configuration_options/jepsen-tests.md#jepsen_test_count) | `SCT_JEPSEN_TEST_COUNT` | [Jepsen tests](configuration_options/jepsen-tests.md) |
| [`jepsen_test_run_policy`](configuration_options/jepsen-tests.md#jepsen_test_run_policy) | `SCT_JEPSEN_TEST_RUN_POLICY` | [Jepsen tests](configuration_options/jepsen-tests.md) |
| [`jmx_heap_memory`](configuration_options/scylla-installation-and-configuration.md#jmx_heap_memory) | `SCT_JMX_HEAP_MEMORY` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`k8s_cert_manager_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_cert_manager_version) | `SCT_K8S_CERT_MANAGER_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_connection_bundle_file`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_connection_bundle_file) | `SCT_K8S_CONNECTION_BUNDLE_FILE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_db_node_service_type`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_db_node_service_type) | `SCT_K8S_DB_NODE_SERVICE_TYPE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_db_node_to_client_broadcast_ip_type`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_db_node_to_client_broadcast_ip_type) | `SCT_K8S_DB_NODE_TO_CLIENT_BROADCAST_IP_TYPE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_db_node_to_node_broadcast_ip_type`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_db_node_to_node_broadcast_ip_type) | `SCT_K8S_DB_NODE_TO_NODE_BROADCAST_IP_TYPE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_deploy_monitoring`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_deploy_monitoring) | `SCT_K8S_DEPLOY_MONITORING` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_enable_alternator`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_enable_alternator) | `SCT_K8S_ENABLE_ALTERNATOR` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_enable_performance_tuning`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_enable_performance_tuning) | `SCT_K8S_ENABLE_PERFORMANCE_TUNING` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_enable_sni`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_enable_sni) | `SCT_K8S_ENABLE_SNI` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_enable_tls`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_enable_tls) | `SCT_K8S_ENABLE_TLS` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_functional_test_dataset`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_functional_test_dataset) | `SCT_K8S_FUNCTIONAL_TEST_DATASET` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_instance_type_auxiliary`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_instance_type_auxiliary) | `SCT_K8S_INSTANCE_TYPE_AUXILIARY` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_instance_type_monitor`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_instance_type_monitor) | `SCT_K8S_INSTANCE_TYPE_MONITOR` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_loader_cluster_name`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_loader_cluster_name) | `SCT_K8S_LOADER_CLUSTER_NAME` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_loader_run_type`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_loader_run_type) | `SCT_K8S_LOADER_RUN_TYPE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_local_volume_provisioner_type`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_local_volume_provisioner_type) | `SCT_K8S_LOCAL_VOLUME_PROVISIONER_TYPE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_log_api_calls`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_log_api_calls) | `SCT_K8S_LOG_API_CALLS` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_minio_storage_size`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_minio_storage_size) | `SCT_K8S_MINIO_STORAGE_SIZE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_n_auxiliary_nodes`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_n_auxiliary_nodes) | `SCT_K8S_N_AUXILIARY_NODES` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_n_loader_pods_per_cluster`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_n_loader_pods_per_cluster) | `SCT_K8S_N_LOADER_PODS_PER_CLUSTER` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_n_monitor_nodes`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_n_monitor_nodes) | `SCT_K8S_N_MONITOR_NODES` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_n_scylla_pods_per_cluster`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_n_scylla_pods_per_cluster) | `SCT_K8S_N_SCYLLA_PODS_PER_CLUSTER` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_cluster_name`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_cluster_name) | `SCT_K8S_SCYLLA_CLUSTER_NAME` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_cpu_limit`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_cpu_limit) | `SCT_K8S_SCYLLA_CPU_LIMIT` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_disk_class`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_disk_class) | `SCT_K8S_SCYLLA_DISK_CLASS` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_disk_gi`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_disk_gi) | `SCT_K8S_SCYLLA_DISK_GI` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_memory_limit`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_memory_limit) | `SCT_K8S_SCYLLA_MEMORY_LIMIT` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_operator_chart_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_operator_chart_version) | `SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_operator_docker_image`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_operator_docker_image) | `SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_operator_helm_repo`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_operator_helm_repo) | `SCT_K8S_SCYLLA_OPERATOR_HELM_REPO` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_operator_upgrade_chart_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_operator_upgrade_chart_version) | `SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_operator_upgrade_docker_image`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_operator_upgrade_docker_image) | `SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_operator_upgrade_helm_repo`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_operator_upgrade_helm_repo) | `SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_scylla_utils_docker_image`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_scylla_utils_docker_image) | `SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`k8s_use_chaos_mesh`](configuration_options/kubernetes-backends-eks-gke-kind.md#k8s_use_chaos_mesh) | `SCT_K8S_USE_CHAOS_MESH` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`kafka_backend`](configuration_options/kafka-cdc-connectors.md#kafka_backend) | `SCT_KAFKA_BACKEND` | [Kafka / CDC connectors](configuration_options/kafka-cdc-connectors.md) |
| [`kafka_connectors`](configuration_options/kafka-cdc-connectors.md#kafka_connectors) | `SCT_KAFKA_CONNECTORS` | [Kafka / CDC connectors](configuration_options/kafka-cdc-connectors.md) |
| [`keyspace_num`](configuration_options/stress-commands-and-load-generation.md#keyspace_num) | `SCT_KEYSPACE_NUM` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`keystore_backend`](configuration_options/general-and-provisioning.md#keystore_backend) | `SCT_KEYSTORE_BACKEND` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`keystore_sm_prefix`](configuration_options/general-and-provisioning.md#keystore_sm_prefix) | `SCT_KEYSTORE_SM_PREFIX` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`keystore_sm_region`](configuration_options/general-and-provisioning.md#keystore_sm_region) | `SCT_KEYSTORE_SM_REGION` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`kms_key_rotation_interval`](configuration_options/scylla-installation-and-configuration.md#kms_key_rotation_interval) | `SCT_KMS_KEY_ROTATION_INTERVAL` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`large_partition_stress_during_upgrade`](configuration_options/upgrade-tests.md#large_partition_stress_during_upgrade) | `SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`latency_decorator_error_thresholds`](configuration_options/general-and-provisioning.md#latency_decorator_error_thresholds) | `SCT_LATENCY_DECORATOR_ERROR_THRESHOLDS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`latte_schema_parameters`](configuration_options/stress-commands-and-load-generation.md#latte_schema_parameters) | `SCT_LATTE_SCHEMA_PARAMETERS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`ldap_server_type`](configuration_options/scylla-installation-and-configuration.md#ldap_server_type) | `SCT_LDAP_SERVER_TYPE` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`loader_swap_size`](configuration_options/stress-commands-and-load-generation.md#loader_swap_size) | `SCT_LOADER_SWAP_SIZE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`loaders_private_ip`](configuration_options/baremetal-backend.md#loaders_private_ip) | `SCT_LOADERS_PRIVATE_IP` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`loaders_public_ip`](configuration_options/baremetal-backend.md#loaders_public_ip) | `SCT_LOADERS_PUBLIC_IP` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`logs_transport`](configuration_options/logs-diagnostics-and-teardown.md#logs_transport) | `SCT_LOGS_TRANSPORT` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`manager_backup_restore_method`](configuration_options/scylla-manager.md#manager_backup_restore_method) | `SCT_MANAGER_BACKUP_RESTORE_METHOD` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`manager_prometheus_port`](configuration_options/scylla-manager.md#manager_prometheus_port) | `SCT_MANAGER_PROMETHEUS_PORT` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`manager_scylla_backend_version`](configuration_options/scylla-manager.md#manager_scylla_backend_version) | `SCT_MANAGER_SCYLLA_BACKEND_VERSION` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`manager_version`](configuration_options/scylla-manager.md#manager_version) | `SCT_MANAGER_VERSION` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`max_deviation`](configuration_options/performance-regression-tests.md#max_deviation) | `SCT_MAX_DEVIATION` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`max_events_severities`](configuration_options/monitoring-events-and-reporting.md#max_events_severities) | `SCT_MAX_EVENTS_SEVERITIES` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`mgmt_agent_backup_config`](configuration_options/scylla-manager.md#mgmt_agent_backup_config) | `SCT_MGMT_AGENT_BACKUP_CONFIG` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_docker_image`](configuration_options/scylla-manager.md#mgmt_docker_image) | `SCT_MGMT_DOCKER_IMAGE` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_nodetool_refresh_flags`](configuration_options/scylla-manager.md#mgmt_nodetool_refresh_flags) | `SCT_MGMT_NODETOOL_REFRESH_FLAGS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_prepare_snapshot_size`](configuration_options/scylla-manager.md#mgmt_prepare_snapshot_size) | `SCT_MGMT_PREPARE_SNAPSHOT_SIZE` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_restore_extra_params`](configuration_options/scylla-manager.md#mgmt_restore_extra_params) | `SCT_MGMT_RESTORE_EXTRA_PARAMS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_reuse_backup_snapshot_name`](configuration_options/scylla-manager.md#mgmt_reuse_backup_snapshot_name) | `SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_skip_post_restore_stress_read`](configuration_options/scylla-manager.md#mgmt_skip_post_restore_stress_read) | `SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`mgmt_snapshots_preparer_params`](configuration_options/scylla-manager.md#mgmt_snapshots_preparer_params) | `SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`migrator_run_validator`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_run_validator) | `SCT_MIGRATOR_RUN_VALIDATOR` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_source_hosts`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_source_hosts) | `SCT_MIGRATOR_SOURCE_HOSTS` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_source_keyspace`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_source_keyspace) | `SCT_MIGRATOR_SOURCE_KEYSPACE` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_source_table`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_source_table) | `SCT_MIGRATOR_SOURCE_TABLE` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_source_test_id`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_source_test_id) | `SCT_MIGRATOR_SOURCE_TEST_ID` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_step_timeout_minutes`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_step_timeout_minutes) | `SCT_MIGRATOR_STEP_TIMEOUT_MINUTES` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_target_keyspace`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_target_keyspace) | `SCT_MIGRATOR_TARGET_KEYSPACE` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`migrator_target_table`](configuration_options/spark-migrator-cassandra-to-scylla.md#migrator_target_table) | `SCT_MIGRATOR_TARGET_TABLE` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`mini_k8s_version`](configuration_options/kubernetes-backends-eks-gke-kind.md#mini_k8s_version) | `SCT_MINI_K8S_VERSION` | [Kubernetes backends (EKS/GKE/kind)](configuration_options/kubernetes-backends-eks-gke-kind.md) |
| [`minicloud_container_cpus`](configuration_options/minicloud.md#minicloud_container_cpus) | `SCT_MINICLOUD_CONTAINER_CPUS` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_container_memory`](configuration_options/minicloud.md#minicloud_container_memory) | `SCT_MINICLOUD_CONTAINER_MEMORY` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_container_name`](configuration_options/minicloud.md#minicloud_container_name) | `SCT_MINICLOUD_CONTAINER_NAME` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_docker_image`](configuration_options/minicloud.md#minicloud_docker_image) | `SCT_MINICLOUD_DOCKER_IMAGE` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_endpoint_url`](configuration_options/minicloud.md#minicloud_endpoint_url) | `SCT_MINICLOUD_ENDPOINT_URL` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_gcs_bucket`](configuration_options/minicloud.md#minicloud_gcs_bucket) | `SCT_MINICLOUD_GCS_BUCKET` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_keep_alive`](configuration_options/minicloud.md#minicloud_keep_alive) | `SCT_MINICLOUD_KEEP_ALIVE` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_lightweight`](configuration_options/minicloud.md#minicloud_lightweight) | `SCT_MINICLOUD_LIGHTWEIGHT` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_lightweight_memory`](configuration_options/minicloud.md#minicloud_lightweight_memory) | `SCT_MINICLOUD_LIGHTWEIGHT_MEMORY` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_lightweight_vcpus`](configuration_options/minicloud.md#minicloud_lightweight_vcpus) | `SCT_MINICLOUD_LIGHTWEIGHT_VCPUS` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_regions`](configuration_options/minicloud.md#minicloud_regions) | `SCT_MINICLOUD_REGIONS` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_s3_passthrough_buckets`](configuration_options/minicloud.md#minicloud_s3_passthrough_buckets) | `SCT_MINICLOUD_S3_PASSTHROUGH_BUCKETS` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_skip_memory_check`](configuration_options/minicloud.md#minicloud_skip_memory_check) | `SCT_MINICLOUD_SKIP_MEMORY_CHECK` | [Minicloud](configuration_options/minicloud.md) |
| [`minicloud_state_dir`](configuration_options/minicloud.md#minicloud_state_dir) | `SCT_MINICLOUD_STATE_DIR` | [Minicloud](configuration_options/minicloud.md) |
| [`monitor_branch`](configuration_options/monitoring-events-and-reporting.md#monitor_branch) | `SCT_MONITOR_BRANCH` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`monitor_nodes_private_ip`](configuration_options/baremetal-backend.md#monitor_nodes_private_ip) | `SCT_MONITOR_NODES_PRIVATE_IP` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`monitor_nodes_public_ip`](configuration_options/baremetal-backend.md#monitor_nodes_public_ip) | `SCT_MONITOR_NODES_PUBLIC_IP` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`monitor_swap_size`](configuration_options/monitoring-events-and-reporting.md#monitor_swap_size) | `SCT_MONITOR_SWAP_SIZE` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`n_db_nodes`](configuration_options/general-and-provisioning.md#n_db_nodes) | `SCT_N_DB_NODES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`n_db_zero_token_nodes`](configuration_options/general-and-provisioning.md#n_db_zero_token_nodes) | `SCT_N_DB_ZERO_TOKEN_NODES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`n_loaders`](configuration_options/general-and-provisioning.md#n_loaders) | `SCT_N_LOADERS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`n_monitor_nodes`](configuration_options/general-and-provisioning.md#n_monitor_nodes) | `SCT_N_MONITOR_NODES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`n_stress_process`](configuration_options/performance-regression-tests.md#n_stress_process) | `SCT_N_STRESS_PROCESS` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`n_test_oracle_db_nodes`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#n_test_oracle_db_nodes) | `SCT_N_TEST_ORACLE_DB_NODES` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`n_vector_store_nodes`](configuration_options/vector-store.md#n_vector_store_nodes) | `SCT_N_VECTOR_STORE_NODES` | [Vector Store](configuration_options/vector-store.md) |
| [`nemesis_add_node_cnt`](configuration_options/nemesis-chaos-testing.md#nemesis_add_node_cnt) | `SCT_NEMESIS_ADD_NODE_CNT` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_class_name`](configuration_options/nemesis-chaos-testing.md#nemesis_class_name) | `SCT_NEMESIS_CLASS_NAME` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_double_load_during_grow_shrink_duration`](configuration_options/nemesis-chaos-testing.md#nemesis_double_load_during_grow_shrink_duration) | `SCT_NEMESIS_DOUBLE_LOAD_DURING_GROW_SHRINK_DURATION` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_during_prepare`](configuration_options/nemesis-chaos-testing.md#nemesis_during_prepare) | `SCT_NEMESIS_DURING_PREPARE` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_filter_seeds`](configuration_options/nemesis-chaos-testing.md#nemesis_filter_seeds) | `SCT_NEMESIS_FILTER_SEEDS` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_grow_shrink_instance_type`](configuration_options/nemesis-chaos-testing.md#nemesis_grow_shrink_instance_type) | `SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_interval`](configuration_options/nemesis-chaos-testing.md#nemesis_interval) | `SCT_NEMESIS_INTERVAL` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_multiply_factor`](configuration_options/nemesis-chaos-testing.md#nemesis_multiply_factor) | `SCT_NEMESIS_MULTIPLY_FACTOR` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_seed`](configuration_options/nemesis-chaos-testing.md#nemesis_seed) | `SCT_NEMESIS_SEED` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_selector`](configuration_options/nemesis-chaos-testing.md#nemesis_selector) | `SCT_NEMESIS_SELECTOR` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`nemesis_sequence_sleep_between_ops`](configuration_options/nemesis-chaos-testing.md#nemesis_sequence_sleep_between_ops) | `SCT_NEMESIS_SEQUENCE_SLEEP_BETWEEN_OPS` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`new_scylla_repo`](configuration_options/upgrade-tests.md#new_scylla_repo) | `SCT_NEW_SCYLLA_REPO` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`new_version`](configuration_options/upgrade-tests.md#new_version) | `SCT_NEW_VERSION` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`nonroot_offline_install`](configuration_options/scylla-installation-and-configuration.md#nonroot_offline_install) | `SCT_NONROOT_OFFLINE_INSTALL` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`num_loaders_step`](configuration_options/performance-regression-tests.md#num_loaders_step) | `SCT_NUM_LOADERS_STEP` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`num_nodes_to_rollback`](configuration_options/upgrade-tests.md#num_nodes_to_rollback) | `SCT_NUM_NODES_TO_ROLLBACK` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`num_threads_step`](configuration_options/performance-regression-tests.md#num_threads_step) | `SCT_NUM_THREADS_STEP` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`nvme_self_test_type`](configuration_options/logs-diagnostics-and-teardown.md#nvme_self_test_type) | `SCT_NVME_SELF_TEST_TYPE` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`oci_image_db`](configuration_options/oci-backend.md#oci_image_db) | `SCT_OCI_IMAGE_DB` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_image_db_oracle`](configuration_options/oci-backend.md#oci_image_db_oracle) | `SCT_OCI_IMAGE_DB_ORACLE` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_image_loader`](configuration_options/oci-backend.md#oci_image_loader) | `SCT_OCI_IMAGE_LOADER` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_image_monitor`](configuration_options/oci-backend.md#oci_image_monitor) | `SCT_OCI_IMAGE_MONITOR` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_image_username`](configuration_options/oci-backend.md#oci_image_username) | `SCT_OCI_IMAGE_USERNAME` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_instance_type_db`](configuration_options/oci-backend.md#oci_instance_type_db) | `SCT_OCI_INSTANCE_TYPE_DB` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_instance_type_db_oracle`](configuration_options/oci-backend.md#oci_instance_type_db_oracle) | `SCT_OCI_INSTANCE_TYPE_DB_ORACLE` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_instance_type_loader`](configuration_options/oci-backend.md#oci_instance_type_loader) | `SCT_OCI_INSTANCE_TYPE_LOADER` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_instance_type_monitor`](configuration_options/oci-backend.md#oci_instance_type_monitor) | `SCT_OCI_INSTANCE_TYPE_MONITOR` | [OCI backend](configuration_options/oci-backend.md) |
| [`oci_region_name`](configuration_options/oci-backend.md#oci_region_name) | `SCT_OCI_REGION_NAME` | [OCI backend](configuration_options/oci-backend.md) |
| [`oracle_scylla_version`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#oracle_scylla_version) | `SCT_ORACLE_SCYLLA_VERSION` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`oracle_user_data_format_version`](configuration_options/auxiliary-db-cluster-oracle-cassandra.md#oracle_user_data_format_version) | `SCT_ORACLE_USER_DATA_FORMAT_VERSION` | [Auxiliary DB cluster (oracle / Cassandra)](configuration_options/auxiliary-db-cluster-oracle-cassandra.md) |
| [`parallel_node_operations`](configuration_options/general-and-provisioning.md#parallel_node_operations) | `SCT_PARALLEL_NODE_OPERATIONS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`peer_verification`](configuration_options/scylla-installation-and-configuration.md#peer_verification) | `SCT_PEER_VERIFICATION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`perf_gradual_step_duration`](configuration_options/performance-regression-tests.md#perf_gradual_step_duration) | `SCT_PERF_GRADUAL_STEP_DURATION` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`perf_gradual_threads`](configuration_options/performance-regression-tests.md#perf_gradual_threads) | `SCT_PERF_GRADUAL_THREADS` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`perf_gradual_throttle_steps`](configuration_options/performance-regression-tests.md#perf_gradual_throttle_steps) | `SCT_PERF_GRADUAL_THROTTLE_STEPS` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`perf_gradual_write_preload_data`](configuration_options/performance-regression-tests.md#perf_gradual_write_preload_data) | `SCT_PERF_GRADUAL_WRITE_PRELOAD_DATA` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`perf_simple_query_extra_command`](configuration_options/performance-regression-tests.md#perf_simple_query_extra_command) | `SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`perf_stress_keyspace`](configuration_options/performance-regression-tests.md#perf_stress_keyspace) | `SCT_PERF_STRESS_KEYSPACE` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`perf_stress_table`](configuration_options/performance-regression-tests.md#perf_stress_table) | `SCT_PERF_STRESS_TABLE` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`post_behavior_db_nodes`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_db_nodes) | `SCT_POST_BEHAVIOR_DB_NODES` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_behavior_dedicated_host`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_dedicated_host) | `SCT_POST_BEHAVIOR_DEDICATED_HOST` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_behavior_emr_cluster`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_emr_cluster) | `SCT_POST_BEHAVIOR_EMR_CLUSTER` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_behavior_k8s_cluster`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_k8s_cluster) | `SCT_POST_BEHAVIOR_K8S_CLUSTER` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_behavior_loader_nodes`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_loader_nodes) | `SCT_POST_BEHAVIOR_LOADER_NODES` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_behavior_monitor_nodes`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_monitor_nodes) | `SCT_POST_BEHAVIOR_MONITOR_NODES` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_behavior_vector_store_nodes`](configuration_options/logs-diagnostics-and-teardown.md#post_behavior_vector_store_nodes) | `SCT_POST_BEHAVIOR_VECTOR_STORE_NODES` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`post_prepare_cql_cmds`](configuration_options/longevity-tests.md#post_prepare_cql_cmds) | `SCT_POST_PREPARE_CQL_CMDS` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`pre_create_keyspace`](configuration_options/longevity-tests.md#pre_create_keyspace) | `SCT_PRE_CREATE_KEYSPACE` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`pre_create_schema`](configuration_options/longevity-tests.md#pre_create_schema) | `SCT_PRE_CREATE_SCHEMA` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`pre_filter_unavailable_availability_zones`](configuration_options/general-and-provisioning.md#pre_filter_unavailable_availability_zones) | `SCT_PRE_FILTER_UNAVAILABLE_AVAILABILITY_ZONES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`pre_flight_capacity_probe`](configuration_options/general-and-provisioning.md#pre_flight_capacity_probe) | `SCT_PRE_FLIGHT_CAPACITY_PROBE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`prepare_cs_user_profiles`](configuration_options/stress-commands-and-load-generation.md#prepare_cs_user_profiles) | `SCT_PREPARE_CS_USER_PROFILES` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`prepare_saslauthd`](configuration_options/scylla-installation-and-configuration.md#prepare_saslauthd) | `SCT_PREPARE_SASLAUTHD` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`prepare_stress_cmd`](configuration_options/stress-commands-and-load-generation.md#prepare_stress_cmd) | `SCT_PREPARE_STRESS_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`prepare_stress_duration`](configuration_options/stress-commands-and-load-generation.md#prepare_stress_duration) | `SCT_PREPARE_STRESS_DURATION` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`prepare_verify_cmd`](configuration_options/stress-commands-and-load-generation.md#prepare_verify_cmd) | `SCT_PREPARE_VERIFY_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`prepare_wait_no_compactions_timeout`](configuration_options/stress-commands-and-load-generation.md#prepare_wait_no_compactions_timeout) | `SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`prepare_write_cmd`](configuration_options/stress-commands-and-load-generation.md#prepare_write_cmd) | `SCT_PREPARE_WRITE_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`prepare_write_stress`](configuration_options/stress-commands-and-load-generation.md#prepare_write_stress) | `SCT_PREPARE_WRITE_STRESS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`print_kernel_callstack`](configuration_options/monitoring-events-and-reporting.md#print_kernel_callstack) | `SCT_PRINT_KERNEL_CALLSTACK` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`rack_aware_loader`](configuration_options/stress-commands-and-load-generation.md#rack_aware_loader) | `SCT_RACK_AWARE_LOADER` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`raid_level`](configuration_options/general-and-provisioning.md#raid_level) | `SCT_RAID_LEVEL` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`region_aware_loader`](configuration_options/stress-commands-and-load-generation.md#region_aware_loader) | `SCT_REGION_AWARE_LOADER` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`region_name`](configuration_options/general-and-provisioning.md#region_name) | `SCT_REGION_NAME` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`reuse_cluster`](configuration_options/general-and-provisioning.md#reuse_cluster) | `SCT_REUSE_CLUSTER` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`root_disk_size_db`](configuration_options/general-and-provisioning.md#root_disk_size_db) | `SCT_ROOT_DISK_SIZE_DB` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`root_disk_size_loader`](configuration_options/general-and-provisioning.md#root_disk_size_loader) | `SCT_ROOT_DISK_SIZE_LOADER` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`root_disk_size_monitor`](configuration_options/general-and-provisioning.md#root_disk_size_monitor) | `SCT_ROOT_DISK_SIZE_MONITOR` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`root_disk_size_runner`](configuration_options/general-and-provisioning.md#root_disk_size_runner) | `SCT_ROOT_DISK_SIZE_RUNNER` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`round_robin`](configuration_options/stress-commands-and-load-generation.md#round_robin) | `SCT_ROUND_ROBIN` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`run_commit_log_check_thread`](configuration_options/longevity-tests.md#run_commit_log_check_thread) | `SCT_RUN_COMMIT_LOG_CHECK_THREAD` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`run_db_node_benchmarks`](configuration_options/performance-regression-tests.md#run_db_node_benchmarks) | `SCT_RUN_DB_NODE_BENCHMARKS` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`run_full_partition_scan`](configuration_options/longevity-tests.md#run_full_partition_scan) | `SCT_RUN_FULL_PARTITION_SCAN` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`run_fullscan`](configuration_options/longevity-tests.md#run_fullscan) | `SCT_RUN_FULLSCAN` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`run_gemini_in_rolling_upgrade`](configuration_options/upgrade-tests.md#run_gemini_in_rolling_upgrade) | `SCT_RUN_GEMINI_IN_ROLLING_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`run_scylla_doctor`](configuration_options/scylla-doctor.md#run_scylla_doctor) | `SCT_RUN_SCYLLA_DOCTOR` | [Scylla Doctor](configuration_options/scylla-doctor.md) |
| [`run_scylla_doctor_only`](configuration_options/scylla-doctor.md#run_scylla_doctor_only) | `SCT_RUN_SCYLLA_DOCTOR_ONLY` | [Scylla Doctor](configuration_options/scylla-doctor.md) |
| [`run_tombstone_gc_verification`](configuration_options/longevity-tests.md#run_tombstone_gc_verification) | `SCT_RUN_TOMBSTONE_GC_VERIFICATION` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`s3_baremetal_config`](configuration_options/baremetal-backend.md#s3_baremetal_config) | `SCT_S3_BAREMETAL_CONFIG` | [Baremetal backend](configuration_options/baremetal-backend.md) |
| [`sct_aws_account_id`](configuration_options/aws-backend.md#sct_aws_account_id) | `SCT_SCT_AWS_ACCOUNT_ID` | [AWS backend](configuration_options/aws-backend.md) |
| [`sct_ngrok_name`](configuration_options/monitoring-events-and-reporting.md#sct_ngrok_name) | `SCT_SCT_NGROK_NAME` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`sct_public_ip`](configuration_options/general-and-provisioning.md#sct_public_ip) | `SCT_SCT_PUBLIC_IP` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`scylla_apt_keys`](configuration_options/scylla-installation-and-configuration.md#scylla_apt_keys) | `SCT_SCYLLA_APT_KEYS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_d_overrides_files`](configuration_options/scylla-installation-and-configuration.md#scylla_d_overrides_files) | `SCT_SCYLLA_D_OVERRIDES_FILES` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_doctor_edition`](configuration_options/scylla-doctor.md#scylla_doctor_edition) | `SCT_SCYLLA_DOCTOR_EDITION` | [Scylla Doctor](configuration_options/scylla-doctor.md) |
| [`scylla_doctor_full_tarball_url`](configuration_options/scylla-doctor.md#scylla_doctor_full_tarball_url) | `SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL` | [Scylla Doctor](configuration_options/scylla-doctor.md) |
| [`scylla_doctor_version`](configuration_options/scylla-doctor.md#scylla_doctor_version) | `SCT_SCYLLA_DOCTOR_VERSION` | [Scylla Doctor](configuration_options/scylla-doctor.md) |
| [`scylla_encryption_options`](configuration_options/scylla-installation-and-configuration.md#scylla_encryption_options) | `SCT_SCYLLA_ENCRYPTION_OPTIONS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_linux_distro`](configuration_options/scylla-installation-and-configuration.md#scylla_linux_distro) | `SCT_SCYLLA_LINUX_DISTRO` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_linux_distro_loader`](configuration_options/scylla-installation-and-configuration.md#scylla_linux_distro_loader) | `SCT_SCYLLA_LINUX_DISTRO_LOADER` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_mgmt_address`](configuration_options/scylla-manager.md#scylla_mgmt_address) | `SCT_SCYLLA_MGMT_ADDRESS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`scylla_mgmt_agent_address`](configuration_options/scylla-manager.md#scylla_mgmt_agent_address) | `SCT_SCYLLA_MGMT_AGENT_ADDRESS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`scylla_mgmt_agent_version`](configuration_options/scylla-manager.md#scylla_mgmt_agent_version) | `SCT_SCYLLA_MGMT_AGENT_VERSION` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`scylla_mgmt_pkg`](configuration_options/scylla-manager.md#scylla_mgmt_pkg) | `SCT_SCYLLA_MGMT_PKG` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`scylla_mgmt_upgrade_to_repo`](configuration_options/scylla-manager.md#scylla_mgmt_upgrade_to_repo) | `SCT_SCYLLA_MGMT_UPGRADE_TO_REPO` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`scylla_network_config`](configuration_options/scylla-installation-and-configuration.md#scylla_network_config) | `SCT_SCYLLA_NETWORK_CONFIG` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_repo`](configuration_options/scylla-installation-and-configuration.md#scylla_repo) | `SCT_SCYLLA_REPO` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`scylla_repo_m`](configuration_options/scylla-manager.md#scylla_repo_m) | `SCT_SCYLLA_REPO_M` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`scylla_rsyslog_setup`](configuration_options/monitoring-events-and-reporting.md#scylla_rsyslog_setup) | `SCT_SCYLLA_RSYSLOG_SETUP` | [Monitoring, events and reporting](configuration_options/monitoring-events-and-reporting.md) |
| [`scylla_version`](configuration_options/scylla-installation-and-configuration.md#scylla_version) | `SCT_SCYLLA_VERSION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`seeds_num`](configuration_options/general-and-provisioning.md#seeds_num) | `SCT_SEEDS_NUM` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`seeds_selector`](configuration_options/general-and-provisioning.md#seeds_selector) | `SCT_SEEDS_SELECTOR` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`server_encrypt`](configuration_options/scylla-installation-and-configuration.md#server_encrypt) | `SCT_SERVER_ENCRYPT` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`server_encrypt_mtls`](configuration_options/scylla-installation-and-configuration.md#server_encrypt_mtls) | `SCT_SERVER_ENCRYPT_MTLS` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`service_level_shares`](configuration_options/scylla-installation-and-configuration.md#service_level_shares) | `SCT_SERVICE_LEVEL_SHARES` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`simulated_racks`](configuration_options/general-and-provisioning.md#simulated_racks) | `SCT_SIMULATED_RACKS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`simulated_regions`](configuration_options/general-and-provisioning.md#simulated_regions) | `SCT_SIMULATED_REGIONS` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`sizing_db`](configuration_options/general-and-provisioning.md#sizing_db) | `SCT_SIZING_DB` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`sizing_db_oracle`](configuration_options/general-and-provisioning.md#sizing_db_oracle) | `SCT_SIZING_DB_ORACLE` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`sizing_loader`](configuration_options/general-and-provisioning.md#sizing_loader) | `SCT_SIZING_LOADER` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`sizing_monitor`](configuration_options/general-and-provisioning.md#sizing_monitor) | `SCT_SIZING_MONITOR` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`skip_download`](configuration_options/refresh-sstable-loading-tests.md#skip_download) | `SCT_SKIP_DOWNLOAD` | [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) |
| [`skip_test_stages`](configuration_options/general-and-provisioning.md#skip_test_stages) | `SCT_SKIP_TEST_STAGES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`sla`](configuration_options/nemesis-chaos-testing.md#sla) | `SCT_SLA` | [Nemesis (chaos testing)](configuration_options/nemesis-chaos-testing.md) |
| [`space_node_threshold`](configuration_options/longevity-tests.md#space_node_threshold) | `SCT_SPACE_NODE_THRESHOLD` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`spot_max_price`](configuration_options/aws-backend.md#spot_max_price) | `SCT_SPOT_MAX_PRICE` | [AWS backend](configuration_options/aws-backend.md) |
| [`ssh_transport`](configuration_options/general-and-provisioning.md#ssh_transport) | `SCT_SSH_TRANSPORT` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`sstable_file`](configuration_options/refresh-sstable-loading-tests.md#sstable_file) | `SCT_SSTABLE_FILE` | [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) |
| [`sstable_md5`](configuration_options/refresh-sstable-loading-tests.md#sstable_md5) | `SCT_SSTABLE_MD5` | [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) |
| [`sstable_size`](configuration_options/longevity-tests.md#sstable_size) | `SCT_SSTABLE_SIZE` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`sstable_url`](configuration_options/refresh-sstable-loading-tests.md#sstable_url) | `SCT_SSTABLE_URL` | [Refresh (sstable loading) tests](configuration_options/refresh-sstable-loading-tests.md) |
| [`stop_on_hw_perf_failure`](configuration_options/performance-regression-tests.md#stop_on_hw_perf_failure) | `SCT_STOP_ON_HW_PERF_FAILURE` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`stop_test_on_stress_failure`](configuration_options/stress-commands-and-load-generation.md#stop_test_on_stress_failure) | `SCT_STOP_TEST_ON_STRESS_FAILURE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`store_cdclog_reader_stats_in_es`](configuration_options/stress-commands-and-load-generation.md#store_cdclog_reader_stats_in_es) | `SCT_STORE_CDCLOG_READER_STATS_IN_ES` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_after_cluster_upgrade`](configuration_options/upgrade-tests.md#stress_after_cluster_upgrade) | `SCT_STRESS_AFTER_CLUSTER_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`stress_before_migration`](configuration_options/stress-commands-and-load-generation.md#stress_before_migration) | `SCT_STRESS_BEFORE_MIGRATION` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_before_upgrade`](configuration_options/upgrade-tests.md#stress_before_upgrade) | `SCT_STRESS_BEFORE_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`stress_cdc_log_reader_batching_enable`](configuration_options/stress-commands-and-load-generation.md#stress_cdc_log_reader_batching_enable) | `SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cdclog_reader_cmd`](configuration_options/stress-commands-and-load-generation.md#stress_cdclog_reader_cmd) | `SCT_STRESS_CDCLOG_READER_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd`](configuration_options/stress-commands-and-load-generation.md#stress_cmd) | `SCT_STRESS_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_1`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_1) | `SCT_STRESS_CMD_1` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_cache_warmup`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_cache_warmup) | `SCT_STRESS_CMD_CACHE_WARMUP` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_complex_prepare`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_complex_prepare) | `SCT_STRESS_CMD_COMPLEX_PREPARE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_complex_verify_delete`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_complex_verify_delete) | `SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_complex_verify_more`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_complex_verify_more) | `SCT_STRESS_CMD_COMPLEX_VERIFY_MORE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_complex_verify_read`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_complex_verify_read) | `SCT_STRESS_CMD_COMPLEX_VERIFY_READ` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_d`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_d) | `SCT_STRESS_CMD_LWT_D` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_dc`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_dc) | `SCT_STRESS_CMD_LWT_DC` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_de`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_de) | `SCT_STRESS_CMD_LWT_DE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_i`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_i) | `SCT_STRESS_CMD_LWT_I` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_ine`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_ine) | `SCT_STRESS_CMD_LWT_INE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_mixed`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_mixed) | `SCT_STRESS_CMD_LWT_MIXED` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_mixed_baseline`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_mixed_baseline) | `SCT_STRESS_CMD_LWT_MIXED_BASELINE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_u`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_u) | `SCT_STRESS_CMD_LWT_U` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_uc`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_uc) | `SCT_STRESS_CMD_LWT_UC` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_lwt_ue`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_lwt_ue) | `SCT_STRESS_CMD_LWT_UE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_m`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_m) | `SCT_STRESS_CMD_M` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_mv`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_mv) | `SCT_STRESS_CMD_MV` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_no_mv`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_no_mv) | `SCT_STRESS_CMD_NO_MV` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_no_mv_profile`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_no_mv_profile) | `SCT_STRESS_CMD_NO_MV_PROFILE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_r`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_r) | `SCT_STRESS_CMD_R` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_read_10m`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_read_10m) | `SCT_STRESS_CMD_READ_10M` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_read_60m`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_read_60m) | `SCT_STRESS_CMD_READ_60M` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_read_cl_one`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_read_cl_one) | `SCT_STRESS_CMD_READ_CL_ONE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_read_cl_quorum`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_read_cl_quorum) | `SCT_STRESS_CMD_READ_CL_QUORUM` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_read_disk`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_read_disk) | `SCT_STRESS_CMD_READ_DISK` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_cmd_w`](configuration_options/stress-commands-and-load-generation.md#stress_cmd_w) | `SCT_STRESS_CMD_W` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_duration`](configuration_options/stress-commands-and-load-generation.md#stress_duration) | `SCT_STRESS_DURATION` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_during_entire_upgrade`](configuration_options/upgrade-tests.md#stress_during_entire_upgrade) | `SCT_STRESS_DURING_ENTIRE_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`stress_image`](configuration_options/stress-commands-and-load-generation.md#stress_image) | `SCT_STRESS_IMAGE` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_multiplier`](configuration_options/stress-commands-and-load-generation.md#stress_multiplier) | `SCT_STRESS_MULTIPLIER` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_multiplier_m`](configuration_options/stress-commands-and-load-generation.md#stress_multiplier_m) | `SCT_STRESS_MULTIPLIER_M` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_multiplier_r`](configuration_options/stress-commands-and-load-generation.md#stress_multiplier_r) | `SCT_STRESS_MULTIPLIER_R` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_multiplier_w`](configuration_options/stress-commands-and-load-generation.md#stress_multiplier_w) | `SCT_STRESS_MULTIPLIER_W` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_process_step`](configuration_options/performance-regression-tests.md#stress_process_step) | `SCT_STRESS_PROCESS_STEP` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`stress_read_cmd`](configuration_options/stress-commands-and-load-generation.md#stress_read_cmd) | `SCT_STRESS_READ_CMD` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_step_duration`](configuration_options/performance-regression-tests.md#stress_step_duration) | `SCT_STRESS_STEP_DURATION` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`stress_template_context`](configuration_options/stress-commands-and-load-generation.md#stress_template_context) | `SCT_STRESS_TEMPLATE_CONTEXT` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`stress_threads_start_num`](configuration_options/performance-regression-tests.md#stress_threads_start_num) | `SCT_STRESS_THREADS_START_NUM` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`target_manager_version`](configuration_options/scylla-manager.md#target_manager_version) | `SCT_TARGET_MANAGER_VERSION` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`target_scylla_mgmt_agent_address`](configuration_options/scylla-manager.md#target_scylla_mgmt_agent_address) | `SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`target_scylla_mgmt_server_address`](configuration_options/scylla-manager.md#target_scylla_mgmt_server_address) | `SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`target_upgrade_version`](configuration_options/upgrade-tests.md#target_upgrade_version) | `SCT_TARGET_UPGRADE_VERSION` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`teardown_validators`](configuration_options/logs-diagnostics-and-teardown.md#teardown_validators) | `SCT_TEARDOWN_VALIDATORS` | [Logs, diagnostics and teardown](configuration_options/logs-diagnostics-and-teardown.md) |
| [`test_duration`](configuration_options/general-and-provisioning.md#test_duration) | `SCT_TEST_DURATION` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`test_id`](configuration_options/general-and-provisioning.md#test_id) | `SCT_TEST_ID` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`test_metadata`](configuration_options/general-and-provisioning.md#test_metadata) | `SCT_TEST_METADATA` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`test_method`](configuration_options/general-and-provisioning.md#test_method) | `SCT_TEST_METHOD` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`unified_package`](configuration_options/scylla-installation-and-configuration.md#unified_package) | `SCT_UNIFIED_PACKAGE` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`update_db_packages`](configuration_options/scylla-installation-and-configuration.md#update_db_packages) | `SCT_UPDATE_DB_PACKAGES` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`upgrade_node_packages`](configuration_options/upgrade-tests.md#upgrade_node_packages) | `SCT_UPGRADE_NODE_PACKAGES` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`upgrade_node_system`](configuration_options/upgrade-tests.md#upgrade_node_system) | `SCT_UPGRADE_NODE_SYSTEM` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`upgrade_sstables`](configuration_options/upgrade-tests.md#upgrade_sstables) | `SCT_UPGRADE_SSTABLES` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`use_capacity_reservation`](configuration_options/aws-backend.md#use_capacity_reservation) | `SCT_USE_CAPACITY_RESERVATION` | [AWS backend](configuration_options/aws-backend.md) |
| [`use_cloud_manager`](configuration_options/scylla-manager.md#use_cloud_manager) | `SCT_USE_CLOUD_MANAGER` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`use_dedicated_host`](configuration_options/aws-backend.md#use_dedicated_host) | `SCT_USE_DEDICATED_HOST` | [AWS backend](configuration_options/aws-backend.md) |
| [`use_dns_names`](configuration_options/general-and-provisioning.md#use_dns_names) | `SCT_USE_DNS_NAMES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`use_hdrhistogram`](configuration_options/performance-regression-tests.md#use_hdrhistogram) | `SCT_USE_HDRHISTOGRAM` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`use_ldap`](configuration_options/scylla-installation-and-configuration.md#use_ldap) | `SCT_USE_LDAP` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`use_ldap_authentication`](configuration_options/scylla-installation-and-configuration.md#use_ldap_authentication) | `SCT_USE_LDAP_AUTHENTICATION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`use_ldap_authorization`](configuration_options/scylla-installation-and-configuration.md#use_ldap_authorization) | `SCT_USE_LDAP_AUTHORIZATION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`use_legacy_cluster_init`](configuration_options/general-and-provisioning.md#use_legacy_cluster_init) | `SCT_USE_LEGACY_CLUSTER_INIT` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`use_mgmt`](configuration_options/scylla-manager.md#use_mgmt) | `SCT_USE_MGMT` | [Scylla Manager](configuration_options/scylla-manager.md) |
| [`use_placement_group`](configuration_options/aws-backend.md#use_placement_group) | `SCT_USE_PLACEMENT_GROUP` | [AWS backend](configuration_options/aws-backend.md) |
| [`use_preinstalled_scylla`](configuration_options/scylla-installation-and-configuration.md#use_preinstalled_scylla) | `SCT_USE_PREINSTALLED_SCYLLA` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`use_prepared_loaders`](configuration_options/stress-commands-and-load-generation.md#use_prepared_loaders) | `SCT_USE_PREPARED_LOADERS` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`use_scylla_doctor_on_failure`](configuration_options/scylla-doctor.md#use_scylla_doctor_on_failure) | `SCT_USE_SCYLLA_DOCTOR_ON_FAILURE` | [Scylla Doctor](configuration_options/scylla-doctor.md) |
| [`use_zero_nodes`](configuration_options/general-and-provisioning.md#use_zero_nodes) | `SCT_USE_ZERO_NODES` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`user_credentials_path`](configuration_options/general-and-provisioning.md#user_credentials_path) | `SCT_USER_CREDENTIALS_PATH` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`user_data_format_version`](configuration_options/scylla-installation-and-configuration.md#user_data_format_version) | `SCT_USER_DATA_FORMAT_VERSION` | [Scylla installation and configuration](configuration_options/scylla-installation-and-configuration.md) |
| [`user_prefix`](configuration_options/general-and-provisioning.md#user_prefix) | `SCT_USER_PREFIX` | [General and provisioning](configuration_options/general-and-provisioning.md) |
| [`user_profile_table_count`](configuration_options/stress-commands-and-load-generation.md#user_profile_table_count) | `SCT_USER_PROFILE_TABLE_COUNT` | [Stress commands and load generation](configuration_options/stress-commands-and-load-generation.md) |
| [`validate_large_collections`](configuration_options/longevity-tests.md#validate_large_collections) | `SCT_VALIDATE_LARGE_COLLECTIONS` | [Longevity tests](configuration_options/longevity-tests.md) |
| [`validator_step_timeout_minutes`](configuration_options/spark-migrator-cassandra-to-scylla.md#validator_step_timeout_minutes) | `SCT_VALIDATOR_STEP_TIMEOUT_MINUTES` | [Spark migrator (Cassandra to Scylla)](configuration_options/spark-migrator-cassandra-to-scylla.md) |
| [`vector_store_docker_image`](configuration_options/vector-store.md#vector_store_docker_image) | `SCT_VECTOR_STORE_DOCKER_IMAGE` | [Vector Store](configuration_options/vector-store.md) |
| [`vector_store_port`](configuration_options/vector-store.md#vector_store_port) | `SCT_VECTOR_STORE_PORT` | [Vector Store](configuration_options/vector-store.md) |
| [`vector_store_scylla_port`](configuration_options/vector-store.md#vector_store_scylla_port) | `SCT_VECTOR_STORE_SCYLLA_PORT` | [Vector Store](configuration_options/vector-store.md) |
| [`vector_store_threads`](configuration_options/vector-store.md#vector_store_threads) | `SCT_VECTOR_STORE_THREADS` | [Vector Store](configuration_options/vector-store.md) |
| [`vector_store_version`](configuration_options/vector-store.md#vector_store_version) | `SCT_VECTOR_STORE_VERSION` | [Vector Store](configuration_options/vector-store.md) |
| [`verify_data_after_entire_test`](configuration_options/upgrade-tests.md#verify_data_after_entire_test) | `SCT_VERIFY_DATA_AFTER_ENTIRE_TEST` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`verify_stress_after_cluster_upgrade`](configuration_options/upgrade-tests.md#verify_stress_after_cluster_upgrade) | `SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`verify_stress_after_migration`](configuration_options/upgrade-tests.md#verify_stress_after_migration) | `SCT_VERIFY_STRESS_AFTER_MIGRATION` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`workload_name`](configuration_options/performance-regression-tests.md#workload_name) | `SCT_WORKLOAD_NAME` | [Performance regression tests](configuration_options/performance-regression-tests.md) |
| [`write_stress_during_entire_test`](configuration_options/upgrade-tests.md#write_stress_during_entire_test) | `SCT_WRITE_STRESS_DURING_ENTIRE_TEST` | [Upgrade tests](configuration_options/upgrade-tests.md) |
| [`xcloud_availability_zones`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_availability_zones) | `SCT_XCLOUD_AVAILABILITY_ZONES` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`xcloud_credentials_path`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_credentials_path) | `SCT_XCLOUD_CREDENTIALS_PATH` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`xcloud_env`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_env) | `SCT_XCLOUD_ENV` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`xcloud_provider`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_provider) | `SCT_XCLOUD_PROVIDER` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`xcloud_replication_factor`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_replication_factor) | `SCT_XCLOUD_REPLICATION_FACTOR` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`xcloud_scaling_config`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_scaling_config) | `SCT_XCLOUD_SCALING_CONFIG` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`xcloud_vpc_peering`](configuration_options/scylla-cloud-xcloud-backend.md#xcloud_vpc_peering) | `SCT_XCLOUD_VPC_PEERING` | [Scylla Cloud (xcloud) backend](configuration_options/scylla-cloud-xcloud-backend.md) |
| [`zero_token_instance_type_db`](configuration_options/general-and-provisioning.md#zero_token_instance_type_db) | `SCT_ZERO_TOKEN_INSTANCE_TYPE_DB` | [General and provisioning](configuration_options/general-and-provisioning.md) |
>>>>>>> 905145b03 (docs(sct_config): split the option reference by group, and finish the regrouping)
