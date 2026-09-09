# General and provisioning

[← All configuration options](configuration_options.md)

Cluster topology, region/AZ placement, instance provisioning, credentials and test-level
plumbing. Options here apply to every backend and every test type.

**71 options.**


## **adaptive_timeout_multipliers** / SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS

Optional dict of adaptive-timeout multipliers keyed by operation name (from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). If the current operation key is absent, multiplier 1.0 is used.<br>YAML example:<br>[`adaptive_timeout_multipliers`](#adaptive_timeout_multipliers):<br>  decommission: 4<br>  new_node: 2<br>Environment variable examples:<br>SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS="{'decommission': 4, 'new_node': 2}"<br>Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4

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

Number of additional data volumes attached to instances<br>if [`data_volume_disk_num`](#data_volume_disk_num) > 0, then data volumes (ebs on aws) will be<br>used for scylla data directory

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

On capacity errors, automatically retry provisioning in the next available AZ in the same region. Backend-agnostic parameter; supersedes [`aws_fallback_to_next_availability_zone`](aws-backend.md#aws_fallback_to_next_availability_zone).

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

[`instance_provision`](#instance_provision): spot|on_demand|spot_fleet

**default:** spot

**type:** Literal['spot', 'on_demand', 'spot_fleet', 'spot_low_price']

**backend overrides:**
- `on_demand`: oci, k8s-gke, k8s-eks


## **instance_provision_fallback_on_demand** / SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND

[`instance_provision_fallback_on_demand`](#instance_provision_fallback_on_demand): create instance on_demand provision type if instance with selected [`instance_provision`](#instance_provision) type creation failed. Expected values: true|false (default - false

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

AWS Secrets Manager secret name prefix when [`keystore_backend`](#keystore_backend)=secretsmanager (default: 'sct/')

**default:** sct/

**type:** str (appendable)


## **keystore_sm_region** / SCT_KEYSTORE_SM_REGION

AWS region holding the KeyStore secrets when [`keystore_backend`](#keystore_backend)=secretsmanager (default: 'us-east-1')

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

Number of zero token nodes in cluster. Value should be set as '0 1 1' for multidc configuration in same manner as [`n_db_nodes`](#n_db_nodes) and should be equal number of regions

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

Before provisioning, probe capacity by launching and terminating one on-demand instance per dynamic type ([`instance_type_db_target`](#instance_type_db_target), [`nemesis_grow_shrink_instance_type`](nemesis-chaos-testing.md#nemesis_grow_shrink_instance_type)) in the chosen AZ. On capacity errors, raise to trigger AZ/region fallback. Costs ~1 min per type. AWS-only.

**default:** False

**type:** bool


## **raid_level** / SCT_RAID_LEVEL

Number of of raid level: 0 - RAID0, 5 - RAID5

**default:** 0

**type:** int


## **region_name** / SCT_REGION_NAME

Cloud region(s) to run in. A space-separated list or YAML list provisions a multi-region cluster, one entry per datacenter. Despite the AWS-sounding default, this is the generic region option; GCE uses [`gce_datacenter`](gce-backend.md#gce_datacenter) and Azure uses [`azure_region_name`](azure-backend.md#azure_region_name).

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eu-west-1']`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **reuse_cluster** / SCT_REUSE_CLUSTER

If [`reuse_cluster`](#reuse_cluster) is set it should hold [`test_id`](#test_id) of the cluster that will be reused.<br>`reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`

**default:** N/A

**type:** str (appendable)


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

Forces GossipingPropertyFileSnitch (regardless [`endpoint_snitch`](scylla-installation-and-configuration.md#endpoint_snitch)) to simulate racks.<br>Provide number of racks to simulate. Takes effect only with more than one DB node: a<br>single-node cluster stays in one rack and [`endpoint_snitch`](scylla-installation-and-configuration.md#endpoint_snitch) is left alone. On the docker<br>backend the rack is passed to the image entrypoint as `--dc/--rack`, which requires Scylla<br>>= 2026.1; an older image fails the configuration, so set 1 to opt out.

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

Cloud-agnostic instance sizing constraints for loader nodes. Loaders default to Arm. A stress tool whose loader image is published for linux/amd64 only (cassandra-harry, hydra-kcl, ndbench, nosqlbench, and the alternator DNS sidecar used by YCSB when [`alternator_use_dns_routing`](alternator-dynamodb-api.md#alternator_use_dns_routing) is set) sets arch to x86_64 for you. Set arch here to pick the architecture yourself

**default:** {'vcpu': 4, 'memory': '>=8'}

**type:** dict


## **sizing_monitor** / SCT_SIZING_MONITOR

Cloud-agnostic instance sizing constraints for monitor nodes

**default:** {'vcpu': 2, 'memory': '>=8'}

**type:** dict


## **skip_test_stages** / SCT_SKIP_TEST_STAGES

Skip selected stages of a test scenario, as a mapping of stage name to true/false<br>(e.g. `{"setup": true}`). Used to reuse a cluster across runs or to shorten a debug<br>cycle. See `docs/skip-test-stages.md` for the stage names and what each one covers.

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

Set the [`test_id`](#test_id) of the run manually. Use only from the env before running Hydra

**default:** N/A

**type:** str (appendable)


## **test_metadata** / SCT_TEST_METADATA

Structured metadata for test documentation and labeling, embedded in the test-case YAML and validated on config load by `sdcm.test_metadata.TestMetadata`. Flows to Argus. Covers description, tier, test_type, stress_tools, nemesis_labels and features; see `skills/reviewing-pipeline-docs/` for the field-by-field guide and `skills/labeling-pipelines/references/taxonomy-values.md` for the accepted values. `sct.py lint-test-docs` checks coverage.

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


## **zero_token_instance_type_db** / SCT_ZERO_TOKEN_INSTANCE_TYPE_DB

Instance type for zero-token DB nodes -- nodes that join the ring for reads/writes but own no token range. Falls back to [`instance_type_db`](#instance_type_db) when unset.

**default:** N/A

**type:** str (appendable)
