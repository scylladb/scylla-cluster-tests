# ScyllaDB Cloud (xcloud) backend

## Overview

The xcloud backend allows SCT to provision and test Scylla on ScyllaDB Cloud. This backend leverages the ScyllaDB Cloud REST API to create, configure, and manage clusters across different cloud providers (AWS and GCE).

Unlike other SCT backends where infrastructure is provisioned and operated directly, the xcloud backend provisions managed ScyllaDB clusters, where the database is already installed and configured.

## Key features

- **Managed ScyllaDB Clusters**: DB nodes are fully managed by ScyllaDB Cloud
- **Multi-Cloud Support**: deploy clusters on AWS or GCE cloud providers
- **VPC Peering**: private connectivity between SCT infrastructure and cloud clusters
- **Vector Search Support**: deploy Vector Search nodes alongside DB nodes
- **Monitoring Integration**: federation with ScyllaDB Cloud Prometheus endpoint
- **Automated Cleanup**: keep-tag based resource cleanup mechanism

## Prerequisites

### API credentials

To use the xcloud backend, you need Siren user API credentials for ScyllaDB Cloud.

There are pre-created Siren users for different cloud environments (lab, staging, production).

Additionally, developers can configure their own Siren user in a ScyllaDB Cloud environment and generate API credentials, which can then be provided to SCT via a local file.
Instructions for configuring test billing for a Siren user and generating API credentials can be found in [ScyllaDB Cloud API access for SCT](https://scylladb.atlassian.net/wiki/spaces/RND/pages/175178675/ScyllaDB+Cloud+API+access+for+SCT).

### AWS/GCE access

Since loader and monitor nodes are provisioned directly on AWS/GCE (not a part of ScyllaDB Cloud cluster), the appropriate cloud provider credentials should be configured.

### SSH connectivity to DB nodes

SSH access to cloud DB nodes is performed via a StrongDM bastion host. StrongDM hosts are configured only for pre-created Siren users.

## Configuration

### Required configuration parameters

| Parameter | Environment Variable | Description |
|-----------|---------------------|-------------|
| `cluster_backend` | `SCT_CLUSTER_BACKEND` | Must be set to `xcloud` |
| `xcloud_provider` | `SCT_XCLOUD_PROVIDER` | Cloud provider: `aws` or `gce` |
| `scylla_version` | `SCT_SCYLLA_VERSION` | Scylla version to deploy |

### Optional configuration parameters

| Parameter | Environment Variable | Description | Default |
|-----------|---------------------|-------------|---------|
| `xcloud_env` | `SCT_XCLOUD_ENV` | ScyllaDB Cloud environment (`lab`, `staging` or `prod`) | `lab` |
| `xcloud_credentials_path` | `SCT_XCLOUD_CREDENTIALS_PATH` | Path to local credentials file | By default the credentials of pre-created Siren users are used |
| `xcloud_replication_factor` | `SCT_XCLOUD_REPLICATION_FACTOR` | Cluster replication factor | Based on node count |
| `xcloud_vpc_peering` | `SCT_XCLOUD_VPC_PEERING` | VPC peering configuration (see below) | Enabled |
| `n_vector_store_nodes` | `SCT_N_VECTOR_STORE_NODES` | Number of Vector Search nodes | 0 (VS nodes deployment is disabled) |

### VPC peering configuration

VPC peering enables private connectivity between SCT infrastructure (runner/loaders/monitors) and ScyllaDB Cloud cluster nodes.
It is highly recommended to use VPC peering instead of public IPs, to reduce network latency and data transfer costs.

**Configuration format:**
```yaml
xcloud_vpc_peering:
  enabled: true
  cidr_pool_base: '172.31.0.0/16'  # base CIDR for cluster networks
  cidr_subnet_size: 24              # subnet size for each cluster
```

### Monitoring configuration

ScyllaDB Cloud clusters provide a Prometheus endpoint for metrics federation. SCT automatically configures monitoring when a monitor node is provisioned.

## Running tests

### Basic test example

```bash
# set required configuration parameters
export SCT_SCYLLA_VERSION=2025.3.0
export SCT_XCLOUD_ENV=lab
export SCT_XCLOUD_PROVIDER=aws

# run a simple provision test
hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend xcloud \
  --config test-cases/PR-provision-test.yaml --config configurations/network_config/test_communication_public.yaml
```

### SCT configuration example

**test-cases/custom/my-xcloud-test.yaml:**
```yaml
test_duration: 60
n_db_nodes: 3
n_loaders: 1
n_monitor_nodes: 1

instance_type_loader: 'c6i.large'
gce_instance_type_loader: 'e2-standard-2'

stress_cmd: [
  "cassandra-stress write cl=QUORUM duration=30m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=1000 -pop seq=1..5000000"
]

user_prefix: 'my-xcloud-test'

xcloud_vpc_peering:
  enabled: true
  cidr_pool_base: '172.31.0.0/16'
  cidr_subnet_size: 24
```

## Listing cloud clusters

```bash
# list using `list-resources` hydra command
hydra list-resources -b xcloud --get-all

# list using cleanup script in dry-run mode
./docker/env/hydra.sh python -m utils.cloud_cleanup.xcloud.clean_xcloud --dry-run
```

## Reusing existing xcloud cluster

Existing ScyllaDB Cloud clusters can be reused similarly to other backends - by specifying the test ID of the test run that created the cluster.
Find more details in [reuse_cluster documentation](./reuse_cluster.md#scylla-cloud-xcloud-backend).

## Resources cleanup

ScyllaDB Cloud clusters created by SCT are named with pattern: `{user_prefix}-db-cluster-{test_id}-keep-{hours}h`

### Automatic cleanup using keep-tag

Cloud clusters created by SCT include a keep-tag suffix in their name (e.g., `-keep-2h`). The tag indicates for how long the cluster should be retained before automated deletion.
A periodic cleanup job deletes clusters older than the specified duration.

Keep duration is automatically set based on test duration and is calculated as:
`test_duration + prepare_stress_duration + margin`

**Executing cleanup script manually:**
```bash
# dry-run mode to see what would be deleted
./docker/env/hydra.sh python -m utils.cloud_cleanup.xcloud.clean_xcloud --dry-run

# cleanup of test clusters on specific environment
./docker/env/hydra.sh python -m utils.cloud_cleanup.xcloud.clean_xcloud --environments lab

# cleanup for all environments
./docker/env/hydra.sh python -m utils.cloud_cleanup.xcloud.clean_xcloud
```

### Manual cleanup

Cloud clusters are created under a Siren account and are not tied to a specific user. Therefore, manual cleanup is performed based on the ID of the test that created the cluster.

```bash
# get test ID from an SCT run
export TEST_ID=$(cat ~/sct-results/latest/test_id)

hydra clean-resources --test-id $TEST_ID -b xcloud
```

## Architecture details

### Cluster components

When running tests with xcloud backend, SCT provisions:

1. **Cloud cluster** (managed by ScyllaDB Cloud):
   - Database nodes
   - Vector Search nodes (optional)

2. **SCT Infrastructure** (provisioned on AWS/GCE):
   - SCT runner node
   - Loader nodes
   - Monitor nodes

### Network connectivity

```
┌────────────────────────────────────────────┐
│         ScyllaDB Cloud                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ DB Node  │  │ DB Node  │  │ DB Node  │  │
│  │ (private)│  │ (private)│  │ (private)│  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  │
│       │ VPC Peering │             │        │
└───────┼─────────────┼─────────────┼────────┘
        │             │             │
        └─────────────┴─────────────┘
                      │
        ┌─────────────┴──────────┐
        │   SCT Infrastructure   │
        │  ┌──────────┐          │
        │  │  Loader  │          │
        │  │  Nodes   │          │
        │  └──────────┘          │
        │  ┌──────────┐          │
        │  │ Monitor  │          │
        │  │  Node    │          │
        │  └──────────┘          │
        │  ┌──────────┐          │
        │  │  SCT     │          │
        │  │  Runner  │          │
        │  └──────────┘          │
        └────────────────────────┘
```

## Additional resources

- [Scylla Cloud Documentation](https://cloud.docs.scylladb.com/)
- [Scylla Cloud REST API](https://cloud.docs.scylladb.com/stable/api.html)
- [Vector Search in Scylla Cloud](https://cloud.docs.scylladb.com/stable/vector-search/)
