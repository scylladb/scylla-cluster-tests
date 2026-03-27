# Callback-Based Node Configuration

## Problem

`_add_new_node_in_new_dc()` in `sdcm/nemesis/__init__.py` modifies `scylla.yaml` and `cassandra-rackdc.properties` after `wait_for_init()`, requiring a node restart (~30-60s overhead). The config should be applied before Scylla starts.

## Approach

Add a `after_config` parameter to `add_nodes()` that gets stored on the node and executed during `node_setup()` after `config_setup()` but before `node_startup()`:

```
Current:  add_nodes() → wait_for_init() → [config_setup() → node_startup()] → modify config → restart
Proposed: add_nodes(after_config=fn) → wait_for_init() → [config_setup() → after_config() → node_startup()]
```

The callback is optional (`None` default), backward compatible, and exceptions propagate naturally through `wait_for_init_wrap`.

## Files to Modify

- `sdcm/cluster.py` — `BaseNode.__init__` (store callback), `BaseCluster.add_nodes` (accept param), `BaseScyllaCluster.node_setup` (execute callback)
- `sdcm/cluster_aws.py` — `AWSCluster.add_nodes`, `_create_node`, `AWSNode.__init__`, `ScyllaAWSCluster.add_nodes`, `CassandraAWSCluster.add_nodes`
- `sdcm/cluster_gce.py` — `GCECluster.add_nodes`, `_create_node`, `GCENode.__init__`
- `sdcm/cluster_azure.py` — `AzureCluster.add_nodes`, `_create_node`, `AzureNode.__init__`
- `sdcm/cluster_oci.py` — `OciCluster.add_nodes`, `_create_node`, `OciNode.__init__`
- `sdcm/cluster_docker.py` — All `add_nodes`/`_create_node`/`DockerNode.__init__` overrides
- `sdcm/cluster_baremetal.py` — `add_nodes`, `_create_node`
- `sdcm/cluster_cloud.py` — `CloudCluster.add_nodes`, `_create_node`, `CloudNode.__init__`
- `sdcm/cluster_k8s/__init__.py` — All `add_nodes`/`_create_node` overrides
- `sdcm/cluster_k8s/eks.py` — `add_nodes`
- `sdcm/cluster_k8s/gke.py` — `add_nodes`
- `sdcm/kafka/kafka_cluster.py` — `add_nodes`
- `sdcm/nemesis/__init__.py` — `_add_new_node_in_new_dc()` uses callback instead of post-init restart

## Verification

- [ ] `uv run sct.py pre-commit`
- [ ] `uv run sct.py unit-tests`
- [ ] `grep -rn "def add_nodes" sdcm/` — all overrides accept `after_config`
- [ ] Manual: run `_add_new_node_in_new_dc()` on Docker backend, verify node starts without restart
