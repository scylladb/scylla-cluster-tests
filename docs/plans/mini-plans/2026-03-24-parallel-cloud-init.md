# Parallelize Cloud-Init Waits During Cluster Provisioning

## Problem

In `sdcm/tester.py:get_cluster_aws()`, three clusters (db, loaders, monitors) are created
sequentially. Each cluster constructor calls `add_nodes()` → `_create_node()` → `node.init()`
→ `wait_for_cloud_init()`, which blocks ~4 minutes per node.

For a typical 3-DB + 1-loader + 1-monitor setup, this adds ~12 minutes of sequential
cloud-init waits. With parallelism this could be reduced to ~4-5 minutes.

This affects all cloud backends (AWS, GCE, Azure, OCI) with the same pattern.

### Call chain

```
get_cluster_aws()                              # tester.py:1975-2008
  ├─ ScyllaAWSCluster(n_nodes=3)               # blocks ~12 min (3 nodes × 4 min)
  ├─ LoaderSetAWS(n_nodes=1)                   # blocks ~4 min
  └─ MonitorSetAWS(n_nodes=1, targets=...)     # blocks ~4 min (needs db+loaders first)

Each constructor:
  BaseCluster.__init__()                        # cluster.py:3994
    └─ add_nodes(count)                         # cluster_aws.py:511
         ├─ EC2 create_instances (fast, async)  # line 549
         └─ for instance in instances:          # line 553 — SEQUENTIAL
              └─ _create_node(instance)         # line 557
                   └─ node.init()               # line 590 — BLOCKS
                        ├─ wait_ssh_up()         # cluster.py:534
                        └─ wait_for_cloud_init() # cluster.py:535 — ~4 min
```

## Approach

### Level 1: Parallelize `node.init()` within `add_nodes()`

Currently `AWSCluster.add_nodes()` (cluster_aws.py:553-570) calls `_create_node()` in a
sequential for-loop. Each `_create_node()` calls `node.init()` which blocks on cloud-init.

**Fix:** Split `_create_node()` into two phases:
1. Create node object (fast) — keep sequential for deterministic `_node_index` assignment
2. Run `node.init()` on all nodes in parallel after the loop

```python
# In add_nodes(), after the existing for-loop that creates all nodes:
new_nodes = self.nodes[-count:]
with ThreadPoolExecutor(max_workers=count) as executor:
    list(executor.map(lambda n: n.init(), new_nodes))
```

Remove the `node.init()` call from `_create_node()` in all cloud backends.

**Existing pattern to reuse:** `BaseCluster.run_func_parallel()` at cluster.py:4137 uses
the same `ThreadPoolExecutor` pattern.

**Expected savings:** For 3 DB nodes, cloud-init drops from ~12 min to ~4 min.

### Level 2: Parallelize cluster creation in `get_cluster_aws()`

In `tester.py:get_cluster_aws()`, `db_cluster` (line 1975) and `loaders` (line 1990) are
independent — create them in parallel. `monitors` (line 2000) depends on both
(`targets=dict(db_cluster=..., loaders=...)`), so it must wait.

```python
with ThreadPoolExecutor(max_workers=2) as executor:
    db_future = executor.submit(create_cluster, db_type)
    loader_future = executor.submit(lambda: LoaderSetAWS(...))
    self.db_cluster = db_future.result()
    self.loaders = loader_future.result()

# monitors depend on both
self.monitors = MonitorSetAWS(..., targets=dict(db_cluster=..., loaders=...))
```

Apply same pattern to `get_cluster_gce()`, `get_cluster_azure()`, `get_cluster_oci()`.

**Expected savings:** db and loader cloud-init overlap, saving ~4 min.

### Combined savings estimate

| Scenario (3 DB + 1 loader + 1 monitor) | Before | After |
|-----------------------------------------|--------|-------|
| Level 1 only (parallel node init)       | ~20 min | ~12 min |
| Level 1 + Level 2 (parallel clusters)   | ~20 min | ~8 min |

### Risks

- **Thread safety of `_node_index`:** Per-cluster instance, sequential in the loop — safe.
- **Logging interleave:** Each node logger has a node-name prefix — acceptable.
- **boto3 waiters:** `instance.wait_until_running` is thread-safe.
- **Error handling:** `@cluster.terminate_on_failure` decorator on `node.init()` handles cleanup.
- **Monitor dependency:** Must create monitors after db+loaders — handled by the sequential call.

## Files to Modify

| File | Change |
|------|--------|
| `sdcm/cluster_aws.py:575-590` | Remove `node.init()` from `_create_node()`. Add parallel init block after the for-loop in `add_nodes()` (line 570). Same for `MonitorSetAWS._create_node()`, `VectorStoreSetAWS._create_node()`. |
| `sdcm/cluster_gce.py` | Same split for `GCECluster._create_node()` and `add_nodes()`. |
| `sdcm/cluster_azure.py` | Same split for Azure `_create_node()` and `add_nodes()`. |
| `sdcm/cluster_oci.py` | Same split for OCI `_create_node()` and `add_nodes()`. |
| `sdcm/tester.py:1975-2008` | Parallelize db_cluster + loaders in `get_cluster_aws()`. Same for GCE/Azure/OCI. |

## Verification

- [ ] `uv run sct.py pre-commit` passes
- [ ] `uv run sct.py unit-tests` — no regressions
- [ ] Docker backend test passes (behavioral baseline): `./run-docker-cassandra-test.sh`
- [ ] AWS provision test with timing comparison: `./run-aws-cassandra-test.sh`
- [ ] Multi-node cluster (3 DB nodes) shows parallel cloud-init in logs
- [ ] Monitor node still has correct `targets` references
