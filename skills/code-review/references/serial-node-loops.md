# Serial Per-Node Blocking I/O (SKILL.md Check 12)

## Trigger

PR adds or changes a loop over the nodes of a cluster (`for node in self.nodes`, `cluster.nodes`, `node_list`,
the created `instances`, a comprehension, or a call its caller makes once per node) whose body blocks on the
node, directly or through a helper it calls:

- SSH: `node.remoter.run/sudo/stop`, `node.remote_scylla_yaml()`, `node.init()`, `wait_cloud_init_completes`
- A cloud API call: instance termination (`node.destroy()`), tagging (`set_keep_alive`), labels
- An HTTP or CQL request: Argus, the REST API, Grafana, a `db_up()` TCP probe
- Log, coredump, or screenshot collection and its S3 upload; a container stop or removal
- `wait.wait_for(...)` polling

## Why This Matters

Each iteration waits for its round trip before the next node starts, so the loop costs N × the per-node time,
and a `wait.wait_for(..., timeout=1200)` loop up to N × 20 minutes. Cloud termination waits (AWS
`wait_until_terminated`, Azure `terminate(wait=True)`, GCE `_instance_wait_safe`) and SSH bootstraps take tens
of seconds per node, so on a 100-node cluster one loop costs an hour of setup or teardown. A serial loop is
correct code: it passes the unit tests and the small-cluster runs, and no linter sees it. Only manual audits found
them, on the setup path (SCT-720) and on the teardown path (SCT-719).

## How to Check

1. List the node loops in the diff: `git diff <base>...<head> -- sdcm/ | grep -nE '^\+.*for .+ in .*(nodes|node_list|instances)'`
   (`.+` also catches `for i, node in enumerate(...)`).
   Also read the comprehensions over nodes that call a method. Four shapes hide from that grep:
   - **A blocking call added inside an existing loop.** The most common case: the `for` line is not in the diff.
     For each added SSH, cloud-API, HTTP, or `wait_for` call, read the loop that encloses it.
   - **A loop over clusters** (`for cluster in [db, loaders, monitors]`) whose body waits on each cluster's nodes,
     like `destroy()`. The waits stack even when each cluster is parallel inside. Flag it.
   - **A search for the first live node** that probes one node at a time and breaks at the first hit. Each dead
     node before it costs the probe's timeout, and `db_up()` has no connect timeout. Flag it, unless the probe has
     a short timeout.
   - **A function that works on one node** (`terminate_node(node)`). The diff may add no loop, but its callers
     loop over it. Read the callers.
2. Read each loop body and follow the calls it makes. The I/O often hides one call down: `add_nodes` loops over
   `_create_node`, which calls `node.init()`, which runs `wait_ssh_up`, `wait_for_cloud_init`, `set_hostname`
   and more over SSH. `terminate_node()` wraps its work in `DbNodeLogger`, which runs `logger` over SSH on every
   other node.
3. Weigh the path. It matters most where it runs on every test or scales with the cluster: `add_nodes`,
   `_create_node`, `wait_for_init`, `sdcm/provision/`, `sdcm/sct_provision/`, post-setup validation, `tearDown`,
   `stop_resources*`, `clean_resources`, `*.destroy()`, `get_backtraces`, log and screenshot collection, and
   `hydra clean-resources` (`sdcm/utils/resources_cleanup.py`).
4. Decide whether the iterations are independent: no iteration reads what an earlier one wrote, and no shared
   state is written without a lock. A body that only reads cached attributes (`node.ip_address`, building a dict)
   is not a finding. Monitor and loader sets are not a fixed set: they grow with the DCs and the test config, so
   their loops count. Only a set whose size never depends on the test config is exempt.
5. Independent → flag the loop and ask for the parallel version. Dependent → ask for a one-line comment that says
   why it stays serial. These stay serial on purpose:
   - Seed or first-node work that the other nodes need first
   - Rolling restarts and upgrades, and any step that must keep quorum
   - Topology changes (bootstrap, decommission), which Scylla runs one at a time

   Two serial loops in a row where the second waits for the first (stop all, then wait for all) become two
   parallel phases, not one.

## The Fix

```python
# WRONG — 100 nodes × termination wait
for node in self.nodes:
    node.destroy()

# RIGHT — one termination wait per wave of 32; a failure or a timeout still raises
# (on a timeout, the nodes still queued are cancelled and come back with a TimeoutError)
results = ParallelObject(objects=self.nodes, timeout=600, num_workers=min(32, len(self.nodes)) or None).run(
    lambda node: node.destroy(), ignore_exceptions=True
)
if failed := [res for res in results if res.exc]:
    for res in failed:
        LOGGER.error("Failed to destroy %s: %s", res.obj, res.exc)
    raise RuntimeError(f"Failed to destroy {len(failed)} of {len(results)} nodes")
```

Keep the error behavior of the loop: `destroy()` raised on the first failure, so the parallel version still raises.
A cleanup that logs and goes on instead needs a comment that says so.

Review the parallel code as hard as the loop it replaces:

| Helper | Trap |
|---|---|
| `ParallelObject(objects, timeout=6, num_workers=None)` (`sdcm/utils/parallel_object.py`) | The default `timeout` is **6 seconds**, too short for any SSH bootstrap, cloud-init wait, or termination. The timeout is not one deadline: each future gets its own wait, in submission order, so when the wait on a future starts, the earlier ones are done and this one is already running. Size `timeout` for the slowest single node, with a margin; a larger one only delays the detection of a hung node. After the first timeout every later future gets 0.001 s, `clean_up()` cancels the queued futures, and the running ones keep going in the background. So a timeout drops the nodes still queued: on teardown they leak (SCT-1537). On teardown, report or retry the nodes whose `exc` is a `TimeoutError`. `num_workers=0` raises `ValueError`, and teardown runs on empty clusters after a failed setup: use `min(32, len(nodes)) or None`. |
| `ParallelObject.run(..., ignore_exceptions=...)` | `ignore_exceptions=True` keeps the per-node `try/except` of a cleanup loop, so one bad node does not stop the rest, but the caller must log or raise from each `ParallelObjectResult.exc`. Without it, `ParallelObjectException` is raised after all the futures end, but only when none timed out: after a timeout `run()` stops waiting and raises the same `ParallelObjectException` right away, with a `TimeoutError` in `exc` for each node that timed out or was still queued, and the running futures keep going in the background, as in the row above. Setup code must still fail when one node fails. |
| `BaseCluster.run_func_parallel(func, node_list)` (`sdcm/cluster.py`) | `ThreadPoolExecutor()` with the default worker count, `min(32, cpu + 4)`: on a builder with 8 CPUs, 100 nodes run in batches of 12. No timeout. A failure is raised only after all the other nodes finish. |
| Raw `ThreadPoolExecutor` | Check 3b: the pool needs a `with` block or a `shutdown()` on every path. |

Also check that the parallel version keeps:
- **The node order and indexes**: `self.nodes` order, `_node_index`, and the rack assignment must not depend on
  which thread finishes first. Create the nodes in order, and run only the I/O in parallel.
- **The per-node timeout**: a timeout that bounded each node in the loop must still bound each node.
- **A test**: a unit test that asserts every node is processed, and that a failure on one node still fails a
  setup call or is reported by a cleanup call.

## Known Instances

Examples of each shape the check must flag. The epics hold the current status; a fixed ticket stays as an example.

**Setup ([SCT-720](https://scylladb.atlassian.net/browse/SCT-720))**
- SCT-721: `add_nodes` → `_create_node` → `node.init()`, every backend. The biggest one.
- SCT-733: `provision_instances_with_fallback`, `wait_cloud_init_completes` per VM
- SCT-731 `update_seed_provider`, SCT-1526 `validate_seeds_on_all_nodes`: `scylla.yaml` over SSH
- SCT-730: `CassandraAWSCluster.wait_for_init`, `wait_for` up to 1200 s per node
- SCT-729 `_reconfigure_vector_store_nodes`, SCT-1527 `validate_raft_on_nodes`, SCT-1528 `update_rack_info_in_argus`
- SCT-1531 GCE `_create_or_find_instances`, SCT-1532 GCE `_destroy_partial_batch`, SCT-1533 AWS
  `set_tags_on_instances`, SCT-1534 Azure `_provision_with_stuck_recreate`: a cloud API call per VM

**Teardown ([SCT-719](https://scylladb.atlassian.net/browse/SCT-719))**
- SCT-728: `BaseCluster.destroy()`. The biggest one.
- SCT-724 `terminate_node()` and SCT-1518 its `DbNodeLogger` (SSH on every other node); SCT-1535 the nemesis
  failure path that calls it per node
- SCT-725 `get_backtraces`, SCT-722 Grafana screenshots, SCT-723 `update_default_time_range`, SCT-734
  `MonitorSetDocker.destroy`
- SCT-726, SCT-727: stopping the node threads; SCT-732, SCT-1515, SCT-1516: killing the stress tools; SCT-1517:
  `set_keep_alive_on_failure`
- First-live-node searches with `db_up()`: SCT-1519 `gather_failure_statistics`, SCT-1520 `save_schema`
- A loop over clusters: SCT-1525 `clean_resources`, `collect_logs`
- `hydra clean-resources`: SCT-1522 baremetal, SCT-1523 docker, SCT-1524 AWS, OCI, Azure
- Parallel, but with a trap: SCT-1529 `_get_live_node` (6 s default timeout), SCT-1536 NVMe self-tests and
  SCT-1537 `clean_instances_gce` (the timeout drops queued nodes), SCT-1538 `_set_perf_collector_running` (the
  `run_func_parallel` worker cap), SCT-1598 `LogCollector.collect_logs` (logs lost after a timeout); SCT-1521
  `NvmeValidator._collect_health` is plain serial

**Runtime (no epic)**
- SCT-1593 nemesis `log_on_all_nodes`, SCT-1594 `wait_for_schema_agreement` and `wait_all_nodes_un`, SCT-1595
  `temporarily_disable_auto_repair`, SCT-1596 `restore_with_nodetool_refresh`, SCT-1597 `wait_prepare_compactions`
- SCT-1599 `upgrade_os`: a timeout cancels queued nodes, which are then reported as upgraded
