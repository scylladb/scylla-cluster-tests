# What Each Test Type Actually Reads

Shrinking a param the test method never reads is dead configuration: the file looks smaller,
the run still executes at full size. This page lists, per entry point, the params that size
the load, the ones that are ignored, and the values that must not change.

---

## `longevity_test.LongevityTest.test_custom_time`

The general-purpose longevity entry point, and the one most local smoke runs use.

**Sizes the load:**

| Param | Notes |
|---|---|
| `prepare_write_cmd` | List, one command per loader. Runs before the main load |
| `stress_cmd` | List. The main workload — shrink `duration=`, `n=`, `threads=` |
| `stress_read_cmd` | Optional read phase after the main load |
| `keyspace_num` | Multiplies the whole workload across N keyspaces — drop to 1 locally |
| `cs_user_profiles`, `cs_duration`, `user_profile_table_count` | The user-profile path, if the test-case uses it |
| `n_loaders` | One stress process per loader |
| `test_duration` | Wall-clock budget; SCT kills the run when it expires |

**Also read, and worth turning off locally:**
`run_fullscan` (a background full-scan thread — extra load a 1-vCPU guest does not need),
`nemesis_during_prepare`, `space_node_threshold`.

**Grows the cluster if set:** `cluster_target_size` and `add_node_cnt` — `test_custom_time`
grows to the target like `GrowClusterTest` does, so the memory gate budgets the target.

**Worked example:** `test-cases/minicloud-provision-test.yaml` (a purpose-built 3+1+1 shape)
and `test-cases/longevity/longevity-minicloud-10gb-1h.yaml` (`longevity-10gb-3h` shrunk from 6
db + 2 loaders + `duration=180m` to 3 db + 1 loader + `duration=60m`, with `run_fullscan`,
`sizing_loader` and `client_encrypt` dropped).

---

## `upgrade_test.UpgradeTest.test_rolling_upgrade`

The heaviest downscale in the repo, because the test reads about a dozen stress params and
asserts on most of them.

**Sizes the load** — every one of these is a stress command to shrink in place:
`prepare_write_stress`, `write_stress_during_entire_test`, `stress_cmd_read_cl_quorum`,
`verify_stress_after_cluster_upgrade`, `stress_after_cluster_upgrade`,
`large_partition_stress_during_upgrade`, and the four `stress_cmd_complex_*` keys.

**Must not change:**

- The keyspace name in `write_stress_during_entire_test` — the test calls `metric_has_data()`
  against it to confirm the write workload reached Prometheus **before** it starts upgrading.
  This is why `n_monitor_nodes: 1` is load-bearing here, not optional.
- The `ops(...)` sets in the `stress_cmd_complex_*` commands, and the profile path
  `/tmp/complex_schema.yaml` — `check_required_files()` resolves it against `data_dir/`. The
  complex profile runs unless tablets disable it, so these keys must be present and small
  rather than removed.
- Consistency levels: `prepare_write_stress` writes at a strong CL before any node is
  upgraded so a later validation failure is unambiguous; the during-upgrade commands use
  QUORUM because not every node is available.

**Cluster floor:** 3 db nodes — RF=3 with one node down at a time still has quorum.

**Also worth setting:** `use_preinstalled_scylla: true` (skip a repo install on emulated
storage), and on the OS-upgrade variant `upgrade_node_system: false` — an `apt dist-upgrade` of
the stock image exercises Canonical's mirrors, not the upgrade path, at ~15 minutes per
1-vCPU guest.

**Worked examples:** `configurations/minicloud/rolling-upgrade.yaml` and
`configurations/minicloud/generic-rolling-upgrade.yaml`.

---

## `grow_cluster_test.GrowClusterTest.test_grow_x_to_y`

The trap case. This test **builds** its own cassandra-stress command.

**Sizes the load — only these three:**

| Param | Default | Local value |
|---|---|---|
| `cassandra_stress_population_size` | 1,000,000 | ~50,000 |
| `cassandra_stress_threads` | 1,000 | ~10 |
| `test_duration` | — | Doubles as the stress duration, so it also decides how long the run takes |

**Ignored — shrinking these does nothing:** `stress_cmd`, `prepare_write_cmd`,
`stress_read_cmd`, `run_fullscan` (only wired up in `longevity_test`).

**Cluster shape:** `n_db_nodes` is where it starts, `cluster_target_size` is the peak, and
`add_node_cnt` is the step size. The memory gate budgets the **peak**. `n_monitor_nodes: 1` is
required: `add_nodes()` reconfigures Scylla monitoring after every node, and `setUp()` builds a
Prometheus metrics object for the add-node timing.

**Nemesis:** starts only after the cluster reaches its target size, so `nemesis_during_prepare:
false`.

**Worked example:** `configurations/minicloud/scale-cluster.yaml` — production 15 to 25 nodes
(112 GiB of guests at the peak) shrunk to 3 to 4.

---

## `artifacts_test.ArtifactsTest.test_scylla_service`

**Usually needs no downscale at all.** The artifacts test-cases are already 1 db node, 0
loaders, 0 monitors — one guest, ~4 GiB. It is the cheapest way to exercise the provisioning
and image path locally.

**Reads:** `use_preinstalled_scylla` (image boot versus repo install), `pre_create_schema` and
its `keyspace_num` / `sstable_size` / `compaction_strategy` companions, `client_encrypt`,
`run_scylla_doctor`, `unified_package`, `nonroot_offline_install`.

**Watch out for:** `client_encrypt` and the scylla-doctor path pull in extra machinery that a
downscale run rarely needs; the AWS image path needs a **released** version.

---

## `performance_regression_*` — the gradual-throughput family

`performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest` and
its siblings. **Downscale these only to validate the harness** — the test code, a new step
definition, a decorator change, log collection. Any number they produce locally is noise.

**Sizes the load — and none of it is a `stress_cmd`:**

| Param | What it is |
|---|---|
| `perf_gradual_throttle_steps` | A dict of `{threads, concurrency, rate}` steps per sub-test. The production steps ask for 300K-1.1M ops/s at up to 8000 parallelism — cut to one or two steps at a few thousand ops/s |
| `perf_gradual_step_duration` | Per-sub-test step duration, `30m` in production. Minutes locally |
| `test_duration` | 500 in production. Must still cover the shrunk steps plus provisioning |
| `n_loaders` | 4 in production — one is enough when each guest has 1 vCPU |

**Watch out for:**

- The jenkinsfile's `sub_tests` list runs each sub-test as a separate job. Pick **one**
  locally; running four sequentially multiplies the wall clock by four.
- The latency-decorator threshold overlays
  (`configurations/performance/latency-decorator-error-thresholds-*.yaml`) assert on measured
  latency. On 1-vCPU guests those thresholds will be blown by orders of magnitude. Either drop
  that file from the config list or accept that the assertion failure is meaningless.
- These test-cases commonly pin an **arm64** db type (`i8g.4xlarge`). Guests run under KVM, so
  on an x86_64 host you must pin the db role to an x86_64 type and image.

---

## A Test Not Listed Here

1. Find the class: the entry point is `<module>.<Class>.<method>`.
2. `grep -n "params.get\|params\[" <module>.py` and read the method body, plus any helper it
   calls in the same class.
3. Anything not in that list is dead configuration for this test — leave it at its production
   value in the overlay so the next person is not misled.
4. Check the base class too: `sdcm/tester.py` reads the cluster-shape params and the nemesis
   params for every test.
5. Ask three questions before choosing counts:
   - Does it query Prometheus or reconfigure monitoring? Then the monitor is not optional.
   - Does it take a node down, or grow? Then 3 db nodes is a floor, and the peak is what the
     memory gate budgets.
   - Does it assert on anything inside a stress command? Then that part is shape, not volume.
