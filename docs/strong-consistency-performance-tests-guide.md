# How to Run Strong Consistency Performance Tests in SCT

## Overview

This guide explains how to use Scylla Cluster Tests (SCT) to run performance tests for the Strong Consistency (SC) feature. The test suite includes write, cached-read, and read-from-disk workloads, each run as a pair of SC and Eventual Consistency (EC) baseline tests for direct comparison. All tests use the gradual throughput increase methodology (`PerformanceRegressionPredefinedStepsTest`), where load is applied at predefined throttle steps and latency is measured at each step.

The key difference between SC and EC configurations is the keyspace creation: SC keyspaces include a `consistency` clause (`consistency = 'global'` in every current config under `configurations/strong_consistency/`), while EC keyspaces omit it. Both use `tablets = {'enabled': true}` with `NetworkTopologyStrategy` and `replication_factor = 3`.

## Jenkins Pipelines

All related automation jobs can be found at:
[Scylla Master Strong Consistency Jobs](https://jenkins.scylladb.com/view/master/job/scylla-master/job/StrongConsistency/)

The following Jenkins pipelines are available:

| Pipeline | Test | Consistency |
|----------|------|-------------|
| `...-predefined-throughput-steps-write-tablets-sc.jenkinsfile` | `test_write_gradual_increase_load` | SC |
| `...-predefined-throughput-steps-read-tablets-sc.jenkinsfile` | `test_read_gradual_increase_load` | SC |
| `...-predefined-throughput-steps-read-tablets-ec.jenkinsfile` | `test_read_gradual_increase_load` | EC baseline |
| `...-predefined-throughput-steps-read-disk-tablets-sc.jenkinsfile` | `test_read_disk_only_gradual_increase_load` | SC |
| `...-predefined-throughput-steps-read-disk-tablets-ec.jenkinsfile` | `test_read_disk_only_gradual_increase_load` | EC baseline |

All pipelines are located under `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/` and use the test class `performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest`.

### Throttle Steps

The SC-specific load steps are defined in `configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml`:

| Workload | Threads | Throttle Steps (ops/s) |
|----------|---------|------------------------|
| write | 400 | 50000, 75000, 100000, 200000, unthrottled |
| read | 620 | 150000, 300000, 450000, 600000, 700000, unthrottled |
| read_disk_only | 620 | 80000, 165000, 250000, 300000, unthrottled |

Each step runs for 30 minutes.

## Execution Guide

Each test is configured by stacking multiple YAML config fragments. The order matters -- later files override earlier ones.

### 1. Enable Strong Consistency

For any SC test, include the experimental feature flag configuration:
`configurations/strong_consistency/enable_experimental_sc.yaml`

This file does three things:
- Enables the `strongly-consistent-tables` experimental feature
- Sets `--blocked-reactor-notify-ms 50` (the non-SC default is `5`)
- Sets `api_address: "0.0.0.0"` so that the Scylla REST API is accessible from loader nodes (required for cql-stress leader-aware load balancing)

When running EC baselines, omit this file.

### 2. Choose the Keyspace Preparation Configuration

Select the config fragment matching your test type and stress tool:

**Write tests:**

| Tool | SC Config | EC Config |
|------|-----------|-----------|
| cassandra-stress | `prepare_cs_ks_with_sc.yaml` | `prepare_cs_ks_with_ec.yaml` |
| cql-stress | `prepare_cql_stress_ks_with_sc.yaml` | `prepare_cql_stress_ks_with_ec.yaml` |

**Read tests (cache-warmed):**

| Tool | SC Config | EC Config |
|------|-----------|-----------|
| cassandra-stress | `prepare_cs_read_ks_with_sc.yaml` | `prepare_cs_read_ks_with_ec.yaml` |
| cql-stress | `prepare_cql_stress_read_ks_with_sc.yaml` | `prepare_cql_stress_read_ks_with_ec.yaml` |

**Read-from-disk tests (no cache warmup, 650M rows):**

| Tool | SC Config | EC Config |
|------|-----------|-----------|
| cassandra-stress | `prepare_cs_read_disk_ks_with_sc.yaml` | `prepare_cs_read_disk_ks_with_ec.yaml` |
| cql-stress | `prepare_cql_stress_read_disk_ks_with_sc.yaml` | `prepare_cql_stress_read_disk_ks_with_ec.yaml` |

All files are under `configurations/strong_consistency/`.

The `cassandra-stress` configs (`prepare_cs_*`) set only `pre_create_keyspace` and rely on the base test YAML for `stress_cmd_*` and `prepare_write_cmd`. The `cql-stress` configs (`prepare_cql_stress_*`) additionally override `stress_cmd_*` and `prepare_write_cmd` to use the `cql-stress-cassandra-stress` binary instead.

### 3. Base Config Chain

Every SC/EC performance pipeline uses a common chain of base configs:

1. `test-cases/performance/perf-regression-predefined-throughput-steps.yaml` -- base test case
2. `configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml` -- SC-specific throttle steps and thread counts
3. `configurations/disable_kms.yaml` -- disables KMS encryption
4. `configurations/disable_speculative_retry.yaml` -- disables speculative retry
5. `configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml` -- latency error thresholds for enterprise tablets
6. *(SC only)* `configurations/strong_consistency/enable_experimental_sc.yaml` -- enables SC feature
7. `configurations/strong_consistency/prepare_*` -- keyspace and stress command configuration


Note: The EC baseline omits `enable_experimental_sc.yaml`.

## Leader-Awareness with cql-stress

> **IMPORTANT: To run a test with leader-awareness using the `cql-stress` command, you must attach the following file to your test configuration:**
> **`configurations/stress_images/cql-stress-strong-consistency.yaml`**

This config fragment overrides the default `cql-stress-cassandra-stress` Docker image with a custom build that includes a Raft-leader-aware load balancing policy. The leader-aware driver routes requests to the tablet leader node, which is critical for accurate SC performance measurements.

Additionally, the SC config (`enable_experimental_sc.yaml`) sets `api_address: "0.0.0.0"` to expose the Scylla REST API on all interfaces, allowing the cql-stress driver on loader nodes to query Raft leader information.

For instructions on building custom cql-stress images from source, see [Building a Custom cql-stress Docker Image](building-custom-cql-stress-image.md).

## Topology Operations (latency during grow / replace / shrink)

The gradual-throughput jobs above measure SC on a cluster that never changes shape. A second job
family measures SC latency **while topology operations run**, using
`performance_regression_test.PerformanceRegressionTest` and the test-case
`test-cases/performance/perf-regression-latency-650gb-with-nemesis.yaml`.

### What the test does

Each sub-test (`test_latency_write_with_nemesis`, `test_latency_read_with_nemesis`,
`test_latency_mixed_with_nemesis`) preloads 650GB, starts a steady fixed-rate load on all loaders,
waits one `nemesis_interval` (30 min) to establish a baseline, then runs `NemesisSequence` once.
That disruption (`Nemesis.disrupt_run_unique_sequence`) is a fixed sequence of topology work:

1. Scylla Manager repair (skipped when `use_mgmt` is false)
2. grow the cluster (`_grow_cluster`)
3. terminate and replace a node (`_terminate_and_replace_node`)
4. shrink the cluster back (`_shrink_cluster`)

with `nemesis_sequence_sleep_between_ops` (10 min) between steps. The latency decorator records
P90/P99 per disruption and reports them to Argus; the duration limits live in
`configurations/performance/latency-decorator-error-thresholds-nemesis-sc-tablets.yaml`, where the
topology disruptions are report-only until calibrated.

When the sequence ends, the load is killed on purpose. `_stop_load_when_nemesis_threads_end()`
silences the stress-tool failure event that the kill produces, matching the event class to the tool
in use - cql-stress and cassandra-stress publish unrelated sibling event classes.

### Pipelines

All four live under `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/`
and run all three sub-tests:

| Pipeline | db instance | Consistency |
|----------|-------------|-------------|
| `...-latency-650gb-with-nemesis-cql-stress-tablets-sc.jenkinsfile` | `i4i.4xlarge` | SC |
| `...-latency-650gb-with-nemesis-cql-stress-i8g-tablets-sc.jenkinsfile` | `i8g.4xlarge` | SC |
| `...-latency-650gb-with-nemesis-cql-stress-tablets-ec.jenkinsfile` | `i4i.4xlarge` | EC baseline |
| `...-latency-650gb-with-nemesis-cql-stress-i8g-tablets-ec.jenkinsfile` | `i8g.4xlarge` | EC baseline |

### Config chain

Fragment order matters. Dict options (`append_scylla_yaml`) are merged key by key, but string
options (`append_scylla_args`, `instance_type_*`) are replaced by the last fragment that sets them.

1. `test-cases/performance/perf-regression-latency-650gb-with-nemesis.yaml`
2. `configurations/disable_kms.yaml`
3. `configurations/disable_speculative_retry.yaml`
4. `configurations/performance/latency-decorator-error-thresholds-nemesis-sc-tablets.yaml`
5. `configurations/aws/i4i_4xlarge.yaml` **or** `configurations/arm_instance_types/i8g_4xlarge.yaml`
6. `configurations/strong_consistency/nemesis_650gb_cql_stress_base.yaml` - loader shape
7. *(SC)* `configurations/strong_consistency/enable_experimental_sc.yaml`
   **or** *(EC)* `configurations/strong_consistency/ec_baseline_align_with_sc.yaml`
8. `configurations/strong_consistency/enable_commitlog_sync_batch.yaml`
9. `configurations/stress_images/cql-stress-strong-consistency-leader-awarness.yaml`
10. `configurations/strong_consistency/prepare_cql_stress_nemesis_ks_with_{sc,ec}_1kpershard.yaml`

Unlike the gradual EC baseline, the EC job here keeps the commitlog and scylla-args settings of the
SC job (step 7, `ec_baseline_align_with_sc.yaml`), so the per-disruption SC-vs-EC delta is
attributable to the keyspace `consistency` clause alone.

`unit_tests/unit/test_sc_topology_operations_pipelines.py` resolves all four chains and asserts these
invariants, so a fragment reorder fails in CI rather than 14 hours into a run.

### 1000 connections per shard

The workload fragments set `-mode connectionsPerShard=250`, and each `stress_cmd_*` is a single
command, so it runs on **all** loaders: 250 x 4 loaders = 1000 connections per shard in aggregate.
The same convention is used by `prepare_cql_stress_ks_with_sc_1kpershard.yaml`. Holding that many
connections needs `c7i.8xlarge` loaders, which `nemesis_650gb_cql_stress_base.yaml` sets.

Rates follow the project convention that a configured rate is per loader, so the aggregate is 4x
the `fixed=` value: write `fixed=12500/s` (50k aggregate, the throttled step of the SC gradual job),
read `fixed=10310/s` and mixed `fixed=8750/s` (the cassandra-stress baseline rates). All three are
starting points to be calibrated from the first run.

## Configuration Files Reference

| File | Purpose |
|------|---------|
| `configurations/strong_consistency/enable_experimental_sc.yaml` | Enables SC feature, sets reactor notify ms, exposes REST API |
| `configurations/strong_consistency/prepare_cs_ks_with_sc.yaml` | SC keyspace + cassandra-stress write commands |
| `configurations/strong_consistency/prepare_cs_ks_with_ec.yaml` | EC keyspace + cassandra-stress write commands |
| `configurations/strong_consistency/prepare_cs_read_ks_with_sc.yaml` | SC keyspace for read tests (cassandra-stress) |
| `configurations/strong_consistency/prepare_cs_read_ks_with_ec.yaml` | EC keyspace for read tests (cassandra-stress) |
| `configurations/strong_consistency/prepare_cs_read_disk_ks_with_sc.yaml` | SC keyspace for disk-only read tests (cassandra-stress) |
| `configurations/strong_consistency/prepare_cs_read_disk_ks_with_ec.yaml` | EC keyspace for disk-only read tests (cassandra-stress) |
| `configurations/strong_consistency/prepare_cql_stress_ks_with_sc.yaml` | SC keyspace + cql-stress write commands |
| `configurations/strong_consistency/prepare_cql_stress_ks_with_ec.yaml` | EC keyspace + cql-stress write commands |
| `configurations/strong_consistency/prepare_cql_stress_read_ks_with_sc.yaml` | SC keyspace + cql-stress read/warmup commands |
| `configurations/strong_consistency/prepare_cql_stress_read_ks_with_ec.yaml` | EC keyspace + cql-stress read/warmup commands |
| `configurations/strong_consistency/prepare_cql_stress_read_disk_ks_with_sc.yaml` | SC keyspace + cql-stress disk-only read commands |
| `configurations/strong_consistency/prepare_cql_stress_read_disk_ks_with_ec.yaml` | EC keyspace + cql-stress disk-only read commands |
| `configurations/stress_images/cql-stress-strong-consistency.yaml` | Custom cql-stress image with leader-aware load balancing |
| `configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml` | SC-specific throttle steps, thread counts, step durations |
| `configurations/stress_images/cql-stress-strong-consistency-leader-awarness.yaml` | cql-stress image with Raft-leader-aware load balancing (topology-operations jobs) |
| `configurations/strong_consistency/nemesis_650gb_cql_stress_base.yaml` | Loader shape for the topology-operations jobs |
| `configurations/strong_consistency/prepare_cql_stress_nemesis_ks_with_sc_1kpershard.yaml` | SC keyspace + cql-stress write/read/mixed load for topology-operations jobs |
| `configurations/strong_consistency/prepare_cql_stress_nemesis_ks_with_ec_1kpershard.yaml` | EC twin of the above |
| `configurations/strong_consistency/ec_baseline_align_with_sc.yaml` | Restores the non-SC settings of `enable_experimental_sc.yaml` for EC baselines |
| `configurations/performance/latency-decorator-error-thresholds-nemesis-sc-tablets.yaml` | Per-disruption latency limits for the SC topology-operations jobs |
