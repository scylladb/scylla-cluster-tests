# Mini-Plan: Strong Consistency Read-from-Disk Performance Test

**Date:** 2026-04-07
**Estimated LOC:** ~80
**Related PR:** N/A (follows SC read test: SCYLLADB-1115 / SCYLLADB-1328)

## Problem

The SC performance test suite has write and cached-read tests but no **read-from-disk** test. The existing non-SC test (`test_read_disk_only_gradual_increase_load` at `performance_regression_gradual_grow_throughput.py:182-207`) reads the full 650M-row dataset so that data exceeds RAM and forces disk I/O, skips cache warmup, and uses dedicated throttle steps (`read_disk_only` in `configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml:7`). An equivalent SC vs EC comparison is needed to measure how strong consistency affects disk-bound read latency.

## Approach

No Python code changes are required. The `test_read_disk_only_gradual_increase_load` test method already exists, `preload_data()` already calls `_pre_create_keyspace()` when `pre_create_keyspace` is set (`performance_regression_gradual_grow_throughput.py:250-251`), and the SC-specific `perf_gradual_throttle_steps.read_disk_only` steps are already defined in `cassandra_stress_gradual_load_steps_strong_consistency.yaml:7` as `['80000', '165000', '250000', '300000', 'unthrottled']`.

The work consists of new config fragments and Jenkins pipelines:

- **Create cassandra-stress disk-only config fragments.** Two new files following the pattern of the existing read configs (`prepare_cs_read_ks_with_sc.yaml`, `prepare_cs_read_ks_with_ec.yaml`) but dedicated to the disk-only test so future changes to either test do not leak across:
  - `prepare_cs_read_disk_ks_with_sc.yaml` -- blanks `prepare_stress_cmd`, sets `pre_create_keyspace` with `consistency = 'local'` and tablets. `stress_cmd_read_disk` and `prepare_write_cmd` are inherited from the base YAML (`test-cases/performance/perf-regression-predefined-throughput-steps.yaml:6-11,42-47`).
  - `prepare_cs_read_disk_ks_with_ec.yaml` -- same but without `consistency = 'local'`.
- **Create cql-stress disk-only config fragments (preparatory, no pipeline yet).** Two new files mirroring the cassandra-stress variants but overriding `prepare_write_cmd` and `stress_cmd_read_disk` with `cql-stress-cassandra-stress` commands. No `stress_cmd_cache_warmup` override is needed because the disk-only test does not use cache warmup (`cs_cmd_warm_up=None` at `performance_regression_gradual_grow_throughput.py:193`).
  - `prepare_cql_stress_read_disk_ks_with_sc.yaml` (SC)
  - `prepare_cql_stress_read_disk_ks_with_ec.yaml` (EC)
- **Create Jenkins pipeline for SC disk-only read test.** Config chain: base perf YAML, `cassandra_stress_gradual_load_steps_strong_consistency.yaml`, `disable_kms.yaml`, `disable_speculative_retry.yaml`, `latency-decorator-error-thresholds-steps-ent-tablets.yaml`, `enable_experimental_sc.yaml`, `prepare_cs_read_disk_ks_with_sc.yaml`. Sub-test: `test_read_disk_only_gradual_increase_load`.
- **Create Jenkins pipeline for EC baseline disk-only read test.** Same config chain but swaps `prepare_cs_read_disk_ks_with_sc.yaml` for `prepare_cs_read_disk_ks_with_ec.yaml` and drops `enable_experimental_sc.yaml`. Note: dropping `enable_experimental_sc.yaml` also changes `append_scylla_args` (`--blocked-reactor-notify-ms 50` vs base default `5`) and removes `api_address: "0.0.0.0"`, so the comparison is not perfectly isolated to the SC feature alone (same known limitation as the existing read/write SC tests).

## Files to Modify

- `configurations/strong_consistency/prepare_cs_read_disk_ks_with_sc.yaml` -- (new) SC disk-only keyspace config for cassandra-stress
- `configurations/strong_consistency/prepare_cs_read_disk_ks_with_ec.yaml` -- (new) EC disk-only keyspace config for cassandra-stress
- `configurations/strong_consistency/prepare_cql_stress_read_disk_ks_with_sc.yaml` -- (new) SC disk-only config for cql-stress-cassandra-stress
- `configurations/strong_consistency/prepare_cql_stress_read_disk_ks_with_ec.yaml` -- (new) EC disk-only config for cql-stress-cassandra-stress
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-read-disk-tablets-sc.jenkinsfile` -- (new) SC disk-only read pipeline
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-read-disk-tablets-ec.jenkinsfile` -- (new) EC baseline disk-only read pipeline

## Verification

- [ ] Existing unit tests pass: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
- [ ] SC config resolves without errors: `SCT_CLUSTER_BACKEND=aws SCT_SCYLLA_VERSION=2025.3.0 uv run sct.py conf --config test-cases/performance/perf-regression-predefined-throughput-steps.yaml --config configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml --config configurations/disable_kms.yaml --config configurations/disable_speculative_retry.yaml --config configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml --config configurations/strong_consistency/enable_experimental_sc.yaml --config configurations/strong_consistency/prepare_cs_read_disk_ks_with_sc.yaml`
- [ ] EC config resolves without errors: `SCT_CLUSTER_BACKEND=aws SCT_SCYLLA_VERSION=2025.3.0 uv run sct.py conf --config test-cases/performance/perf-regression-predefined-throughput-steps.yaml --config configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml --config configurations/disable_kms.yaml --config configurations/disable_speculative_retry.yaml --config configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml --config configurations/strong_consistency/prepare_cs_read_disk_ks_with_ec.yaml`
- [ ] `stress_cmd_read_disk` is present in resolved config (inherited from base YAML)
- [ ] `prepare_write_cmd` is non-empty in resolved config (inherited from base YAML)
- [ ] `pre_create_keyspace` contains `consistency = 'local'` in SC variant, absent in EC variant
- [ ] Manual: Run SC and EC disk-only read tests on AWS and confirm keyspaces are created with correct properties, data is populated, and disk-only read steps execute with latency results reported to Argus
