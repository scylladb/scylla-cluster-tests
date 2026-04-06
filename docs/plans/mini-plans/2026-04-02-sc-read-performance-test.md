# Mini-Plan: Strong Consistency Read Performance Test

**Date:** 2026-04-02
**Estimated LOC:** ~100
**Related PR:** N/A (follows SCYLLADB-1115 / SCYLLADB-1328 write test commits)

## Problem

The SC write performance test exists but there is no corresponding **read** test to measure SC read latency under gradual load increase and compare it against eventual consistency (EC).

## Approach

- **Fix `preload_data()` to pre-create keyspace.** The gradual test's `preload_data()` (`performance_regression_gradual_grow_throughput.py:249-278`) does not call `_pre_create_keyspace()` before running population commands, unlike the parent class (`performance_regression_test.py:215-216`). Without this, cassandra-stress auto-creates the keyspace without `consistency = 'local'`, defeating SC testing. Add the same guarded call used by all existing callers: `if self.params.get("pre_create_keyspace"): self._pre_create_keyspace()`. The guard is required because `_pre_create_keyspace()` passes the value directly to `session.execute()` and would crash on `None`.
- **Create SC read config fragments for `cassandra-stress`.** The existing SC configs (`configurations/strong_consistency/prepare_*`) blank out `prepare_write_cmd` and only define `stress_cmd_w`. The read test needs `prepare_write_cmd` (to populate data) and relies on `stress_cmd_r` / `stress_cmd_cache_warmup` from the base YAML. Create two new configs:
  - `prepare_cs_read_ks_with_sc.yaml` -- `pre_create_keyspace` with `consistency = 'local'` and tablets, `prepare_write_cmd` with 4 population commands (cl=ALL, n=162500001 each, ~650M rows total)
  - `prepare_cs_read_ks_with_ec.yaml` -- same but without `consistency = 'local'` (EC baseline)
- **Create SC read config fragments for `cql-stress-cassandra-stress`.** Mirror the cassandra-stress variants, replacing the tool name in `prepare_write_cmd`, `stress_cmd_r`, and `stress_cmd_cache_warmup`. These are preparatory configs for future cql-stress pipelines; no consuming pipeline is added in this PR.
  - `prepare_cql_stress_read_ks_with_sc.yaml` (SC)
  - `prepare_cql_stress_read_ks_with_ec.yaml` (EC)
- **Create Jenkins pipeline for SC read test (cassandra-stress).** New Jenkinsfile: `scylla-enterprise-perf-regression-predefined-throughput-steps-read-tablets-sc.jenkinsfile`. Config chain: base perf YAML, `cassandra_stress_gradual_load_steps_strong_consistency.yaml`, `disable_kms.yaml`, `disable_speculative_retry.yaml`, `latency-decorator-error-thresholds-steps-ent-tablets.yaml`, `enable_experimental_sc.yaml`, `prepare_cs_read_ks_with_sc.yaml`. Sub-test: `test_read_gradual_increase_load`.
- **Create Jenkins pipeline for EC baseline read test (cassandra-stress).** New Jenkinsfile: `scylla-enterprise-perf-regression-predefined-throughput-steps-read-tablets-ec.jenkinsfile`. Same config chain as the SC pipeline but swaps `prepare_cs_read_ks_with_sc.yaml` for `prepare_cs_read_ks_with_ec.yaml` and drops `enable_experimental_sc.yaml`. Note: dropping `enable_experimental_sc.yaml` also changes `append_scylla_args` (e.g. `--blocked-reactor-notify-ms 50` vs base default `5`), so the comparison is not perfectly isolated to the SC feature alone.

## Files to Modify

- `performance_regression_gradual_grow_throughput.py` -- Add `_pre_create_keyspace()` call at top of `preload_data()` (line ~250)
- `configurations/strong_consistency/prepare_cs_read_ks_with_sc.yaml` -- (new) SC read config for cassandra-stress
- `configurations/strong_consistency/prepare_cs_read_ks_with_ec.yaml` -- (new) EC read config for cassandra-stress
- `configurations/strong_consistency/prepare_cql_stress_read_ks_with_sc.yaml` -- (new) SC read config for cql-stress-cassandra-stress
- `configurations/strong_consistency/prepare_cql_stress_read_ks_with_ec.yaml` -- (new) EC read config for cql-stress-cassandra-stress
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-read-tablets-sc.jenkinsfile` -- (new) SC read pipeline
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-read-tablets-ec.jenkinsfile` -- (new) EC baseline read pipeline

## Verification

- [ ] Existing unit tests pass: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
- [ ] YAML configs validate: `uv run sct.py lint-yamls`
- [ ] SC config resolves without errors: `SCT_CLUSTER_BACKEND=aws SCT_SCYLLA_VERSION=2025.3.0 uv run sct.py conf --config test-cases/performance/perf-regression-predefined-throughput-steps.yaml --config configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml --config configurations/disable_kms.yaml --config configurations/disable_speculative_retry.yaml --config configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml --config configurations/strong_consistency/enable_experimental_sc.yaml --config configurations/strong_consistency/prepare_cs_read_ks_with_sc.yaml`
- [ ] EC config resolves without errors: `SCT_CLUSTER_BACKEND=aws SCT_SCYLLA_VERSION=2025.3.0 uv run sct.py conf --config test-cases/performance/perf-regression-predefined-throughput-steps.yaml --config configurations/performance/cassandra_stress_gradual_load_steps_strong_consistency.yaml --config configurations/disable_kms.yaml --config configurations/disable_speculative_retry.yaml --config configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml --config configurations/strong_consistency/prepare_cs_read_ks_with_ec.yaml`
- [ ] `stress_cmd_r` and `stress_cmd_cache_warmup` are present in resolved config (inherited from base YAML for cassandra-stress; overridden in cql-stress configs)
- [ ] `prepare_write_cmd` is non-empty in resolved config
- [ ] `pre_create_keyspace` contains `consistency = 'local'` in SC variant, absent in EC variant
- [ ] Manual: Run SC and EC read tests on AWS and confirm keyspaces are created with correct properties, data is populated, and read steps execute with latency results reported to Argus
