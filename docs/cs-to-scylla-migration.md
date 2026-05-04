# Cassandra -> Scylla migration tests

End-to-end SCT tests that provision a Cassandra source, a Scylla target, and an
Amazon EMR cluster, then drive the [scylla-migrator](https://github.com/scylladb/scylla-migrator)
job between them. Two variants exist:

- **Small** (`test_migration_cs_to_scylla_small`, ~30 min): 100K rows of simple
  schema, can be used as PR-gating regression coverage.
- **Scale** (`test_migration_cs_to_scylla_scale`, hours): 500GB latte preload
  (4 × 125 M rows), can be used for weekly throughput + validator coverage.

## Quick start

The test cases are under `test-cases/spark-migrator/`:

```sh
# small (PR-gating)
hydra run-test spark_migrator_test.SparkMigratorTest.test_migration_cs_to_scylla_small \
    --backend aws --config test-cases/spark-migrator/cs-to-scylla-small.yaml

# scale (nightly)
hydra run-test spark_migrator_test.SparkMigratorTest.test_migration_cs_to_scylla_scale \
    --backend aws --config test-cases/spark-migrator/cs-to-scylla-500gb.yaml
```

Both yamls set `db_type: mixed_cassandra` so SCT provisions BOTH a Scylla target
cluster (`self.db_cluster`) AND a Cassandra source cluster (`self.cs_db_cluster`)
in a single run, plus an EMR cluster (`self.emr_cluster`) sized for the migration job.

## Reusing a provisioned cluster

The 500GB preload takes a long time. To skip preload and rerun the migration + validation
against an already-provisioned cluster, set `SCT_REUSE_CLUSTER` to the test_id of the
previous run:

```sh
SCT_REUSE_CLUSTER=<previous-test-id> hydra run-test ...
```

When `SCT_REUSE_CLUSTER` is set, `test_migration_cs_to_scylla_scale` skips the
`prepare_write_cmd` chunks entirely. The schema apply loop is idempotent
(`CREATE TABLE|TYPE|INDEX` statements are rewritten to `... IF NOT EXISTS` on the fly)
so re-running against pre-existing target tables is safe.


## Validation strategy

- **Small test**: post-migration sanity = `_count_rows_parallel` (token-range sharded
  `count(*)`) + 10-row sample compare against source.
  Validator EMR step is **off** by default (`migrator_run_validator: false` in test config).
- **Scale test**: `count(*)` + sampling does **not** apply — at 500 M rows, sharded
  `count(*)` hits Scylla's server-side `read_request_timeout` regardless of client-side
  parallelism. Correctness is delegated to the validator EMR step
  (`migrator_run_validator: true` by default in the 500GB test config), which scans both source
  and target end-to-end and reports per-row mismatches. Tune `validator_step_timeout_minutes`
  (default 240) when source/target sizes change.

## Validator output

When `migrator_run_validator: true` (default in the 500GB yaml), a second EMR step runs
scylla-migrator's `com.scylladb.migrator.Validator` main class against the same
source/target tables after the migration completes.

The EMR step state is the verdict. Per upstream `Validator.scala main()`:

- When zero failures during migration -> `log.info("No comparison failures found - enjoy your day!")`
- is emitted and step state is COMPLETED.
- One or more failures occurred during migration -> `log.error("Found N comparison failure(s) in sample: <breakdown>")`,
  plus dump of `RowComparisonFailure` entries, are emitted and step state is FAILED.

| EMR step state | SCT verdict                                                                |
|----------------|----------------------------------------------------------------------------|
| COMPLETED      | pass - test continues                                                      |
| FAILED         | fail - validator step stdout (including the upstream `Found N` summary line and per-row `RowComparisonFailure` records) is downloaded from S3 and dumped to `sct.log` for investigation, then `AssertionError` is raised |

`emr_spark_migrator_release` should be pinned to a known-compatible tag (the 500GB yaml currently
pins `v2.0.1`); bump deliberately and re-verify the upstream output contract when moving to a
new migrator release.

## Cluster lifecycle and cost

- `post_behavior_db_nodes` controls the tear-down policy for BOTH the Scylla target AND the
  Cassandra source - `mixed_cassandra` does NOT have a separate `post_behavior_cs_db_nodes`
  parameter (`sdcm/tester.py:3835`). Setting `keep-on-failure` therefore retains a 500GB
  Cassandra source on failure; destroy it manually if the cluster is no longer needed.
- `post_behavior_emr_cluster: destroy` is set by default so EMR clusters do not leak; expected
  hourly cost for the scale yaml is dominated by the EMR core nodes (2 × `m5.2xlarge`,
  ~$0.20/h on-demand at time of writing).
- Region coherence is asserted at the start of each test — the EMR cluster must be provisioned
  in the same region as the SCT clusters, otherwise cross-region data transfer turns the
  migration into a slow + expensive run. Fix the region in the yaml if the assertion fires.

## Schema transfer

scylla-migrator v2.0.x does not auto-create CQL tables on the target. Both tests therefore
pre-create the target schema on Scylla before submitting the migrator step:

- The small test uses a hardcoded simple schema via `_prepare_target_schema`.
- The scale test calls `BaseCassandraCluster.dump_schema` on the Cassandra source and
  applies the resulting CQL on the Scylla target. The dump strips Cassandra-only table
  options (`compression.crc_check_chance`, `speculative_retry`, etc.) so the apply loop
  is byte-for-byte compatible with Scylla's CQL grammar.

## Cross-VPC notes

`test_migration_from_external_source` migrates from a pre-existing Cassandra cluster discovered
by EC2 tags (`migrator_source_test_id`). When the source cluster lives in a different VPC than
the EMR cluster, security groups must allow inbound CQL (`9042`) from the EMR worker subnet -
otherwise the migrator step hangs on connection setup. `test_migration_cs_to_scylla_*` tests
provision both clusters in the same VPC so this concern only applies to the external-source variant.
