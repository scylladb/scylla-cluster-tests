# Taxonomy Field Values Reference

All valid values are defined in `docs/pipeline-labels/taxonomy.yaml` (single source of truth).
The pydantic model in `sdcm/test_metadata.py` loads and enforces them at runtime.

## test_type
longevity, performance, upgrade, artifacts, manager, functional, scale, jepsen, gemini, features, platform-migration, vector-search, cdc, operator, cassandra, kafka, microbenchmarking, load

## tier
sanity, tier1, tier2, release, ondemand

## duration_class

`test_duration` in config is in **minutes**:

| Value | Threshold | Example |
|-------|-----------|---------|
| `short` | `test_duration < 360` | under 6 hours |
| `medium` | `360 <= test_duration <= 1440` | 6–24 hours |
| `long` | `test_duration > 1440` | over 24 hours |

## workload
write, read, mixed, scan, counter, delete, user-profile

## stress_tools
cassandra-stress, cql-stress-cassandra-stress, scylla-bench, ycsb, latte, gemini, ndbench, nosqlbench, cdcreader

## nemesis_labels
Any class from NemesisRegistry + NemesisRunner (124 values). See `docs/pipeline-labels/taxonomy.yaml` nemesis_labels section for the full list.

## features
tls-ssl, multi-dc, cdc, tablets, vnodes, alternator, encryption-at-rest, ldap, authorization, ipv6, counters, large-partitions, materialized-views, secondary-indexes, lwt, schema-changes, tombstone-gc, twcs, elasticity, ... (see taxonomy.yaml for full list)

## supported_backends
aws, gce, azure, docker, k8s-eks, k8s-gke, k8s-local-kind, baremetal, xcloud, oci
<<<<<<< HEAD
||||||| parent of 2a6cb6c61 (refactor(test-metadata): use GitHub team slugs for team_ownership)

## team_ownership
core-test-infra, core-qa, storage-qa, cloud-qa, qa-tools, scylladb/alternator, scylladb/mgmt — the team
responsible for maintaining this test: a QA team, or the GitHub team (`scylladb/<team>`) that owns the feature.
=======

## team_ownership
The GitHub team (`scylladb/<team>`) responsible for maintaining this test, and its Jira team:

| Value | Jira team |
|-------|-----------|
| scylladb/testing-infrastructure | Core Test Infra, QA Tools |
| scylladb/core-qa | Core QA |
| scylladb/storage-qa | Storage QA |
| scylladb/rnd-cloud-qa | Cloud QA |
| scylladb/alternator | Alternator |
| scylladb/mgmt | Cloud Data (Scylla Manager) |
>>>>>>> 2a6cb6c61 (refactor(test-metadata): use GitHub team slugs for team_ownership)
