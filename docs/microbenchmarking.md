# Microbenchmarking tests in SCT

SCT runs the scylla microbenchmarks (`scylla perf-*`) on a single provisioned DB node and sends
their results to Argus.

- [Weekly microbenchmark pipelines](../jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/)
  (`scylla-enterprise-perf-{simple-query,cql-raw,alternator}-weekly-microbenchmark_*`)
- [Main SCT test file](../microbenchmarking_test.py): `PerfSimpleQueryTest`, `PerfCqlRawTest`,
  `PerfAlternatorTest`. See [test-catalog.md](performance/test-catalog.md#microbenchmarking_testpy)
