# Microbenchmarking tests in SCT
Currently, SCT contains 2 Microbenchmarking tests implementation:

#### 1. Aggregation and reports in Elastic results of Scylla-pkg micro benchmarking pipeline
Tests run not in SCT. SCT just does the aggregation and reports

#### 2. Provision separate ENV and run microbenchmarking via SCT itself
Tests run by  SCT. SCT also controls which env to run it

> The main difference between these approaches is the environment where tests run.
1st runs on Jenkins agent, 2nd on specific provisioned for this test instance type

#### The main files for both approaches:
1. - [scylla-pkg pipeline](https://github.com/scylladb/scylla-pkg/blob/next/scripts/jenkins-pipelines/microbenchmarks.jenkinsfile)
    - [main SCT file](../sdcm/microbenchmarking.py)
 ---
2.  - [perf-simple-query test pipeline](../jenkins-pipelines/performance_staging/perf-simple-query.jenkinsfile)
    - [main SCT test file](../microbenchmarking_test.py)
