# Performance Testing in SCT

Reference documentation for SCT's performance test suite: what exists, how it works,
and the procedures for changing it.

> **Scope note.** This folder documents *what SCT does today*. For where performance testing
> is *going* — statistical baselines, metrics expansion, efficiency scoring — see the strategy
> document at [../performance-testing-strategy.md](../performance-testing-strategy.md), which
> arrives with [PR #14297](https://github.com/scylladb/scylla-cluster-tests/pull/14297).
> If it is later moved into this folder, update these links in one pass.

## Where do I start?

| I want to... | Read |
|---|---|
| Understand what performance tests exist | [test-catalog.md](test-catalog.md) |
| Understand what a perf test actually does, phase by phase | [anatomy.md](anatomy.md) |
| Understand how results are collected and how pass/fail is decided | [results-and-regression.md](results-and-regression.md) |
| Understand which job runs when, and where | [pipelines-and-scheduling.md](pipelines-and-scheduling.md) |
| Migrate tests to a new instance type | [runbook-new-instance-type.md](runbook-new-instance-type.md) |
| Add or switch a stress tool | [runbook-new-stress-tool.md](runbook-new-stress-tool.md) |
| Add a brand-new performance test | [runbook-new-perf-test.md](runbook-new-perf-test.md) |
| Re-derive throughput steps and latency thresholds | [runbook-recalibrate-steps.md](runbook-recalibrate-steps.md) |
| Investigate a latency spike in a finished run | [../performance-tests.md](../performance-tests.md) (`hydra hdr-investigate`) |
| Understand where perf testing is heading | [../performance-testing-strategy.md](../performance-testing-strategy.md) |

## The 30-second model

```
test-case YAML  +  overlay configs        ->  SCT config
(test-cases/performance/)  (configurations/performance/)

        |
        v

Jenkinsfile  ->  perfRegressionParallelPipeline  ->  hydra run-test <test_name>
(jenkins-pipelines/performance/)                      |
                                                      v
                                        Python driver (performance_*_test.py)
                                                      |
                     +--------------------------------+--------------------------------+
                     |                                                                 |
                     v                                                                 v
        stress tool on loaders                                          Prometheus (SCT + Scylla)
        -> HDR histogram .hdr files                                     -> coordinator latency, c-s gauges
                     |                                                                 |
                     +--------------------------------+--------------------------------+
                                                      |
                                     latency_calculator_decorator (one "cycle")
                                                      |
                              +-----------------------+-----------------------+
                              v                                               v
                  latency_results.json (local)                    Argus GenericResultTable
                  result_gradual_increase.log                     + ValidationRule per column
                  (informational only)                                        |
                                                                Argus server validates cells
                                                                              |
                                                        DataValidationError -> FailedResultEvent(ERROR)
                                                                              |
                                                                      test status = FAILED
```

**Key fact:** there is no numeric regression comparison inside SCT. SCT submits raw
numbers plus validation *rules*; Argus evaluates them server-side and the client turns a
validation failure into an ERROR event. See
[results-and-regression.md](results-and-regression.md).

## Vocabulary

| Term | Meaning |
|---|---|
| **cycle** | One measured window. A gradual-load step, a nemesis disruption, an upgrade of one node, or the steady-state baseline. Unit of a row in an Argus result table. |
| **step** / **throttle step** | One rung of the throughput staircase, defined in `perf_gradual_throttle_steps`. Named by its rate (`"300000"`) or `unthrottled`. |
| **steady state** | An unperturbed measurement cycle used as the reference point for "relative to steady" deltas. |
| **HDR tag** | Label written into the `.hdr` file by the stress tool, identifying which operation the histogram belongs to (`READ-st`, `fn--write`, `INSERT`, ...). |
| **workload** | One of `read`, `write`, `mixed`, `read_disk_only`. Selects the Argus result table and the threshold namespace. |
| **overlay config** | A YAML in `configurations/performance/` merged on top of a test-case YAML via the pipeline's `test_config` list. |

## Related documents

- [../performance-tests.md](../performance-tests.md) — `hydra hdr-investigate` utility
- [../perf-tests-region-scheduling.md](../perf-tests-region-scheduling.md) — region non-overlap assignment
- [../microbenchmarking.md](../microbenchmarking.md) — the two microbenchmark approaches
- [../cross-cloud-sizing.md](../cross-cloud-sizing.md) — constraint-based instance sizing
- [../plans/i8g-performance-jobs-migration.md](../plans/i8g-performance-jobs-migration.md) — the worked example behind [runbook-new-instance-type.md](runbook-new-instance-type.md)
- [../plans/latte-concurrency-gradual-throughput.md](../plans/latte-concurrency-gradual-throughput.md) — the worked example behind [runbook-new-stress-tool.md](runbook-new-stress-tool.md)
