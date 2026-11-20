# Best Throughput Under a P99 Limit, On Demand

**Jira:** SCT-580

## Problem

The gradual performance tests do not give the highest throughput that a
cluster serves while the P99 latency stays under 10 ms. Nobody can ask for
that number for a new setup: a DB instance type, a node count, and a Scylla
version.

- The predefined-steps jobs run a fixed list of throttled rates and one
  unthrottled step. A person reads one Argus table for each step and finds
  the last step with a P99 under 10 ms by eye.
- The rate list is calibrated by hand for each instance type. i8g.4xlarge
  serves about two times the rates of i4i.4xlarge on the same dataset, so the
  two have separate step files. An instance type that nobody calibrated has
  no step list.
- Each instance type needs its own Jenkins job and configuration. A new
  instance type needs both, written by hand.
- A run gives no single "best throughput under 10 ms" value. Argus cannot
  track it across Scylla versions or apply a validation rule to it.
- The dataset is a fixed set of key ranges for nodes with 128 GiB RAM and
  3750 GB disk. On a node with a different RAM or disk size, the read
  workloads do not test what their names say (cache or disk).
- A run takes long. The predefined-steps jobs run 30-minute steps.

## Who it affects

- The performance team, which answers "what does this setup serve under
  10 ms?" for new instance types and Scylla versions.
- The `read`, `write`, `mixed`, and `read_disk_only` sub-tests of
  `PerformanceRegressionPredefinedStepsTest`.
- The goodput work of SCYLLADB-3267, which needs the best throughput as its
  base rate.
- AWS first: i4i, i8g, then z4d.

## Evidence

- A run that finds the best throughput by reading the steps:
  https://argus.scylladb.com/tests/scylla-cluster-tests/1dfde12a-3e8c-4d6e-8a8e-0c1d88b5a939
  (from SCT-580: "We ran a few steps with gradually increasing throttling.
  The last step with latency under 10 ms is the best throughput.")
- Separate step lists for i4i and i8g:
  `configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml`
  and `configurations/performance/cassandra_stress_gradual_load_steps_i8g.yaml`.
- Unthrottled steps at today's thread counts, from the predefined-steps jobs
  (median of the latest 3-5 runs, 2026-10). One thread count does not fit
  both instance types:

  | Workload (threads per loader) | i4i.4xlarge | i8g.4xlarge |
  |---|---|---|
  | read (620) | 869k op/s, P99 42 ms | 1.67M op/s, P99 5.4 ms |
  | read_disk_only (620) | 191k op/s, P99 208 ms | 574k op/s, P99 49 ms |
  | mixed (1900) | 537k op/s, P99 read 477 ms | 875k op/s, P99 read 200 ms |
  | write (400) | no recent results | 708k op/s, P99 6.5 ms |

- A staging run of an unthrottled thread search, read on 3 x i8g.2xlarge
  (https://argus.scylladb.com/tests/scylla-cluster-tests/4e155615-d977-40de-9af4-2c60edc9e58c):
  336 to 672 threads per command all gave 765k-840k op/s, while P99 went from
  3.18 ms to 27.48 ms. The run took 7 hours for one workload.

## Definition of done

1. A user gets the best throughput of a setup from one launcher job, with a
   short list of parameters (DB instance type, Scylla version). Nobody writes
   a job or a configuration for the instance type.
2. Each hardware setup keeps one Argus history across Scylla versions.
3. Each run reports one number for each workload: the highest throughput at
   which every operation has a P99 within the limit (default 10 ms). The
   number is the rate that the load really sent. It is within 5% of the
   highest rate that keeps P99 within the limit.
4. The run needs no rate list calibrated for the instance type.
5. The dataset follows the RAM and disk of the DB nodes. i4i.4xlarge and
   i8g.4xlarge keep today's dataset.
6. A workload takes less time than with today's 30-minute steps.
7. The existing predefined-steps jobs keep their steps, step names, and Argus
   history.
8. Optional: the run reports the goodput at 1.3 and 2 times the best
   throughput, as a ratio of it (SCYLLADB-3267, expected >= 0.9).
9. After the test is tuned on i8g, it runs on z4d.

## Out of scope

- New workloads: CQL UPDATE, DELETE and LWT variants, and Alternator.
- Backends other than AWS.
- Changes to `MaximumPerformanceSearchTest`.
- A search that starts from the result of the last run in Argus.
