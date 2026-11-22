# perf-alternator microbenchmark

**Jira:** SCT-1382

## Problem

Scylla has a microbenchmark for Alternator, `scylla perf-alternator`. It
measures the cost of one Alternator request: allocations, tasks, instructions,
and CPU cycles per operation. SCT does not run it.

SCT runs the same kind of benchmark for CQL on each release and on PGO
builds of master: perf-simple-query and perf-cql-raw report their results to
Argus, and Argus flags a change in the per-operation cost. Alternator has no such tracking. A change in Scylla that
makes each Alternator request more expensive goes unnoticed until a bigger
Alternator test or a user finds it.

## Who it affects

- The performance team, which watches the microbenchmark results in Argus and
  reports them each week.
- Alternator developers, who get no per-request cost history for a release.
- The `perf-regression` trigger, which runs the microbenchmarks for each
  Scylla release and for master.

## Evidence

SCT-1382:

> Track weekly runs of an alternator perf workload set, similar to
> perf-simple-query but for Alternator.
>
> A perf-simple-query like benchmark was added for Alternator in
> https://github.com/scylladb/scylla-enterprise/commit/55030b155078c4cf6d11631e1141618ebcd7e3e4.
> We can start tracking it on a weekly basis, the same way we do for Scylla.

The commit that added the tool:

> The above seem more realistic than results from perf_simple_query which are
> 96k and 49k tps (per core).

A local run of the 2026.3.4 Docker image, `--workload read --duration 10
--json-result`, exit code 0. The stats of the result file:

```json
"stats": {
    "allocs_per_op": 361.10286879482493,
    "cpu_cycles_per_op": 0.0,
    "instructions_per_op": 0.0,
    "logallocs_per_op": 0.0,
    "mad tps": 2335.7733226031705,
    "max tps": 44045.804507330286,
    "median tps": 42615.507995508204,
    "min tps": 32972.24236817496,
    "tasks_per_op": 41.141283457554024
}
```

The instructions and the cycles are 0 because the local host blocks the
perf counters (`perf_event_paranoid=4`).

## Definition of done

- The Alternator microbenchmark runs for each release from 2026.4, and on the
  PGO builds of master, as perf-simple-query does, for the read and the write
  workloads, on x86_64 and on arm64.
- Each run puts one Argus result per workload, with the same per-operation
  metrics as the other microbenchmarks. The instructions and the cycles per
  operation are not 0.
- A release older than 2026.4 does not trigger the benchmark. Before 2026.2
  its `perf-alternator` tool cannot write a result file, and only the numbers
  of new releases are worth tracking.
- The benchmark table uses tablets, as on a production node.
- A PGO build triggers the benchmark too, as it triggers perf-cql-raw.
- After the run, the Scylla server of the DB node runs again, also when the
  benchmark fails.
- The perf-cql-raw benchmark gives the same results as before the change.
- The performance docs include the new benchmark.

## Out of scope

- Thresholds that fail a run. The first runs set the baseline.
- The `scan`, `write_gsi`, `write_rmw`, and batch workloads.
- The weekly perf report and the version comparison report. Both list each
  job by its Argus test ID, which exists only after the first run.
- The "promote to quick tests" item of SCT-1382. Its meaning is not clear,
  and it waits for the answer of the reporter.
