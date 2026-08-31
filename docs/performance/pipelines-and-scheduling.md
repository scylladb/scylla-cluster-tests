# Pipelines and Scheduling

What runs, at which cadence, and where a new test can fit.

`configurations/triggers/perf-regression.yaml` is the single source of truth. Read it for the
current state; this page explains the shape it has.

---

## 1. Cadence

Each cron entry in the trigger YAML fires with a `labels_selector`. A job entry runs when its
`labels` list contains that selector — **but label gating applies to `master` only**. For a
release branch the selector is ignored and the entry is chosen purely by version, so every
release-branch entry carries `labels: []` and runs on whichever trigger firing matches its
version.

| Cadence | `labels_selector` | What occupies it today (master) |
|---|---|---|
| Every 14 days | `master-2weeks` | Throughput staircase, i8g tablets (all four workloads) |
| Every 21 days | `master-3weeks` | Latency 650 GB under nemesis and under rolling upgrade, i8g tablets |
| 1st of month | `master-monthly` | Throughput staircase and latency-under-nemesis on i8g **vnodes**; 2.5 TB elasticity |
| 2nd Tuesday | `gce-custom-monthly` | latte customer steady-state scenario, GCE, tablets and vnodes |
| Saturday | `alternator-weekly` | Alternator `test_full` |
| Daily | `alternator-daily` | Nothing currently — the cron exists with no job entries |

Two more cadences appear in job entries and in `runbook-new-perf-test.md` but have no cron in
the perf trigger today: `master-weekly` and `master-daily` (the daily sanity staircase job is
`disabled: true`).

**Rules of thumb for a new test.** Nemesis and upgrade tests are long and expensive — 3-weekly.
Secondary topologies (vnodes when tablets is the default) — monthly. Core regression coverage
you want to bisect against — 2-weekly. Anything experimental goes under
`jenkins-pipelines/performance_staging/` with no trigger entry at all until it is stable.

## 2. What runs for releases

Release branches get a reduced version of the same jobs, selected by `include_versions` /
`exclude_versions` (prefix matches, so `2025.3` also matches `2025.3.3`):

- **Throughput staircase**, x86 vnodes and tablets, on the currently supported release
  branches. `write` is split into its own job so the four workloads do not serialise.
- **Latency 650 GB under nemesis**, x86 vnodes and tablets.
- **Latency 650 GB during rolling upgrade**, tablets.
- The **i8g** variants of the above, gated with `exclude_versions` so they pick up release
  branches new enough to support Graviton.
- **perf-simple-query microbenchmarks**, arm64 and x86_64, read and write — no version gating,
  so these run for every branch on every firing.

Release entries typically carry a shorter `sub_tests` list than their master counterparts
(often `test_mixed_gradual_increase_load` or `test_latency_mixed_with_nemesis` alone), because
the point is a smoke-level regression signal, not full coverage.

`version_resolution: "common"` makes every job in one triggered run measure the same build.
Performance numbers are only comparable that way; without it each backend would pick up
whatever nightly it happened to have.

## 3. How a trigger becomes a run

```
configurations/triggers/perf-regression.yaml      <- edit this
                |
                |  pre-commit hook: generate-trigger-jenkinsfiles
                v
jenkins-pipelines/master-triggers/sct_triggers/perf-regression-trigger.jenkinsfile  (GENERATED)
                |
                |  triggerMatrixPipeline
                v
jenkins-pipelines/performance/branch-perf-v17/.../<job>.jenkinsfile
                |
                |  perfRegressionParallelPipeline
                v
hydra run-test <test_name> --backend <backend>     (one parallel stage per sub-test)
```

> **Do not hand-edit `perf-regression-trigger.jenkinsfile`.** Run
> `python3 utils/build_system/generate_trigger_jenkinsfiles.py`, or let the
> `generate-trigger-jenkinsfiles` pre-commit hook do it.

The legacy `testRegionMatrix` in `vars/perfRegressionParallelPipelinebyRegion.groovy` is still
in the tree and its header comment is still a readable summary of the old matrix, but new work
goes in the trigger YAML.

### Job entry keys

| Key | Meaning |
|---|---|
| `job_name` | Fully-qualified Jenkins path, leading `/` |
| `backend` | `aws`, `gce`, `azure`, ... |
| `arch` | `x86_64` (default) or `aarch64` |
| `include_versions` / `exclude_versions` | Mutually exclusive version gating |
| `labels` | Which `labels_selector` fires this entry (master only) |
| `job_throttle_category` | Concurrency group, one per (region, instance family) |
| `params.region` | Region for this entry |
| `params.sub_tests` | JSON-encoded list of test method names |

### Per-job pipeline

`vars/perfRegressionParallelPipeline.groovy` fans out **one parallel stage per sub-test**, each
an independent SCT run with its own cluster — so sub-tests in a job do not share state or
interfere. Its `test_config` list is where config layering happens; later entries win, so
thresholds go last:

```groovy
test_config: '''["test-cases/performance/perf-regression-predefined-throughput-steps.yaml",
 "configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml",
 "configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml"]''',
```

## 4. Region assignment

Perf jobs in the same region at the same time contend for capacity and network, which shows up
as latency noise. Rules and the current allocation:
[../perf-tests-region-scheduling.md](../perf-tests-region-scheduling.md).

- Weekly, 3-weekly and monthly tests must not overlap in a region.
- Release-triggered runs (unpredictable timing) **may** overlap with monthly, but **not** with
  weekly or 3-weekly — hence separate MASTER and RELEASE regions per job.
- Regions in use: `us-east-1`, `us-east-2`, `us-west-2`, `eu-west-2`, `eu-west-3`, `eu-north-1`.

When you add a job, pick a region free at its cadence, record it in that document, and set
`job_throttle_category` to match (region, instance family).
