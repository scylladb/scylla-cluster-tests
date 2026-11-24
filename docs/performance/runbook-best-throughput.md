# Runbook: Find the Best Throughput of a Setup

How to get "the highest throughput with P99 ≤ 10 ms" for a DB instance type, node count and Scylla version,
without preparing a job or a config for it ([SCT-580](https://scylladb.atlassian.net/browse/SCT-580)).

## 1. One-time setup

1. The jobs run from `branch-perf-v17`, so the change must be there (`backport/perf-v17`).
2. Run `create-performance-jobs` from `branch-perf-v17`. It creates the
   `scylla-enterprise/perf-regression/best-throughput` folder with the `best-throughput-request` launcher and
   the `best-throughput-generic` job.
3. Calibrate: run the launcher once for i4i.4xlarge and once for i8g.4xlarge. If a workload stops with
   `min rate fails` or `max rate passes`, change the default `min_rate_per_shard` or `max_rate_per_shard` in
   `configurations/performance/best-throughput/best_throughput.yaml`. Then add each workload's best throughput
   per shard to `perf_best_throughput_baselines` there, for the instance family (`i8g`), so later runs start from
   it. Take the middle of a few runs rather than one, since one run's number varies by about +-10% (section 4).
   i8g read has one already, from four runs.

## 2. Start the launcher

Jenkins job `scylla-enterprise/perf-regression/best-throughput/best-throughput-request` (created from
`jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/best-throughput/`). Fill in at
least:

| Parameter | Example |
|---|---|
| `instance_type_db` | `i8g.4xlarge` |
| `scylla_version` (or an image parameter) | `2026.2.0`, `master:latest` |

Everything else has a default; each parameter's description in Jenkins says what it does. Tick `dry_run` first
to see the job name and the exact parameters it would run with.

From the command line, the same request as a JSON file (the dry run still checks the instance type with AWS, so
it needs AWS credentials):

```bash
./docker/env/hydra.sh create-best-throughput-job --request-file request.json --dry-run
```

```json
{"backend": "aws", "instance_type_db": "i8g.4xlarge", "scylla_version": "2026.2.0"}
```

### Parameters

Empty means the default.

| Group | Parameter | Default |
|---|---|---|
| Job identity (forms the job name) | `backend` | `aws` (the only one for now) |
| | `instance_type_db` | required |
| | `n_db_nodes` | 3 |
| | `tablets` | true; unchecked runs vnodes |
| | `job_name_suffix` | empty; set it to give an experiment its own job and history. Required with a `test_config` other than the default, or with `n_loaders`, `p99_limit_ms`, `step_duration` or `search_precision_pct` set |
| Setup | `test_config` | the latte predefined-steps test-case and the latte tablets job's configs (see section 3). Another list needs a `job_name_suffix`: the job's Argus history is for one test configuration |
| | `extra_environment_variables` | empty; `KEY=VALUE` lines, and variables left empty are ignored. The instance type and node count come from their own parameters |
| | `region` | us-east-1, where the i4i and i8g predefined-steps jobs run |
| | `instance_type_loader`, `n_loaders` | the test-case's (c7i.8xlarge, 4). Each workload's key range is split into one stress command per loader, so every loader generates load |
| | `sub_tests` | all four workloads |
| | `provision_type` | `on_demand` |
| | `use_job_throttling` | true: the sub-tests run one at a time; unchecked runs them in parallel |
| | `job_throttle_category` | empty: the perf pipeline's `SCT-perf-<region>`, e.g. `SCT-perf-us-east-1` |
| Search and result | `p99_limit_ms` | 10 |
| | `step_duration` | 30m for write and mixed, 10m for read and read_disk_only; set, it applies to every workload |
| | `min_rate_per_shard` | 1000 op/s; times the DB shards. It runs only when the search narrows down to it, and must stay within the limit there |
| | `max_rate_per_shard` | 60000 op/s; times the DB shards. It runs only when the search narrows up to it, and must go over the limit there |
| | `max_search_steps` | 10, the bounds included |
| | `search_precision_pct` | 5 |
| | `overload_factors` | empty: no overload steps; e.g. `1.3,2` |
| Per run (not in the job name) | `scylla_version`, or `scylla_ami_id` / `gce_image_db` / `azure_image_db` / `scylla_repo` / `new_scylla_repo` / `update_db_packages` | one is required |
| | `requested_by_user`, `email_recipients` | empty (set by automated requests); the test-case's recipients |
| | `dry_run` | false |

Each step runs latte at a fixed rate, for 30 minutes on write and mixed, so that compactions and tablet splits
run within it, and for 10 minutes on read and read_disk_only. Without a baseline, the search starts at the middle
of the minimum and maximum rates on a log scale, then bisects between the highest passing and the lowest failing
rate until they are within 5%: 7 steps for bounds 60 times apart. Until a step passes, the minimum counts as the
highest passing rate, and until a step fails, the maximum counts as the lowest failing rate, without being run:
almost every run passes the minimum, and a step at the maximum overloads the cluster and leaves a compaction
backlog for the steps after it. A bound runs only when the search narrows to it, as an 8th step, or, when no
step has passed by then, the minimum runs as the last step of the budget. With a baseline in
`perf_best_throughput_baselines` for the DB instance family and the workload, the search starts at the baseline,
between bounds 25% below and above it that run only when the search narrows to them, and bisects on the
plain middle: about 4 steps for a result
within ~20% of the baseline. A bound that runs and is crossed moves out by another 25%, so a big regression or
improvement still gets a number, in more steps; far from the baseline the step budget can run out first
(`budget`).

Each step gets rate x the P99 limit requests in flight, split over the stress commands; latte keeps the threads of
the test-case's first step for the workload, and gets the requests split over them as its concurrency, which is
per thread. So the load tool never holds the rate back. A step passes when P99 of every operation is within the
limit and it measured at least 98% of its rate. Each step is followed by waits for compactions (at least 3
minutes) and tablet splits (up to 6). Write steps run for the step duration too, over the test-case's key range,
each on a new schema. Each workload runs on its own cluster. After the preload, read takes ~1.75 h without a
baseline and ~1 h with one; write and mixed ~4 h and ~2.5 h.

With `use_job_throttling` (the default) the workloads run one at a time, so all four take ~15.5 h with
their setup and preloads, or ~11 h with baselines. They queue in the Jenkins throttle category with the other perf
jobs that use it; every `SCT-perf-*` category allows one build at a time, so no other perf job in that category
runs alongside and skews the results. Uncheck it to run the four in parallel, in as long as the longest of
them.

The launcher sets `SCT_TEST_DURATION` for the longest run the settings allow, so the test timeout doesn't end a
run before it reports: 180 minutes for setup, preload and teardown, plus (step duration + 10 minutes) for each
of up to `max_search_steps` search steps, 2 reruns and one step per overload factor, for the longest workload.
A step without a duration, e.g. a write step of a custom `test_config` that writes its whole key range, is
allowed 120 minutes. That is 660 minutes at the defaults. Set `SCT_TEST_DURATION` in `extra_environment_variables` to override it, e.g.
when a file in your `test_config` adds steps.

## 3. What the launcher does

1. Validates the request: required fields, supported backend, the config files exist, a `test_config` other than
   the default, or a set `n_loaders`, `p99_limit_ms`, `step_duration` or `search_precision_pct`, comes with a `job_name_suffix`, since each
   changes the result, the instance type is offered in the region, and
   `extra_environment_variables` doesn't contradict the job name.
2. Names the job from the hardware: `<backend>-<instance_type_db>-<n_db_nodes>n-<tablets|vnodes>[-<suffix>]`, e.g.
   `scylla-enterprise/perf-regression/best-throughput/aws-i8g.4xlarge-3n-tablets`, in the launcher's own folder.
3. Creates that job from `best-throughput-generic.jenkinsfile` the first time, waiting for its parameter-loading
   build #1. Later requests reuse it, so the configuration's Argus history continues across Scylla versions.
4. Waits until Argus has a test for the job. Argus imports new Jenkins jobs every 5 minutes, and a run of a job
   it doesn't know yet can't report results, so the first request for a configuration waits up to 30 minutes.
   If the test still isn't there, the launcher fails without starting the job; rerun the request once Argus has
   imported it.
5. Starts it with the final `test_config` and `extra_environment_variables`:
   - `test_config`: yours (default: the latte predefined-steps test-case, its step grid, of which the search uses
     only the latte threads, `disable_kms.yaml` and `disable_speculative_retry.yaml`), then
     `tablets_disabled.yaml` for vnodes, then for the default test-case its RAM/disk-sized dataset
     (`sized_dataset.yaml`) and its time-bound write steps (`latte_time_bound_write.yaml`), and last
     `best_throughput.yaml`, which turns the rate search on.
   - `extra_environment_variables`: yours (variables left empty are ignored), then `SCT_INSTANCE_TYPE_DB`,
     `SCT_N_DB_NODES`, the variables of the loader and search parameters you set, and `SCT_TEST_DURATION` unless
     you set it (section 2). A parameter wins over the same variable in your text.

## 4. Read the result

Each workload (read, write, mixed, read_disk_only) runs on its own cluster, one at a time by default:

- `<workload> - best throughput under P99 limit` (Argus): the measured throughput of the best step, its rate and
  latte threads, P99 read/write, the limit and why the search stopped:
  - `narrowed`: the passing and failing rates are within the precision. The number is the limit.
  - `budget`: the step budget ran out first; the gap to the limit is wider.
  - `min rate fails`: the search narrowed down to the minimum rate, which went over the limit. Throughput 0 with step
    `none passed`; the run reports an error. Lower `min_rate_per_shard`.
  - `max rate passes`: the search narrowed up to the maximum rate, which stayed within the limit, so the
    throughput is a lower bound; the run reports an error. Raise `max_rate_per_shard`.
  - `not repeatable`: no passing rate passed all its repeat runs (below) within their budget. Throughput 0 with
    step `none passed`; the run reports an error.
- Repeat runs (`rate_<rate>_repeat_<n>`): after the search, the best rate runs `repeat_runs` (2) more times, and
  is the result only when all 3 runs pass. When one fails, the next lower passing rate is repeated instead, up to
  `max_repeat_steps` (6) repeat runs. The reported step is one of the confirmed rate's runs; the other rates, and
  the rate that failed a repeat, can't be the best step. `repeat_runs: 0` turns it off.
- Per-step tables `<workload> - rate_<rate> - latencies`: what the search tried.
- The run log gives each step's measured throughput against its rate. A `..._retry` step is a rerun of a step
  that collected no results. If the rerun has none either, the search stops with an error and sends no
  best-throughput row, since a result without that rate would be too low.
- A step that measured under 98% of its rate fails, whatever its P99. A loader over 90% CPU during a step stops
  the search with an error and sends no best-throughput row, as the step measured the loaders, not the cluster:
  raise `n_loaders`.
- A failing step's P99 is not a latency. Latte measures each request from when it was scheduled, so in an
  overloaded step P99 is how far the loaders fell behind: it grows through the step, and with the step's length.
  Compare failing steps by their measured throughput against their rate, not by P99.

### How much one run's number varies

On 3 x i8g.4xlarge read (Scylla 2026.3.4), four runs of the same job found 1,378,334, 1,378,343, 1,470,060 and
1,566,399 op/s, a 13% spread. The search behaved the same in all four; the cluster did not:

- The limit is the busiest shard: with tablets, one shard per node holds 18 tablets instead of 17 and serves
  5.5% more reads, and the cluster fails when that shard on the slowest node runs out of CPU.
- That shard holds a rate steadily at 99%+ of its CPU, or tips into an overload it doesn't recover from: once a
  backlog forms, each read costs it about a third more CPU. Near saturation its load hardly moves with the rate
  (+10% rate took it from 99.0% to 99.7%), so where it tips moves by 5-10% between runs, even on clusters whose
  shards cost the same per read within 1%. A node 4% slower than usual (the VM's host) moved one run lower.
- The search then reports the highest rate it ran below that point, which rounds the limit down by up to a
  ~6.5% step.

Step length was not a factor: every step held steady to its end or broke down within 8 minutes. The two
15-minute runs gave the two highest results, but they differ from each other by 6.5%, a whole search step, and
match each other within 0.2% at every rate both passed. So read one run's number to about +-10%: a single run doesn't show a smaller regression. Compare a new result with
the range of several earlier runs of the job, and check its busiest shard in the run's monitor (per-shard
reactor utilization and `sl:default` task time) before calling a change. The run log gives each step's busiest
shard utilization (`busiest shard: 98.84%`), and the results log keeps it as `busiest_shard_util_pct`.

The repeat runs (section 4) guard against the luckiest case: a rate that passed once, just before its busiest
shard would have tipped, must pass twice more to be the result. They add 2 steps when the best rate holds, and
more when it doesn't.

For a number that repeats more closely, set `max_busiest_shard_util_pct` in the workload's
`perf_best_throughput_search` entry, e.g. `read: {max_busiest_shard_util_pct: 98.5}`: a step whose busiest shard
averaged more fails, whatever its P99. Replayed on the four i8g read runs, 98.5% would have reported 1.29M-1.38M
op/s (6.6% apart, 3.3% without the run on a slower node) instead of 1.38M-1.57M (13.6%). The number then means
the highest rate that keeps ~1.5% CPU headroom on the busiest shard, so it is lower than the P99-only one, and a
baseline has to come from runs with the same setting. It is off by default: write and mixed run compaction on the
same shards, which counts in that utilization, and their runs have to show first whether the limit fits them.

## 5. Variations

| Want | Do |
|---|---|
| A separate history for an experiment on the same hardware | Set `job_name_suffix` |
| Another stress tool, distribution, compaction or compression | Add its file to `test_config`, and set `job_name_suffix` |
| A different limit | `p99_limit_ms`, and set `job_name_suffix` |
| A finer or longer search | `search_precision_pct`, `max_search_steps`, `step_duration`; `SCT_TEST_DURATION` grows with them (section 2) |
| A narrower search | `min_rate_per_shard`, `max_rate_per_shard` around the expected limit |
| Goodput under overload ([SCYLLADB-3267](https://scylladb.atlassian.net/browse/SCYLLADB-3267)) | `overload_factors`, e.g. `1.3,2`: after the search, offer that many times the best throughput; `<workload> - goodput under overload` in Argus, failing the run under 0.9 of the best. The overload steps scale the best step's latte concurrency by the factor. Check `Attempted ratio`: a step under 0.95 of its target didn't overload the cluster (the run warns) |
| A one-off run that shouldn't create a job | Run `best-throughput-generic` (next to the launcher) directly |
| Repeat a run | Rebuild the generated job's last build |
