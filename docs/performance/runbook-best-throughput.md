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
   `configurations/performance/best-throughput/best_throughput.yaml`.

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
| | `job_name_suffix` | empty; set it to give an experiment its own job and history. Required with a `test_config` other than the default |
| Setup | `test_config` | the latte predefined-steps test-case and the latte tablets job's configs (see section 3). Another list needs a `job_name_suffix`: the job's Argus history is for one test configuration |
| | `extra_environment_variables` | empty; `KEY=VALUE` lines, and variables left empty are ignored. The instance type and node count come from their own parameters |
| | `region` | us-east-1, where the i4i and i8g predefined-steps jobs run |
| | `instance_type_loader`, `n_loaders` | the test-case's (c7i.8xlarge, 4). Each workload's key range is split into one stress command per loader, so every loader generates load |
| | `sub_tests` | all four workloads |
| | `provision_type` | `on_demand` |
| | `use_job_throttling` | true: the sub-tests run one at a time; unchecked runs them in parallel |
| | `job_throttle_category` | empty: the perf pipeline's `SCT-perf-<region>`, e.g. `SCT-perf-us-east-1` |
| Search and result | `p99_limit_ms` | 10 |
| | `step_duration` | 10m, for every workload |
| | `min_rate_per_shard` | 1000 op/s; times the DB shards, the first step, which must stay within the limit |
| | `max_rate_per_shard` | 60000 op/s; times the DB shards. It runs only when the search narrows up to it, and must go over the limit there |
| | `max_search_steps` | 10, the bounds included |
| | `search_precision_pct` | 5 |
| | `overload_factors` | empty: no overload steps; e.g. `1.3,2` |
| Per run (not in the job name) | `scylla_version`, or `scylla_ami_id` / `gce_image_db` / `azure_image_db` / `scylla_repo` / `new_scylla_repo` / `update_db_packages` | one is required |
| | `requested_by_user`, `email_recipients` | empty (set by automated requests); the test-case's recipients |
| | `dry_run` | false |

Each step runs latte at a fixed rate for 10 minutes. The search runs the minimum rate, then bisects between the
highest passing and the lowest failing rate on a log scale until they are within 5%: 8 steps for bounds 60 times
apart. Until a step fails, the maximum counts as the lowest failing rate without being run, so the search doesn't
overload the cluster or, for mixed, leave a compaction backlog for the steps after it. The maximum runs only when
the search narrows up to it, as a 9th step. Each step gets rate x the P99 limit requests in flight (latte keeps
the threads of the test-case's first step for the workload and gets the rest as its concurrency), so the load tool
never holds the rate back. A step passes when P99 of every operation is within the limit and it measured at least
95% of its rate. A workload takes ~3 h at the defaults: the preload, then up to 10 steps, each followed by waits
for compactions (at least 3 minutes) and tablet splits (up to 6). Write steps run for the step duration too, over
the test-case's key range, each on a new schema. Each workload runs on its own cluster.

With `use_job_throttling` (the default) the workloads run one at a time, so all four take ~12 h. They queue in the Jenkins throttle category with the other perf
jobs that use it; every `SCT-perf-*` category allows one build at a time, so no other perf job in that category
runs alongside and skews the results. Uncheck it to run the four in parallel, in as long as the longest of
them.

The launcher sets `SCT_TEST_DURATION` for the longest run the settings allow, so the test timeout doesn't end a
run before it reports: 180 minutes for setup, preload and teardown, plus (step duration + 10 minutes) for each
of up to `max_search_steps` search steps, 2 reruns and one step per overload factor, for the longest workload.
A step without a duration, e.g. a write step of a custom `test_config` that writes its whole key range, is
allowed 120 minutes. That is 420 minutes at the defaults. Set `SCT_TEST_DURATION` in `extra_environment_variables` to override it, e.g.
when a file in your `test_config` adds steps.

## 3. What the launcher does

1. Validates the request: required fields, supported backend, the config files exist, a `test_config` other than
   the default comes with a `job_name_suffix`, the instance type is offered in the region, and
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
  - `min rate fails`: the first step, at the minimum rate, went over the limit. Throughput 0 with step
    `none passed`; the run reports an error. Lower `min_rate_per_shard`.
  - `max rate passes`: the search narrowed up to the maximum rate, which stayed within the limit, so the
    throughput is a lower bound; the run reports an error. Raise `max_rate_per_shard`.
- Per-step tables `<workload> - rate_<rate> - latencies`: what the search tried.
- The run log gives each step's measured throughput against its rate. A step that measured under 95% of its rate
  fails, and the log says the loaders held it back: raise `n_loaders`. A `..._retry` step is a rerun of a step
  that collected no results.

## 5. Variations

| Want | Do |
|---|---|
| A separate history for an experiment on the same hardware | Set `job_name_suffix` |
| Another stress tool, distribution, compaction or compression | Add its file to `test_config`, and set `job_name_suffix` |
| A different limit | `p99_limit_ms` |
| A finer or longer search | `search_precision_pct`, `max_search_steps`, `step_duration`; `SCT_TEST_DURATION` grows with them (section 2) |
| A narrower search | `min_rate_per_shard`, `max_rate_per_shard` around the expected limit |
| Goodput under overload ([SCYLLADB-3267](https://scylladb.atlassian.net/browse/SCYLLADB-3267)) | `overload_factors`, e.g. `1.3,2`: after the search, offer that many times the best throughput; `<workload> - goodput under overload` in Argus, failing the run under 0.9 of the best. The overload steps scale the best step's latte concurrency by the factor. Check `Attempted ratio`: a step under 0.95 of its target didn't overload the cluster (the run warns) |
| A one-off run that shouldn't create a job | Run `best-throughput-generic` (next to the launcher) directly |
| Repeat a run | Rebuild the generated job's last build |
