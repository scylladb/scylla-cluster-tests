# Runbook: Find the Best Throughput of a Setup

How to get "the highest throughput with P99 ≤ 10 ms" for a DB instance type, node count and Scylla version,
without preparing a job or a config for it ([SCT-580](https://scylladb.atlassian.net/browse/SCT-580)).

## 1. One-time setup

1. The jobs run from `branch-perf-v17`, so the change must be there (`backport/perf-v17`).
2. Run `create-performance-jobs` from `branch-perf-v17`. It creates the
   `scylla-enterprise/perf-regression/best-throughput` folder with the `best-throughput-request` launcher and
   the `best-throughput-generic` job.
3. Calibrate: run the launcher once for i4i.4xlarge and once for i8g.4xlarge. If most search steps went into
   doubling, raise the default `start_threads_per_shard` in
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
| | `job_name_suffix` | empty; set it to give an experiment its own job and history |
| Setup | `test_config` | the predefined-steps test-case and the i8g job's configs (see section 3) |
| | `extra_environment_variables` | `SCT_INSTANCE_TYPE_DB=` (a placeholder; empty variables are ignored) |
| | `region` | us-east-1, where the i4i and i8g predefined-steps jobs run |
| | `instance_type_loader`, `n_loaders` | the test-case's (c7i.8xlarge, 4) |
| | `sub_tests` | all four workloads |
| | `provision_type` | `on_demand` |
| Search and result | `p99_limit_ms` | 10 |
| | `step_duration` | 30m |
| | `start_threads_per_shard` | 16 |
| | `max_search_steps` | 10; it only limits the bisection once the limit is found |
| | `search_precision_pct` | 5 |
| | `overload_factors` | empty: no overload steps; e.g. `1.3,2` |
| Per run (not in the job name) | `scylla_version`, or `scylla_ami_id` / `gce_image_db` / `azure_image_db` / `scylla_repo` / `new_scylla_repo` / `update_db_packages` | one is required |
| | `requested_by_user`, `email_recipients` | empty (set by automated requests); the test-case's recipients |
| | `dry_run` | false |

A run takes ~7-8 h per workload at the defaults: the preload, then 6-10 steps of 30 minutes, each followed by
waits for compactions (at least 3 minutes) and tablet splits (up to 6). The four workloads run in parallel on
their own clusters.

The launcher sets `SCT_TEST_DURATION` for the longest run the settings allow, so the test timeout doesn't end a
run before it reports: 180 minutes for setup, preload and teardown, plus (step duration + 10 minutes) for each
of up to max(`max_search_steps`, 13) search steps, 2 reruns and one step per overload factor. 13 is the steps
from 1 thread to the cap of 4096 threads per command, which the budget doesn't cut short. That is 780 minutes at
the defaults. Set `SCT_TEST_DURATION` in `extra_environment_variables` to override it, e.g. when a file in your
`test_config` adds steps.

## 3. What the launcher does

1. Validates the request: required fields, supported backend, the config files exist, the instance type is offered
   in the region, and `extra_environment_variables` doesn't contradict the job name.
2. Names the job from the hardware: `<backend>-<instance_type_db>-<n_db_nodes>n-<tablets|vnodes>[-<suffix>]`, e.g.
   `scylla-enterprise/perf-regression/best-throughput/aws-i8g.4xlarge-3n-tablets`, in the launcher's own folder.
3. Creates that job from `best-throughput-generic.jenkinsfile` the first time, waiting for its parameter-loading
   build #1. Later requests reuse it, so the configuration's Argus history continues across Scylla versions.
4. Waits until Argus has a test for the job. Argus imports new Jenkins jobs every 5 minutes, and a run of a job
   it doesn't know yet can't report results, so the first request for a configuration waits up to 30 minutes.
   If the test still isn't there, the launcher fails without starting the job; rerun the request once Argus has
   imported it.
5. Starts it with the final `test_config` and `extra_environment_variables`:
   - `test_config`: yours (default: the predefined-steps test-case, the i8g step grid, which the search ignores,
     `c-s-driver-version-4.yaml`, `disable_kms.yaml` and `disable_speculative_retry.yaml`), then
     `tablets_disabled.yaml` for vnodes, then for the default test-case its time-bound write commands and
     RAM/disk-sized dataset, and last `best_throughput.yaml`, which turns the thread search on.
   - `extra_environment_variables`: yours (variables left empty, like the default `SCT_INSTANCE_TYPE_DB=`, are
     ignored), then `SCT_INSTANCE_TYPE_DB`, `SCT_N_DB_NODES`, the variables of the loader and search
     parameters you set, and `SCT_TEST_DURATION` unless you set it (section 2). A parameter wins over the same
     variable in your text.

## 4. Read the result

Each workload (read, write, mixed, read_disk_only) runs in parallel on its own cluster:

- `<workload> - best throughput under P99 limit` (Argus): the best throughput, its thread count, P99 read/write,
  the limit and why the search stopped. Throughput 0 with step `none passed` means no thread count stayed within
  the limit. Stop reason `thread cap` means every step up to the cap stayed within it: the throughput is a lower
  bound, not the limit.
- Per-step tables `<workload> - unthrottled_<threads>_threads - latencies`: what the search tried.
- The run log says why the search stopped (narrowed to the precision, throughput saturated, reached the cap of
  threads per command, or the step budget ran out while bisecting; the budget never stops the search before it
  finds the limit). A `..._threads_retry` step is a rerun of a step that collected no results.

## 5. Variations

| Want | Do |
|---|---|
| A separate history for an experiment on the same hardware | Set `job_name_suffix` |
| Another stress tool, distribution, compaction or compression | Add its file to `test_config` |
| A different limit | `p99_limit_ms` |
| A finer or longer search | `search_precision_pct`, `max_search_steps`, `step_duration`; `SCT_TEST_DURATION` grows with them (section 2) |
| Goodput under overload ([SCYLLADB-3267](https://scylladb.atlassian.net/browse/SCYLLADB-3267)) | `overload_factors`, e.g. `1.3,2`: after the search, offer that many times the best throughput; `<workload> - goodput under overload` in Argus, failing the run under 0.9 of the best |
| A one-off run that shouldn't create a job | Run `best-throughput-generic` (next to the launcher) directly |
| Repeat a run | Rebuild the generated job's last build |
