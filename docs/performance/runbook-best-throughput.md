# Runbook: Find the Best Throughput of a Setup

How to get "the highest throughput with P99 ≤ 10 ms" for a DB instance type, node count and Scylla version,
without preparing a job or a config for it ([SCT-580](https://scylladb.atlassian.net/browse/SCT-580)).

## 1. Start the launcher

Jenkins job `best-throughput/best-throughput-request` (created from
`jenkins-pipelines/performance/branch-perf-v17/best-throughput/`). Fill in at least:

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

## 2. What the launcher does

1. Validates the request: required fields, supported backend, the config files exist, the instance type is offered
   in the region, and `extra_environment_variables` doesn't contradict the job name.
2. Names the job from the hardware: `<backend>-<instance_type_db>-<n_db_nodes>n-<tablets|vnodes>[-<suffix>]`, e.g.
   `best-throughput/aws-i8g.4xlarge-3n-tablets`.
3. Creates that job from `best-throughput-generic.jenkinsfile` the first time, waiting for its parameter-loading
   build #1. Later requests reuse it, so the configuration's Argus history continues across Scylla versions.
4. Starts it with the final `test_config` and `extra_environment_variables`:
   - `test_config`: yours (default: the predefined-steps test-case, the i8g step grid, which the search ignores,
     `c-s-driver-version-4.yaml`, `disable_kms.yaml` and `disable_speculative_retry.yaml`), then
     `tablets_disabled.yaml` for vnodes, then for the default test-case its time-bound write commands and
     RAM/disk-sized dataset, and last `best_throughput.yaml`, which turns the thread search on.
   - `extra_environment_variables`: yours (variables left empty, like the default `SCT_INSTANCE_TYPE_DB=`, are
     ignored), then `SCT_INSTANCE_TYPE_DB`, `SCT_N_DB_NODES` and the variables of the loader and search
     parameters you set. A parameter wins over the same variable in your text.

## 3. Read the result

Each workload (read, write, mixed, read_disk_only) runs in parallel on its own cluster:

- `<workload> - best throughput under P99 limit` (Argus): the best throughput, its thread count, P99 read/write
  and the limit. Throughput 0 with step `none passed` means no thread count stayed within the limit.
- Per-step tables `<workload> - unthrottled_<threads>_threads - latencies`: what the search tried.
- The run log says why the search stopped (narrowed to the precision, throughput saturated, or the step budget
  ran out while bisecting; the budget never stops the search before it finds the limit).

## 4. Variations

| Want | Do |
|---|---|
| A separate history for an experiment on the same hardware | Set `job_name_suffix` |
| Another stress tool, distribution, compaction or compression | Add its file to `test_config` |
| A different limit | `p99_limit_ms` |
| A finer or longer search | `search_precision_pct`, `max_search_steps`, `step_duration` |
| Goodput under overload ([SCYLLADB-3267](https://scylladb.atlassian.net/browse/SCYLLADB-3267)) | `overload_factors`, e.g. `1.3,2`: after the search, offer that many times the best throughput; `<workload> - goodput under overload` in Argus, failing the run under 0.9 of the best |
| A one-off run that shouldn't create a job | Run `best-throughput/best-throughput-generic` directly |
| Repeat a run | Rebuild the generated job's last build |
