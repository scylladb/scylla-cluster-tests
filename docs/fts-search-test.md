# Running the FTS (BM25 full-text search) performance test

`fts_test.FtsSearchTest.test_fts_search` drives a full-text-search benchmark:
load documents → build a `fulltext_index` → run query sets → record index build time,
indexing throughput and query latency in the run's results file, `search_results.jsonl`. Index
build time is read from vector-store's own "full scan" log lines (see "Index build timing" below).

The flow is not specific to full text. It lives in `search_perf_test.py` and is shared with the
other benchmarks of a vector-store-served index; `fts_test.py` is the full-text half — the rune
script to run, the vocabulary to report in, and the names to report under, all in one
`SearchWorkload`. Index build timing lives in
`sdcm/utils/vector_store_index.py`, index and readiness polling in `sdcm/utils/vector_store_client.py`.

Two ways to run it:

| | Backend | Purpose | Cost | Wall clock |
|---|---|---|---|---|
| [Local](#1-local-correctness-run-docker-backend) | `docker` | Verify the test *orchestration* is correct | none | ~5 min |
| [Real](#2-real-performance-run-aws-backend) | `aws` | Actual performance numbers | AWS instances | hours |

The local run produces meaningless numbers — under a thousand synthetic documents on a
containerised Scylla. Use it to check that shard staging, index building, metric
parsing and the results file all work before spending money on AWS.

> **Note:** this test has not been tried on minicloud yet (`docs/minicloud.md`). Its lightweight
> guests (1 vCPU, 4 GiB each) suit the local plan only, and minicloud's x86 KVM needs an x86
> vector-store AMI.

---

## 1. Local correctness run (docker backend)

### One-time setup

**Images.** The test case runs `scylladb/scylla:latest` and `scylladb/vector-store:latest`;
full-text indexes need ScyllaDB 2026.3+ and vector-store 1.11.0+. A cached `latest` is not
re-pulled, so `docker pull` it to refresh. For results you mean to compare, pin both builds instead,
e.g. `SCT_SCYLLA_VERSION=2026.3.3 SCT_VECTOR_STORE_VERSION=1.11.0`.
The docker backend takes a prebuilt image only: it has no equivalent of the AWS source-build path
(`vector_store_source_repo` / `vector_store_source_ref`) and rejects those params outright. To test
an unreleased vector-store, build it from the vector-store repo and point
`vector_store_docker_image` / `vector_store_version` in
`test-cases/fts-search/fts-search-test-docker.yaml` at it:

```bash
cd <path-to>/vector-store
docker build -t local/vector-store:dev .
```

**Optional — silence a spurious ERROR event.** Scylla in an unprivileged container
logs `Perf-based stall detector creation failed (EACCESS) ... to enable kernel
backtraces`. SCT's BACKTRACE pattern `^(?!.*audit:).*backtrace` matches the word
"backtraces" and promotes it to an ERROR event, which makes `finalize_teardown()`
fail the run even when the test body passed. To get a fully green run:

```bash
sudo sysctl -w kernel.perf_event_paranoid=1     # host-wide; 2 is the Fedora default
```

Without this you get `1 passed, 1 error`, where the error is teardown-only.

### Every run

```bash
cd <path-to>/scylla-cluster-tests

unset DOCKER_HOST          # SCT's docker backend needs a real dockerd, not podman
export JOB_NAME=local_run  # see note below

# Generate the corpora if you have not already -- they are not tracked in git. A run with no
# 'base_url' reads them in place and leaves them alone, so this is a one-off.
python3 data_dir/latte/fts_search/generate_local_dataset.py

./docker/env/hydra.sh run-test fts_test.FtsSearchTest.test_fts_search \
  --backend docker \
  --config test-cases/fts-search/fts-search-test-docker.yaml
```

**Why `JOB_NAME=local_run`.** Hydra forwards `-e JOB_NAME="${JOB_NAME}"`. With the
variable unset on the host that arrives inside the container as an *empty string*
rather than unset, which defeats the `local_run` default in `get_job_name()`
(`sdcm/utils/ci_tools.py`). SCT then treats the run as CI and connects to the real
Argus, creating a junk run there. Setting it explicitly keeps Argus in replay-only
mode: every submission is written to `argus_replay_log_*.jsonl` in the run's log
directory and nothing is posted. The test's own results do not depend on it -- they go to
`search_results.jsonl` either way.

### Use hydra, not a bare `sct.py`, on Fedora

Running SCT outside the hydra container **fails on a Fedora host**:

```bash
# Does NOT work on Fedora 43.
export SCT_CLUSTER_BACKEND=docker
export SCT_CONFIG_FILES=test-cases/fts-search/fts-search-test-docker.yaml
uv run sct.py run-test fts_test.FtsSearchTest.test_fts_search
```

`DockerLoaderNode` runs on the host via `LOCALRUNNER` (`sdcm/cluster_docker.py`), so
`SetUp()` installs packages onto the host. The Fedora entry in `sdcm/utils/distro.py`
recognises only `34`/`35`/`36`, so on Fedora 43 the distro resolves to `UNKNOWN`,
`is_rhel_like` is `False`, and `install_package` falls through to the apt branch:

```
Distro: missed key for ('fedora', '43')
Unable to detect Linux distribution name
sudo apt-get ... install -y tar   ->   sudo: apt-get: command not found
```

Inside hydra the loader's "host" is the hydra container, which SCT recognises as
Debian-like, so `apt-get` is correct there. Adding `43` to that Fedora entry would make the
non-hydra path work.

Also do not substitute a bare `pytest fts_test.py::...` on Python 3.14: SCT's
`EventsDevice` is not picklable and 3.14 defaults to the `forkserver` start method,
so the event system dies with `TypeError: cannot pickle 'weakref.ReferenceType'`.
`ensure_start_method()` (which forces `fork`) is called from `sct.py` and
`unit_tests/conftest.py`, but not from the repo-root `conftest.py`.

### What to check afterwards

The numbers say nothing here, so "did it work?" has to be answered from the results file. Logs
land in `~/sct-results/<timestamp>/`:

```bash
D=$(ls -dt ~/sct-results/*/ | head -1)

# Index build times come from vector-store's own 'full scan' log lines, not from anything the
# stress tool prints -- see "Index build timing" below.
grep -E "Index build time \(vector-store full scan\)" $D/sct.log

# Cross-check against the source those numbers are read from. Each reported build should match a
# 'starting'/'finished' pair for the same index (note the lower-cased index name).
grep -E "(starting|finished) full scan on" $D/*vs-set*/*/system.log

# The results file: one JSON object per line -- an 'index_build' record per step and a 'search'
# record per query entry.
jq -c 'select(.phase == "index_build") | {dataset, step, record_count, build_time_s}' \
  $D/search_results.jsonl
jq -c 'select(.phase == "search") | {dataset, step, query_index, query_set, p99_ms}' \
  $D/search_results.jsonl
```

The shape to expect — one build record per step and one search record per query entry, in plan
order:

```
{"dataset":"local_tiny","step":1,"record_count":300,"build_time_s":<s>}
{"dataset":"local_tiny","step":2,"record_count":900,"build_time_s":<s>}
{"dataset":"local_smoke","step":1,"record_count":10,"build_time_s":<s>}
{"dataset":"local_smoke","step":2,"record_count":10,"build_time_s":<s>}
{"dataset":"local_smoke","step":3,"record_count":10,"build_time_s":<s>}

{"dataset":"local_tiny","step":1,"query_index":1,"query_set":"term_common","p99_ms":<ms>}
{"dataset":"local_tiny","step":2,"query_index":1,"query_set":"term_common","p99_ms":<ms>}
{"dataset":"local_tiny","step":2,"query_index":2,"query_set":"term_common","p99_ms":<ms>}
{"dataset":"local_tiny","step":2,"query_index":3,"query_set":"natural","p99_ms":<ms>}
{"dataset":"local_smoke","step":1,"query_index":1,"query_set":"natural","p99_ms":<ms>}
{"dataset":"local_smoke","step":3,"query_index":1,"query_set":"natural","p99_ms":<ms>}
```

A build whose time could not be read from the log is still recorded, with `null`s, and so is a
query entry whose latency was not measured.

local_smoke's second and third builds have no load — they rebuild the index on the corpus the
first step already loaded, so their build time reflects only the index rebuild, not the load (see
"Repeated builds on the same data" below). Step 2 also has no queries at all, and step 3 repeats
step 1's query set.

The pieces that can be checked without a cluster already are, so a failure here is more likely to be
the orchestration than the plumbing underneath it:

```bash
# staging, the load and the whole dataset cycle, against ScyllaDB and vector-store
pytest -m integration unit_tests/integration/test_search_perf_test.py
# index-status polling and the build-time log parsing, against a real vector-store
pytest -m integration unit_tests/integration/test_vector_store.py
```

### Expected noise (all harmless)

- `Dashboard with title 'Overview' was not found`, then a connection failure to
  alertmanager on `127.0.0.1:9093` — log collection looking for Grafana dashboards
  the docker monitor does not have. Costs ~3 minutes at the end of the run.
- `nodetool_*_failure_*.log`, `StorageConfigurationCollector: FAIL`,
  `TCPConnectionsCollector: FAIL` — scylla-doctor probes that do not apply in a
  container.

### Cleanup

The test case sets `execute_post_behavior: true` with `post_behavior_*: keep-on-failure`, so a
passing run removes its containers and a failing one leaves them up for inspection. Without that,
`clean_resources()` logs "Resources will continue to run" and every run leaks its db and
vector-store containers — the default is `false` because in Jenkins a separate stage does the
destroying, and a local run has no such stage.

Containers are labelled with the run's TestId, so a failed or interrupted run cleans up with:

```bash
docker ps -a --filter label=TestId=<test-id> -q | xargs -r docker rm -f
```

To sweep every SCT container regardless of run (careful — this takes the monitoring stack too):

```bash
docker ps -a --filter label=TestId -q | xargs -r docker rm -f
```

---

## 2. Real performance run (AWS backend)

Uses `test-cases/fts-search/fts-search-test.yaml`: 1× `i4i.xlarge` DB, 1× `c5.large` loader,
1× `r8g.xlarge` vector-store from the newest AMI, datasets pulled from
`s3://full-text-search-sct/`, `test_duration: 240`. It defaults to the sanity-sized
`sanity_config.yaml` plan; see [test config format](#notes-on-the-test-config-format) for the
full run.

`test_duration` covers both plans: a full `large_config.yaml` run, 100 shards per dataset, takes
about an hour. Raise it with `SCT_TEST_DURATION` for a bigger plan; the Jenkins job's timeouts
follow it. A single latte phase is bounded by the plan instead: a search by its `duration`, a load and an index
build by `max_shard_load_secs` and `max_index_wait_secs` (defaults in `search_perf_test.py`).

vector-store is the newest AMI, which serves full-text indexes since 1.11.0; pin a
release with `vector_store_version`. To test unreleased code, set `vector_store_source_repo`
and/or `vector_store_source_ref` instead: either one switches to a source build over that AMI, the
other defaulting to `scylladb/vector-store` and `master`. The installer SCT ships
(`data_dir/install_vector_store_from_source.sh`) shallow-fetches the ref over https without
credentials, so any branch, tag or full SHA works, but a fork has to be public.

Like every aws test case, the test case does not pin `scylla_version`: set `SCT_SCYLLA_VERSION`
(or the job's `scylla_version` parameter) to a build with full-text index support, e.g.
`master:latest`.

### Running it by hand

The run takes hours, so drive it from an SCT runner rather than a laptop:

```bash
export SCT_REGION_NAME=us-east-1        # as the Jenkins job; the default is eu-west-1
export SCT_SCYLLA_VERSION=master:latest

./docker/env/hydra.sh --execute-on-new-runner \
  run-test fts_test.FtsSearchTest.test_fts_search \
  --backend aws \
  --config test-cases/fts-search/fts-search-test.yaml
```

The runner's IP lands in `./sct_runner_ip`; `--execute-on-runner $(cat sct_runner_ip)` reuses it.

From a laptop, add `--config configurations/network_config/test_communication_public.yaml` so
SCT connects to the nodes' public addresses, and stay in a region whose `SCT-2-sg` security group
lets your IP in on port 22 -- `eu-west-1` (open to all), not `us-east-1`. Otherwise the run
provisions its cluster and then fails in `Waiting for SSH to be up`.

This test's own settings, as environment variables:

```bash
export SCT_VECTOR_STORE_SOURCE_REF=<branch|tag|full-sha>                     # a source build
export SCT_VECTOR_STORE_SOURCE_REPO=https://github.com/<you>/vector-store.git  # of a fork
export SCT_SEARCH_TEST_CONFIG=data_dir/latte/fts_search/large_config.yaml     # another plan
export SCT_SEARCH_TEST_CONFIG=s3://my-bucket/my_plan.yaml                    # ... or one on S3
```

### Via Jenkins / Argus

The job is `jenkins-pipelines/performance_staging/fts-search-test.jenkinsfile`, on aws in
`us-east-1`; launching it from Argus runs the same job. Its pipeline,
`perfRegressionParallelPipeline`, has only the generic parameters, so this test's settings go in
the `extra_environment_variables` field, one `KEY=value` per line:

```properties
SCT_VECTOR_STORE_SOURCE_REF=my-branch
SCT_SEARCH_TEST_CONFIG=data_dir/latte/fts_search/large_config.yaml
```

### Results in Argus

Every record of `search_results.jsonl` is also sent to Argus:

- **`FTS index builds`** and **`FTS searches`** -- the run's log: every index build in
  the first, every query configuration in the second, numbered in run order across both
  (`#01 index build`, `#02 term_common`, ...), with the dataset, step and configuration in their
  columns. All of them are text, so Argus shows them but graphs nothing: they are the run's
  overview, and an exploratory plan entry costs no graph.
- **`FTS <track>`** -- only the entries the plan puts in that track, one row per entry. These
  are graphed: one graph per measurement column, one series per row across runs.

`FTS` is the workload's `table_prefix`: it keeps the tables apart should several workloads report to
one Argus test, such as sub-tests of one Jenkins job.

What is tracked is decided in the plan. A query entry names a track with `track:`, and a step puts
its index build in one with `build_track:`. Each also needs a row name, `label:` / `build_label:`,
which names its series across runs:

```yaml
steps:
  - shards: [0..99]
    build_track: indexing                # -> table "FTS indexing"
    build_label: 10M_20tok               # its row
    queries:
      - set: term_common
        stress_options: {concurrency: 1}
        rate: 50
        track: term latency              # -> table "FTS term latency"
        label: 10M_20tok term_common     # its row
      - set: boolean_or                  # untracked: in the searches log only
```

Group entries whose values are on one scale, so a graph stays readable, and name rows by what they
measure rather than by where they sit in the plan: a series continues across runs only while its
track and label stay the same. Search tracks graph `P50`, `P90`, `P99`, `P99.9` and, unless
rate-limited (whose throughput is just the rate asked for), `throughput`. Index-build tracks graph
`build time` and `indexing throughput`. The query configuration, the document count and an example
query are text columns.

The plan is checked when it is read (`validate_plan_tracks()`): a label appears once per track, a
track holds index builds or searches but not both, its searches are all rate-limited or all
unthrottled, and a rate-limited track's searches share one `expected_p99_read_ms` (the table's
`P99` limit).

Only a rate-limited track checks P99. An unthrottled query runs at saturation, where P99 follows
concurrency over throughput, so a throughput track has no rule yet. Argus can check throughput
against the best earlier result of each row, which is worth adding once the rows have a history. A
rate-limited track's P99 over its limit fails the run: Argus marks the cell as an error and SCT
reports the test as failed, but the run carries on to the end and records everything else. Any
other entry's limit is only recorded, as `p99_within_expectation` in the results file.

The x-axis is not the usual ScyllaDB build date alone: it is the later of the ScyllaDB and the
vector-store build dates, so a new build of either gives a new point, and runs of the same two
builds share one. The vector-store date is its AMI's creation date, or for a source build the date
of the commit it built. `sct.log` shows what a run used (`Argus x-axis:`).

---

## Notes on the test config format

The dataset/query plan is a separate YAML from the SCT test case:

`search_test_config` accepts three forms (`resolve_test_config_path()` in `search_perf_test.py`):

| Value | Resolved as |
|---|---|
| `s3://bucket/key` | downloaded into `data_dir/latte/fts_search/downloaded/<bucket>/<key>` (gitignored) |
| `/abs/local/path` | used as-is |
| `data_dir/latte/fts_search/plan.yaml` | relative to the SCT root |

The option is not full-text specific: the plan format belongs to the shared flow, so a vector-search
test case will name its own plan through the same option.

The plans live in the repo, next to the rune scripts they drive. Only the corpora come from S3: a
dataset with a `base_url` downloads into a fresh folder under `downloaded/datasets/`, removed when
the dataset is done. Its shards are fetched one at a time, right before the latte invocation that
loads them, and deleted once loaded, as the full corpora do not fit next to each other on a
runner's disk. A dataset without `base_url` is read in place and never deleted.

A dataset's files -- the documents, `queries_<set>.tsv` and `qrels_<set>.tsv` -- follow the "Dataset
Format" of `latte/full-text-search/README.md` in scylladb/vector-store, where `fts.rn` comes from.
The 10M corpora were generated for VECTOR-765: an MS MARCO sample scaled to 10M documents, with
about 510k `natural` queries and synthetic sets of 30 queries each (`term_*`, `phrase`,
`boolean_*`), and no qrels. Each dataset's `manifest.json` on S3 records how it was built.

| Plan | Used by | Size |
|---|---|---|
| `sanity_config.yaml` | the aws test case, by default | 5 of the 100 shards of `10M_20tok` (~500k docs), 2 loads with 2 rebuilds after each |
| `large_config.yaml` | `SCT_SEARCH_TEST_CONFIG=data_dir/latte/fts_search/large_config.yaml` | both 10M-document datasets in full, 3 steps each |
| `local_config.yaml` | the docker test case | no `base_url` at all, everything read from disk |

The default is deliberately the small one: an aws run should be able to answer "does this build
work end to end?" without committing to the full sweep.

To run a plan that is not in the repo, use the S3 form — it is a single string, so unlike a
nested test-case mapping it can be overridden from the environment:

```bash
SCT_SEARCH_TEST_CONFIG=s3://my-bucket/my_plan.yaml ./sct.py run-test ...
```

The load and index waits and the query settings are plan values, not SCT params. Per dataset:

| Key | Default | What it controls |
|---|---|---|
| `max_index_wait_secs` | 1800 | the rune script's own budget for probing the index until it answers, the index-build phase timeout, and how long SCT waits for a dropped index to disappear |
| `max_shard_load_secs` | 3600 | the load phase timeout, **per shard** — shards load one at a time |
| `has_invalid_queries` | `false` | set it when the dataset's query sets include queries vector-store cannot parse, e.g. a third-party corpus in a performance run. ScyllaDB's error for such a query then no longer fails the test; latte still logs each one as a warning. Leave it off for regression runs, whose query sets have to be valid |

### Every query needs an expected latency

Every query entry must resolve an `expected_p99_read_ms`, either on the entry itself or the
dataset's `defaults` — there is no SCT-side default or hardcoded threshold, and missing it on both
raises `ValueError` as soon as the plan is read:

```yaml
defaults:
  expected_p99_read_ms: 10   # inherited by every query in the dataset unless overridden

steps:
  - queries:
      - set: term_common          # uses the default: 10ms
      - set: natural
        expected_p99_read_ms: 50  # overridden: 50ms
```

Each search record carries it as `expected_p99_read_ms`, next to the measured `p99_ms` and
`p99_within_expectation`, together with the query's `limit`, `rate`, `stress_options` and a
`query_example` (the first line of the query set's `.tsv`).

The whole plan is validated right after it is read, so a typo fails before a step spends tens of
minutes loading shards. A query's `duration` has to be latte's *time* form (`60s`, `5m`, `1h`):
SCT derives the phase timeout from it and cannot read the request-count form.

### Stress options

A query entry's `stress_options` are passed to latte as `--<name> <value>`, so using another latte
option, or one a newer latte adds, is a plan change. They merge by name over the dataset's
`defaults`, so an entry setting one option keeps the others; `true` is a bare flag and `false` drops
a default's:

```yaml
defaults:
  stress_options: {concurrency: 32, warmup: 200}

steps:
  - queries:
      - set: term_common
        stress_options: {concurrency: 1}   # --concurrency 1 --warmup 200
      - set: natural
        stress_options: {warmup: false}    # --concurrency 32
```

SCT checks only that they are shell-safe and not `rate`, a plan key SCT reads itself. latte itself
rejects an option it does not know, and one given twice, as one SCT also sets, such as `duration`,
would be. They are reported as given: a `stress options` column in Argus and `stress_options` in the
search record. Without `concurrency` latte runs 128 queries in flight, so a plan sets it. The plans
warm up 200 queries on the sets of about 30 queries a run repeats, and not on `natural`, whose
queries are mostly new and meant to run cold.

### Repeated query configs

A step may list the same query configuration more than once, e.g. to sample run-to-run variance.
Every entry gets its own search record, told apart by `query_index`, its 1-based position in the
step. `local_tiny`'s second step in `local_config.yaml` repeats one verbatim.

### Repeated builds on the same data

A step with an **empty** `shards` list loads nothing — `_load_step_shards` returns 0 — so it only
drops the previous index and rebuilds one on the corpus already in the table. Useful for sampling
index-build-time variance in isolation from load time:

```yaml
steps:
  - shards: [0]          # load ~100k documents (cold build, includes any one-off warmup cost)
  - shards: []           # rebuild on the same 100k documents, warm
  - shards: []
  - shards: []
```

Each build still gets its own record (keyed by dataset and step, one per step regardless of
whether it loaded anything), so repeats do not collide -- see `sanity_config.yaml`, which follows
each of its two loads with two warm rebuilds.

An *absent* `shards` key is a different thing: it falls back to the step's `documents_file`, i.e.
a single unsharded corpus. `local_smoke` in `local_config.yaml` uses one, and then a warm rebuild
on the same corpus.

A step's `queries` are optional too, so a step can be build-only.

### Index build timing

Index build time and indexing throughput are measured by SCT from **vector-store's own log**
(`sdcm.utils.vector_store_index.parse_full_scan_seconds`), not from anything the `build_index`
stress command prints. vector-store brackets an index's initial table scan with two INFO lines
carrying microsecond timestamps, and that scan *is* the build:

```
2026-07-30T23:05:37.908018Z  INFO ... db_index{fts_bench.fts_idx_10m_20tok_0}: starting full scan on fts_bench.fts_idx_10m_20tok_0
2026-07-30T23:06:43.914698Z  INFO ... db_index{fts_bench.fts_idx_10m_20tok_0}: finished full scan on fts_bench.fts_idx_10m_20tok_0
```

The stress tool still owns the DDL and still decides when the index is usable (its `build_index`
probes BM25 until it answers); SCT only reads the log afterwards, via `BaseNode.system_log` — which
resolves to `hosts/<host>/messages.log` under `logs_transport: vector` and to
`<node.logdir>/system.log` otherwise.

**Case folding.** Scylla folds unquoted identifiers, so `CREATE CUSTOM INDEX fts_idx_10M_20tok_0` is
`fts_idx_10m_20tok_0` everywhere downstream — that is the name in `system_schema.indexes`, the key
vector-store uses, and the key in the log lines above. Anything SCT sends to or matches against the
vector-store API is folded the same way (`sdcm.utils.vector_store_index.index_key`); querying with
the unfolded name 404s forever.
