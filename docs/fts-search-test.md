# Running the FTS (BM25 full-text search) performance test

`fts_test.FtsSearchTest.test_fts_search` drives a full-text-search benchmark:
load documents → build a `fulltext_index` → run query sets → report latency and
index-build metrics to Argus. Index build time is read from vector-store's own "full scan"
log lines (see "Index build timing" below).

The flow is not specific to full text. It lives in `search_perf_test.py` and is shared with the
other benchmarks of a vector-store-served index; `fts_test.py` is the full-text half — the rune
script to run, the vocabulary to report in, and the names to report under, all in one
`SearchWorkload`. Index build timing and the Argus build table live in
`sdcm/utils/vector_store_index.py`, index and readiness polling in `sdcm/utils/vector_store_client.py`.

So far there is one way to run it:

| | Backend | Purpose | Cost | Wall clock |
|---|---|---|---|---|
| [Local](#1-local-correctness-run-docker-backend) | `docker` | Verify the test *orchestration* is correct | none | ~5 min |

The local run produces meaningless numbers — 1 shard of 300 synthetic documents on a
containerised Scylla. Use it to check that shard staging, index building, metric
parsing and the Argus tables all work. A run on real hardware against real corpora comes
separately.

> **Note:** minicloud cannot run this test. It emulates only `i4i.large` and
> `n2-highmem-2`, the vector-store AMI is arm64 (minicloud is KVM on x86), and the
> integration is still an unmerged draft. See `docs/plans/minicloud-local-testing.md`.

---

## 1. Local correctness run (docker backend)

### One-time setup

**Build a vector-store image.** The docker backend takes a prebuilt image only — it
has no way to build vector-store itself. Build the commit you care
about from the vector-store repo (it needs `fulltext_index` support, i.e.
`crates/vector-store/src/fts_index/`):

```bash
cd <path-to>/vector-store
docker build -t local/vector-store:fts .
```

If you build a commit other than `local/vector-store:fts`, update
`vector_store_docker_image` / `vector_store_version` in
`test-cases/fts-search/fts-search-test-docker.yaml`.

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
directory and nothing is posted.

Drop that line if you *want* to see the tables render in Argus for real — which is
the strongest check of any change to the `expected_p99_read_ms` table split.

**If the tables do not show up in Argus,** check both gates before suspecting the test —
either one silently downgrades the whole run to replay-only, and neither fails loudly:

```python
# sdcm/test_config.py, TestConfig.init_argus_client()
if params.get("enable_argus") and get_job_name() != "local_run":
```

So `unset JOB_NAME SCT_ENABLE_ARGUS` for a run whose results you want posted. Both are
*environment* state, so they outlive the run that needed them — a `JOB_NAME=local_run`
exported for one run is still set for the next one in the same shell, and that run will
silently post nothing either. To confirm which gate you tripped:

```bash
grep -m1 -oE "'(enable_argus|job_name)': [^,]*" ~/sct-results/latest/argus.log
grep -c "replay-log-only" ~/sct-results/latest/argus.log   # >0 means nothing was posted
```

The results themselves are still in `argus_replay_log_*.jsonl` (one record per
`submit_results` call), but there is no replay CLI to push them after the fact — a run whose
numbers you actually need has to be repeated.

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

The numbers say nothing here, so "did it work?" has to be answered from the result tables. Logs
land in `~/sct-results/<timestamp>/`:

```bash
D=$(ls -dt ~/sct-results/*/ | head -1)

# Index build times come from vector-store's own 'full scan' log lines, not from anything the
# stress tool prints -- see "Index build timing" below.
grep -E "Index build time \(vector-store full scan\)" $D/sct.log

# Cross-check against the source those numbers are read from. Each reported build should match a
# 'starting'/'finished' pair for the same index (note the lower-cased index name).
grep -E "(starting|finished) full scan on" $D/*vs-set*/*/system.log

# The expected_p99_read_ms split: local_config.yaml exercises fts_search_p99_10ms (term_common)
# and fts_search_p99_50ms (natural).
python3 -c "import json; print(list(json.load(open('$D/latency_results.json'))))"

# Argus submissions (replay-only mode). Expect at least these tables:
#   FTS Index Build Time
#   read - fts_search_p99_10ms - latencies
#   read - fts_search_p99_50ms - latencies
grep -o "read - fts_search_p99_[a-z0-9_]* - latencies\|FTS Index Build Time" \
  $D/argus_replay_log_*.jsonl | sort -u
```

The shape to expect — one build row per step, one latency table per distinct
`expected_p99_read_ms`, a `step #N` in every query row, and a `run #N` only where a step repeats
a query configuration verbatim:

```
Index build time (vector-store full scan): <s> (local_tiny | 300 docs | build #1)
Index build time (vector-store full scan): <s> (local_tiny | 900 docs | build #2)
Index build time (vector-store full scan): <s> (local_smoke | 10 docs | build #1)
Index build time (vector-store full scan): <s> (local_smoke | 10 docs | build #2)
Index build time (vector-store full scan): <s> (local_smoke | 10 docs | build #3)

FTS Index Build Time                  rows: 5 (one per step, 'build #N'-suffixed)
read - fts_search_p99_10ms - lat.     rows: local_tiny | 300 docs | step #1 | term_common,
                                             local_tiny | 900 docs | step #2 | term_common |
                                               limit=5 concurrency=2 rate=0 run #1,
                                             ... the same with run #2
read - fts_search_p99_50ms - lat.     rows: local_tiny | 900 docs | step #2 | natural,
                                             local_smoke | 10 docs | step #1 | natural,
                                             local_smoke | 10 docs | step #3 | natural
```

local_smoke's second and third builds have no load — they rebuild the index on the corpus the
first step already loaded, so their build time reflects only the index rebuild, not the load (see
"Repeated builds on the same data" below). `build #2` also has no queries at all, and `build #3`
repeats `build #1`'s query set — which is why its query rows differ from step #1's only in the
`step #N`.

The pieces that can be checked without a cluster already are, so a failure here is more likely to be
the orchestration than the plumbing underneath it:

```bash
# corpus staging and the load, against a real ScyllaDB
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

It has no default — which datasets, shards and query sets to run *is* the definition of the test,
so a test case has to name a plan. The plans live in the repo, next to the rune scripts they
drive; only the corpora come from S3, and only for a dataset that carries a `base_url`:

| Plan | Used by | Size |
|---|---|---|
| `local_config.yaml` | the docker test case | no `base_url` at all, everything read from disk |

A dataset's shard files are fetched one at a time, right before the latte invocation that loads
them, and deleted again once loaded — the full corpora do not fit next to each other on a runner's
disk. Only what the run downloaded is deleted: a `base_url`-less dataset is the input rather than a
cache, so a local plan keeps working across runs.

To run a plan that is not in the repo, use the S3 form — it is a single string, so unlike a
nested test-case mapping it can be overridden from the environment:

```bash
SCT_FTS_TEST_CONFIG=s3://my-bucket/my_plan.yaml ./sct.py run-test ...
```

Rate, duration and index-wait are per-query-set values inside the plan YAML — there are no SCT
params for them. Per dataset:

| Key | Default | Bounds |
|---|---|---|
| `max_index_wait_secs` | 1800 | the rune script's own budget for probing the index until it answers, the index-build phase timeout, and how long SCT waits for a dropped index to disappear |
| `max_shard_load_secs` | 3600 | the load phase timeout, **per shard** — shards load one at a time |

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

The value selects the Argus table — `read - fts_search_p99_{value}ms - latencies`
(`_cycle_name()` in `search_perf_test.py`) — and becomes that table's `P99 read` validation rule.
It is a property of the table, not a column, so queries resolving to the same value share a table.
What varies per row is reported as columns instead: `limit`, `concurrency`, `rate` and a
`query_example` (the first line of the query set's `.tsv`).

The whole plan is resolved up front, right after it is read (`validate_plan_queries()` in
`search_perf_test.py`), so a typo fails before a step spends tens of minutes loading shards. Note
that a query's `duration` has to be latte's *time* form (`60s`, `5m`, `1h`), not its request-count
form — `get_timeout_from_stress_cmd()` only parses the former, and a duration it cannot read
silently gives the search phase the whole `test_duration` as its timeout.

### Repeated query configs get a disambiguating suffix

Argus row labels are built from dataset / document count / step ordinal / query set
(`row_labels_for_step()` in `search_perf_test.py`); the query parameters are columns, not part of
the label. Two entries for the same set within one step therefore collide, and a colliding label
gets a suffix naming its query configuration — plus a positional `run #N` when the entries agree
on every parameter too. A label that does not collide is left byte-identical, so Argus history
stays continuous. So:

```yaml
- set: term_common
  concurrency: 1
  rate: 50
- set: term_common
  limit: 100
- set: term_common     # identical to the next one
- set: term_common
```

produces

```
ds | N docs | step #1 | term_common | limit=5 concurrency=1 rate=50
ds | N docs | step #1 | term_common | limit=100 concurrency=32 rate=0
ds | N docs | step #1 | term_common | limit=5 concurrency=32 rate=0 run #1
ds | N docs | step #1 | term_common | limit=5 concurrency=32 rate=0 run #2
```

`local_config.yaml` exercises it deliberately — `local_tiny`'s second step repeats
`set: term_common` with identical parameters. Covered by `unit_tests/unit/test_search_perf_test.py`.

### Repeated builds on the same data

A step with an **empty** `shards` list loads nothing — `_load_step_shards` returns 0 — so it only
drops the previous index and rebuilds one on the corpus already in the table. Useful for sampling
index-build-time variance in isolation from load time:

(An *absent* `shards` key is a different thing: it falls back to the step's `documents_file`, i.e.
a single unsharded corpus. See `local_smoke` in `local_config.yaml`.)

```yaml
steps:
  - shards: [0]          # load ~100k documents (cold build, includes any one-off warmup cost)
  - shards: []           # rebuild on the same 100k documents, warm
  - shards: []
  - shards: []
```

Each build still gets its own Argus row (`{dataset} | {doc_count} docs | build #{N}`, one per
step regardless of whether it loaded anything), so repeats do not collide -- see `local_smoke`
in `local_config.yaml`, which does a build followed by a warm rebuild on the same corpus.

Queries are optional too (`step.get("queries", [])` defaults to none), so a step can be build-only.

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
vector-store API goes through `sdcm.utils.vector_store_index.index_key`; querying with the unfolded
name 404s forever.
