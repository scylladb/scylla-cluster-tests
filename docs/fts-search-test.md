# Running the FTS (BM25 full-text search) performance test

`fts_test.FtsSearchTest.test_fts_search` drives a full-text-search benchmark:
load documents → build a `fulltext_index` → record index build time and indexing
throughput in the run's results file, `search_results.jsonl`. Build time is read from
vector-store's own "full scan" log lines (see "Index build timing" below).

The query phase — running query sets against the index and reporting their latency — lands on
top of this.

The flow is not specific to full text. It lives in `search_perf_test.py` and is shared with the
other benchmarks of a vector-store-served index; `fts_test.py` is the full-text half — the rune
script to run, the vocabulary to report in, and the names to report under, all in one
`SearchWorkload`. Index build timing lives in
`sdcm/utils/vector_store_index.py`, index and readiness polling in `sdcm/utils/vector_store_client.py`.

So far there is one way to run it:

| | Backend | Purpose | Cost | Wall clock |
|---|---|---|---|---|
| [Local](#1-local-correctness-run-docker-backend) | `docker` | Verify the test *orchestration* is correct | none | ~5 min |

The local run produces meaningless numbers — under a thousand synthetic documents on a
containerised Scylla. Use it to check that shard staging, index building, metric
parsing and the results file all work. A run on real hardware against real corpora comes
separately.

> **Note:** this test has not been tried on minicloud yet (`docs/minicloud.md`). Its lightweight
> guests (1 vCPU, 4 GiB each) suit the local plan only, and minicloud's x86 KVM needs an x86
> vector-store AMI.

---

## 1. Local correctness run (docker backend)

### One-time setup

**Images.** The test case runs `scylladb/scylla:latest` and `scylladb/vector-store:latest`;
full-text indexes need ScyllaDB 2026.3+ and vector-store 1.11.0+. A cached `latest` is not
re-pulled, so `docker pull` it to refresh. For results you mean to compare, pin both builds instead,
e.g. `SCT_SCYLLA_VERSION=2026.3.3 SCT_VECTOR_STORE_VERSION=1.11.0`. For now vector-store has to be
pinned: its `latest` is still 1.5.1 (scylladb/vector-store#635).
The docker backend takes a prebuilt image only, so to test an unreleased vector-store, build it
from the vector-store repo and point `vector_store_docker_image` / `vector_store_version` in
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

# Generate the corpora if you have not already -- they are not tracked in git. The run reads
# them in place and leaves them alone, so this is a one-off.
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

# The results file: one JSON object per line, one line per index build.
jq -c '{dataset, step, record_count, build_time_s, indexing_throughput}' $D/search_results.jsonl
```

The shape to expect — one build record per step:

```
{"dataset":"local_tiny","step":1,"record_count":300,"build_time_s":<s>,"indexing_throughput":<docs/s>}
{"dataset":"local_tiny","step":2,"record_count":900,"build_time_s":<s>,"indexing_throughput":<docs/s>}
{"dataset":"local_smoke","step":1,"record_count":10,"build_time_s":<s>,"indexing_throughput":<docs/s>}
{"dataset":"local_smoke","step":2,"record_count":10,"build_time_s":<s>,"indexing_throughput":<docs/s>}
```

A build whose time could not be read from the log is still recorded, with `null`s.

local_smoke's second build (step 2) has no load -- it rebuilds the index on the corpus the
first step already loaded, so its build time reflects only the index rebuild, not the load. See
"Repeated builds on the same data" below.

The pieces that can be checked without a cluster already are, so a failure here is more likely to be
the orchestration than the plumbing underneath it:

```bash
# vector-store's 'latest' is too old for now (scylladb/vector-store#635)
export SCT_FTS_IT_VECTOR_STORE_IMAGE=scylladb/vector-store:1.11.0
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

## Notes on the test config format

The dataset/query plan is a separate YAML from the SCT test case:

`search_test_config` accepts two forms (`resolve_test_config_path()` in `search_perf_test.py`):

| Value | Resolved as |
|---|---|
| `/abs/local/path` | used as-is |
| `data_dir/latte/fts_search/plan.yaml` | relative to the SCT root |

The option is not full-text specific: the plan format belongs to the shared flow, so a vector-search
test case will name its own plan through the same option.

It has no default — which datasets and shards to run *is* the definition of the test, so a test
case has to name a plan. The plans live in the repo, next to the rune scripts they
drive:

| Plan | Used by | Size |
|---|---|---|
| `local_config.yaml` | the docker test case | two tiny generated corpora, read from disk |

The index and load waits are plan values, not SCT params. Per dataset:

| Key | Default | Bounds |
|---|---|---|
| `max_index_wait_secs` | 1800 | the rune script's own budget for probing the index until it answers, the index-build phase timeout, and how long SCT waits for a dropped index to disappear |
| `max_shard_load_secs` | 3600 | the load phase timeout, **per shard** — shards load one at a time |

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
whether it loaded anything), so repeats do not collide.

An *absent* `shards` key is a different thing: it falls back to the step's `documents_file`, i.e.
a single unsharded corpus. `local_smoke` in `local_config.yaml` uses one, and then a warm rebuild
on the same corpus.

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
