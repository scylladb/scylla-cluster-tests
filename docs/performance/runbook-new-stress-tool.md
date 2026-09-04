# Runbook: Add or Switch a Stress Tool

Wiring a new load generator into SCT, or moving an existing perf test onto a different one.

The worked example is the latte integration —
[../plans/latte-concurrency-gradual-throughput.md](../plans/latte-concurrency-gradual-throughput.md)
and [PR #13574](https://github.com/scylladb/scylla-cluster-tests/pull/13574/).

---

## Tool selection

**latte is the default for new performance tests.** It is Rust, async, has no GC pauses, gives
precise latency measurement and rate control, and defines workloads in Rune scripts.

Use something else only for a specific reason:

| Tool | Use it when |
|---|---|
| cassandra-stress | You have existing c-s YAML profiles that would be costly to rewrite, or need its statistical distributions for column data |
| scylla-bench | You are specifically exercising the Go driver path, or need >500k OPS from minimal hardware |
| YCSB | Results must be comparable to published industry-standard YCSB numbers |
| cql-stress | Drop-in c-s-compatible CLI without the JVM |

Constraints worth knowing before you commit:

- **cassandra-stress** is thread-per-client on the JVM. A single process stops scaling past
  ~16-32 threads; saturating a large loader needs multiple processes, not more threads. See
  [runbook-recalibrate-steps.md](runbook-recalibrate-steps.md#b-split-the-load-across-processes-not-threads).
- **scylla-bench** does not support the full complexity of c-s YAML profiles (complex
  statistical distributions, custom query logic), and its output format differs — existing
  parsers need work.
- **latte** uses Rune scripts, which are entirely incompatible with the c-s YAML profile
  library. Smaller ecosystem, fewer third-party scripts.

---

## Part A: Adding a brand-new tool

### 1. Stress thread class

Subclass `DockerBasedStressThread` (`sdcm/stress/base.py:33`) in `sdcm/<tool>_thread.py` or
`sdcm/stress/<tool>_thread.py`. Model it on `sdcm/stress/latte_thread.py`
(`LatteStressThread`), which is the most recent example.

Alongside it, an events publisher — a `FileFollowerThread` subclass that tails the tool's
output and raises SCT events for errors and progress. Every existing tool has one
(`CassandraStressEventsPublisher`, `ScyllaBenchStressEventsPublisher`, ...).

### 2. Register in the dispatcher

`ClusterTester.run_stress_thread` (`sdcm/tester.py:3017`) selects by inspecting the command
string. Add a `run_<tool>_thread` method and a branch.

> **Ordering trap.** The dispatcher is a chain of `in` / `startswith` checks, first match wins.
> `cql-stress-cassandra-stress` must be tested **before** `cassandra-stress`, because the
> former string contains the latter. Put your check where it cannot be shadowed by, or shadow,
> an existing one. An unmatched command raises
> `ValueError("Unsupported stress command: ...")`.

### 3. Pin the Docker image

Add the image to `configurations/stress_images/`:

```yaml
stress_image:
  <tool>: 'scylladb/hydra-loaders:<tool>-<version>'
```

For a new architecture, confirm the image is multi-arch or publish an arch-specific tag.

### 4. HDR histogram tags — the part that breaks

Perf tests get latency and throughput from `.hdr` files, not from the tool's stdout. Two things
must work.

**Your tool must write HDR logs** to the loaders logdir, matching the glob `*/hdrh-*.hdr`.

**Its tags must be classifiable.** `_get_workload_type_by_hdr_tag`
(`sdcm/utils/hdrhistogram.py:411`) resolves a tag to READ or WRITE with a substring match, in
this order:

```python
hdr_tag = hdr_tag.lower().strip()
if any(w in hdr_tag for w in ("write", "insert", "update", "delete")):
    return "WRITE"
elif any(r in hdr_tag for r in ("read", "select", "get", "count", "scan")):
    return "READ"
elif self.stress_operation in ("WRITE", "READ"):
    return self.stress_operation          # scylla-bench 'co-fixed' / 'raw' fallback
raise ValueError(f"Failed to detect the workload type for the following hdr_tag: {hdr_tag}")
```

How existing tools land here:

| Tool | Tags | Resolution |
|---|---|---|
| cassandra-stress | `WRITE-st`, `READ-st`, `WRITE-rt`, `READ-rt` (`-rt` = coordinated-omission-fixed, when `-rate 'fixed=N/s'`) | substring |
| latte | arbitrary — Rune function names: `fn--write`, `fn--write-batch`, `fn--get`, `fn--get-many`, `fn--read` | substring |
| scylla-bench | `co-fixed`, `raw` (identical for read and write); `co-fixed-write` / `co-fixed-read` for mixed | falls through to `stress_operation` |
| YCSB | `SCAN`, `READ`, `UPDATE`, `INSERT`, `DELETE`, `WRITE` — one file per tag | substring |

**Name your tags so the substring match works.** If you cannot, you must set
`stress_operation` so the fallback catches it — otherwise every run raises `ValueError` during
result collection, which becomes a `TestFrameworkEvent(ERROR)` and fails the test.

Note tag matching in `_build_histogram_from_file` is **case-insensitive**, because
user-profile cassandra-stress writes lowercase tags.

### 5. Throttle syntax

`current_throttle()` (`performance_regression_gradual_grow_throughput.py:505`) formats the rate
per tool. Add a branch:

```python
if is_latte_command(stress_cmd):
    current_throttle = f"--rate={throttle_value}"
elif stress_cmd.startswith("scylla-bench"):
    current_throttle = f"-max-rate={throttle_value}"
else:  # cassandra-stress and cql-cassandra-stress
    current_throttle = f"fixed={throttle_value}/s"
```

`throttle_value = int(rate) // (num_loaders * stress_num)` — the config rate is a cluster
total. `rate == "unthrottled"` must produce an empty string.

### 6. Step configuration format

Two formats are supported.

**String** (cassandra-stress, scylla-bench) — the step *is* the rate, threads come from
`perf_gradual_threads`:

```yaml
perf_gradual_threads: {"read": 620, "write": 400}
perf_gradual_throttle_steps: {"read": ['500000', '900000', 'unthrottled'], ...}
```

**Dict** (latte, and any tool needing more than one concurrency parameter):

```yaml
perf_gradual_throttle_steps:
  read:
    - {threads: 32, concurrency: 55, rate: '300000'}   # 1760 parallelism
    - {threads: 32, concurrency: 55, rate: '600000'}
    - {threads: 32, concurrency: 55}                   # rate omitted = unthrottled
```

Both may coexist in one config. Anything in the dict beyond `rate` is passed through as
`step_params` and substituted into the command template, so a new tool's extra knobs need no
framework change — just placeholders in the command template.

### 7. Argus results

If your tool produces the standard latency/throughput shape, nothing to do — the decorator
routes by `workload`, not by tool. If it produces something else (like the microbenchmarks or
`LatteStressLatencyComparison`), add a `StaticGenericResultTable` subclass in
`sdcm/argus_results.py` with `ColumnMetadata` including `higher_is_better`, and submit through
`submit_results_to_argus` so validation failures become `FailedResultEvent`.

### 8. Unit tests

`unit_tests/` — command construction, HDR tag resolution, throttle formatting, and step-config
parsing. These are cheap and catch the tag and ordering traps above.

---

## Part B: Moving an existing test to a different tool

You are not editing the test driver. You are producing a parallel config + job set.

1. **New step config.** `configurations/performance/<tool>_gradual_load_steps_<hw>.yaml`.
   Do not reuse the old tool's rates — different clients reach different ceilings on the same
   cluster. Re-run baseline discovery per
   [runbook-recalibrate-steps.md](runbook-recalibrate-steps.md).

   Translating concurrency is not arithmetic, but total parallelism is the right thing to hold
   roughly constant. The latte plan documents its mapping from the c-s enterprise config:

   | Load | c-s threads | latte total parallelism (threads x concurrency) |
   |---|---|---|
   | read | 620 | 1760 |
   | write | 400 | 640 |
   | mixed | 1900 | 8000 |
   | read_disk_only | 620 | 1440 |

2. **New test-case YAML** if the stress commands differ structurally, e.g.
   `test-cases/performance/latte-perf-regression-predefined-throughput-steps-tablets.yaml`.

3. **New threshold config.** Latency on a different client is not comparable — the shipped
   `latency-decorator-error-thresholds-steps-latte-{tablets,vnodes}.yaml` files ship with
   values mostly `null`, pending baseline accumulation. That is the honest starting point:
   record first, gate later.

4. **New jenkinsfiles**, suffixed by tool
   (`...-predefined-throughput-steps-latte-tablets.jenkinsfile`).

5. **Trigger entry** in `configurations/triggers/perf-regression.yaml`, then run the generator.

6. **Run both in parallel** for long enough to build a baseline on the new tool before
   retiring the old jobs. The two tools' Argus histories are separate series and do not merge.

---

## Checklist

- [ ] `DockerBasedStressThread` subclass + events publisher
- [ ] Registered in `run_stress_thread`, ordering checked against existing substring matches
- [ ] Docker image pinned in `configurations/stress_images/`, arch-appropriate
- [ ] Writes `*/hdrh-*.hdr` to the loaders logdir
- [ ] HDR tags resolve via `_get_workload_type_by_hdr_tag`, or `stress_operation` set
- [ ] Throttle branch in `current_throttle()`, `unthrottled` -> empty string
- [ ] Step config format chosen; placeholders in the command template for extra params
- [ ] Argus result table if the output shape is non-standard
- [ ] Unit tests for command construction, tag resolution, throttle formatting
- [ ] Baseline re-derived — old rates not carried over
- [ ] Thresholds start `null`, tightened once a baseline exists
