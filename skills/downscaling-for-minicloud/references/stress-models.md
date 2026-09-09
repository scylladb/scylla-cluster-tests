# Stress Models and Where Each Keeps Its Volume

SCT drives nine stress tools, and they do not share a way of expressing "how much load".
Shrinking the wrong expression leaves the run at full size. This page covers each one: the
knob that carries the volume, the shape that must not change, and a sane local target.

The rule is the same everywhere — **cut volume, keep shape**. Row counts, thread counts,
concurrency, rates and durations shrink. Keyspace and table names, consistency levels,
replication factors, column shapes, operation sets and profile paths do not: those are what
the test asserts on.

---

## cassandra-stress — `stress_cmd`, `prepare_write_cmd`, `stress_read_cmd`

The default, and a flat command string (or a list of them, one per loader).

| Carries volume | Local target |
|---|---|
| `n=` and the matching `-pop seq=1..N` | 100k-300k rows. **Always change both together** — a `-pop` range wider than `n=` changes the access pattern, not just the size |
| `duration=` | Minutes. Mutually exclusive with `n=` |
| `-rate threads=` | ~10. A 1-vCPU guest cannot serve more, and trying produces client timeouts that look like a Scylla problem |
| `-col 'n=FIXED(x) size=FIXED(y)'` | Leave alone unless the row is enormous — it is part of the schema shape |

Keep: `cl=`, the `-schema` block (keyspace name, `replication_factor`, compression,
compaction), and the **number of commands in a list** — two commands means two parallel
streams, and collapsing them changes the test.

**User profiles** (`cassandra-stress user profile=... ops'(...)'`): shrink `n=` and `threads=`
only. The `ops(...)` set and the profile path are asserted on; a path must stay resolvable
against `data_dir/`.

---

## cql-stress-cassandra-stress — same keys

A Rust reimplementation with a cassandra-stress-compatible CLI, selected per test-case. Shrink
it exactly like cassandra-stress; the flags carry the same meaning.

---

## scylla-bench — `stress_cmd`, `prepare_write_cmd`

Flag-based rather than parameter-string based, so the knobs look nothing like cassandra-stress.

| Carries volume | Local target |
|---|---|
| `-partition-count` | Tens, not thousands |
| `-clustering-row-count` | Hundreds |
| `-clustering-row-size` | Leave alone — schema shape |
| `-concurrency` | ~4 |
| `-rows-per-request` | Leave alone |
| `-max-rate` | A few hundred ops/s, or drop it with the rest shrunk |
| `-duration` | Minutes |

Keep: `-workload`, `-mode`, `-replication-factor`, `-consistency-level`. Data-validation
configs reference `scylla_bench.test` and a partition range by number — shrink
`-partition-count` and the `data_validation` block **together**, or validation reads
partitions that were never written.

---

## latte and the gradual-throughput perf family

Load lives in config keys, not in a command string. `stress_cmd` is not read.

| Carries volume | Local target |
|---|---|
| `perf_gradual_throttle_steps` | A dict of `{threads, concurrency, rate}` lists per sub-test. Production asks 300K-1.1M ops/s at up to 8000 parallelism. Keep one or two steps at a few thousand ops/s and drop `unthrottled` — on a 1-vCPU guest it is a self-inflicted overload |
| `perf_gradual_step_duration` | `30m` per step in production. Minutes |
| `stress_step_duration` | The non-gradual perf variants' equivalent |
| `perf_gradual_write_preload_data` | Turn off if the preload dwarfs the steps |
| `n_loaders` | 4 in production; 1 locally |

The jenkinsfile's `sub_tests` list becomes a separate job per entry — run **one** locally.
Drop any `latency-decorator-error-thresholds-*.yaml` from the config list: it asserts on
measured latency, which is meaningless here.

---

## gemini — `gemini_cmd` and an oracle cluster

Gemini compares Scylla against an oracle, so it provisions a **second cluster**:
`n_test_oracle_db_nodes` (default 1, counted by the memory gate) plus
`instance_type_db_oracle` and `oracle_scylla_version`.

Shrink the gemini command's duration, concurrency and dataset flags, and budget the oracle
node as a guest like any other. If the change under test has nothing to do with gemini, prefer
a cheaper test — this is the most expensive shape per unit of coverage.

---

## ycsb, ndbench, nosqlbench, cdcreader

Command-string tools like cassandra-stress, with their own spellings of the same three ideas:

- **record/operation count** — `recordcount` / `operationcount` (ycsb), `cycles` (nosqlbench),
  `numWriters`/`numReaders` (ndbench)
- **concurrency** — `threadcount` (ycsb), `threads=` (nosqlbench)
- **rate and duration** — `target` (ycsb), `cyclerate` (nosqlbench)

Cut all three; leave the workload/table/consistency arguments alone. `cdcreader` follows a CDC
log rather than generating load, so its cost tracks whatever wrote the data — shrink the
writer, not the reader.

---

## Finding the Model for a Test You Do Not Recognise

1. `test_metadata.stress_tools` in the test-case yaml names the tools it drives.
2. `grep -n "params.get" ` in the test class shows which keys carry them.
3. If no `stress_cmd`-shaped key appears, the load is in config keys — the test builds its own
   command. Look for the params interpolated into it.
4. Resolve the config and read the values back before running:
   `uv run sct.py conf` prints exactly what the merge produced.
