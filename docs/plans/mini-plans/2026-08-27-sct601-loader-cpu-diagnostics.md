# Mini-Plan: Loader CPU/Thread Diagnostic Logging for Perf Tests

**Date:** 2026-08-27
**Estimated LOC:** ~280
**Related PR:** TBD
**Jira:** [SCT-601](https://scylladb.atlassian.net/browse/SCT-601)

## Problem

`test_latency_mixed_with_nemesis` run `936dcdb2` failed `_double_cluster_load` with a client-observed
P99 read of 697 ms while a run of the *same* Scylla revision hours earlier reported 5.34 ms, and every
server-side metric (`rlatencyp99` ~4 ms, `reactor_utilization` 50-60%, `sstable_overloads` 0) was flat.
The only differentiating signal was loader `load1` (peak 89.6 vs 56.1) — too coarse to say whether the
loaders were CPU-starved by a noisy neighbour, by the c-s JVM itself, or not at all. There is no
loader-side per-process/per-thread CPU capture and no per-step loader load figure in Argus, so a
client-side P99 spike can currently only be correlated, never root-caused.

## Approach

Follows the shape of the SCT-468 work (`cs_safepoint_logging`, commit `dd6dc98ef`, still unmerged on
branch `sct-468-cs-safepoint-logging`): one opt-in flag, an artifact written on the loader host,
carried into the loader logdir, collected by `LoaderLogCollector`, documented in
`docs/performance-tests.md`. This branch is based on master and does not depend on it - the two only
meet in the docs, when both have landed.

**Commit 1 — loader host sampler + collection**

- Add `loader_cpu_diagnostics` and `loader_cpu_diagnostics_per_thread` boolean flags (default
  `false`), next to `cs_extra_jvm_opts`.
- Add `data_dir/loader_cpu_sampler.sh`: a 1 Hz loop writing one timestamped block per sample —
  per-CPU busy/steal/irq/soft (`mpstat -P ALL 1 1`, falling back to `/proc/stat` deltas when
  `sysstat` is missing), `/proc/pressure/cpu` (PSI some/full avg10), `/proc/loadavg`, and per-process
  CPU plus `voluntary_ctxt_switches`/`nonvoluntary_ctxt_switches` for the c-s JVM PIDs.
- Emit per-**thread** samples (`pidstat -tu`) only every 10th sample, behind a separate
  `loader_cpu_diagnostics_per_thread` flag, and cap the output file size in the script. A c-s perf run
  uses hundreds to ~1000 threads, so `pidstat -t 1` alone would produce ~1000 lines/s (hundreds of MB
  per run).
- Install `sysstat` in `BaseLoaderSet.node_setup` with timeout + retries + non-interactive flags (per
  the `package-installation` skill); degrade to the `/proc`-only path when the install fails.
- Start the sampler as a systemd unit (`loader-cpu-diag.service`) in the same method, right after
  `node_exporter_setup.install(node)`, so it is killable, survives a reboot, and covers the whole run.
- Stream the file off the loader live with a new `LoaderCpuFileLogger(SSHGeneralFileLogger)` (only
  `REMOTE_LOG_PATH` differs), started next to `start_journal_thread` and stopped in the existing
  `stop_task_threads` path. Streaming rather than a teardown `receive_files` because perf loaders get
  terminated and the interesting samples are exactly the ones near a failure.
- Keep the sampler **per loader**, started at setup — not per stress thread like the safepoint log:
  several stress threads share a loader and CPU contention is a host property.
- Collect via one `FileLog(name="loader-cpu-*.log", search_locally=True)` entry in
  `LoaderLogCollector.log_entities`.

**Commit 2 — enable the `processes` node_exporter collector**

- Add `--collector.processes` to the `ExecStart` in `NodeExporterSetup.install` (off by default in
  node_exporter) for `node_procs_running` / `node_procs_blocked` on every loader and DB node.
- The `pressure` collector is *already* enabled (the current flag list disables 15 collectors but not
  `pressure`), so loader PSI is in the archived Prometheus snapshot of existing runs — no change needed
  and worth checking against run `936dcdb2` before/while implementing.

**Commit 3 — surface per-step loader load in Argus**

- In `collect_latency`, add per-loader aggregates over the step window it already receives:
  `max/avg` of `1 - rate(node_cpu_seconds_total{mode="idle"}[..])`, `max` of
  `rate(node_cpu_seconds_total{mode="steal"}[..])`, `max` of
  `rate(node_pressure_cpu_waiting_seconds_total[..])`, `max` of `node_load1`, and
  `rate(node_context_switches_total[..])`.
- Filter targets by `instance` against the loader nodes — loaders and DB nodes share the single
  `node_exporter` scrape job.
- The keys ride through `send_result_to_argus` into the per-cycle Argus row next to the P99, making the
  run A vs run B comparison from the ticket a table lookup instead of a Prometheus dig.

**Commit 4 — docs**

- `docs/performance-tests.md`: a "Loader CPU diagnostics" section on reading the sampler output,
  mirroring the safepoint one — read PSI/steal first, then per-process, then per-thread; pair it with
  `cs-safepoint-*.log` (large `At safepoint` = JVM's fault, large `Reaching safepoint` + high
  steal/PSI = host contention).
- `docs/collected-logs.md`: one row for `loader-cpu-*.log`.
- Regenerate `docs/configuration_options.md` for the new flags.

## Files to Modify

- `sdcm/sct_config.py` -- add `loader_cpu_diagnostics` and `loader_cpu_diagnostics_per_thread` fields
  next to `cs_extra_jvm_opts` (~line 2003)
- `defaults/test_default.yaml` -- defaults `false` for both flags
- `data_dir/loader_cpu_sampler.sh` -- (new file) the 1 Hz sampler
- `sdcm/cluster.py` -- `BaseLoaderSet.node_setup` (~line 6840): install `sysstat`, install + start the
  sampler unit; start/stop the new logger alongside `_journal_thread` (~lines 1475, 1729, 1754)
- `sdcm/utils/remote_logger.py` -- add `LoaderCpuFileLogger(SSHGeneralFileLogger)` (~line 341)
- `sdcm/logcollector.py` -- add the `loader-cpu-*.log` `FileLog` to `LoaderLogCollector.log_entities`
  (~line 1154)
- `sdcm/node_exporter_setup.py` -- add `--collector.processes` to `ExecStart`
- `sdcm/utils/latency.py` -- `collect_latency` (~line 24): add the per-loader step-window aggregates
- `docs/performance-tests.md`, `docs/collected-logs.md`, `docs/configuration_options.md` -- docs
- `unit_tests/unit/test_loader_cpu_diagnostics.py` -- (new file) sampler command/unit rendering and
  flag handling
- `unit_tests/integration/test_cassandra_stress_thread.py` -- integration test mirroring
  `test_07_cassandra_stress_safepoint_logging`

**Answered while implementing**

- `sdcm/argus_results.py` does require the columns to be declared: the latency tables are
  `StaticGenericResultTable`s with an explicit `Meta.Columns` list, and `send_result_to_argus` writes
  only the columns it names, so extra keys in the result dict are silently dropped. The four
  loader-load columns are declared once in `LOADER_LOAD_COLUMNS` and spliced into all four latency
  tables; a unit test asserts they are present in every one of them.
- Whether `sysstat` is present on the current loader AMIs — decides if the `/proc` fallback is the
  common path or the exception.

**Deliberately out of scope — follow-up PRs**

- `process-exporter` on loaders at `:9256` plus a scrape job next to the loader `node_exporter` targets
  in `configure_scylla_monitoring` (~`sdcm/cluster.py:7454-7481`), for per-process CPU as queryable
  metrics rather than text. ~80 LOC, own PR.
- JVM thread-name attribution: a single `jcmd <pid> Thread.print` taken **after** the stress command
  finishes, to map `pidstat -t` OS tids to JVM thread names (`nid=0x...`). Never during the run — a
  thread dump is itself a safepoint and would corrupt the tail latency being measured. Documented in
  `docs/performance-tests.md` already, not automated.
- The `de9cb6bc` anomaly (P99 read == P99 write == 91335.16 ms) — a stress-tool/results-parsing
  artifact, separate ticket, as the Jira issue itself states.

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_loader_cpu_diagnostics.py -v`
- [ ] Integration test passes: the sampler file exists on the loader, is non-empty, and lands in the
      loader logdir — `uv run python -m pytest unit_tests/integration/test_cassandra_stress_thread.py -k loader_cpu -v`
- [ ] With `loader_cpu_diagnostics: false` (the default) no sampler unit is started on the loaders and
      no `loader-cpu-*.log` appears — verified on a short AWS perf run
- [ ] `loader-cpu-*.log` is present in the collected-logs archive under each loader directory and its
      samples cover the whole run at ~1 s granularity, with per-thread blocks only every ~10 s
- [ ] Per-thread output stays bounded: sampler log under ~200 MB for a 2-hour run with 1000 c-s threads
- [ ] `node_procs_running` is queryable for a loader instance in the run's Prometheus
- [ ] The Argus per-cycle row for a latency step shows the loader busy/steal/PSI/load1 fields
- [ ] Overhead control: `latency-650gb-with-nemesis` run with the flag on vs off — steady-state P99 and
      throughput within run-to-run noise (this is the precondition for enabling it by default in the
      perf pipelines, which is a follow-up decision, not part of this PR)
- [ ] `uv run sct.py pre-commit` passes
