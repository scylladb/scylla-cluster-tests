# Runbook: Add a New Performance Test

From an idea to a scheduled job.

Read [anatomy.md](anatomy.md) first — it defines the phase skeleton you are filling in.

---

## Step 0 — Does it need to be a new test?

Cheapest to most expensive:

| Option | When |
|---|---|
| New test-case YAML on an existing driver | Same measurement, different workload, dataset or topology. **Most new perf tests are this.** |
| New overlay config | Only steps or thresholds change |
| New test method on an existing class | New measurement shape, existing lifecycle |
| New driver class | Genuinely new lifecycle (new preload semantics, new phase structure) |

Check [test-catalog.md](test-catalog.md) before writing a class. Seven families and ~25
classes already exist.

## Step 1 — Pick the base class

| Base | Gives you |
|---|---|
| `PerformanceRegressionTest` | Preload, fstrim, compaction waits, `run_workload`, the latency decorator wiring. **Default choice.** |
| `PerformanceRegressionPredefinedStepsTest` | All of the above plus the throughput staircase loop |
| `PerformanceRegressionUpgradeTest` | Rolling upgrade with per-node cycles |
| `MicrobenchmarkTest` | Single-node `scylla perf-*` tool, no cluster workload |
| `ClusterTester` | Only when none of the perf lifecycle applies (e.g. `ScaleUpTest`, `ThroughputLimitFunctionalTest`) |

## Step 2 — Write the test method

Rules that are not optional:

- **Every measured window is wrapped in `latency_calculator_decorator`** with an explicit
  `cycle_name`. Nothing reaches Argus otherwise.
- **`use_hdrhistogram` must be enabled**, or the decorator returns early and collects nothing.
- **Settle before measuring**: `run_fstrim_on_all_db_nodes()` ->
  `wait_no_compactions_running()` -> `run_fstrim_on_all_db_nodes()`.
- **Warm up and discard** if the workload is cache-sensitive.
- Name the method `test_<workload>_<what>` — the decorator infers `workload` from the test
  name when `workload_type` is not passed explicitly, matching on `read_disk_only`, `read`,
  `write`, `mixed`.

```python
@latency_calculator_decorator(legend="My new scenario", cycle_name="my_scenario")
def _run_my_scenario(self, stress_cmd):
    ...

def test_latency_my_scenario(self):
    self.preload_data()
    self.run_fstrim_on_all_db_nodes()
    self.wait_no_compactions_running(n=240, sleep_time=180)
    self.run_fstrim_on_all_db_nodes()
    self._run_my_scenario(self.params.get("stress_cmd_m"))
```

If the cycle name is dynamic, apply the decorator dynamically — see the staircase loop in
[anatomy.md](anatomy.md#5-measure--the-cycle).

## Step 3 — Test-case YAML

`test-cases/performance/<name>.yaml`. Follow the naming of the family you are joining
(`perf-regression-latency-650gb-*`, `perf-regression-predefined-throughput-steps-*`).

Essentials:

```yaml
test_duration: <minutes>
n_db_nodes: 3
n_loaders: <enough that loader CPU stays under 70%>
n_monitor_nodes: 1

prepare_write_cmd: <preload>
stress_cmd_m: <workload>

use_hdrhistogram: true
round_robin: true

user_prefix: 'perf-<name>'
argus_email_report_template: email_report_performance.yaml
```

Sizing conventions from [strategy.md](../performance-testing-strategy.md): RF=3, QUORUM (or LOCAL_QUORUM multi-DC),
local NVMe only, and preload to ~10x RAM for read/latency tests so the working set does not
fit in cache.

Add a `test_metadata` block — see the `reviewing-pipeline-docs` skill for the schema, and
`lint-test-docs` to check it.

## Step 4 — Thresholds

Add a `latency_decorator_error_thresholds` entry, either inline in the test-case YAML (as
`perf-regression-alternator-*.yaml` does) or as a separate overlay under
`configurations/performance/`.

**Cycle keys must match the decorator's `name` exactly** — the `cycle_name` you passed, the
wrapped function's name, or `"Steady State"`. A mismatch silently falls back to the `default`
block (`P99 <= 10 ms`). See
[results-and-regression.md](results-and-regression.md#key-matching-gotcha).

**Start with `null` and tighten later.** You have no baseline on day one; a guessed limit
either never fires or fires constantly. Record for a few runs, then set values from observed
results. `null` is explicit and documents intent — an omitted key is not the same thing.

## Step 5 — Jenkinsfile

`jenkins-pipelines/performance/branch-perf-v17/scylla-{enterprise,master}/perf-regression/<name>.jenkinsfile`.

Use `performance_staging/` first if the test is still experimental.

```groovy
perfRegressionParallelPipeline(
    backend: 'aws',
    region: 'eu-west-2',
    test_name: 'my_perf_test.MyPerfTest',
    test_config: '''["test-cases/performance/<name>.yaml",
        "configurations/performance/<steps>.yaml",
        "configurations/performance/<thresholds>.yaml"]''',
    sub_tests: '["test_latency_my_scenario"]',
    ...
)
```

`test_config` order matters — later entries override earlier ones. Thresholds last.

Each entry in `sub_tests` becomes an independent parallel stage with its own cluster.

## Step 6 — Trigger entry

Edit `configurations/triggers/perf-regression.yaml` (**not** the generated jenkinsfile):

```yaml
- job_name: "/scylla-enterprise/perf-regression/<name>"
  backend: "aws"
  include_versions: ["master"]
  labels: ["master-monthly"]
  job_throttle_category: "SCT-perf-<region>-<family>"
  params:
    region: "<region>"
    sub_tests: '["test_latency_my_scenario"]'
```

Then regenerate:

```bash
python3 utils/build_system/generate_trigger_jenkinsfiles.py
```

Choosing a cadence:

| Label | Cadence | Suits |
|---|---|---|
| `master-weekly` | weekly | Core regression coverage |
| `master-2weeks` | 14 days | |
| `master-3weeks` | 21 days | Nemesis and upgrade tests (long, expensive) |
| `master-monthly` | 1st of month | Secondary topologies (vnodes) |
| no label | version-gated only | Release-branch runs |

Remember label gating applies to `master` only; release entries use `labels: []` plus version
gating.

Pick a region free at that cadence and record it in
[../perf-tests-region-scheduling.md](../perf-tests-region-scheduling.md).

## Step 7 — Validate

- [ ] Run manually against a recent build
- [ ] Results appear in Argus under the expected table names
      (`<workload> - <name> - latencies`)
- [ ] Cycle row names are what you expect
- [ ] Thresholds attach to the right cycles — check the Argus Results tab, not just the logs
- [ ] `hdr_summary` is populated (empty means the HDR tags did not resolve)
- [ ] Reactor stall tables appear if stalls occurred
- [ ] Test duration fits the cadence

## Step 8 — Unit tests

`unit_tests/` for any new config parsing, step generation or result computation. See the
`writing-unit-tests` skill.

---

## Checklist

- [ ] Confirmed a new test is actually needed (Step 0)
- [ ] Base class chosen
- [ ] Every measured window wrapped in `latency_calculator_decorator` with explicit `cycle_name`
- [ ] `use_hdrhistogram: true`
- [ ] fstrim + compaction wait before first measurement
- [ ] Test-case YAML with `test_metadata`
- [ ] Thresholds start `null`, keys matching cycle names
- [ ] Jenkinsfile with correct `test_config` ordering
- [ ] Trigger entry added, generator run
- [ ] Region free at chosen cadence, recorded
- [ ] Manual run verified in Argus
- [ ] Unit tests
