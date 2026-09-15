---
name: downscaling-for-minicloud
description: >-
  Downscale an SCT test-case yaml or pipeline jenkinsfile to fit on one machine
  and run it against minicloud, the local QEMU/KVM cloud emulator. Use for "fit
  this test on my laptop", "shrink a jenkinsfile to run locally", "validate my
  change before spending cloud time", "write a configurations/minicloud
  overlay", or a local run that fails preflight, OOMs with exit 137, or crawls
  on 1-vCPU guests. Not for performance conclusions or minicloud host setup.
---

# Downscaling Tests for minicloud

Take any SCT test-case or pipeline job, shrink it until one machine can run it against the
local emulator, run it, and read the failures.

**Input is a test-case yaml or a jenkinsfile.** A jenkinsfile is the better starting point —
it carries the production config stack in order, the test name and the backend, and SCT
already parses it. Either way the output is the same: a small overlay layered on top of the
production config, plus a runner script.

Platform setup (KVM, docker, `minicloud0`, firewalld, the image cache) is out of scope —
see [docs/minicloud.md](../../docs/minicloud.md), "Running locally".

## The Short Version

1. **Get the config list.** From a jenkinsfile, `parse_jenkinsfile()` prints it; from a yaml,
   it is that file plus whatever overlays the job layers.
2. **Read what the test method actually consumes** — `grep -n "params.get" <module>.py`.
3. **Budget the guests:** `guests x minicloud_lightweight_memory + ~2 GiB` must fit in
   `MemAvailable`. That decides the node counts, not trial and error.
4. **Write the overlay** in `configurations/minicloud/`, layered *after* the test-case, with
   `configurations/minicloud.yaml` last.
5. **Verify without booting:** `sct.py conf` resolves the merge and prints what you got.
6. **Run it** through a phase runner, and triage from the tables in the references.

Full procedure: [downscale-a-test.md](workflows/downscale-a-test.md).

## Essential Principles

### Budget the Guests Before Editing Anything

`preflight_check()` enforces `guests x per-guest memory + ~2 GiB` against `MemAvailable` and
prints the arithmetic when it fails. Compute it first and let it decide the node counts.

**Why:** a test that slips past the gate dies as a cgroup OOM kill (container exit 137) that
takes every VM down at once and surfaces as a wall of SSH timeouts far from the cause.

### Write an Overlay, Never a Copy

A new file under `configurations/minicloud/`, layered *after* the production test-case.

**Why:** a copy silently misses any param the test-case later grows. With an overlay a new key
arrives at its production value — too big for one host — so the memory gate reports it
immediately, at the right place.

### Cut Volume, Keep Shape

Row counts, thread counts, durations and partition counts shrink. Keyspace names, consistency
levels, compaction settings, `ops(...)` sets and the number of commands in a list do not.

**Why:** tests assert on shape. `test_rolling_upgrade` calls `metric_has_data()` against a
keyspace name from the stress command. Renaming one turns a downscale into a different test
that fails for the wrong reason.

### Shrink What the Test Method Actually Reads

Read the test method first; `grep -n "params.get" ` in the test class settles it.

**Why:** `GrowClusterTest.get_stress_cmd()` builds its own cassandra-stress command from
`cassandra_stress_population_size`, `cassandra_stress_threads` and `test_duration`, and never
reads `stress_cmd`. Shrinking that is dead configuration — the run still executes at full size.

### Instance Types Are Labels Here, Not Sizes

Lightweight mode gives every guest 1 vCPU and `minicloud_lightweight_memory` regardless of the
type asked for. The type only has to exist in the emulated catalog, and be x86_64.

**Why:** the only levers that change what the host carries are node counts and per-guest
memory. Time spent picking a smaller `instance_type_db` changes nothing.

## When to Use

- Validating an SCT framework, provisioning or log-collection change before spending cloud time
- Smoke-running a new nemesis, test-case or configuration end to end
- Making a production test-case or pipeline job fit on a laptop or lab host
- A local run that fails preflight, dies with exit 137, or crawls on 1-vCPU guests
- Deciding how far an already-shrunk test can be grown on a bigger machine

## When NOT to Use

- **Performance, latency or scalability conclusions** — the shrink preserves shape, not scale.
  Real cloud and the perf pipelines; see `perf-weekly-status-report`.
- **Anything needing KMS, spot, real NVMe, arm64 guests, or disruptive node lifecycle** —
  minicloud implements none of them. Real backend.
- **Host setup** — [docs/minicloud.md](../../docs/minicloud.md), "Running locally".
- **Writing a brand-new test-case** — `writing-plans` first, then downscale the result.
- **Migrating `instance_type_*` to constraint-based sizing** — that is `migrate-to-sizing`.

## The Overlay Contract

Layer order matters, and `configurations/minicloud.yaml` goes **last**:

```bash
-c test-cases/upgrades/rolling-upgrade.yaml \
-c configurations/minicloud/rolling-upgrade.yaml \
-c configurations/minicloud.yaml
```

- `configurations/minicloud.yaml` is **mandatory** — KMS off, `on_demand`, private SSH, no
  iotune, no AZ/region fallback, no kernel-panic checker, no placement groups or capacity
  reservations, and `developer_mode`. `validate_minicloud_params()` refuses the run without it.
  Do not re-set any of it in your overlay.
- Later files win. `SCT_*` env vars are applied after the merge and win over all of them, but
  **cannot substitute for the overlay** — the emulator reads those params from the built config.
- Passing your own `SCT_TEST_CASE` to `scripts/run-minicloud-test.sh` drops the flavor's shrink
  overlay, by design.

## Running It

```bash
# the five built-in flavors: ami | repo | provision | upgrade | scale
SCT_SCYLLA_VERSION=2026.2 scripts/run-minicloud-test.sh -f provision -b aws

# anything else: same config list for every phase
uv run sct.py start-minicloud -b aws -c <test-case> -c <overlay> -c configurations/minicloud.yaml
uv run sct.py run-test <module.Class.method> --backend aws -c <test-case> -c <overlay> -c configurations/minicloud.yaml
```

**Export `SCT_MINICLOUD_ENDPOINT_URL` for every invocation.** Each `sct.py` call is a separate
process; `start-minicloud` sets the endpoint inside its own environment and that does not reach
`run-test`. Without it, `is_minicloud_active()` is false and boto3 provisions against **real
AWS** — real instances, real money — with a log that looks almost identical. A DB node whose
private IP is `10.4.x.x` rather than `10.164.x.x` in `eu-west-1` is a run on the real cloud.

Teardown runs **after** log collection — removing the container kills every guest with it.

For anything you will run twice, generate a phase runner:
[run-script-template.md](references/run-script-template.md).

## Verify Without Booting Anything

```bash
SCT_SCYLLA_VERSION=2026.2 uv run sct.py conf -b aws \
  '["test-cases/x.yaml","configurations/minicloud/x.yaml","configurations/minicloud.yaml"]'
```

Resolves the whole merge, runs `verify_configuration()` and `check_required_files()`, and
prints every resolved value. Check the node counts against your budget here. A version must be
supplied or `conf` stops at `_check_version_supplied`. `conf` validates the *configuration*;
the overlay and the memory budget are checked later, by `start-minicloud`.

## Reference Index

| File | Content |
|------|---------|
| [sizing-knobs.md](references/sizing-knobs.md) | The memory budget, the floors, every knob that changes what the host carries, and what the shared overlay already handles |
| [test-type-params.md](references/test-type-params.md) | Which params size the load per test entry point, and which are ignored |
| [stress-models.md](references/stress-models.md) | All nine stress tools SCT drives — each keeps its volume somewhere different |
| [triage-local-runs.md](references/triage-local-runs.md) | Symptom to cause to the overlay value that fixes it, and how to reclaim host memory |
| [worked-examples.md](references/worked-examples.md) | Two overlays produced by this skill, annotated |
| [run-script-template.md](references/run-script-template.md) | Generating the phase runner that drives a downscaled run safely |

| Workflow | Purpose |
|----------|---------|
| [downscale-a-test.md](workflows/downscale-a-test.md) | 5-phase process from a test-case or jenkinsfile to a green local run |

The repository ships no library of ready-made downscales beyond the few a pipeline runs: a
shrink is specific to a test, a change and a host, and one sitting unused goes stale against
the test-case it shrinks. Produce the overlay you need; keep it if a job will run it.

## Success Criteria

- [ ] Is an overlay in `configurations/minicloud/`, not a copied test-case
- [ ] Header comment states the guest budget arithmetic it assumes
- [ ] Passes `uv run sct.py conf` with `configurations/minicloud.yaml` layered last
- [ ] Fits the host: guests x per-guest memory + 2 GiB is under `MemAvailable`
- [ ] Shrinks only params the test method actually reads
- [ ] Keeps keyspace names, consistency levels and ops sets identical to production
- [ ] Sets `nemesis_selector: "not disruptive"` if it runs a nemesis at all
- [ ] Pins an x86_64 db type, and catalog types for roles the base leaves unpinned
- [ ] Does not re-set anything `configurations/minicloud.yaml` already carries
- [ ] Ships with a runner that exports `SCT_MINICLOUD_ENDPOINT_URL` and asserts the emulator is
      the target before provisioning
