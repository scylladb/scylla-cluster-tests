---
name: downscaling-for-minicloud
description: >-
  Guides shrinking an SCT test-case or Jenkins pipeline job so it runs against
  minicloud, the local QEMU/KVM cloud emulator, on a single developer machine or
  lab host. Takes a jenkinsfile or a test-case yaml as input. Use when
  validating an SCT change locally before spending cloud time, smoke-running a
  new nemesis or test-case, fitting a production test-case into host RAM,
  writing a configurations/minicloud overlay, or triaging a local run that fails
  preflight, dies with exit 137, or crawls on 1-vCPU guests. Not for performance
  or scalability conclusions, and not for minicloud host setup.
---

# Downscaling Tests for minicloud

Turn a production SCT test-case into something one machine can actually run, run it against
the local emulator, and read the failures it produces.

A production test-case is 6-15 nodes of `i4i.2xlarge` with 20M-row workloads. Under minicloud
every guest is a QEMU VM with 1 vCPU and 4 GiB, and the whole test has to fit in one host's
free RAM. The gap is closed by a **configuration overlay**, not by editing the test-case.

**Input is a jenkinsfile or a config list.** A jenkinsfile is the better starting point: it
carries the production config stack, the test name and the backend, and SCT already parses it
(`sdcm.utils.lint.jenkins_parser.parse_jenkinsfile`). Phase 0 of the workflow turns one into a
ready-to-use `-c` list.

Platform setup (KVM, docker, `minicloud0`, firewalld, the image cache) is out of scope here —
see [docs/minicloud.md](../../docs/minicloud.md), "Running locally".

## Essential Principles

### Budget the Guests Before Editing Anything

```
(n_db_nodes + n_loaders + n_monitor_nodes + n_db_zero_token_nodes + n_vector_store_nodes
 [+ n_test_oracle_db_nodes, only when db_type is mixed_scylla])
    x minicloud_lightweight_memory + ~2 GiB headroom
```

Compute this first and let it decide the node counts, instead of shrinking values until
something works. `GUEST_NODE_COUNT_PARAMS` in `preflight.py` is the authority. The pools past
the first three are easy to forget, and the oracle one is conditional: `n_test_oracle_db_nodes`
defaults to 1, but the cluster is only built for `db_type: mixed_scylla`, so an ordinary run
must not be charged for it.

**Why:** `preflight_check()` in `sdcm/utils/minicloud/preflight.py` enforces exactly this
arithmetic before the container starts and prints it when it fails. A test that slips past the
gate dies as a cgroup OOM kill (container exit 137) that takes every VM down at once and
surfaces as a wall of SSH timeouts far from the cause.

### Write an Overlay, Never a Copy

Add a file under `configurations/minicloud/` and layer it *after* the production test-case.
Never copy a test-case and edit the copy.

**Why:** the test method reads a dozen params. A copy silently misses any param the real
test-case later grows, and the run fails somewhere obscure mid-flight. With an overlay a new
key arrives at its production value, which is too big for one host — so the memory gate reports
it immediately, at the right place.

### Cut Volume, Keep Shape

`n=`, `threads=`, `duration=`, partition counts and row counts shrink. Keyspace names,
consistency levels, compaction settings, `ops(...)` sets, and the number of commands in a list
stay identical.

**Why:** tests assert on shape. `test_rolling_upgrade` calls `metric_has_data()` against a
keyspace name from the stress command; the complex-profile steps assert on their ops sets.
Renaming or dropping one turns a downscale into a different test that fails for the wrong
reason.

### Shrink What the Test Method Actually Reads

Read the test method before touching any value. `grep -n "params.get" ` in the test class
settles which keys it consumes.

**Why:** `GrowClusterTest.get_stress_cmd()` *builds* its cassandra-stress command from
`cassandra_stress_population_size`, `cassandra_stress_threads` and `test_duration`. It never
reads `stress_cmd` or `prepare_write_cmd`, and `run_fullscan` is only wired up in
`longevity_test`. Shrinking those keys for a grow test is dead configuration — the run still
executes at full size.

### Instance Types Are Labels Here, Not Sizes

`minicloud_lightweight` (default `true`) gives every guest `minicloud_lightweight_vcpus` (1)
and `minicloud_lightweight_memory` (4GiB) **regardless of the instance type the test asked
for**. The type only has to exist in the emulated catalog and pass SCT validation, so keep it
realistic and move on.

**Why:** the only levers that change what the host carries are node counts and per-guest
memory. Time spent picking a smaller `instance_type_db` changes nothing.

## When to Use

- Validating an SCT framework, provisioning or log-collection change before spending cloud time
- Smoke-running a new nemesis, a new test-case, or a new configuration end to end
- Making an existing production test-case fit on a laptop or a lab host
- A local minicloud run that fails preflight, dies with exit 137, or crawls on 1-vCPU guests
- Deciding how far an already-shrunk test can be grown on a bigger machine

## When NOT to Use

- **Performance, latency or scalability conclusions** — the shrink preserves shape, not scale.
  Use the real cloud and the perf pipelines; see `perf-weekly-status-report`.
- **Anything that needs KMS, spot instances, real NVMe, or disruptive node lifecycle** —
  minicloud implements none of them. Run it on a real backend.
- **Host setup: KVM, docker, `minicloud0`, firewalld, first-run image caching** —
  [docs/minicloud.md](../../docs/minicloud.md), "Running locally".
- **Writing a brand-new test-case from scratch** — use `writing-plans` and the existing
  test-case conventions first, then downscale the result.
- **Migrating `instance_type_*` to constraint-based sizing** — that is `migrate-to-sizing`.

## The Guest Memory Budget

Every guest costs `minicloud_lightweight_memory` whether it is a db node, a loader or a
monitor. What a host can carry:

| Host free RAM | Guests at 4GiB | Guests at 3GiB | Realistic shape |
|---|---|---|---|
| 16 GiB | 3 | 4 | 3 db + 1 loader at 3GiB, no monitor |
| 32 GiB | 7 | 10 | 3 db + 1 loader + 1 monitor (the default shape) |
| 64 GiB | 15 | 20 | 6 db + 2 loaders + 1 monitor, or a 3 to 6 grow test |

Two adjustments the gate makes:

- **A growing cluster is budgeted by `cluster_target_size`, not `n_db_nodes`.** Budgeting the
  starting cluster would pass and then let the run die at the moment it adds the node nobody
  accounted for.
- **`minicloud_container_memory`, when set, replaces host free memory as the budget** — the
  cgroup OOM killer enforces the cap, so measuring the host would pass a test the cap kills.

`minicloud_skip_memory_check: true` disables the gate for a host you know can take it. The
price is the exit-137 failure mode above if you were wrong.

### When the Budget Does Not Fit

A development laptop is rarely short of RAM because the test is too big — it is short because
something else is holding it. Find the holder before shrinking the test further:

```bash
free -m                                            # available, not free, is the number that matters
ps -eo rss,comm --no-headers | awk '{a[$2]+=$1} END{for(c in a) printf "%8.2f GiB  %s\n", a[c]/1048576, c}' | sort -rn | head
docker ps --format '{{.Names}}\t{{.Status}}'      # long-running containers from earlier work
swapon --show                                      # a full swap device means real pressure, not fragmentation
```

Reclaim in this order — cheapest and least disruptive first:

1. **Idle agent, editor and shell sessions.** Dozens of day-old sessions at ~0.4 GiB each add
   up faster than anything else on a development box.
2. **The browser.** Usually the single largest process group after the sessions.
3. **Exited and forgotten containers** from earlier test work, and any cluster left running.
4. **Stale VM disks** in `minicloud_state_dir/instances` — disk, not memory, but a full disk
   fails a run just as hard. Never `amis/`.

Measure before stopping anything: reputation is a poor guide to footprint. An idle local model
server holds tens of MiB, not gigabytes, until a model is actually loaded — the `ps` aggregate
above settles it in a second, and stopping the wrong service costs you the service and buys
nothing.

**Swap is not the answer.** `MemAvailable` — what the gate measures — excludes swap, so adding
swap does not raise the budget directly; it only helps to the extent the kernel then evicts
cold pages from idle processes. And a guest that swaps is worse than a guest that never
started: Scylla in a swapping VM produces timeouts and latency artifacts that look like
findings. Give the guests real RAM or run fewer of them.

## The Overlay Contract

Layer order matters, and `configurations/minicloud.yaml` goes **last**:

```bash
-c test-cases/upgrades/rolling-upgrade.yaml \
-c configurations/minicloud/rolling-upgrade.yaml \
-c configurations/minicloud.yaml
```

- `configurations/minicloud.yaml` is **mandatory**. It carries KMS off, `instance_provision:
  on_demand`, `ip_ssh_connections: private`, `force_run_iotune: false`, AZ/region fallback off,
  the kernel-panic checker off, and `append_scylla_yaml.developer_mode`. Without it
  `validate_minicloud_params()` aborts with the exact missing values. Do not re-set any of it
  in your overlay.
- Later files win; `SCT_*` env vars are applied after the merge and win over all of them. They
  can override a param for one run, but they **cannot** substitute for the overlay — the
  emulator reads those params out of the built config, not the environment.
- Backend overlays `configurations/minicloud/aws.yaml` and `gce.yaml` set `instance_type_runner`
  and `root_disk_size_runner` for CI runs on an sct-runner. A local run does not need them; a
  multi-guest test that does include one must set its own `instance_type_runner` **after** it.
- Passing your own `SCT_TEST_CASE` to `scripts/run-minicloud-test.sh` **drops the flavor's
  shrink overlay** — by design, so the script never silently shrinks someone else's yaml.

## Floors You Cannot Go Below

| Value | Floor | Why |
|---|---|---|
| `n_db_nodes` | 3 | RF=3 keeps quorum with one node down; below that a nemesis or a rolling restart breaks the cluster |
| `n_loaders` | 1 | No loader, no workload |
| `n_monitor_nodes` | 1 when the test reads Prometheus | `test_rolling_upgrade` gates on `metric_has_data()`; `GrowClusterTest` reconfigures monitoring per added node |
| `minicloud_lightweight_memory` | ~3 GiB | Scylla reserves ~1.7 GiB for the guest OS and needs 1 GiB per shard; below this it fails to boot with "memory per shard too low" |
| `gce_n_local_ssd_disk_db` | 1 on GCE | Guests get qcow2-backed disks, no NVMe passthrough; the `gce_config.yaml` default of 4 cannot be served |
| guest architecture | x86_64 | minicloud does not support arm64 guests. Override `instance_type_db` to an x86_64 type of the same shape — the image follows, because SCT derives the arch from the instance type |

A test that genuinely needs more than 3 db nodes is not a candidate for a 16 GiB laptop — move
it to a lab host rather than dropping below a floor.

## Nemesis Under minicloud

Always `nemesis_selector: "not disruptive"`. Terminate, reboot and stop nemeses exercise
emulated instance-lifecycle paths that are not all implemented, so a failure there is a
minicloud gap, not a Scylla finding — and it is indistinguishable from a real one in the logs.
Widen the selector only on a real backend.

## Running It

```bash
# the five built-in flavors: ami | repo | provision | upgrade | scale
SCT_SCYLLA_VERSION=2026.2 scripts/run-minicloud-test.sh -f provision -b aws

# anything else: start the emulator with the same config list the test will use
uv run sct.py start-minicloud -b aws -c <test-case> -c <overlay> -c configurations/minicloud.yaml
uv run sct.py run-test <module.Class.method> --backend aws -c <test-case> -c <overlay> -c configurations/minicloud.yaml
```

Both commands need the **same** config list: `start-minicloud` sizes the container from the
node counts the test will provision, so a mismatch means the memory gate checked a different
test than the one that runs.

**Export `SCT_MINICLOUD_ENDPOINT_URL` for every invocation.** Each `sct.py` call is a separate
process; `start-minicloud` sets the endpoint inside its own environment and that does not reach
`run-test`. Without it exported, `is_minicloud_active()` is false and boto3 provisions against
**real AWS** — real instances, real money — with a log that looks almost identical. A DB node
whose private IP is `10.4.x.x` rather than `10.164.x.x` in `eu-west-1`, or that has a routable
public IP, is a run on the real cloud.

Teardown (`clean-resources`, or `docker rm -f minicloud`) runs **after** log collection —
removing the container kills every guest with it.

For anything you will run more than once, generate a phase runner alongside the overlay rather
than pasting commands: it makes the ordering unmissable and asserts the emulator is really the
target before provisioning. See
[run-script-template.md](references/run-script-template.md).

## Verify Without Booting Anything

```bash
SCT_SCYLLA_VERSION=2026.2 uv run sct.py conf -b aws \
  '["test-cases/x.yaml","configurations/minicloud/x.yaml","configurations/minicloud.yaml"]'
```

This resolves the whole merge, runs `verify_configuration()` and `check_required_files()`, and
prints every resolved value. Check the node counts against your budget here — before spending
an hour discovering the arithmetic was wrong.

A version has to be supplied (`SCT_SCYLLA_VERSION`, or `SCT_AMI_ID_DB_SCYLLA` /
`SCT_GCE_IMAGE_DB`) or `conf` fails on `_check_version_supplied` before it gets to anything
interesting. Note that `conf` validates the *configuration*; the minicloud parameter overlay
and the memory budget are checked later, by `start-minicloud`.

## Reference Index

| File | Content |
|------|---------|
| [sizing-knobs.md](references/sizing-knobs.md) | Every knob that changes what the host carries, what it costs, and what to leave alone |
| [test-type-params.md](references/test-type-params.md) | Which params size the load per test entry point, and which are ignored |
| [stress-models.md](references/stress-models.md) | How to shrink each stress tool SCT drives, and where each one keeps its volume |
| [triage-local-runs.md](references/triage-local-runs.md) | Symptom to cause to the overlay value that fixes it |
| [worked-examples.md](references/worked-examples.md) | Two overlays produced by this skill, annotated — what good output looks like |
| [run-script-template.md](references/run-script-template.md) | Generating the phase runner that drives a downscaled run safely |

| Workflow | Purpose |
|----------|---------|
| [downscale-a-test.md](workflows/downscale-a-test.md) | 5-phase process from a production test-case to a green local run |

Worked examples already in the repo: `configurations/minicloud/rolling-upgrade.yaml`,
`scale-cluster.yaml`, `generic-rolling-upgrade.yaml`, and the two purpose-built test-cases
`test-cases/minicloud-provision-test.yaml` and
`test-cases/longevity/longevity-minicloud-10gb-1h.yaml`.

Those five exist because a pipeline runs them. The repository deliberately ships no library of
ready-made downscales beyond them: a shrink is specific to a test, a change and a host, and one
that sits in the tree unused goes stale against the test-case it shrinks. Produce the overlay
you need, keep it if a job will run it, delete it if not.

## Success Criteria

A good minicloud downscale:

- [ ] Is an overlay in `configurations/minicloud/`, not a copied test-case
- [ ] Has a header comment stating the guest budget arithmetic it assumes
- [ ] Passes `uv run sct.py conf` with `configurations/minicloud.yaml` layered last
- [ ] Fits the host: guests x per-guest memory + 2 GiB is under free RAM
- [ ] Shrinks only params the test method actually reads, and says so where non-obvious
- [ ] Keeps keyspace names, consistency levels and ops sets identical to production
- [ ] Sets `nemesis_selector: "not disruptive"` if it runs a nemesis at all
- [ ] Sets `gce_n_local_ssd_disk_db: 1` if it supports the GCE backend
- [ ] Does not re-set anything `configurations/minicloud.yaml` already carries
- [ ] Comments *why* each value is what it is, not just what it is
- [ ] Ships with a runner that exports `SCT_MINICLOUD_ENDPOINT_URL` and asserts the emulator is the target before provisioning
