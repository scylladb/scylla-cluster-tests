# Sizing Knobs Under minicloud

Every knob that changes what the host has to carry, what it actually costs, and the ones to
leave alone. Defaults live in `defaults/test_default.yaml`; each `minicloud_*` option has an
automatic `SCT_*` environment form (the code reads no bare `MINICLOUD_*` variables).

---

## Cluster Shape — the only thing that really costs memory

| Param | Cost | Notes |
|---|---|---|
| `n_db_nodes` | 1 guest each | Multi-DC list form `"3 3"` is summed. Floor of 3 for RF=3 quorum |
| `n_loaders` | 1 guest each | Floor of 1; a second loader buys throughput a 1-vCPU db cannot use |
| `n_monitor_nodes` | 1 guest each | 0 only when the test never reads Prometheus |
| `n_test_oracle_db_nodes` | 1 guest each, **only for `db_type` `mixed_scylla` or `mixed_cassandra`** | Gemini's oracle cluster. Defaults to 1 but is provisioned only for those two db types, so an ordinary run is not charged for it. `mixed_cassandra` builds it as a `CassandraAWSCluster` rather than through `_create_oracle_cluster()` |
| `n_db_zero_token_nodes` | 1 guest each | Zero-token topology tests |
| `n_vector_store_nodes` | 1 guest each | Vector-store tests |
| `cluster_target_size` | 1 guest per node above `n_db_nodes` | **This**, not `n_db_nodes`, is what the memory gate budgets for a growing test |
| `add_node_cnt` | none directly | How many nodes are added per step; smaller means more steps, longer run |

`GUEST_NODE_COUNT_PARAMS` and `ORACLE_GUEST_DB_TYPES` in `sdcm/utils/minicloud/preflight.py`
are the authority if they ever diverge from this table.

Everything else on this page changes how long the run takes or whether it works at all — not
how much RAM it needs.

---

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

**The ~2 GiB headroom covers steady state, not a first run.** Measured on a 5-guest run whose
formula said 22 GiB: the container peaked at **25.6 GiB**, with the `minicloud` process alone
holding ~8 GiB RSS while it converted an uncached image. Each SCT run also starts its own
`*-vector` log-shipper container. A first run against a version needs real slack above the
formula; later runs against the cached image do not. (Steady state during the stress phase was
not measured, so treat this as a cold-cache peak rather than a standing requirement.)

### When the Budget Does Not Fit

A development laptop is rarely short of RAM because the test is too big — it is short because
something else is holding it. Find the holder before shrinking the test further, and reclaim
idle sessions and the browser before anything else. **Swap does not help:** `MemAvailable`,
what the gate measures, excludes it, and a guest that swaps produces timeouts and latency
artifacts that read as findings. See
"Making room on the host" in the triage reference.

---

## Floors You Cannot Go Below

| Value | Floor | Why |
|---|---|---|
| `n_db_nodes` | 3 | RF=3 keeps quorum with one node down; below that a nemesis or a rolling restart breaks the cluster |
| `n_loaders` | 1 | No loader, no workload |
| `n_monitor_nodes` | 1 when the test reads Prometheus | `test_rolling_upgrade` gates on `metric_has_data()`; `GrowClusterTest` reconfigures monitoring per added node |
| `minicloud_lightweight_memory` | ~3 GiB | Scylla reserves ~1.7 GiB for the guest OS and needs 1 GiB per shard; below this it fails to boot with "memory per shard too low" |
| `gce_n_local_ssd_disk_db` | 1 on GCE | Guests get qcow2-backed disks, no NVMe passthrough; the `gce_config.yaml` default of 4 cannot be served |
| guest architecture | x86_64, **every role** | minicloud does not support arm64 guests. The loader is the trap: its catalog default is `arch: arm64` by construction, so a test-case pinning only the db role resolves `c7g.xlarge` (AWS) or `n4a-highcpu-4` (GCE). Pin each role to an x86_64 type; the image follows, because SCT derives the arch from the instance type |

A test that genuinely needs more than 3 db nodes is not a candidate for a 16 GiB laptop — move
it to a lab host rather than dropping below a floor.

---

## Per-Guest Budget

| Param | Default | Effect |
|---|---|---|
| `minicloud_lightweight` | `true` | Every guest gets the fixed vCPU/RAM below, **ignoring the instance type the test asked for** |
| `minicloud_lightweight_memory` | `4GiB` | Per guest. Floor ~3 GiB: Scylla reserves ~1.7 GiB for the guest OS and needs 1 GiB per shard, below which it fails to boot with "memory per shard too low" |
| `minicloud_lightweight_vcpus` | `1` | One Scylla shard per vCPU. Multiplies across every guest — raise only on a host with cores to spare |

Because lightweight mode ignores instance types, `instance_type_db`, `instance_type_loader`
and the `gce_instance_type_*` keys only have to **exist in the emulated catalog and pass SCT
validation**. Keep them realistic (`i4i.large`, `n2-standard-2`) and spend no time on them.

Pin the GCE keys even though nothing forces you to. `backend_required_params["gce"]` does list
all three, but the check never fires: sizing resolution runs in `SCTConfiguration.__init__`,
while the required-params check runs later in `verify_configuration()` — by then the sizing
defaults have filled every role and validation passes. What fails instead is the emulator
rejecting the resolved type at `instances.insert`, long after `sct.py conf` said the config was
fine.

"Realistic" is not enough on its own — the emulator serves a **fixed catalog** and rejects
anything outside it, so a perfectly valid real-cloud type can still fail at launch. Check
against the running emulator rather than guessing:

```bash
aws --endpoint-url http://localhost:5000 ec2 describe-instance-types \
  --query 'InstanceTypes[].InstanceType' --output text | tr '\t' '\n' | sort
CLOUDSDK_API_ENDPOINT_OVERRIDES_COMPUTE=http://localhost:5000/ \
  gcloud compute machine-types list --zones us-east1-b
```

This bites hardest on roles the test-case does not pin. A base test-case that sets `sizing_db`
resolves the *db* type into the catalog, while the loader and monitor fall through to defaults
that may not be in it. Pin the roles the base leaves unpinned.

**minicloud does not support arm64 guests.** Being in the catalog makes a type requestable,
not runnable: guests run under KVM rather than emulation, so an arm64 type (`i8g.*`,
`im4gn.*`, `c7g.*`, `c6g.*`) cannot boot on an x86_64 host however small it is. Performance
test-cases pin arm64 db types routinely.

The override is one key. SCT derives the architecture from the instance type and resolves the
image to match, so changing the type is enough — do not also hand-pick an AMI id:

```yaml
# the base pins i8g.4xlarge (arm64); minicloud has no arm64 guests, and lightweight mode
# ignores the size anyway, so any x86_64 type in the catalog will do
instance_type_db: 'i4i.4xlarge'
```

Confirm it took effect before running anything:

```bash
uv run sct.py get-db-arch -b aws '["<test-case>","<overlay>","configurations/minicloud.yaml"]'
```

It prints `arm64` or `x86_64`. Check every role the test-case pins, not just the db.

---

## Emulator Container

| Param | Default | Effect |
|---|---|---|
| `minicloud_container_memory` | empty (no limit) | A docker `--memory` cap. Setting it also makes **the cap**, not host free memory, the budget the preflight gate measures against |
| `minicloud_container_cpus` | empty | A docker `--cpus` cap |
| `minicloud_skip_memory_check` | `false` | Disables the preflight gate. Price: an oversized run dies as a cgroup OOM kill (exit 137) that takes every guest with it |
| `minicloud_state_dir` | `~/.cache/minicloud` | Image cache, per-instance disks, `minicloud.log`. Tens of GiB — never delete casually |
| `minicloud_container_name` | `minicloud` | One container per host; the name is a host singleton |
| `minicloud_keep_alive` | `false` | Keep the container after the test, so guests survive for post-mortem |
| `minicloud_regions` | empty (all) | Narrows AWS region preparation (~2s each) at start-up |
| `minicloud_docker_image` | empty | Overrides the renovate-managed default. **Selecting an image does not activate minicloud** |

A reused container keeps the sizing it was started with — the docker caps are cgroup limits
fixed at `docker run`. Changing `minicloud_lightweight_memory` and rerunning against a live
container gets you the *previous* value; SCT detects the drift and reports it.

---

## Disks and Images

| Param | Under minicloud | Why |
|---|---|---|
| `gce_n_local_ssd_disk_db` | `1` | Guests get qcow2-backed disks, no NVMe passthrough. The `gce_config.yaml` default of `4` cannot be served |
| `use_preinstalled_scylla` | `true` where possible | Booting the image skips a repo install, which is slow on emulated storage |
| `scylla_version` (AWS path) | a **released** version | Dev AMIs (`master:latest`) have snapshots that are not shared with the QA account, so the disk build fails with `SnapshotNotFound`. The GCE path is unaffected |
| `root_disk_size_runner` | `160` | CI only — the runner's disk holds the OS, docker, the hydra image, the guest image cache and the test data |

---

## Stress Volume

Shrink these, never the shape around them:

| Knob | Where | Typical shrunk value |
|---|---|---|
| `n=` and `-pop seq=1..N` | inside `cassandra-stress` commands | 100k-300k rows instead of 10-20M |
| `threads=` | inside `cassandra-stress` commands | 10 instead of 200-1000 |
| `duration=` | inside stress commands | minutes instead of hours |
| `-partition-count`, `-clustering-row-count`, `-concurrency` | inside `scylla-bench` commands | tens instead of thousands |
| `cassandra_stress_population_size` | config key | 50000 instead of the 1M default |
| `cassandra_stress_threads` | config key | 10 instead of the 1000 default |

`threads=1000` against a 1-vCPU guest does not produce more load — it produces client-side
timeouts and a test that fails for a reason unrelated to the change under test.

---

## Durations

| Param | Effect |
|---|---|
| `test_duration` | The run's wall-clock budget in minutes. SCT kills the run when it expires, so it must cover the shrunk workload plus provisioning, which is slow on emulated storage |
| `test_duration`, first run | **Budget for the image, once.** On a measured first run, 33 of 60 minutes went to image export plus a 3.4 GiB qcow2 download before the first stress command started. Later runs against the same version skip all of it. A `test_duration` sized for the steady-state run kills a correct overlay on its first outing |
| `test_duration` in `GrowClusterTest` | Does double duty: `get_stress_cmd()` uses it as the stress duration, so it also decides how long the load runs |

Shrinking the workload without shrinking `test_duration` wastes wall-clock; shrinking
`test_duration` below the workload it wraps kills the run mid-flight.

---

## Nemesis

| Param | Under minicloud |
|---|---|
| `nemesis_selector` | **Add** `not disruptive`, never replace. Terminate/reboot/stop exercise emulated lifecycle paths that are not all implemented, but a base selector usually protects something the test asserts on. The field takes a boolean expression: `'not disruptive and not ModifyTableCompactionMonkey'` |
| `nemesis_class_name` | `SisyphusMonkey` is the house choice; the selector does the filtering |
| `nemesis_interval` | 1-5 minutes, so a short run sees more than one disruption |
| `nemesis_during_prepare` | `false` for grow tests — the nemesis starts after the cluster reaches its target size |

---

## Not Handled by the Shared Overlay — Set Them Yourself

`configurations/minicloud.yaml` covers what *every* run needs. A test-case can still ask for
something the emulator does not implement:

| Param | Set to | Why |
|---|---|---|
| `instance_type_db` | an x86_64 type | When the base pins arm64, see above |
| `gce_instance_type_loader` / `_monitor` | a catalog type | When the base pins only the db role, via `sizing_db` |

---

## Already Handled — Do Not Re-Set

`configurations/minicloud.yaml` carries all of these. Repeating them in an overlay adds noise
and a second place to update when the emulator grows the missing feature:

`enterprise_disable_kms`, `enable_kms_key_rotation`, `instance_provision`,
`ip_ssh_connections`, `force_run_iotune`, `fallback_to_next_availability_zone`,
`fallback_to_next_region`, `enable_kernel_panic_checker`, `use_placement_group`,
`use_capacity_reservation`, `append_scylla_yaml: {developer_mode: true}`.

The last two matter most to anyone downscaling a performance test-case, which sets both.
Neither placement groups nor capacity reservations exist in minicloud's EC2 surface, and the
emulator fails closed rather than forwarding an action it does not implement. Both are checked
by `validate_minicloud_params()`, so a run that misses the overlay is refused at startup
instead of dying in provisioning.

Note that `append_scylla_yaml` **merges** with the default dict rather than replacing it, so
an overlay adding its own keys there does not lose `developer_mode`.

---

## CI Only — Irrelevant to a Local Run

These matter when the run happens on a cloud sct-runner that hosts the guests, not on your
laptop:

| Param | Constraint |
|---|---|
| `instance_type_runner` | Must support nested virtualization: AWS `c8i`/`m8i`/`r8i`, GCE `N1`/`N2`/`N2D`/`C2`/`C2D`/`C3`/`C3D`/`M1`-`M3`. The default runner types cannot boot a single guest |
| `root_disk_size_runner` | `160`, for the guest-image cache |

`configurations/minicloud/aws.yaml` and `gce.yaml` set both, sized for a **one-guest** run. A
multi-guest test that includes one of those overlays must set its own `instance_type_runner`
**after** it, or not include the overlay at all.
