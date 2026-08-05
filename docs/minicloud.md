# Running SCT against minicloud

[minicloud](https://github.com/scylladb/minicloud) is a local cloud emulator: one container that
serves the EC2 and GCE compute APIs and backs them with real QEMU/KVM virtual machines on the
host. SCT points its AWS/GCP SDKs at it and runs otherwise unchanged - same test-cases, same
provisioning code, same log collection - without allocating real cloud compute or paying for it.
Real credentials are still required for the passthrough services: S3 (keystore, job artifacts,
downloads), and on the GCE path GCS/Cloud Build for image export.

Use it for provisioning-path development, artifact smoke tests, and any test whose value is in
exercising SCT itself rather than real cloud hardware.

## How activation works

`is_minicloud_active()` (`sdcm/utils/minicloud/`) switches SCT into minicloud mode when any of
these is set, in this order of specificity:

| knob | scope | notes |
|---|---|---|
| `AWS_ENDPOINT_URL` / `GCE_ENDPOINT_URL` pointing at localhost | env | direct SDK override |
| `SCT_MINICLOUD_ENDPOINT_URL` | env | **what the Jenkins pipelines and `scripts/` wrappers set** |
| `minicloud_endpoint_url` | SCT param | the only way a test-case yaml can switch it on |

Selecting a docker image activates **nothing**: `minicloud_docker_image` (explicit override)
and `stress_image.minicloud` (renovate-managed default from
`defaults/docker_images/minicloud/values_minicloud.yaml`) only choose which image runs.
Redirecting a run and picking an image are separate decisions. Empty values are ignored
everywhere: an exported-but-empty variable never blanks out a resolved default.

Once active, `tester.py` builds a `MinicloudManager` that preflights, starts the container and
prepares regions by itself. In CI the container is instead started up front by
`hydra start-minicloud` with `keep_alive`, because the pipeline's provision/collect/clean stages
are separate hydra invocations that all need to reach the same live endpoint.

## Configuration

`configurations/minicloud.yaml` is **mandatory** for every minicloud run - layer it after the
test-case yaml. It is the single delivery mechanism for the params the emulator requires:
KMS off (minicloud implements no KMS endpoint), `instance_provision: on_demand` (no spot
market), `ip_ssh_connections: private` (guests live on the host's userspace switch),
`force_run_iotune: false`, AZ/region fallbacks off, kernel-panic checker off, and
`developer_mode: true` via `append_scylla_yaml`. `preflight_check()` fails fast with the exact
missing values when the overlay is not in the config list. Env exports cannot substitute for
it: `SCT_*` variables set after `SCTConfiguration` is built never reach params.

Params (defaults in `defaults/test_default.yaml`, reference in
[configuration_options](./configuration_options.md)):

- `stress_image.minicloud` - image reference; the default tag lives in
  `defaults/docker_images/minicloud/values_minicloud.yaml` so renovate bumps it.
- `minicloud_lightweight` (default `true`) - every guest gets 1 vCPU and a fixed amount of RAM,
  ignoring the instance type the test asked for. SCT still validates and reports the requested
  type, so keep it realistic in the yaml.
- `minicloud_lightweight_memory` (default `4GiB`) - per-guest RAM. Scylla needs >1 GiB per shard
  on top of the ~1.7 GiB it reserves for the guest OS, so anything under ~3 GiB fails to boot
  with "memory per shard too low".

S3 passthrough buckets (keystore, job artifacts, `downloads.scylladb.com`) are configured via
`minicloud_s3_passthrough_buckets`; this is why minicloud runs still need the
`qa-aws-secret-*` credentials. Every knob is a documented `minicloud_*` config option with its
automatic `SCT_*` env form - the code reads no bare `MINICLOUD_*` environment variables.

GCE runs on qcow2-backed disks with no NVMe passthrough - cap local SSDs in the run's config
(e.g. `gce_n_local_ssd_disk_db: 1`) when the test-case yaml asks for more.

### Memory arithmetic

The whole test must fit in the host's free RAM:

```
(n_db_nodes + n_loaders + n_monitor_nodes) x minicloud_lightweight_memory + ~2 GiB host headroom
```

`preflight_check(params=...)` enforces exactly this before the container starts and prints the
arithmetic when it fails. Without the check, an oversized test dies mid-run as a cgroup OOM kill
(container exit 137) that takes every VM with it and surfaces as a wall of SSH timeouts far from
the cause. `n_db_nodes` is a multi-DC list (`"3 3"`), and is summed.

The arithmetic is deliberately conservative. `minicloud_skip_memory_check: true` (or
`SCT_MINICLOUD_SKIP_MEMORY_CHECK=true`) disables the
gate for development on a machine you know can handle the workload - at the price of the
exit-137 failure mode above if you were wrong.

## Container lifecycle

- One container named `minicloud`, `--network host` (API on `localhost:5000`) but **not**
  `--pid host`: QEMU guests share the container's PID namespace, so `docker rm -f minicloud`
  kills every VM with it. Teardown must therefore run **after** log collection:
  `collect-logs` never restarts a dead container (that would destroy the exit code and logs it
  came to collect), and `clean-resources` fails closed when the emulator is unreachable rather
  than auto-starting a fresh, empty one that would make cleanup "succeed".
- AWS credentials are passed to the container as name-only `--env` flags (values never appear
  in argv or logs), and `docker inspect` snapshots are credential-redacted before they land in
  the collected logdir.
- Health probe is EC2 `DescribeVpcs` - an action minicloud implements and serves locally.
- An unexpected container death mid-test publishes an ERROR event with the exit code and a
  state snapshot, even in CI (`keep_alive` controls teardown, not death reporting) - any SSH
  failure after that event is a consequence, not the cause.
- EC2 resources are scoped per region, as on real AWS: a VPC created in one region is invisible
  from another, so `prepare_regions()` sets up every SCT-supported region up front (~2s each).
  `minicloud_regions` (comma-separated) narrows that when start-up time matters.
- Region-less API calls land in the container's own `--aws-region`. That is not cosmetic:
  RunInstances validates a not-yet-cached AMI against real AWS in *that* region, so a mismatch
  fails with `InvalidAMIID.NotFound`. Scylla AMIs live in `eu-west-1`, the default.
- Guest disks for AWS AMIs are built by reading the AMI's snapshot over the EBS direct API and
  cached under `~/.cache/minicloud/amis` (tens of GiB, tens of minutes to build - never delete
  it casually). **Dev AMIs (`master:latest`) do not work on the AWS path today**: their
  snapshots are not shared with the QA account, so `ListSnapshotBlocks` returns
  `SnapshotNotFound` and the VM never boots. Use a released version. The GCE path is
  unaffected - dev images cache fine there.

## What minicloud does not implement

- **KMS** (AWS or GCP) - hence `enterprise_disable_kms: true` in the overlay. When minicloud
  grows KMS support, only the overlay changes; no pipeline knows about KMS.
- **Spot** - `instance_provision` must stay `on_demand`.
- **Local SSDs / NVMe passthrough** - guests get qcow2-backed disks.
- Anything not in the emulated API surface fails closed with an explicit error rather than
  being silently ignored - by design, on both sides.

## Running locally

Host prerequisites: KVM (`/dev/kvm` writable by your user), docker, ~80 GiB free in `$HOME` for
the image cache, and AWS credentials for the passthrough buckets (GCP credentials additionally
for the GCE path's image export). One-time network setup (the `minicloud0` TUN device carrying
`10.127.0.1`) is created by the container's setup script under sudo, or pre-create it via a
boot-time unit and no sudo is needed at run time. A networking-setup failure aborts the start -
guests without `minicloud0` would pass API health checks and then be unreachable over SSH.

```bash
# start the container and prepare regions (keep_alive - survives across hydra invocations)
./docker/env/hydra.sh start-minicloud -b aws -c test-cases/minicloud-provision-test.yaml -c configurations/minicloud.yaml

# then run any SCT command against it
export SCT_MINICLOUD_ENDPOINT_URL=http://localhost:5000
./docker/env/hydra.sh run-test artifacts_test --backend aws
```

`scripts/run-minicloud-test.sh` is the single local entry point - it starts the container via
`sct.py start-minicloud` and runs the chosen flavor (`-f ami|repo|provision|upgrade`) on the chosen
backend (`-b aws|gce`), directly or through hydra (`-m direct|hydra`), layering
`configurations/minicloud.yaml` for you.

The `upgrade` flavor runs `upgrade_test.UpgradeTest.test_rolling_upgrade` - the same test the
`rollingUpgradePipeline` runs in CI - and needs a target to upgrade *to* on top of the base version:

```bash
SCT_SCYLLA_VERSION=2026.1 \
SCT_NEW_SCYLLA_REPO=http://downloads.scylladb.com/deb/unified/2026.2/scylladb-2026.2/scylla.list \
  scripts/run-minicloud-test.sh -f upgrade -b aws
```

`SCT_NEW_VERSION` works instead on the GCE backend; `sct_config` rejects it for AWS AMIs, and the
script fails on that up front rather than after provisioning a cluster. The production test-case is
6x `i4i.2xlarge` with 20M-row workloads, so the flavor layers
`configurations/minicloud/rolling-upgrade.yaml` on top of it: 3 db nodes (the floor for keeping
quorum with one node down at a time), 1 loader, 1 monitor - the monitor is load-bearing, because the
test queries Prometheus to confirm the write workload landed before it starts upgrading - and every
workload cut to what a 1-vCPU guest can serve. That is 5 guests x `minicloud_lightweight_memory`
= 20GiB plus host headroom, so raise the per-guest memory only if the host has room for 5x the
increase.

It is an overlay rather than a separate test-case copy on purpose: `test_rolling_upgrade` reads a
dozen stress params, and a copy would silently miss any new one the real test-case grows. Passing
your own `SCT_TEST_CASE` drops the overlay, since shrinking someone else's yaml by name would be
wrong.

The `scale` flavor works the same way, running `grow_cluster_test.GrowClusterTest.test_grow_x_to_y`
over `test-cases/scale/scale-cluster.yaml` - the cluster-growth test proper, which starts at
`n_db_nodes` and adds nodes under load until it reaches `cluster_target_size`:

```bash
SCT_SCYLLA_VERSION=2026.2 scripts/run-minicloud-test.sh -f scale
```

**What survives the shrink is the shape of the test, not the scale.** Production is 15 db nodes
growing to 25 - 28 guests x `minicloud_lightweight_memory` = 112GiB at the peak, which no single
host has. `configurations/minicloud/scale-cluster.yaml` cuts that to 3 -> 4 nodes and sizes the load
down to what a 1-vCPU guest can serve, keeping what the test exists for: the add-node path
(bootstrap, streaming, monitoring reconfigured per node) under load, with a nemesis once the target
size is reached. It tells you nothing about behaviour at scale, so do not read performance or
scalability conclusions off a local run. Grow further on a bigger host with `SCT_N_DB_NODES` /
`SCT_CLUSTER_TARGET_SIZE` - every added node costs another `minicloud_lightweight_memory`.

One trap worth knowing if you tune that overlay: `test_grow_x_to_y` does **not** read the base
test-case's `stress_cmd` or `prepare_write_cmd`, and `run_fullscan` is only wired up in
`longevity_test`. `GrowClusterTest.get_stress_cmd()` builds its own cassandra-stress command from
`cassandra_stress_population_size`, `cassandra_stress_threads` and `test_duration`, so those three
are what actually size the load - the defaults are 1M rows at 1000 threads. `test_duration` doubles
as the stress duration, so it also decides how long a local run takes.

Note that the memory gate sizes a growing cluster by `cluster_target_size`, not `n_db_nodes`:
budgeting the initial cluster would pass and then let the run die at the exact moment it adds the
node nobody accounted for.
`scripts/run-minicloud-clean-resources.sh` is a local-dev convenience only - the pipelines use
the regular `clean-resources` path.

## Troubleshooting

| symptom | cause |
|---|---|
| preflight fails with "missing its parameter overlay" | `configurations/minicloud.yaml` was not layered after the test-case yaml |
| container exit 137 mid-test, all VMs unreachable at once | cgroup OOM kill - the test was too big for the host. The preflight prints the required arithmetic; reduce node counts or `minicloud_lightweight_memory`, or use a bigger host |
| container exit 143 | someone ran `docker stop minicloud` |
| `InvalidAMIID.NotFound` on launch | AMI not cached and the container's `--aws-region` differs from where the AMI lives - or the AMI id is wrong |
| `SnapshotNotFound` from `ListSnapshotBlocks` | dev AMI whose snapshot is not shared with the QA account - use a released version |
| "memory per shard too low" in a guest's Scylla log | `minicloud_lightweight_memory` set below ~3 GiB |
| start aborts with "could not extract minicloud-setup.sh" or "minicloud-setup.sh failed" | host networking could not be configured - pre-create the `minicloud0` device or grant passwordless sudo |
| `clean-resources` refuses to run: "minicloud is not reachable" | the container died; collect its logs (`docker logs minicloud`) - cleanup against a fresh emulator would only pretend to succeed |

The container's own log is the emulator's view of the run: `docker logs minicloud`, and
`minicloud.log` in the test's logdir.

## Running in Jenkins

See the pipeline documentation - this section is filled in by the pipeline-integration change
that adds the `minicloud: true` opt-in to the regular SCT pipelines.
