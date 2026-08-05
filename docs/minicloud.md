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

### Guest network ranges

The emulated guest networks deliberately do NOT share CIDRs with any real cloud network, and
every guest lands inside `10.160.0.0/11`:

- **AWS**: when minicloud is active, `AwsRegion` shifts the region index by 160, so eu-west-1's
  emulated VPC is `10.164.0.0/16` instead of the real `10.4.0.0/16` (all indexes land in
  `10.160.0.0/16 .. 10.175.0.0/16`).
- **GCE**: `prepare_gce_network()` pre-creates the emulated `qa-vpc` in custom mode with one
  explicit subnet per supported region (`10.176.0.0/16 .. 10.179.0.0/16`). Without it,
  minicloud emulates GCE auto-mode and hands out /20s from `10.128.0.0/9` - unroutable from
  the host and inside the real GCE VPC space a runner lives in.

SCT passes exactly the `10.160.0.0/11` range (plus the emulator's default-VPC `172.31.0.0/16`)
to `minicloud-setup.sh` via its `MINICLOUD_VPC_ROUTES` override (scylladb/minicloud#187)
instead of the script's historical blanket `10.0.0.0/8`. Both halves exist for the same
reason: the host running the guests may itself live inside a real cloud VPC - an sct-runner
always does - where an equal emulated CIDR sends guest traffic out `eth0` instead of the TUN,
and a `10/8` route black-holes the QA infra (Argus, argus-proxy). A host still carrying the
old blanket route is reconfigured on the next start; an image whose setup script predates the
override fails the start with an upgrade message rather than 20 minutes later as Argus
connect-timeouts. Constants: `MINICLOUD_REGION_INDEX_OFFSET`, `MINICLOUD_GCE_*`,
`MINICLOUD_HOST_VPC_ROUTES` in `sdcm/utils/minicloud/config.py`.

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

`minicloud: true` in a jenkinsfile is the whole opt-in; the regular pipelines
(`artifactsPipeline`, `longevityPipeline`, `rollingUpgradePipeline`) do the rest. There is no
dedicated minicloud pipeline - there used to be a fork, and it drifted.

### The sct-runner topology

Every minicloud job provisions a **nested-virtualization sct-runner** and runs there: the guests,
the emulator and the test process all live on that one instance, so the Jenkins builder only
drives hydra. Nested virtualization narrows the instance families - c8i/m8i/r8i on AWS,
N1/N2/C2/C3 on GCE (E2 has none) - so the runner is sized by `instance_type_runner`:
longevity/rolling-upgrade test-cases set it themselves, and the artifacts jobs layer
`configurations/minicloud/aws.yaml` or `configurations/minicloud/gce.yaml`, which also raise
`root_disk_size_runner` for the guest-image cache. Teardown is just the runner's termination -
'Clean SCT Runners' - which takes the container and every guest with it, so it runs after log
collection.

Running minicloud on a long-lived KVM Jenkins agent instead is a follow-up; it needs agent
validation, workspace reclaim and a local teardown path that this PR deliberately leaves out.

### The jobs

Under `jenkins-pipelines/oss/minicloud/`:

| job | covers |
|---|---|
| `minicloud-artifact-ami` | AWS AMI boot, preinstalled Scylla |
| `minicloud-artifact-gce-image` | GCE image boot, preinstalled Scylla |
| `minicloud-artifact-deb-aws` | `.deb` install onto stock Ubuntu, AWS backend |
| `minicloud-artifact-rpm-gce` | `.rpm` install onto stock Rocky, GCE backend |
| `minicloud-provision-test` | 3-node provision + write smoke + non-disruptive nemesis |
| `longevity-minicloud-10gb-1h` | a regular longevity test-case end to end |

Job knobs: `minicloud_docker` selects the image for one run
(`-s minicloud_docker=ghcr.io/scylladb/minicloud:master-<sha>` via `staging_trigger.py`) - the
pipeline exports it as `SCT_MINICLOUD_DOCKER_IMAGE` for the whole build, because a stage that
cannot see the override restarts the container to the default image (and in longevity topology
that restart would kill the already-provisioned guests). Keep any new parameter name distinct
from the env var it feeds by more than case: Jenkins' `EnvVars` map is case-insensitive, so a
parameter named exactly like its variable swallows the export. A
per-run `extra_environment_variables` override always beats the jenkinsfile's defaults.

#### Sizing

The guest and container budget is the one thing a test-case yaml cannot settle, because the same
test wants a smaller budget on a lab machine than on a CI agent. Each knob is an ordinary SCT
config option with a default in `defaults/test_default.yaml`; the pipeline layer only overrides it.

| Job parameter | SCT config option | Empty means |
|---|---|---|
| `minicloud_lightweight_memory` | `minicloud_lightweight_memory` | keep the yaml value (4GiB) |
| `minicloud_lightweight_vcpus` | `minicloud_lightweight_vcpus` | keep the yaml value (1) |
| `minicloud_container_memory` | `minicloud_container_memory` | no docker limit on the container |
| — (jenkinsfile / `extra_environment_variables` only) | `minicloud_container_cpus` | no docker limit |
| — (jenkinsfile / `extra_environment_variables` only) | `minicloud_state_dir` | `~/.cache/minicloud` |
| — (jenkinsfile / `extra_environment_variables` only) | `minicloud_container_name` | `minicloud` |

`startMinicloud.exportSizing()` resolves each one as **`extra_environment_variables` > job
parameter > jenkinsfile `pipelineParams`**, and exports it build-wide. Build-wide matters for the
same reason the image does: `start-minicloud`, `provision-resources`, `run-test`, `collect-logs`
and `clean-resources` are separate hydra invocations that each rebuild `MinicloudConfig`, so an
invocation that cannot see `minicloud_container_name` or `minicloud_state_dir` looks for the
container under the default name or in the wrong directory - teardown then leaves the real
container running and collection finds no logs.

Empty stays unset at every level, so a string parameter nobody filled in does not override the
yaml with nothing. The three knobs without a job parameter are agent properties rather than
per-build choices; set them in the jenkinsfile, or per run via `extra_environment_variables`.

Setting `minicloud_container_memory` also moves the preflight guest-memory gate onto that cap
instead of the host's free memory - the cgroup OOM killer enforces the cap, so measuring the host
would pass a test the cap then kills.

### Triggering from staging_trigger.py

`generate` and `trigger` work on these jobs like any other. One gotcha: the `artifacts` preset
injects `scylla_version: master:latest`, which **overrides the jenkinsfile** - and dev AMIs do
not work on the AWS path (snapshot sharing, see above). Pass a released version explicitly for
the preinstalled-image jobs:

```bash
python staging_trigger.py -f scylla-staging/<user> trigger -b <branch> \
    -s scylla_version=2026.3.0-rc0 oss/minicloud/minicloud-artifact-ami-test
```

The deb/rpm jobs are fine with `master:latest` - they install over the network from
`downloads.scylladb.com`.
