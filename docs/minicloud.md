# Running SCT against minicloud

[minicloud](https://github.com/scylladb/minicloud) is a local cloud emulator: one container that
serves the EC2 and GCE compute APIs and backs them with real QEMU/KVM virtual machines on the
host. SCT points its AWS/GCP SDKs at it and runs otherwise unchanged - same test-cases, same
provisioning code, same log collection - without real cloud credentials, cost, or quota.

Use it for provisioning-path development, artifact smoke tests, and any test whose value is in
exercising SCT itself rather than real cloud hardware.

## How activation works

`is_minicloud_active()` (`sdcm/utils/minicloud.py`) switches SCT into minicloud mode when any of
these is set, in this order of specificity:

| knob | scope | notes |
|---|---|---|
| `MINICLOUD_DOCKER` | env | the `scripts/` wrappers' switch; also selects the image |
| `AWS_ENDPOINT_URL` / `GCE_ENDPOINT_URL` pointing at localhost | env | direct SDK override |
| `SCT_MINICLOUD_ENDPOINT_URL` | env | **what the Jenkins pipelines set** |
| `minicloud_endpoint_url` | SCT param | the only way a test-case yaml can switch it on |

`SCT_MINICLOUD_DOCKER_IMAGE` (param: `minicloud_docker_image`) does **not** activate anything -
it only selects which image runs. Empty values are ignored everywhere: an exported-but-empty
variable never blanks out a resolved default.

Once active, `tester.py` builds a `MinicloudManager` that preflights, starts the container and
prepares regions by itself. In CI the container is instead started up front by
`hydra start-minicloud` with `keep_alive`, because the pipeline's provision/collect/clean stages
are separate hydra invocations that all need to reach the same live endpoint.

## Configuration

Params (defaults in `defaults/test_default.yaml`, reference in
[configuration_options](./configuration_options.md)):

- `minicloud_docker_image` - image reference. The default is a `master-<sha>` tag, not a
  release: the newest release predates the region-scoping and filtered-`Describe*` support that
  region preparation requires.
- `minicloud_lightweight` (default `true`) - every guest gets 1 vCPU and a fixed amount of RAM,
  ignoring the instance type the test asked for. SCT still validates and reports the requested
  type, so keep it realistic in the yaml.
- `minicloud_lightweight_memory` (default `4GiB`) - per-guest RAM. Scylla needs >1 GiB per shard
  on top of the ~1.7 GiB it reserves for the guest OS, so anything under ~3 GiB fails to boot
  with "memory per shard too low".
- `minicloud_s3_passthrough_buckets` - S3 buckets proxied to real AWS (keystore, job artifacts,
  `downloads.scylladb.com`). This is why minicloud runs still need the `qa-aws-secret-*`
  credentials.

`configurations/minicloud.yaml` is the overlay every minicloud job layers after its test-case
yaml: KMS off (minicloud implements no KMS endpoint), `instance_provision: on_demand` (there is
no spot market to be interrupted by), `ip_ssh_connections: private` (guests live on the host's
userspace switch). `configurations/minicloud/gce.yaml` additionally caps
`gce_n_local_ssd_disk_db: 1` - guests are qcow2-backed and have no NVMe to pass through.

### Memory arithmetic

The whole test must fit in the host's free RAM:

```
(n_db_nodes + n_loaders + n_monitor_nodes) x minicloud_lightweight_memory + ~2 GiB host headroom
```

`preflight_check(params=...)` enforces exactly this before the container starts and prints the
arithmetic when it fails. Without the check, an oversized test dies mid-run as a cgroup OOM kill
(container exit 137) that takes every VM with it and surfaces as a wall of SSH timeouts far from
the cause. `n_db_nodes` is a multi-DC list (`"3 3"`), and is summed.

## Container lifecycle

- One container named `minicloud`, `--network host` (API on `localhost:5000`) but **not**
  `--pid host`: QEMU guests share the container's PID namespace, so `docker rm -f minicloud`
  kills every VM with it. Teardown must therefore run **after** log collection.
- Health probe is EC2 `DescribeVpcs` - an action minicloud implements and serves locally.
- EC2 resources are scoped per region, as on real AWS: a VPC created in one region is invisible
  from another, so `prepare_regions()` sets up every SCT-supported region up front (~2s each).
  `MINICLOUD_AWS_REGION` (comma-separated) narrows that when start-up time matters.
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
the image cache, and AWS credentials for the passthrough buckets. One-time network setup
(the `minicloud0` TUN device carrying `10.127.0.1`) is created by the container's setup script
under sudo, or pre-create it via a boot-time unit and no sudo is needed at run time.

```bash
# start the container and prepare regions (keep_alive - survives across hydra invocations)
./docker/env/hydra.sh start-minicloud -b aws -c test-cases/minicloud-provision-test.yaml

# then run any SCT command against it
export SCT_MINICLOUD_ENDPOINT_URL=http://localhost:5000
./docker/env/hydra.sh run-test artifacts_test --backend aws
```

The `scripts/run-minicloud-*.sh` wrappers bundle these steps for the common local flows
(provision test, artifact test, AWS and GCE variants); `scripts/start-minicloud.sh` is the
container-only entry point. `scripts/run-minicloud-clean-resources.sh` is a local-dev
convenience only - the pipelines use the regular `clean-resources` path.

## Troubleshooting

| symptom | cause |
|---|---|
| container exit 137 mid-test, all VMs unreachable at once | cgroup OOM kill - the test was too big for the host. The preflight prints the required arithmetic; reduce node counts or `minicloud_lightweight_memory`, or use a bigger host |
| container exit 143 | someone ran `docker stop minicloud` |
| `InvalidAMIID.NotFound` on launch | AMI not cached and the container's `--aws-region` differs from where the AMI lives - or the AMI id is wrong |
| `SnapshotNotFound` from `ListSnapshotBlocks` | dev AMI whose snapshot is not shared with the QA account - use a released version |
| "memory per shard too low" in a guest's Scylla log | `minicloud_lightweight_memory` set below ~3 GiB |
| every VM silently unreachable, test proceeds anyway | `minicloud0` was never created and host networking setup only warned - pre-create the device or grant passwordless sudo |
| `clean-resources` reports success but instances survive | the container died and something auto-started a fresh, empty one; check `docker logs minicloud` and the run's `minicloud.log` |

The container's own log is the emulator's view of the run: `docker logs minicloud`, and
`minicloud.log` in the test's logdir.

## Running in Jenkins

See the pipeline documentation - this section is filled in by the pipeline-integration change
that adds the `minicloud: true` opt-in to the regular SCT pipelines.
