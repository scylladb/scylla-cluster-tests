# SCT-901 — simulating a bare-metal configuration on AWS

Runbook for the experiment asked for in
[SCT-901](https://scylladb.atlassian.net/browse/SCT-901?focusedCommentId=97937):

> create an instance with Fedora (latest version) on AWS, collect the addresses,
> install Scylla with RPM and try to run artifact test with baremetal backend

Scripts: [`scripts/baremetal-simulation/`](../../scripts/baremetal-simulation/).
Everything this runbook produces outside that directory (the node JSON, the
generated test case, the `distro.py` patch) is deliberately uncommitted local
state, removed again by step 9.

## 1. What "simulate bare metal" means here

We do **not** use the `aws` backend. The hosts are plain EC2 instances provisioned
by `01_launch_hosts.py`, outside SCT. SCT is then pointed at them through the
`baremetal` backend exactly as it would be pointed at a Spider machine: it SSHes
in, installs ScyllaDB and runs the test, and never creates, tags, reboots or
destroys the host.

That gives a cheap, repeatable rehearsal of the whole bare-metal path — KeyStore
JSON → `PhysicalMachineNode` → `clean_scylla()` → `_scylla_install()` →
`scylla_setup` → artifact checks → log collection — without booking hardware, and
it produces the first data point for the drift question SCT-901 has to answer.

### Code this relies on

| What | Where |
| --- | --- |
| Backend classes | `sdcm/cluster_baremetal.py` — `PhysicalMachineNode`, `PhysicalMachineCluster`, `ScyllaPhysicalCluster`, `LoaderSetPhysical`, `MonitorSetPhysical` |
| Cluster wiring | `sdcm/tester.py:2363` `get_cluster_baremetal()`, dispatched at `sdcm/tester.py:2997` |
| Node JSON lookup | `sdcm/keystore.py:383` `get_baremetal_config()` — reads `./<name>.json` from the CWD **before** the `scylla-qa-keystore` S3 bucket |
| Required params | `sdcm/sct_config/defaults.py:126` — `s3_baremetal_config` + `user_credentials_path` |
| Backend defaults | `defaults/baremetal_config.yaml` |
| Install axis | `sdcm/cluster.py:6447` `_scylla_install()` → `clean_scylla()`, then `install_mode` or `unified_package` |
| RPM install | `sdcm/cluster.py:2845` `install_scylla()` — rhel-like branch |
| Disk detection | `sdcm/cluster.py:3060` `detect_disks(nvme=True)` — needs an unpartitioned `/dev/nvmeXnY` |
| `scylla_setup` | `sdcm/cluster.py:3184`; the bare-metal override that swallows "already run" is in `sdcm/cluster_baremetal.py` |
| Sizing skipped | `sdcm/sct_config/config.py:167` `_SIZING_SKIP_BACKENDS` contains `baremetal` |
| Version-check hole | `sdcm/sct_config/config.py:2251` `_check_version_supplied()` exempts `baremetal` from requiring `scylla_repo` |
| Log collection | `sdcm/logcollector.py:2103` `get_baremetal_instances_by_testid()` |

### Traps — read before starting

1. **Fedora > 36 is unknown to SCT.** `sdcm/utils/distro.py:43` declares
   `("FEDORA", "fedora", ["34", "35", "36"], DistroBase.RHEL)`. On Fedora 44 the
   enum resolves to `Distro.UNKNOWN`, `is_rhel_like` is `False`, and
   `install_scylla()` falls into the **Debian/apt** branch and dies on an RPM host.
   Step 3 patches it locally; the patch deserves its own PR plus a unit test.
2. **`db_nodes_public_ip` / `loaders_public_ip` / `monitor_nodes_*` are dead ends.**
   They exist in `sdcm/sct_config/mixins/baremetal.py` but `get_cluster_baremetal()`
   never reads them — only the JSON named by `s3_baremetal_config`. The existing
   `test-cases/performance/perf-regression-throughput-baremetal-5gb.yaml` sets them,
   which is misleading.
3. **`PhysicalMachineNode.reboot()` raises `NotImplementedError`.** Nothing in the
   artifact-test setup path calls it, but SELinux must therefore be dealt with up
   front — the launcher sets it permissive from user-data.
4. **`SCT-2-sg` only allows traffic inside itself.** A runner outside the VPC cannot
   SSH in; step 1 adds a second, IP-scoped security group.
5. **`logs_transport` defaults to `vector`** (`defaults/test_default.yaml:79`), which
   makes the DB node push logs *to* the runner — hopeless from a laptop behind NAT.
   The generated test case uses `ssh`.
6. **`use_mgmt: true` and `run_scylla_doctor: true` are global defaults.** Both are
   turned off for the first run so a Manager/Doctor packaging problem on Fedora
   cannot mask the actual result.

## 2. Prerequisites

- AWS credentials for the target region in `~/.aws`.
- `~/.ssh/scylla_test_id_ed25519` — the private half of the EC2 key pair
  `scylla_test_id_ed25519` that SCT already imports into every prepared region.
- SCT infrastructure in the region (`SCT-2-vpc`, `SCT-2-sg`, the key pair). If a
  lookup fails: `hydra prepare-regions -c aws -r <region>`.
- Docker + `hydra`.
- No AWS CLI needed — the scripts use boto3 from the repo venv (`uv run python ...`).

Defaults (all in `scripts/baremetal-simulation/config.env`):

| Choice | Value | Why |
| --- | --- | --- |
| Region / AZ | `eu-west-1` / `eu-west-1a` | SCT VPC, SG and key pair already there; the example bare-metal jenkinsfile also uses eu-west-1 |
| OS | latest stable Fedora Cloud AMI, resolved at launch (owner `125523088429`) | as asked on the ticket; Rawhide/ELN/Prerelease are filtered out |
| SSH user | `fedora` | Fedora cloud image default |
| Instance type | `i4i.large` (2 vCPU, 16 GiB, 1×468 GB NVMe) | a **real local NVMe** disk, so `detect_disks()` and `scylla_setup` behave as on a physical host |
| Root volume | 40 GiB gp3 | the stock Fedora cloud root (~6 GiB) is too small |
| Phase A | 1 DB node, 0 loaders, 0 monitors | the artifact test only needs the DB node |
| Install | `https://downloads.scylladb.com/rpm/centos/scylla-2025.3.repo` | the baseurl uses only `$basearch` (no `$releasever`), so a CentOS repo file is usable on Fedora |

## 3. Steps

The whole sequence runs from one command:

```bash
uv run python scripts/baremetal-simulation/run_simulation.py              # phase A
uv run python scripts/baremetal-simulation/run_simulation.py --teardown   # and clean up afterwards
```

`run_simulation.py` runs steps 0-7 in order (including one repeat on the dirty
host), times them, records progress in `.state/progress.json`, and prints a
summary with the test ids, the result directories and the checklist below. A
failed step stops the run and leaves the hosts up for inspection; `--resume`
continues from there without re-provisioning. `--phase perf` runs step 8 instead
of 5-7. `--list` and `--dry-run` show what it would do.

The individual steps are documented below because each one stays independently
runnable — for debugging, for a partial re-run, or when the hosts come from
somewhere else (a real Spider machine, for instance: fill in the JSON from step 2
by hand and start at step 3).

### Step 0 — preflight

```bash
uv run python scripts/baremetal-simulation/00_preflight.py
```

Checks AWS reachability, the VPC/subnet/SG/key pair, the SSH key's mode, hydra and
the container runtime, resolves the Fedora AMI, and warns if the distro patch is
not applied yet. Blocking problems exit non-zero.

### Step 1 — provision the "physical" hosts

```bash
uv run python scripts/baremetal-simulation/01_launch_hosts.py [--dry-run]
```

Launches the hosts into the SCT subnet with the SCT key pair, attaches `SCT-2-sg`
plus a one-off `<tag>-sg` that lets this workstation's public IP in, sets SELinux
permissive from user-data, and tags everything with
`TestId=<SIM_TEST_TAG>`, `keep=<hours>`, `keep_action=terminate` so
`utils/cloud_cleanup/aws/clean_aws.py` neither reaps them mid-run nor leaks them.
Refuses to run on top of an existing simulation.

### Step 2 — collect the addresses

```bash
uv run python scripts/baremetal-simulation/02_write_baremetal_config.py
```

Writes `<repo-root>/baremetal_sct901.json`:

```json
{
  "db_nodes":      {"username": "fedora", "node_list": [{"public_ip": "...", "private_ip": "..."}]},
  "loader_nodes":  {"username": "fedora", "node_list": []},
  "monitor_nodes": {"username": "fedora", "node_list": []}
}
```

All three sections must exist — `get_cluster_baremetal()` indexes them
unconditionally. An empty `node_list` is fine: `n_nodes` comes from the list
length, and `PhysicalMachineCluster` only raises `NodeIpsNotConfiguredError` when
there are fewer IPs than nodes.

The script then waits for SSH and prints the facts that decide whether the run can
work at all: distro, SELinux mode, and what `detect_disks(nvme=True)` would return.

### Step 3 — the one code change

```bash
uv run python scripts/baremetal-simulation/03_patch_sct_for_fedora.py --apply
# undo later with --revert, inspect with --check
```

### Step 4 — render the test case

```bash
uv run python scripts/baremetal-simulation/04_render_test_case.py
hydra conf -b baremetal test-cases/artifacts/baremetal-fedora.yaml   # sanity check
```

Deliberately absent from the generated file: `instance_type_db` / `sizing_db`
(inert on this backend) and the `*_public_ip` options (never read). `scylla_repo`
is set explicitly even though the backend is exempt from that check — which is
precisely gap #1 on the ticket.

### Step 5 — run the artifact test

```bash
scripts/baremetal-simulation/05_run_artifact_test.sh
```

Expected sequence in the log:

1. SSH up, `disable_firewall()`, `update_repo_cache()`.
2. `clean_scylla()` (no-op on a fresh host).
3. `download_scylla_repo` → `/etc/yum.repos.d/scylla.repo` → `yum install -y scylla`.
4. `detect_disks(nvme=True)` → `['/dev/nvme1n1']` (the root disk is filtered out because it has partitions).
5. `scylla_setup --nic <dev> --disks /dev/nvme1n1 --setup-nic-and-disks` → RAID + XFS on `/var/lib/scylla`.
6. `scylla-server` starts; artifact sub-tests run. `verify_snitch` self-skips because
   `use_preinstalled_scylla` is false.
https://argus.scylladb.com/tests/scylla-cluster-tests/2401961c-42ff-4e5f-b974-69b5b1760f8f
Results land in `~/sct-results/<timestamp>/`.

### Step 6 — collect logs

```bash
scripts/baremetal-simulation/06_collect_logs.sh
```

Collection is part of the experiment: "log collection works for this backend" is
one of the things SCT-901 needs answered.

### Step 7 — the dirty-host repeat (criterion 2)

```bash
scripts/baremetal-simulation/07_rerun_dirty_host.sh
```

Snapshots `io.conf`, mounts, RAID state, packages, CPU governor and sysctl into
`.state/host-state-<n>/`, diffs against the previous pass, then re-runs the
identical test on the un-reset host. `clean_scylla()` removes the package and wipes
the data dirs but leaves the RAID array, `io.conf`, the tuning and the governor
behind, and `PhysicalMachineNode.scylla_setup()` swallows the "already run"
failure — the exact seam where drift accumulates silently.

### Step 8 — optional perf smoke

```bash
SIM_DB_COUNT=3 SIM_LOADER_COUNT=1 SIM_MONITOR_COUNT=1 \
    uv run python scripts/baremetal-simulation/01_launch_hosts.py
uv run python scripts/baremetal-simulation/02_write_baremetal_config.py
scripts/baremetal-simulation/08_run_perf_test.sh
```

Expect extra friction on the monitor node (docker + scylla-monitoring on Fedora)
and on the loader (cassandra-stress / java). Note both as findings; neither
invalidates the DB-node conclusion.

### Step 9 — teardown (always)

```bash
uv run python scripts/baremetal-simulation/99_teardown.py --yes --all
uv run python scripts/baremetal-simulation/03_patch_sct_for_fedora.py --revert
```

## 4. Pass/fail criteria to record on SCT-901

- [ ] `Distro.FEDORA44` detected after the step-3 patch, RPM install branch taken.
- [ ] `scylla` RPM installed from the CentOS repo file on Fedora with no unresolved
      dependencies. *(Main unknown: Scylla RPMs are built for CentOS/RHEL 9 and
      Fedora carries newer glibc/openssl/python3. A failure here is a finding, not a
      blocker — see the fallbacks.)*
- [ ] `scylla_setup` completed and `/proc/mounts` contains ` /var/lib/scylla `.
- [ ] `nodetool status` UN; cassandra-stress smoke and cqlsh pass.
- [ ] Log collection through the bare-metal collector produced DB-node logs.
- [ ] Install wall-clock recorded (criterion 3 of the ticket).
- [ ] Second pass on the dirty host green, with the `io.conf` diff attached
      (criterion 2 — the trigger for reopening the bare-metal-image option).
- [ ] Every place the code assumed a cloud backend (instance type, region, tags,
      reboot) listed.

## 5. Fallbacks, in order

1. **RPM dependencies fail on Fedora** — switch to the relocatable unified package,
   which is the option SCT-901 proposes to adopt anyway:
   `SIM_UNIFIED_PACKAGE=<url> uv run python scripts/baremetal-simulation/04_render_test_case.py --force`.
   `_scylla_install()` then routes to `offline_install_scylla()` and `scylla_setup`
   gets `--no-verify-package` automatically.
2. **Still failing** — repeat the identical run on Rocky 9 / CentOS Stream 9
   (`SIM_AMI_ID=<rocky9 ami> SIM_SSH_USER=rocky`) to prove the *backend path* works
   and isolate the failure to Fedora packaging. That distinction is what the ticket
   actually needs.
3. **SSH unreachable from the hydra container** — widen the one-off security group,
   or run from inside the VPC:
   `hydra create-runner-instance -c aws -r <region> -a <az> -t <test-id> -d 2`
   then `hydra --execute-on-runner <runner-ip> run-test ...`.
4. **`scylla_setup` refuses because it ran before** — expected, and already swallowed
   by `PhysicalMachineNode.scylla_setup()`. Record it; it is the drift seam.

## 6. What to write back on the ticket

- Whether the comment's recipe works, with the exact failure if it does not.
- The Fedora gap in `sdcm/utils/distro.py`, as a concrete follow-up PR.
- Install wall-clock for repo-install vs unified-package on the same host.
- The dirty-host repeat result — the first real data point for criterion 2.
- Confirmation of the three known holes that Phase 3 of the ticket proposes to fix:
  the dead `*_public_ip` options, the missing version provenance for `baremetal` in
  `_check_version_supplied()`, and the inert `instance_type_db` in the perf YAML.
