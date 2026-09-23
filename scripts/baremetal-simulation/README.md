# SCT-901 — bare-metal simulation on AWS

Scripts that rehearse the `baremetal` backend end to end on throwaway AWS hosts,
so the bare-metal install path can be exercised before a Spider machine is booked.

The hosts are plain EC2 instances provisioned **by these scripts, outside SCT**.
SCT is then pointed at them through the `baremetal` backend exactly the way it
would be pointed at a physical machine: it never creates, tags, reboots or
destroys them.

Full runbook, including the reasoning and the pass/fail criteria:
[`docs/plans/sct901-baremetal-aws-simulation-runbook.md`](../../docs/plans/sct901-baremetal-aws-simulation-runbook.md).

## One command

```bash
uv run python scripts/baremetal-simulation/run_simulation.py              # phase A, hosts left running
uv run python scripts/baremetal-simulation/run_simulation.py --teardown   # and clean up afterwards
uv run python scripts/baremetal-simulation/run_simulation.py --phase perf # 3 db + loader + monitor
```

`run_simulation.py` chains the numbered steps below, times each one, records
progress in `.state/progress.json` and prints a summary with the data points
SCT-901 asks for. By default it also resets the host and repeats the artifact test once, which is
SCT-901 criterion 1 (`--reset-passes 0` drops it).  `--dirty-passes N` adds runs
on the *un-reset* host instead -- criterion 2, and expected to fail.

Useful flags: `--list` (show the steps), `--dry-run` (print the commands),
`--resume` (continue after a failed step without re-provisioning), `--from-step
<key>`, `--skip <key>`, `--teardown`.

A failing step stops the run and leaves the hosts up so the box can be
inspected; they still carry `keep=<SIM_KEEP_HOURS>`/`keep_action=terminate`, so
they cannot leak past that window.

## Order of operations

Each step is also runnable on its own:

| Step | Command | Does |
| --- | --- | --- |
| 0 | `uv run python scripts/baremetal-simulation/00_preflight.py` | AWS creds, SCT VPC/subnet/SG/key pair, SSH key, hydra, Fedora AMI, distro patch |
| 1 | `uv run python scripts/baremetal-simulation/01_launch_hosts.py` | launches the Fedora hosts + a security group for your own IP |
| 2 | `uv run python scripts/baremetal-simulation/02_write_baremetal_config.py` | writes `<repo>/baremetal_sct901.json` and probes the hosts |
| 3 | `uv run python scripts/baremetal-simulation/03_patch_sct_for_fedora.py --apply` | teaches `sdcm/utils/distro.py` about Fedora 41-45 |
| 4 | `uv run python scripts/baremetal-simulation/04_render_test_case.py` | `test-cases/artifacts/baremetal-fedora.yaml` is committed; this only writes it if missing. Pass `--force` to re-render it from `config.env` (e.g. after changing node counts or setting `SIM_UNIFIED_PACKAGE`). |
| 5 | `scripts/baremetal-simulation/05_run_artifact_test.sh` | `hydra run-test artifacts_test ... --backend baremetal` |
| 6 | `scripts/baremetal-simulation/06_collect_logs.sh` | `hydra collect-logs` through the bare-metal collector |
| 7 | `scripts/baremetal-simulation/09_reset_host.sh` | reset the host to a pre-`scylla_setup` state (required between passes) |
| 8 | `scripts/baremetal-simulation/07_rerun_dirty_host.sh` | optional: second pass on the **un-reset** host, the drift experiment — expected to fail |
| 9 | `scripts/baremetal-simulation/08_run_perf_test.sh` | optional perf smoke, needs db + loader + monitor hosts |
| 10 | `uv run python scripts/baremetal-simulation/99_teardown.py --yes --all` | terminates everything, removes the generated files. Refuses while one of this simulation's runs is still executing — a passing pytest summary is not the end of a run, log collection follows it. `--force` overrides. |

Steps 3 and 4 produce **uncommitted local state**; step 9 (`--all`) and
`03_patch_sct_for_fedora.py --revert` undo them.

## Configuration

Everything lives in [`config.env`](config.env); any value can be overridden from
the environment, which always wins:

```bash
SIM_REGION=us-east-1 SIM_INSTANCE_TYPE=i4i.xlarge \
    uv run python scripts/baremetal-simulation/01_launch_hosts.py
```

Phase A (artifact test) is `SIM_DB_COUNT=1`, no loaders, no monitors.
Phase B (perf smoke) is `SIM_DB_COUNT=3 SIM_LOADER_COUNT=1 SIM_MONITOR_COUNT=1`.

## Things that will bite you

- **Fedora > 36 is unknown to SCT.** `sdcm/utils/distro.py` lists only 34/35/36, so
  Fedora 44 resolves to `Distro.UNKNOWN`, `is_rhel_like` is `False`, and
  `install_scylla()` takes the **apt** branch on an RPM host. Step 3 patches it;
  this is the one real code finding of the exercise and deserves its own PR.
- **`SCT-2-sg` only allows traffic inside itself** — a runner outside the VPC cannot
  even SSH in. Step 1 creates a second, IP-scoped security group for that.
- **Instance type must have a local NVMe disk** (`i4i.large` by default).
  `detect_disks(nvme=True)` needs an unpartitioned `/dev/nvmeXnY`, otherwise
  `scylla_setup` has nothing to build the RAID on.
- **`db_nodes_public_ip` & friends are dead options** for this backend —
  `get_cluster_baremetal()` reads only the JSON from step 2.
- **`logs_transport` defaults to `vector`**, which makes the node push logs *to* the
  runner; from a laptop behind NAT that never arrives. The generated test case uses
  `ssh`.
- **A reused host must be reset before a second run.** `scylla_setup` refuses to
  redo its work while `/etc/systemd/system/var-lib-scylla.mount` exists, which also
  skips `scylla_io_setup`, so nothing regenerates `/etc/scylla.d/io.conf` after the
  RPM reinstall has reset it -- and Scylla then dies with `Bad I/O Scheduler
  configuration`. That is what `09_reset_host.sh` is for.
- **SCT resolves the version from the repo at config time** with a 30s budget and no
  retry, on every invocation. Repeated runs can get throttled by
  downloads.scylladb.com and fail before touching the hardware;
  `05_run_artifact_test.sh` retries once when a run dies within 150s.
- **`PhysicalMachineNode.reboot()` raises `NotImplementedError`** — SELinux is set to
  permissive from user-data at launch instead.
