# Triaging a Downscaled Local Run

Symptom, cause, and the overlay value that fixes it. Platform-level failures (host networking,
image cache, container lifecycle) are in the troubleshooting table in
[docs/minicloud.md](../../../docs/minicloud.md) — this page covers the ones a downscale causes.

The emulator's own view of a run is `docker logs minicloud` and `minicloud.log` in the test's
logdir. Read them before theorising about SCT.

---

## The test was too big

| Symptom | Cause | Fix |
|---|---|---|
| Container exit 137 mid-test, every guest unreachable at the same moment | cgroup OOM kill — the run did not fit | Recompute the budget. Lower `n_db_nodes`/`n_loaders`/`n_monitor_nodes`, or `minicloud_lightweight_memory`, or move to a bigger host |
| Preflight refuses to start, printing guest count x per-guest memory | The gate did its job | Same fix. `minicloud_skip_memory_check: true` only if you are certain the host can take it |
| Preflight passes, then the run dies at the moment a node is added | The budget counted `n_db_nodes`, not the peak | Set `cluster_target_size` and let the gate budget the peak |
| Exit 137 on a host with plenty of free RAM | `minicloud_container_memory` is set, and the cgroup cap is the real limit | Raise the cap or lower the guest count; the cap, not the host, is the budget |

---

## The guests are too small

| Symptom | Cause | Fix |
|---|---|---|
| "memory per shard too low" in a guest's Scylla log; the node never comes up | `minicloud_lightweight_memory` below ~3 GiB | Raise it to at least 3 GiB. Scylla reserves ~1.7 GiB for the guest OS and needs 1 GiB per shard |
| Scylla refuses to start on emulated storage | `developer_mode` missing | `configurations/minicloud.yaml` carries it — it is not layered, or not layered last |
| cassandra-stress reports client-side timeouts and near-zero throughput | `threads=` far above what 1 vCPU can serve | Cut to ~10 threads. More threads do not produce more load here |
| The run is dominated by `iotune` or repo installation | `force_run_iotune` on, or `use_preinstalled_scylla: false` | The overlay carries `force_run_iotune: false`; set `use_preinstalled_scylla: true` where the flow allows it |

---

## The shrink broke the test

| Symptom | Cause | Fix |
|---|---|---|
| The test fails asserting a metric has no data | The keyspace name in the stress command was changed, and the assertion queries it by name | Restore the production keyspace name; shrink only the volume |
| `check_required_files()` fails on a profile path | A `profile=` path was edited | Keep `/tmp/complex_schema.yaml` and the other profile paths exactly as the test-case has them |
| A complex-profile step fails on an unexpected operation | An `ops(...)` set was trimmed | Restore the full ops set; shrink `n=` instead |
| A verification step reads rows that were never written | Prepare and verify commands were shrunk to different sizes | Shrink every command in a prepare/verify pair by the same factor |
| The test ends before it does the thing you wanted to test | `test_duration` expired, or the workload finished early | Match `test_duration` to the shrunk workload, and remember it doubles as the stress duration in `GrowClusterTest` |
| The overlay appears to have no effect | Layered before the file it is meant to override, or `SCT_TEST_CASE` was set and dropped the overlay | Later files win; `configurations/minicloud.yaml` goes last. Confirm with `uv run sct.py conf` |

---

## The cluster shape was wrong

| Symptom | Cause | Fix |
|---|---|---|
| Quorum errors, or a nemesis leaves the cluster unusable | Fewer than 3 db nodes with RF=3 | 3 db nodes is a floor, not a suggestion |
| The test fails setting up Prometheus, or in `reconfigure_scylla_monitoring()` | `n_monitor_nodes: 0` on a test that reads Prometheus | Give it a monitor and budget the extra guest |
| A nemesis fails on node terminate, reboot or stop | A disruptive nemesis against emulated instance lifecycle | `nemesis_selector: "not disruptive"`. This is a minicloud gap, not a Scylla finding |
| A guest never boots, or boots unusably slowly, with an arm64 instance type | KVM cannot run a foreign architecture; the type is in the catalog but not runnable on this host | Pin the db role to an x86_64 type and an x86_64 image |
| GCE provisioning fails asking for local SSDs | The `gce_config.yaml` default of 4 local SSDs | `gce_n_local_ssd_disk_db: 1` — guests get qcow2-backed disks |
| GCE config validation fails before anything starts | `backend_required_params` needs the GCE instance types even though they are ignored | Set `gce_instance_type_db`, `_loader` and `_monitor` to types that exist in the catalog |
| `RunInstances` or `instances.insert` rejects the machine type, after config validation passed | The type is valid on the real cloud but outside the emulator's fixed catalog — commonly a loader or monitor role the base test-case never pinned | Pin that role to a type the emulator serves; list them with `describe-instance-types` / `machine-types list` against `localhost:5000` |

---

## The run was not local at all

| Symptom | Cause | Fix |
|---|---|---|
| A DB node gets a routable public IP, or a private IP like `10.4.x.x` in `eu-west-1` instead of `10.164.x.x` | `SCT_MINICLOUD_ENDPOINT_URL` was not exported for that invocation, so `is_minicloud_active()` was false and boto3 went to real AWS | Kill the run, terminate the instances, then `clean-resources --test-id ...` — `--user` will not match a run that died before tagging finished. Export the endpoint for **every** `sct.py` call and assert it before provisioning |
| The instance type in the log matches the yaml | Lightweight mode gives every guest 1 vCPU regardless, so a honoured instance type means the guest is not a guest | Same as above |
| `WaitForTimeoutError: Waiting for SSH to be up: timeout - 1500 seconds - expired`, after ~25 minutes of nothing | **The usual way this is discovered.** The run is on real AWS, and `configurations/minicloud.yaml` sets `ip_ssh_connections: private` — correct for minicloud, where guests are routable from the host, but a real AWS private address is not reachable from a laptop. So the overlay turns a wrong endpoint into a silent 25-minute hang | Check the node line in `sct.log`: `[<public> | <private>]`. A routable public IP with a `10.4.x.x` private is real AWS. Export `SCT_MINICLOUD_ENDPOINT_URL` and assert before provisioning |

---

## The run never got that far

| Symptom | Cause | Fix |
|---|---|---|
| `GetPlacementGroupError`: no placement group with tags found | `use_placement_group: true`, common in performance test-cases; minicloud implements no placement-group API, so the group SCT looks for was never created | `configurations/minicloud.yaml` sets it false — layer it last. Preflight now refuses the run up front rather than letting it die in provisioning |
| `CapacityReservationError` during provisioning | `use_capacity_reservation: true`; there is nothing to reserve against a local QEMU host | Same: the shared overlay sets it false |
| Preflight: "missing its parameter overlay", naming specific values | `configurations/minicloud.yaml` not in the config list, or not last | Add it last. Exported `SCT_*` variables cannot substitute for it |
| `SnapshotNotFound` from the AMI disk build | A dev AMI (`master:latest`) whose snapshot is not shared with the QA account | Use a released version on the AWS path; the GCE path is unaffected |
| `InvalidAMIID.NotFound` on launch | The AMI is not cached and lives in a different region than the emulator's | Keep the run in the region the images live in |
| Guests boot and get DHCP, then SSH fails with an authentication error | Host firewall blocked the guests' metadata requests, so no SSH key was injected | Host setup, not the downscale — see docs/minicloud.md |
| `Error: No such option: -c` from `collect-logs` or `clean-resources`, after the test passed | Only `start-minicloud` and `run-test` take a `-c` list; the other two read `SCT_CONFIG_FILES` from the environment | Export `SCT_CONFIG_FILES` for every phase, as `vars/runCollectLogs.groovy` and `vars/runCleanupResource.groovy` do |
| `ValueError: update_sct_runner_tags requires either the test_runner_ip or test_id argument` | `collect-logs` was given `--logdir` but no `--test-id`, so `collector.test_id` is `None`. A local run has no sct-runner to tag, but the call happens anyway | Add `--test-id`. The guests are still up at this point, so just re-run collection — nothing is lost |
| `clean-resources` fails with `test_id not found ... /latest/test_id: No such file` | Its `--logdir` means the **parent** results dir, not the run dir — the opposite of `collect-logs` | Use `--test-id` instead; unambiguous for both commands |
| `clean-resources` refuses to run | The container died; cleanup against a fresh emulator would only pretend to succeed | Collect `docker logs minicloud` first, then clean up manually |

---

## Reading a Result Honestly

A green downscaled run says the code path works. It says nothing about throughput, latency,
compaction behaviour under real load, or anything at scale — the shrink preserves the shape of
the test, not its scale. Report it as "exercised locally", and let the cloud run make the
performance claim.
