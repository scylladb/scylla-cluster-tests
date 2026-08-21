---
status: draft
domain: nemesis
created: 2026-02-01
last_updated: 2026-08-10
owner: fruch
---
# Preserve Coredump Storage During Out-of-Space Nemesis

## 1. Problem Statement

When Scylla crashes during an out-of-space (ENOSPC) nemesis, systemd-coredump refuses to write the core and the crash is undebuggable:

```
systemd-coredump: Not enough disk space for coredump of 13964 (scylla), refusing.
```

**Root cause:** `scylla_coredump_setup --dump-to-raiddir` installs a systemd mount unit that bind-mounts the data directory over the coredump directory:

```ini
# /etc/systemd/system/var-lib-systemd-coredump.mount
[Mount]
What=/var/lib/scylla/coredump
Where=/var/lib/systemd/coredump
Type=none
Options=bind
```

`disrupt_nodetool_enospc` then fills `/var/lib/scylla` to ~100% with `fallocate`, so the coredump target has no space left. The crashes that matter most — those triggered *by* the disk-full condition — are exactly the ones we cannot collect.

**Why this matters:** this has been open since [scylla#4614](https://github.com/scylladb/scylla/issues/4614) and was seen again in [scylla#7085](https://github.com/scylladb/scylla/issues/7085). Every ENOSPC-triggered crash costs a full re-run to reproduce, if it reproduces at all.

## 2. Current State

### How coredump storage is set up

**`scylla_coredump_setup`** ([source](https://github.com/scylladb/scylladb/blob/master/dist/common/scripts/scylla_coredump_setup)), invoked by `scylla_setup` from `BaseNode.scylla_setup()` (`sdcm/cluster.py:3057`):

- Writes `/etc/systemd/coredump.conf` with `Storage=external`, `Compress=yes` (when zstd is available), `ProcessSizeMax=1024G`, `ExternalSizeMax=1024G`.
- Only with `--dump-to-raiddir`: writes the `var-lib-systemd-coredump.mount` unit above, then `enable`s and `start`s it. **The bind mount is a systemd unit**, so `systemctl stop/start var-lib-systemd-coredump.mount` is the correct API — not a raw `umount`.
- Sets `kernel.core_pattern=|/usr/lib/systemd/systemd-coredump …`.

### How SCT collects coredumps

**`sdcm/coredump.py`** — `CoredumpExportSystemdThread` (line 354), started per node from `sdcm/cluster.py:1404`:

- Polls `sudo coredumpctl -q --json=short` every 30s (`lookup_period`) for new PIDs.
- Reads the on-disk path from `coredumpctl info` → `Storage:` field, then hard-links, compresses and uploads that path (`hard_link_corefile`, `_upload_coredump`).
- **The path recorded in the journal is absolute** (`/var/lib/systemd/coredump/core.scylla.…`) and is resolved *at upload time*, not at crash time.

### The ENOSPC nemesis

**`sdcm/nemesis/__init__.py`**

- `disrupt_nodetool_enospc()` (line 1829) — restricted to Scylla ≤ 2025.3; K8s takes `_k8s_fake_enospc_error()`; otherwise `reach_enospc_on_node()` fills the disk and a `finally` runs `clean_enospc_on_node()`.
- `disrupt_end_of_quota_nemesis()` (line 1860) — a *different* mechanism: an `xfs_quota` block limit on the **scylla user**, filled by `fallocate` run as `su - scylla` (`sdcm/utils/quota.py:99`).

**`sdcm/utils/common.py`** — `reach_enospc_on_node()` (line 2554) fills to ~100% with `fallocate` in a `wait_for` loop; `clean_enospc_on_node()` (line 2576) removes `occupy_90percent.*` and restarts Scylla.

### Current workaround

None. Coredumps are silently lost during ENOSPC nemesis.

## 3. Goals

1. **Primary:** coredumps produced while `/var/lib/scylla` is full are written to disk *and* successfully collected and uploaded by `CoredumpExportSystemdThread`.
2. **Secondary:** no behaviour change for non-ENOSPC tests — the bind mount stays as-is.
3. **Constraint:** the bind mount must be restored even if the nemesis body raises.

**Success metrics:**
- A SIGQUIT injected into Scylla during the disk-full window yields an uploaded core with a decodable backtrace.
- No change in coredump capture rate for non-ENOSPC longevity runs.
- No leaked unmounted state: `systemctl is-active var-lib-systemd-coredump.mount` is `active` after the nemesis finishes.

## 4. Implementation Phases

### Phase 1: `coredump_to_root()` context manager — *Importance: High*

**Description:** temporarily stop the bind mount unit so cores land on the root filesystem, and restore it on exit.

**Definition of Done:**
- [ ] Add `coredump_to_root(node)` to `sdcm/utils/context_managers.py`, alongside `nodetool_context` and `DbNodeLogger`.
- [ ] No-op (log and yield) when the mount unit is not present or not active — covers Docker, K8s and any node set up without `--dump-to-raiddir`.
- [ ] Verify the state transition with `wait.wait_for()` on `systemctl is-active`, **not** a fixed `time.sleep()`.
- [ ] Drain in-flight cores before switching in either direction (see "core drain" below).
- [ ] Restore in a `finally`, and log-and-continue on restore failure rather than masking the nemesis's own exception.
- [ ] Do **not** `disable`/`enable` the unit — the original setup leaves it enabled, and a failed restore would otherwise persist across reboots.

**Two problems the switch must solve**

*(a) Cores in flight.* `systemd-coredump@.service` is configured with `RuntimeMaxSec=infinity` and a multi-GB core can take minutes to write. Stopping the mount underneath a running dump truncates it. `CoredumpExportSystemdThread._wait_for_pending_coredumps()` (`sdcm/coredump.py:404`) already implements exactly this wait — reuse that logic rather than duplicating it.

*(b) Cores stranded by the restore.* This is the subtle one. `/var/lib/systemd/coredump` is the *same path* whether or not the bind mount is active. A core written while unmounted lives on the root FS at that path; once the mount is restored, that path resolves to `/var/lib/scylla/coredump` and the core is **shadowed and unreachable** — while `coredumpctl` still reports the original `Storage:` path, so the upload silently fails on a missing file. Since collection is asynchronous (30s poll + compress + upload), the restore will routinely race ahead of it.

The context manager must therefore, before restoring the mount, move any cores written during the window out of the shadowed path:

```
/var/lib/systemd/coredump/core.*  →  /var/lib/scylla/coredump/
```

Moving them *into the bind-mount source* keeps the `coredumpctl`-reported path valid after the remount, so no change to `CoredumpExportSystemdThread` is needed. The disk has been freed by `clean_enospc_on_node()` at this point, so there is space. Cores that predate the window must be left alone (they are already under the mount).

**Sketch** (final shape to be settled in review):

```python
@contextmanager
def coredump_to_root(node):
    """Temporarily route coredumps to the root filesystem.

    scylla_coredump_setup --dump-to-raiddir bind-mounts /var/lib/scylla/coredump over
    /var/lib/systemd/coredump, so a full data disk means no coredumps. Stop that mount
    for the duration of the disk-full window, then hand any cores collected meanwhile
    back to the bind-mount source before restoring it.
    """
    unit = "var-lib-systemd-coredump.mount"
    was_active = node.remoter.sudo(f"systemctl is-active {unit}", ignore_status=True).ok
    if not was_active:
        node.log.debug("%s is not active, coredumps already go to the root filesystem", unit)
        yield
        return

    wait_for_pending_coredumps(node)
    node.log.info("Routing coredumps to the root filesystem for the duration of the disruption")
    node.remoter.sudo(f"systemctl stop {unit}")
    wait_for(lambda: not node.remoter.sudo(f"systemctl is-active {unit}", ignore_status=True).ok,
             timeout=60, step=2, text=f"waiting for {unit} to stop", throw_exc=True)
    try:
        yield
    finally:
        try:
            wait_for_pending_coredumps(node)
            # cores written here would be shadowed by the restored bind mount
            node.remoter.sudo(
                "sh -c 'mv -f /var/lib/systemd/coredump/core.* /var/lib/scylla/coredump/ 2>/dev/null'",
                ignore_status=True)
            node.remoter.sudo(f"systemctl start {unit}")
            wait_for(lambda: node.remoter.sudo(f"systemctl is-active {unit}", ignore_status=True).ok,
                     timeout=60, step=2, text=f"waiting for {unit} to start", throw_exc=True)
        except Exception:  # noqa: BLE001
            node.log.error("Failed to restore %s; coredumps stay on the root filesystem", unit, exc_info=True)
```

**Deliverables:** context manager + unit tests.

### Phase 2: Use it in the ENOSPC nemesis — *Importance: High*

**Description:** wrap the disk-fill window in `disrupt_nodetool_enospc()`.

**Definition of Done:**
- [ ] Wrap only the non-K8s branch — on K8s there is no mount unit and `_k8s_fake_enospc_error()` does not fill a real disk.
- [ ] Enter the context *after* the `/proc/mounts` guard, so nodes without dedicated storage skip it along with the rest of the disruption.
- [ ] The context must cover the `finally: clean_enospc_on_node(...)` too — Scylla is restarted there and can crash on the still-full disk.

```python
for node in nodes:
    with ignore_no_space_errors(node=node):
        if self._is_it_on_kubernetes():
            self._k8s_fake_enospc_error(node)
        else:
            result = node.remoter.run("cat /proc/mounts")
            if "/var/lib/scylla" not in result.stdout:
                self.log.error("Scylla doesn't use an individual storage, skip enospc test")
                continue

            with coredump_to_root(node):
                try:
                    with DbNodeLogger(self.cluster.nodes, "fill disk space", target_node=node):
                        ...
                finally:
                    with DbNodeLogger(self.cluster.nodes, "clean disk space", target_node=node):
                        ...
```

**`disrupt_end_of_quota_nemesis` is deliberately out of scope.** That nemesis applies an `xfs_quota` limit to the **scylla user**; systemd-coredump runs as root and is not subject to it, and the filesystem itself is not full. The reported symptom has never been observed there. If a quota-window crash does turn out to lose cores, it is a separate, smaller change — flagged in Open Questions.

**Dependencies:** Phase 1.

### Phase 3: Root filesystem capacity — *Importance: High (blocking)*

**Description:** the whole approach assumes the root filesystem can hold a Scylla core. On most backends today, it cannot.

`root_disk_size_db` defaults: **AWS 30 GB**, **Azure 30 GB**, **OCI 30 GB**, GCE 50 GB, GKE 50 GB. An uncompressed Scylla core is roughly the size of resident memory — tens of GB on the instance types used for ENOSPC longevity. `coredump.conf` sets `Compress=yes` where zstd exists, but systemd compresses *while writing*, and `ExternalSizeMax=1024G` imposes no useful bound. A 30 GB root disk shared with the OS, logs and the Scylla install will not hold one, and filling the *root* filesystem is considerably worse than losing a core.

**Definition of Done:**
- [ ] Measure actual compressed core size for the instance types used by ENOSPC test cases, and derive the required headroom (**Needs Investigation** — the 50 GB figure in earlier drafts of this plan was a guess and is not supported by data).
- [ ] Raise `root_disk_size_db` in the ENOSPC test-case YAMLs (`configurations/nemesis/EnospcMonkey.yaml` consumers, `test-cases/longevity/longevity-oos-*.yaml`) to the measured value.
- [ ] In `coredump_to_root()`, log available root-FS space on entry and publish a warning event if it is below the threshold, so a silent "still no core" outcome is diagnosable from the logs.

**Dependencies:** none — can proceed in parallel with Phase 1.

### Phase 4: Testing and validation — *Importance: Medium*

**Definition of Done:**
- [ ] Unit tests (Phase 1 deliverable, listed in §5).
- [ ] Manual validation on **AWS** with `longevity_oos_test.LongevityOutOfSpaceTest.test_oos_write` plus `EnospcMonkey`: inject `pkill -SIGQUIT scylla` during the disk-full window and confirm the core is uploaded and its backtrace decodes.
- [ ] Regression: a non-ENOSPC longevity run still reports `var-lib-systemd-coredump.mount` active and collects cores normally.
- [ ] Confirm the mount is active on every node at teardown.

**Docker backend is not a valid target for this.** `DockerCluster.node_setup()` (`sdcm/cluster_docker.py:462`) never calls `scylla_setup`, so there is no `var-lib-systemd-coredump.mount` and no dedicated `/var/lib/scylla` mount — `disrupt_nodetool_enospc` skips out at the `/proc/mounts` check. Docker is useful only to confirm the no-op path is taken.

**Dependencies:** Phases 1–3.

## 5. Testing Requirements

### Unit tests — `unit_tests/test_context_managers.py`

Using a fake remoter (see `unit_tests/lib/` fake-remoter helpers):

| Test | Asserts |
|------|---------|
| `test_coredump_to_root_stops_and_restores_mount` | `systemctl stop` on entry, `systemctl start` on exit, no `disable`/`enable` |
| `test_coredump_to_root_restores_on_exception` | mount restored when the body raises, and the original exception propagates |
| `test_coredump_to_root_noop_when_unit_inactive` | no `systemctl` calls when `is-active` fails; body still runs |
| `test_coredump_to_root_moves_cores_before_restore` | cores are moved to `/var/lib/scylla/coredump` *before* `systemctl start` |
| `test_coredump_to_root_restore_failure_does_not_mask` | restore failure is logged, body's exception still propagates |

### Manual validation (AWS)

1. Run an ENOSPC test case with a raised `root_disk_size_db`.
2. During the disk-full window: `mount | grep coredump` shows no bind mount; `df -h /` shows headroom.
3. `pkill -SIGQUIT scylla` on the target node.
4. Confirm a `CoreDumpEvent` with an upload URL, and that the downloaded core decodes.
5. After the nemesis: `systemctl is-active var-lib-systemd-coredump.mount` → `active`, and the core is present under `/var/lib/scylla/coredump`.

## 6. Success Criteria

- A SIGQUIT during the disk-full window produces an uploaded, decodable core.
- The mount unit is `active` on all nodes after the nemesis, including when the nemesis body fails.
- Non-ENOSPC runs show no behavioural change.
- Root filesystem usage stays below 80% throughout.
- Unit tests pass; code review approved.

## 7. Risk Mitigation

| # | Risk | Impact | Mitigation |
|---|------|--------|------------|
| 1 | Cores stranded at the shadowed path after restore | Core exists on disk but upload fails — silently, looks identical to today's failure | Move cores to `/var/lib/scylla/coredump` before restoring (Phase 1); assert on it in unit tests |
| 2 | Root filesystem too small; cores still refused, or **root** fills up | No improvement, or a far worse failure than the one being fixed | Phase 3 is blocking: measure, raise `root_disk_size_db`, warn on low headroom |
| 3 | Mount stopped underneath an in-flight dump | Truncated, undecodable core | Drain via the existing `_wait_for_pending_coredumps()` logic before each transition |
| 4 | Restore fails, leaving the node unmounted | Later cores land on root FS and may fill it | `finally` + log-and-continue; verify mount state at teardown; never `disable` the unit, so a reboot recovers |
| 5 | Restore masks the nemesis's own failure | Real disruption failure hidden behind a mount error | Catch and log inside the `finally`; never re-raise from there |
| 6 | Backends without the bind mount (Docker, K8s, `--no-raid-setup`) | Spurious `systemctl` failures | `is-active` guard makes the whole thing a logged no-op |

### Rollback

- Phase 1 alone is inert — nothing calls it.
- Phase 2 is a single `with` block; reverting it restores today's behaviour exactly.
- Phase 3 (`root_disk_size_db`) is independently revertible.

## Open Questions

1. **Compressed core size on ENOSPC instance types** — drives the `root_disk_size_db` value in Phase 3. Must be measured, not guessed.
2. **Does `disrupt_end_of_quota_nemesis` actually lose cores?** Reasoning in Phase 2 says no (root is not quota-limited, filesystem is not full), but this has not been observed empirically.
3. **`--no-raid-setup` / offline-install nodes** (`sdcm/cluster.py:6029`) — presumed no bind mount, so the no-op path applies; worth confirming on one run.
4. **Non-systemd nodes** — `scylla_coredump_setup` has a Gentoo/OpenRC path with no mount unit. Not used by SCT today; the `is-active` guard covers it regardless.

## References

- [scylla#7085](https://github.com/scylladb/scylla/issues/7085), [scylla#4614](https://github.com/scylladb/scylla/issues/4614) — original reports
- [scylladb#2548](https://github.com/scylladb/scylla-cluster-tests/issues/2548) — SCT tracking issue
- [`scylla_coredump_setup`](https://github.com/scylladb/scylladb/blob/master/dist/common/scripts/scylla_coredump_setup)
- `man systemd-coredump`, `man coredump.conf`
