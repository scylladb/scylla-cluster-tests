---
status: draft
domain: nemesis
created: 2026-02-01
last_updated: 2026-09-23
owner: fruch
---
# Preserve Coredump Storage During Out-of-Space Nemesis

## 1. Problem Statement

When Scylla crashes during an out-of-space nemesis, systemd-coredump refuses to write the core, and nobody can debug the crash:

> `systemd-coredump: Not enough disk space for coredump of 13964 (scylla), refusing.`

**Root cause.** `scylla_coredump_setup --dump-to-raiddir` installs a systemd mount unit, `var-lib-systemd-coredump.mount`, that bind-mounts `/var/lib/scylla/coredump` over `/var/lib/systemd/coredump`. So cores are written to the Scylla data disk. The ENOSPC nemesis fills that disk to ~100%. The end-of-quota nemesis exhausts the scylla user's XFS quota on the same directory. Either way, the coredump target has no room left. The crashes we most need to debug, the ones the full disk *causes*, are exactly the ones we lose.

**Why this matters.** The bug has been open since [scylla#4614](https://github.com/scylladb/scylla/issues/4614) and showed up again in [scylla#7085](https://github.com/scylladb/scylla/issues/7085). Each ENOSPC-triggered crash costs a full re-run to reproduce, if it reproduces at all.

## 2. Current State

### Coredump storage setup
- `BaseNode.scylla_setup()` calls `scylla_setup`, which calls [`scylla_coredump_setup`](https://github.com/scylladb/scylladb/blob/master/dist/common/scripts/scylla_coredump_setup).
- That script writes `/etc/systemd/coredump.conf` (`Storage=external`, `Compress=yes` where zstd exists, size caps of 1024G that don't limit anything in practice).
- With `--dump-to-raiddir` it also installs the bind-mount unit above and leaves it enabled and started. Because the bind mount is a **systemd unit**, `systemctl` is the right way to manage it, not a raw `umount`.

### Coredump collection
- `CoredumpExportSystemdThread` (`sdcm/coredump.py`) runs per node. Every 30 s it polls `coredumpctl` for new crashes, then hard-links, compresses and uploads each core.
- It reads the core's path from `coredumpctl info` **when it uploads, not when the crash happens**. The path is absolute and always sits under `/var/lib/systemd/coredump`.
- It already has private logic that waits until in-flight `systemd-coredump` writes finish.

### The nemeses
- `disrupt_nodetool_enospc` (`sdcm/nemesis/__init__.py`) fills `/var/lib/scylla` with `fallocate` through `reach_enospc_on_node()`, then frees it and restarts Scylla in `clean_enospc_on_node()`. It skips nodes with no dedicated `/var/lib/scylla` mount and fakes the error on K8s. It has an `all_nodes` mode that fills every data node. It is **gated to Scylla ≤ 2025.3** (SCT#12989).
- `disrupt_end_of_quota_nemesis` sets an XFS quota on the scylla user and fills it. It is currently disabled by `SkipPerIssues` ([scylla-enterprise#3736](https://github.com/scylladb/scylla-enterprise/issues/3736)).
- `EnospcMonkey` is not limited to OOS test cases. Nemesis selection picks the disruption in **most longevity runs** unless nemesis attributes filter it out, and only a minority of tests filter it out (review comment from @vponomaryov).

### Current workaround
None. Both nemeses lose cores silently.

## 3. Goals

1. **Primary:** a core produced while `/var/lib/scylla` is unwritable (disk full or quota exhausted) is written to disk, and `CoredumpExportSystemdThread` collects and uploads it.
2. **Fits the majority of tests:** the fix works wherever the ENOSPC nemesis can be picked. It must not depend on per-test-case YAML tuning.
3. **Cost only when needed:** no extra disk, root-disk growth or behaviour change for runs that never execute either nemesis.
4. **Always restored:** the coredump setup returns to its original state, even when the nemesis body raises.

**Success metrics**
- A `SIGQUIT` sent to Scylla during the disk-full window produces an uploaded core with a backtrace that decodes.
- Coredump capture rate doesn't change for runs that never run either nemesis.
- `var-lib-systemd-coredump.mount` is `active` on every node at teardown.

## 4. Design: a preallocated loop filesystem on the data disk

Just before the nemesis makes `/var/lib/scylla` unwritable, SCT reserves room for one core on the data disk itself and mounts it over the coredump directory:

1. **Size the reserve** from the node's memory (`/proc/meminfo`, or the memory Scylla was started with). A core is about Scylla's resident memory, compressed as it is written. Reading it from the node means no per-instance-type catalog.
2. **Preallocate** a root-owned file of that size on `/var/lib/scylla`, outside the Scylla data directories.
3. **Format and loop-mount** it at `/var/lib/systemd/coredump`. Cores written from here on land in the reserve.
4. **Run the disruption.** The fill can't take blocks that are already allocated, and the scylla-user quota doesn't apply to a root-owned file. That covers both nemeses.
5. **Hand back, then tear down.** Move every core in the reserve into `/var/lib/scylla/coredump`, unmount, detach the loop device and delete the file.

Why this approach: it is the same shell procedure on every backend that has a real data disk. It needs no cloud API, credentials or quota, adds no cost when the nemesis doesn't run, and leaves no resource behind that could leak. The alternatives considered are listed in §4.3.

### 4.1 Rules the implementation must follow

- **No-op guard.** When `var-lib-systemd-coredump.mount` is missing or inactive (Docker, K8s, `--no-raid-setup` nodes), log it and run the nemesis unchanged.
- **Reserve-or-skip.** If the data disk doesn't have room for the reserve, or preallocation or mount fails, publish a warning event and run the nemesis unchanged. Cores are then lost as they are today, never worse.
- **Drain before every switch.** A multi-GB core can take minutes to write. Switching the target underneath an in-flight dump truncates it. Reuse the collector's existing wait-for-pending logic: pull it out into a module-level helper in `sdcm/coredump.py`, and don't reach into the thread's private method. The thread might never have started on that node.
- **Hand cores back before unmounting.** `/var/lib/systemd/coredump` is the same path with or without the reserve, and the collector resolves that path lazily. A core left in the reserve disappears with it. `coredumpctl` still reports the old path, so the upload fails in a way that looks just like today's bug. Moving cores into the bind-mount source keeps the reported path valid. By that point `clean_enospc_on_node()` has freed the disk, so there is room. Cores written before the window aren't in the reserve and aren't touched.
- **Never swallow a failed move.** Log a warning unless the failure only means "no cores were written". If the move fails, **keep the reserve mounted** instead of deleting a core. Coredumps then go to the reserve for the rest of the run, which is safe because it is on the data disk.
- **Leave the systemd unit alone.** Stack the loop mount on top of the active bind mount rather than stopping or disabling the unit (confirm in Open Question 2). A reboot then recovers the original layout on its own.
- **Wait on state changes, don't sleep.** Poll mount state with `wait_for()`.
- **Tear down in `finally`, log and continue.** A teardown failure must never hide the nemesis's own exception.
- **Scope.** Cover the fill *and* the cleanup-plus-restart step. Scylla can crash while it restarts on a disk that is still full. In `disrupt_nodetool_enospc`, enter after the `/proc/mounts` guard and apply per node in `all_nodes` mode. In `disrupt_end_of_quota_nemesis`, wrap both the quota window and the final restart.
- **Fill after reserve.** The reserve exists before `reach_enospc_on_node()` measures free space, so the fill targets whatever is left.

### 4.2 Backend coverage

| Backend | Behaviour |
|---|---|
| AWS, GCE, Azure, OCI | Full support. Shell-only, the same on all four |
| minicloud | Full support if the guest kernel has loop devices (verified in Phase 0). **First target for every experiment and validation run** |
| Baremetal / on-prem | Full support |
| Docker | No-op: no bind mount, and the nemesis skips (no dedicated `/var/lib/scylla`) |
| K8s | No-op: the nemesis fakes ENOSPC and never fills a real disk |

### 4.3 Alternatives considered

| | Root filesystem | Hot-attached cloud disk | **Loop file on data disk (chosen)** |
|---|---|---|---|
| New cloud-API code | none | 4 backend implementations. SCT has no runtime volume attach today | none |
| Cost when nemesis doesn't run | bigger root disk on every DB node | none | none |
| Fits the majority of tests | only if raised globally | yes | yes |
| Added latency | none | ~30 s–2 min per node | seconds |
| Leak risk | none | orphaned volumes; cleanup jobs don't cover them | none |
| Worst failure | root FS fills | nemesis fails / stray volume | reserve not made → as today |
| Size | S | L | S–M |

- **Root filesystem:** rejected. ENOSPC is picked in most tests, so per-test-case tuning misses it, and growing the root disk everywhere is overkill (review, @vponomaryov).
- **Hot-attached disk:** rejected. It has no existing code to build on and brings leak risk. Detaching destroys cores that weren't handed back, and in `all_nodes` mode it puts N cloud-API round trips in the critical path. It stays as the fallback if Open Question 1 fails.

## 5. Implementation Phases

### Phase 0: Feasibility check (High, blocking)
**Description:** a manual experiment on one **minicloud** node, with no code change. This is a classic minicloud case: a local, disposable node with a real `scylla_setup` bind mount and a dedicated data disk, so the disk can be filled to 100% repeatedly at no cloud cost. Use a downscaled OOS config (see [`docs/minicloud.md`](../../minicloud.md)).
**Definition of Done**
- [ ] Preallocate a reserve and loop-mount it. Fill the host XFS to 100%, then write a multi-GB file into the loop mount. It succeeds (Open Question 1).
- [ ] If it fails, repeat with a zero-filled reserve and record how long filling one core's worth takes. If that is also unworkable, stop and reopen the choice of approach.
- [ ] Confirm that stacking over the active bind mount works and that systemd keeps treating the unit as active (Open Question 2).
- [ ] Confirm the minicloud guest kernel supports loop devices. If it doesn't, run this phase on one AWS node instead and file the gap against minicloud.
- [ ] Pick the filesystem type for the reserve and record both answers in this plan.

### Phase 1: Reserve context manager and pending-core helper (High)
**Description:** implement §4 and §4.1 as a context manager in `sdcm/utils/context_managers.py`.
**Definition of Done**
- [ ] Wait-for-pending logic extracted from `CoredumpExportSystemdThread` into a module-level helper. The thread delegates to it, with no behaviour change.
- [ ] Context manager covers every rule in §4.1, including reserve-or-skip and keep-on-failed-move.
- [ ] Unit tests from §6.

**Dependencies:** Phase 0.

### Phase 2: Wire into both nemeses (High)
**Description:** wrap the disk-fill window of `disrupt_nodetool_enospc` (non-K8s branch, per node) and the quota window plus restart of `disrupt_end_of_quota_nemesis`.
**Definition of Done**
- [ ] Both nemeses use the context manager with the scope from §4.1.
- [ ] End-of-quota change is unit-tested. Live validation waits until scylla-enterprise#3736 is lifted.

**Dependencies:** Phase 1.

### Phase 3: Validation (Medium)
Run every scenario on minicloud first, and move to real clouds only after it passes there.

**Definition of Done**
- [ ] minicloud: a downscaled `longevity_oos_test` with `EnospcMonkey`, in single-node and `all_nodes` modes, including the `SIGQUIT` check and the regression run below.
- [ ] AWS: `longevity_oos_test` with `EnospcMonkey`. Send `SIGQUIT` to Scylla during the full window. The core is uploaded and decodes.
- [ ] Repeat once in `all_nodes` mode.
- [ ] One GCE run (different disk layout).
- [ ] Regression: a run without either nemesis shows no reserve file, the mount `active`, and normal core collection.
- [ ] Record the measured compressed core size per instance type in this plan, to confirm the sizing rule.

Docker isn't a valid target here. It never runs `scylla_setup`, so it can only confirm the no-op path.

**Dependencies:** Phases 1–2.

## 6. Testing Requirements

**Unit tests**, in `unit_tests/test_context_managers.py` with a fake remoter:

| Scenario | Expectation |
|---|---|
| Mount unit active | Reserve sized from memory, preallocated, mounted on entry; cores handed back, then unmounted and removed on exit |
| Body raises | Reserve torn down, original exception propagates |
| Mount unit inactive / missing | No reserve commands; body still runs |
| Not enough free space / preallocate fails | Warning event, no mount, body still runs |
| Cores written during window | Moved to `/var/lib/scylla/coredump` **before** unmount |
| Move fails | Warning logged, reserve stays mounted and isn't deleted; "no cores" result logs nothing |
| Teardown fails | Error logged, body's exception (if any) not masked |
| Pending-core helper | Existing collector tests still pass after extraction |

**Manual validation** is listed in Phases 0 and 3.

## 7. Success Criteria
- A `SIGQUIT` during the disk-full window yields an uploaded, decodable core, in both single-node and `all_nodes` modes.
- No reserve file or extra mount is left on any node after the nemesis, unless a failed hand-back logged a warning.
- No cloud resources, root-disk growth or behaviour change for runs without either nemesis.
- Unit tests pass. Code review approved.

## 8. Risk Mitigation

| # | Risk | Impact | Mitigation |
|---|------|--------|------------|
| 1 | Loop FS can't absorb writes on a 100%-full host, e.g. XFS unwritten-extent conversion needs metadata space | Core refused as today | Phase 0 gates everything; zero-filled reserve as fallback; hot-attach as last resort |
| 2 | Core deleted along with the reserve | Silent loss, looks like today's bug | Mandatory hand-back before unmount; keep the reserve if the move fails; asserted in unit tests |
| 3 | Target switched during an in-flight dump | Truncated core | Drain with the shared pending-core helper before mount and before unmount |
| 4 | Data disk too full to reserve at nemesis start | No improvement for that run | Warning event; nemesis runs unchanged |
| 5 | Reserve shrinks the nemesis's fill | Slightly less data written before ENOSPC | None needed: the disk still reaches 100%, and the reserve is a few percent of a typical data disk |
| 6 | Teardown error masks nemesis failure | Real disruption failure hidden | Never re-raise from the teardown path |
| 7 | Stray reserve after an SCT crash | A few GB left on a node that is torn down anyway | None needed; the file goes with the node |

### Rollback
- Phase 1 does nothing on its own; nothing calls it.
- Phase 2 is one wrapping block per nemesis. Each can be reverted on its own, which restores today's behaviour exactly.

## Open Questions

1. **Can a loop filesystem on a preallocated file take writes once the host XFS is 100% full?** The blocks are allocated, but converting unwritten extents may need metadata space. Answered by Phase 0.
2. **Can the reserve stack on the active bind mount?** Or does the unit need a stop/start around it, and does systemd stay consistent? Answered by Phase 0.

### Answered
- **Which source of spare space?** The loop file on the data disk (@fruch, 2026-09-23). The root disk and hot-attach approaches were rejected (§4.3).
- **Does end-of-quota actually lose cores?** Yes (@fruch, 2026-09-23). Cores go to the same data directory the quota limits.
- **Non-systemd nodes?** Not supported by SCT, so out of scope (@fruch, 2026-09-23).
- **Grow the root disk?** Not globally; overkill for a rare event (@vponomaryov, 2026-09-23).

## References
- [scylla#7085](https://github.com/scylladb/scylla/issues/7085), [scylla#4614](https://github.com/scylladb/scylla/issues/4614): original reports
- [SCT#2548](https://github.com/scylladb/scylla-cluster-tests/issues/2548): SCT tracking issue
- [`scylla_coredump_setup`](https://github.com/scylladb/scylladb/blob/master/dist/common/scripts/scylla_coredump_setup)
- `man systemd-coredump`, `man coredump.conf`, `man losetup`
