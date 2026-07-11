---
status: draft
domain: testing
created: 2026-07-09
last_updated: 2026-07-10
owner: null
---

# NVMe Namespace-Rescan Repro Test (AWS, Azure, GCE, OCI)

## 1. Problem Statement

scylla-machine-image commit `8f8031d` (branch `fix/nvme-namespace-rescan`) fixes a cloud
race where an NVMe controller enumerates on boot but its namespace block device
(`/dev/nvme*n1`) does not appear in time, causing `scylla_create_devices` to abort with
`NoDiskFoundError` and `scylla-image-setup.service` to fail — which blocks
`scylla-server.service` (`RequiredBy=scylla-server.service`) and leaves the node
permanently unusable. The fix adds a retry loop with an explicit NVMe rescan
(`rescan_nvme_controllers()` in `common/scylla_create_devices`) between attempts.

Neither the fix's author nor its reviewer can trigger the underlying cloud race on
demand — it depends on cloud-provider boot timing that isn't reproducible locally. The
fix currently has only mocked unit tests (`tests/test_scylla_create_devices.py`), which
prove the retry/rescan control flow in isolation but never exercise it against a real
kernel, real NVMe driver, and real `scylla-image-setup.service` boot sequence. This plan
adds a deterministic, real-hardware repro test in scylla-cluster-tests (SCT) that fakes
the "disk missing at boot" condition and asserts the node still comes up via the fix's
recovery path — turning an untestable production race into a CI-checkable scenario.

The bug itself is not Azure-specific — the fix's commit message references both
OCI (SCT-447) and Azure (SCT-297) hitting the same race, and `wait_for_devices()`/
`rescan_nvme_controllers()` sit in the code path used by every backend
(`common/scylla_create_devices` — `get_default_devices()` for GCE/Azure/OCI,
`get_disk_devices()`'s `instance_store` branch for AWS/EC2). The repro mechanism
designed below is likewise generic cloud-init/systemd infrastructure with no
Azure-specific dependency (see Current State), so this plan targets all four
backends SCT provisions on: AWS, Azure, GCE, OCI.

Production incidents split into two distinct failure modes, and Phases 1–4's
unbind-based test covers neither of them exactly:

- **Controller bound, namespace missing** (OCI incident
  `long-100gb-4h-oci-master-db-node-86f82825`): root disk is SCSI (`8:1`), the single
  NVMe controller enumerates and binds (`nvme nvme0: 16/0/0 default/read/poll queues`)
  but no `nvme0n1` ever appears. This is the fix's *primary* branch scenario (write `1`
  to `nvme0/rescan_controller`). No real-hardware repro exists for it — see the
  "Scope limitation" risk entry.
- **Controller never enumerated on the PCI bus at all** (Azure incident
  `longevity-10gb-3h-master-db-node-87adee8c`, `Standard_L8s_v4`): the root disk itself
  is NVMe (`nvme0n1p1` mounted at `/`, device `259:1`), and the VM's separate
  local-disk NVMe controller(s) never appear anywhere in dmesg for the whole boot. On
  the pre-`02bb3c74` code, `rescan_nvme_controllers()` found the healthy *root*
  controller's `rescan_controller`, wrote to it successfully, and that success alone
  skipped the PCI-bus rescan entirely — even though a *different*, still-missing
  controller was the actual problem. scylla-machine-image commit `02bb3c74` fixes this
  by always issuing the PCI-bus rescan unconditionally. Phases 5–6 below add a test
  variant that deterministically resurfaces this NVMe-root failure mode (fails against
  pre-`02bb3c74` code, passes with it).

## 2. Current State

- **The fix**: `common/scylla_create_devices` (scylla-machine-image) — `wait_for_devices()`
  retries `_get_fresh_local_disks()` for `device_wait_seconds`, calling
  `rescan_nvme_controllers()` between attempts. That function writes `1` to each
  `/sys/class/nvme/nvme*/rescan_controller` if present, else falls back to
  `echo 1 > /sys/bus/pci/rescan`. On success it prints `"Found devices: [...]"`; while
  waiting it prints `"No device names available yet, retrying..."`. If `wait_seconds` is
  falsy, the function returns immediately without retrying (`common/scylla_create_devices`,
  `wait_for_devices`).
- **`device_wait_seconds` is not test-specific** — it is not set anywhere in
  scylla-machine-image by default (`lib/user_data.py:55-56` defaults to `0`, i.e.
  disabled). In SCT, `ScyllaUserDataObject.scylla_machine_image_json`
  (`sdcm/sct_provision/user_data_objects/scylla.py:41`) unconditionally sets
  `device_wait_seconds: 60` for every node, every backend — so the retry/rescan loop is
  already live on all SCT-provisioned nodes today. This plan does not need to add or
  override this value.
- **Idempotency marker**: `scylla_image_setup` (scylla-machine-image,
  `common/scylla_image_setup`) checks `/etc/scylla/machine_image_configured`; if it
  exists, setup is skipped entirely on that boot. Removing this file is required to force
  a real re-run of device detection on a later boot.
- **User-data delivery is the same cloud-init pattern on all four backends**, only the
  channel each provider API uses to hand it to the VM differs:
  - Azure: `AzureVirtualMachineProvider.provision_vm`
    (`sdcm/provision/azure/virtual_machine_provider.py:134-155`) sets
    `osProfile.customData` from `UserDataBuilder.build_user_data_yaml()`
    (`sdcm/provision/user_data.py`), plus top-level `properties.userData` (base64
    `scylla_machine_image.json`, the `device_wait_seconds` channel).
  - AWS: `sdcm/sct_provision/aws/user_data.py` builds the equivalent payload.
  - GCE: `sdcm/provision/gce/instance_provider.py:265-268` sets instance
    metadata `user-data` (cloud-init) and separately supports raw `startup-script`
    metadata — this plan uses the `user-data`/cloud-init channel, consistent with the
    other three backends.
  - OCI: `sdcm/provision/oci/virtual_machine_provider.py:288-291` builds user data via
    the same `UserDataBuilder`, base64-encoded into instance `metadata["user_data"]`.
  - All four assemble the cloud-init YAML from the same `UserDataObject` list
    (`sdcm/sct_provision/user_data_objects/*.py`, one `*_region_definition_builder.py`
    per backend under `sdcm/sct_provision/<backend>/`). Each object's `script_to_run`
    becomes a `write_files` entry plus a sequential `runcmd` invocation
    (`sdcm/provision/user_data.py:92-114`) — this part of the plan (Phase 1) is written
    once and applies unchanged to all four.
- **No existing ordering hook**: cloud-init's `runcmd` stage has no guaranteed ordering
  relative to `scylla-image-setup.service`, which only requires `network.target`
  (`common/scylla-image-setup.service:4`). A `runcmd`-fired script could run before,
  during, or after the service, on any backend. Systemd unit ordering (`Before=`/`After=`)
  is reliable, but a newly-installed unit only takes effect starting the *next* boot.
- **No existing fault-injection or NVMe-aware mechanism in SCT** — confirmed by search:
  no references to `scylla-image-setup.service`, `machine_image_configured`, or
  `scylla_create_devices` anywhere in scylla-cluster-tests. Each backend already has an
  equivalent local-NVMe artifact test-case to use as a template: `test-cases/artifacts/
  ami.yaml` (AWS, `i4i.large`), `azure-image.yaml` (Azure, `Standard_L8s_v3`/Lsv3),
  `gce-image.yaml` (GCE), `oci-image.yaml` (OCI, `VM.DenseIO.E5.Flex:8`, a DenseIO
  local-NVMe shape) — each run via its own `jenkins-pipelines/oss/artifacts/
  artifacts-<backend>-image.jenkinsfile` through the shared `ArtifactsTest` class
  (`artifacts_test.py:52`, already handles all four backends via its `BACKENDS` dict).
  `ArtifactsTest`'s test methods run only after the node is already fully provisioned and
  Scylla-ready — the fault must be injected before that point (i.e. from
  provisioning-time user-data), not from within a test method, on any backend.
- **Local-disk detection is NVMe-based on all four clouds**: `lib/scylla_cloud.py`
  (scylla-machine-image) has `AwsInstance`, `GcpInstance`, `AzureInstance`, and
  `OciInstance`, each computing ephemeral disks from real `/dev/nvme*` devices via the
  same `nvmes_present`/root-device-exclusion pattern — so the injector's "find the
  non-root NVMe PCI device" logic (Phase 1) is mount-point-based, not cloud-specific,
  and is portable unchanged. Some shapes (e.g. OCI DenseIO, AWS instance-store) expose
  more than one local NVMe namespace, so the injector must unbind *any* non-root NVMe
  device, not assume exactly one — matching how the fix's own code already handles a
  list of local disks.
- **Reboot support already exists and is backend-agnostic**: `node.reboot()`
  (`sdcm/cluster.py:1534`).
- **Real-hardware scouting for the NVMe-root variant (done, 2026-07-10, on a live
  `Standard_L8s_v4` in eastus)** — both prerequisites for Phases 5–6 verified:
  - *Topology*: root is `nvme0` ("MSFT NVMe Accelerator v1.0", PCI `c05b:00:00.0` —
    same model and address pattern as the production incident node); local disks are
    four separate "Microsoft NVMe Direct Disk v2" controllers (`nvme1`–`nvme4`), one
    namespace each, each in its own Hyper-V vPCI PCI domain. Matches the Lsv4 docs
    (NVMe-only local temp disks, NVMe-capable OS image required — i.e. NVMe root).
  - *Fault primitive*: `echo 1 > /sys/bus/pci/devices/<addr>/remove` fully deletes the
    `pci_dev` on Hyper-V vPCI; the device does **not** come back spontaneously (held
    for 10s+, repeated 3/3 iterations). Writing `1` to the surviving root controller's
    `rescan_controller` — exactly what the pre-`02bb3c74` code did before
    short-circuiting — brings back **0/4** removed devices. A single
    `echo 1 > /sys/bus/pci/rescan` re-enumerates **4/4** removed controllers with
    their namespaces within seconds, logging genuine re-enumeration lines
    (`pci <addr>: [1414:b111] type 00 class 0x010802 PCIe Endpoint`,
    `nvme nvmeX: pci function <addr>`) usable as test assertions.
  - *Why `unbind` cannot cover this*: `driver/unbind` leaves the `pci_dev` registered
    (just driverless); a PCI-bus rescan skips slots that already have a `pci_dev`
    (verified in run `db-cluster-b0fd0433`, ~144 retry attempts over 20 minutes with
    zero effect). Only `remove` reproduces the incident's "never enumerated on the
    bus" state that a bus rescan can actually discover.
  - *Disk-classification mechanics*: `AzureInstance._non_root_nvmes()`
    (scylla-machine-image `lib/scylla_cloud.py:492`) computes EPHEMERAL as every
    `/dev/nvme\d+n\d+` not backing `/`. With all four local controllers removed, only
    root `nvme0n1` remains, so `get_local_disks()` returns empty and
    `wait_for_devices()`'s retry loop genuinely fires. **Removing fewer than all
    non-root controllers must be treated as injection failure**: the survivors keep
    `get_local_disks()` non-empty, the first pass succeeds immediately, no rescan is
    attempted, and RAID silently builds on the surviving subset.
  - *Enumeration race on the repro boot*: local controllers enumerate at ~2.2s, the
    injector unit starts at ~3s (`DefaultDependencies=no`). A controller that
    enumerates *after* the injector ran would survive injection and void the test (see
    previous bullet), so the remove-mode injector must know how many non-root
    controllers to expect (recorded on the settled first boot) and wait for all of
    them before removing anything.

## 3. Goals

- Deterministically reproduce, on a real VM on each of AWS, Azure, GCE, and OCI, the
  "NVMe controller present initially, local disk not detected on first pass" condition
  that the fix handles, without relying on actual cloud boot-timing luck.
- Assert `scylla-image-setup.service` still completes successfully via the fix's recovery
  path (PCI-bus-rescan fallback branch — see Risk Mitigation for why the primary
  `rescan_controller` branch is out of scope here) and that Scylla ends up running on the
  recovered disk, on all four backends.
- Share one fault-injection mechanism and one test-assertion method across all four
  backends (Phases 1 and 3) — only the per-backend test-case config and Jenkinsfile
  (Phases 2 and 4) are backend-specific.
- Make this runnable as a normal SCT test-case, on demand and eventually as part of each
  backend's artifact-validation flow, not as a one-off manual script.
- Do not touch the currently-hardcoded `device_wait_seconds: 60` (per decision — accepted
  as sufficient for this test rather than adding a config override).
- Deterministically reproduce, on a real Azure NVMe-root VM (`Standard_L8s_v4`), the
  "local NVMe controller absent from PCI enumeration while the root NVMe controller is
  healthy" incident mode, such that the test fails against pre-`02bb3c74`
  scylla-machine-image code and passes with it — proving the unconditional PCI-bus
  rescan specifically (via re-enumeration evidence in the kernel log, not just
  "Found devices") did the recovery (Phases 5–6).

## 4. Implementation Phases

### Phase 1 — Fault-injection `UserDataObject`

**Files**: new `sdcm/sct_provision/user_data_objects/nvme_fault_inject.py`.

- Subclass the existing `UserDataObject` base (`sdcm/provision/common/user_data.py`).
  `is_applicable` gated on `node_type == "scylla-db"` and a new params flag (e.g.
  `nvme_rescan_repro: true`, read via `self.params.get(...)`, defaulting to `False` so no
  other test-case is affected).
- Via `write_files` (not `script_to_run`/`runcmd`, since ordering there is not
  guaranteed — see Current State):
  - `/etc/systemd/system/sct-nvme-fault-inject.service` — `Type=oneshot`,
    `DefaultDependencies=no`, `Before=local-fs-pre.target scylla-image-setup.service`
    (not just `Before=scylla-image-setup.service` — see Risk Mitigation's "busy-mount
    race" entry for why `local-fs-pre.target` ordering is required). `ExecStart` runs a
    script that:
    1. Identifies the ephemeral/local-disk NVMe controller, excluding whichever backs the
       root filesystem (mirror the exclusion logic scylla-machine-image already applies —
       see `lib/scylla_cloud.py`'s local-disk/root-dev filtering — by checking mount
       points, not by assuming a fixed controller index).
    2. Safety guard: abort (non-zero exit, logged) if the identified device is mounted at
       `/` — belt-and-suspenders even though these test configs always provision a
       separate ephemeral/fast disk alongside the root disk.
    3. Unbinds that device's PCI function: `echo <pci-addr> > /sys/bus/pci/devices/<pci-addr>/driver/unbind`.
  - `/etc/systemd/system/scylla-image-setup.service.d/override.conf` —
    `After=sct-nvme-fault-inject.service`.
- A `runcmd` entry (ordering-insensitive, since it only needs to happen before the
  *reboot*, not before any particular service) that:
  1. `systemctl daemon-reload`
  2. `systemctl enable sct-nvme-fault-inject.service`
  3. Removes `/etc/scylla/machine_image_configured`.
  4. Resets whatever RAID/mount/fstab state the successful first boot already created, so
     `scylla_create_devices`/`scylla_configure` do a genuine, full re-run on the next boot:
     remove the `machine_image_configured` marker, `mdadm --stop`/`--zero-superblock` any
     leftover RAID array, and remove the `var-lib-scylla.mount` unit and its `/etc/fstab`
     line. Deliberately does **not** `umount /var/lib/scylla` — since the injector unit
     runs `Before=local-fs-pre.target`, the mount is never (re)activated on this boot in
     the first place, so there's nothing live to unmount (see Risk Mitigation).
  5. Triggers a reboot (or lets Phase 3's test code call `node.reboot()` explicitly instead
     of self-rebooting from cloud-init — **Needs Investigation**, whichever is more
     reliable to sequence from the SCT test side).

**DoD**: unit test for the new `UserDataObject` (`is_applicable` gating, generated
`write_files`/`runcmd` content) alongside existing tests for sibling objects (e.g.
`sdcm/sct_provision/user_data_objects/docker_service.py`'s pattern).

### Phase 2 — Test-case configs (one per backend)

**Files**: new `test-cases/artifacts/<backend>-image-nvme-rescan.yaml` for each of:

- `azure-image-nvme-rescan.yaml` — copy of `azure-image.yaml` (Standard_L8s_v3, spot).
- `ami-nvme-rescan.yaml` — copy of `ami.yaml` (AWS, `i4i.large`).
- `gce-image-nvme-rescan.yaml` — copy of `gce-image.yaml`.
- `oci-image-nvme-rescan.yaml` — copy of `oci-image.yaml` (`VM.DenseIO.E5.Flex:8`).

Each copy keeps its backend's existing instance type, region, and
`nemesis_class_name: NoOpMonkey`/`n_db_nodes: 1` shape, plus adds `nvme_rescan_repro:
true`.

**DoD**: all four configs validate against SCT's config schema/validator. Land AWS or
Azure first (whichever backend is easiest to iterate on) and use it to validate Phase 1's
`UserDataObject` end-to-end before copying to the remaining three — the four configs
should not require separate Phase-1 code paths, only separate yaml files.

### Phase 3 — Test assertions

**Files**: `artifacts_test.py` — new test method on `ArtifactsTest` (or a small subclass
used only by this test-case), gated on the same params flag.

- After node setup completes (which now spans the extra reboot from Phase 1), SSH in and
  assert, in order:
  1. `/etc/scylla/machine_image_configured` exists and its mtime is after the reboot
     timestamp (proves setup actually re-ran, not skipped).
  2. `journalctl -u sct-nvme-fault-inject.service` exited `0` (proves the fault was
     actually injected — guards against the injector silently no-op'ing, e.g. picking the
     wrong PCI address).
  3. `journalctl -u scylla-image-setup.service` contains, in order, `"No device names
     available yet, retrying..."` then `"Found devices:"`, and the unit itself exited `0`
     (not failed, not hit the `TimeoutStartSec=900` ceiling).
  4. `systemctl is-active scylla-server` = `active`.
  5. Existing `ArtifactsTest` disk/RAID checks pass (recovered disk is actually usable, not
     just detected).

**DoD**: test fails clearly (not a generic timeout) if the fix regresses — a regression
makes `scylla_create_devices` raise `NoDiskFoundError`, `scylla-image-setup.service`
fails, `scylla-server.service` never starts (blocked by its `RequiredBy`), and the node
never reaches Scylla-ready — this already surfaces as a normal SCT provisioning-failure
path (e.g. `_wait_for_preinstalled_scylla` timeout), no special-casing needed.

### Phase 4 — CI wiring (one per backend)

**Files**: new `jenkins-pipelines/oss/artifacts/artifacts-<backend>-image-nvme-rescan.jenkinsfile`
for each of AWS, Azure, GCE, OCI — same `artifactsPipeline(...)` shape as each backend's
existing `artifacts-<backend>-image.jenkinsfile`, pointing at the matching new test-case
yaml from Phase 2. Run on-demand initially; fold into each backend's nightly
artifact-validation flow once proven stable (not part of this plan's DoD).

### Phase 5 — Remove-mode fault injection (Azure NVMe-root variant)

**Files**: `sdcm/sct_provision/user_data_objects/nvme_fault_inject.py`,
`sdcm/sct_config.py`, `defaults/test_default.yaml`,
`unit_tests/unit/provisioner/test_nvme_fault_inject.py`.

- New config option `nvme_rescan_repro_mode: unbind|remove` (string, default `unbind`
  in `defaults/test_default.yaml` per the config standard — no code default). `unbind`
  keeps Phases 1–4's behavior byte-for-byte (existing unit tests and the four existing
  test-case yamls stay untouched); `remove` selects the NVMe-root variant.
- In `remove` mode, `NvmeFaultInjectUserDataObject.script_to_run` changes only the
  fault step; the service unit, ordering (`DefaultDependencies=no`,
  `After=systemd-remount-fs.service`, `Before=local-fs-pre.target
  scylla-image-setup.service`), idempotency-marker/RAID/fstab teardown, and
  enable/daemon-reload scaffolding carry over unchanged:
  - **First boot (cloud-init)**: additionally record the number of non-root NVMe
    controllers (counted on the fully settled system) to
    `/var/lib/sct-nvme-fault-inject/expected_controllers`.
  - **Repro boot (injector unit)**: wait (up to 60s) until that many non-root NVMe
    controllers are enumerated — closing the ~2.2s-enumeration vs ~3s-unit-start race
    (see Current State); then `echo 1 > /sys/bus/pci/devices/<addr>/remove` each one.
    Fail the unit (exit non-zero) if the expected count never shows up or any removal
    leaves the device present — partial injection must fail loudly, not silently pass
    (see Current State's "removing fewer than all" bullet). Write the removed PCI
    addresses to `/run/sct-nvme-fault-inject/removed` for the test's re-enumeration
    assertions.

**DoD**: unit tests cover the mode gating (default `unbind` output unchanged — existing
tests pass unmodified), the remove-mode script content (expected-count recording, wait
loop, `remove` writes, removed-addresses state file, no `unbind` write), and
`is_applicable` unaffected by mode. ≤200 LOC including tests.

### Phase 6 — NVMe-root test-case, assertions, CI wiring

**Files**: new `test-cases/artifacts/azure-image-nvme-rescan-root.yaml`, new
`jenkins-pipelines/oss/artifacts/artifacts-azure-image-nvme-rescan-root.jenkinsfile`,
`artifacts_test.py`.

- Test-case yaml: copy of `azure-image-nvme-rescan.yaml` with
  `azure_instance_type_db: 'Standard_L8s_v4'` (the scouted, incident-matching
  NVMe-root SKU) and `nvme_rescan_repro_mode: 'remove'`.
- `test_nvme_namespace_rescan_repro` gains remove-mode-only assertions (gated on the
  new option, so the four existing unbind test-cases are unaffected):
  1. `/run/sct-nvme-fault-inject/removed` is non-empty and its count equals the
     recorded `/var/lib/sct-nvme-fault-inject/expected_controllers` — all local
     controllers were injected.
  2. For every removed PCI address, the current boot's kernel log contains
     `nvme nvmeX: pci function <addr>` at least twice — once from initial boot
     enumeration, once from the fix's PCI-bus rescan re-enumerating it. This proves
     recovery happened via genuine re-enumeration of that address, not a side effect
     (a single occurrence means either the removal or the rediscovery never happened).
  - The existing assertions (marker refresh, injector exit 0, "retrying..." →
    "Found devices:" ordering, image-setup exit 0, scylla up, disk checks) apply as-is.
- Jenkinsfile: same `artifactsPipeline(...)` shape as
  `artifacts-azure-image-nvme-rescan.jenkinsfile`, pointing at the new yaml.

**DoD**: config validates; running the new test-case against a scylla-machine-image
build with `02bb3c74` passes; against a build without it (e.g. `d86a9ae`) fails on the
"retrying → Found devices" assertion (device never returns). ≤200 LOC.

## 5. Testing Requirements

- Unit test for the new `UserDataObject` (Phase 1 DoD above) — runs in SCT's existing unit
  test suite, no cloud resources needed.
- The repro test itself *is* the integration/manual test — it requires a real VM with a
  local-NVMe instance type on the target backend and is not expected to run in a fast
  unit-test loop. Run manually via the new Jenkinsfile/test-case (start with one backend,
  then the rest) before merging the scylla-machine-image fix, then periodically
  thereafter as a regression guard on each backend.
- The NVMe-root variant (Phase 6 DoD) must additionally be validated in both directions
  before it counts as a regression guard: one run against a scylla-machine-image build
  with `02bb3c74` (must pass) and one against its parent `d86a9ae` (must fail on the
  recovery assertion).
- No changes needed to scylla-machine-image's own test suite
  (`tests/test_scylla_create_devices.py` already covers the mocked control flow).

## 6. Success Criteria

- On each of AWS, Azure, GCE, and OCI: running the new test-case against a
  scylla-machine-image build **without** the `fix/nvme-namespace-rescan` fix fails
  deterministically (disk never reappears, service fails/times out).
- On each of AWS, Azure, GCE, and OCI: running it against a build **with** the fix passes
  deterministically (disk reappears via PCI-bus rescan, service completes, Scylla
  starts).
- The NVMe-root variant (Phases 5–6) fails against pre-`02bb3c74` scylla-machine-image
  code and passes with it, with kernel-log evidence that every removed controller was
  re-enumerated by the PCI-bus rescan on the repro boot.
- No flakiness attributable to the injected fault itself on any backend (only to normal
  per-backend provisioning flakiness, which is out of scope here).

## 7. Risk Mitigation

- **Scope limitation (accepted)**: unbinding a PCI device removes both the NVMe
  controller and its namespace — there is no userspace-reachable way to remove only the
  namespace while leaving the controller present. This test therefore exercises the fix's
  **PCI-bus-rescan fallback branch** (`rescan_nvme_controllers()`'s `else` path in
  `common/scylla_create_devices`), not its primary `rescan_controller`-write branch. The
  primary branch remains covered only by the existing mocked unit tests. Document this
  explicitly in the new test's docstring so it isn't mistaken for full coverage of the fix.
  A racy unbind/rebind variant that targets the primary branch was considered and
  rejected for CI: the kernel's own async namespace scan on driver bind races with the
  test's injected rescan, so such a test could pass regardless of whether the fix's
  explicit rescan call did anything.
  Consider revisiting this and asking the scylla-machine-image maintainers if there's a way to enforce this from the driver's perspective, or reach out to the kernel/cloud-provider teams for a more controlled test harness — a way to reliably reproduce "controller present, namespace missing" without full PCI unbind.
- **PCI-bus rescan on NVMe-root backends (validated on real hardware, 2026-07-10;
  covered by Phases 5–6 on Azure)**: the originally-flagged risk — a healthy NVMe root
  controller's successful `rescan_controller` write causing `rescan_nvme_controllers()`
  to skip the PCI-bus rescan — was real: it is exactly the production Azure
  `Standard_L8s_v4` incident (see Problem Statement) and was confirmed live on a
  scouted L8s_v4 node (root controller rescan brought back 0/4 removed local
  controllers). scylla-machine-image commit `02bb3c74` removes the short-circuit (the
  bus rescan now always runs), and the Phase 5–6 remove-mode variant on
  `Standard_L8s_v4` exists specifically to catch a regression of it. Two honest
  remainders: (1) coverage is Azure/Hyper-V-vPCI-only — the `remove`+`rescan` primitive
  was verified on Hyper-V vPCI, not on AWS Nitro or OCI DenseIO NVMe-root shapes, so
  `nvme_rescan_repro_mode: remove` must not be assumed portable to those backends
  without the same scouting exercise; (2) the *unbind*-based Phases 1–4 test remains
  structurally unable to exercise this scenario on any backend — a PCI-bus rescan is a
  no-op against an unbound-but-still-registered `pci_dev` (verified in run
  `db-cluster-b0fd0433`), which is precisely why the remove primitive was introduced.
- **Reboot-based sequencing**: relies on cloud-init's `runcmd` stage running at some point
  before the *second* boot's fault-inject unit needs to be active — not before the
  service, only before the reboot, so no strict ordering dependency exists here. This
  holds identically on all four backends since it's plain systemd/cloud-init behavior,
  not backend-specific.
- **Per-backend PCI-device identification (Needs Investigation)**: the injector's
  "find the non-root NVMe device" step is designed to be mount-point-based and portable,
  but must be validated against each backend's actual local-NVMe layout during
  implementation — in particular OCI DenseIO shapes and some AWS instance-store types
  can expose multiple local NVMe namespaces (see Current State), so the injector must
  iterate all non-root NVMe PCI devices, not assume a single one, and the test assertions
  (Phase 3) must tolerate multiple `sct-nvme-fault-inject`-unbound devices being recovered
  together.
- **State-reset correctness (Needs Investigation)**: if Phase 1's cleanup of
  `machine_image_configured` and RAID/mount state is incomplete, `scylla_create_devices`
  could behave unexpectedly on re-run (e.g. finding a stale RAID array). Confirm exact
  cleanup steps against `common/scylla_configure.py` during implementation, ideally by
  diffing on-disk state before/after a real first boot.
- **Remove primitive is hypervisor-mediated (low likelihood, test-breaking impact)**:
  `/sys/bus/pci/devices/<addr>/remove` + `/sys/bus/pci/rescan` on Azure goes through
  Hyper-V vPCI, not bare-metal PCIe hotplug. It behaved exactly like physical hotplug
  in scouting (3/3 single-device iterations plus a remove-all-4/rescan-once round), but
  a future Azure host-side change could alter that. The test's assertions fail loudly
  (injector unit exits non-zero, or re-enumeration lines missing) rather than
  silently passing, so drift is detectable. Mitigation: keep the scouted behavior
  documented here; re-scout before porting remove mode to another backend.
- **Forced scylla-image-setup re-run clobbers `intra_node_comm_public`'s scylla.yaml patch
  (found and fixed via a real run, 2026-07-11, on `db-cluster-1eb7c0f6`)**: the repro
  reboot forces `scylla-image-setup.service` — and therefore scylla-machine-image's
  `scylla_configure` — to genuinely re-run. `scylla_configure` regenerates
  `listen_address`/`seed_provider` from its own machine-image defaults (private IP),
  but never touches `broadcast_address`. Since Azure's two nvme-rescan test-cases set
  `intra_node_comm_public: true` (the SCT runner runs in AWS and needs a public address
  to reach the Azure node), the node's *initial* `config_setup()` call had already
  patched `broadcast_address` to the public IP — a patch that lives only in the running
  `/etc/scylla/scylla.yaml`, not in anything `scylla_configure` re-derives. The re-run
  therefore leaves `broadcast_address` (public IP) and `seed_provider`/`listen_address`
  (private IP, freshly regenerated) disagreeing, and Scylla's own startup validation
  refuses to start: `Startup failed: std::runtime_error (Use broadcast_address for seeds
  list)`. Confirmed live: `scylla-image-setup.service` itself succeeded (`exited,
  status=0/SUCCESS`, all 4 removed controllers recovered and RAID0 rebuilt) — the NVMe
  fix validation worked correctly — but `scylla-server.service` then failed on this
  unrelated config mismatch, with no `Restart=` on its unit to retry automatically.
  Fixed in `test_nvme_namespace_rescan_repro` by re-applying `node.config_setup(...)`
  (the same patch normal node provisioning applies once) immediately before starting
  scylla-server, then explicitly calling `node.restart_scylla_server(...)` instead of a
  passive `wait_db_up()` (which never starts a stopped service). This is a test-design
  gap specific to combining a forced mid-test `scylla-image-setup` re-run with
  `intra_node_comm_public` — not a bug in scylla-machine-image's fix, and not backend-
  general (AWS/GCE/OCI's nvme-rescan test-cases don't set `intra_node_comm_public`).
- **Cross-repo coupling**: this test asserts against specific log strings
  (`"No device names available yet, retrying..."`, `"Found devices:"`) and file paths
  (`/etc/scylla/machine_image_configured`) owned by scylla-machine-image. If that repo
  changes wording or paths, this test breaks silently until run. No automated
  cross-repo contract check exists; rely on scylla-machine-image PR review to flag this
  test as a consumer of those strings/paths.
