# SCT-632: Azure stuck-VM recovery hardening — design

**Date:** 2026-07-16 (rev 2, after design review)
**Issue:** [SCT-632](https://scylladb.atlassian.net/browse/SCT-632)
**Builds on:** [PR #15004](https://github.com/scylladb/scylla-cluster-tests/pull/15004) (stuck-VM detection and redeploy, SCT-434)

## Problem

Since ~2026-07-07 Azure eastus intermittently fails to provision `Standard_L8s_v4` VMs:
ARM accepts the create request but the VM never leaves `provisioningState=Creating`.
The #15004 recovery machinery detects this and recovers via redeploy → delete+recreate,
and it rescued nodes in most runs. Three runs failed (#1020, #1023, #1025 of
`longevity-10gb-3h-azure-test`) exposing three gaps, confirmed by console-log evidence
across 8 builds:

1. **Redeploy never works for `Creating`-stuck VMs** — 0 rescues in ~14 attempts; it
   either hangs past the 300 s `DEFAULT_REDEPLOY_TIMEOUT` or fails instantly with Azure
   `VMRedeploymentFailed: internal error`. A VM that never finished allocation has no
   node to redeploy from. Every successful rescue came from delete+recreate. Cost: up to
   5 min wasted per VM per attempt.
2. **Per-VM waits are serialized** — `AzureVmProvider.get_or_create` gives each VM its
   own full 900 s `_wait_until_provisioned` in a loop. Two stuck VMs → ~31 min of pure
   waiting per recovery round (build #1025).
3. **No coordination with the Jenkins stage budget** — worst case for one VM is ~83 min
   against the 90-min "Provision Resources" stage timeout; for two VMs it cannot fit.
   The stage timeout then SIGTERMs hydra mid-recovery: no `ProvisionUnrecoverableError`
   is raised, events are not flushed, and runner log collection fails.

## Scope

Fixes 1–3 only, confined to `sdcm/provision/azure/` plus one new config param and its
wiring. **Deferred** (separate follow-ups): SKU/region fallback on repeated stuck
recreates; the "runner logs not collected on provision failure" gap.

## Design

### 1. Remove redeploy (`sdcm/provision/azure/provisioner.py`)

- A recovery round becomes: publish `InstanceProvisionStuckEvent` →
  `_delete_stuck_node(vm_name)` → the loop re-enters `_provision_resources`, which
  recreates only the deleted VMs (healthy ones are cached).
- Event message: `"recreating stuck VM on fresh capacity"`. (Safe: nothing parses the
  message; tests filter on the event class name only.)
- Deletion checklist (all verified to have no other callers):
  - `provisioner.py`: `_redeploy_stuck_node()`, `DEFAULT_REDEPLOY_TIMEOUT`, the
    redeploy-first wording in the `_provision_with_stuck_recreate` docstring;
  - `virtual_machine_provider.py`: `recheck_provisioned()` (only caller is
    `_redeploy_stuck_node`), `_wait_until_provisioned()` (absorbed into
    `_wait_all_provisioned`, see §2), the "will be redeployed to a new node" log line
    and adjacent comment (lines ~188–190) — reword to "will be recreated on fresh
    capacity";
  - `unit_tests/unit/provisioner/fake_azure_service.py`: `begin_redeploy` and the
    `VMCreateBehavior.redeploy_fails` script hook become dead once redeploy tests are
    dropped — remove.

### 2. Shared-deadline wait (`sdcm/provision/azure/virtual_machine_provider.py`)

Replace the serialized per-VM wait loop in `get_or_create` with a single
`_wait_all_provisioned(names, pricing_model, deadline) -> tuple[list[VirtualMachine], list[str]]`:

- One wait deadline shared by all pending VMs:
  `min(time.monotonic() + self._stuck_vm_timeout, deadline)` where `deadline` is the
  caller-supplied total-recovery deadline (see §3; the initial provisioning call passes
  it too — it is far away then and has no effect).
- Each sweep polls every pending VM via `virtual_machines.get(..., expand="instanceView")`
  (keeping the per-VM instance-state debug log); a VM reaching `Succeeded` is collected
  and removed from pending; sleep `self._stuck_vm_poll_interval` between sweeps.
- On deadline expiry the remaining pending names are returned as stuck; `get_or_create`
  raises `StuckVMProvisioningError` exactly as today, and the existing error precedence
  is preserved: a `ProvisionError` from the quick per-poller create-error check still
  wins over `StuckVMProvisioningError`.
- **Per-poll exception contract** (replaces `wait_for`'s implicit retry-all-exceptions):
  - `ResourceNotFoundError` on a **spot** VM → `SpotTerminationEvent` + immediate
    `OperationPreemptedError`. *Deliberate change*: today the 404 is swallowed by
    tenacity and surfaces only after the full 900 s window; failing fast on a real spot
    eviction saves up to 15 min and is strictly better.
  - Any other exception (transient `AzureError`, ARM 429/5xx, non-spot 404 from ARM
    read-propagation lag) → log at WARNING and keep the VM pending; if it never
    recovers, the deadline classifies it as stuck (delete of a nonexistent VM is
    harmless; recreate follows). This preserves today's tolerance of transient poll
    errors — a naive propagate-first-error loop would abort the whole batch on one
    throttled GET.
- The quick per-poller create-error check (`poller.wait(timeout=poll_interval)`) stays
  unchanged — it surfaces synchronous API errors (e.g. `OperationPreempted`).

### 3. Total recovery deadline (new config param)

- `azure_provision_stuck_vm_total_timeout` (int, seconds, default **4500**):
  - field in `sdcm/sct_config.py` (azure options section); description must state the
    interaction: recovery stops at whichever budget is exhausted first — this time
    budget or `azure_provision_stuck_vm_recreate_attempts`;
  - default in `defaults/azure_config.yaml`;
  - module-level fallback constant `DEFAULT_STUCK_VM_TOTAL_TIMEOUT = 4500` next to the
    existing `DEFAULT_STUCK_VM_TIMEOUT` (construction paths that pass no config —
    sct_runner, cleanup/discovery — must keep working);
  - wired through the two existing pass-through points: `sdcm/tester.py` (~line 1749)
    and `sdcm/sct_provision/azure/azure_region_definition_builder.py` (~line 59);
  - `docs/configuration_options.md` regenerated (update-conf-docs pre-commit hook).
- **Budget-aware loop** in `_provision_with_stuck_recreate`
  (`deadline = time.monotonic() + total_timeout` captured at entry):
  1. before starting a recovery round, require
     `time.monotonic() + self._stuck_vm_timeout <= deadline`; otherwise give up now —
     a round that cannot finish inside the budget is never started;
  2. the in-round wait is capped at `deadline` (passed down to
     `_wait_all_provisioned`, see §2), so a round that legitimately started cannot wait
     past the budget even with slow deletes/creates;
  3. give-up (attempts exhausted **or** budget exhausted) raises the same
     `ProvisionUnrecoverableError`; the give-up event message states which budget ran
     out.
  Together these bound give-up at ≈ `total_timeout` + one delete/create overhead —
  comfortably before the 90-min stage SIGTERM.
- Default rationale: with the 900 s wait, rounds complete at a ~17-min cadence
  (t≈15 stuck detected, rounds end t≈32/49/66); 4500 s (75 min) admits all 3 default
  recovery rounds (pre-check at t≈49: 49+15=64 ≤ 75) and leaves ≥15 min of stage margin
  for cleanup and reporting. (3600 s would silently cut recovery to 2 effective rounds.)
- **Known limitation** (accepted): the budget is per `get_or_create_instances` call, not
  per Jenkins stage. Sequentially stuck batches (e.g. a stuck batch after spot →
  on-demand fallback re-enters provisioning) each get a fresh budget. In practice only
  the L8s_v4 DB batch sticks; not worth cross-call state.

### Resulting timing

| Scenario | Today | After |
|---|---|---|
| 1 stuck VM, 3 attempts | ~83 min (barely fits the stage) | ~66 min |
| 2 stuck VMs, 3 attempts | ~2×83 min → stage kill mid-recovery | ~66 min (waits shared) |
| Pathological (slow deletes/creates) | stage kill, no error, no logs | give-up bounded at ~75 min + one round's non-wait overhead; error raised, events published, logs collected |

## Error handling

- `OperationPreemptedError` (spot) still aborts provisioning immediately and resets
  resource providers — unchanged (and now fires without waiting out the 900 s window,
  see §2).
- `ProvisionError` (create-time API errors) — unchanged, still takes precedence over
  stuck classification.
- Give-up path raises `ProvisionUnrecoverableError` before the Jenkins stage timeout can
  kill the process, so events flush and log collection runs.

## Testing

Adapt `unit_tests/unit/provisioner/test_stuck_vm_recreate.py` (flat pytest functions,
`fake_azure_service.py`; tests already shrink `_stuck_vm_timeout`/`_stuck_vm_poll_interval`
to sub-second values by attribute assignment, which the monotonic-deadline loop honors):

- drop redeploy-path tests (and the fake service's redeploy hooks, §1);
- two stuck VMs are recovered within a single shared-deadline round (no serialized
  double wait);
- recreate-succeeds-on-attempt-N still returns the VM set;
- total-timeout exhaustion: budget pre-check gives up with attempts remaining, raising
  `ProvisionUnrecoverableError` with the budget message;
- in-round wait is capped by the total deadline (wait ends at `deadline`, not
  `now + stuck_vm_timeout`);
- transient poll error (`AzureError` on one `get()` mid-sweep) does not abort the batch —
  VM stays pending and is collected once it reports `Succeeded`;
- spot-eviction 404 → immediate `OperationPreemptedError` (port the existing direct
  `_wait_until_provisioned` spot tests to `_wait_all_provisioned`).
