# Test Plan — GCE AZ / Region / Multi-DC Fallback (PR #15427)

Scope: whole-cluster region fallback and per-DC (multi-region) fallback for GCE, the centralized
fallback loop shared with the legacy tester and modern provisioning paths, the placement handoff
across provision→run steps, and enabling AZ+region fallback by default for GCE.

Coverage mirrors the AWS fallback tests (`unit_tests/unit/provisioner/test_az_fallback.py`,
`test_az_resolver.py`) since GCE deliberately follows the same design.

Legend: [x] covered by unit tests in this PR · [~] partially covered · [ ] verify manually / follow-up.

## 1. Zone (AZ) resolver — `GceAZResolver`  (`test_gce_zone_resolver.py`, `test_gce_region_fallback.py`)
- [x] `resolve()` disabled (`pre_filter_unavailable_availability_zones=false`) leaves AZ unchanged.
- [x] `resolve()` replaces an unsupported AZ letter with a valid alternative in the same region.
- [x] `resolve()` with unset `availability_zone` picks a random valid zone letter.
- [x] `resolve()` raises `NoValidAvailabilityZoneError` when no zone supports the machine types.
- [x] `get_region_fallback_candidates()` excludes the current region and regions lacking supported zones.
- [x] `get_region_fallback_candidates()` never performs a peering lookup (global VPC).
- [x] `get_dc_fallback_candidates(idx)` excludes ALL in-use DC regions and filters by zone support.
- [ ] Machine-type gating: a type whose node count is 0 (e.g. vector store) does not constrain candidates.

## 2. Single-region whole-cluster fallback (`provision_with_region_fallback`)
- [x] Success on first attempt: no switch, no cleanup, placement unchanged.
- [x] `ZoneResourcesExhaustedError` → relocate to next region → success; exhausted region cleaned up.
- [x] Capacity-class `ProvisionError` (`is_zone_capacity_error`) treated as exhaustion → relocate.
- [x] Non-capacity error propagates unchanged and restores original region/AZ/env.
- [x] All candidates exhausted → `error_factory` raised; original placement + env restored.
- [x] Multi-DC config rejected by `enforce_single_region_gate` (single-region loop only).
- [x] `switch_region` / `restore_region` update `gce_datacenter`, `availability_zone`, and `SCT_GCE_DATACENTER`.
- [x] Env isolation: `SCT_GCE_DATACENTER` popped when originally unset / restored to prior value.

## 3. Multi-DC per-DC fallback (`provision_with_dc_fallback`)  — new
- [x] Exhausted DC relocated to a candidate region, whole cluster retried, success; only that DC moves.
- [x] Failing DC exhausts its candidates → `error_factory` raised; original multi-DC placement restored.
- [x] Non-capacity error propagates and restores; no cleanup performed.
- [x] Unattributable capacity error (no configured region named) → stop, no relocation/retry.
- [x] `switch_dc_region` updates only the target DC and leaves the global `availability_zone` untouched.
- [x] `_failed_dc_index` attributes the error to the DC whose region appears in the message (or None).
- [ ] Two different DCs exhaust in sequence across retries (per-DC candidate iterators advance independently).
- [ ] `dc_count` upper bound on attempts prevents an infinite loop when relocation keeps failing.

## 4. Routing — both paths delegate to the centralized router (`provision_with_fallback`)
- [x] Router: single-region config → `provision_with_region_fallback`; multi-region → `provision_with_dc_fallback`.
- [x] Modern path (`provision_sct_resources`): fallback enabled → router; disabled → provision once.
- [x] Modern path: multi-DC + enabled → router (dispatches to per-DC fallback).
- [x] Legacy path (`get_cluster_gce`): fallback enabled (single or multi-DC) → fallback wrapper; disabled → once.
- [x] Legacy wrapper delegates to the router (candidates computed internally, `error_factory=CriticalTestFailure`).

## 5. Cleanup / state hygiene
- [x] `_cleanup_legacy_partial_gce_clusters` destroys clusters, resets attrs to `None`, clears `credentials`.
- [x] `cleanup_region` sweeps only the matching region; discovery failure is swallowed (logged).
- [~] Multi-DC cleanup sweeps every currently-configured DC region before a retry (covered indirectly).
- [ ] No leaked instances after a mid-fallback abort (verify against real GCE project sweep).

## 6. Placement handoff (split provision→run pipeline)
- [x] `_apply_resolved_placement` applies a persisted region to `gce_datacenter`/`SCT_GCE_DATACENTER` for GCE.
- [~] `provision_sct_resources` persists the relocated `gce_datacenter` only when it changed.
- [ ] End-to-end: `sct.py provision-resources` relocates a region, the separate Run Test step picks it up.

## 7. Config defaults
- [x] `is_region_fallback_enabled` true for aws/gce, false for other backends / when flag off.
- [ ] `defaults/gce_config.yaml` enables `fallback_to_next_availability_zone` + `fallback_to_next_region`
      and they propagate to gce-derived backends (verified via regenerated `configuration_options.md`).
- [ ] A multi-DC GCE test inheriting the defaulted flag provisions normally (fallback is a graceful no-op
      when no capacity error occurs).

## 8. Manual / CI verification (not unit-testable)
Verified manually by injecting a simulated capacity stockout via the `BUILD_SIMULATE_GCE_EXHAUSTION`
env var (comma-separated zone-name substrings), which makes `VirtualMachineProvider.get_or_create`
raise `ZONE_RESOURCE_POOL_EXHAUSTED` for any matching zone without depending on a real GCE stockout.
This hook lives on a separate, temporary commit that is dropped before merge.

- [x] `Jenkinsfile` runs the provision step for the `gce` backend (new), so the modern path is exercised.
- [x] **Scenario A — single-region relocation** (region fallback): exhausting the whole configured
      region relocates the cluster to the next region and the run proceeds against it.

      ```bash
      export SCT_SCYLLA_VERSION=master:latest
      export SCT_TEST_ID=$(uuidgen)
      export BUILD_SIMULATE_GCE_EXHAUSTION="us-east1"
      uv run sct.py provision-resources -b gce -t longevity_test.LongevityTest.test_custom_time \
        -c test-cases/PR-provision-test.yaml \
        --config configurations/network_config/test_communication_public.yaml
      uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend gce \
        --config test-cases/PR-provision-test.yaml \
        --config configurations/network_config/test_communication_public.yaml
      ```

- [x] **Scenario B — multi-DC relocation** (per-DC fallback): exhausting one DC's region relocates
      only that DC, and the whole (multi-DC) cluster is retried against the updated placement.

      ```bash
      export SCT_IP_SSH_CONNECTIONS=public
      export SCT_SCYLLA_VERSION=master:latest
      export SCT_N_DB_NODES='3 3'
      export SCT_TEST_ID=$(uuidgen)
      export SCT_GCE_DATACENTER="us-east1 us-west1"
      export BUILD_SIMULATE_GCE_EXHAUSTION="us-west1"
      uv run sct.py provision-resources -b gce -t longevity_test.LongevityTest.test_custom_time \
        -c test-cases/PR-provision-test.yaml \
        --config configurations/network_config/test_communication_public.yaml
      uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend gce \
        --config test-cases/PR-provision-test.yaml \
        --config configurations/network_config/test_communication_public.yaml
      ```

- [x] **Scenario C — split provision→run handoff**: `provision-resources` relocates the region and
      persists it to `resolved_placement.yaml`; the separate `run-test` step (with the simulation
      unset) picks up the relocated placement.

      ```bash
      export SCT_IP_SSH_CONNECTIONS=public
      export SCT_SCYLLA_VERSION=master:latest
      export SCT_TEST_ID=$(uuidgen)
      export SCT_CONFIG_FILES='["test-cases/PR-provision-test.yaml", "configurations/network_config/test_communication_public.yaml"]'
      export BUILD_SIMULATE_GCE_EXHAUSTION="us-east1"
      uv run sct.py provision-resources -b gce -t longevity_test.LongevityTest.test_custom_time
      cat ~/sct-results/$SCT_TEST_ID/resolved_placement.yaml
      unset BUILD_SIMULATE_GCE_EXHAUSTION
      uv run sct.py run-test longevity_test.LongevityTest.test_custom_time -b gce
      ```

## Notes / intentional differences from AWS
- No VPC-peering eligibility gate (single global VPC) and no per-region image re-resolution
  (global images), so GCE has no analogue of AWS's `test_region_fallback_skips_region_without_equivalent_ami`
  or peering tests.
- GCE `availability_zone` is a single global setting (zones chosen per region at provision time), so
  multi-DC relocation moves the region only, not a per-DC AZ.
- Multi-DC fallback retries the whole cluster per relocation (uniform for both provisioning paths) rather
  than AWS's surgical per-DC DB provisioning; acceptable because capacity exhaustion is rare.
