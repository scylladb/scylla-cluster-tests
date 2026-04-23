---
status: draft
domain: cluster
created: 2026-01-24
last_updated: 2026-04-23
owner: fruch
---
# Multi-Cloud Provisioning Resilience for Artifact Tests

## Problem Statement

Two AWS-specific provisioning features need to be extended to GCE, Azure, and OCI backends for artifact testing scenarios:

1. **instance_provision_fallback_on_demand**: Fall back from spot/preemptible to on-demand when spot provisioning fails
2. **aws_fallback_to_next_availability_zone**: Try all availability zones sequentially to get instance capacity

**Primary Use Case**: Artifact tests (testing new OS images, Scylla packages, database versions) where test resilience matters more than specific infrastructure topology.

## Current State

| Backend | Spot Fallback | Zone Fallback | Region Fallback |
|---------|---------------|---------------|-----------------|
| AWS     | complete      | complete      | planned         |
| GCE     | partial       | not implemented | planned       |
| Azure   | partial       | not implemented | planned       |
| OCI     | partial       | not implemented | planned       |

## Goals

1. Rename `aws_fallback_to_next_availability_zone` → `fallback_to_next_availability_zone` (backend-agnostic)
2. Add `fallback_to_next_region` parameter for all backends
3. Implement zone fallback for GCE, Azure, and OCI (single-node artifact tests only)
4. Implement region fallback for all backends (single-node artifact tests only)
5. Ensure spot-to-ondemand fallback works consistently across all backends
6. Validate configuration: zone and region fallback require `n_db_nodes=1`
7. Maintain backward compatibility with existing AWS artifact tests

**Out of Scope**: Multi-node cluster fallback support.

## Key Constraints

**Zone and region fallback require `n_db_nodes=1`** to prevent mixed-AZ or split-region cluster configurations. For multi-node clusters, specify zones/regions explicitly or use spot-to-ondemand fallback (which supports multi-node).

When a DB node falls back to a different zone/region, loaders and monitors must follow to the same location. The resolved zone/region from DB provisioning is propagated to all subsequent node provisioning.

## Implementation Paths

Two code paths must both be updated:

1. **Legacy path** (`cluster_aws.py`, `cluster_gce.py`, `cluster_azure.py`): Direct cluster instantiation
2. **Modern path** (`sdcm/provision/`, `sdcm/sct_provision/`): Uses `provision_instances_with_fallback()`

The modern path is authoritative going forward. New logic should live there; legacy classes get thin wrappers.

## Implementation Phases

### Phase 1: Configuration & Foundation

**Goal**: Make configuration backend-agnostic and add validation.

- Rename `aws_fallback_to_next_availability_zone` → `fallback_to_next_availability_zone` in `sdcm/sct_config.py`
- Add `fallback_to_next_region` parameter
- Keep deprecated `aws_fallback_to_next_availability_zone` with backward-compat mapping
- Add validation in `SCTConfiguration` raising `ValueError` when `n_db_nodes > 1` with fallback enabled, or when `fallback_to_next_region` is set but only one region configured
- Update `defaults/test_default.yaml` and all `test-cases/artifacts/*.yaml` files
- Update `sdcm/tester.py` line 1821 to use new parameter name

**Definition of Done**:
- Parameter renamed with backward-compat and deprecation warning
- Validation blocks `n_db_nodes > 1` with zone or region fallback
- All artifact test configs updated
- Unit tests pass

**Tests**: `uv run pytest unit_tests/test_config.py -x -v`

---

### Phase 2: GCE Zone Fallback

**Goal**: Implement zone fallback for GCE matching AWS behavior.

- Add `_get_all_gce_zones_common_params()` in `sdcm/tester.py` to enumerate GCE zones per region
- Add `_create_auto_zone_scylla_gce_cluster()` in `sdcm/tester.py` to iterate zones and catch GCE capacity errors (`ZONE_RESOURCE_POOL_EXHAUSTED`, `QUOTA_EXCEEDED`, `stockout`)
- Wire it into GCE cluster creation when `fallback_to_next_availability_zone` is enabled
- Raise `CriticalTestFailure` when all zones are exhausted

**Definition of Done**:
- Zone iteration with capacity error detection implemented
- Logs show zone attempts and failures
- `CriticalTestFailure` raised when all zones exhausted
- Unit tests pass

**Tests**: `uv run pytest unit_tests/test_cluster_gce.py -x -v`

---

### Phase 3: Azure Zone Fallback

**Goal**: Implement zone fallback for Azure matching AWS/GCE behavior.

- Add `_get_all_azure_zones_common_params()` in `sdcm/tester.py` (Azure zones: 1, 2, 3)
- Add `_create_auto_zone_scylla_azure_cluster()` in `sdcm/tester.py` to iterate zones and catch Azure capacity errors (`SkuNotAvailable`, `ZonalAllocationFailed`, `AllocationFailed`, `InsufficientCapacity`, `QuotaExceeded`, `Overconstrainted`)
- Wire it into Azure cluster creation when `fallback_to_next_availability_zone` is enabled
- Raise `CriticalTestFailure` when all zones are exhausted

OCI follows the same pattern once its cluster implementation is complete.

**Definition of Done**:
- Zone iteration with capacity error detection implemented
- Logs show zone attempts and failures
- `CriticalTestFailure` raised when all zones exhausted
- Unit tests pass

**Tests**: `uv run pytest unit_tests/test_cluster_azure.py -x -v`

---

### Phase 4: Region Fallback

**Goal**: Fall back to other configured regions when all zones in the primary region are exhausted.

- Parse `region_name` as comma-separated list of regions
- For each region: discover zones, try zone fallback (if enabled), catch capacity errors
- On exhausting all zones in a region, log and proceed to next region
- Re-configure region-scoped infrastructure (AMI IDs, security groups, VPC, networking) before each region attempt
- Propagate resolved region to loader and monitor provisioning after DB node succeeds
- Raise `CriticalTestFailure` when all regions exhausted

**Definition of Done**:
- Region iteration with zone fallback nesting works for AWS, GCE, Azure, OCI (when ready)
- Infrastructure re-config per region handled (AMIs, networking, security groups)
- Resolved region propagated to loaders/monitors
- Validation enforces `n_db_nodes=1` and multiple regions configured
- Unit tests pass

**Tests**: `uv run pytest unit_tests/test_cluster_aws.py -x -v -k region_fallback`

---

### Phase 5: GCE Spot Fallback

**Goal**: Ensure GCE spot-to-ondemand fallback works consistently through the modern provision path.

- Verify `cluster_gce.py` uses `provision_instances_with_fallback()` from the modern path
- Add `handle_gce_preemption_error()` to map `GoogleAPIError`/`HttpError` with preemption/stockout keywords to `OperationPreemptedError`
- GCE preemption keywords: `"Instance failed to start due to preemption"`, `"ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS"`, `"ZONE_RESOURCE_POOL_EXHAUSTED"`, `"stockout"`, `"does not have enough resources"`
- Log pricing model change when fallback to on-demand occurs

**Definition of Done**:
- GCE preemption errors reliably map to `OperationPreemptedError`
- `provision_instances_with_fallback()` retries with on-demand when flag is set
- Fallback does not trigger when `instance_provision_fallback_on_demand` is disabled
- Unit tests pass

**Tests**: `uv run pytest unit_tests/provisioner/test_gce_provision_fallback.py -x -v`

---

### Phase 6: Azure Spot Fallback

**Goal**: Confirm and fix Azure spot-to-ondemand fallback through the modern provision path.

- Investigate `sdcm/provision/azure/provisioner.py` line 166: verify that `OperationPreemptedError` handling actually retries with on-demand pricing (not just cleans up)
- If retry is missing, add explicit retry-with-on-demand in `get_or_create_instances()`
- Verify `virtual_machine_provider.py` line 161 `ODataV4Error` → `OperationPreemptedError` conversion covers all Azure eviction codes
- Log pricing model change when fallback to on-demand occurs

**Definition of Done**:
- Investigation documented in a code comment
- Missing retry path filled if needed
- `OperationPreemptedError` triggers on-demand retry when flag is set
- Unit tests pass

**Tests**: `uv run pytest unit_tests/provisioner/test_azure_provision_fallback.py -x -v`

---

### Phase 7: Cross-Backend Validation & Documentation

**Goal**: Verify consistent behavior across all backends and document the features.

- Add parametrized unit tests for zone fallback across AWS, GCE, Azure
- Add parametrized unit tests for spot fallback across AWS, GCE, Azure
- Create `docs/FALLBACK_FEATURES.md` covering configuration, constraints, supported backends, and usage examples
- Run integration tests on real backends for each artifact test

**Definition of Done**:
- Cross-backend consistency verified (both legacy and modern paths)
- `docs/FALLBACK_FEATURES.md` written and covers all features
- Integration tests pass on AWS, GCE, Azure

**Tests**:
```
uv run pytest unit_tests/test_cross_backend_fallback.py -x -v
hydra run-test artifacts_test.ArtifactsTest.test_scylla_artifacts --backend aws --config test-cases/artifacts/ami.yaml
hydra run-test artifacts_test.ArtifactsTest.test_scylla_artifacts --backend gce --config test-cases/artifacts/ubuntu2204.yaml
hydra run-test artifacts_test.ArtifactsTest.test_scylla_artifacts --backend azure --config test-cases/artifacts/ubuntu2204.yaml
```

---

## File Changes Summary

| File | Change |
|------|--------|
| `sdcm/sct_config.py` | Rename param, add `fallback_to_next_region`, add validation, add deprecation compat |
| `defaults/test_default.yaml` | Add new params with `false` defaults |
| `test-cases/artifacts/*.yaml` (×9) | Replace `aws_fallback_to_next_availability_zone` → `fallback_to_next_availability_zone` |
| `sdcm/tester.py` | Add `_create_auto_zone_scylla_{gce,azure}_cluster()`, `_create_auto_region_scylla_*()`, update param reference |
| `sdcm/cluster_gce.py` | Add preemption error detection, zone fallback in legacy path |
| `sdcm/cluster_azure.py` | Add zone fallback in legacy path |
| `sdcm/cluster_oci.py` | Add zone fallback (when cluster implementation complete) |
| `sdcm/provision/azure/provisioner.py` | Fix/confirm spot retry-with-on-demand |
| `sdcm/provision/azure/virtual_machine_provider.py` | Ensure all eviction codes map to `OperationPreemptedError` |
| `sdcm/sct_provision/instances_provider.py` | Ensure zone/region fallback integration |
| `unit_tests/test_config.py` | Config param and validation tests |
| `unit_tests/test_cluster_{gce,azure,aws}.py` | Zone/region fallback tests |
| `unit_tests/provisioner/test_{gce,azure}_provision_fallback.py` | Spot fallback tests |
| `unit_tests/test_cross_backend_fallback.py` | Cross-backend consistency tests |
| `docs/FALLBACK_FEATURES.md` | New user guide |

## Success Criteria

- Zone and region fallback work identically on AWS, GCE, Azure, OCI (when available)
- Both legacy and modern provisioning paths support all fallback features
- Configuration is backend-agnostic
- Validation prevents `n_db_nodes > 1` with zone or region fallback
- No breaking changes to existing AWS artifact tests
- All pre-commit and unit tests pass
