---
status: draft
domain: cluster
created: 2026-05-07
last_updated: 2026-05-12
owner: fruch
---
# GCE Capacity Validation and Automatic Zone Fallback

## Problem Statement

Tests running on GCE fail with `ZONE_RESOURCE_POOL_EXHAUSTED` errors when a zone lacks capacity for the requested machine type:

```
ZoneResourcesExhaustedError: Zone us-east1-b resource pool exhausted:
The zone 'projects/gcp-sct-project-1/zones/us-east1-b' does not have enough resources
available to fulfill the request. Try a different zone, or try again later.
```

### Pain Points

1. **Random zone selection with no fallback**: `random_zone()` in `sdcm/utils/gce_utils.py` picks a zone randomly from a hardcoded list (`SUPPORTED_REGIONS`). If that zone is exhausted, the test fails immediately with `ZoneResourcesExhaustedError`.
2. **Zone exhaustion is treated as unrecoverable**: `VirtualMachineProvider._wait_for_instance_creation()` (line ~147 in `sdcm/provision/gce/instance_provider.py`) raises `ZoneResourcesExhaustedError` which inherits from `ProvisionUnrecoverableError` — no fallback attempted.
3. **Hardcoded zone lists**: `SUPPORTED_REGIONS` (lines 89-96 in `gce_utils.py`) is manually maintained and already has a workaround (us-east1-b excluded due to "frequent allocation failures").
4. **No upfront machine type validation**: Unlike AWS `describe_instance_type_offerings`, GCE doesn't filter zones by machine type availability before provisioning.
5. **No cross-zone retry**: Once `ZoneResourcesExhaustedError` propagates up through `provision_instances_with_fallback()` → `_create_instances()` → `add_nodes()`, the test fails with no recovery attempt.
6. **15-minute retry sleep on non-capacity errors**: `_create_instance_with_retry()` retries 3× with 900-second sleep — but zone exhaustion bypasses this and fails fast, so recovery must happen at a higher level.

### Relationship to AWS Capacity AZ Fallback Plan

The [AWS Capacity AZ Fallback plan](aws-capacity-az-fallback.md) addresses the same class of problems for AWS. This plan is the GCE equivalent:
- **AWS plan**: Covers `InsufficientInstanceCapacity` with upfront AZ filtering + fallback retry
- **This plan**: Covers `ZONE_RESOURCE_POOL_EXHAUSTED` with upfront zone validation + fallback retry

Shared elements: backend-agnostic config naming (`fallback_to_next_availability_zone`), error detection centralization, the general fallback loop pattern.

---

## Current State

### Provisioning Code Path (GCE has a single path)

**Instance creation flow:**
1. `GCECluster.add_nodes()` (line 484, `sdcm/cluster_gce.py`)
2. → `GCECluster._create_instances()` (line 360) — builds definitions, determines pricing model
3. → `provision_instances_with_fallback()` (line 44, `sdcm/sct_provision/instances_provider.py`) — handles spot→on-demand fallback only
4. → `provision_with_retry()` (line 37) — retries `ProvisionError` 3× with 5s sleep
5. → `GceProvisioner.get_or_create_instances()` → `VirtualMachineProvider.get_or_create()` (line 90, `sdcm/provision/gce/instance_provider.py`)
6. → Phase 1: `_build_and_insert_instance()` for all instances (parallel)
7. → Phase 2: `_wait_for_instance_creation()` for all operations
8. → On `ZONE_RESOURCE_POOL_EXHAUSTED`: raises `ZoneResourcesExhaustedError` (fails fast, no retry)

### Zone Selection Logic

| Code | Location | What It Does |
|------|----------|--------------|
| `random_zone(region)` | `sdcm/utils/gce_utils.py:104-108` | Picks random zone letter from hardcoded `SUPPORTED_REGIONS` dict |
| `SUPPORTED_REGIONS` | `sdcm/utils/gce_utils.py:89-96` | Hardcoded zone letters per region (manually maintained) |
| `GceProvisioner.__init__()` | `sdcm/provision/gce/provisioner.py:46-64` | Uses `random_zone()` if `availability_zone` is empty; constructs full zone name |
| `gce_datacenter` param | `sdcm/sct_config.py:1068` | Region config (e.g., "us-east1") — no zone letter component |

### Error Handling

| Code | Location | What It Does |
|------|----------|--------------|
| `ZONE_EXHAUSTED_MARKER` | `instance_provider.py:43` | String `"ZONE_RESOURCE_POOL_EXHAUSTED"` |
| `_is_zone_exhausted()` | `instance_provider.py:45-47` | Checks if error contains the marker |
| `ZoneResourcesExhaustedError` | `provisioner.py:73` | Inherits `ProvisionUnrecoverableError` — not caught by any retry |
| `_cleanup_failed_instance()` | `instance_provider.py:344-351` | Cleans up on preemption/quota errors only |

### Config Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `gce_datacenter` | `us-east1` | GCE region(s) — string or list |
| `availability_zone` | (empty) | Zone letter(s) — when empty, `random_zone()` is used |
| `instance_provision_fallback_on_demand` | `false` | Spot-to-on-demand fallback (works for GCE) |
| `fallback_to_next_availability_zone` | N/A | Does NOT exist yet for GCE |

### GCE API Capabilities for Zone Validation

GCE provides `machineTypes.list` and `machineTypes.get` APIs that can verify whether a machine type is available in a specific zone. Unlike AWS, GCE machine type availability is deterministic per zone (not real-time capacity). This allows true upfront filtering.

```
GET https://compute.googleapis.com/compute/v1/projects/{project}/zones/{zone}/machineTypes/{machineType}
```

Returns 404 if machine type is not offered in that zone.

---

## Goals

1. Automatically retry in the next available zone when provisioning fails with `ZONE_RESOURCE_POOL_EXHAUSTED`
2. Validate machine type availability in candidate zones upfront (GCE API supports this deterministically)
3. Replace hardcoded `SUPPORTED_REGIONS` with dynamic zone discovery via GCE API
4. Reuse backend-agnostic config parameter `fallback_to_next_availability_zone` (shared with AWS plan)
5. Enable zone fallback by default for all GCE tests
6. Optionally support region fallback for single-node artifact tests

---

## Implementation Phases

### Phase 1: Centralize Zone Discovery and Capacity Error Detection

**Importance: Critical** | **Scope: ~80 LOC, 1 PR** | **Dependencies: None**

Replace hardcoded `SUPPORTED_REGIONS` with dynamic zone discovery and centralize error detection.

**Files to modify:**

- `sdcm/utils/gce_utils.py` — Add `GceZoneResolver` class:
  - `get_zones_for_region(project: str, region: str) -> list[str]` — calls `compute_v1.RegionsClient().get(project=project, region=region).zones` to get all zones in a region
  - `get_zones_for_machine_type(project: str, region: str, machine_type: str) -> list[str]` — filters zones where `machineTypes.get(zone, machineType)` returns 200 (not 404)
  - `get_common_zones(project: str, region: str, machine_types: list[str], preferred_zones: list[str] | None = None) -> list[str]` — intersection of zones supporting ALL machine types, preferred zones first
  - Cache results (zone offerings are stable; 1-hour TTL sufficient)

- `sdcm/provision/gce/capacity_errors.py` — New file, centralize error detection:
  - `CAPACITY_ERROR_MARKERS = ["ZONE_RESOURCE_POOL_EXHAUSTED", "RESOURCE_POOL_EXHAUSTED", "stockout"]`
  - `QUOTA_ERROR_MARKERS = ["QUOTA_EXCEEDED", "quotaExceeded"]` — regional zone quota exhaustion
  - `TYPE_NOT_AVAILABLE_MARKERS = ["RESOURCE_NOT_FOUND", "machineTypeNotFound"]` — machine type not offered in zone/region at all
  - `is_zone_capacity_error(exception: Exception) -> bool` — returns True for transient capacity exhaustion (retryable in another zone)
  - `is_quota_error(exception: Exception) -> bool` — returns True for quota-related failures (may need region fallback or quota increase)
  - `is_type_unavailable_error(exception: Exception) -> bool` — returns True when the machine type doesn't exist in the region at all (no retry will help within same region)
  - `classify_provisioning_error(exception: Exception) -> str` — returns one of `"capacity"`, `"quota"`, `"type_unavailable"`, `"unknown"` for routing fallback decisions
  - Replaces inline `_is_zone_exhausted()` in `instance_provider.py`

**Definition of Done:**
- [ ] `GceZoneResolver.get_zones_for_region()` returns zones dynamically from GCE API
- [ ] `GceZoneResolver.get_zones_for_machine_type()` correctly filters by machine type availability
- [ ] `GceZoneResolver.get_common_zones()` returns intersection with preferred ordering
- [ ] `is_zone_capacity_error()` identifies all known GCE capacity error variants
- [ ] `is_quota_error()` identifies regional quota exhaustion errors
- [ ] `is_type_unavailable_error()` identifies machine type not available in region
- [ ] `classify_provisioning_error()` correctly routes errors for fallback decisions
- [ ] `SUPPORTED_REGIONS` dict retained as fallback when API is unavailable (graceful degradation)
- [ ] Unit tests with mocked GCE compute clients

---

### Phase 2: Upfront Zone Filtering (Pre-Provisioning Validation)

**Importance: Critical** | **Scope: ~100 LOC, 1 PR** | **Dependencies: Phase 1**

Before provisioning begins, validate that candidate zones support all required machine types.

**Files to modify:**

- `sdcm/sct_config.py` — Add config parameter:
  - `pre_filter_unavailable_availability_zones: Boolean` — "Filter availability zones upfront to only those supporting all required machine types" (reuse same parameter name as AWS plan for backend-agnostic naming)
- `defaults/gce_config.yaml` — Add `pre_filter_unavailable_availability_zones: true`

- `sdcm/provision/gce/zone_resolver.py` — New file, `GceAZResolver` class:
  - Accepts SCT config params
  - Extracts ALL machine types needed: `gce_instance_type_db`, `gce_instance_type_loader`, `gce_instance_type_monitor`, `instance_type_db_target`, `nemesis_grow_shrink_instance_type`
  - Per region, calls `GceZoneResolver.get_common_zones()` from Phase 1
  - Returns validated zone list, logging warnings for filtered-out zones
  - Falls back to `random_zone()` behavior if API unavailable

**Integration points:**
- `sdcm/provision/gce/provisioner.py` — In `GceProvisioner.__init__()`, replace bare `random_zone()` call with `GceAZResolver` validated zone selection
- `sdcm/cluster_gce.py` — In `GCECluster.__init__()`, validate zones before passing to provisioners

**Definition of Done:**
- [ ] Zones that don't support required machine types are filtered out before provisioning
- [ ] Replacement zones are found automatically (e.g., zone "b" replaced with "c" if "b" doesn't support the type)
- [ ] Warning logged when zones are filtered/replaced
- [ ] If NO valid zone exists in the region, clear error raised before provisioning
- [ ] Feature disabled when `pre_filter_unavailable_availability_zones: false`
- [ ] Unit tests: mock GCE API responses, verify filtering logic
- [ ] Existing tests unaffected (zones that support all types pass through unchanged)

---

### Phase 3: Zone Fallback on Capacity Errors

**Importance: Critical** | **Scope: ~150 LOC, 1 PR** | **Dependencies: Phase 1, Phase 2**

When provisioning fails with zone exhaustion, automatically try the next available zone.

**Files to modify:**

- `sdcm/sct_config.py` — Add config parameter:
  - `fallback_to_next_availability_zone: Boolean` (backend-agnostic, shared with AWS plan)
- `defaults/gce_config.yaml` — Add `fallback_to_next_availability_zone: true`

- `sdcm/cluster_gce.py` — Wrap `_create_instances()` with zone fallback:
  - New `_create_instances_with_zone_fallback()` method:
    1. Get ordered zone candidates from `GceAZResolver.get_fallback_candidates()`
    2. For each candidate zone: attempt `_create_instances()`
    3. On capacity error (`classify_provisioning_error() == "capacity"`): log warning, update provisioner zone, try next zone
    4. On quota error (`classify_provisioning_error() == "quota"`): log warning, skip remaining zones in same region, escalate to region fallback (Phase 5) or fail with clear message about quota limits
    5. On type-unavailable error: fail immediately — no zone/region retry will help
    6. On other errors: re-raise immediately
    7. If all candidates exhausted: raise clear error with all attempted zones
  - Update `add_nodes()` to call `_create_instances_with_zone_fallback()` instead of `_create_instances()`

- `sdcm/provision/gce/provisioner.py` — Add `update_zone(new_zone: str)` method to `GceProvisioner`:
  - Updates `self._availability_zone` and `self._zone` (full zone name)
  - Reinitializes `VirtualMachineProvider` with new zone

- `sdcm/provision/gce/instance_provider.py` — Change `ZoneResourcesExhaustedError` handling:
  - Keep raising it (fast-fail at instance level is correct)
  - But now it will be caught at the `_create_instances_with_zone_fallback()` level

- `sdcm/provision/gce/zone_resolver.py` — Add `get_fallback_candidates()` method:
  - For single-zone config: returns all valid zones in order (preferred first, then alternatives)
  - For multi-zone config: returns permutations replacing each failing zone
  - Excludes already-tried zones

**Critical concerns addressed:**

1. **Partial provisioning cleanup**: If some instances created before exhaustion (parallel creation may partially succeed), must terminate them before retrying in new zone. Add `_cleanup_partial_instances()` in `cluster_gce.py`.

2. **Provisioner zone consistency**: After zone change, `GCECluster._get_instances_by_name()` uses `self.provisioners[dc_idx].availability_zone` for lookup — this is automatically consistent after `update_zone()`.

3. **VirtualMachineProvider cache**: `self._cache` in instance_provider.py caches instances by name. Must clear cache when switching zones.

**Definition of Done:**
- [ ] On `ZONE_RESOURCE_POOL_EXHAUSTED`, provisioning automatically retries in next zone
- [ ] All instances for a cluster role move to the new zone together
- [ ] Partial provisioning is cleaned up before retry
- [ ] Non-capacity errors do NOT trigger fallback
- [ ] Provisioner zone state is consistent after fallback
- [ ] Feature disabled when `fallback_to_next_availability_zone: false`
- [ ] Unit tests: mock provisioner to fail with zone exhaustion on first zone, succeed on second
- [ ] Unit tests: verify partial cleanup
- [ ] Unit tests: verify non-capacity errors propagate

---

### Phase 4: Replace Hardcoded SUPPORTED_REGIONS with Unified Region Registry

**Importance: Medium** | **Scope: ~50 LOC, 1 PR** | **Dependencies: Phase 1**

Remove the manually-maintained `SUPPORTED_REGIONS` dict and replace all usages with dynamic discovery. Expose supported regions as a single source of truth consumable by all layers (Python, Groovy pipelines, gce_builders.py).

**Files to modify:**

- `sdcm/utils/gce_utils.py`:
  - Deprecate `SUPPORTED_REGIONS` (keep as fallback with deprecation warning)
  - Update `random_zone()` to use `GceZoneResolver.get_zones_for_region()` with fallback to `SUPPORTED_REGIONS` on API failure
  - Add region validation: if a region is not in GCE API, raise early with helpful error

- `sdcm/utils/gce_region.py` (new or existing) — Single source of truth for supported GCE regions:
  - `get_supported_regions() -> list[str]` — returns the canonical list of regions SCT is configured for
  - Reads from a shared config file (e.g., `defaults/gce_regions.yaml`) rather than hardcoded Python dict
  - This file is also consumed by `vars/gce_builders.groovy` and Jenkins pipeline region selectors

- `defaults/gce_regions.yaml` (new) — Declarative region registry:
  ```yaml
  # Single source of truth for GCE regions available to SCT.
  # Consumed by: sdcm/utils/gce_region.py, vars/gce_builders.groovy, jenkins pipelines
  supported_regions:
    us-east1: {zones: [b, c, d]}   # fallback until dynamic discovery is default
    us-central1: {zones: [a, b, c, f]}
    ...
  ```

- `vars/gce_builders.groovy` — Update to read region list from `defaults/gce_regions.yaml` instead of maintaining a separate hardcoded list

- All callers of `random_zone()` — should already be replaced by Phase 2's `GceAZResolver`, but verify no remaining direct callers

**Definition of Done:**
- [ ] `random_zone()` uses dynamic zone discovery by default
- [ ] Falls back to `SUPPORTED_REGIONS` if GCE API is unavailable (e.g., missing credentials in unit tests)
- [ ] No more manual zone list maintenance needed when GCE adds/removes zones
- [ ] Single `defaults/gce_regions.yaml` defines supported regions for Python and Groovy
- [ ] `gce_builders.groovy` reads from the shared config file
- [ ] Unit tests verify both dynamic and fallback paths

---

### Phase 5: Region Fallback (Future, Optional)

**Importance: Low** | **Scope: ~100 LOC, 1 PR** | **Dependencies: Phase 3**

When all zones in a region are exhausted, try a different region. Scoped to single-node artifact tests.

**Constraint: `n_db_nodes=1` only** — Same rationale as AWS plan: multi-node region fallback creates cross-region clusters with unacceptable latency.

**Files to modify:**
- `sdcm/sct_config.py` — Enable `fallback_to_next_region` (backend-agnostic, shared with AWS)
- `sdcm/provision/gce/zone_resolver.py` — Extend to generate cross-region candidates
- `sdcm/cluster_gce.py` — Outer loop around region selection

**Prerequisites:** GCE image must be available in fallback region. Network/firewall rules must exist.

**Definition of Done:**
- [ ] When all zones exhausted and `fallback_to_next_region: true`, tries next region
- [ ] Validates `n_db_nodes=1` — clear error if multi-node tries region fallback
- [ ] Image availability checked in fallback region before attempting
- [ ] Unit tests for region fallback flow

---

## Testing Requirements

### Unit Tests (all phases)
- Mock `compute_v1.RegionsClient` and `compute_v1.MachineTypesClient` for zone/machine type discovery
- Mock `compute_v1.InstancesClient` to raise `GoogleAPIError` with `ZONE_RESOURCE_POOL_EXHAUSTED`
- Verify fallback candidate generation for single-zone and multi-zone configs
- Verify partial cleanup on failure during parallel instance creation
- Verify non-capacity errors propagate without fallback
- Verify `GceAZResolver` correctly intersects zones for multiple machine types
- Verify cache invalidation when switching zones

### Multi-AZ Setup Tests (Phase 2, mirroring AWS PR #14561 patterns)

Tests must cover the case where `availability_zone` is a comma-separated multi-zone config (e.g., `"b,c,d"` for multi-rack/multi-AZ Scylla deployments):

- **`test_resolve_replaces_invalid_zone_with_valid_alternative`**: Single zone configured ("b") not available for required machine types → replaced with first valid alternative
- **`test_resolve_multi_az_drops_unsupported_and_fills_alternatives`**: Multi-zone config "b,c,f" where "f" doesn't support the type → "f" replaced with next valid zone while "b" and "c" preserved
- **`test_resolve_multi_region_intersects_supported_zone_letters`**: Multi-region config (e.g., "us-east1 us-central1") → only zone letters valid in ALL regions are kept
- **`test_resolve_multi_region_multi_az_drops_unsupported_and_fills`**: Multi-AZ × multi-region combination: letter must be supported in EVERY region; drop+fill applies
- **`test_resolve_raises_when_no_valid_zone_in_region`**: No zone supports all required machine types → `NoValidAvailabilityZoneError` raised before provisioning
- **`test_resolve_with_unset_availability_zone_stays_unset`**: Empty/None `availability_zone` (letting `random_zone()` pick) → resolver does not inject explicit zone letters
- **`test_resolve_disabled_returns_unchanged`**: `pre_filter_unavailable_availability_zones: false` → no modification to configured zones
- **`test_required_machine_types_skips_target_when_none`**: `instance_type_db_target=None` does not constrain zone filtering
- **`test_required_machine_types_gated_by_node_count`**: Machine types for roles with `n_*_nodes=0` are excluded from the intersection (e.g., `gce_instance_type_loader` excluded when `n_loaders=0`)

**Test structure** (following AWS PR #14561 pattern):
```python
@pytest.fixture(name="mock_gce_zone_resolver")
def mock_gce_zone_resolver_fixture():
    """Patch GceZoneResolver to return configurable per-region zone lists."""
    region_returns: dict[str, list[str]] = {}

    class _ResolverStub:
        def get_common_zones(self, project, region, machine_types, preferred_zones=None):
            return region_returns.get(region, [])

    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver", _ResolverStub):
        yield region_returns


def test_resolve_multi_az_drops_unsupported_and_fills(mock_gce_zone_resolver):
    mock_gce_zone_resolver["us-east1"] = ["us-east1-a", "us-east1-c", "us-east1-d"]
    # configured b,c,f — only "c" supported; must replace b + f with valid alternatives
    params = _make_params(availability_zone="b,c,f")
    GceAZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert len(result) == 3
    assert "c" in result
    assert "b" not in result
    assert "f" not in result
```

### Error Classification Tests (Phase 1)

- **`test_is_zone_capacity_error_pool_exhausted`**: `ZONE_RESOURCE_POOL_EXHAUSTED` → True
- **`test_is_zone_capacity_error_stockout`**: `stockout` keyword in error → True
- **`test_is_quota_error_quota_exceeded`**: `QUOTA_EXCEEDED` in error → True (indicates regional quota limit, may need region fallback)
- **`test_is_type_unavailable_error_not_found`**: `machineTypeNotFound` / `RESOURCE_NOT_FOUND` for machine type → True (no retry will help)
- **`test_classify_provisioning_error_routing`**: Verify correct classification for each error type
- **`test_non_capacity_error_returns_false`**: Generic `GoogleAPIError` (e.g., permission denied) → all capacity checks return False

### Integration Tests
- Use a mock GCE project or service account with limited quotas
- Test the full provisioning flow through `GCECluster.add_nodes()`
- Verify zone transition logging

### Manual Validation
- Run an artifact test with `availability_zone: b` in us-east1 (known to have frequent exhaustion)
- Verify it automatically falls back to another zone (c or d)
- Verify logging clearly shows the zone transition
- Verify provisioned instances are in the fallback zone

---

## Success Criteria

1. Tests that previously failed with `ZONE_RESOURCE_POOL_EXHAUSTED` automatically recover by trying alternative zones
2. Machine type availability is validated upfront, eliminating provisioning attempts in unsupported zones
3. Hardcoded `SUPPORTED_REGIONS` is replaced with dynamic discovery (no manual maintenance)
4. Zone fallback enabled by default for all GCE tests
5. No orphaned instances left behind when fallback triggers
6. All existing tests continue to work without configuration changes
7. Backend-agnostic config parameters shared with AWS plan for future unification

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Partial parallel creation cleanup fails — orphaned instances | Medium | High (cost) | Track all initiated operations; terminate by name pattern in cleanup; safety check in test teardown |
| GCE API unavailable during zone discovery | Low | Medium | Fallback to hardcoded `SUPPORTED_REGIONS`; log warning |
| Zone capacity fluctuates — valid zone becomes exhausted between check and provision | High | Low | Upfront check is optimization only; runtime fallback (Phase 3) handles this case |
| Provisioner zone state inconsistency after fallback | Medium | High | Clear all caches; reinitialize VirtualMachineProvider; verify zone in instance lookup |
| Network/firewall rules missing in fallback zone | Low | High | GCE firewall rules are regional (not zonal); subnets are regional; only zone-specific resources (disks) need attention |
| Machine type name differences across zones | Very Low | Medium | GCE machine types are consistent within a region; only custom machine types could differ |
| Rate limiting on GCE API during zone discovery | Low | Low | Cache zone/machine-type results with 1-hour TTL; retry with backoff |
