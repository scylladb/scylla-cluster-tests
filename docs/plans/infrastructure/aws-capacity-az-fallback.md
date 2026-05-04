---
status: draft
domain: cluster
created: 2026-04-12
last_updated: 2026-05-04
owner: fruch
---
# AWS Capacity Validation and Automatic AZ/Region Fallback

## Problem Statement

Tests frequently fail with `InsufficientInstanceCapacity` errors when AWS lacks capacity for the requested instance type in the configured Availability Zone:

```
TestFrameworkEvent Severity.CRITICAL: Failed to provision aws resources: ClientError:
An error occurred (InsufficientInstanceCapacity) when calling the RunInstances operation
(reached max retries: 4): We currently do not have sufficient i8g.2xlarge capacity in the
Availability Zone you requested (eu-west-1a).
```

### Pain Points

1. **No upfront validation**: Provisioning attempts proceed in AZs that don't even offer the requested instance type, wasting time on guaranteed failures
2. **Retries in the same AZ**: The `retrying` decorator retries 4-5 times in the same AZ, adding minutes of delay before ultimately failing with the same error
3. **AZ fallback only for artifact tests**: The existing `aws_fallback_to_next_availability_zone` feature only works in the legacy provisioning path and only wraps DB cluster creation -- loaders and monitors are not covered
4. **No cross-AZ fallback in modern path**: The modern provisioning path (`sdcm/sct_provision/`) has zero AZ fallback logic -- any capacity error is a hard failure
5. **Multi-node tests cannot use AZ fallback**: The existing fallback is effectively limited to single-node artifact tests
6. **Dynamically-added instance types not considered upfront**: `instance_type_db_target` (platform-migration) and `nemesis_grow_shrink_instance_type` are not part of the upfront AZ filter or capacity-reservation builder, so the chosen AZ may end up being one that does not offer the type the test will try to launch later. Mid-test recovery via cross-AZ retry is unsafe (rack/AZ alignment, tablet placement) and often futile (region-wide pressure), so mitigation is prevention-side only: extend upfront filter, extend capacity reservation, optional pre-flight probe. When prevention fails, the test aborts as today

### Relationship to PR #13317

PR #13317 has an implementation plan for multi-cloud provisioning resilience scoped to single-node artifact tests across GCE, Azure, and OCI. This plan is complementary:
- **PR #13317**: Extends AZ/region fallback to other cloud backends for single-node artifact tests
- **This plan**: Makes AZ fallback robust for ALL AWS tests (including multi-node), adds upfront filtering, and fixes both provisioning code paths

Shared elements adopted from PR #13317: backend-agnostic config naming, `fallback_to_next_region` parameter, single-node restriction for region fallback, capacity error keyword patterns. This plan should be implemented first; PR #13317's multi-cloud extension can then build on the centralized utilities created here.

---

## Current State

### Two Provisioning Code Paths

**1. Legacy path** (`sdcm/tester.py` + `sdcm/cluster_aws.py`):
- `sdcm/tester.py` -- `_create_auto_zone_scylla_aws_cluster()` iterates AZs when `aws_fallback_to_next_availability_zone: true`
- Only wraps `ScyllaAWSCluster` creation -- loaders and monitors are NOT covered by fallback
- Only used for artifact tests (effectively single-node)
- Uses `AwsRegion.get_availability_zones_for_instance_type()` (`sdcm/utils/aws_region.py`) to discover valid AZs
- Catches `InsufficientInstanceCapacity` and `Unsupported` errors, tries next AZ

**2. Modern path** (`sdcm/sct_provision/aws/layout.py` + `sdcm/sct_provision/aws/cluster.py`):
- `SCTProvisionAWSLayout.provision()` provisions db, monitor, loader, oracle sequentially
- `ClusterBase.provision()` iterates regions and AZs but has NO fallback logic
- `ClientError` propagates uncaught -- test fails immediately on first capacity error

### Existing Reusable Code

| Code | Location | What It Does |
|------|----------|--------------|
| `AwsRegion.get_availability_zones_for_instance_type()` | `sdcm/utils/aws_region.py` | Queries `describe_instance_type_offerings` for a single instance type |
| `SCTCapacityReservation._get_supported_availability_zones()` | `sdcm/provision/aws/capacity_reservation.py` | Finds AZs supporting ALL required instance types (intersection), orders preferred AZ first |
| `_create_auto_zone_scylla_aws_cluster()` | `sdcm/tester.py` | Legacy AZ fallback loop pattern (try AZ, catch capacity error, try next) |
| `ProvisionPlanBuilder` | `sdcm/provision/common/provision_plan_builder.py` | Handles spot-to-on-demand fallback via provision steps |

### Config Parameters

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `aws_fallback_to_next_availability_zone` | `sdcm/sct_config.py` | `false` | Enable AZ fallback (legacy path only) |
| `instance_provision_fallback_on_demand` | `sdcm/sct_config.py` | `false` | Spot-to-on-demand fallback |
| `availability_zone` | `sdcm/sct_config.py` | `a` | Comma-separated AZ letters |
| `region_name` | `sdcm/sct_config.py` | `eu-west-1` | Space-separated regions |

### Key AWS API Limitation

AWS has no "check real-time capacity" API. `describe_instance_type_offerings` only tells you if an instance type is *offered* in an AZ, not whether capacity is currently available. The only reliable detection method is to attempt provisioning and catch the error. Strategy: **filter what we can upfront, then fallback on actual capacity errors**.

---

## Goals

1. Eliminate provisioning attempts in AZs that do not offer the required instance types (upfront filtering), including types added dynamically mid-test
2. Automatically retry in the next available AZ when provisioning fails with capacity errors, for all test types including multi-node
3. Cover both provisioning code paths (legacy and modern) with the same fallback behavior
4. Enable AZ fallback by default for all AWS tests
5. Prepare backend-agnostic config naming for future multi-cloud support (GCE, Azure, OCI via PR #13317)
6. Optionally support region fallback for single-node artifact tests when all AZs are exhausted
7. Ensure `SCTCapacityReservation` reserves capacity for every instance type the test will use, including dynamically-added ones

---

## Implementation Phases

### Phase 1: Centralize AZ Discovery and Capacity Error Detection

**Importance: Critical** | **Scope: ~100 LOC, 1 PR** | **Dependencies: None**

Extract and unify duplicated AZ filtering logic into reusable utilities.

**Files to modify:**
- `sdcm/utils/aws_region.py` -- Add `get_common_availability_zones(instance_types: list[str], preferred_azs: list[str] | None = None) -> list[str]` method to `AwsRegion`. Computes intersection of AZs supporting ALL instance types, orders preferred AZs first. Reuses existing `describe_instance_type_offerings` call pattern.
- `sdcm/provision/aws/capacity_reservation.py` -- Refactor `_get_supported_availability_zones()` to delegate to the new `AwsRegion.get_common_availability_zones()`, eliminating duplication.

**New file:**
- `sdcm/provision/aws/capacity_errors.py` -- Centralize capacity error detection:
  - `CAPACITY_ERROR_KEYWORDS = ["InsufficientInstanceCapacity", "Unsupported", "InsufficientCapacity"]`
  - `is_capacity_error(exception: Exception) -> bool` -- single source of truth for capacity error identification, replaces inline keyword lists scattered in `tester.py` and decorators

**Definition of Done:**
- [ ] `AwsRegion.get_common_availability_zones()` works for multi-instance-type queries
- [ ] `capacity_reservation.py` delegates to the new method (no behavior change)
- [ ] `is_capacity_error()` correctly identifies all capacity-related `ClientError` variants
- [ ] Unit tests with mocked `describe_instance_type_offerings` responses (intersection logic, preferred ordering, empty result)
- [ ] Unit tests for `is_capacity_error()` with various error strings

---

### Phase 2: Upfront AZ Filtering (Pre-Provisioning Validation)

**Importance: Critical** | **Scope: ~120 LOC, 1 PR** | **Dependencies: Phase 1**

Before provisioning begins, filter configured AZs to only those that support all required instance types. This is a pure optimization -- it only removes AZs that would definitely fail.

**Files to modify:**
- `sdcm/sct_config.py` -- Add config parameter: `pre_filter_unavailable_availability_zones: Boolean` with description "Filter availability zones upfront to only those supporting all required instance types."
- `defaults/test_default.yaml` -- Add `pre_filter_unavailable_availability_zones: true`

**New file:**
- `sdcm/provision/aws/az_resolver.py` -- `AZResolver` class:
  - Accepts `SCTConfiguration` params
  - Extracts ALL instance types needed across all cluster roles, including types added mid-test:
    - `instance_type_db`
    - `instance_type_loader`
    - `instance_type_monitor`
    - `zero_token_instance_type_db`
    - `instance_type_db_oracle`
    - `instance_type_db_target` -- used by platform-migration tests, dynamically added mid-test
    - `nemesis_grow_shrink_instance_type` -- used by grow-shrink nemesis, dynamically added mid-test
  - Skips empty / unset values; only types actually present in params participate in the intersection
  - Per region, calls `AwsRegion.get_common_availability_zones()` from Phase 1
  - Returns validated AZ list, logging warnings for filtered-out AZs
  - For multi-AZ configs (e.g., "a,b,c"), validates each AZ individually and replaces invalid ones with alternatives

**Integration points:**
- `sdcm/sct_provision/aws/layout.py` -- In `provision()`, before any cluster provisioning, call `AZResolver` and update `self._params["availability_zone"]` with validated AZs
- `sdcm/tester.py` -- In `get_cluster_aws()`, before cluster creation, call `AZResolver` and update params

**Definition of Done:**
- [ ] AZs that don't support required instance types are filtered out before provisioning
- [ ] Replacement AZs are found automatically (e.g., "a" replaced with "b" if "a" doesn't support the type)
- [ ] Warning logged when AZs are filtered/replaced
- [ ] If NO valid AZ exists in the region, clear error raised before provisioning
- [ ] Feature disabled when `pre_filter_unavailable_availability_zones: false`
- [ ] Unit tests: mock AWS responses, verify filtering logic, verify replacement logic
- [ ] Unit test where `instance_type_db_target` differs from `instance_type_db` (e.g. ARM target on x86 source) and only some AZs offer the target -- AZResolver returns intersection only
- [ ] Existing tests unaffected (AZs that support all types pass through unchanged)

---

### Phase 3: AZ Fallback on Capacity Errors -- Modern Path

**Importance: Critical** | **Scope: ~150 LOC, 1 PR** | **Dependencies: Phase 1, Phase 2**

When provisioning fails with a capacity error in the modern path, automatically try the next available AZ.

**Files to modify:**

- `sdcm/sct_config.py` -- Config parameter changes:
  - Add `fallback_to_next_availability_zone: Boolean` (new, backend-agnostic name)
  - Keep `aws_fallback_to_next_availability_zone` as deprecated alias (maps to new param)
  - Change default to `true` in `defaults/test_default.yaml`
  - Add `fallback_to_next_region: Boolean` (default `false`, for Phase 5)

- `sdcm/sct_provision/aws/layout.py` -- Wrap the provisioning sequence with AZ fallback:
  - Extract current `provision()` body into `_do_provision()`
  - New `provision()` iterates AZ candidates from `AZResolver.get_fallback_candidates()`
  - On `ClientError` matching `is_capacity_error()`: log warning, clean up partial provisions, try next AZ set
  - On non-capacity errors: re-raise immediately
  - If all AZ candidates exhausted: raise clear error

- `sdcm/provision/aws/az_resolver.py` -- Add `get_fallback_candidates()` method:
  - For single-AZ config (e.g., "a"): returns [["a"], ["b"], ["c"], ...] -- all valid AZs in order
  - For multi-AZ config (e.g., "a,b,c"): returns [["a","b","c"], ["d","b","c"], ...] -- replacing each failing AZ with alternatives
  - Uses upfront-filtered AZ list from Phase 2 as the candidate pool

**Critical concerns addressed:**

1. **Partial provisioning cleanup**: When DB cluster provisions but loader fails, already-provisioned instances must be terminated before retrying in a new AZ. `_cleanup_partial_provision()` handles this.

2. **Cached property invalidation**: `ClusterBase._azs` (`@cached_property`, `sdcm/sct_provision/aws/cluster.py`) caches AZ-dependent data. `_clear_cluster_caches()` deletes cached cluster objects from `self.__dict__`, forcing fresh creation.

3. **Capacity reservation coordination**: `SCTCapacityReservation.reserve()` already does its own AZ fallback. When `use_capacity_reservation` is enabled, skip the general AZ fallback to avoid double-wrapping.

**Definition of Done:**
- [ ] On `InsufficientInstanceCapacity`, provisioning automatically retries in next AZ
- [ ] All cluster types (db, loader, monitor, oracle) move to the new AZ together
- [ ] Partial provisioning is cleaned up before retry
- [ ] Non-capacity errors do NOT trigger fallback
- [ ] Capacity reservation path is not double-wrapped
- [ ] Unit tests: mock provisioner to fail with capacity error on first AZ, succeed on second
- [ ] Unit tests: verify partial cleanup
- [ ] Unit tests: verify non-capacity errors propagate

---

### Phase 4: AZ Fallback on Capacity Errors -- Legacy Path

**Importance: High** | **Scope: ~100 LOC, 1 PR** | **Dependencies: Phase 1, Phase 2**

Extend the existing AZ fallback in the legacy path to cover ALL cluster types (not just DB) and use the centralized utilities.

**Files to modify:**

- `sdcm/tester.py` -- Refactor `get_cluster_aws()`:
  - Replace `_create_auto_zone_scylla_aws_cluster()` with a broader fallback wrapper covering db + loader + monitor creation together
  - Use `AZResolver.get_fallback_candidates()` and `is_capacity_error()` from earlier phases
  - When fallback triggers: tear down partial clusters, regenerate `common_params` with new AZ (since `common_params` contains AZ-specific subnet IDs), retry all clusters
  - Remove `if self.params.get("aws_fallback_to_next_availability_zone")` branch -- fallback is now always enabled by default

- Clean up artifact test configs that explicitly set `aws_fallback_to_next_availability_zone: true` (no longer needed since default changed to `true`):
  - `test-cases/artifacts/ami.yaml`
  - `test-cases/artifacts/oel8.yaml`
  - `test-cases/artifacts/oel9.yaml`
  - `test-cases/artifacts/centos9.yaml`
  - `test-cases/artifacts/ubuntu2004-fips.yaml`
  - `test-cases/artifacts/ubuntu2204.yaml`
  - `test-cases/artifacts/ubuntu2404.yaml`
  - `test-cases/artifacts/amazon2023.yaml`
  - Other files found via `grep -r aws_fallback_to_next_availability_zone test-cases/`

**Definition of Done:**
- [ ] Legacy path retries all cluster types together on capacity errors
- [ ] `_create_auto_zone_scylla_aws_cluster()` removed or deprecated
- [ ] Centralized `is_capacity_error()` and `AZResolver` used consistently
- [ ] Artifact test configs cleaned up
- [ ] Old `aws_fallback_to_next_availability_zone` param still works (backward compat via alias)
- [ ] Unit tests for the refactored legacy fallback flow

---

### Phase 5: Region Fallback (Future, Optional)

**Importance: Low** | **Scope: ~200 LOC, 1 PR** | **Dependencies: Phase 3, Phase 4**

When all AZs in a region are exhausted, try a different region. Scoped to single-node artifact tests (consistent with PR #13317's approach).

**Constraint: `n_db_nodes=1` only** -- Multi-node region fallback creates split-region clusters with 50-200ms+ cross-region latency and unpredictable behavior.

**Files to modify:**
- `sdcm/sct_config.py` -- Enable `fallback_to_next_region` (already added in Phase 3)
- `sdcm/provision/aws/az_resolver.py` -- Extend to generate cross-region candidates
- `sdcm/sct_provision/aws/layout.py` -- Outer loop around region selection
- `sdcm/tester.py` -- Outer loop around region selection in legacy path

**Prerequisites:** AMI must be available in fallback region (via SSM parameter resolution or multi-region AMI config). VPC/subnet infrastructure must exist (from `prepare-aws-region`).

**Definition of Done:**
- [ ] When all AZs exhausted and `fallback_to_next_region: true`, tries next region
- [ ] Validates AMI availability in fallback region before attempting
- [ ] Validates `n_db_nodes=1` -- clear error if multi-node tries region fallback
- [ ] Unit tests for region fallback flow

---

### Phase 6: Extend `SCTCapacityReservation` Request Builder

**Importance: Important** | **Scope: ~30 LOC, 1 PR** | **Dependencies: Phase 1**

The same instance-type omission pattern applies to `_get_cr_request_based_on_sct_config`. Today it reserves only `instance_type_db`, `nemesis_grow_shrink_instance_type` (already covered), `zero_token_instance_type_db`, `instance_type_loader`. Missing: `instance_type_db_target`, `instance_type_db_oracle`. Tests that opt into CR but use these types still hit `InsufficientInstanceCapacity` mid-test. Reserving these types upfront prevents the failure rather than reacting to it.

**Files to modify:**
- `sdcm/provision/aws/capacity_reservation.py` -- Extend `_get_cr_request_based_on_sct_config` (line ~39) to include:
  - `instance_type_db_target` x peak concurrent target-node count. For platform-migration this is the count of target-arch nodes that coexist with source nodes at peak. Reserve `n_db_nodes` (matches the source count, since migration replaces source 1:1 at peak overlap) -- NOT `cluster_target_size or n_db_nodes`, which would double-book against the existing `instance_type_db` reservation. If `cluster_target_size` differs from `n_db_nodes` for a specific test, that test should set its own override; default to `n_db_nodes`.
  - `instance_type_db_oracle` x `n_test_oracle_db_nodes`, when set
  - Existing types unchanged
- Confirm `SCTCapacityReservation._create()` is called for every type in the request dict (already true).

**Needs Investigation -- migration-overlap assumption:** This phase assumes platform-migration runs source and target nodes concurrently during the migration window (each target replaces one source 1:1 before the source is decommissioned). Confirm with the platform-migration test author before merging. If migration drains source before adding target (no overlap), `instance_type_db_target` reservation can be smaller (batch_size, not full cluster); document the chosen overlap semantics in `_get_cr_request_based_on_sct_config` docstring.

**Notes:**
- Pure enabler -- does nothing for tests that do not set `use_capacity_reservation: true`.
- Does NOT change which tests opt into CR. Platform-migration test should NOT opt in by default (functional test, not a perf test); CR pins single AZ and costs money for unused reserved capacity.

**Definition of Done:**
- [ ] Unit test passing config with both source and target types -- both reserved
- [ ] Unit test passing config with oracle node count > 0 -- oracle type reserved
- [ ] Existing CR tests unchanged
- [ ] No regression: tests that do not set the new types are unaffected

---

### Phase 7: Pre-flight Capacity Probe for Dynamically-Added Types

**Importance: Important** | **Scope: ~50 LOC, 1 PR** | **Dependencies: Phase 2**

For tests with long stress phases that grow the cluster mid-run (platform-migration, upgrade tests, grow-shrink-driven longevities) -- fail-fast at setup time if a dynamically-added type is unavailable region-wide. Avoids wasting hours of stress only to fail at the migration / grow step. With mid-test reactive fallback ruled out (rack/AZ alignment, tablet placement), pre-flight probing becomes the primary proactive defense for dynamic-add types beyond what `describe_instance_type_offerings` (offered != available) and capacity reservation (only when opted in) cover.

**Approach:**
- After AZResolver runs at setup, for each dynamically-added type (`instance_type_db_target`, `nemesis_grow_shrink_instance_type` when set), attempt one short on-demand spin-then-terminate of a single instance in the chosen AZ. If `InsufficientInstanceCapacity`, trigger AZ fallback (initial-provisioning fallback from Phases 3-4) before the stress phase begins.
- AWS `DryRun=True` only validates IAM/params; it does NOT probe live capacity. A real spin-then-terminate is the only way to detect actual availability. Cost: ~1 minute of one instance per probed type.
- Gated behind config parameter `pre_flight_capacity_probe: Boolean` (default `false`). Enable in long-running test classes (platform-migration, upgrade-with-grow) only.

**Files to modify:**
- `sdcm/sct_config.py` -- Add `pre_flight_capacity_probe: Boolean`, default `false`
- `sdcm/provision/aws/az_resolver.py` -- Add `probe_capacity_for_types(types: list[str], az: str) -> dict[str, bool]`
- Integrate into `SCTProvisionAWSLayout.provision()` setup-time hook and `tester.py` get_cluster_aws() equivalent

**Definition of Done:**
- [ ] Probe runs at setup when enabled
- [ ] Probe failure triggers AZ fallback before stress phase begins
- [ ] Probe results logged
- [ ] Unit tests with mocked spin-then-terminate succeeding/failing
- [ ] Disabled by default; opt-in documented in `docs/configuration_options.md`

---

## Testing Requirements

### Unit Tests (all phases)
- Mock `describe_instance_type_offerings` responses for AZ filtering
- Mock boto3 `create_instances` to raise `ClientError` with `InsufficientInstanceCapacity`
- Verify fallback candidate generation for single-AZ and multi-AZ configs
- Verify partial cleanup on failure during initial provisioning
- Verify non-capacity errors propagate without fallback
- Verify backward compatibility with old `aws_fallback_to_next_availability_zone` param
- Verify `instance_type_db_target` and `nemesis_grow_shrink_instance_type` participate in upfront AZ filter when set
- Verify `SCTCapacityReservation` reserves `instance_type_db_target` and `instance_type_db_oracle` when configured
- Verify pre-flight probe spin-then-terminate succeeds and fails as expected when enabled

### Integration Tests
- Use `moto` to simulate AWS EC2 with capacity errors in specific AZs
- Test the full provisioning flow through `SCTProvisionAWSLayout.provision()`

### Manual Validation
- Run an artifact test with `availability_zone: a` where `a` is known to have capacity issues for the instance type
- Verify it automatically falls back to another AZ
- Verify logging clearly shows the AZ transition
- Run a platform-migration test (`x86-to-arm-batched.yaml`) with `pre_flight_capacity_probe: true` in an AZ known to be tight on the target instance type
- Verify the probe detects the shortage at setup and AZ fallback selects a different AZ before the stress phase begins

---

## Success Criteria

1. Tests that previously failed with `InsufficientInstanceCapacity` automatically recover by trying alternative AZs
2. AZ fallback works for both single-node and multi-node tests
3. Both legacy and modern provisioning paths have consistent fallback behavior
4. Upfront filtering eliminates AZs that do not offer the required instance types before any provisioning attempt -- including types added dynamically mid-test
5. No orphaned instances left behind when initial-provisioning fallback triggers
6. All existing tests continue to work without configuration changes
7. `SCTCapacityReservation` reserves capacity for every instance type the test will use, including `instance_type_db_target` and `instance_type_db_oracle`
8. Pre-flight capacity probe (when enabled) detects region-wide shortage of dynamically-added types at setup, before stress phases consume hours

---

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Partial provisioning cleanup fails -- orphaned instances left running | Medium | High (cost) | Track provisioned instance IDs; wrap in try/finally; add safety check in test teardown |
| Cached property staleness -- `_azs` returns old value after AZ change | High | High | Clear `__dict__` entries for cached properties before retry; create fresh cluster objects |
| Placement group incompatibility -- placement groups are AZ-scoped | Low | Medium | Skip placement groups when doing AZ fallback, or recreate in new AZ |
| Capacity reservation double-fallback -- both CR and general fallback try AZ iteration | Medium | Low | When `use_capacity_reservation` is enabled, skip general AZ fallback |
| Subnet missing in fallback AZ -- `prepare-aws-region` did not create subnets for all AZs | Low | High | Validate subnet existence in `AZResolver` before returning AZ as candidate |
| Backward compatibility -- tests using old `aws_fallback_to_next_availability_zone` param | Low | Low | Keep old param as deprecated alias mapping to new param |
| Mid-test capacity error wastes hours of stress before failing | Medium | High (time) | Prevent via Phase 2 (extended type list) + Phase 6 (CR for all types) + Phase 7 (pre-flight probe). Mid-test recovery via cross-AZ retry rejected: rack/AZ misalignment risks tablet/topology drift, and region-wide pressure makes alternate AZ likely to fail too. Test aborts as today when prevention fails |
| Region-wide exhaustion cannot be mitigated by AZ fallback | Low (transient) / Medium (peak hours) | High | Phase 5 region fallback for single-node; multi-node tests must accept rerun; Phase 7 pre-flight probe surfaces the failure at setup rather than mid-test |
