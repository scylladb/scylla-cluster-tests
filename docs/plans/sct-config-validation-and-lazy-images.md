# SCT Configuration: Extract Validation & Lazy Image Resolution

## Problem Statement

[PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) migrated `SCTConfiguration` from a custom `dict`-based class to a Pydantic `BaseModel`, providing type-safe field definitions and automatic validation. However, the `__init__` method remains a ~470-line monolith that mixes configuration loading, cloud image resolution (calling external AWS/GCE/Azure APIs), and inline validation. This makes the constructor fragile, hard to test, and impossible to instantiate without network access.

Key pain points identified during the PR #13104 review ([comment by @soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971791016), [response by @pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)):

1. **Validation in `__init__`**: ~180 lines of inline validation checks (steps 11–21 plus performance params) live inside `__init__`, making the constructor fragile and impossible to test validators in isolation.
2. **Image resolution in `__init__`**: ~150 lines call external cloud APIs (AWS AMI lookup, GCE image lookup, Azure image lookup) during construction, preventing lightweight instantiation for utilities, unit tests, or CI tooling.

This plan covers the first two follow-up phases agreed upon in the [PR #13845 discussion](https://github.com/scylladb/scylla-cluster-tests/pull/13845). The later phases (package split, typed attribute access, nested config structure) are tracked separately.

## Current State

### File: `sdcm/sct_config.py` (~3,883 lines)

> **Note**: Line numbers reference the current `master` branch at the time of writing. They will shift as changes are merged, but the logical sections remain the same.

**Class**: `SCTConfiguration(BaseModel)` — line 440

**`__init__` method** (lines 2300–2766, ~470 lines) performs these steps sequentially:

1. **Lines 2308–2328**: Initialize via `super().__init__()`, load environment variables, determine backend, load default config files
2. **Lines 2330–2337**: Load user-provided YAML config files
3. **Lines 2339–2364**: Handle region data for AWS/GCE/Azure
4. **Lines 2366–2396**: Merge environment variables, set billing project, configure event severities
5. **Lines 2398–2401**: Convert AMI names to IDs (`convert_name_to_ami_if_needed`)
6. **Lines 2403–2502**: **Image resolution** — resolve `scylla_version` to Docker image, AWS AMI, GCE image, Azure image, or repo URL by calling external cloud APIs
7. **Lines 2504–2530**: **Oracle image resolution** — resolve `oracle_scylla_version` to AMIs (step 6.1)
8. **Lines 2532–2550**: **Vector Store image resolution** — resolve `vector_store_version` to AMIs (step 6.2)
9. **Lines 2552–2559**: Support lookup of repos for upgrade tests (step 7)
10. **Lines 2561–2585**: Resolve repo symlinks, build `user_prefix` (steps 8–9)
11. **Lines 2587–2765**: **Inline validation** — steps 11–21 plus performance throughput validation and random driver selection

**Inline validation blocks** (lines 2587–2765):

| Step | Lines | Description |
|------|-------|-------------|
| 11 | 2587–2589 | `instance_provision` allowed values check |
| 12 | 2591–2605 | Authenticator + alternator params cross-check |
| 13 | 2607–2623 | `stress_duration` / `prepare_stress_duration` integer coercion |
| 14 | 2625–2638 | `run_fullscan` format validation |
| 15 | 2640–2653 | `endpoint_snitch` + simulated regions/racks enforcement |
| 16 | 2655–2658 | `use_dns_names` backend check |
| 17 | 2660–2697 | `scylla_network_config` validation |
| 18 | 2699–2701 | K8S TLS + SNI cross-check |
| — | 2703–2704 | Capacity reservation + dedicated hosts |
| — | 2706–2725 | Zero token nodes validation |
| 21 | 2727–2761 | Performance throughput params validation |
| — | 2763–2765 | Random `c_s_driver_version` selection |

**Image resolution functions called from `__init__`:**
- `convert_name_to_ami_if_needed` (line 2401) — from `sdcm/utils/common.py`
- `get_scylla_ami_versions` (lines 2429, 2435) — from `sdcm/utils/aws_utils.py`
- `get_branched_ami` (line 2432) — from `sdcm/utils/aws_utils.py`
- `get_scylla_gce_images_versions` (lines 2449, 2456) — from `sdcm/utils/gce_utils.py`
- `get_branched_gce_images` (line 2452) — from `sdcm/utils/gce_utils.py`
- `azure_utils.get_scylla_images` (lines 2471, 2476) — from `sdcm/utils/azure_utils.py`
- `azure_utils.get_released_scylla_images` (line 2481) — from `sdcm/utils/azure_utils.py`
- `get_vector_store_ami_versions` (line 2541) — from `sdcm/utils/aws_utils.py`
- `parse_scylla_version_tag` (lines 2427, 2447, 2469) — from `sdcm/utils/version_utils.py`

**Orchestrator** (line 3875):
```python
def init_and_verify_sct_config() -> SCTConfiguration:
    sct_config = SCTConfiguration()
    sct_config.log_config()
    sct_config.verify_configuration()
    sct_config.verify_configuration_urls_validity()
    sct_config.get_version_based_on_conf()
    sct_config.update_config_based_on_version()
    sct_config.check_required_files()
    return sct_config
```

**Existing `verify_configuration()`** (line 3056): A separate validation method already exists and runs *after* `__init__`. It delegates to helpers like `_check_unexpected_sct_variables()`, `_validate_sct_variable_values()`, `_check_per_backend_required_values()`, etc. The inline validation in `__init__` is *in addition to* this — it duplicates the "validate during construction" pattern rather than using Pydantic's declarative validator system.

### Unit test mocking

Unit tests already mock cloud APIs via the `mock_cloud_services` autouse fixture in `unit_tests/conftest.py` (session-scoped). This fixture patches `KeyStore`, `convert_name_to_ami_if_needed`, `get_scylla_ami_versions`, and other cloud functions to prevent network calls. After Phase 2, many of these mocks would become unnecessary for basic configuration tests.

## Goals

1. **Declarative validation (no network calls)**: Inline validation blocks in `__init__` (steps 11–21) that perform **pure data checks** (no network/cloud API calls) are replaced by Pydantic `@field_validator` / `@model_validator` methods. Any validation that requires network access stays out of Pydantic validators and runs in its own explicit stage (e.g., inside `resolve_images()` or `verify_configuration()`).
2. **Lean constructor**: `__init__` should only load and merge configuration data — no inline validation beyond what Pydantic handles declaratively.
3. **Disableable validators for testing**: Provide a mechanism to skip or disable Pydantic validators when constructing `SCTConfiguration` in tests, so unit tests can create config objects with partial/invalid data without triggering cross-field validation.
4. **Lazy image resolution**: Cloud image lookups (AWS AMI, GCE images, Azure images) happen in an explicit `resolve_images()` step, not during construction. `SCTConfiguration()` can be instantiated without network access.
5. **Testable in isolation**: Configuration can be constructed for unit tests without mocking cloud APIs for image resolution.
6. **Incremental migration**: Each phase is a standalone PR that doesn't break existing functionality.

## Implementation Phases

### Phase 1: Extract Validation from `__init__` into Pydantic Validators

**Objective**: Move all inline validation blocks (steps 11–21 in `__init__`) into proper Pydantic `@model_validator` and `@field_validator` methods, making them declarative and independently testable. **Only pure data validators** (no network/cloud API calls) become Pydantic validators.

**Constraint — no network calls in validators**: All 11 validation blocks (steps 11–21) have been audited and confirmed to be pure data checks — none make network calls. Any future validation that requires network access must **not** be added as a `@field_validator` or `@model_validator`; it should live in a separate explicit stage (e.g., `resolve_images()` or `verify_configuration()`).

**What moves out of `__init__`:**

| Current Step | Lines | Validation | Target |
|-------------|-------|------------|--------|
| Step 11 | 2587–2589 | `instance_provision` allowed values | `Literal["spot", "on_demand", "spot_fleet"]` type annotation |
| Step 12 | 2591–2605 | Authenticator + alternator params cross-check | `@model_validator(mode='after')` |
| Step 13 | 2607–2623 | `stress_duration` / `prepare_stress_duration` integer coercion | `@field_validator` or `BeforeValidator` |
| Step 14 | 2625–2638 | `run_fullscan` format validation | `@field_validator` |
| Step 15 | 2640–2653 | `endpoint_snitch` + simulated regions/racks | `@model_validator(mode='after')` |
| Step 16 | 2655–2658 | `use_dns_names` backend check | `@model_validator(mode='after')` |
| Step 17 | 2660–2697 | `scylla_network_config` validation | `@field_validator` or dedicated sub-model |
| Step 18 | 2699–2701 | K8S TLS + SNI cross-check | `@model_validator(mode='after')` |
| — | 2706–2725 | Zero token nodes validation | `@model_validator(mode='after')` |
| Step 21 | 2727–2761 | Performance throughput params | `@model_validator(mode='after')` |

**Implementation approach:**
- Use `@field_validator` for single-field validations (steps 13, 14)
- Use `@model_validator(mode='after')` for cross-field validations (steps 12, 15, 16, 18, 21, zero-token)
- Replace string-based choices with `Literal` types where possible (step 11)
- Keep the validation logic identical — move it from imperative `__init__` code to declarative validators
- Preserve existing error messages to avoid breaking any downstream error handling

**Disabling validators for testing:**
Provide a way to construct `SCTConfiguration` in tests without triggering Pydantic validators. Recommended approach — use `model_construct()`:

```python
# In tests: bypass all validators (including field/model validators)
config = SCTConfiguration.model_construct(**partial_data)

# In production: normal construction with full validation
config = SCTConfiguration(**data)
```

Pydantic v2's `model_construct()` skips all validation entirely, which is ideal for unit tests that need to set up config objects with partial or intentionally invalid data. For tests that need *some* validators to run but not others, a class-level flag (e.g., `_skip_cross_field_validation: ClassVar[bool] = False`) can be checked inside `@model_validator` methods and toggled in test fixtures.

**Relationship with `verify_configuration()`:**
The existing `verify_configuration()` method (line 3056) runs *after* `__init__` and handles a different set of concerns: checking for unexpected SCT variables, validating variable value types against schema, checking backend-specific required values, and verifying Docker/xcloud parameters. The inline validation blocks in `__init__` (steps 11–21) are *complementary* — they check field value constraints and cross-field consistency. After Phase 1, the split remains: Pydantic validators handle field-level and cross-field validation declaratively, while `verify_configuration()` continues to handle schema-level and backend-specific checks that depend on the fully-loaded configuration.

**Items that stay in `__init__`:**
- Lines 2703–2704 (`SCTCapacityReservation.get_cr_from_aws`, `SCTDedicatedHosts.reserve`) — these are side-effectful resource reservations, not validation
- Lines 2763–2765 (random `c_s_driver_version` selection) — this is configuration mutation, not validation

**Definition of Done:**
- [ ] All 11 validation blocks (steps 11–21) removed from `__init__`
- [ ] Equivalent Pydantic validators added with identical error messages
- [ ] No Pydantic validator makes network/cloud API calls (enforced by convention + documented)
- [ ] `model_construct()` works for tests that need to bypass validation
- [ ] `__init__` reduced by ~180 lines (validation removed, loading + image resolution remains)
- [ ] Existing unit tests in `unit_tests/test_config.py` pass without changes
- [ ] New unit tests validate each extracted validator independently (valid and invalid inputs)
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: None (PR #13104 — Pydantic BaseModel foundation — is already merged)

---

### Phase 2: Make Image Resolution Lazy / Explicit

**Objective**: Extract cloud image resolution (AMI, GCE, Azure lookups) **and** version detection from `__init__` and `get_version_based_on_conf()` into a single `resolve_images()` method, so `SCTConfiguration` can be constructed without network access.

**What moves out of `__init__`:**

| Current Step | Lines | Resolution | Target |
|-------------|-------|-----------|--------|
| Step 5 | 2398–2401 | AMI name → ID conversion | `resolve_images()` |
| Step 6 | 2403–2502 | `scylla_version` → Docker/AMI/GCE/Azure image | `resolve_images()` |
| Step 6.1 | 2504–2530 | `oracle_scylla_version` → AMI | `resolve_images()` |
| Step 6.2 | 2532–2550 | `vector_store_version` → AMI | `resolve_images()` |
| Step 7 | 2552–2559 | `new_version` → repo lookup | `resolve_images()` |
| Step 8 | 2561–2567 | Repo symlink resolution | `resolve_images()` |

**What gets consolidated from `get_version_based_on_conf()`** (line 3405):

`get_version_based_on_conf()` reads cloud image tags (AWS AMI tags via `get_ami_tags`, GCE image tags via `get_gce_image_tags`, Azure image tags via `azure_utils.get_image_tags`) to determine the Scylla version and whether it's enterprise. This is tightly coupled with image resolution — it depends on resolved image IDs and also makes cloud API calls. It should be consolidated into `resolve_images()` so all cloud-provider lookups live in one place.

| Method | Lines | Resolution | Target |
|--------|-------|-----------|--------|
| `get_version_based_on_conf` | 3405–3491 | Image tags → `scylla_version` + `is_enterprise` | `resolve_images()` |

**Implementation approach** — Explicit `resolve_images()` method (recommended):

```python
class SCTConfiguration(BaseModel):
    def __init__(self, /, **data):
        # Only load and merge YAML/env configuration (steps 1–4, step 9)
        ...

    def resolve_images(self):
        """Resolve version strings to cloud provider image IDs and detect version.

        Consolidates all cloud API calls:
        - Image resolution from __init__ (steps 5–8)
        - Version detection from get_version_based_on_conf()

        Call this explicitly when cloud image resolution is needed.
        Not called during unit tests or utility usage.
        """
        self._resolve_ami_names()
        self._resolve_scylla_images()
        self._resolve_oracle_images()
        self._resolve_vector_store_images()
        self._resolve_upgrade_repos()
        self._resolve_repo_symlinks()
        self._resolve_version_from_images()   # <-- was get_version_based_on_conf()
```

Update the orchestrator to call `resolve_images()` explicitly:

```python
def init_and_verify_sct_config() -> SCTConfiguration:
    sct_config = SCTConfiguration()
    sct_config.resolve_images()     # <-- consolidates image resolution + version detection
    sct_config.log_config()
    sct_config.verify_configuration()
    sct_config.verify_configuration_urls_validity()
    sct_config.update_config_based_on_version()
    sct_config.check_required_files()
    return sct_config
```

Note: `get_version_based_on_conf()` is removed from the orchestrator — its logic moves into `resolve_images()` as `_resolve_version_from_images()`. The `update_config_based_on_version()` call remains since it operates on the already-resolved version data.

**Why explicit over lazy**: An explicit `resolve_images()` method makes it clear when network calls happen, keeps the resolution order deterministic, and allows `init_and_verify_sct_config()` to control the flow. Lazy properties would hide network calls behind attribute access and make debugging harder.

**Definition of Done:**
- [ ] `__init__` contains no calls to external cloud APIs (AWS, GCE, Azure)
- [ ] `__init__` is reduced to ~150 lines (config loading and merging only)
- [ ] `SCTConfiguration()` can be instantiated without network access or cloud API mocks
- [ ] New `resolve_images()` method contains all image resolution logic
- [ ] `get_version_based_on_conf()` logic consolidated into `resolve_images()` as `_resolve_version_from_images()`
- [ ] `init_and_verify_sct_config()` calls `resolve_images()` explicitly (no separate `get_version_based_on_conf()` call)
- [ ] Unit tests can create `SCTConfiguration` without mocking cloud APIs for image resolution
- [ ] Integration tests verify that image resolution and version detection still work end-to-end
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: Phase 1 (clean `__init__` — validation already extracted)

## Testing Requirements

### Phase 1: Validation Extraction

**Unit tests:**
- Test each extracted `@field_validator` independently with valid and invalid inputs
- Test each `@model_validator` with combinations of fields that trigger cross-field checks
- Verify that error messages match the current inline validation messages exactly
- Parametrized tests for multi-case validators (e.g., `run_fullscan` with valid JSON, invalid JSON, empty list)

**Regression tests:**
- All existing `unit_tests/test_config.py` tests pass unchanged
- `uv run sct.py unit-tests` passes
- `uv run sct.py pre-commit` passes

### Phase 2: Lazy Image Resolution

**Unit tests:**
- Test that `SCTConfiguration()` can be instantiated without any cloud API mocks (beyond what Pydantic field defaults require)
- Test that `resolve_images()` correctly calls cloud APIs and sets image fields
- Test that `resolve_images()` raises appropriate errors for invalid versions
- Test that skipping `resolve_images()` leaves image fields at their default values
- Test that `_resolve_version_from_images()` (consolidated from `get_version_based_on_conf`) correctly detects version and enterprise status per backend

**Integration tests:**
- `init_and_verify_sct_config()` still works end-to-end with Docker backend
- At least one artifact test (AWS or Docker) to verify end-to-end config loading
- Verify that version detection (`artifact_scylla_version`, `is_enterprise`) is correct after `resolve_images()`

**Manual tests:**
- Verify AWS/GCE/Azure artifact tests still resolve images correctly
- Verify that longevity tests work with the new flow

## Success Criteria

1. **`__init__` under ~150 lines** — contains only configuration file loading and merging
2. **All validation is declarative and network-free** — Pydantic `@field_validator` / `@model_validator` methods with no network/cloud API calls; not inline `if/raise` blocks
3. **Validators disableable for testing** — `model_construct()` bypasses all validation; optional `ClassVar` flag for selective disabling
4. **`SCTConfiguration()` instantiable without network access** — no cloud API calls during construction
5. **Existing tests pass** without modifications (beyond test-specific config access updates)
6. **Each phase is a standalone PR** — can be reviewed and merged independently

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Pydantic validator execution order differs from `__init__` order | Medium | Pydantic v2 runs `@field_validator` in definition order, `@model_validator(mode='after')` after all fields. Verify with tests that cross-field validators see the expected state. |
| Validators running on every assignment (`validate_assignment=True`) | Medium | `model_config` has `validate_assignment=True`. Ensure extracted validators handle partial state gracefully (e.g., during `merge_dicts_append_strings` updates in `__init__`). May need to temporarily disable validation during bulk loading. |
| Network-calling code accidentally added as Pydantic validator | High | Document the "no network calls in validators" rule in code comments and plan. Code review must enforce this. Any validation requiring network access goes into `resolve_images()` or `verify_configuration()`. |
| Tests need config objects with partial/invalid data | Medium | Use `model_construct()` to bypass all Pydantic validation in tests. For selective disabling, add a `ClassVar` flag checked by `@model_validator` methods. |
| `resolve_images()` called too late or not at all | High | Add a guard in image-accessing code paths (e.g., `verify_configuration()`) that checks whether resolution has been performed. Document the required call order in `init_and_verify_sct_config()`. |
| Breaking error messages that downstream tools parse | Low | Preserve exact error message strings. Add tests that assert on error message content. |
| Merge conflicts with parallel development on `sct_config.py` | High | Small, focused PRs. Coordinate with team on merge order. Phase 1 before Phase 2. |
| `mock_cloud_services` fixture changes affect other tests | Medium | Phase 2 *simplifies* the fixture (fewer mocks needed), but verify that all tests still pass during the transition. |
