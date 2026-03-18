---
status: draft
domain: config
created: 2026-03-04
last_updated: 2026-03-19
owner: fruch
---

# SCT Configuration: Extract Validation & Lazy Image Resolution

## Problem Statement

[PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) migrated `SCTConfiguration` from a custom `dict`-based class to a Pydantic `BaseModel`, providing type-safe field definitions and automatic validation. However, the `__init__` method remains a ~470-line monolith that mixes configuration loading, cloud image resolution (calling external AWS/GCE/Azure APIs), and inline validation. This makes the constructor fragile, hard to test, and impossible to instantiate without network access.

Key pain points identified during the PR #13104 review ([comment by @soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971791016), [response by @pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)):

1. **Validation in `__init__`**: ~180 lines of inline validation checks (steps 11–21 plus performance params) live inside `__init__`, making the constructor fragile and impossible to test validators in isolation.
2. **Image resolution in `__init__`**: ~150 lines call external cloud APIs (AWS AMI lookup, GCE image lookup, Azure image lookup) during construction, preventing lightweight instantiation for utilities, unit tests, or CI tooling.

This plan covers the first two follow-up phases agreed upon in the [PR #13845 discussion](https://github.com/scylladb/scylla-cluster-tests/pull/13845). The later phases (package split, typed attribute access, nested config structure) are tracked separately.

## Current State

### File: `sdcm/sct_config.py` (~4,053 lines)

> **Note**: Line numbers reference `upstream/master` at commit `8f6fcb600` (2026-03-19). They will shift as changes are merged, but the logical sections remain the same.

**Class**: `SCTConfiguration(BaseModel)` — line 441

**`__init__` method** (lines 2375–2896, ~522 lines) performs these steps sequentially:

1. **Lines 2383–2404**: Initialize via `super().__init__()`, load environment variables, determine backend, load default config files, load docker image defaults
2. **Lines 2406–2412**: Load user-provided YAML config files
3. **Lines 2418–2442**: Handle region data for AWS/GCE, merge environment variables
4. **Lines 2444–2471**: Set billing project (from JOB_NAME, git branch, or default), configure event severities. **Note**: billing project detection runs `LOCALRUNNER.run("git rev-parse ...")` — a local subprocess call.
5. **Lines 2473–2476**: Convert AMI names to IDs (`convert_name_to_ami_if_needed`)
6. **Lines 2478–2632**: **Image resolution** — resolve `scylla_version` to Docker image, AWS AMI, GCE image, Azure image, OCI image, xcloud version, or repo URL by calling external cloud APIs (~155 lines). Includes auto-discovery of target images for platform migration (lines 2601–2632).
7. **Lines 2634–2660**: **Oracle image resolution** — resolve `oracle_scylla_version` to AMIs (step 6.1)
8. **Lines 2662–2680**: **Vector Store image resolution** — resolve `vector_store_version` to AMIs (step 6.2)
9. **Lines 2682–2689**: Support lookup of repos for upgrade tests (step 7)
10. **Lines 2691–2718**: Resolve repo symlinks, build `user_prefix` (steps 8–9)
11. **Lines 2717–2895**: **Inline validation** — steps 11–18, zero-token nodes validation, step 21, and random driver selection

**Inline validation blocks** (lines 2717–2895):

| Step | Lines | Description | Notes |
|------|-------|-------------|-------|
| 11 | 2717–2719 | `instance_provision` allowed values check | **Redundant** — field already has `Literal["spot", "on_demand", "spot_fleet", "spot_low_price"]` at line 746. But runtime check only allows 3 of 4 values (excludes `spot_low_price`). See note below. |
| 12 | 2721–2735 | Authenticator + alternator params cross-check | |
| 13 | 2737–2753 | `stress_duration` / `prepare_stress_duration` integer coercion | |
| 14 | 2755–2768 | `run_fullscan` format validation | |
| 15 | 2770–2783 | `endpoint_snitch` + simulated regions/racks (mutating + **uses `assert`**) | Must replace `assert` with `raise ValueError` |
| 16 | 2785–2788 | `use_dns_names` backend check | |
| 17 | 2790–2827 | `scylla_network_config` validation | |
| 18 | 2829–2831 | K8S TLS + SNI cross-check | |
| — | 2833–2834 | Capacity reservation + dedicated hosts | **Not validation** — cloud API resource reservation calls. Makes network calls. |
| — | 2836–2855 | Zero token nodes validation (**uses `assert`**) | Must replace `assert` with `raise ValueError` |
| 21 | 2857–2891 | Performance throughput params validation | |
| — | 2893–2895 | Random `c_s_driver_version` selection | **Not validation** — config mutation |

**Step 11 note — `instance_provision` Literal discrepancy (RESOLVED)**: The field type at line 746 is `Literal["spot", "on_demand", "spot_fleet", "spot_low_price"]` (4 values), but the runtime check at line 2718 only allows `["spot", "on_demand", "spot_fleet"]` (3 values — excludes `spot_low_price`). `"spot_low_price"` appears nowhere else in the codebase (no `.py` or `.yaml` files use it), and the field description also only lists 3 values. **Decision**: Drop `"spot_low_price"` from the Literal (it was a mistake during the Pydantic migration in PR #13104). Then remove the redundant runtime check entirely — the corrected Literal type handles validation. Update the Literal to `Literal["spot", "on_demand", "spot_fleet"]`.

**Image resolution functions called from `__init__`:**
- `convert_name_to_ami_if_needed` (line 2476) — from `sdcm/utils/common.py`
- `get_scylla_ami_versions` (lines 2504, 2510) — from `sdcm/utils/aws_utils.py`
- `get_branched_ami` (line 2507) — from `sdcm/utils/aws_utils.py`
- `get_scylla_gce_images_versions` (lines 2524, 2531) — from `sdcm/utils/gce_utils.py`
- `get_branched_gce_images` (line 2527) — from `sdcm/utils/gce_utils.py`
- `azure_utils.get_scylla_images` (lines 2546, 2551) — from `sdcm/utils/azure_utils.py`
- `azure_utils.get_released_scylla_images` (line 2556) — from `sdcm/utils/azure_utils.py`
- `oci_utils.get_scylla_images` (line 2579) — from `sdcm/utils/oci_utils.py` (**not covered in original plan**)
- `get_vector_store_ami_versions` (line 2671) — from `sdcm/utils/aws_utils.py`
- `parse_scylla_version_tag` (lines 2502, 2522, 2544) — from `sdcm/utils/version_utils.py`
- `get_arch_from_instance_type` (lines 2499, 2607) — from `sdcm/utils/aws_utils.py`
- `self._resolve_xcloud_version_tag` (line 2593) — instance method for xcloud backend

**Orchestrator** (line 4045):
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

**Existing `verify_configuration()`** (line 3194): A separate validation method already exists and runs *after* `__init__`. It delegates to helpers like `_check_unexpected_sct_variables()`, `_validate_sct_variable_values()`, `_check_per_backend_required_values()`, etc. The inline validation in `__init__` is *in addition to* this — it duplicates the "validate during construction" pattern rather than using Pydantic's declarative validator system.

### Unit test mocking

Unit tests already mock cloud APIs via the `mock_cloud_services` fixture in `unit_tests/conftest.py` (session-scoped, injected by `pytest_collection_modifyitems` for all non-integration tests). This fixture patches:
- `convert_name_to_ami_if_needed` — returns the input unchanged (no SSM lookup)
- `find_scylla_repo` — returns a plausible URL without S3 access
- `get_s3_scylla_repos_mapping` — returns empty dict
- `get_arch_from_instance_type` — returns `"x86_64"` without EC2 API call
- `KeyStore` — fakes `get_file_contents`, `get_json`, `get_ssh_key_pair`, `download_file`, `sync`, `s3`, `s3_client`
- `HOME` — redirected to a temp directory with dummy SSH key files

Note: this fixture does **not** mock `get_scylla_ami_versions`, `get_branched_ami`, `get_scylla_gce_images_versions`, or other AMI/GCE/Azure/OCI image lookup functions. These are not mocked because the default test backend is `docker`, which skips those code paths entirely. Tests that set `cluster_backend` to AWS/GCE must either provide explicit `ami_id_*` values (bypassing image lookup) or mock those functions individually.

After Phase 2, `SCTConfiguration()` would not need any of these image-resolution mocks since `resolve_images()` is called separately. The `mock_cloud_services` fixture would still be needed for `KeyStore`, `find_scylla_repo`, and credential mocking.

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

| Current Step | Lines | Validation | Target | Notes |
|-------------|-------|------------|--------|-------|
| Step 11 | 2717–2719 | `instance_provision` allowed values | Remove runtime check — **field already has `Literal` type** | See discrepancy note in Current State: Literal has 4 values, runtime check has 3. Resolve first. |
| Step 12 | 2721–2735 | Authenticator + alternator params cross-check | `@model_validator(mode='after')` | |
| Step 13 | 2737–2753 | `stress_duration` / `prepare_stress_duration` integer coercion | `@field_validator(mode='before')` | |
| Step 14 | 2755–2768 | `run_fullscan` format validation | `@field_validator` | |
| Step 15 | 2770–2783 | `endpoint_snitch` + simulated regions/racks (mutating — sets `endpoint_snitch` value) | `@model_validator(mode='after')` | **Uses `assert`** — must replace with `raise ValueError`. Also has operator precedence bug (see note). |
| Step 16 | 2785–2788 | `use_dns_names` backend check | `@model_validator(mode='after')` | |
| Step 17 | 2790–2827 | `scylla_network_config` validation | `@field_validator` (or sub-model with its own validator) | |
| Step 18 | 2829–2831 | K8S TLS + SNI cross-check | `@model_validator(mode='after')` | |
| — | 2836–2855 | Zero token nodes validation | `@model_validator(mode='after')` | **Uses `assert`** — must replace with `raise ValueError` |
| Step 21 | 2857–2891 | Performance throughput params | `@model_validator(mode='after')` | |

**Step 15 operator precedence bug (RESOLVED — fix in Phase 1)**: The condition at lines 2773–2778 has an `or`/`and` precedence issue:
```python
if (
    (self.get("simulated_regions") or 0) > 1
    or (self.get("simulated_racks") or 0) > 1
    and num_of_db_nodes > 1
    and cluster_backend != "docker"
):
```
Due to Python's `and` binding tighter than `or`, the `simulated_regions > 1` case does not require `num_of_db_nodes > 1` or non-docker backend. **Decision**: This is a bug. Fix it during Phase 1 when migrating Step 15 to `@model_validator`. The correct logic is:
```python
if (
    ((simulated_regions > 1) or (simulated_racks > 1))
    and num_of_db_nodes > 1
    and cluster_backend != "docker"
):
```

**Implementation approach:**
- Use `@field_validator(mode='before')` for single-field validations that coerce or normalise input before type-casting (step 13 — `stress_duration` / `prepare_stress_duration` convert string → int and apply `abs()`; running before Pydantic's type coercion ensures consistent error messages)
- Use `@field_validator` (default `mode='after'`) for post-coercion format checks (step 14 — `run_fullscan`)
- Use `@model_validator(mode='after')` for cross-field validations (steps 12, 15, 16, 18, 21, zero-token)
- Replace string-based choices with `Literal` types where possible (step 11)
- Keep the validation logic identical — move it from imperative `__init__` code to declarative validators
- Preserve existing error messages to avoid breaking any downstream error handling
- **Step 15 is a mutating validator**: it sets `endpoint_snitch` to `GossipingPropertyFileSnitch` as a side effect. The `@model_validator(mode='after')` can return a modified `self`, which is the correct Pydantic pattern for this. The existing `assert` at line 2780 must be replaced with `raise ValueError` — assertions are stripped by Python's `-O` flag

**Boundary with `verify_configuration()` — the rule:**
The existing `verify_configuration()` method (line 3194) and new Pydantic validators are *not* interchangeable. The deciding rule is:

- **Pydantic validator** (`@field_validator` / `@model_validator`): the check can be evaluated from field values alone, without knowing the full configuration context (backend, file paths, external APIs). Examples: `stress_duration` is a positive integer; `k8s_enable_sni` requires `k8s_enable_tls`.
- **`verify_configuration()`**: the check requires global configuration context — e.g., whether a required field is set *for the active backend*, whether URLs are reachable, whether Docker-specific params are consistent. These run after `__init__` when the full config is assembled.

Steps 15, 16, and 18 (backend checks for `endpoint_snitch`, `use_dns_names`, and K8S TLS) are cross-field validators with no network access and can be evaluated from field values alone — they belong in Pydantic. The fact that they reference `cluster_backend` does not make them backend-specific in the `verify_configuration()` sense; `cluster_backend` is itself a config field available to validators.

This rule prevents duplication: a validation check lives in exactly one place.

**`validate_assignment=True` and partial state during `__init__`:**
`model_config` has `validate_assignment=True`, which causes `@model_validator(mode='after')` to fire on every field assignment during `__init__`'s bulk loading. Cross-field validators that inspect sibling fields will see incomplete state and may produce false failures.

Concrete mitigation — use a `PrivateAttr` guard:

```python
class SCTConfiguration(BaseModel):
    _config_loaded: bool = PrivateAttr(default=False)

    @model_validator(mode='after')
    def _validate_authenticator_params(self) -> 'SCTConfiguration':
        if not self._config_loaded:  # skip during bulk loading
            return self
        # ... validation logic
        return self

    def __init__(self, /, **data):
        super().__init__(**data)
        # ... load YAML, env vars, merge ...
        self._config_loaded = True
        # trigger all cross-field validators explicitly once loading is complete
        self._validate_authenticator_params()
        self._validate_k8s_tls_sni()
        self._validate_endpoint_snitch()
        self._validate_dns_names()
        self._validate_zero_token_nodes()
        self._validate_performance_throughput()
```

**Why not `self.model_validate(self.model_dump())`**: `model_validate()` is a classmethod that returns a *new* instance. That new instance would have `_config_loaded=False` (the `PrivateAttr` default), causing all cross-field validators to skip. Instead, call each validator method explicitly after setting `_config_loaded = True`.

An alternative is to use Pydantic's `model_post_init` hook, which runs after the model's own `__init__` completes — this would make the `_config_loaded` flag unnecessary for triggering, though it's still needed to guard against `validate_assignment=True` during bulk loading.

This ensures cross-field validators run exactly once, after full loading, not on each intermediate assignment.

**Disabling validators for testing:**
Two complementary approaches:

1. **`model_construct()` — bypass everything**: Pydantic v2's `model_construct()` skips all validators, field defaults factories, and type coercions. Use for tests that need arbitrary partial data or intentionally invalid state. Note that objects built this way may not resemble production objects (no type coercions, no defaults) — use only when testing code that accesses specific fields directly.

```python
# In tests: bypass all validators AND all type coercions / default factories
config = SCTConfiguration.model_construct(stress_duration=30, cluster_backend="aws")
```

2. **`ClassVar` flag — selective disabling**: For tests that need production-like construction but want to skip cross-field validators:

```python
class SCTConfiguration(BaseModel):
    _cross_field_validation_enabled: ClassVar[bool] = True

    @model_validator(mode='after')
    def _validate_k8s_tls_sni(self) -> 'SCTConfiguration':
        if not SCTConfiguration._cross_field_validation_enabled:
            return self
        if self.get("k8s_enable_sni") and not self.get("k8s_enable_tls"):
            raise ValueError("'k8s_enable_sni=true' requires 'k8s_enable_tls' also to be 'true'.")
        return self

# Shared pytest fixture in unit_tests/conftest.py:
@pytest.fixture
def skip_cross_field_validation():
    SCTConfiguration._cross_field_validation_enabled = False
    yield
    SCTConfiguration._cross_field_validation_enabled = True
```

All cross-field `@model_validator` methods must follow this exact pattern (check the `ClassVar` flag at the top, return `self` immediately if disabled). This prevents ad-hoc per-test workarounds and keeps disabling consistent.

**Thread-safety note**: The `ClassVar` flag is shared across all instances. If tests run with `pytest-xdist` (parallel workers in separate processes), this is safe since each worker gets its own process. However, if threading-based parallelism is ever introduced, consider replacing with a `contextvars.ContextVar` instead.

**Items that stay in `__init__` (Phase 1):**
- Lines 2833–2834 (`SCTCapacityReservation.get_cr_from_aws`, `SCTDedicatedHosts.reserve`) — these are side-effectful resource reservations, not validation. **Note**: these make cloud API calls, which contradicts the Phase 2 goal of "instantiate without network access." They must be moved out in Phase 2 — either into `resolve_images()` or a separate `reserve_resources()` stage. **This is a Phase 2 action item, not optional.**
- Lines 2893–2895 (random `c_s_driver_version` selection) — this is configuration mutation, not validation

**Definition of Done:**
- [ ] All 11 validation blocks (steps 11–21) removed from `__init__`
- [ ] Equivalent Pydantic validators added with identical error messages
- [ ] No Pydantic validator makes network/cloud API calls (enforced by convention + documented)
- [ ] `@field_validator(mode='before')` used for step 13 (`stress_duration` coercion); `@field_validator` for step 14
- [ ] All cross-field `@model_validator` methods guard on `_config_loaded` flag to avoid false failures during bulk loading
- [ ] `model_construct()` works for tests that need to bypass all validation
- [ ] `_cross_field_validation_enabled` ClassVar flag with `skip_cross_field_validation` pytest fixture in `conftest.py`
- [ ] `__init__` reduced by ~168 lines (validation removed, loading + image resolution remains)
- [ ] All `assert` statements in migrated validators replaced with `raise ValueError` (steps 15 and zero-token nodes)
- [ ] Step 11: drop `"spot_low_price"` from `instance_provision` Literal, remove redundant runtime check
- [ ] Step 15: fix `or`/`and` operator precedence bug when migrating to `@model_validator`
- [ ] Existing unit tests in `unit_tests/test_config.py` pass without changes
- [ ] New unit tests validate each extracted validator independently (valid and invalid inputs)
- [ ] Regression test verifying cross-field validators actually run after `__init__` completes (i.e., the `_config_loaded` re-validation mechanism works — invalid cross-field combinations produce errors, not silent passes)
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: None (PR #13104 — Pydantic BaseModel foundation — is already merged)

---

### Phase 2: Make Image Resolution Lazy / Explicit

**Objective**: Extract cloud image resolution (AMI, GCE, Azure lookups) **and** version detection from `__init__` and `get_version_based_on_conf()` into a single `resolve_images()` method, so `SCTConfiguration` can be constructed without network access.

**What moves out of `__init__`:**

| Current Step | Lines | Resolution | Target |
|-------------|-------|-----------|--------|
| Step 5 | 2473–2476 | AMI name → ID conversion | `resolve_images()` |
| Step 6 | 2478–2599 | `scylla_version` → Docker/AMI/GCE/Azure/**OCI**/xcloud image | `resolve_images()` |
| — | 2601–2632 | Target image auto-discovery for platform migration (AWS) | `resolve_images()` |
| Step 6.1 | 2634–2660 | `oracle_scylla_version` → AMI | `resolve_images()` |
| Step 6.2 | 2662–2680 | `vector_store_version` → AMI | `resolve_images()` |
| Step 7 | 2682–2689 | `new_version` → repo lookup | `resolve_images()` |
| Step 8 | 2691–2697 | Repo symlink resolution | `resolve_images()` |
| — | 2833–2834 | `SCTCapacityReservation.get_cr_from_aws`, `SCTDedicatedHosts.reserve` | `resolve_images()` or separate `reserve_resources()` |

**What gets consolidated from `get_version_based_on_conf()`** (line 3562):

`get_version_based_on_conf()` (lines 3562–3661) does more than read cloud image tags — its full scope includes:
- **Cloud image tag lookups**: AWS AMI tags via `get_ami_tags`, GCE image tags via `get_gce_image_tags`, Azure image tags via `azure_utils.get_image_tags`, **OCI image tags via `oci_utils.get_image_tags`**
- **Tarball download**: for the `unified_package` path, it runs `LOCALRUNNER.run(curl ...)` to download and inspect a tarball (lines 3576–3589)
- **Package repo queries**: for the `use_preinstalled_scylla` path, it calls `get_branch_version(scylla_repo)` to query a package repository (lines 3590–3593)
- **`xcloud` backend branch**: explicit handling at line 3654 for the `xcloud` backend
- **`oci` backend branch**: explicit handling at lines 3634–3646 for the OCI backend (**not mentioned in original plan**)
- **`baremetal` backend**: handled via the `use_preinstalled_scylla` path
- **Docker/K8S backends**: handled at lines 3647–3656 — reads from config fields directly (no network)
- **Argus side effect**: calls `self.update_argus_with_version()` at line 3659 regardless of which path is taken. **Consider moving this to the orchestrator** so that Argus updates are a separate, visible step rather than hidden inside image resolution.

This is tightly coupled with image resolution — it depends on resolved image IDs and also makes cloud API calls. It should be consolidated into `resolve_images()` so all cloud-provider lookups live in one place. When consolidated, the `update_argus_with_version()` side effect should be explicitly documented as part of `resolve_images()` — or moved to the orchestrator if Argus updates should be a separate, visible step.

| Method | Lines | Resolution | Target |
|--------|-------|-----------|--------|
| `get_version_based_on_conf` | 3562–3661 | Image tags → `scylla_version` + `is_enterprise` | `resolve_images()` |

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
        - Target image auto-discovery for platform migration
        - Version detection from get_version_based_on_conf()
        - Resource reservation (capacity reservation, dedicated hosts)

        Call this explicitly when cloud image resolution is needed.
        Not called during unit tests or utility usage.
        """
        self._resolve_ami_names()             # step 5
        self._resolve_scylla_images()         # step 6 (Docker/AMI/GCE/Azure/OCI/xcloud)
        self._resolve_target_images()         # target image auto-discovery
        self._resolve_oracle_images()         # step 6.1
        self._resolve_vector_store_images()   # step 6.2
        self._resolve_upgrade_repos()         # step 7
        self._resolve_repo_symlinks()         # step 8
        self._resolve_version_from_images()   # was get_version_based_on_conf()
        self._reserve_resources()             # SCTCapacityReservation + SCTDedicatedHosts
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

**`resolve_images()` is a coordinator, not a monolith**: The public method itself contains only ordered calls to private `_resolve_*` helpers. Each `_resolve_*` method handles one concern and can be called and tested independently. The 7-step sequence in `resolve_images()` is the explicit contract for callers — it doesn't prevent the internal decomposition from being granular.

**External callers of `get_version_based_on_conf()`**: The method is currently called in two places outside `init_and_verify_sct_config()`:
- `mgmt_cli_test.py:932` — calls `self.params.get_version_based_on_conf()[0]` to get the version
- `unit_tests/test_config_get_version_based_on_conf.py` — 8 direct calls testing the method

`get_version_based_on_conf()` must be **kept as a public method** for backward compatibility. Its implementation in Phase 2 should delegate to `_resolve_version_from_images()` (which `resolve_images()` also calls), keeping the existing return type `(version, is_enterprise)`. This avoids breaking either the production caller (`mgmt_cli_test.py`) or the unit tests without requiring a separate deprecation phase.

**Definition of Done:**
- [ ] `__init__` contains no calls to external cloud APIs (AWS, GCE, Azure)
- [ ] `__init__` is reduced to ~185 lines (config loading and merging only — steps 1–4 plus step 9)
- [ ] `SCTConfiguration()` can be instantiated without network access or cloud API mocks
- [ ] New `resolve_images()` method contains all image resolution logic, implemented as ordered calls to private `_resolve_*` helpers
- [ ] OCI and xcloud backends handled in `_resolve_scylla_images()` and `_resolve_version_from_images()`
- [ ] `SCTCapacityReservation.get_cr_from_aws` and `SCTDedicatedHosts.reserve` moved out of `__init__` into `resolve_images()` or a separate `_reserve_resources()` step
- [ ] Target image auto-discovery for platform migration (`instance_type_db_target`) handled in `_resolve_target_images()`
- [ ] `get_version_based_on_conf()` logic extracted into `_resolve_version_from_images()` and called from `resolve_images()`; `get_version_based_on_conf()` kept as a public backward-compatible wrapper delegating to `_resolve_version_from_images()`
- [ ] `init_and_verify_sct_config()` calls `resolve_images()` explicitly (no separate `get_version_based_on_conf()` call)
- [ ] Guard added to `verify_configuration()` (or equivalent) to detect when `resolve_images()` was not called and raise a clear error
- [ ] `mgmt_cli_test.py` and `unit_tests/test_config_get_version_based_on_conf.py` continue to work without changes
- [ ] Unit tests can create `SCTConfiguration` without mocking cloud APIs for image resolution
- [ ] Integration tests verify that image resolution and version detection still work end-to-end
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: Phase 1 is a **recommended** prerequisite, not a strict one. Phase 2 does not require Pydantic validators to exist — it depends on `__init__` being simpler to reason about. Doing Phase 1 first reduces `__init__` by ~180 lines, making the Phase 2 extraction cleaner and lower-risk. The phases can be done in parallel or reversed if scheduling requires it, but Phase 1 → Phase 2 is the preferred order.

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
- **Note**: Docker backend integration tests do not exercise cloud image resolution paths (AMI/GCE/Azure lookups). Cloud backend verification relies on manual testing (see below)

**Manual tests:**
- Verify AWS/GCE/Azure artifact tests still resolve images correctly
- Verify that longevity tests work with the new flow

## Success Criteria

1. **`__init__` under ~185 lines** — contains only configuration file loading and merging (after both phases)
2. **All validation is declarative and network-free** — Pydantic `@field_validator` / `@model_validator` methods with no network/cloud API calls; not inline `if/raise` blocks
3. **Validators disableable for testing** — `_cross_field_validation_enabled` ClassVar flag (selective) and `model_construct()` (full bypass) both work; single canonical `skip_cross_field_validation` fixture in `conftest.py`
4. **`validate_assignment=True` safe** — `_config_loaded` flag prevents cross-field validators from firing on incomplete state during bulk loading
5. **`SCTConfiguration()` instantiable without network access** — no cloud API calls during construction
6. **`get_version_based_on_conf()` preserved** — existing callers (`mgmt_cli_test.py`, unit tests) work unchanged
7. **Existing tests pass** without modifications (beyond test-specific config access updates)
8. **Each phase is a standalone PR** — can be reviewed and merged independently

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Pydantic validator execution order differs from `__init__` order | Medium | Pydantic v2 runs `@field_validator` in definition order, `@model_validator(mode='after')` after all fields. Verify with tests that cross-field validators see the expected state. |
| Validators fire on incomplete state during bulk loading (`validate_assignment=True`) | High | Add `_config_loaded: bool = PrivateAttr(default=False)` flag. All cross-field `@model_validator` methods check this flag and return early if False. Set `_config_loaded = True` at the end of `__init__` and re-validate once. This is a concrete requirement in Phase 1 DoD, not a "may need to." |
| Network-calling code accidentally added as Pydantic validator | High | Document the "no network calls in validators" rule in code comments and plan. Code review must enforce this. Any validation requiring network access goes into `resolve_images()` or `verify_configuration()`. |
| Tests need config objects with partial/invalid data | Medium | Two concrete patterns: (1) `model_construct()` to bypass all Pydantic validation, defaults, and coercions — note objects will not match production types; (2) `_cross_field_validation_enabled` ClassVar flag with `skip_cross_field_validation` fixture for production-like construction without cross-field validation. Both patterns documented and enforced via the Phase 1 DoD. |
| `resolve_images()` called too late or not at all | High | Add a guard in `verify_configuration()` that checks an `_images_resolved: bool = PrivateAttr(default=False)` flag and raises `RuntimeError` if unset. Set the flag at the end of `resolve_images()`. This guard is a Phase 2 DoD item. |
| External callers of `get_version_based_on_conf()` broken | High | Keep `get_version_based_on_conf()` as a public backward-compatible wrapper delegating to `_resolve_version_from_images()`. Confirmed callers: `mgmt_cli_test.py:932` and `unit_tests/test_config_get_version_based_on_conf.py` (8 calls). Phase 2 DoD requires these to pass unchanged. |
| Breaking error messages that downstream tools parse | Low | Preserve exact error message strings. Add tests that assert on error message content. |
| Merge conflicts with parallel development on `sct_config.py` | High | Small, focused PRs. Coordinate with team on merge order. Phase 1 before Phase 2 (recommended). |
| `mock_cloud_services` fixture changes affect other tests | Medium | Phase 2 *simplifies* the fixture (fewer mocks needed), but verify that all tests still pass during the transition. |
