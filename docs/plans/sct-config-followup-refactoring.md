---
status: draft
domain: config
created: 2026-03-01
last_updated: 2026-03-17
owner: null
---
# Splitting `sct_config.py` into a Module

## Problem Statement

`sdcm/sct_config.py` is a ~4,500-line monolith containing 430+ configuration fields, validation logic, image resolution, helper functions, and backend-specific defaults — all in a single class and file. This makes it extremely hard for both humans and LLMs to review, maintain, or extend.

[PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) migrated `SCTConfiguration` from a custom `dict` to a Pydantic `BaseModel`, providing a foundation for further refactoring. This plan focuses specifically on **splitting the monolithic file into a well-organized package**.

> **Note**: Related refactoring goals — extracting validation from `__init__`, making image resolution lazy/optional, and migrating `.get()` to typed attribute access — will be tracked in separate plans (to be created).

Key pain points from the PR #13104 review ([comment by @soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971791016), [response by @pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)):

1. **Monolithic file**: 4,500+ lines in a single file with 430+ configuration fields cannot be reasonably reviewed or maintained.
2. **Single class owns everything**: All fields, all validation, all image resolution, all backend defaults live in one class — making it impossible to test or mock individual parts.
3. **Validation coupled to structure**: The flat "wide" config means all validation must live in one place, growing linearly with every new field.
4. **Hard to mock in tests**: Without structural separation, tests that only need a few config fields must construct or mock the entire configuration object.

## Current State

### File: `sdcm/sct_config.py` (~4,500 lines)

> **Note**: Line numbers reference the current `master` branch as of commit `eb82fef`. They will shift as PR #13104 and other changes are merged, but the logical sections remain the same.

**Class structure:**
- `SCTConfiguration(dict)` — line 237 (current master), migrated to `SCTConfiguration(BaseModel)` in PR #13104
- `config_options` list — lines 262–2798 (~2,500 lines of field definitions), converted to Pydantic field annotations in PR #13104
- Class-level attributes — lines 2800–3050: `required_params`, `backend_required_params`, `defaults_config_files`, `stress_cmd_params`, `ami_id_params`, `aws_supported_regions`

**`__init__` method** (lines 3052–3527, ~475 lines) performs these steps sequentially:
1. **Lines 3052–3074**: Initialize instance variables, load environment variables
2. **Lines 3076–3088**: Load default and user-provided YAML config files
3. **Lines 3090–3116**: Handle region data for AWS/GCE/Azure
4. **Lines 3117–3152**: Merge environment variables, set billing project, convert AMI names
5. **Lines 3154–3256**: **Image resolution** — resolve `scylla_version` to cloud images via external APIs
6. **Lines 3258–3305**: **Oracle/Vector Store image resolution** — similar external API calls
7. **Lines 3308–3341**: Resolve repo symlinks, build `user_prefix`
8. **Lines 3342–3527**: **Inline validation** — 12 numbered validation blocks plus capacity reservation

**Validation methods** (lines 3788–3832, plus ~700 lines of `_validate_*` helpers):
- `verify_configuration()` — delegates to `_check_unexpected_sct_variables()`, `_validate_sct_variable_values()`, `_check_per_backend_required_values()`, etc.
- Various `_validate_*` methods scattered across multiple sections

**Properties** (lines 3542–3620):
- `total_db_nodes`, `region_names`, `gce_datacenters`, `cloud_provider_params`, `cloud_env_credentials`

**Orchestrator function** (line 4488):
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

**Module-level utility functions** (lines 80–235):
- Type converters: `_str()`, `_file()`, `str_or_list()`, `str_or_list_or_eval()`, `int_or_space_separated_ints()`, `dict_or_str()`, `dict_or_str_or_pydantic()`, `boolean()`
- Config helpers: `is_config_option_appendable()`, `merge_dicts_append_strings()`

### Configuration field groupings (430+ fields)

Grouped by domain (using heuristics from @fruch):

| Group | Count | Examples |
|-------|-------|---------|
| **Common provisioning** | ~80 | `cluster_backend`, `n_db_nodes`, `n_loaders`, `instance_type_db`, `region_name`, `availability_zone`, `root_disk_size_*` |
| **Stress configuration** | ~50 | `stress_cmd`, `stress_cmd_w`, `stress_cmd_r`, `prepare_write_cmd`, `gemini_cmd`, `cs_user_profiles` |
| **Kubernetes (K8s/EKS/GKE)** | ~40 | `k8s_scylla_*`, `k8s_loader_*`, `k8s_enable_*`, `eks_*`, `gke_*` |
| **Scylla config & features** | ~30 | `append_scylla_yaml`, `append_scylla_args`, `authenticator`, `authorizer`, `server_encrypt`, `alternator_*` |
| **Per-backend: AWS** | ~25 | `ami_id_db_scylla`, `ami_db_scylla_user`, region-specific AMI/instance fields |
| **Per-backend: GCE** | ~20 | `gce_project`, `gce_datacenter`, `gce_instance_type_*`, `gce_image_*` |
| **Per-test suite: Longevity** | ~25 | `stress_multiplier*`, `keyspace_num`, `compaction_strategy`, `data_validation` |
| **Per-test suite: Performance** | ~20 | `perf_gradual_threads`, `perf_gradual_throttle_steps`, `perf_simple_query` |
| **Per-backend: Docker/Baremetal** | ~15 | `docker_image`, `docker_network`, `db_nodes_*_ip` |
| **XCloud** | ~15 | `xcloud_cluster_id`, `xcloud_provider`, `xcloud_scaling_config` |
| **Feature flags** | ~15 | `use_mgmt`, `use_ldap`, `use_zero_nodes`, `use_dns_names` |
| **Nemesis** | ~12 | `nemesis_class_name`, `nemesis_interval`, `nemesis_seed`, `nemesis_during_prepare` |
| **Per-backend: Azure** | ~10 | `azure_region_name`, `azure_instance_type_*`, `azure_image_*` |
| **Manager** | ~10 | `mgmt_docker_image`, `mgmt_agent_backup_config`, `mgmt_restore_extra_params` |
| **Monitoring** | ~10 | `monitor_branch`, `email_recipients`, `enable_argus_report` |
| **Full-scan & validation** | ~10 | `run_fullscan`, `run_tombstone_gc_verification`, `validate_large_collections` |
| **Per-test suite: Upgrade** | ~8 | `new_scylla_repo`, `new_version`, `upgrade_rollback_dist` |
| **Test level** | ~6 | `test_duration`, `prepare_stress_duration`, `user_prefix` |
| **Miscellaneous** | ~40 | `simulated_regions`, `use_capacity_reservation`, `vector_store_*`, `teardown_validators` |

## Goals

1. **Modular file structure**: Split `sct_config.py` into a `sct_config/` package with logically grouped sub-modules, each under ~500 lines.
2. **Encapsulated validation**: Each domain group owns its own validation logic, rather than all validation living in one monolithic class.
3. **Ability to select which parts of the config are used and validated**: Different tests and utilities should be able to work with only the config sections they need, without requiring the full configuration to be constructed and validated.
4. **Incremental migration**: Each step is a standalone PR that doesn't break existing functionality. All existing imports and YAML configs continue to work.

## Approaches for Splitting

Two approaches were discussed during the PR #13104 review. Both achieve the goals above but differ in how fields are organized on the class and in YAML.

### Approach A: Mixins (Flat Structure, Modular Code)

*Proposed by [@soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3877378425)*

Break the model into domain-specific base models (Mixins). The main class inherits from all of them. Fields remain flat — only the source code is split.

```python
# sdcm/sct_config/mixins/nemesis.py
class NemesisConfigMixin(BaseModel):
    nemesis_class_name: StringOrList = SctField(...)
    nemesis_interval: IntOrList = SctField(...)
    nemesis_seed: IntOrList = SctField(...)

    @field_validator("nemesis_interval")
    @classmethod
    def validate_nemesis_interval(cls, v):
        ...

# sdcm/sct_config/mixins/stress.py
class StressConfigMixin(BaseModel):
    stress_cmd: StringOrList = SctField(...)
    stress_cmd_w: StringOrList = SctField(...)
    ...

# sdcm/sct_config/config.py
class SCTConfiguration(
    NemesisConfigMixin,
    StressConfigMixin,
    ProvisioningConfigMixin,
    ...
):
    """Assembled configuration from all domain mixins."""

    @model_validator(mode='after')
    def cross_mixin_validation(self):
        """Validation that spans multiple domains goes here."""
        ...
```

**Benefits:**
- Flat attribute access preserved: `config.nemesis_class_name` (no breaking changes)
- Full IDE autocomplete and type checking via Pydantic
- No YAML config file changes — all existing configs continue to work
- Each mixin file is small, reviewable, and testable
- Pydantic seamlessly inherits and runs validators from all parent classes
- Cross-mixin validators live in the assembler class

**Drawbacks:**
- All fields still end up on one class at runtime (wide, not deep)
- Mocking requires patching individual fields, not entire sections
- A mixin's validators might reference fields from other mixins — cross-mixin validators must live in the assembler class
- Doesn't fundamentally change the "one class with 430 fields" problem — it only splits the source code

**Testing approach:**
- Each mixin can have its own unit test file
- Cross-mixin validators tested on the assembled class
- Mocking: patch individual fields as before

### Approach B: Nested Sub-Models (Deep Structure)

*Proposed by [@pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)*

Restructure flat prefixed fields into nested Pydantic sub-models. Each sub-model is a separate class with its own validation. YAML configs would use nested format.

```python
# sdcm/sct_config/models/nemesis.py
class NemesisConfig(BaseModel):
    class_name: StringOrList = SctField(...)
    interval: IntOrList = SctField(...)
    seed: IntOrList = SctField(...)

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v):
        ...

# sdcm/sct_config/models/stress.py
class StressConfig(BaseModel):
    cmd: StringOrList = SctField(...)
    cmd_w: StringOrList = SctField(...)
    ...

# sdcm/sct_config/config.py
class SCTConfiguration(BaseModel):
    nemesis: NemesisConfig = SctField(...)
    stress: StressConfig = SctField(...)
    provisioning: ProvisioningConfig = SctField(...)
    ...
```

YAML config would change from flat to nested:
```yaml
# Current (flat)
nemesis_class_name: SisyphusMonkey
nemesis_interval: 5
nemesis_seed: 42

# New (nested)
nemesis:
  class_name: SisyphusMonkey
  interval: 5
  seed: 42
```

**Benefits:**
- Each sub-model is a fully independent Pydantic class — can be instantiated, validated, and tested in isolation
- Natural module boundaries: each sub-model = one file with its own validation
- Enables true mocking by section: `config.nemesis = MockNemesisConfig(...)` replaces the entire section
- Naturally limits validation scope: `NemesisConfig` only validates nemesis fields
- YAML structure mirrors code structure

**Drawbacks:**
- **Breaking change**: All existing YAML configs must be updated (430+ test config files)
- All `config.nemesis_class_name` access must change to `config.nemesis.class_name` across the codebase
- Needs a backward-compatibility bridge during transition (support both flat and nested format)
- Some fields don't have clear group affiliation — "miscellaneous" bucket remains
- Nested YAML is more verbose and harder to override via environment variables (e.g., `SCT_NEMESIS_CLASS_NAME` vs `SCT_NEMESIS__CLASS_NAME`)

**Testing approach:**
- Each sub-model tested independently: `NemesisConfig(class_name="X", interval=5)`
- Section mocking: `config.nemesis = NemesisConfig.model_construct(...)` bypasses validation
- Tests that don't need nemesis config can skip it entirely

### Recommendation

**Start with Approach A (Mixins)** to achieve the immediate goal of splitting the file into manageable modules, then evaluate Approach B (Nested Sub-Models) via a PoC for a subset of fields (e.g., nemesis fields).

Rationale:
- Approach A has **zero breaking changes** — no YAML updates, no consumer code changes
- Approach A delivers the file split immediately, unblocking further refactoring
- Approach B needs a PoC to validate: backward-compatible YAML loading, environment variable mapping, impact on 430+ config files
- The mixin structure from Approach A can be incrementally converted to nested models later

## Implementation Phases

### Phase 1: Create Package and Extract Types/Helpers

**Objective**: Convert `sdcm/sct_config.py` into a `sdcm/sct_config/` package. Extract module-level utility functions and custom types into their own files.

**Implementation:**

1. Create `sdcm/sct_config/` directory
2. Create `sdcm/sct_config/__init__.py` with re-exports for backward compatibility:
   ```python
   from sdcm.sct_config.config import SCTConfiguration
   from sdcm.sct_config.config import init_and_verify_sct_config
   ```
3. Move the existing file to `sdcm/sct_config/config.py`
4. Extract module-level functions into `sdcm/sct_config/types.py`:
   - Type definitions: `String`, `ExistingFile`, `StringOrList`, `IntOrList`, `BooleanOrList`, `DictOrStr`, `DictOrStrOrPydantic`, `MultitenantValue`
   - Converter functions: `_str()`, `_file()`, `str_or_list_or_eval()`, `int_or_space_separated_ints()`, `dict_or_str()`, `dict_or_str_or_pydantic()`, `boolean()`
   - `SctField` definition
5. Extract into `sdcm/sct_config/helpers.py`:
   - `is_config_option_appendable()`
   - `merge_dicts_append_strings()`
   - `IgnoredType`, `is_ignored_field()`

**Resulting structure:**
```
sdcm/sct_config/
├── __init__.py       # Re-exports (backward compatible)
├── config.py         # SCTConfiguration class (everything else for now)
├── types.py          # Custom Pydantic types, converters, SctField
└── helpers.py        # merge_dicts_append_strings, appendable logic
```

**Definition of Done:**
- [ ] `from sdcm.sct_config import SCTConfiguration` works unchanged
- [ ] `from sdcm.sct_config import init_and_verify_sct_config` works unchanged
- [ ] All existing tests pass without changes
- [ ] Pre-commit and linting pass

**Dependencies**: PR #13104 merged

---

### Phase 2: Extract Defaults and Backend Configuration

**Objective**: Move class-level data attributes (backend lists, required params, default config file paths) out of the `SCTConfiguration` class into a dedicated module.

**What moves:**
- `available_backends` list (lines 242–260)
- `required_params` list (lines 2800–2810)
- `backend_required_params` dict (lines 2813–2950)
- `defaults_config_files` dict (lines 2952–2974)
- `per_provider_multi_region_params` dict (lines 2976–2979)
- `xcloud_per_provider_required_params` dict (lines 2981–2993)
- `stress_cmd_params` list (lines 2995–3032)
- `ami_id_params` list (lines 3033–3040)
- `aws_supported_regions` list (lines 3041–3050)

**Target file**: `sdcm/sct_config/defaults.py`

The `SCTConfiguration` class will import these as needed. They remain accessible via the class for backward compatibility.

**Definition of Done:**
- [ ] `sdcm/sct_config/defaults.py` contains all data constants
- [ ] `config.py` reduced by ~250 lines
- [ ] All existing tests pass without changes

**Dependencies**: Phase 1

---

### Phase 3: Extract Field Definitions into Domain Mixins

**Objective**: Split the ~2,500 lines of field definitions into domain-specific mixin classes, each in its own file. `SCTConfiguration` inherits from all mixins.

**Proposed domain groups** (based on @fruch's categorization):

| Mixin Class | File | Fields | Description |
|------------|------|--------|-------------|
| `CommonProvisioningMixin` | `mixins/provisioning.py` | ~80 | `cluster_backend`, `n_db_nodes`, `n_loaders`, `instance_type_*`, `region_name`, `availability_zone`, `root_disk_*`, `user_credentials_path` |
| `AwsProvisioningMixin` | `mixins/aws.py` | ~25 | `ami_id_*`, `aws_instance_profile_*`, AWS region-specific fields |
| `GceProvisioningMixin` | `mixins/gce.py` | ~20 | `gce_project`, `gce_datacenter`, `gce_instance_type_*`, `gce_image_*` |
| `AzureProvisioningMixin` | `mixins/azure.py` | ~10 | `azure_region_name`, `azure_instance_type_*`, `azure_image_*` |
| `K8sProvisioningMixin` | `mixins/kubernetes.py` | ~40 | `k8s_*`, `eks_*`, `gke_*` |
| `DockerProvisioningMixin` | `mixins/docker.py` | ~15 | `docker_image`, `docker_network`, baremetal IP fields |
| `XcloudProvisioningMixin` | `mixins/xcloud.py` | ~15 | `xcloud_*` fields |
| `NemesisConfigMixin` | `mixins/nemesis.py` | ~12 | `nemesis_class_name`, `nemesis_interval`, `nemesis_seed` |
| `StressConfigMixin` | `mixins/stress.py` | ~50 | `stress_cmd*`, `prepare_*_cmd`, `gemini_cmd` |
| `ScyllaConfigMixin` | `mixins/scylla.py` | ~30 | `scylla_version`, `scylla_repo`, `append_scylla_yaml`, encryption, auth |
| `TestLevelMixin` | `mixins/test_level.py` | ~25 | `test_duration`, `user_prefix`, `email_recipients`, longevity params |
| `ManagerConfigMixin` | `mixins/manager.py` | ~10 | `mgmt_*` fields |
| `MonitoringConfigMixin` | `mixins/monitoring.py` | ~10 | `monitor_*`, `email_*`, reporting fields |
| `PerformanceConfigMixin` | `mixins/performance.py` | ~20 | `perf_*` fields |
| `UpgradeConfigMixin` | `mixins/upgrade.py` | ~8 | `new_scylla_repo`, `new_version`, upgrade params |
| `FeatureConfigMixin` | `mixins/features.py` | ~40 | `use_*` flags, `run_fullscan`, `vector_store_*`, misc |

**Assembled class:**
```python
# sdcm/sct_config/config.py
class SCTConfiguration(
    CommonProvisioningMixin,
    AwsProvisioningMixin,
    GceProvisioningMixin,
    AzureProvisioningMixin,
    K8sProvisioningMixin,
    DockerProvisioningMixin,
    XcloudProvisioningMixin,
    NemesisConfigMixin,
    StressConfigMixin,
    ScyllaConfigMixin,
    TestLevelMixin,
    ManagerConfigMixin,
    MonitoringConfigMixin,
    PerformanceConfigMixin,
    UpgradeConfigMixin,
    FeatureConfigMixin,
):
    """SCT Configuration assembled from domain-specific mixins."""
    ...
```

**Resulting structure:**
```
sdcm/sct_config/
├── __init__.py
├── config.py              # SCTConfiguration (assembler + __init__ + cross-domain logic)
├── types.py               # Custom types and converters
├── helpers.py             # Merge/append helpers
├── defaults.py            # Backend defaults, required params
└── mixins/
    ├── __init__.py
    ├── provisioning.py    # Common provisioning fields
    ├── aws.py             # AWS-specific fields
    ├── gce.py             # GCE-specific fields
    ├── azure.py           # Azure-specific fields
    ├── kubernetes.py      # K8s/EKS/GKE fields
    ├── docker.py          # Docker/Baremetal fields
    ├── xcloud.py          # XCloud fields
    ├── nemesis.py         # Nemesis fields
    ├── stress.py          # Stress command fields
    ├── scylla.py          # Scylla config fields
    ├── test_level.py      # Test-level parameters
    ├── manager.py         # Scylla Manager fields
    ├── monitoring.py      # Monitoring/reporting fields
    ├── performance.py     # Performance test fields
    ├── upgrade.py         # Upgrade test fields
    └── features.py        # Feature flags and misc
```

**Migration strategy:**
1. Extract one mixin at a time (one PR per mixin or small group)
2. Each mixin file contains field definitions + field-level validators
3. Cross-field validators that span multiple mixins remain in `config.py`
4. Validate with `uv run sct.py unit-tests` after each extraction

**Recommended extraction order** (start with least-connected mixins):
1. `NemesisConfigMixin` — self-contained, few cross-field dependencies
2. `StressConfigMixin` — large but self-contained
3. `PerformanceConfigMixin` — small, self-contained
4. `UpgradeConfigMixin` — small, self-contained
5. `ManagerConfigMixin` — small, self-contained
6. `MonitoringConfigMixin` — small, self-contained
7. Per-backend mixins (AWS, GCE, Azure, Docker, K8s, XCloud) — may have cross-dependencies with provisioning
8. `ScyllaConfigMixin` — some fields referenced by validators in other domains
9. `CommonProvisioningMixin` — most interconnected, extract last (may need sub-splitting if >500 lines)
10. `FeatureConfigMixin` — catch-all for remaining fields

**Definition of Done:**
- [ ] All field definitions moved to mixin files
- [ ] `config.py` contains only the assembler class, `__init__`, cross-domain validators, and orchestrator
- [ ] No file in the package exceeds ~500 lines
- [ ] All existing tests pass without changes

**Dependencies**: Phase 2

---

### Phase 4: Extract Validation Methods

**Objective**: Move the `_validate_*` methods and `verify_configuration()` logic into domain-appropriate locations.

**Implementation:**
- Single-field validators → move into the mixin that owns the field (as `@field_validator`)
- Cross-field validators within one domain → move into that domain's mixin (as `@model_validator` on the mixin)
- Cross-domain validators → remain in `config.py` on the assembled `SCTConfiguration` class
- Backend-specific validators → move into their respective backend mixin

| Validator | Current Location | Target Location |
|-----------|-----------------|-----------------|
| `_validate_seeds_number` | `config.py` | `NemesisConfigMixin` or cross-domain in `config.py` |
| `_validate_docker_backend_parameters` | `config.py` | `DockerProvisioningMixin` |
| `_validate_cloud_backend_parameters` | `config.py` | `XcloudProvisioningMixin` |
| `_validate_placement_group_required_values` | `config.py` | `AwsProvisioningMixin` |
| `_verify_scylla_bench_mode_and_workload_parameters` | `config.py` | `StressConfigMixin` |
| `_verify_rackaware_configuration` | `config.py` | `CommonProvisioningMixin` |
| `_instance_type_validation` | `config.py` | Per-backend mixins |
| `verify_configuration` (orchestrator) | `config.py` | Stays in `config.py` |

**Definition of Done:**
- [ ] Each mixin owns its domain-specific validators
- [ ] `config.py` only has cross-domain validators and the orchestration method
- [ ] All existing tests pass without changes

**Dependencies**: Phase 3

---

### Phase 5: PoC for Nested Sub-Models

**Objective**: Evaluate Approach B (nested sub-models) on a small subset of fields to determine feasibility and migration cost.

**PoC scope**: Convert `nemesis_*` fields to a nested `NemesisConfig` sub-model.

**What the PoC must validate:**
1. **YAML backward compatibility**: Can we load both flat (`nemesis_class_name: X`) and nested (`nemesis: {class_name: X}`) YAML formats?
2. **Environment variable mapping**: How does `SCT_NEMESIS_CLASS_NAME` map to `config.nemesis.class_name`?
3. **Consumer migration cost**: How many files need `config.nemesis_class_name` → `config.nemesis.class_name` changes?
4. **Test mocking**: Can we mock `config.nemesis = NemesisConfig.model_construct(...)` to skip validation?
5. **IDE support**: Does Pyright/mypy properly type-check nested access?

**Decision criteria:**
- If the PoC shows a clean migration path with manageable backward compatibility, proceed with Approach B for all groups
- If the PoC reveals significant friction, continue with Approach A (Mixins) and revisit later

**Definition of Done:**
- [ ] PoC branch demonstrates nemesis fields as nested sub-model
- [ ] Both flat and nested YAML formats load correctly
- [ ] At least 3 consumer files updated to use nested access
- [ ] Unit tests demonstrate section-level mocking
- [ ] Written evaluation with recommendation for or against full migration

**Dependencies**: Phase 3 (mixins provide the grouping that informs sub-model boundaries)

## Testing Requirements

### Per-Phase Testing

| Phase | Unit Tests | Integration Tests | Manual Tests |
|-------|-----------|------------------|-------------|
| Phase 1 | Import tests; verify all fields accessible | Existing test suite passes | Verify no import regressions |
| Phase 2 | Verify defaults accessible from new module | Existing test suite passes | — |
| Phase 3 | Each mixin tested independently for field presence | Existing test suite passes | Verify no import regressions |
| Phase 4 | Validator tests per mixin | Existing test suite passes | Verify error messages match |
| Phase 5 | PoC: flat+nested YAML loading, nested access, mocking | Docker backend config loading | — |

### Regression Testing

Each phase must pass:
- `uv run sct.py unit-tests`
- `uv run sct.py pre-commit`
- At least one artifact test (AWS or Docker) to verify end-to-end config loading

## Success Criteria

1. **No file in `sdcm/sct_config/` exceeds 500 lines** — maintainable module sizes
2. **Each domain has its own validation** — nemesis validation in nemesis mixin, AWS validation in AWS mixin, etc.
3. **All existing imports work unchanged** — `from sdcm.sct_config import SCTConfiguration` still works
4. **All existing YAML configs work unchanged** — no config file migration required (until Phase 5 PoC evaluates nested format)
5. **All existing tests pass** without modifications

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Import cycles from package split | Medium | Careful dependency ordering; `TYPE_CHECKING` imports where needed |
| Pydantic MRO issues with multiple inheritance | Medium | Test early; Pydantic v2 handles multiple inheritance well |
| Merge conflicts with parallel development | High | Small, focused PRs; coordinate with team on merge order |
| Field mis-assignment to wrong mixin | Low | Review field groupings before extraction; can move fields between mixins later |
| Cross-mixin validator complexity | Medium | Keep cross-domain validators in the assembler class; document which validators span domains |
| Phase 5 (nested) requires YAML migration | High | Phase 5 is a PoC only; full migration deferred until PoC validates approach |
