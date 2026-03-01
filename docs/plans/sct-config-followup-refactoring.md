# SCT Configuration Follow-up Refactoring Plan

## Problem Statement

[PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) migrated `SCTConfiguration` from a custom `dict`-based class to a Pydantic `BaseModel`, providing type-safe field definitions and automatic validation. However, the migration was a foundation step — the `__init__` method remains a ~475-line monolith that mixes configuration loading, cloud image resolution (calling external APIs), and inline validation. The file itself is ~4,500 lines and cannot be reasonably reviewed, tested, or maintained in its current form.

Key pain points identified during the PR #13104 review ([comment by @soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971791016), [response by @pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)):

1. **Validation in `__init__`**: ~180 lines of inline validation checks (steps 11–21) inside `__init__` make the constructor fragile and hard to test in isolation.
2. **Image resolution in `__init__`**: ~150 lines (steps 5–6.1) call external cloud APIs (AWS, GCE, Azure) during construction, preventing lightweight instantiation for utilities or testing.
3. **Monolithic file**: 4,500+ lines in a single file with 430+ configuration options make it hard for both humans and LLMs to navigate.
4. **Vague `.get()` access**: ~1,000+ `params.get("string_key")` calls across the codebase bypass IDE type checking and provide no autocomplete or type safety.

## Current State

### File: `sdcm/sct_config.py` (~4,500 lines)

**Class structure:**
- `SCTConfiguration(dict)` — line 237 (current master), migrated to `SCTConfiguration(BaseModel)` in PR #13104
- `config_options` list — lines 262–2798 (~2,500 lines of field definitions), converted to Pydantic field annotations in PR #13104
- Class-level attributes — lines 2800–3050: `required_params`, `backend_required_params`, `defaults_config_files`, `stress_cmd_params`, `ami_id_params`, `aws_supported_regions`

**`__init__` method** (lines 3052–3527, ~475 lines) performs these steps sequentially:
1. **Lines 3052–3074**: Initialize instance variables, load environment variables
2. **Lines 3076–3088**: Load default and user-provided YAML config files
3. **Lines 3090–3116**: Handle region data for AWS/GCE/Azure
4. **Lines 3117–3152**: Merge environment variables, set billing project, convert AMI names
5. **Lines 3154–3256**: **Image resolution** — resolve `scylla_version` to Docker image, AWS AMI, GCE image, or Azure image by calling external cloud APIs
6. **Lines 3258–3305**: **Oracle/Vector Store image resolution** — similar external API calls
7. **Lines 3308–3341**: Resolve repo symlinks, build `user_prefix`
8. **Lines 3342–3527**: **Inline validation** — 12 numbered validation blocks (steps 11–21) plus capacity reservation and Kafka/Manager config validation

**Validation methods** (lines 3788–3832, plus ~700 lines of `_validate_*` helpers):
- `verify_configuration()` — called separately after `__init__`, delegates to `_check_unexpected_sct_variables()`, `_validate_sct_variable_values()`, `_check_per_backend_required_values()`, etc.
- Various `_validate_*` methods scattered through lines 2114–2301 and 4370–4486

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

### Usage across the codebase

- **`self.params.get("key")`**: ~756 calls in `sdcm/` (the framework), ~350 in test files
- **`params.get("key")`**: ~1,071 total calls across `sdcm/`
- Consumers span 40+ files: `sdcm/tester.py` (287 calls), `sdcm/cluster.py` (108 calls), `sdcm/cluster_k8s/__init__.py` (76 calls), etc.

### Configuration field groupings (430+ fields)

| Group | Count | Examples |
|-------|-------|---------|
| Stress commands | ~43 | `stress_cmd`, `stress_cmd_w`, `prepare_write_cmd` |
| Kubernetes | ~35 | `k8s_scylla_utils_docker_image`, `k8s_enable_tls` |
| GCE | ~21 | `gce_project`, `gce_datacenter`, `gce_image_db` |
| Scylla core | ~16 | `scylla_version`, `scylla_repo`, `scylla_linux_distro` |
| Feature flags | ~15 | `use_mgmt`, `use_ldap`, `use_zero_nodes` |
| Nemesis | ~12 | `nemesis_class_name`, `nemesis_interval`, `nemesis_seed` |
| Azure | ~9 | `azure_region_name`, `azure_image_db` |
| Manager | ~8 | `mgmt_docker_image`, `mgmt_agent_backup_config` |
| Cluster sizing | ~7 | `n_db_nodes`, `n_loaders`, `n_monitors` |
| AMI | ~7 | `ami_id_db_scylla`, `ami_id_loader` |
| Instance types | ~6 | `instance_type_db`, `instance_type_loader` |
| XCloud | ~6 | `xcloud_provider`, `xcloud_scaling_config` |
| Performance | ~5 | `perf_gradual_threads`, `perf_gradual_throttle_steps` |
| Other | ~235+ | `test_duration`, `user_prefix`, `cluster_backend`, etc. |

## Goals

1. **Lean constructor**: `SCTConfiguration.__init__` should only load and merge configuration data — no validation, no external API calls.
2. **Lazy image resolution**: Cloud image lookups (AWS AMI, GCE images, Azure images) should happen on-demand or in an explicit `resolve_images()` step, not during construction.
3. **Modular file structure**: Split `sct_config.py` into a `sct_config/` package with logically grouped sub-modules, each under ~500 lines.
4. **Typed attribute access**: Consumers should use `config.test_duration` (with IDE autocomplete and type checking) instead of `config.get("test_duration")`.
5. **Testable in isolation**: Configuration can be constructed for unit tests without AWS credentials or network access.
6. **Incremental migration**: Each phase is a standalone PR that doesn't break existing functionality.

## Implementation Phases

### Phase 1: Extract Validation from `__init__` into Pydantic Validators

**Objective**: Move all inline validation blocks (steps 11–21 in `__init__`) into proper Pydantic `@model_validator` and `@field_validator` methods, making them declarative and independently testable.

**What moves out of `__init__`:**

| Current Step | Lines | Validation | Target |
|-------------|-------|------------|--------|
| Step 11 | 3342–3344 | `instance_provision` allowed values | `Literal["spot", "on_demand", "spot_fleet"]` type annotation |
| Step 12 | 3346–3360 | Authenticator params cross-check | `@model_validator(mode='after')` |
| Step 13 | 3362–3378 | `stress_duration` integer coercion | `@field_validator` or `BeforeValidator` |
| Step 14 | 3380–3393 | `run_fullscan` format validation | `@field_validator` |
| Step 15 | 3395–3407 | `endpoint_snitch` + simulated regions | `@model_validator(mode='after')` |
| Step 16 | 3409–3412 | `use_dns_names` backend check | `@model_validator(mode='after')` |
| Step 17 | 3414–3451 | `scylla_network_config` validation | `@field_validator` or dedicated sub-model |
| Step 18 | 3453–3455 | K8S TLS+SNI cross-check | `@model_validator(mode='after')` |
| Step 19 | 3460–3462 | Kafka config instantiation | `@field_validator` with `DictOrStrOrPydantic` |
| Step 20 | 3464–3466 | Manager backup config | `@field_validator` with `DictOrStrOrPydantic` |
| Step 21 | 3467–3486 | Zero token nodes validation | `@model_validator(mode='after')` |
| Perf | 3488–3522 | Performance throughput params | `@model_validator(mode='after')` |

**Implementation approach:**
- Use `@field_validator` for single-field validations (steps 13, 14, 19, 20)
- Use `@model_validator(mode='after')` for cross-field validations (steps 12, 15, 16, 18, 21, perf)
- Replace string-based choices with `Literal` types where possible (step 11 already a candidate)
- Keep the validation logic identical — just move it from imperative `__init__` code to declarative validators

**Definition of Done:**
- [ ] All 12 validation blocks removed from `__init__`
- [ ] Equivalent Pydantic validators added with same error messages
- [ ] `__init__` reduced to ~300 lines (loading + image resolution only)
- [ ] Existing unit tests in `unit_tests/test_config.py` pass without changes
- [ ] New unit tests validate each extracted validator independently

**Dependencies**: PR #13104 merged (Pydantic BaseModel foundation)

---

### Phase 2: Make Image Resolution Lazy / Optional

**Objective**: Extract cloud image resolution (AMI, GCE, Azure lookups) from `__init__` into a separate `resolve_images()` method (or lazy properties), so `SCTConfiguration` can be constructed without network access.

**What moves out of `__init__`:**

| Current Step | Lines | Resolution | Target |
|-------------|-------|-----------|--------|
| Step 5 | 3150–3152 | AMI name → ID conversion | Lazy property or explicit `resolve_images()` |
| Step 6 | 3154–3256 | `scylla_version` → Docker/AMI/GCE/Azure image | Lazy property or explicit `resolve_images()` |
| Step 6.1 | 3258–3305 | `oracle_scylla_version` → AMI/GCE/Azure image | Same |
| Step 6.2 | 3287–3305 | `vector_store_version` → AMI | Same |

**Implementation approach:**

Option A — **Explicit `resolve_images()` method** (recommended):
```python
class SCTConfiguration(BaseModel):
    def __init__(self):
        # Only load and merge YAML/env configuration
        ...

    def resolve_images(self):
        """Resolve version strings to cloud provider image IDs.

        Call this explicitly when cloud image resolution is needed.
        Not called during unit tests or utility usage.
        """
        self._resolve_scylla_images()
        self._resolve_oracle_images()
        self._resolve_vector_store_images()
```

Option B — **Lazy `@cached_property`**:
```python
@cached_property
def resolved_ami_id_db_scylla(self) -> str:
    """Resolve AMI ID on first access."""
    if self.ami_id_db_scylla:
        return self.ami_id_db_scylla
    return self._resolve_aws_ami()
```

**Recommended**: Option A, because it keeps the resolution step explicit and allows `init_and_verify_sct_config()` to call it at the right time:
```python
def init_and_verify_sct_config() -> SCTConfiguration:
    sct_config = SCTConfiguration()
    sct_config.resolve_images()     # <-- explicit step
    sct_config.log_config()
    sct_config.verify_configuration()
    ...
```

**Definition of Done:**
- [ ] `__init__` contains no calls to external cloud APIs (AWS, GCE, Azure)
- [ ] `SCTConfiguration()` can be instantiated without network access
- [ ] `resolve_images()` method contains all image resolution logic
- [ ] `init_and_verify_sct_config()` calls `resolve_images()` explicitly
- [ ] Unit tests can create `SCTConfiguration` without mocking cloud APIs
- [ ] Integration tests verify image resolution still works end-to-end

**Dependencies**: Phase 1 (clean `__init__`)

---

### Phase 3: Split `sct_config.py` into a Package

**Objective**: Convert the monolithic `sdcm/sct_config.py` into a `sdcm/sct_config/` package with logically grouped sub-modules.

**Proposed package structure:**

```
sdcm/sct_config/
├── __init__.py              # Re-exports SCTConfiguration and init_and_verify_sct_config
│                            # for backward compatibility
├── config.py                # SCTConfiguration class (core: __init__, merge, load)
│                            # ~300 lines after Phase 1+2
├── fields/
│   ├── __init__.py          # Re-exports all field groups
│   ├── types.py             # Custom types: StringOrList, IntOrList, BooleanOrList,
│   │                        # MultitenantValue, DictOrStrOrPydantic, SctField, etc.
│   │                        # (currently lines 80–235 + Pydantic type definitions)
│   ├── cluster.py           # Cluster sizing, backend, regions, instance types
│   │                        # (~50 fields: n_db_nodes, cluster_backend, region_names, etc.)
│   ├── scylla.py            # Scylla core: version, repo, linux_distro, docker_image
│   │                        # (~20 fields)
│   ├── cloud_images.py      # AMI, GCE, Azure image fields
│   │                        # (~40 fields: ami_id_*, gce_image_*, azure_image_*)
│   ├── stress.py            # Stress tool configuration
│   │                        # (~43 fields: stress_cmd_*, prepare_*_cmd)
│   ├── nemesis.py           # Nemesis configuration
│   │                        # (~12 fields: nemesis_class_name, nemesis_interval, etc.)
│   ├── kubernetes.py        # K8S/EKS/GKE fields
│   │                        # (~35 fields: k8s_*, eks_*)
│   ├── cloud_providers.py   # GCE, Azure, AWS-specific fields
│   │                        # (~30 fields: gce_*, azure_*, aws_*)
│   ├── manager.py           # Scylla Manager fields
│   │                        # (~8 fields: mgmt_*)
│   ├── monitoring.py        # Monitoring and reporting fields
│   │                        # (~10 fields: monitor_*, email_*)
│   └── performance.py       # Performance test fields
│                            # (~5 fields: perf_*)
├── validators.py            # Cross-field validators extracted from __init__
│                            # and verify_configuration() (~300 lines)
├── image_resolution.py      # Cloud image resolution logic
│                            # (~200 lines: resolve AMI, GCE, Azure, Docker images)
├── defaults.py              # Default config file paths, backend defaults,
│                            # required_params, backend_required_params
│                            # (currently lines 2800–3050)
└── helpers.py               # merge_dicts_append_strings, is_config_option_appendable,
                             # converter functions
                             # (currently lines 80–235 not covered by types.py)
```

**Migration strategy** (inspired by [Nemesis rework](https://github.com/scylladb/scylla-cluster-tests/blob/master/docs/plans/nemesis-rework.md)):

1. Create `sdcm/sct_config/` package with `__init__.py` that re-exports everything:
   ```python
   # sdcm/sct_config/__init__.py
   from sdcm.sct_config.config import SCTConfiguration
   from sdcm.sct_config.config import init_and_verify_sct_config
   ```
2. Move code file-by-file, keeping `__init__.py` as the public API
3. No consumer code changes needed — all imports (`from sdcm.sct_config import SCTConfiguration`) continue to work

**Sub-phase 3a**: Create package, move types and helpers
**Sub-phase 3b**: Extract field groups into sub-modules using Pydantic model composition
**Sub-phase 3c**: Extract validators and image resolution
**Sub-phase 3d**: Extract defaults and backend configuration

**Open Question**: Whether to use Pydantic model composition (nested models) or keep all fields flat on `SCTConfiguration` and just organize the source code into modules. The recommendation from the PR #13104 review ([comment by @pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)) is to go "deep not wide" — restructuring prefixed fields (e.g., `nemesis_*`) into nested config objects (e.g., `config.nemesis.class_name`). This is a larger change that should be evaluated with a PoC.

**Definition of Done:**
- [ ] `sdcm/sct_config/` package exists with sub-modules
- [ ] `from sdcm.sct_config import SCTConfiguration` works (backward compatible)
- [ ] No file in the package exceeds ~500 lines
- [ ] All existing tests pass without changes
- [ ] Pre-commit and linting pass

**Dependencies**: Phase 2 (image resolution extracted)

---

### Phase 4: Typed Attribute Access (Eliminate `.get("string_key")`)

**Objective**: Enable IDE autocomplete and type checking for configuration access by migrating from `params.get("key")` to `params.key` across the codebase.

**Current problem:**
```python
# No type checking, no autocomplete, no IDE support
duration = self.params.get("test_duration")  # returns Any
backend = self.params.get("cluster_backend")  # returns Any
```

**Target state:**
```python
# Full IDE support: autocomplete, type checking, refactoring
duration = self.params.test_duration   # IDE knows this is int
backend = self.params.cluster_backend  # IDE knows this is str
```

**Implementation approach:**

This phase leverages the Pydantic `BaseModel` from PR #13104, where fields are already typed:
```python
class SCTConfiguration(BaseModel):
    test_duration: int = SctField(description="...")
    cluster_backend: str = SctField(description="...")
```

**Migration strategy** (incremental, file-by-file):

1. **Enable `__getattr__` deprecation warnings** on `SCTConfiguration.get()` to track migration progress:
   ```python
   def get(self, key, default=_SENTINEL):
       warnings.warn(
           f"SCTConfiguration.get('{key}') is deprecated, use attribute access instead",
           DeprecationWarning,
           stacklevel=2,
       )
       return getattr(self, key, default)
   ```

2. **Migrate file-by-file**, prioritizing high-usage files:

   | File | `.get()` calls | Priority |
   |------|---------------|----------|
   | `sdcm/tester.py` | ~287 | High |
   | `sdcm/cluster.py` | ~108 | High |
   | `sdcm/cluster_k8s/__init__.py` | ~76 | Medium |
   | `sdcm/logcollector.py` | ~21 | Medium |
   | `sdcm/mgmt/operations.py` | ~25 | Medium |
   | `sdcm/ycsb_thread.py` | ~22 | Low |
   | All other files | ~217 | Low |

3. **Use automated refactoring** where possible:
   - `sed`/regex to convert `self.params.get("key")` → `self.params.key`
   - Handle `self.params.get("key", default)` cases by ensuring defaults are in YAML files
   - Pyright/mypy to validate correctness after migration

4. **Address `params.get("key", default)` anti-pattern**: Per the [SCT Configuration Guide](https://github.com/scylladb/scylla-cluster-tests/blob/master/docs/sct-configuration.md), defaults should live in `defaults/*.yaml`, not scattered in code. Each `.get("key", default)` should be reviewed — the default should move to YAML if it's not there already.

**Definition of Done:**
- [ ] All `params.get("key")` calls in `sdcm/` replaced with `params.key`
- [ ] All `params.get("key", default)` calls reviewed — defaults moved to YAML where appropriate
- [ ] Pyright type checking passes for migrated files
- [ ] IDE autocomplete works for all configuration attributes
- [ ] No runtime behavior changes

**Dependencies**: PR #13104 merged (Pydantic BaseModel)

---

### Phase 5 (Future): Deep Configuration Structure

**Objective**: Restructure flat prefixed fields into nested configuration objects for better organization and per-group validation.

**Current (flat):**
```python
config.nemesis_class_name
config.nemesis_interval
config.nemesis_seed
config.stress_cmd
config.stress_cmd_w
config.stress_cmd_r
```

**Target (nested):**
```python
config.nemesis.class_name
config.nemesis.interval
config.nemesis.seed
config.stress.cmd
config.stress.cmd_w
config.stress.cmd_r
```

**Implementation approach:**
- Define sub-models as Pydantic `BaseModel` classes (e.g., `NemesisConfig`, `StressConfig`)
- Each sub-model handles its own validation
- Maintain backward compatibility with flat access via `__getattr__` bridge
- YAML config files would support both flat and nested formats during transition

**Open Questions:**
- How to handle backward compatibility with existing YAML configs that use flat keys
- Whether to support both `config.nemesis_class_name` and `config.nemesis.class_name` during transition
- Impact on 430+ test configuration YAML files

**This phase needs a separate PoC** before committing to a specific approach. It is documented here for completeness but is out of scope for the initial follow-up.

**Dependencies**: Phases 1–3 completed

## Testing Requirements

### Per-Phase Testing

| Phase | Unit Tests | Integration Tests | Manual Tests |
|-------|-----------|------------------|-------------|
| Phase 1 | Test each validator independently with valid/invalid inputs | Existing config loading tests | Verify error messages match |
| Phase 2 | Test `SCTConfiguration()` without network; test `resolve_images()` with mocked APIs | Full `init_and_verify_sct_config()` with Docker backend | AWS/GCE/Azure artifact tests |
| Phase 3 | Import tests; verify all fields accessible | Existing test suite passes | Verify no import regressions |
| Phase 4 | Pyright/mypy type check coverage | Existing test suite passes | IDE autocomplete verification |

### Regression Testing

Each phase must pass:
- `uv run sct.py unit-tests`
- `uv run sct.py pre-commit`
- At least one artifact test (AWS or Docker) to verify end-to-end config loading
- Longevity test to verify runtime behavior

## Success Criteria

1. **`SCTConfiguration()` can be instantiated without network access** — enables fast unit tests and utility scripts
2. **`__init__` is under 150 lines** — only configuration file loading and merging
3. **No file in `sdcm/sct_config/` exceeds 500 lines** — maintainable module sizes
4. **Zero `params.get("string_key")` calls in `sdcm/`** — full typed attribute access
5. **Pyright reports no type errors** on configuration access patterns
6. **All existing tests pass** without modifications (beyond test-specific config access updates)

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Breaking existing YAML configs | High | Backward-compatible changes only; Pydantic aliases for renamed fields |
| Breaking `params.get()` consumers | High | Incremental migration with deprecation warnings; keep `.get()` working throughout |
| Cloud API changes during refactor | Medium | Mock all cloud APIs in unit tests; integration tests catch regressions |
| Import cycle from package split | Medium | Careful dependency ordering; `TYPE_CHECKING` imports where needed |
| Performance regression from Pydantic validation | Low | Benchmark config loading time before/after; Pydantic v2 is fast |
| Merge conflicts with parallel development | High | Small, focused PRs; coordinate with team on merge order |
| Test YAML files need updates for nested config (Phase 5) | High | Phase 5 deferred until PoC validates approach; maintain flat format support |
