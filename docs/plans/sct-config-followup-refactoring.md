---
status: in_progress
domain: config
created: 2026-03-01
last_updated: 2026-09-06
owner: fruch
---
# Splitting `sct_config.py` into a Module

## Problem Statement

`sdcm/sct_config.py` is a ~5,400-line monolith containing 537 configuration fields, validation logic, image resolution, helper functions, and backend-specific defaults — all in a single class and file. This makes it extremely hard for both humans and LLMs to review, maintain, or extend.

[PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) migrated `SCTConfiguration` from a custom `dict` to a Pydantic `BaseModel`, providing a foundation for further refactoring. This plan focuses specifically on **splitting the monolithic file into a well-organized package**.

> **Note**: Related refactoring goals — extracting validation from `__init__`, making image resolution lazy/optional, and migrating `.get()` to typed attribute access — will be tracked in separate plans (to be created).

Key pain points from the PR #13104 review ([comment by @soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971791016), [response by @pehala](https://github.com/scylladb/scylla-cluster-tests/pull/13104#issuecomment-3971881663)):

1. **Monolithic file**: 5,400+ lines in a single file with 537 configuration fields cannot be reasonably reviewed or maintained.
2. **Single class owns everything**: All fields, all validation, all image resolution, all backend defaults live in one class — making it impossible to test or mock individual parts.
3. **Validation coupled to structure**: The flat "wide" config means all validation must live in one place, growing linearly with every new field.
4. **Hard to mock in tests**: Without structural separation, tests that only need a few config fields must construct or mock the entire configuration object.

## Current State

### File: `sdcm/sct_config.py` (~5,390 lines)

> **Note**: Line numbers reference `upstream/master` at commit `d2bff8c102` (2026-09-06) and are
> indicative only — they shift constantly. The logical sections are what matter.

**Module level (lines 1–732), before the class:**
- Imports (18–105), including 25 `sdcm.*` modules — this is what makes the module expensive to import.
- Type aliases and their `BeforeValidator` converters (162–485): `String`, `StringOrList`, `IntOrList`,
  `BooleanOrList`, `DictOrStr`, `DictOrStrOrPydantic`, `Boolean`, plus `IgnoredType`, `InputType`,
  `is_ignored_field` and the `AdaptiveTimeoutMultipliers` RootModel.
- `SctField` (488–523), the custom `FieldInfo` carrying `appendable`.
- Backend constants (526–565): `available_backends`, `AWS_SUPPORTED_REGIONS`, `BACKEND_IMAGE_FIELD` —
  these are **already** module level, not class attributes.
- Helpers (112–160, 448–483, 568–626): `_nested_env_subkey`, the keystore-env tracking pair,
  `is_config_option_appendable`, `merge_dicts_append_strings`, `count_regions`,
  `_load_docker_images_defaults_cached`, `simulated_racks_enabled`.
- Oracle image resolvers (628–732): `_resolve_oracle_images_{aws,azure,gce,oci}` and
  `_ORACLE_IMAGE_RESOLVERS`.

**Class `SCTConfiguration(BaseModel)` — line 734:**
- Field definitions — 739–2749 (~2,000 lines, **537** fields as of this commit).
- Class-level lookup tables — 2750–2993, eight `Annotated[..., IgnoredType]` attributes:
  `required_params`, `backend_required_params`, `defaults_config_files`,
  `per_provider_multi_region_params`, `xcloud_per_provider_required_params`, `stress_cmd_params`,
  `ami_id_params`, `aws_supported_regions`. These are real Pydantic fields, not `ClassVar`s: they
  appear in `model_dump()`, and Pydantic deep-copies their mutable defaults per instance, which
  `_check_backend_defaults` relies on when it mutates `self.backend_required_params`.
- `__init__` — 3036–3599 (~560 lines): YAML loading, region data, env-var merge, cloud image
  resolution, repo symlinks, `user_prefix`, then inline validation.
- Methods — 3002–5385: dict-compat accessors, properties, `_load_environment_variables`,
  `_resolve_instance_sizes`, `verify_configuration` and its ~25 `_validate_*` helpers,
  `get_version_based_on_conf`, and the `dump_help_config_*` doc generators.

**Orchestrator** — `init_and_verify_sct_config()` at 5382.

### Coupling that constrains the split

Discovered while executing Phase 1; every one of these is a silent failure if missed.

1. **~49 monkeypatch targets are strings** of the form `"sdcm.sct_config.<name>"`, in production
   code (`sdcm/utils/lint/validator.py`'s `_CLOUD_API_PATCHES`) and across 9 test modules, plus 16
   `patch.object(sct_config, ...)` statements in `unit_tests/unit/test_config.py`. They name
   third-party functions (`convert_name_to_ami_if_needed`, `KeyStore`, `get_branched_ami`, …) that
   the config module merely imports. Moving the call sites into a submodule makes those patches
   target a module nobody reads — they do not error, they simply stop working, and a unit test
   then falls through to a real cloud API call.
   *Mitigation*: keep the package `__init__.py` narrow — re-export only what the repo imports, so a
   stale patch raises `AttributeError` loudly instead of silently no-op'ing.
2. **`is_config_option_appendable()` reads `SCTConfiguration.model_fields`**, so any `helpers`
   module holding it would import `config` — a cycle. *Mitigation*: pass the model class as a
   parameter.
3. **`pathlib.Path(__file__).parent.parent / "data" / "instance_catalog"`** resolves one directory
   wrong as soon as the file moves into a package, and `_resolve_instance_sizes` swallows the
   resulting `FileNotFoundError` with a warning — constraint-based sizing would silently stop
   resolving on every run. *Mitigation*: `sct_abs_path()`, plus a regression test.
4. **Two path patterns stop matching a package**: `.pre-commit-config.yaml`'s `update-conf-docs`
   hook (`files: (?x)(sdcm/sct_config.py|...)`) and `.coderabbit.yaml`'s `path:`. The first means
   the generated `docs/configuration_options.md` silently rots.
5. **`logging.getLogger(__name__)`** changes every config log line, and one test pins
   `caplog.at_level(..., logger="sdcm.sct_config")`. *Mitigation*: hardcode the logger name.
6. **Importing a submodule does not bypass `__init__`**, so `types.py` is not yet a cheap leaf for
   other packages; the deliberate function-level imports in `sdcm/test_metadata.py` and
   `sdcm/utils/lint/validator.py` must stay.

### Configuration field groupings (537 fields)

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

**Decided: Approach A (Mixins)**, backed by @soyacz, with Approach B evaluated later as the
Phase 5 PoC rather than as a prerequisite. @pehala's position — that the nested structure is the
change that actually enables the split — is recorded in the PR discussion and is what Phase 5
exists to test with evidence rather than argument.

Phases 1 & 2 have since shipped without needing either approach: the package split and the
type/helper/defaults extraction are orthogonal to how the *fields* are organised. That is the
concrete evidence that the file split does not depend on nesting. What Phase 3 will show is
whether flat mixins alone get validation down to a reviewable size, or whether the wide class
remains the binding constraint — which is exactly the question Phase 5 then answers.

Rationale:
- Approach A has **zero breaking changes** — no YAML updates, no consumer code changes
- Approach A delivers the file split immediately, unblocking further refactoring
- Approach B needs a PoC to validate: backward-compatible YAML loading, environment variable mapping, impact on 430+ config files
- The mixin structure from Approach A can be incrementally converted to nested models later

## Implementation Phases

### Phases 1 & 2: Create the Package, Extract Types, Helpers and Defaults — DONE

**Objective**: Convert `sdcm/sct_config.py` into a `sdcm/sct_config/` package and lift out
everything that is not the configuration model itself. Shipped together as one PR because both are
pure moves over the same file.

**Resulting structure:**
```
sdcm/sct_config/
├── __init__.py       # narrow public re-exports
├── config.py         # SCTConfiguration + init_and_verify_sct_config (~4.6K lines)
├── types.py          # Annotated aliases, converters, IgnoredType/InputType, SctField,
│                     # AdaptiveTimeoutMultipliers  (no sdcm.* imports)
├── helpers.py        # appendable merge, env sub-key parsing, count_regions,
│                     # docker-image defaults cache, simulated_racks_enabled
└── defaults.py       # available_backends, AWS_SUPPORTED_REGIONS, BACKEND_IMAGE_FIELD
                      # and the eight requirement tables
```

**Decisions worth carrying into Phase 3:**

- `__init__.py` re-exports **only** the names the repo imports (`SCTConfiguration`,
  `init_and_verify_sct_config`, `available_backends`, `AWS_SUPPORTED_REGIONS`,
  `BACKEND_IMAGE_FIELD`, `count_regions`, `simulated_racks_enabled`, `AdaptiveTimeoutMultipliers`,
  the four converters, `SctField`/`StringOrList`/`IntOrList`, and `TestConfig`). Everything else is
  deliberately absent so stale patch targets fail loudly. **Do not** add `from .config import *`.
- The eight class-level tables stay class fields, assigned from `defaults.py` constants. Turning
  them into direct module references is a behaviour change (see Current State §Coupling).
- `is_config_option_appendable(option_name, model)` and
  `merge_dicts_append_strings(d1, d2, model)` take the model class explicitly.
- Both loggers are named `"sdcm.sct_config"` literally, not `__name__`.
- The catalog directory uses `sct_abs_path()`, guarded by
  `test_catalog_directory_resolves_to_a_real_directory`.

**Also updated**: 49 string patch targets + 16 `patch.object` statements, the
`.pre-commit-config.yaml` and `.coderabbit.yaml` path patterns, doc prose, and the stale patch
target taught by `skills/writing-unit-tests/references/common-pitfalls.md`.

**Definition of Done:**
- [x] `from sdcm.sct_config import SCTConfiguration` / `init_and_verify_sct_config` work unchanged
- [x] `SCTConfiguration.model_fields` — 537 fields, order byte-identical to before
- [x] `docs/configuration_options.md` regenerates with no diff
- [x] `sct.py conf -b docker <test case>` dumps a byte-identical config
- [x] Unit tests, pre-commit and `lint-pipelines` (1136/1136) pass

**Dependencies**: PR #13104 (merged)

---

### Phase 3: Extract Field Definitions into Domain Mixins — DONE

Shipped in [PR #15972](https://github.com/scylladb/scylla-cluster-tests/pull/15972) alongside
Phases 1-2. `config.py` went 5,390 -> 2,711 lines; no mixin module exceeds 250.

**The section comments looked like the grouping, and weren't.** `sct_config.py` carried comments
(`# AWS config options`, `# Nemesis config options`, `# LongevityTest`, ...) that read like
maintained section boundaries, and the first split trusted them. They do not bound their sections:
options had been appended to whichever comment happened to be last, so

- `# minicloud params` had accumulated **37 unrelated options** — log collection, `post_behavior_*`
  teardown, upgrade stress, AZ fallback;
- the AWS section held **every `gce_*` option**;
- `# spark-migrator` held scylla-doctor and zero-token options.

Grouping is now decided per option by **what it configures**, with the option's own description as
the evidence. That is why the documentation audit below is part of the same work: an option whose
description only restated its name could not be placed, and 19 options had no description at all.

**Lessons for Phase 4**, which faces the same hazard with the `_validate_*` methods:

1. Do not trust in-file section comments as structure. They drift silently, because appending to
   the end of a file is easier than finding the right section, and nothing checks it.
2. An exhaustiveness assertion turns "did I miss one?" into a build error. The split is generated
   under an assertion that all 539 class entries are accounted for: **523 options across 28
   mixins**, plus 16 that stay on the assembler.
3. Encode the convention as a test, not a review comment. `test_option_groups.py` now asserts one
   mixin per option, prefix/group agreement (with a commented exception list), and a description
   that says more than the option's name. The original file failed all three, which is exactly how
   the drift went unnoticed for years.

**Actual mixins**, in `CONFIG_GROUPS` order (cross-cutting, then per backend, then per test type):

| Group | Fields | Group | Fields | Group | Fields |
|---|---|---|---|---|---|
| `common` | 70 | `aws` | 25 | `longevity` | 13 |
| `scylla` | 44 | `gce` | 23 | `performance` | 17 |
| `nemesis` | 12 | `azure` | 13 | `upgrade` | 20 |
| `stress` | 73 | `oci` | 10 | `grow_cluster` | 2 |
| `monitoring` | 17 | `kubernetes` | 44 | `refresh` | 6 |
| `logs` | 19 | `docker` | 2 | `jepsen` | 4 |
| `manager` | 26 | `baremetal` | 7 | `emr` | 13 |
| `aux_db` | 10 | `xcloud` | 12 | `spark_migrator` | 9 |
| `alternator` | 10 | `minicloud` | 14 |  |  |
| `vector_store` | 6 |  |  |  |  |
| `kafka` | 2 |  |  |  |  |

Differences from the proposal above, all driven by what the options actually configure:

- 28 groups, not 16. `logs`, `aux_db`, `alternator` and `kafka` exist because those options had
  nowhere sensible to go; `security`, `monitoring`, `vector_store`, `minicloud`, `emr`,
  `spark_migrator`, `oci`, `grow_cluster`, `refresh` and `jepsen` are real self-contained domains.
- No `FeatureConfigMixin` catch-all. Every option has a domain, and the exhaustiveness assertion is
  what proved it.
- No `security` group. Review pointed out it was a mixture: LDAP, encryption and the
  authenticator/authorizer settings are Scylla features (→ `scylla`), while `keystore_*` and
  `user_credentials_path` are SCT's own credentials (→ `common`).
- `aux_db` is named for the role rather than for Gemini's "oracle", so migration tests that use a
  Cassandra cluster fit the same group.
- `test_level` does not exist: those options split between `common` and `monitoring`.
- `minicloud` is described as an AWS-API-compatible environment rather than a backend, since it runs
  with `cluster_backend: aws` plus an endpoint override. The naming is still open (see PR #15972).

**Documentation is part of the grouping work.** 19 options had no description, 21 shared boilerplate
with a sibling (all 13 cassandra-stress commands carried one identical paragraph), and 20 only
restated their own name. All now say what they control; `docs/configuration_options.md` went from
one flat list of 523 options to 28 browsable sections.

**Stays on the assembler** (16 entries): runtime state (`multi_region_params`, `regions_data`,
`artifact_scylla_version`, `is_enterprise`, `scylla_version_upgrade_target`, `target_db_image_ids`,
`log`, `_THROTTLE_STEP_FIELD_CHECKS`) and the eight `IgnoredType` lookup tables. The tables must
stay class *fields* assigned from `defaults.py` constants — Pydantic deep-copies mutable defaults
per instance and `_check_backend_defaults` mutates them, so a module global would accumulate across
every `SCTConfiguration()` in a process.

**Grouped documentation.** `docs/configuration_options.md` was a flat list of 523 options and now
carries a section per group. Both doc dumpers go through `_fields_by_group()`, which reads each
mixin's `model_fields` — **not** `__dict__["__annotations__"]`: Python 3.14 defers annotation
evaluation (PEP 649), so that key is absent until something forces it, and the first attempt
silently filed all 523 options under "Other". Unclaimed fields still land in a trailing "Other"
section so nothing can drop out of the docs unnoticed.

**Definition of Done:**
- [x] All field definitions moved to mixin files
- [x] `config.py` holds only the assembler, `__init__`, cross-domain validators and doc generation
- [x] No file in the package exceeds ~500 lines except `config.py` (2,711 — Phase 4 territory)
- [x] Every option documented, one mixin per option, prefixes agree with groups (guard test)
- [x] Field *set* byte-identical; order follows `CONFIG_GROUPS` by design
- [x] A docker config dump compares key-for-key against master (244 keys, same values)
- [x] All existing tests pass unmodified (4134 passed)

**Dependencies**: Phases 1-2

---

### Phase 4: Extract Validation Methods

**Objective**: Move the `_validate_*` methods and `verify_configuration()` logic into domain-appropriate locations.

**This is now where the remaining size lives.** After Phase 3, `config.py` is 2,711 lines:
the ~560-line `__init__`, ~25 `_validate_*` methods, and the doc generators. The 25 mixins from
Phase 3 give each validator an obvious destination — `_validate_docker_backend_parameters` to
`DockerConfigMixin`, `_validate_placement_group_required_values` to `AwsConfigMixin`, and so on.

Two things Phase 3 established that this phase should reuse:
- Mixins are plain `BaseModel`s inheriting only from `BaseModel`, so a `@field_validator` on a
  mixin runs on the assembled model with no extra wiring.
- The exhaustiveness assertion pattern: enumerate what must move, assert nothing is left behind,
  and let it fail the build rather than relying on review to spot a gap.

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
| Phases 1–2 | Import tests; field count and order unchanged | Existing test suite passes | Config dump diffed against `master` |
| Phase 3 | Each mixin tested independently for field presence | Existing test suite passes | Verify no import regressions |
| Phase 4 | Validator tests per mixin | Existing test suite passes | Verify error messages match |
| Phase 5 | PoC: flat+nested YAML loading, nested access, mocking | Docker backend config loading | — |

### Regression Testing

Each phase must pass:
- `uv run sct.py unit-tests`
- `uv run sct.py pre-commit`
- `uv run sct.py lint-pipelines` — the only thing that exercises `_CLOUD_API_PATCHES`; a stale
  patch target there falls through to real cloud calls
- `grep -rn '"sdcm\.sct_config\.[a-z_]' --include=*.py .` names no moved function
- `docs/configuration_options.md` regenerates with no unexpected diff (field **order** shifts once
  mixins land in Phase 3 — that diff is expected and should be reviewed, not suppressed)
- A config dump (`sct.py conf -b docker <test case>`) diffed against `master` for the same inputs
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
