# Typed Configuration Access Migration Plan

## Problem Statement

After [PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) migrated `SCTConfiguration` from a custom `dict`-based class to a Pydantic `BaseModel`, every configuration field is now a typed class attribute with IDE-resolvable type information. However, roughly **1,100 call sites** across ~80 files still access configuration through the legacy `params.get("string_key")` pattern, which:

1. **Bypasses IDE support** — no autocomplete, no type checking, no go-to-definition
2. **Returns `Any`** — callers get no static type information, hiding bugs until runtime
3. **Scatters implicit defaults** — a handful of `.get("key", fallback)` calls duplicate defaults that should live in `defaults/*.yaml`
4. **Blocks future refactoring** — the `.get()` method contains special-case logic (multi-region string joining, dotted-key dict traversal) that complicates reasoning about configuration access

Converting these to direct attribute access (`params.key`) will fully leverage the Pydantic migration and make the codebase safer, more navigable, and easier to maintain.

## Current State

### `SCTConfiguration.get()` method — `sdcm/sct_config.py:2918`

The current `.get()` method wraps `getattr` with three special behaviors:

```python
def get(self, key: str | None):
    if key is None:
        return None
    if key and "." in key:
        if ret_val := self._dotted_get(key):
            return ret_val
    ret_val = getattr(self, key, None)
    if key in self.multi_region_params and isinstance(ret_val, list):
        ret_val = " ".join(str(v) for v in ret_val)
    return ret_val
```

1. **`None` guard** — returns `None` when called with `key=None` (no call sites actually use this today)
2. **Dotted-key traversal** (`_dotted_get`) — handles `params.get("stress_image.ycsb")` by splitting on `.` and traversing nested dicts (4 unique dotted keys used across 6 call sites: `stress_image.cassandra-stress`, `stress_image.scylla-bench`, `stress_image.alternator-dns`, `stress_image.ycsb`). All dotted keys access sub-keys of the `stress_image: DictOrStr` field, which is a `dict` at runtime.
3. **Multi-region string joining** — when the key is in `multi_region_params` (`region_name`, `ami_id_db_scylla`, `ami_id_loader`, `gce_datacenter`) and the value is a list, it joins elements with spaces

### Dict-like access methods — `sdcm/sct_config.py:2266–2298`

```python
def __getitem__(self, item):       # config["key"]
def __setitem__(self, key, value): # config["key"] = value
def __contains__(self, key):       # "key" in config
def update(self, other=None, **new_data):
```

These enable backward-compatible dict-style access and are used in ~40 `self.params["key"]` call sites and various `update()` calls.

### Call-site distribution

| Category | Call sites | Files |
|----------|-----------|-------|
| `self.params.get("literal_key")` in `sdcm/` | ~755 | ~46 |
| `self.params.get("literal_key")` in test files (`*_test.py`) | ~351 | ~36 |
| `X.params.get("literal_key")` (non-self, e.g., `self.cluster.params`, `self.tester.params`) in `sdcm/` | ~317 | ~30+ |
| `params.get(variable_key)` (dynamic/computed keys) | ~30 | ~12 |
| `params.get("dotted.key")` (dict traversal) | ~6 | ~4 |
| `params.get(f"dynamic_{key}")` (f-string keys) | ~4 | ~3 |
| `self.params["key"]` (dict-style bracket access) | ~40 | ~5 |
| **Total** | **~1,150+** | **~82** |

### Highest-usage files

| File | `.get()` calls | Access pattern |
|------|---------------|----------------|
| `sdcm/tester.py` | ~287 | `self.params.get(...)` |
| `sdcm/cluster.py` | ~142 | ~108 `self.params.get(...)` + ~34 `X.params.get(...)` (non-self) |
| `sdcm/cluster_k8s/__init__.py` | ~85 | `self.params.get(...)` |
| `sdcm/nemesis/__init__.py` | ~72 | `self.cluster.params.get(...)`, `self.tester.params.get(...)` |
| `upgrade_test.py` | ~59 | `self.params.get(...)` |
| `performance_regression_test.py` | ~50 | `self.params.get(...)` |
| `longevity_test.py` | ~40 | `self.params.get(...)` |
| `sdcm/mgmt/operations.py` | ~30 | `self.params.get(...)` |
| `sdcm/cluster_k8s/eks.py` | ~35 | `self.params.get(...)`, `params.get(...)` |
| `sdcm/cluster_k8s/gke.py` | ~32 | `self.params.get(...)`, `params.get(...)` |

### Most-accessed configuration keys (top 15)

| Key | Call sites | Typical usage |
|-----|-----------|---------------|
| `cluster_backend` | 35 | Backend routing: `if self.params.get("cluster_backend") == "aws"` |
| `user_prefix` | 22 | Resource naming |
| `user_credentials_path` | 18 | SSH key path |
| `n_loaders` | 18 | Cluster sizing |
| `n_db_nodes` | 18 | Cluster sizing |
| `reuse_cluster` | 14 | Test mode toggle |
| `client_encrypt` | 13 | TLS configuration |
| `use_mgmt` | 12 | Feature flag |
| `n_monitor_nodes` | 12 | Cluster sizing |
| `logs_transport` | 12 | Log routing |
| `simulated_regions` | 11 | Region simulation |
| `availability_zone` | 11 | AWS AZ configuration |
| `alternator_port` | 9 | DynamoDB-compatible API |
| `k8s_local_volume_provisioner_type` | 8 | K8S storage |
| `db_type` | 8 | Database type routing |

### Existing attribute-style access (already working)

Some properties and computed attributes are already accessed via attribute style:

- `self.params.region_names` (~22 call sites) — `@property`
- `self.params.cloud_provider_params` (~9 call sites) — `@property`
- `self.params.artifact_scylla_version` (~6 call sites) — plain field
- `self.params.gce_datacenters` (~4 call sites) — `@property`
- `self.params.is_enterprise` (~1 call site) — plain field

This confirms that attribute access works today for any field and the codebase already has precedent for it.

## Goals

1. **Typed attribute access**: All configuration access should use `params.key` (with IDE autocomplete and type checking) instead of `params.get("key")`
2. **Eliminate implicit defaults**: The ~4 `.get("key", fallback)` calls should move their defaults into `defaults/*.yaml` configuration files
3. **Preserve dynamic access**: The ~30 call sites using variable keys (e.g., `params.get(param_name)`) should use `getattr(params, key)` with explicit handling
4. **Preserve dotted-key access**: The ~6 dotted-key call sites (e.g., `params.get("stress_image.ycsb")`) should use explicit dict traversal on the parent attribute
5. **No runtime behavior changes**: The migration must be purely mechanical — same values, same defaults, same error handling
6. **Incremental migration**: Each phase is a standalone PR that doesn't break existing functionality

## Implementation Phases

### Phase 1: Add Deprecation Warning to `.get()` and Prepare Tooling

**Objective**: Instrument the `.get()` method with a deprecation warning so migration progress can be tracked, and prepare automated migration tooling.

**Implementation**:

1. Add a `DeprecationWarning` to `SCTConfiguration.get()` for literal string keys:
   ```python
   def get(self, key: str | None):
       warnings.warn(
           f"SCTConfiguration.get('{key}') is deprecated, "
           "use attribute access (params.key) instead",
           DeprecationWarning,
           stacklevel=2,
       )
       # ... existing logic
   ```

2. Add a `pytest` warning filter in `unit_tests/pytest.ini` to surface these during test runs:
   ```ini
   filterwarnings =
       default::DeprecationWarning:sdcm.sct_config
   ```

3. Create a migration helper script (in `/tmp`, not committed) that performs safe `sed`-based conversion:
   - `self.params.get("literal_key")` → `self.params.literal_key`
   - Flags `.get("key", default)` calls for manual review
   - Flags variable-key and dotted-key calls as non-convertible

**Definition of Done**:
- [ ] `DeprecationWarning` added to `SCTConfiguration.get()`
- [ ] Warning filter configured in test infrastructure
- [ ] `uv run sct.py unit-tests` passes (warnings don't fail tests)
- [ ] Migration progress trackable via warning count

**Dependencies**: [PR #13104](https://github.com/scylladb/scylla-cluster-tests/pull/13104) merged (Pydantic `BaseModel` foundation)

---

### Phase 2: Migrate `sdcm/` Framework Files (High-Priority)

**Objective**: Convert the ~755 `self.params.get("key")` calls in `sdcm/` framework files to direct attribute access, starting with the highest-usage files.

**Sub-phase 2a — Core files** (~430 calls, 3 files):

| File | Calls | Notes |
|------|-------|-------|
| `sdcm/tester.py` | ~287 | Straightforward literal key conversion |
| `sdcm/cluster.py` | ~108 | `self.params.get()` calls only; ~34 additional `X.params.get()` calls from non-self access are handled alongside their respective callers. Contains multi-region and f-string dynamic keys — convert literals, keep `getattr()` for dynamic |
| `sdcm/cluster_k8s/__init__.py` | ~76 | Mix of literal and some dynamic keys |

**Conversion rules**:

| Pattern | Before | After |
|---------|--------|-------|
| Literal key | `self.params.get("test_duration")` | `self.params.test_duration` |
| Truthiness check | `if self.params.get("use_mgmt"):` | `if self.params.use_mgmt:` |
| Assignment | `x = self.params.get("n_db_nodes")` | `x = self.params.n_db_nodes` |
| Comparison | `self.params.get("cluster_backend") == "aws"` | `self.params.cluster_backend == "aws"` |
| Dynamic key (variable) | `self.params.get(param_name)` | `getattr(self.params, param_name)` |
| Dynamic key (f-string) | `self.params.get(f"post_behavior_{key}_nodes")` | `getattr(self.params, f"post_behavior_{key}_nodes")` |
| Dotted key | `self.params.get("stress_image.ycsb")` | `self.params.stress_image["ycsb"]` |
| With explicit default | `self.params.get("key", False)` | `self.params.key` (move default to YAML) |

**Note on dotted-key access alternatives**: Pydantic does not natively support dotted-string key traversal (e.g., `params.get("stress_image.ycsb")`). Third-party libraries that do:
- **[glom](https://glom.readthedocs.io/)** — `glom(params.model_dump(), "stress_image.ycsb")`, read-only, powerful path expressions
- **[python-box](https://github.com/cdgriffith/Box)** — wraps dicts for attribute access (`Box(d).stress_image.ycsb`), but adds a runtime wrapper
- **[dotty-dict](https://dotty-dict.readthedocs.io/)** — `dotty(d)["stress_image.ycsb"]`, supports get/set

However, since all 6 dotted-key call sites access sub-keys of a single `dict` field (`stress_image`), introducing a dependency for this is unnecessary. Direct dict access (`self.params.stress_image["ycsb"]`) is clearer, has zero dependencies, and provides the same functionality. If future phases introduce nested Pydantic sub-models (see Phase 5 of the [follow-up plan](https://github.com/scylladb/scylla-cluster-tests/pull/13845)), dotted access would become native attribute chaining (`self.params.stress.image.ycsb`).

**Sub-phase 2b — Cluster backend files** (~120 calls, ~8 files):

| File | Calls |
|------|-------|
| `sdcm/cluster_k8s/mini_k8s.py` | ~22 |
| `sdcm/cluster_k8s/eks.py` | ~10 |
| `sdcm/cluster_k8s/gke.py` | ~7 |
| `sdcm/cluster_aws.py` | ~13 |
| `sdcm/cluster_azure.py` | ~4 |
| `sdcm/cluster_gce.py` | ~4 |
| `sdcm/cluster_cloud.py` | ~8 |
| `sdcm/cluster_docker.py` | ~6 |

**Sub-phase 2c — Remaining `sdcm/` files** (~205 calls, ~35 files):
- Provisioning: `sdcm/sct_provision/` (~45 calls, mostly dynamic keys from class constants)
- Stress threads: `sdcm/ycsb_thread.py`, `sdcm/scylla_bench_thread.py`, etc. (~40 calls)
- Utilities: `sdcm/logcollector.py`, `sdcm/mgmt/`, etc. (~50 calls)
- Config builders: `sdcm/provision/scylla_yaml/` (~25 calls)
- Other: remaining scattered files (~45 calls)

**Special handling for `sct_provision/` dynamic access pattern**:

Files like `sdcm/sct_provision/region_definition_builder.py` and `sdcm/sct_provision/aws/instance_parameters_builder.py` use class-level string constants as keys:
```python
# Before
return self.params.get(self._INSTANCE_TYPE_PARAM_NAME)

# After — keep getattr for dynamic keys
return getattr(self.params, self._INSTANCE_TYPE_PARAM_NAME)
```

These are intentionally dynamic (the key is resolved via class hierarchy) and should use `getattr()`.

**Definition of Done**:
- [ ] All literal-key `params.get()` in `sdcm/` converted to attribute access
- [ ] Dynamic-key calls converted to `getattr()`
- [ ] Dotted-key calls converted to explicit dict traversal
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: Phase 1

---

### Phase 3: Migrate Test Files

**Objective**: Convert the ~351 `self.params.get("key")` calls in test files (`*_test.py` and `functional_tests/`) to direct attribute access.

**Sub-phase 3a — High-usage test files** (~150 calls, 5 files):

| File | Calls |
|------|-------|
| `upgrade_test.py` | ~59 |
| `performance_regression_test.py` | ~50 |
| `longevity_test.py` | ~40 |
| `mgmt_cli_test.py` | ~24 |
| `artifacts_test.py` | ~21 |

**Sub-phase 3b — Medium-usage test files** (~100 calls, ~10 files):

| File | Calls |
|------|-------|
| `performance_regression_gradual_grow_throughput.py` | ~17 |
| `gemini_test.py` | ~15 |
| `performance_regression_alternator_test.py` | ~12 |
| `performance_search_max_throughput_test.py` | ~10 |
| `mgmt_upgrade_test.py` | ~8 |
| `longevity_alternator_ttl_test.py` | ~8 |
| `grow_cluster_test.py` | ~8 |
| Other test files | ~22 |

**Sub-phase 3c — Remaining test files** (~100 calls, ~20 files):
- All remaining `*_test.py` files with <8 calls each
- `functional_tests/scylla_operator/` files

**Definition of Done**:
- [ ] All literal-key `params.get()` in test files converted to attribute access
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: Phase 2 (framework files migrated first to validate patterns)

---

### Phase 4: Migrate `params["key"]` Bracket Access

**Objective**: Convert the ~40 remaining `self.params["key"]` bracket-access calls to attribute access.

**Files affected**:

| File | Calls | Notes |
|------|-------|-------|
| `sdcm/tester.py` | ~12 | Mix of read and write: `self.params["key"] = value` |
| `sdcm/ycsb_thread.py` | ~3 | Read-only bracket access |
| `sdcm/stress/latte_thread.py` | ~3 | Read-only bracket access |
| `sdcm/cluster_k8s/mini_k8s.py` | ~1 | Bracket access `[]` only (`.get()` calls covered in Phase 2b) |
| `sdcm/cluster.py` | ~1 | Bracket access `[]` only (`.get()` calls covered in Phase 2a) |

**Conversion rules**:

| Pattern | Before | After |
|---------|--------|-------|
| Read | `self.params["test_duration"]` | `self.params.test_duration` |
| Write | `self.params["key"] = value` | `self.params.key = value` |

**Definition of Done**:
- [ ] All `params["key"]` bracket reads converted to `params.key`
- [ ] All `params["key"] = value` writes converted to `params.key = value`
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: Phase 2

---

### Phase 5: Remove Multi-Region String Joining from `.get()`

**Objective**: The `.get()` method silently joins multi-region list values into space-separated strings (`" ".join(str(v) for v in ret_val)`). After all call sites use attribute access, this behavior must be handled explicitly where needed.

**Affected keys** (`per_provider_multi_region_params`):
- AWS: `region_name`, `ami_id_db_scylla`, `ami_id_loader`
- GCE: `gce_datacenter`

**Analysis needed**: Audit each call site accessing these 4 keys to determine whether it expects:
- A `list` (the raw Pydantic field value) — use `self.params.region_name` (returns list)
- A space-joined `str` (the `.get()` behavior) — use `" ".join(str(v) for v in self.params.region_name)`

**Needs Investigation**: The exact set of call sites expecting the joined string vs. the raw list needs to be identified during implementation. The `multi_region_params` joining in `.get()` was a legacy convenience from the dict-based era and may mask type confusion in callers.

**Definition of Done**:
- [ ] All call sites for multi-region keys audited and converted
- [ ] String-joining moved to call sites that specifically need it
- [ ] `uv run sct.py unit-tests` passes
- [ ] Multi-region integration tests pass (AWS, GCE)

**Dependencies**: Phases 2–3 (most `.get()` calls already migrated)

---

### Phase 6: Clean Up and Deprecate `.get()` Method

**Objective**: After all call sites are migrated, simplify or remove the `.get()` method and the dict-like compatibility layer.

**Implementation**:

1. Remove the `DeprecationWarning` from `.get()` and replace with hard deprecation or removal
2. Simplify `.get()` to a thin wrapper if any dynamic-key uses remain:
   ```python
   def get(self, key: str | None):
       """Deprecated: use attribute access instead. Kept for dynamic key access only."""
       if key is None:
           return None
       return getattr(self, key, None)
   ```
3. Evaluate whether `__getitem__`, `__setitem__`, `__contains__`, and `update()` can be simplified or removed
4. Update `docs/sct-configuration.md` to document attribute access as the standard pattern

**Definition of Done**:
- [ ] `.get()` method simplified or removed
- [ ] Multi-region string joining removed from `.get()`
- [ ] Dotted-key traversal removed from `.get()` (replaced by explicit dict access)
- [ ] Documentation updated to recommend attribute access
- [ ] Zero `DeprecationWarning` emissions in test suite
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

**Dependencies**: Phases 2–5 completed

## Testing Requirements

### Per-Phase Testing

| Phase | Unit Tests | Integration Tests | Manual Tests |
|-------|-----------|------------------|-------------|
| Phase 1 | Verify deprecation warning fires on `.get()` | Existing tests pass with warnings | N/A |
| Phase 2 | Existing `unit_tests/test_config.py` + all unit tests pass | At least one backend test (Docker) | IDE autocomplete verification |
| Phase 3 | All unit tests pass | N/A | Spot-check test files |
| Phase 4 | All unit tests pass | N/A | N/A |
| Phase 5 | Multi-region config tests in `test_config.py` | AWS multi-region test | Verify multi-region value formats |
| Phase 6 | All unit tests pass, zero deprecation warnings | Full test suite | Documentation review |

### Regression Testing

Each phase must pass:
- `uv run sct.py unit-tests`
- `uv run sct.py pre-commit`
- At least one artifact test (AWS or Docker) to verify end-to-end config loading

### Automated Validation

After each sub-phase, verify migration progress:
```bash
# Count remaining .get() calls (should decrease toward zero)
grep -rn 'params\.get(' sdcm/ --include='*.py' | grep -v 'def get' | wc -l
grep -rn 'self\.params\.get(' *_test.py --include='*.py' | wc -l
```

## Success Criteria

1. **Zero literal-key `params.get("string")` calls** in `sdcm/` and test files
2. **Zero `params["string"]` bracket-access calls** across the codebase
3. **All remaining dynamic-key access** uses explicit `getattr(params, key)` pattern
4. **Multi-region string joining** handled at call sites, not hidden in `.get()`
5. **All existing tests pass** without modifications to test logic (only access pattern changes)
6. **IDE autocomplete works** for all configuration attributes on `self.params`

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Subtle behavior difference between `.get()` and attribute access | High | `.get()` returns `None` for unset fields via `getattr(self, key, None)`. Attribute access also returns `None` for `Optional` fields with `None` default. Verify equivalence for each field. |
| Multi-region string joining silently changes behavior | High | Phase 5 dedicated to auditing these 4 keys. Conservative approach: keep `.get()` for these keys until each call site is individually verified. |
| Dotted-key access breaks after `.get()` removal | Medium | Only 4 unique dotted keys (`stress_image.*`). Convert each to explicit `self.params.stress_image["sub_key"]`. |
| Dynamic-key access via class constants | Medium | ~30 call sites use variables. Convert to `getattr()` with explicit `None` handling where needed. |
| Breaking test YAML parsing or config loading | High | Run full unit test suite after each sub-phase. Keep `.get()` working throughout migration (deprecation, not removal). |
| Merge conflicts with parallel development | High | Small, focused PRs per sub-phase. Coordinate with team on merge order. Prioritize framework files first. |
| Performance regression from attribute access vs `.get()` | Low | Direct `getattr` is faster than the `.get()` method's conditional logic. No regression expected. |
