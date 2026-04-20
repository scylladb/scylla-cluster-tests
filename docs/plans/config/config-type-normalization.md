---
status: draft
domain: config
created: 2026-04-17
last_updated: 2026-04-17
owner: null
---

# Config Type Normalization Plan

## Problem Statement

The SCT configuration system defines four union-type aliases — `StringOrList`, `IntOrList`, `BooleanOrList`, and `DictOrStr` — whose `BeforeValidator` functions do not guarantee a single canonical output type. Consumers must defensively check `isinstance(value, list)` vs scalar everywhere, leading to:

- **~37 scattered `isinstance` guards** across `tester.py`, `sct_config.py`, `mgmt/operations.py`, `cluster_tools.py`, `longevity_test.py`, `loader_utils.py`, `operations_thread.py`, `performance_regression_test.py`, and others that manually wrap scalars in lists or branch on type before use.
- **Silent type-unsafety**: `ast.literal_eval()` in all four validators can return any Python literal (int, tuple, dict, str), not the declared type. For example, `str_or_list_or_eval("3")` returns `3` (an `int`), not `["3"]` or `[3]`.
- **Runtime `TypeError` risk**: ~25 call sites in K8s backends, capacity reservation, performance tests, and `grow_cluster_test.py` assume scalar `int` for `IntOrList` params like `n_db_nodes`, while other sites assume `list`. Both can't be right — the type system should enforce one.

## Current State

### Validator Functions (`sdcm/sct_config.py`)

**`str_or_list_or_eval`** (line 124):
- String input: `ast.literal_eval(value)` returns the eval result **unchecked** — can be `int`, `dict`, `tuple`, not just `list[str]`. Falls back to `[str(value)]` only if eval raises.
- List input: Runs `ast.literal_eval()` per item, can produce mixed-type lists.
- Type annotation claims `-> List[str] | None` but the implementation does not enforce this.

**`int_or_space_separated_ints`** (line 159):
- Returns bare `int` for single-value input (line 163-164).
- Returns `list[int]` for space-separated strings or list input.
- Consumers face `int | list[int]` ambiguity.

**`boolean_or_space_separated_booleans`** (line 188):
- Returns bare `bool` for single-value input (lines 201-202, 207-210, 227-228).
- Returns `list[bool]` for multi-value input.
- Single-item lists are unwrapped to scalar (lines 205-210).

**`dict_or_str`** (line 289):
- `ast.literal_eval(value)` returns any literal unchecked (line 294).
- `yaml.safe_load(value)` can return `str`, `int`, `list`, not just `dict` (line 301).
- Neither path verifies the result is actually a `dict`.

### Type Aliases

| Alias | Declared Type | Actual Output |
|-------|--------------|---------------|
| `StringOrList` (line 154) | `str \| list[str]` | `Any` (via `ast.literal_eval`) or `list[str]` |
| `IntOrList` (line 185) | `int \| list[int]` | `int` or `list[int]` |
| `BooleanOrList` (line 236) | `bool \| list[bool]` | `bool` or `list[bool]` |
| `DictOrStr` (line 311) | `dict \| str` | `Any` (via `ast.literal_eval`/`yaml.safe_load`) or `dict` |

### Consumer Patterns — Full isinstance Guard Inventory

**`n_db_nodes`** (IntOrList) — 17 guards:
- `sdcm/tester.py`: lines 910, 1626, 1628, 1736, 1738, 1815, 1817, 2536, 2538
- `sdcm/sct_config.py`: lines 2910, 3474, 3485, 3530, 4209
- `sdcm/utils/cluster_tools.py`: line 45
- `sdcm/mgmt/operations.py`: lines 549, 605
- `platform_migration_test.py`: line 282
- `sdcm/nemesis/__init__.py`: line 4449

**`n_loaders`** (IntOrList) — 5 guards:
- `sdcm/sct_config.py`: line 4151
- `sdcm/mgmt/operations.py`: lines 259, 261, 596, 598

**`nemesis_seed`** (IntOrList) — 3 guards:
- `sdcm/utils/operations_thread.py`: line 129 — `if isinstance(nemesis_seed, list): nemesis_seed = nemesis_seed[0]`
- `sdcm/tester.py`: lines 1546, 1548 — `if isinstance(nemesis_seeds, int):` wraps in list / `elif isinstance(nemesis_seeds, str):` splits

**`stress_cmd`** (StringOrList) — 6 guards:
- `sdcm/sct_config.py`: lines 3267, 3283, 4118
- `sdcm/utils/loader_utils.py`: line 356
- `longevity_test.py`: line 421
- `unit_tests/unit/test_gradual_grow_throughput.py`: line 35

**`prepare_write_cmd`** (StringOrList) — 4 guards:
- `performance_regression_test.py`: lines 225, 228
- `longevity_test.py`: line 460
- `performance_regression_alternator_test.py`: line 140

**`cluster_target_size`** (IntOrList) — 1 guard:
- `longevity_test.py`: line 173

**Assumes scalar for IntOrList** (~25 sites, no isinstance guard):
- `sdcm/cluster_k8s/eks.py` (lines 133, 741, 750), `sdcm/cluster_k8s/gke.py` (lines 109, 133, 498), `sdcm/cluster_k8s/mini_k8s.py` (line 551), `sdcm/cluster_cloud.py` (line 1227), `sdcm/provision/aws/capacity_reservation.py` (lines 42, 49, 54, 116), `sdcm/tester.py` (lines 1992, 2068, 2159, 2192, 2219, 2281, 2317, 2353, 2434, 4184), `sdcm/nemesis/__init__.py` (lines 2960-2962), `grow_cluster_test.py` (lines 33, 97, 156), `performance_regression_gradual_grow_throughput.py` (line 59), `performance_regression_alternator_test.py` (line 236), `performance_regression_row_level_repair_test.py` (lines 279-286).

**DictOrStr consumers** (all assume dict, ~20 sites): Every consumer calls `.get()`, `.items()`, or subscript access. None handle a raw string. No isinstance guards exist — a non-dict return would crash at all sites.

## Affected Config Options

**85 parameters total** across 4 type aliases.

### `IntOrList` — 12 params

- `n_db_nodes` (line 521)
- `n_test_oracle_db_nodes` (line 524)
- `n_loaders` (line 527)
- `n_monitor_nodes` (line 530)
- `cluster_target_size` (line 918)
- `n_db_zero_token_nodes` (line 1206)
- `nemesis_interval` (line 900)
- `nemesis_sequence_sleep_between_ops` (line 903)
- `nemesis_seed` (line 909)
- `nemesis_add_node_cnt` (line 912)
- `space_node_threshold` (line 921)
- `nemesis_multiply_factor` (line 2016)

### `StringOrList` — 55 params

- `config_files` (line 477)
- `scylla_apt_keys` (line 579)
- `assert_linux_distro_features` (line 624)
- `email_recipients` (line 785)
- `experimental_features` (line 797)
- `region_name` (line 978)
- `backup_bucket_location` (line 1049)
- `scylla_d_overrides_files` (line 1058)
- `gce_datacenter` (line 1065)
- `aws_dedicated_host_ids` (line 1125)
- `azure_region_name` (line 1259)
- `oci_region_name` (line 1289)
- `eks_admin_arn` (line 1342)
- `db_nodes_private_ip` (line 1491)
- `db_nodes_public_ip` (line 1494)
- `loaders_private_ip` (line 1497)
- `loaders_public_ip` (line 1500)
- `monitor_nodes_private_ip` (line 1503)
- `monitor_nodes_public_ip` (line 1506)
- `pre_create_keyspace` (line 1556)
- `post_prepare_cql_cmds` (line 1559)
- `stress_read_cmd` (line 1582)
- `prepare_verify_cmd` (line 1588)
- `stress_cmd_no_mv` (line 1660)
- `stress_cmd_no_mv_profile` (line 1663)
- `cs_user_profiles` (line 1666)
- `prepare_cs_user_profiles` (line 1669)
- `stress_cmd_mv` (line 1678)
- `prepare_stress_cmd` (line 1681)
- `stress_cmd_lwt_i` (line 1698)
- `stress_cmd_lwt_d` (line 1701)
- `stress_cmd_lwt_u` (line 1704)
- `stress_cmd_lwt_ine` (line 1707)
- `stress_cmd_lwt_uc` (line 1710)
- `stress_cmd_lwt_ue` (line 1713)
- `stress_cmd_lwt_de` (line 1717)
- `stress_cmd_lwt_dc` (line 1720)
- `stress_cmd_lwt_mixed` (line 1723)
- `stress_cmd_lwt_mixed_baseline` (line 1726)
- `stress_cmd_1` (line 1761)
- `stress_cmd_complex_prepare` (line 1764)
- `prepare_write_stress` (line 1767)
- `stress_cmd_read_10m` (line 1770)
- `stress_cmd_read_cl_one` (line 1773)
- `stress_cmd_read_60m` (line 1776)
- `stress_cmd_complex_verify_read` (line 1779)
- `stress_cmd_complex_verify_more` (line 1782)
- `write_stress_during_entire_test` (line 1785)
- `verify_data_after_entire_test` (line 1788)
- `stress_cmd_read_cl_quorum` (line 1791)
- `verify_stress_after_cluster_upgrade` (line 1794)
- `stress_cmd_complex_verify_delete` (line 1800)
- `stress_before_upgrade` (line 1937)
- `large_partition_stress_during_upgrade` (line 1941)
- `stress_during_entire_upgrade` (line 1945)
- `stress_after_cluster_upgrade` (line 1948)
- `jepsen_test_cmd` (line 1956)
- `max_events_severities` (line 1969)
- `nemesis_class_name` (line 885)
- `stress_cmd` (line 934)
- `stress_cmd_w` (line 1630)
- `stress_cmd_r` (line 1633)
- `stress_cmd_m` (line 1636)
- `stress_cmd_read_disk` (line 1639)
- `stress_cmd_cache_warmup` (line 1645)
- `prepare_write_cmd` (line 1651)
- `nemesis_selector` (line 2010)

### `BooleanOrList` — 3 params

- `nemesis_during_prepare` (line 906)
- `nemesis_filter_seeds` (line 929)
- `round_robin` (line 1547)

### `DictOrStr` / `DictOrStrOrPydantic` — 12 params

`DictOrStr`:
- `agent` (line 667)
- `alternator_test_table` (line 850)
- `teardown_validators` (line 1116)
- `skip_test_stages` (line 1200)
- `latency_decorator_error_thresholds` (line 1216)
- `mgmt_snapshots_preparer_params` (line 1625)
- `perf_gradual_threads` (line 1684)
- `perf_gradual_throttle_steps` (line 1688)
- `perf_gradual_step_duration` (line 1692)
- `stress_image` (line 2025)
- `latte_schema_parameters` (line 2032)

`DictOrStrOrPydantic`:
- `append_scylla_yaml` (line 877)

## Goals

1. **Canonical output types**: Each `BeforeValidator` returns exactly one type — `list[T]` for `*OrList` types, `dict` for `DictOrStr` — with no scalar/collection ambiguity.
2. **Zero per-parameter boilerplate**: Fix only the 4 validator functions and 4 type alias declarations; no per-field validators needed.
3. **Eliminate all ~37 `isinstance` guards** in consumer code that currently check and wrap scalars.
4. **Fix ~25 scalar-assuming call sites** for `IntOrList` params to work with `list[int]`.
5. **All existing unit tests pass** after the changes, plus new tests covering edge cases.

## Implementation Phases

Each phase covers one type end-to-end: validator fix, type alias update, all consumer fixes, and all isinstance guard removals. One PR per phase.

### Phase 1: Normalize `IntOrList` → always `list[int]`

**Importance**: Critical
**Description**: Fix `int_or_space_separated_ints` to always return `list[int]`. Update `IntOrList` type alias. Fix all ~25 scalar-assuming consumer sites. Remove all ~26 isinstance guards for `IntOrList` params.

**Deliverables**:

**Validator fix** (`sdcm/sct_config.py`):
- `int_or_space_separated_ints` (line 159): Change lines 163-164 from `return value` to `return [int(value)]`.
- `IntOrList` type alias (line 185): Change `int | list[int]` → `list[int]`.

**Scalar consumer fixes** (~25 sites — use `sum()` for totals, `[0]` for single-region):
- `sdcm/cluster_k8s/eks.py`: lines 133, 741, 750
- `sdcm/cluster_k8s/gke.py`: lines 109, 133, 498
- `sdcm/cluster_k8s/mini_k8s.py`: line 551
- `sdcm/cluster_cloud.py`: line 1227
- `sdcm/provision/aws/capacity_reservation.py`: lines 42, 49, 54, 116
- `sdcm/tester.py`: lines 1992, 2068, 2159, 2192, 2219, 2281, 2317, 2353, 2434, 4184
- `sdcm/nemesis/__init__.py`: lines 2960-2962
- `grow_cluster_test.py`: lines 33, 97, 156
- `performance_regression_gradual_grow_throughput.py`: line 59
- `performance_regression_alternator_test.py`: line 236
- `performance_regression_row_level_repair_test.py`: lines 279-286

**isinstance guard removals** (~26 guards — replace branching with direct list usage):

`n_db_nodes` — 17 guards:
- `sdcm/tester.py`: lines 910, 1626, 1628, 1736, 1738, 1815, 1817, 2536, 2538
- `sdcm/sct_config.py`: lines 2910, 3474, 3485, 3530, 4209
- `sdcm/utils/cluster_tools.py`: line 45
- `sdcm/mgmt/operations.py`: lines 549, 605
- `platform_migration_test.py`: line 282
- `sdcm/nemesis/__init__.py`: line 4449

`n_loaders` — 5 guards:
- `sdcm/sct_config.py`: line 4151
- `sdcm/mgmt/operations.py`: lines 259, 261, 596, 598

`nemesis_seed` — 3 guards:
- `sdcm/utils/operations_thread.py`: line 129
- `sdcm/tester.py`: lines 1546, 1548

`cluster_target_size` — 1 guard:
- `longevity_test.py`: line 173

**Unit tests for `int_or_space_separated_ints`**:
- `3` → `[3]`; `"5"` → `[5]`; `"1 2 3"` → `[1, 2, 3]`
- `[1, 2]` → `[1, 2]`; `None` → `None`
- `"abc"` → raises `ValueError`

**Definition of Done**:
- [ ] `int_or_space_separated_ints` always returns `list[int] | None`
- [ ] `IntOrList` declared as `list[int]`
- [ ] No consumer performs scalar arithmetic/comparison on IntOrList params
- [ ] No `isinstance` guards remain for `n_db_nodes`, `n_loaders`, `nemesis_seed`, `n_monitor_nodes`, `n_test_oracle_db_nodes`, `cluster_target_size`, `n_db_zero_token_nodes`
- [ ] Unit tests for `int_or_space_separated_ints` edge cases added and passing
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 2: Normalize `StringOrList` → always `list[str]`

**Importance**: Critical
**Description**: Fix `str_or_list_or_eval` to always return `list[str]`. Update `StringOrList` type alias. Remove all ~10 isinstance guards for `StringOrList` params.

**Dependencies**: None (independent of Phase 1)

**Deliverables**:

**Validator fix** (`sdcm/sct_config.py`):
- `str_or_list_or_eval` (line 124): After `ast.literal_eval(value)` at line 131, check result is a `list`; if not, wrap in `[result]`.
- `StringOrList` type alias (line 154): Change `str | list[str]` → `list[str]`.

**isinstance guard removals** (~10 guards):

`stress_cmd` — 6 guards:
- `sdcm/sct_config.py`: lines 3267, 3283, 4118
- `sdcm/utils/loader_utils.py`: line 356
- `longevity_test.py`: line 421
- `unit_tests/unit/test_gradual_grow_throughput.py`: line 35

`prepare_write_cmd` — 4 guards:
- `performance_regression_test.py`: lines 225, 228
- `longevity_test.py`: line 460
- `performance_regression_alternator_test.py`: line 140

**Unit tests for `str_or_list_or_eval`**:
- `"3"` → `["3"]` (not `3`); `"{'a': 1}"` → `[{'a': 1}]` (not `{'a': 1}`)
- `"[1,2]"` → `[1, 2]`; `"['cmd1', 'cmd2']"` → `['cmd1', 'cmd2']`
- `"hello"` → `["hello"]`; `""` → `[]`; `None` → `None`
- `["a", "b"]` → `["a", "b"]`

**Definition of Done**:
- [ ] `str_or_list_or_eval` always returns `list[str] | None`; `ast.literal_eval` result is wrapped if not a list
- [ ] `StringOrList` declared as `list[str]`
- [ ] No `isinstance` guards remain for `stress_cmd`, `prepare_write_cmd`, or other `StringOrList` params
- [ ] Unit tests for `str_or_list_or_eval` edge cases added and passing
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 3: Normalize `BooleanOrList` → always `list[bool]`

**Importance**: Important
**Description**: Fix `boolean_or_space_separated_booleans` to always return `list[bool]`. Update `BooleanOrList` type alias. No downstream consumer fixes are expected for `BooleanOrList` params.

**Dependencies**: None (independent of Phases 1-2)

**Deliverables**:

**Validator fix** (`sdcm/sct_config.py`):
- `boolean_or_space_separated_booleans` (line 188):
  - Lines 201-202: `return value` → `return [value]`
  - Lines 205-210: Remove single-item list unwrap; return `[bool_val]`
  - Lines 227-228: Return `[bool(strtobool(values[0]))]` instead of bare bool
- `BooleanOrList` type alias (line 236): Change `bool | list[bool]` → `list[bool]`.

**Consumer verification**: All `BooleanOrList` params (`nemesis_during_prepare`, `nemesis_filter_seeds`, `round_robin`) are used directly. Verify list access patterns work correctly after the type change.

**Unit tests for `boolean_or_space_separated_booleans`**:
- `True` → `[True]`; `False` → `[False]`
- `"true"` → `[True]`; `"false"` → `[False]`
- `"true false"` → `[True, False]`
- `[True]` → `[True]` (not unwrapped to bare `True`)
- `[True, False]` → `[True, False]`
- `None` → `None`

**Definition of Done**:
- [ ] `boolean_or_space_separated_booleans` always returns `list[bool] | None`
- [ ] `BooleanOrList` declared as `list[bool]`
- [ ] Unit tests for `boolean_or_space_separated_booleans` edge cases added and passing
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 4: Normalize `DictOrStr` → always `dict`

**Importance**: Important
**Description**: Fix `dict_or_str` to reject non-dict results from `ast.literal_eval` and `yaml.safe_load`. Update `DictOrStr` type alias. No consumer changes needed — all ~20 sites already assume dict.

**Dependencies**: None (independent of Phases 1-3)

**Deliverables**:

**Validator fix** (`sdcm/sct_config.py`):
- `dict_or_str` (line 289):
  - Line 294: After `ast.literal_eval(value)`, check `isinstance(result, dict)`; if not, fall through to `yaml.safe_load` instead of returning.
  - Line 301: After `yaml.safe_load(value)`, check `isinstance(result, dict)`; if not, raise `ValueError`.
- `DictOrStr` type alias (line 311): Change `dict | str` → `dict`.

**Unit tests for `dict_or_str`**:
- `"{'a': 1}"` → `{'a': 1}`
- `"[1, 2]"` → raises `ValueError` (not a dict)
- `"3"` → raises `ValueError` (not a dict)
- `"plain string"` → raises `ValueError`
- `{"key": "val"}` → `{"key": "val"}` (passthrough)
- `None` → `None`

**Definition of Done**:
- [ ] `dict_or_str` always returns `dict | None`; non-dict eval/yaml results are rejected
- [ ] `DictOrStr` declared as `dict`
- [ ] Unit tests for `dict_or_str` edge cases added and passing
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 5: Documentation Update

**Importance**: Nice-to-have
**Description**: Update `docs/sct-configuration.md` (if applicable) to document that `*OrList` params always produce lists and `DictOrStr` always produces dicts. Add inline docstrings to the type aliases.

**Dependencies**: Phases 1-4

**Definition of Done**:
- [ ] Type alias docstrings explain canonical output type
- [ ] Configuration docs updated if they reference these types

## Testing Requirements

### Unit Tests
- Each phase includes its own validator edge-case tests (see per-phase deliverables).
- Existing `test_config.py` must continue passing after each phase.
- Full unit test suite must pass after each phase: `uv run sct.py unit-tests`.

### Integration Tests
- Run a Docker backend test to verify config loading end-to-end:
  `uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml`

### Manual Testing
- Verify multi-region config (where `n_db_nodes` is legitimately a multi-element list like `"3 3 3"`) still parses correctly.

## Success Criteria

All Definition of Done items across phases are met. Additionally:

1. Every `*OrList` validator provably returns `list[T] | None` — no code path returns a scalar.
2. `dict_or_str` provably returns `dict | None` — no code path returns a non-dict.
3. All ~37 isinstance guards removed across the codebase.
4. No regressions in the full unit test suite.

## Risk Mitigation

### Risk: Downstream Code Assumes Scalar IntOrList Values
**Likelihood**: High (confirmed ~25 sites)
**Impact**: `TypeError` at runtime for any missed consumer site.
**Mitigation**: Phase 1 covers all IntOrList changes in one PR — validator, consumers, and guards together — so nothing breaks mid-migration. Use `grep -rn` to find all accesses to each param name. Run full unit test suite to catch regressions.

### Risk: MultitenantValue Wrapping Interaction
**Likelihood**: N/A — `MultitenantValue` has been removed from the codebase.
**Impact**: None.
**Mitigation**: No action required.

### Risk: Config File Backward Compatibility
**Likelihood**: Low
**Impact**: Existing YAML config files that specify scalar values (e.g., `n_db_nodes: 3`) might not load.
**Mitigation**: The `BeforeValidator` still accepts scalar input — it just wraps the output. `int_or_space_separated_ints(3)` will return `[3]`. No config file changes needed.

### Risk: StringOrList ast.literal_eval Behavior Change
**Likelihood**: Medium
**Impact**: `str_or_list_or_eval("['cmd1', 'cmd2']")` currently returns `['cmd1', 'cmd2']` (a list). After the fix, it still returns `['cmd1', 'cmd2']` because the result of `ast.literal_eval` is already a list. But `str_or_list_or_eval("3")` changes from `3` to `["3"]`.
**Mitigation**: Phase 2 removes all isinstance guards that handled this ambiguity. Unit tests in Phase 2 explicitly verify these edge cases.

## Related Plans

- [sct-config-validation-and-lazy-images.md](../sct-config-validation-and-lazy-images.md) — Broader config validation work; this plan is a focused subset.
- [Typed Config Access Migration](https://github.com/scylladb/scylla-cluster-tests/pull/13878) — Type safety improvements; this plan makes types more predictable for typed access.

## PR History

| Phase | PR | Status |
|-------|-----|--------|
| Phase 1: IntOrList | — | Not started |
| Phase 2: StringOrList | — | Not started |
| Phase 3: BooleanOrList | — | Not started |
| Phase 4: DictOrStr | — | Not started |
| Phase 5: Docs | — | Not started |
