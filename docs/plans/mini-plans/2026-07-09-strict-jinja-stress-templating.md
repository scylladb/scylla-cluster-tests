# Mini-Plan: Strict Jinja Stress Command Templating

**Date:** 2026-07-09
**Estimated LOC:** 250
**Related PR:** none

## Problem

Longevity test configs hardcode row counts in stress commands, so dataset size must be recalculated manually whenever disk size, instance type, or expected compression changes. The implementation now supports runtime-only strict Jinja rendering for stress commands so configs can calculate row counts from available `/var/lib/scylla` capacity through an `effective_disk_size_bytes` template variable, while keeping config loading and validation unaware of Jinja syntax.

## Approach

- Added `effective_compression_ratio` to SCT config with default `1.0` and validation `0 < value <= 1`. It is defined as `on_disk_bytes / logical_uncompressed_bytes`, so `0.68` means the logical dataset is expected to occupy about 68% of its uncompressed size on disk.
- Added `effective_compression_ratio: 1.0` to defaults and updated generated config docs.
- Added `stress_template_context` to SCT config so tests can define shared derived Jinja variables once and reuse them across multiple stress commands.
- Kept config load, `verify_configuration()`, and `check_required_files()` free of Jinja rendering. Existing validation still sees the original literal stress command strings.
- Implemented runtime rendering in `LoaderUtilsMixin` by rendering every stress command immediately before `_run_all_stress_cmds()` performs command-specific handling and dispatches to `run_stress_thread()`.
- Implemented a strict Jinja environment with `StrictUndefined` in `LoaderUtilsMixin.jinja_env`, plus `MixinLazyContext` so template variables are resolved lazily from the mixin owner object only when referenced.
- Added a `stress_template_whitelist` hook to `LoaderUtilsMixin` and overrode it in `LongevityTest`, so all longevity-derived tests automatically restrict owner-backed template variables to `effective_disk_size_bytes` and `db_node_count_per_dc`.
- Implemented ordered `stress_template_context` evaluation in `LoaderUtilsMixin`, with YAML scalar coercion and conflict detection for context keys that shadow built-in owner-backed variables.
- Failed fast on unknown variables by converting `UndefinedError` into a `RuntimeError` that includes the original command. Syntax errors still propagate as Jinja `TemplateSyntaxError` before any stress thread starts.
- Did not add a delimiter pre-scan optimization. Instead, all stress commands go through `render_stress_cmd()`, and literal commands remain unchanged because rendering a template with no Jinja syntax returns the original string.
- Kept `effective_disk_size_bytes` as a cached property on `LongevityTest`, not on `LoaderUtilsMixin`. The exported value is still `floor(average_available_bytes / effective_compression_ratio)`.
- Calculated raw available capacity from DB nodes with `df -B1 /var/lib/scylla`, using the average available bytes across nodes because Scylla load-balances data across nodes.
- Kept RF outside the exported variables. Test configs can use `db_node_count_per_dc` directly in the Jinja formula when all DCs are expected to have the same DB node count.
- The implementation also normalizes rendered multi-line YAML block scalars by collapsing whitespace to a single line before dispatch.
- Added user-facing documentation in `docs/loader_usage.md` with a compact example, runtime behavior notes, the longevity-only whitelist rule, and the shared `stress_template_context` mechanism.

- Example formula supported by the implementation:

```yaml
effective_compression_ratio: 0.68

stress_template_context:
  total_rows: "{{ ((effective_disk_size_bytes * db_node_count_per_dc) // 3 // (200 * 5)) | int }}"
  rows_per_cmd: "{{ total_rows // 4 }}"

prepare_write_cmd:
  - >-
    cassandra-stress write cl=QUORUM n={{ rows_per_cmd }}
    -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)'
    -mode cql3 native
    -rate threads=150
    -col 'size=FIXED(200) n=FIXED(5)'
    -pop seq=1..{{ rows_per_cmd }}
```

- Existing repository YAML delimiter usage outside stress command values remains unaffected because rendering happens only when stress commands are executed.

## Files to Modify

- `sdcm/sct_config.py` -- Added `effective_compression_ratio` field, type constraints, and config help text.
- `sdcm/sct_config.py` -- Added `stress_template_context` field for shared runtime stress command variables.
- `defaults/test_default.yaml` -- Added default `effective_compression_ratio: 1.0`.
- `defaults/test_default.yaml` -- Added default `stress_template_context: {}`.
- `longevity_test.py` -- Added cached `effective_disk_size_bytes`, scalar `db_node_count_per_dc`, and longevity template whitelist entries on `LongevityTest`.
- `sdcm/utils/loader_utils.py` -- Added `MixinLazyContext`, strict Jinja environment setup, shared context evaluation, runtime stress command rendering before stress dispatch, and the whitelist hook used by longevity tests.
- `docs/configuration_options.md` -- Updated generated config documentation for the new option.
- `docs/loader_usage.md` -- Added feature documentation and an example for templated longevity stress commands.
- `unit_tests/unit/test_sct_config_validators.py` -- Added validation tests for accepted and rejected `effective_compression_ratio` values.
- `unit_tests/unit/test_longevity.py` -- Added tests for `effective_disk_size_bytes` calculation, scalar `db_node_count_per_dc` behavior, longevity whitelist enforcement, and context usage from whitelisted built-ins.
- `unit_tests/unit/test_render_stress_cmd.py` -- Added renderer tests for literal commands, successful templates, shared context evaluation, unknown variables, syntax errors, multiline command normalization, and unrestricted generic mixin behavior.
- `unit_tests/unit/test_mixin_lazy_context.py` -- Added focused tests for lazy owner attribute resolution and whitelist behavior in the custom Jinja context.
- `test-cases/features/elasticity/elasticity-90-percent-with-nemesis.yaml` -- Migrated the Tier 1 90%% elasticity testcase to shared `stress_template_context` variables.

## Verification

- [x] `effective_compression_ratio=1.0` exports average raw available bytes unchanged.
- [x] `effective_compression_ratio=0.68` exports `floor(average_available_bytes / 0.68)`.
- [x] Effective disk size calculation averages available capacity across DB nodes instead of using the lowest node value.
- [x] Ratios `0`, negative values, and values above `1` fail config validation.
- [x] A literal command passed through `render_stress_cmd()` is returned unchanged.
- [x] A command with `{{ effective_disk_size_bytes }}` renders to a concrete command string before dispatch.
- [x] Shared `stress_template_context` entries can reference built-in variables and earlier context entries.
- [x] `stress_template_context` values are available to all stress commands without repeating the sizing formula in each command.
- [x] A command with an unknown template variable fails before any stress thread starts.
- [x] Invalid Jinja syntax fails before any stress thread starts.
- [x] Multi-line YAML block scalar output is normalized to a single-line stress command.
- [x] Lazy Jinja owner resolution does not evaluate expensive properties unless the template references them.
- [x] Longevity-derived tests allow `effective_disk_size_bytes` and `db_node_count_per_dc`, and reject other owner-backed template variables.
- [x] Context keys that shadow built-in owner-backed variables are rejected.
- [x] Non-longevity `LoaderUtilsMixin` users remain unrestricted unless they opt into a whitelist.
- [x] Existing YAML delimiter usage outside stress command values remains unaffected because only runtime stress command rendering uses this environment.
- [ ] `sct.py conf` succeeds for a config containing Jinja in stress commands without requiring DB nodes or disk capacity. Needs explicit end-to-end verification.
- [ ] Unit tests pass: `uv run sct.py unit-tests -t test_sct_config_validators.py`.
- [ ] Unit tests pass: `uv run sct.py unit-tests -t test_longevity.py`.
- [ ] Unit tests pass: `uv run sct.py unit-tests -t test_render_stress_cmd.py`.
- [ ] Unit tests pass: `uv run sct.py unit-tests -t test_mixin_lazy_context.py`.
- [ ] `uv run sct.py pre-commit` passes.
