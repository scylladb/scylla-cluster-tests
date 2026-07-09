# Mini-Plan: Strict Jinja Stress Command Templating

**Date:** 2026-07-09
**Estimated LOC:** 250
**Related PR:** none

## Problem

Longevity test configs hardcode row counts in stress commands, so dataset size must be recalculated manually whenever disk size, instance type, or expected compression changes. Add runtime-only strict Jinja rendering for longevity stress commands so configs can calculate row counts from available `/var/lib/scylla` capacity through an `effective_disk_size_bytes` template variable.

## Approach

- Add `effective_compression_ratio` to SCT config with default `1.0` and validation `0 < value <= 1`. Treat it as `on_disk_bytes / logical_uncompressed_bytes`, so `0.68` means generated logical data is expected to occupy 68% of its uncompressed size on disk.
- Add `effective_compression_ratio: 1.0` to defaults and regenerate config docs.
- Do not render or validate Jinja templates during config load, `verify_configuration()`, or `check_required_files()`. Existing config validation should keep seeing the original literal strings.
- Render only at stress execution time in `LoaderUtilsMixin`, immediately before `_run_all_stress_cmds()` inspects command prefixes or dispatches to `run_stress_thread()`.
- Use a strict Jinja environment with `StrictUndefined`; fail fast before starting stress if a command references an unknown variable or has invalid syntax.
- Render only commands containing Jinja delimiters: `{{`, `{%`, or `{#`. Non-templated commands must remain byte-for-byte unchanged.
- Add a cached property for `effective_disk_size_bytes` because capacity is expected to be stable for this purpose.
- Calculate raw available capacity from DB nodes with `df -B1` against `/var/lib/scylla`, use the average available bytes across nodes because Scylla load-balances data across nodes, then export `floor(average_available_bytes / effective_compression_ratio)`.
- Keep RF and node count outside the exported variable. Test configs should account for RF and node count directly in the Jinja formula.
- Document a compact example that hardcodes formula constants in the command:

```yaml
effective_compression_ratio: 0.68

prepare_write_cmd:
  - >-
    {% set rows = ((effective_disk_size_bytes * 3) // 3 // (200 * 5)) | int %}
    cassandra-stress write cl=QUORUM n={{ rows }}
    -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)'
    -mode cql3 native
    -rate threads=150
    -col 'size=FIXED(200) n=FIXED(5)'
    -pop seq=1..{{ rows }}
```

- Existing delimiter scan found no Jinja delimiters in relevant SCT test/config YAML directories: `test-cases/`, `configurations/`, `defaults/`, and `unit_tests/test_configs/`. Existing repository YAML delimiter usage is unrelated GitHub Actions `${{ ... }}` and Helm/minio templates under `sdcm/k8s_configs/minio/templates/`, which are unaffected because only stress command values are rendered.

## Files to Modify

- `sdcm/sct_config.py` -- Add `effective_compression_ratio` field, type constraints, and config help text.
- `defaults/test_default.yaml` -- Add default `effective_compression_ratio: 1.0`.
- `sdcm/utils/loader_utils.py` -- Add cached effective disk capacity calculation and strict runtime Jinja rendering before stress command dispatch.
- `docs/configuration_options.md` -- Regenerate config documentation after adding the new option.
- `docs/sct-configuration.md` -- Add a short usage note and example for templated longevity stress commands.
- `unit_tests/unit/test_sct_config_validators.py` -- Add validation tests for accepted and rejected `effective_compression_ratio` values.
- `unit_tests/unit/test_longevity.py` -- Add runtime rendering tests for successful formulas, strict undefined failures, syntax failures, unchanged literal commands, and cached effective disk size behavior.

## Verification

- [ ] `effective_compression_ratio=1.0` exports average raw available bytes unchanged.
- [ ] `effective_compression_ratio=0.68` exports `floor(average_available_bytes / 0.68)`.
- [ ] Effective disk size calculation averages available capacity across DB nodes instead of using the lowest node value.
- [ ] Ratios `0`, negative values, and values above `1` fail config validation.
- [ ] `sct.py conf` succeeds for a config containing Jinja in stress commands without requiring DB nodes or disk capacity.
- [ ] A command without Jinja delimiters is passed to stress dispatch unchanged.
- [ ] A command with `{{ effective_disk_size_bytes }}` renders to a concrete command before stress dispatch.
- [ ] A command with an unknown template variable fails before any stress thread starts.
- [ ] Existing YAML delimiter usage outside stress command values remains unaffected.
- [ ] Unit tests pass: `uv run sct.py unit-tests -t test_sct_config_validators.py`.
- [ ] Unit tests pass: `uv run sct.py unit-tests -t test_longevity.py`.
- [ ] `uv run sct.py pre-commit` passes.
