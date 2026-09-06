# SCT Config Package Split (SCT-525, Phases 1-3)

## Problem

`sdcm/sct_config.py` had grown to ~5,400 lines: 537 Pydantic field definitions, eight
class-level lookup tables, a ~560-line `__init__`, and ~2,400 lines of loading, validation and
doc-generation methods. It is past the point where a human or an LLM can review or extend it,
which blocks the domain-mixin split described in
[the follow-up refactoring plan](../sct-config-followup-refactoring.md).

This is Phases 1-3 of that plan: turn the module into a package, lift out the parts that are not
the configuration model itself, and split the ~2,000-line field block into domain mixins. Phase 4
(moving the `_validate_*` methods into their mixins) and Phase 5 (the nested sub-model PoC) follow
separately.

## Approach

### Package layout (Phases 1-2)

`sdcm/sct_config.py` becomes `sdcm/sct_config/`:

| Module | Holds |
|---|---|
| `config.py` | `SCTConfiguration` assembler: loading, validation, doc generation |
| `types.py` | `String`/`StringOrList`/`IntOrList`/… aliases, their `BeforeValidator` converters, `IgnoredType`, `InputType`, `SctField`, `AdaptiveTimeoutMultipliers` |
| `helpers.py` | appendable-option merging, env sub-key parsing, `count_regions`, docker-image defaults cache, `simulated_racks_enabled` |
| `defaults.py` | `available_backends`, `AWS_SUPPORTED_REGIONS`, `BACKEND_IMAGE_FIELD` and the eight requirement tables |
| `mixins/` | the field definitions, one module per domain (Phase 3) |
| `__init__.py` | public re-exports |

### Domain mixins (Phase 3)

All 523 user-facing options move into 25 mixins under `sdcm/sct_config/mixins/`, each a
`BaseModel` that `SCTConfiguration` inherits from. Fields stay **flat** —
`config.nemesis_class_name` and every YAML key are unchanged; only the source is split.

The boundaries are not invented: `sct_config.py` already carried section comments
(`# AWS config options`, `# Nemesis config options`, `# LongevityTest`, …) marking the author's own
grouping. Those become the mixins. The two regions with no section comments — the ~90-field leading
block and the ~60-field tail — are assigned by name, and the split is generated from the AST with an
assertion that all 539 entries are accounted for (523 options + 16 internal).

Grouped in `CONFIG_GROUPS` browse order — cross-cutting, then per backend, then per test type:

- **Cross-cutting**: `common` (31), `scylla` (43), `security` (12), `nemesis` (12), `stress` (16),
  `monitoring` (16), `manager` (22), `vector_store` (4)
- **Backends**: `aws` (65), `gce` (10), `azure` (13), `oci` (10), `kubernetes` (44), `docker` (11),
  `baremetal` (7), `xcloud` (12), `minicloud` (51)
- **Test types**: `longevity` (23), `performance` (47), `upgrade` (25), `grow_cluster` (3),
  `refresh` (6), `jepsen` (4), `emr` (13), `spark_migrator` (23)

What stays on `SCTConfiguration`: the 16 internal entries (runtime state like `regions_data` and
`is_enterprise`, plus the eight `IgnoredType` lookup tables) and all the cross-domain loading,
validation and doc-generation logic.

### Grouped documentation (Phase 3)

`docs/configuration_options.md` was a flat list of 523 options. It now has a `# <Group>` section per
mixin in `CONFIG_GROUPS` order, so it can be browsed by domain. Both `dump_help_config_markdown`
and `dump_help_config_yaml` iterate a new `_fields_by_group()` helper.

`_fields_by_group()` reads each mixin's `model_fields` rather than
`__dict__["__annotations__"]`: Python 3.14 defers annotation evaluation (PEP 649), so that key is
absent until something forces it — the first attempt silently put all 523 options in "Other".
Mixins inherit only from `BaseModel`, so `model_fields` is exactly each mixin's own set. Any field
not claimed by a mixin lands in a trailing "Other" section, so a field can never silently vanish
from the docs.

Zero behaviour change: no YAML changes, no consumer-code changes beyond import paths. Field
*order* on the model does change (it now follows `CONFIG_GROUPS`), which is what regroups the
generated docs; the field *set* is identical and nothing depends on order.

### Deliberate deviations from a pure move

1. **`helpers.py` must not import `config.py`.** `is_config_option_appendable()` read
   `SCTConfiguration.model_fields`. It and `merge_dicts_append_strings()` now take the model
   class as a parameter, so the package has no import cycle and no function-level imports.
2. **The instance-catalog path was derived from `__file__`.** One directory deeper it resolves to
   `sdcm/data/instance_catalog`, and `_resolve_instance_sizes` swallows the resulting
   `FileNotFoundError` with a warning — constraint-based sizing would have silently stopped
   resolving on every real run. It now uses `sct_abs_path()`, guarded by a new regression test.
3. **Loggers keep the literal name `"sdcm.sct_config"`** instead of `__name__`, so log lines in
   collected logs and `caplog` targets in tests do not move.

The class-level tables stay class fields, assigned from `defaults.py` constants. They cannot
become direct module references: Pydantic deep-copies mutable field defaults per instance, and
`_check_backend_defaults` mutates them — a module-global would accumulate across every
`SCTConfiguration()` in a process.

`__init__.py` re-exports **only** the names the repo actually imports. Names `config.py` merely
imports for its own use (`KeyStore`, `convert_name_to_ami_if_needed`, `get_branched_ami`, …) are
left out on purpose, so a stale `mock.patch("sdcm.sct_config.<name>")` raises `AttributeError`
instead of silently patching an attribute no call site reads — which would let a unit test fall
through to a real cloud API call.

## Files to Modify

- `sdcm/sct_config.py` → `sdcm/sct_config/config.py` (`git mv`, its own commit), plus `types.py`,
  `helpers.py`, `defaults.py`, `__init__.py` and `mixins/` (25 modules + `__init__.py`).
- `sdcm/utils/lint/validator.py` — `_CLOUD_API_PATCHES` targets become `sdcm.sct_config.config.*`,
  **including `_check_file_exists`**, even though it now lives in `types.py`: the target must name
  the module that *calls* the name, and `config.py` does `from .types import _check_file_exists`,
  which binds it in `config`'s own namespace. `unit_tests/unit/test_lint_patch_targets.py` enforces
  that rule statically.
- 49 string patch targets across 9 `unit_tests/` files. In `unit_tests/unit/test_config.py` and
  `unit_tests/unit/test_instance_sizing_config.py`, also change `from sdcm import sct_config` →
  `from sdcm.sct_config import config as sct_config`; that one line fixes their 16
  `patch.object`/`monkeypatch.setattr` statements, including the `_KEYSTORE_ENV_EXPORTED` rebind
  that a re-export could not have saved.
- `sdcm/cluster_cassandra.py` — import `TestConfig` from `sdcm.test_config`, its real home, so the
  incidental re-export can be dropped from a deliberately narrow `__init__.py`.
- `.pre-commit-config.yaml` (`update-conf-docs` `files:` regex) and `.coderabbit.yaml` (`path:`).
- Prose paths in `docs/sct-configuration.md`, `AGENTS.md`, `.github/copilot-instructions.md`,
  `.github/pull_request_template.md`, and the stale patch target taught by
  `skills/writing-unit-tests/references/common-pitfalls.md`.
- `docs/configuration_options.md` — regenerated, now grouped.

## Verification

- `SCTConfiguration.model_fields` — 537 entries, field **set** byte-identical to master; order now
  follows `CONFIG_GROUPS` (intended, and what regroups the docs).
- `docs/configuration_options.md` — 523 options across 25 group sections, nothing in "Other".
- `sct.py conf -b docker test-cases/longevity/longevity-10gb-3h.yaml` — dump parsed as YAML and
  compared key-by-key against the same command on master: 244 keys, same values, no additions or
  removals (only run-specific `test_id`/`user_prefix` differ).
- `test_catalog_directory_resolves_to_a_real_directory` fails on the old `__file__` form; every
  other sizing test patches `from_directory`, so nothing else would catch it.
- `test_lint_patch_targets.py` fails on the pre-fix `types._check_file_exists` target and names the
  module to patch instead.
- `sct.py lint-pipelines` **run under a fake `$HOME`**, so the credential files the linter patches
  around are genuinely absent — which is what CI sees. A run with the real `$HOME` passes for the
  wrong reason: it was 1136/1136 green while the patch was dead. Under the fake `$HOME` the branch
  matches pristine master exactly (2 pre-existing Azure/OCI failures that need real cloud creds).
- `uv run sct.py unit-tests` and `uv run sct.py pre-commit`.

## Known remaining work (not this PR)

- `config.py` is still ~2,700 lines: the ~560-line `__init__`, ~25 `_validate_*` methods, and the
  doc generators. Phase 4 moves the domain-specific validators into their mixins, which is where
  the next big reduction comes from.
- CodeRabbit flags docstring coverage at 53% on functions touched by the diff. Almost all of those
  are pre-existing `config.py` methods that this PR only relocates; documenting them is a separate
  cleanup, not something to bundle into a move. Functions actually authored or changed here
  (`types.py`, `helpers.py`, `_fields_by_group`) are documented.
