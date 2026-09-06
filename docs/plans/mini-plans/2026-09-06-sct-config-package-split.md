# SCT Config Package Split (SCT-525, Phases 1+2)

## Problem

`sdcm/sct_config.py` had grown to ~5,400 lines: 537 Pydantic field definitions, eight
class-level lookup tables, a ~560-line `__init__`, and ~2,400 lines of loading, validation and
doc-generation methods. It is past the point where a human or an LLM can review or extend it,
which blocks the domain-mixin split described in
[the follow-up refactoring plan](../sct-config-followup-refactoring.md).

This is Phases 1+2 of that plan: turn the module into a package and lift out the parts that are
not the configuration model itself. Phase 3 (16 domain mixins) follows in separate PRs.

## Approach

`sdcm/sct_config.py` becomes `sdcm/sct_config/`:

| Module | Holds |
|---|---|
| `config.py` | `SCTConfiguration` and `init_and_verify_sct_config` (unchanged behaviour) |
| `types.py` | `String`/`StringOrList`/`IntOrList`/… aliases, their `BeforeValidator` converters, `IgnoredType`, `InputType`, `SctField`, `AdaptiveTimeoutMultipliers` |
| `helpers.py` | appendable-option merging, env sub-key parsing, `count_regions`, docker-image defaults cache, `simulated_racks_enabled` |
| `defaults.py` | `available_backends`, `AWS_SUPPORTED_REGIONS`, `BACKEND_IMAGE_FIELD` and the eight requirement tables |
| `__init__.py` | public re-exports |

Zero behaviour change: no YAML changes, no consumer-code changes beyond import paths.

Three deliberate deviations from a pure move:

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

- `sdcm/sct_config.py` → `sdcm/sct_config/config.py` (`git mv`, its own commit) plus the four new
  modules.
- `sdcm/utils/lint/validator.py` — `_CLOUD_API_PATCHES` targets become `sdcm.sct_config.config.*`,
  **including `_check_file_exists`**, even though it now lives in `types.py`: the target must name
  the module that *calls* the name, and `config.py` does `from .types import _check_file_exists`,
  which binds it in `config`'s own namespace. `unit_tests/unit/test_lint_patch_targets.py` now
  enforces that rule statically.
- 49 string patch targets across 9 `unit_tests/` files. In `unit_tests/unit/test_config.py` and
  `unit_tests/unit/test_instance_sizing_config.py`, also change
  `from sdcm import sct_config` → `from sdcm.sct_config import config as sct_config`; that one
  line fixes their 16 `patch.object`/`monkeypatch.setattr` statements at once, including the
  `_KEYSTORE_ENV_EXPORTED` rebind that a re-export could not have saved.
- `.pre-commit-config.yaml` (`update-conf-docs` `files:` regex) and `.coderabbit.yaml` (`path:`).
- Prose paths in `docs/sct-configuration.md`, `AGENTS.md`, `.github/copilot-instructions.md`,
  `.github/pull_request_template.md`, and the stale patch target taught by
  `skills/writing-unit-tests/references/common-pitfalls.md`.

## Verification

- `SCTConfiguration.model_fields` — 537 fields, order byte-identical to `upstream/master`.
- `docs/configuration_options.md` regenerates with **no diff**.
- `uv run sct.py conf -b docker test-cases/longevity/longevity-10gb-3h.yaml` produces a
  byte-identical dump to the same command on `upstream/master` (compared via a detached worktree,
  normalising the repo root).
- New `test_catalog_directory_resolves_to_a_real_directory` fails on the old `__file__` form and
  passes on `sct_abs_path()`; every other sizing test patches `from_directory`, so nothing else
  would have caught it.
- A stale `mock.patch("sdcm.sct_config.convert_name_to_ami_if_needed")` raises `AttributeError`.
- `uv run sct.py unit-tests`, `uv run sct.py pre-commit`, `uv run sct.py lint-pipelines`
  (the last exercises `_CLOUD_API_PATCHES`, and runs in the PR pipeline).
