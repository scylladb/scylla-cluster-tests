# Config validation on the mixins

**Jira:** SCT-525

## Problem

`SCTConfiguration` is assembled from 29 domain mixins in `sdcm/sct_config/mixins/`.
Each mixin owns the options of one domain. The rules that check those options do
not live on the mixins. They live in two places in `sdcm/sct_config/config.py`:

- Inline cross-field checks at the end of `SCTConfiguration.__init__`.
- About 25 `_validate_*` and `_verify_*` methods behind `verify_configuration`.

This causes these problems:

- A rule is not discoverable from its option. An author who adds an option to a
  mixin has no reason to look in `config.py` for the rules that constrain it.
  New rules go into `__init__`, because the existing ones are there.
- `__init__` is more than 500 lines. It mixes the load, the cloud image
  resolution, and the validation, and it carries lint suppressions for its
  branch, local, and statement counts.
- No rule says when a check must run. The zero-token node check and the gradual
  throughput check run both from `__init__` and from `verify_configuration`.
- Checks written as bare `assert` statements are removed under `python -O`.
- The tables in `sdcm/sct_config/defaults.py` that drive the per-backend checks
  are far from the options they name. `verify_configuration` appends to one of
  them in place on each call.

The first attempt to fix this, PR #14124, put validators on the central class
behind a flag that was off by default. It failed. The problems that it showed
are requirements for this change.

## Who it affects

- Authors who add or change configuration options in any mixin.
- Reviewers, who cannot check a new option against its rules in one place.
- Every caller that builds a configuration: `sct.py`, the unit tests, and the
  pipeline linter in `sdcm/utils/lint/validator.py`. The linter builds every
  test case and calls `verify_configuration` on it. It patches several
  `SCTConfiguration` methods by name, so a moved method changes its setup.

## Evidence

The current state of master:

- No mixin declares a validator. The only model validator in the package is on
  the `AdaptiveTimeoutMultipliers` type in `sdcm/sct_config/types.py`.
- `SCTConfiguration` sets `validate_assignment=True`. Each field assignment
  during the load runs every model validator of the class.
- `sdcm/sct_config/config.py` holds 20 `assert` statements, about nine distinct
  checks. `sdcm/sct_config/helpers.py` holds two more, for the "is not
  appendable" check. `verify_configuration_urls_validity` has asserts too.
- The class docstring of `SCTConfiguration` names this work as SCT-525 phase 4.
- `unit_tests/unit/config/test_mixin_validators.py` shows, with test mixins,
  that field and model validators on a mixin run on the assembled class, that
  validators of several mixins all run, that a model validator can read fields
  of other mixins, and that `model_construct` skips them.
- None of this design is built on master yet. Since PR #14124, master added
  more inline checks to `__init__`: the vector store authenticator check, the
  Azure network interface check, and the loader arch check for amd64-only
  stress tools. The network interface rules became a rule table, still called
  from `__init__`.

What PR #14124 showed, by failing:

1. Its flag was off during the whole load, so the validators did not run until
   a manual chain called them at the end.
2. `model_validate`, `model_copy`, and direct construction do not run
   `__init__`. The flag stayed off and the rules never ran.
3. The `params` fixture in `unit_tests/conftest.py` sets the four
   authenticator options in one `update` call. With the flag on, the first key
   raised, because the other three were not set yet. Many test modules build a
   configuration and then update it the same way.
4. A rule that writes a value re-entered the validators under
   `validate_assignment=True`. The PR wrote into the instance dictionary to
   avoid the recursion, and skipped the coercion.

## Definition of done

- Each rule lives on the mixin that owns the options it checks. `config.py`
  keeps only the rules that span domains and the code that runs the checks.
- Each construction-time rule runs once per configuration load.
- The rules run on direct construction and on `model_validate`. No path skips
  them because a flag kept its default.
- A configuration built inside the load of another configuration runs its own
  rules.
- `update` can set options that are only valid together. A failed `update`
  restores every field that it assigned, and raises.
- The existing build-then-update call sites in the unit tests pass without
  changes.
- No configuration check uses `assert`, in `config.py`, in
  `verify_configuration_urls_validity`, or in `helpers.py`.
- `__init__` holds no validation, and loses its branch and statement lint
  suppressions.
- Each mixin declares the per-backend required options that it owns. No
  requirement table changes at validation time.
- Each moved rule raises the message that it raises today.
- `sct.py conf` gives the same configuration dump as master for a test case on
  docker, aws, and gce.
- The pipeline linter reports the same failures as on master.
- PR #14124 is closed with a link to this flow directory.

## Out of scope

- Lazy cloud image resolution. It is Phase 2 of
  `docs/plans/sct-config-validation-and-lazy-images.md`.
- Options with a non-optional type that hold no value, so that an assignment
  of the current value raises. This is a separate defect.
- The `self.keys()` call in the region data branch of `__init__`. The class
  has no `keys()` method.
- A list option changed in place, which skips assignment validation.
- `model_copy` and `model_construct`, which skip validation in pydantic by
  design.
