# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""What the assembled-mixin model supports, and what the validation plumbing guarantees.

SCT-525 phase 4 moves each option's validation onto the mixin that owns it. The first half of this
file establishes the raw Pydantic mechanism with throwaway mixins; the second half guards the
integration with `sdcm.sct_config.validation` on the real `SCTConfiguration`, where the failure
modes are silent ones.

The mechanism, on throwaway mixins:

1. a `@field_validator` on a mixin runs when the assembled model is built;
2. so does a `@model_validator(mode="after")`;
3. validators from *several* mixins all run -- multiple inheritance does not shadow them;
4. a mixin's `model_validator` sees fields owned by *other* mixins, so the awkward cross-domain
   checks have somewhere to live without going back to a central function;
5. `model_construct()` skips validation, which is what lets a test build a config cheaply.

The plumbing, on the real model:

6. a `@cross_field_check` runs exactly once per load, not once per field assignment;
7. the checks run on construction paths that never touch `__init__` -- the silent-skip failure that
   sank PR #14124;
8. `update()` applies its keys as one transaction: jointly-valid options can be set together, and a
   rejected update leaves the configuration untouched;
9. no two mixins declare a validator under the same name, which would silently drop one of them;
10. instance revalidation stays off, which is what `run_cross_field_checks` relies on.
"""

import pytest
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from sdcm.sct_config.config import SCTConfiguration
from sdcm.sct_config.mixins import CONFIG_GROUPS
from sdcm.sct_config.validation import cross_field_check, deferred_checks


class _AlphaMixin(BaseModel):
    """Stands in for a domain mixin owning `alpha`."""

    alpha: int = 1

    @field_validator("alpha")
    @classmethod
    def _alpha_is_positive(cls, value):
        if value < 0:
            raise ValueError("alpha must be positive")
        return value


class _BetaMixin(BaseModel):
    """Stands in for a second domain mixin owning `beta`."""

    beta: int = 1

    @field_validator("beta")
    @classmethod
    def _beta_is_even(cls, value):
        if value % 2:
            raise ValueError("beta must be even")
        return value

    @model_validator(mode="after")
    def _beta_not_larger_than_alpha(self):
        """Deliberately reads `alpha`, which this mixin does not own."""
        if self.beta > getattr(self, "alpha", 0):
            raise ValueError("beta must not exceed alpha")
        return self


class _Assembled(_AlphaMixin, _BetaMixin):
    """Stands in for SCTConfiguration: assembled from the mixins, adding nothing itself."""


def test_field_validator_declared_on_a_mixin_runs_on_the_assembled_model():
    with pytest.raises(ValueError, match="alpha must be positive"):
        _Assembled(alpha=-1)


def test_field_validators_from_several_mixins_all_run():
    """Multiple inheritance must not shadow a sibling mixin's validators."""
    with pytest.raises(ValueError, match="beta must be even"):
        _Assembled(alpha=10, beta=3)

    # and the other mixin's validator is still live
    with pytest.raises(ValueError, match="alpha must be positive"):
        _Assembled(alpha=-2, beta=2)


def test_model_validator_on_a_mixin_can_read_fields_owned_by_another_mixin():
    """This is the escape hatch for genuinely cross-domain rules.

    Phase 4 should still prefer keeping a rule in the mixin that owns the fields, but where a check
    truly spans domains it does not have to move back into a central `verify_configuration()`.
    """
    with pytest.raises(ValueError, match="beta must not exceed alpha"):
        _Assembled(alpha=2, beta=4)

    assert _Assembled(alpha=4, beta=2).beta == 2


def test_model_construct_bypasses_mixin_validation():
    """How a test builds a config without satisfying every rule."""
    built = _Assembled.model_construct(alpha=-1, beta=3)
    assert built.alpha == -1
    assert built.beta == 3


def test_real_mixins_are_plain_base_models_so_the_above_applies():
    """The mechanism above only holds because the real mixins inherit from BaseModel directly."""
    for mixin in CONFIG_GROUPS:
        bases = [base for base in mixin.__bases__ if base is not object]
        assert bases == [BaseModel], (
            f"{mixin.__name__} inherits from {bases} rather than BaseModel directly; "
            f"validator resolution on the assembled model is only proven for the direct case"
        )


# --------------------------------------------------------------------------------------------
# The plumbing, on the real SCTConfiguration
# --------------------------------------------------------------------------------------------


def test_a_cross_field_check_runs_once_per_load_not_once_per_assignment():
    """The whole reason `deferred_checks` exists.

    `SCTConfiguration.__init__` merges the configuration one field assignment at a time, and
    `validate_assignment=True` re-runs every model validator on each of them. An unguarded rule
    therefore runs a couple of hundred times against states that are not yet configurations.
    """
    unguarded, guarded = [], []

    class _Counting(SCTConfiguration):
        @model_validator(mode="after")
        def _count_unguarded(self):
            unguarded.append(1)
            return self

        @cross_field_check
        def _count_guarded(self):
            guarded.append(1)
            return self

    _Counting()

    assert guarded == [1], "a cross-field check must run exactly once per load"
    assert len(unguarded) > 100, (
        f"expected the unguarded validator to run once per field assignment, got {len(unguarded)} -- "
        f"if this dropped to 1, validate_assignment or the loading style changed and the guard may "
        f"no longer be needed"
    )


def test_checks_run_on_construction_paths_that_skip_init():
    """PR #14124's silent-skip failure, pinned.

    Its guard defaulted to *off* and was flipped at the end of `__init__`, so every path that does
    not run `__init__` skipped every rule, forever and without a word. Defaulting to checks-on means
    a path nobody anticipated errs towards validating.
    """
    with pytest.raises(ValidationError, match="k8s_enable_tls"):
        SCTConfiguration.model_validate({"k8s_enable_sni": True, "k8s_enable_tls": False})


def test_update_applies_jointly_valid_options_together(monkeypatch):
    """What `unit_tests/conftest.py`'s `params` fixture needs, and what #14124 broke.

    None of these three is valid without the other two, so a key-by-key update cannot set them.
    """
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    config = SCTConfiguration()

    config.update(
        dict(
            authenticator="PasswordAuthenticator",
            authenticator_user="cassandra",
            authenticator_password="cassandra",
        )
    )

    assert config.authenticator_user == "cassandra"


def test_a_rejected_update_changes_nothing(monkeypatch):
    """Otherwise the next, unrelated assignment inherits the mess and raises somewhere confusing."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    config = SCTConfiguration()
    config.update(dict(authenticator="PasswordAuthenticator", authenticator_user="a", authenticator_password="b"))

    with pytest.raises(ValidationError):
        config.update(dict(authenticator="PasswordAuthenticator", authenticator_user="", authenticator_password=""))

    assert (config.authenticator_user, config.authenticator_password) == ("a", "b")
    config.simulated_regions = 2  # an unrelated assignment must not inherit a rejected update


def test_update_nested_in_a_deferred_window_leaves_checking_to_the_window(monkeypatch):
    """The loader merges through `update()`, so it must not try to validate mid-load."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    config = SCTConfiguration()

    with deferred_checks():
        config.update(dict(authenticator="PasswordAuthenticator"))  # invalid on its own, tolerated here
        config.update(dict(authenticator_user="cassandra", authenticator_password="cassandra"))

    assert config.authenticator_user == "cassandra"


def test_no_two_mixins_declare_a_validator_under_the_same_name():
    """Pydantic keys validators by class attribute name, so a collision drops one without a word.

    A guard test rather than a naming convention, because the failure is invisible: the config still
    loads, one rule just stops existing.
    """
    seen: dict[str, str] = {}
    for mixin in CONFIG_GROUPS:
        decorators = mixin.__pydantic_decorators__
        for name in (*decorators.model_validators, *decorators.field_validators):
            if name in seen:
                raise AssertionError(
                    f"{mixin.__name__} and {seen[name]} both declare a validator named {name!r}; "
                    f"one of them is silently shadowed on the assembled model"
                )
            seen[name] = mixin.__name__


def test_every_mixin_validator_survives_onto_the_assembled_model():
    """The collision guard above proves names are unique; this proves nothing else eats them."""
    assembled = set(SCTConfiguration.__pydantic_decorators__.model_validators)
    for mixin in CONFIG_GROUPS:
        for name in mixin.__pydantic_decorators__.model_validators:
            assert name in assembled, f"{mixin.__name__}.{name} did not reach SCTConfiguration"


def test_instance_revalidation_stays_off():
    """`run_cross_field_checks` re-validates an existing instance to fire the rules once.

    That only works while the model schema short-circuits on an isinstance check. Turning
    `revalidate_instances` on would make it a deep re-validation of every field instead, and the
    mechanism collapses -- silently, into a wall of errors on configurations that are fine.
    """
    for model in (SCTConfiguration, *CONFIG_GROUPS):
        assert model.model_config.get("revalidate_instances", "never") == "never", (
            f"{model.__name__} enables instance revalidation; see sdcm.sct_config.validation"
        )
