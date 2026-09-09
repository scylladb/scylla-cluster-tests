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

"""Proof that validation can move onto the mixins, the Pydantic way.

SCT-525 phase 4 wants each mixin to own the validation of its own options -- including the
per-backend requirement tables in `sdcm/sct_config/defaults.py`, which exist only for validation --
expressed as Pydantic validators rather than the hand-rolled `verify_configuration()` chain.

These tests pin down what the assembled-mixin model actually supports, so phase 4 starts from
demonstrated behaviour instead of an assumption. They deliberately use throwaway mixins rather than
the real ones: the point is the mechanism, and adding validators to the real mixins is phase 4's
job.

What is established here:

1. a `@field_validator` on a mixin runs when the assembled model is built;
2. so does a `@model_validator(mode="after")`;
3. validators from *several* mixins all run -- multiple inheritance does not shadow them;
4. a mixin's `model_validator` sees fields owned by *other* mixins, so the awkward cross-domain
   checks have somewhere to live without going back to a central function;
5. `model_construct()` skips validation, which is what lets a test build a config cheaply.
"""

import pytest
from pydantic import BaseModel, field_validator, model_validator

from sdcm.sct_config.mixins import CONFIG_GROUPS


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
