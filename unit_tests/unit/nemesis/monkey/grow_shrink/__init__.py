"""Tests for the grow/shrink nemesis classes in ``sdcm.nemesis.monkey.grow_shrink``
and the grow/shrink primitives in ``sdcm.nemesis.utils.topology_ops``."""

from unit_tests.unit.nemesis import nemesis_config, params_with_nemesis

MODULE = "sdcm.nemesis.monkey.grow_shrink"
OPS_MODULE = "sdcm.nemesis.utils.topology_ops"


def nemesis_params(sequence_sleep_between_ops=None, **grow_shrink) -> dict:
    """Tester params holding a `nemesis` section with the given grow/shrink options."""
    return params_with_nemesis(
        nemesis_config(sequence_sleep_between_ops=sequence_sleep_between_ops, grow_shrink=grow_shrink)
    )
