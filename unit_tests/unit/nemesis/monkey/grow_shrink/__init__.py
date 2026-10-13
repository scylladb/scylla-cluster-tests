"""Tests for the grow/shrink nemesis classes in ``sdcm.nemesis.monkey.grow_shrink``
and the grow/shrink primitives in ``sdcm.nemesis.utils.topology_ops``."""

from sdcm.sct_config.nemesis_runners import GrowShrinkConfig, NemesisDefaults

MODULE = "sdcm.nemesis.monkey.grow_shrink"
OPS_MODULE = "sdcm.nemesis.utils.topology_ops"


def nemesis_params(sequence_sleep_between_ops=None, **grow_shrink) -> dict:
    """Tester params holding a `nemesis` option with the given grow/shrink options."""
    return {
        "nemesis": NemesisDefaults(
            sequence_sleep_between_ops=sequence_sleep_between_ops, grow_shrink=GrowShrinkConfig(**grow_shrink)
        )
    }
