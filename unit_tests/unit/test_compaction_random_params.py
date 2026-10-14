"""Tests for the seeded random helpers in test_lib.compaction."""

import random

import pytest

from test_lib.compaction import (
    CompactionStrategy,
    TimeWindowCompactionProperties,
    calculate_allowed_twcs_ttl,
    get_compaction_random_additional_params,
)


@pytest.mark.parametrize(
    "strategy",
    [
        pytest.param(CompactionStrategy.INCREMENTAL, id="incremental"),
        pytest.param(CompactionStrategy.SIZE_TIERED, id="size-tiered"),
        pytest.param(CompactionStrategy.LEVELED, id="leveled"),
        pytest.param(CompactionStrategy.TIME_WINDOW, id="time-window"),
    ],
)
def test_get_compaction_random_additional_params_same_seed_same_values(strategy, global_random_state):
    """Two generators with the same seed give the same parameters and the global module is untouched,
    so ToggleTableIcs replays its compaction settings from nemesis_seed."""
    params_a = get_compaction_random_additional_params(strategy, random_generator=random.Random(5))
    params_b = get_compaction_random_additional_params(strategy, random_generator=random.Random(5))

    assert params_a == params_b
    assert random.getstate() == global_random_state


def test_calculate_allowed_twcs_ttl_same_seed_same_ttl(global_random_state):
    """Two generators with the same seed give the same TTL and the global module is untouched,
    so the TWCS default-TTL nemesis replays from nemesis_seed."""
    properties = TimeWindowCompactionProperties(
        class_name="TimeWindowCompactionStrategy", compaction_window_unit="DAYS", compaction_window_size=1
    )

    ttl_a = calculate_allowed_twcs_ttl(properties, random_generator=random.Random(5))
    ttl_b = calculate_allowed_twcs_ttl(properties, random_generator=random.Random(5))

    assert ttl_a == ttl_b
    assert random.getstate() == global_random_state
