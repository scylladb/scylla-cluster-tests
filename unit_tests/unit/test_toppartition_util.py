"""Tests for the seeded argument generation of the toppartitions nemesis commands."""

import random

import pytest

from sdcm.utils.toppartition_util import NewApiTopPartitionCmd, OldApiTopPartitionCmd

KS_CF_LIST = ["ks1.table1", "ks1.table2", "ks2.table3"]


@pytest.mark.parametrize(
    "cmd_class",
    [pytest.param(NewApiTopPartitionCmd, id="new-api"), pytest.param(OldApiTopPartitionCmd, id="old-api")],
)
def test_generate_cmd_arg_values_same_seed_same_command(cmd_class):
    """Two generators with the same seed build the same nodetool toppartitions arguments, so the
    toppartitions nemesis replays from nemesis_seed."""
    cmd_a = cmd_class(KS_CF_LIST, random_generator=random.Random(3))
    cmd_b = cmd_class(KS_CF_LIST, random_generator=random.Random(3))

    cmd_a.generate_cmd_arg_values()
    cmd_b.generate_cmd_arg_values()

    assert cmd_a.get_cmd_args() == cmd_b.get_cmd_args()
