"""Tests for the argument generation of the toppartitions nemesis commands."""

import random
from unittest import mock

import pytest

from sdcm.utils.common import MAX_NAMES_IN_CMD
from sdcm.utils.toppartition_util import NewApiTopPartitionCmd, OldApiTopPartitionCmd

KS_CF_LIST = ["ks1.table1", "ks1.table2", "ks2.table3"]


def test_new_api_filter_ks_cf_caps_table_count():
    cmd = NewApiTopPartitionCmd(ks_cf_list=[f"ks.t{i}" for i in range(5000)], random_generator=random.Random())
    # randint picks the upper bound, the most tables allowed
    with mock.patch.object(cmd.random_generator, "randint", side_effect=lambda low, high: high):
        assert len(cmd._filter_ks_cf()) == MAX_NAMES_IN_CMD


@pytest.mark.parametrize(
    "cmd_class",
    [pytest.param(NewApiTopPartitionCmd, id="new-api"), pytest.param(OldApiTopPartitionCmd, id="old-api")],
)
def test_generate_cmd_arg_values_same_seed_same_command(cmd_class):
    """Two generators with the same seed build the same nodetool toppartitions arguments, so the
    toppartitions nemesis replays from nemesis_seed.
    """
    cmd_a = cmd_class(KS_CF_LIST, random_generator=random.Random(3))
    cmd_b = cmd_class(KS_CF_LIST, random_generator=random.Random(3))

    cmd_a.generate_cmd_arg_values()
    cmd_b.generate_cmd_arg_values()

    assert cmd_a.get_cmd_args() == cmd_b.get_cmd_args()
