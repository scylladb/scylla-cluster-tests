import random
from unittest import mock

import pytest

from sdcm.nemesis import NemesisRunner
from sdcm.utils.common import MAX_NAMES_IN_CMD
from unit_tests.unit.nemesis import TestRunner


def test_few_tables_snapshot_caps_table_count():
    runner = TestRunner(ks_cfs=[f"ks.t{i}" for i in range(5000)])
    runner.target_node.run_nodetool.return_value.stdout = ""
    runner.random = random.Random()

    # first choice of each runner.random.choice: the few-tables option, then the "ks" keyspace
    with (
        mock.patch.object(runner.random, "choice", side_effect=lambda seq: seq[0]),
        pytest.raises(Exception, match="Snapshot name wasn't found"),
    ):
        NemesisRunner.disrupt_snapshot_operations(runner)

    cmd = runner.target_node.run_nodetool.call_args.args[0]
    assert cmd.startswith("snapshot ks -cf ")
    tables = cmd.removeprefix("snapshot ks -cf ").split(",")
    assert len(set(tables)) == MAX_NAMES_IN_CMD


def test_few_keyspaces_snapshot_caps_keyspace_count():
    runner = TestRunner(ks_cfs=[f"ks{i}.t" for i in range(5000)])
    runner.target_node.run_nodetool.return_value.stdout = ""
    runner.random = random.Random()

    # last snapshot option: few keyspaces
    with (
        mock.patch.object(runner.random, "choice", side_effect=lambda seq: seq[-1]),
        pytest.raises(Exception, match="Snapshot name wasn't found"),
    ):
        NemesisRunner.disrupt_snapshot_operations(runner)

    cmd = runner.target_node.run_nodetool.call_args.args[0]
    assert cmd.startswith("snapshot -kc ")
    keyspaces = cmd.removeprefix("snapshot -kc ").split(",")
    assert len(set(keyspaces)) == MAX_NAMES_IN_CMD
