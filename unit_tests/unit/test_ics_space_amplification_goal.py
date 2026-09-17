"""Tests for the ``ics_space_amplification_goal`` post-prepare hook in ``LoaderUtilsMixin``."""

from unittest.mock import MagicMock, call

import pytest

from sdcm.utils.loader_utils import LoaderUtilsMixin

ICS_ALTER = (
    "ALTER TABLE {} WITH compaction = {{'class': 'IncrementalCompactionStrategy', 'space_amplification_goal': '1.2'}}"
)


@pytest.fixture
def tester():
    """``LoaderUtilsMixin`` wired to a cluster that reports two user tables."""
    tester = LoaderUtilsMixin()
    tester.log = MagicMock()
    tester.params = {}
    tester.db_cluster = MagicMock()
    tester.db_cluster.get_non_system_ks_cf_list.return_value = ["keyspace1.standard1", "feeds.table0"]
    return tester


@pytest.mark.parametrize(
    ("goal", "expected_alters"),
    [
        pytest.param(
            1.2,
            [call(cmd=ICS_ALTER.format("keyspace1.standard1")), call(cmd=ICS_ALTER.format("feeds.table0"))],
            id="goal-set",
        ),
        pytest.param(None, [], id="goal-unset"),
    ],
)
def test_post_prepare_applies_ics_space_amplification_goal(events, tester, goal, expected_alters):
    """After prepare every non-system table is switched to ICS with the goal, and nothing is altered without one."""
    tester.params["ics_space_amplification_goal"] = goal

    tester.run_post_prepare_cql_cmds()

    assert tester.db_cluster.nodes[0].run_cqlsh.call_args_list == expected_alters
    if goal:
        tester.db_cluster.get_non_system_ks_cf_list.assert_called_once_with(
            db_node=tester.db_cluster.nodes[0], filter_out_mv=True, filter_empty_tables=False
        )
    else:
        tester.db_cluster.get_non_system_ks_cf_list.assert_not_called()
