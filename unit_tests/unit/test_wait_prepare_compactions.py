"""Tests for LoaderUtilsMixin.wait_prepare_compactions."""

from unittest.mock import MagicMock

import pytest

from sdcm.utils.loader_utils import LoaderUtilsMixin


class FakeLoaderMixin(LoaderUtilsMixin):
    """Minimal LoaderUtilsMixin stand-in with mocked DB nodes and compactions waiter."""

    def __init__(self, params: dict):
        self.params = params
        self.db_cluster = MagicMock()
        self.db_cluster.nodes = [MagicMock(), MagicMock()]
        self.log = MagicMock()
        # 'wait_no_compactions_running' is wrapped by 'measure_time', which returns the elapsed time and the result.
        self.wait_no_compactions_running = MagicMock(return_value=(120.0, None))


@pytest.mark.parametrize("major", [False, True])
def test_no_timeout_skips_waiting(major):
    tester = FakeLoaderMixin({"prepare_wait_no_compactions_trigger_major": major})

    tester.wait_prepare_compactions()

    tester.wait_no_compactions_running.assert_not_called()
    for node in tester.db_cluster.nodes:
        node.run_nodetool.assert_not_called()


def test_major_compaction_runs_on_every_node_before_waiting():
    tester = FakeLoaderMixin(
        {"prepare_wait_no_compactions_timeout": 60, "prepare_wait_no_compactions_trigger_major": True}
    )

    tester.wait_prepare_compactions()

    for node in tester.db_cluster.nodes:
        node.run_nodetool.assert_called_once_with("compact")
    tester.wait_no_compactions_running.assert_called_once_with(n=60)


def test_without_major_compaction_only_waits():
    tester = FakeLoaderMixin(
        {"prepare_wait_no_compactions_timeout": 90, "prepare_wait_no_compactions_trigger_major": False}
    )

    tester.wait_prepare_compactions()

    for node in tester.db_cluster.nodes:
        node.run_nodetool.assert_not_called()
    tester.wait_no_compactions_running.assert_called_once_with(n=90)
