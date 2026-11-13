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

"""Where `test_custom_time` grows the cluster relative to the main load (`grow_cluster_during_stress`).

The scale test is meant to add nodes while the stress runs, and to keep the stress running until the
cluster reaches its target size. By default the cluster is grown first and the stress starts after it,
which every other `cluster_target_size` user keeps.
"""

import threading
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from longevity_test import LongevityTest

LongevityTest.__test__ = False


class _OrderRecordingLongevityTest(LongevityTest):
    def __init__(self, params):  # skip the full ClusterTester.__init__
        self.params = params
        self.calls = []
        self.partitions_attrs = None
        self.validate_large_collections = False
        self.db_cluster = MagicMock()
        self.db_cluster.start_nemesis.side_effect = lambda: self.calls.append("nemesis")
        for name in (
            "get_nemesis_class",
            "download_artifacts_from_s3",
            "pre_create_alternator_tables",
            "run_pre_create_keyspace",
            "run_pre_create_schema",
            "kafka_configure",
            "verify_stress_thread",
        ):
            setattr(self, name, MagicMock())
        self._get_scan_operation_params = MagicMock(return_value=None)
        self._get_tombstone_gc_verification_params = MagicMock(return_value=None)

    def run_prepare_write_cmd(self):
        self.calls.append("prepare")

    def assemble_and_run_all_stress_cmd(self, stress_queue, stress_cmd, keyspace_num):
        self.calls.append("stress")

    def _grow_cluster_to_target_size(self):
        self.calls.append("grow")


def _params(**overrides):
    params = {
        "prepare_write_cmd": ["cassandra-stress write n=10"],
        "stress_cmd": ["cassandra-stress write duration=1m"],
        "keyspace_num": 1,
        "cluster_target_size": [4],
        "nemesis_during_prepare": False,
    }
    params.update(overrides)
    return params


@pytest.mark.parametrize(
    "grow_during_stress, expected_order",
    [
        pytest.param(True, ["prepare", "stress", "grow", "nemesis"], id="grow-under-stress"),
        pytest.param(False, ["prepare", "grow", "stress", "nemesis"], id="grow-before-stress"),
        pytest.param(None, ["prepare", "grow", "stress", "nemesis"], id="unset-keeps-old-order"),
    ],
)
def test_growth_order_relative_to_stress(grow_during_stress, expected_order):
    test = _OrderRecordingLongevityTest(_params(grow_cluster_during_stress=grow_during_stress))

    test.test_custom_time()

    assert test.calls == expected_order


def _cluster_with_nodes(sizes_per_dc):
    cluster = MagicMock()
    cluster.data_nodes = [MagicMock(dc_idx=dc_idx) for dc_idx, size in enumerate(sizes_per_dc) for _ in range(size)]

    def add_nodes(count, dc_idx, **_):
        new_nodes = [MagicMock(dc_idx=dc_idx) for _ in range(count)]
        cluster.data_nodes.extend(new_nodes)
        return new_nodes

    cluster.add_nodes.side_effect = add_nodes
    return cluster


def test_grow_adds_batches_until_target(monkeypatch):
    monkeypatch.setattr("longevity_test.InfoEvent", MagicMock())
    monkeypatch.setattr("longevity_test.adaptive_timeout", MagicMock())
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": [7], "add_node_cnt": 3, "n_db_nodes": [2]}
    test.db_cluster = _cluster_with_nodes([2])
    test.monitors = MagicMock()

    test._grow_cluster_to_target_size()

    # 2 -> 5 -> 7: full batches of add_node_cnt, then only what is left to reach the target
    assert [call.kwargs["count"] for call in test.db_cluster.add_nodes.call_args_list] == [3, 2]
    assert len(test.db_cluster.data_nodes) == 7
    assert test.monitors.reconfigure_scylla_monitoring.call_count == 2


# ------------------------------------------------------------------ re-running stress_cmd during growth


def _stress_thread(done: bool = True) -> SimpleNamespace:
    """A stand-in for a stress thread: only `results_futures` is read while the cluster grows."""
    future = Future()
    if done:
        future.set_result(None)
    return SimpleNamespace(results_futures=[future])


def _finish_after_growth(thread: SimpleNamespace, growth_returned: threading.Event) -> None:
    """End `thread` shortly after the growth returned, so that the growth is already done by then."""

    def finish():
        growth_returned.wait(timeout=10)
        threading.Event().wait(0.3)
        thread.results_futures[0].set_result(None)

    threading.Thread(target=finish, daemon=True).start()


def _no_rerun_expected(number):
    raise AssertionError(f"stress_cmd was run again (round {number + 1}) although the growth had finished")


class _RoundsLongevityTest(LongevityTest):
    def __init__(self, grow, make_round):  # skip the full ClusterTester.__init__
        self.params = {"stress_cmd": ["cassandra-stress write duration=1m"]}
        self.rounds = 0
        self._grow = grow
        self._make_round = make_round

    def assemble_and_run_all_stress_cmd(self, stress_queue, stress_cmd, keyspace_num):
        self.rounds += 1
        stress_queue.append(self._make_round(self.rounds))

    def _grow_cluster_to_target_size(self):
        self._grow()


@pytest.fixture(autouse=True)
def _no_events(monkeypatch):
    monkeypatch.setattr("longevity_test.InfoEvent", MagicMock())


def test_stress_is_run_again_until_the_growth_finishes():
    third_round_started, growth_returned = threading.Event(), threading.Event()
    last_round = _stress_thread(done=False)

    def grow():
        third_round_started.wait(timeout=10)  # the growth outlasts two re-runs of stress_cmd
        growth_returned.set()

    def make_round(number):
        if number == 1:
            return _stress_thread()
        if number == 2:
            third_round_started.set()
            _finish_after_growth(last_round, growth_returned)
            return last_round
        return _no_rerun_expected(number)

    test = _RoundsLongevityTest(grow, make_round)
    first_round = [_stress_thread()]
    stress_queue = list(first_round)

    test._grow_cluster_under_stress(stress_queue, first_round, keyspace_num=1)

    # the first round ended twice before the growth did; the round that outlived the growth is the last one
    assert test.rounds == 2
    assert len(stress_queue) == 3  # every round stays queued, to be verified with the rest of the load
    assert stress_queue[-1] is last_round


def test_stress_is_not_run_again_once_the_growth_finished():
    growth_returned = threading.Event()
    first_round = [_stress_thread(done=False)]
    _finish_after_growth(first_round[0], growth_returned)
    test = _RoundsLongevityTest(growth_returned.set, make_round=_no_rerun_expected)

    test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    assert test.rounds == 0


def test_growth_failure_is_raised_once_the_running_round_ends():
    growth_returned = threading.Event()
    first_round = [_stress_thread(done=False)]
    _finish_after_growth(first_round[0], growth_returned)

    def grow():
        growth_returned.set()
        raise RuntimeError("add_nodes failed")

    test = _RoundsLongevityTest(grow, make_round=_no_rerun_expected)

    with pytest.raises(RuntimeError, match="add_nodes failed"):
        test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)
    assert test.rounds == 0


def test_without_a_stress_round_the_growth_is_awaited_and_nothing_is_re_run():
    grown = threading.Event()

    def grow():
        threading.Event().wait(0.2)
        grown.set()

    test = _RoundsLongevityTest(grow, make_round=_no_rerun_expected)

    test._grow_cluster_under_stress([], [], keyspace_num=1)

    assert grown.is_set()
    assert test.rounds == 0


def test_grow_without_target_is_noop():
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": None}
    test.db_cluster = MagicMock()

    test._grow_cluster_to_target_size()

    test.db_cluster.add_nodes.assert_not_called()
