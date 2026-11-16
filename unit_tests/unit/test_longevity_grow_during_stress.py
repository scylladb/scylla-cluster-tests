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
from sdcm.sct_events import Severity

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

    def _grow_cluster_to_target_size(self, stop_event=None, creating_batch=None):
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
        # the growth returns only once the last stress_cmd round ended: a nemesis started after it never runs
        pytest.param(True, ["prepare", "stress", "nemesis", "grow"], id="grow-under-stress-and-nemesis"),
        pytest.param(False, ["prepare", "grow", "stress", "nemesis"], id="grow-before-stress"),
        pytest.param(None, ["prepare", "grow", "stress", "nemesis"], id="unset-keeps-old-order"),
    ],
)
def test_growth_order_relative_to_stress(grow_during_stress, expected_order):
    test = _OrderRecordingLongevityTest(_params(grow_cluster_during_stress=grow_during_stress))

    test.test_custom_time()

    assert test.calls == expected_order


def test_without_a_target_size_the_flag_keeps_the_old_order():
    test = _OrderRecordingLongevityTest(_params(grow_cluster_during_stress=True, cluster_target_size=None))

    test.test_custom_time()

    # the growth has nothing to add; without this the stress_cmd would get its extra round for nothing
    assert test.calls == ["prepare", "grow", "stress", "nemesis"]


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
    test.nemesis_allocator = MagicMock()

    test._grow_cluster_to_target_size()

    # 2 -> 5 -> 7: full batches of add_node_cnt, then only what is left to reach the target
    assert [call.kwargs["count"] for call in test.db_cluster.add_nodes.call_args_list] == [3, 2]
    assert len(test.db_cluster.data_nodes) == 7
    assert test.monitors.reconfigure_scylla_monitoring.call_count == 2


def test_new_nodes_are_reserved_from_the_nemesis_until_they_are_up(monkeypatch):
    monkeypatch.setattr("longevity_test.InfoEvent", MagicMock())
    monkeypatch.setattr("longevity_test.adaptive_timeout", MagicMock())
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": [4], "add_node_cnt": 2, "n_db_nodes": [2]}
    test.db_cluster = _cluster_with_nodes([2])
    test.monitors = MagicMock()
    steps = MagicMock()
    test.nemesis_allocator = steps.allocator
    test.db_cluster.wait_for_init = steps.wait_for_init
    test.db_cluster.wait_for_nodes_up_and_normal = steps.up_and_normal

    test._grow_cluster_to_target_size()

    new_nodes = test.db_cluster.data_nodes[2:]
    assert [step[0] for step in steps.mock_calls] == [
        "allocator.set_running_nemesis",
        "allocator.set_running_nemesis",
        "wait_for_init",
        "up_and_normal",
        "allocator.unset_running_nemesis",
        "allocator.unset_running_nemesis",
    ]
    reserved = [step.args for step in steps.allocator.set_running_nemesis.call_args_list]
    released = [step.args for step in steps.allocator.unset_running_nemesis.call_args_list]
    assert reserved == released == [(node, "GrowCluster") for node in new_nodes]


def test_nodes_that_a_nemesis_adds_for_a_while_are_not_counted_as_growth(monkeypatch):
    monkeypatch.setattr("longevity_test.InfoEvent", MagicMock())
    monkeypatch.setattr("longevity_test.adaptive_timeout", MagicMock())
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": [7], "add_node_cnt": 3, "n_db_nodes": [2]}
    test.db_cluster = _cluster_with_nodes([2])
    test.monitors = MagicMock()
    test.nemesis_allocator = MagicMock()
    nemesis_nodes = [MagicMock(dc_idx=0) for _ in range(3)]

    def nemesis_adds_its_nodes(**_):  # GrowShrinkCluster, while the first batch bootstraps
        if nemesis_nodes[0] not in test.db_cluster.data_nodes:
            test.db_cluster.data_nodes.extend(nemesis_nodes)

    test.db_cluster.wait_for_init.side_effect = nemesis_adds_its_nodes

    test._grow_cluster_to_target_size()

    # the cluster has 8 nodes after the first batch, but only 3 of the 5 nodes to add were added
    assert [call.kwargs["count"] for call in test.db_cluster.add_nodes.call_args_list] == [3, 2]
    for node in nemesis_nodes:  # the nemesis shrinks the cluster back
        test.db_cluster.data_nodes.remove(node)
    assert len(test.db_cluster.data_nodes) == 7


# ------------------------------------------------------------------ re-running stress_cmd during growth


def _stress_thread(done: bool = True, failed: bool = False) -> SimpleNamespace:
    """A stand-in for a stress thread: `results_futures` is waited for, and `failed` is what its verification finds."""
    future = Future()
    if done:
        future.set_result(None)
    return SimpleNamespace(results_futures=[future], failed=failed)


def _finish_shortly_after(thread: SimpleNamespace, event: threading.Event) -> None:
    """End `thread` shortly after `event` is set, so that what the event reports is already done by then."""

    def finish():
        event.wait(timeout=10)
        threading.Event().wait(0.3)
        thread.results_futures[0].set_result(None)

    threading.Thread(target=finish, daemon=True).start()


def _no_rerun_expected(number):
    raise AssertionError(f"stress_cmd was run again (round {number + 1}) although no more rounds were expected")


class _TestStopped(BaseException):
    """What a critical event raises in the main thread of the test (`CriticalTestFailure`)."""


class _RoundsLongevityTest(LongevityTest):
    def __init__(self, grow, make_round):  # skip the full ClusterTester.__init__
        self.params = {"stress_cmd": ["cassandra-stress write duration=1m"]}
        self.rounds = 0
        self.log = MagicMock()
        self._grow = grow
        self._make_round = make_round

    def assemble_and_run_all_stress_cmd(self, stress_queue, stress_cmd, keyspace_num):
        self.rounds += 1
        stress_queue.append(self._make_round(self.rounds))

    def _grow_cluster_to_target_size(self, stop_event=None, creating_batch=None):
        self.stop_event = stop_event
        self.creating_batch = creating_batch
        self._grow()

    def verify_stress_thread(self, thread_pool, error_handler=None):
        if thread_pool.failed is _TestStopped:
            raise _TestStopped
        return not thread_pool.failed


@pytest.fixture(autouse=True)
def _no_events(monkeypatch):
    monkeypatch.setattr("longevity_test.InfoEvent", MagicMock())


@pytest.fixture
def error_event(monkeypatch):
    """The event that reports a failed round; its `published` is set once the event is published."""
    event = MagicMock()
    event.published = threading.Event()
    event.return_value.publish.side_effect = event.published.set
    monkeypatch.setattr("longevity_test.TestFrameworkEvent", event)
    return event


def test_stress_is_run_again_until_the_growth_finishes_and_once_more_after_it():
    third_round_started, growth_returned = threading.Event(), threading.Event()
    round_that_outlives_the_growth = _stress_thread(done=False)
    round_on_the_full_size_cluster = _stress_thread()

    def grow():
        third_round_started.wait(timeout=10)  # the growth outlasts two re-runs of stress_cmd
        growth_returned.set()

    def make_round(number):
        if number == 1:
            return _stress_thread()
        if number == 2:
            third_round_started.set()
            _finish_shortly_after(round_that_outlives_the_growth, growth_returned)
            return round_that_outlives_the_growth
        if number == 3:
            return round_on_the_full_size_cluster
        return _no_rerun_expected(number)

    test = _RoundsLongevityTest(grow, make_round)
    first_round = [_stress_thread()]
    stress_queue = list(first_round)

    test._grow_cluster_under_stress(stress_queue, first_round, keyspace_num=1)

    # two rounds ended before the growth did, the third outlived it, and one more ran on the full-size cluster
    assert test.rounds == 3
    assert len(stress_queue) == 4  # every round stays queued, to be verified with the rest of the load
    assert stress_queue[-2:] == [round_that_outlives_the_growth, round_on_the_full_size_cluster]


def test_only_one_round_is_run_after_the_growth_finished():
    growth_returned = threading.Event()
    first_round = [_stress_thread(done=False)]
    _finish_shortly_after(first_round[0], growth_returned)

    def make_round(number):
        return _stress_thread() if number == 1 else _no_rerun_expected(number)

    test = _RoundsLongevityTest(growth_returned.set, make_round)

    test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    assert test.rounds == 1


@pytest.mark.parametrize("healthy_rounds", [0, 2], ids=["first-round-fails", "third-round-fails"])
def test_stress_is_not_run_again_after_a_round_failed(error_event, healthy_rounds):
    growth_finished = threading.Event()

    def grow():
        assert error_event.published.wait(timeout=10)  # the growth outlasts the failed round
        growth_finished.set()

    def make_round(number):
        if number <= healthy_rounds:
            return _stress_thread(failed=number == healthy_rounds)
        return _no_rerun_expected(number)

    test = _RoundsLongevityTest(grow, make_round)
    first_round = [_stress_thread(failed=healthy_rounds == 0)]
    stress_queue = list(first_round)

    test._grow_cluster_under_stress(stress_queue, first_round, keyspace_num=1)

    assert test.rounds == healthy_rounds
    assert len(stress_queue) == healthy_rounds + 1  # the failed round stays queued too, for the final verification
    error_event.return_value.publish.assert_called_once_with()
    assert error_event.call_args.kwargs["severity"] == Severity.ERROR
    assert growth_finished.is_set()  # the growth is still awaited, without the load


def test_no_round_is_run_on_the_full_size_cluster_after_a_failed_round(error_event):
    growth_returned = threading.Event()
    first_round = [_stress_thread(done=False, failed=True)]
    _finish_shortly_after(first_round[0], growth_returned)
    test = _RoundsLongevityTest(growth_returned.set, make_round=_no_rerun_expected)

    test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    assert test.rounds == 0
    error_event.return_value.publish.assert_called_once_with()


def test_growth_failure_is_raised_once_the_running_round_ends():
    growth_returned = threading.Event()
    first_round = [_stress_thread(done=False)]
    _finish_shortly_after(first_round[0], growth_returned)

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


def test_stopped_test_does_not_wait_for_the_bootstrap_of_a_batch():
    growth_can_end, growth_ended = threading.Event(), threading.Event()

    def grow():  # the nodes of the batch are created, the growth waits for them to join
        growth_can_end.wait(timeout=10)
        growth_ended.set()

    test = _RoundsLongevityTest(grow, make_round=_no_rerun_expected)
    first_round = [_stress_thread(failed=_TestStopped)]  # the test is stopped while the growth is still running

    with pytest.raises(_TestStopped):
        test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    try:
        assert not growth_ended.is_set()  # the call returned without waiting for the growth
        assert test.stop_event.is_set()  # and the growth was told to add no more batches
        test.log.warning.assert_not_called()
    finally:
        growth_can_end.set()


def test_stopped_test_waits_for_the_batch_that_is_being_created():
    creating_batch, batch_created = threading.Event(), threading.Event()

    def grow():
        with test.creating_batch:
            creating_batch.set()
            test.stop_event.wait(timeout=10)
            threading.Event().wait(0.3)  # the nodes are still being created after the test was stopped
            batch_created.set()

    test = _RoundsLongevityTest(grow, make_round=_no_rerun_expected)
    first_round = [_stress_thread(done=False, failed=_TestStopped)]
    _finish_shortly_after(first_round[0], creating_batch)  # the test is stopped while the batch is being created

    with pytest.raises(_TestStopped):
        test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    # the teardown starts only now: it stops and destroys only the nodes that are registered in the cluster
    assert batch_created.is_set()
    test.log.warning.assert_not_called()


def test_stopped_test_gives_up_on_a_batch_that_takes_too_long_to_create(monkeypatch):
    monkeypatch.setattr("longevity_test.GROWTH_BATCH_CREATION_TIMEOUT", 0.1)
    creating_batch, batch_can_end = threading.Event(), threading.Event()

    def grow():
        with test.creating_batch:
            creating_batch.set()
            batch_can_end.wait(timeout=10)

    test = _RoundsLongevityTest(grow, make_round=_no_rerun_expected)
    first_round = [_stress_thread(done=False, failed=_TestStopped)]
    _finish_shortly_after(first_round[0], creating_batch)

    try:
        with pytest.raises(_TestStopped):
            test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)
        test.log.warning.assert_called_once()
    finally:
        batch_can_end.set()


@pytest.mark.parametrize(
    "stopped_during, expected_batches, expected_waits",
    [
        pytest.param("lock", 0, 0, id="no-batch-when-stopped-right-before-it"),
        pytest.param("add_nodes", 1, 0, id="nodes-just-created-are-not-waited-for"),
        pytest.param("wait_for_init", 1, 1, id="no-batch-after-the-one-that-finished"),
    ],
)
def test_grow_adds_no_more_batches_once_stopped(monkeypatch, stopped_during, expected_batches, expected_waits):
    monkeypatch.setattr("longevity_test.adaptive_timeout", MagicMock())
    stop_event = threading.Event()
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": [8], "add_node_cnt": 3, "n_db_nodes": [2]}
    test.log = MagicMock()
    test.db_cluster = _cluster_with_nodes([2])
    test.monitors = MagicMock()
    test.nemesis_allocator = MagicMock()
    add_nodes = test.db_cluster.add_nodes.side_effect

    def stop_in_add_nodes(**kwargs):
        stop_event.set()
        return add_nodes(**kwargs)

    creating_batch = MagicMock()  # a lock that only records its use
    creating_batch.__exit__.return_value = False
    if stopped_during == "lock":
        creating_batch.__enter__.side_effect = lambda *_: stop_event.set()
    elif stopped_during == "add_nodes":
        test.db_cluster.add_nodes.side_effect = stop_in_add_nodes
    else:
        test.db_cluster.wait_for_init.side_effect = lambda **_: stop_event.set()

    test._grow_cluster_to_target_size(stop_event=stop_event, creating_batch=creating_batch)

    assert test.db_cluster.add_nodes.call_count == expected_batches  # 2 -> 5, and not on to 8
    assert test.db_cluster.wait_for_init.call_count == expected_waits


def test_grow_without_target_is_noop():
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": None}
    test.db_cluster = MagicMock()

    test._grow_cluster_to_target_size()

    test.db_cluster.add_nodes.assert_not_called()
