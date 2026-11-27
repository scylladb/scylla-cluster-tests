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

"""How `test_custom_time` keeps running stress_cmd until `minimum_nemesis_to_execute` nemeses were executed.

A scale test ends by the growth of the cluster and one stress_cmd round, so the number of nemeses it runs is
small and left to chance. With the minimum, stress_cmd is run again, one round after another, until enough
nemeses ran to their end.
"""

import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from argus.common.enums import NemesisStatus
from longevity_test import LongevityTest
from sdcm.nemesis import NemesisRunner
from sdcm.sct_events import Severity

LongevityTest.__test__ = False


class _Runner:
    """A nemesis thread of the test: only what the count of executed nemeses reads."""

    executed_count = NemesisRunner.executed_count

    def __init__(self):
        self.operation_log = []

    def finish(self, *statuses: NemesisStatus) -> None:
        self.operation_log.extend({"operation": "SomeNemesis", "subtype": status.value} for status in statuses)


def _stress_thread(failed: bool = False) -> SimpleNamespace:
    """A stress thread that already ended."""
    future = Future()
    future.set_result(None)
    return SimpleNamespace(results_futures=[future], failed=failed)


class _RoundsLongevityTest(LongevityTest):
    """Runs the rounds of stress_cmd for real, with `during_round(number)` called while each new round runs."""

    def __init__(self, minimum, during_round=lambda number: None, runners=1):  # skip the full ClusterTester.__init__
        self.params = {"stress_cmd": ["cassandra-stress write duration=10m"], "minimum_nemesis_to_execute": minimum}
        self.rounds = 0
        self.log = MagicMock()
        self.start_time = time.time()
        self._duration = 600
        self.db_cluster = SimpleNamespace(
            nemesis=[_Runner() for _ in range(runners)],
            nemesis_threads=[SimpleNamespace(is_alive=lambda: True) for _ in range(runners)],
        )
        self._during_round = during_round

    @property
    def runner(self) -> _Runner:
        return self.db_cluster.nemesis[0]

    def assemble_and_run_all_stress_cmd(self, stress_queue, stress_cmd, keyspace_num):
        self.rounds += 1
        stress_queue.append(_stress_thread(failed=self._during_round(self.rounds) == "fail"))

    def verify_stress_thread(self, thread_pool, error_handler=None):
        return not thread_pool.failed

    def _grow_cluster_to_target_size(self, stop_event=None, creating_batch=None):
        pass

    def rerun(self, first_round=None, first_round_took: float = 0) -> list:
        """Run `_rerun_stress_cmd` with a first round that ended, and return the queue of all the rounds.

        The rounds that follow end at once. `first_round_took` is how long the first one took, in seconds.
        """
        first_round = [_stress_thread()] if first_round is None else first_round
        stress_queue = list(first_round)
        self._rerun_stress_cmd(stress_queue, first_round, keyspace_num=1, round_started=time.time() - first_round_took)
        return stress_queue

    def leave_time(self, seconds: float) -> None:
        """Make the test time out in `seconds` from now."""
        self.start_time = time.time() - self.test_duration * 60 + seconds


@pytest.fixture(autouse=True)
def info_event(monkeypatch):
    event = MagicMock()
    monkeypatch.setattr("longevity_test.InfoEvent", event)
    return event


@pytest.fixture
def framework_event(monkeypatch):
    event = MagicMock()
    monkeypatch.setattr("longevity_test.TestFrameworkEvent", event)
    return event


def _messages(event: MagicMock) -> list[str]:
    return [call.kwargs["message"] for call in event.call_args_list]


def test_stress_is_run_again_until_the_minimum_is_reached(info_event, framework_event):
    # one nemesis ended during the first round, and one more ends in each round that follows
    test = _RoundsLongevityTest(minimum=3, during_round=lambda _: test.runner.finish(NemesisStatus.SUCCEEDED))
    test.runner.finish(NemesisStatus.SUCCEEDED)

    stress_queue = test.rerun()

    assert test.rounds == 2
    assert len(stress_queue) == 3  # every round stays queued, to be verified with the rest of the load
    assert _messages(info_event) == [
        "1 of the minimum of 3 nemeses were executed, running stress_cmd again",
        "2 of the minimum of 3 nemeses were executed, running stress_cmd again",
        "3 nemeses were executed, the minimum of 3 is reached",
    ]
    framework_event.assert_not_called()


def test_stress_is_not_run_again_when_the_minimum_is_reached_in_the_first_round(info_event):
    test = _RoundsLongevityTest(minimum=2)
    test.runner.finish(NemesisStatus.SUCCEEDED, NemesisStatus.SUCCEEDED, NemesisStatus.SUCCEEDED)

    test.rerun()

    assert test.rounds == 0
    assert _messages(info_event) == ["3 nemeses were executed, the minimum of 2 is reached"]


@pytest.mark.parametrize(
    "statuses_per_round, expected_rounds",
    [
        pytest.param([[NemesisStatus.SKIPPED] * 3, [NemesisStatus.SUCCEEDED] * 2], 2, id="skipped-are-not-counted"),
        pytest.param([[NemesisStatus.FAILED], [NemesisStatus.FAILED]], 2, id="failed-are-counted"),
        pytest.param([[NemesisStatus.SKIPPED, NemesisStatus.FAILED, NemesisStatus.SUCCEEDED]], 1, id="mixed"),
    ],
)
def test_only_the_nemeses_that_ran_are_counted(statuses_per_round, expected_rounds):
    test = _RoundsLongevityTest(
        minimum=2, during_round=lambda number: test.runner.finish(*statuses_per_round[number - 1])
    )

    test.rerun()

    assert test.rounds == expected_rounds


def test_the_nemeses_of_all_the_nemesis_threads_are_counted():
    test = _RoundsLongevityTest(minimum=2, runners=2)
    for runner in test.db_cluster.nemesis:
        runner.finish(NemesisStatus.SUCCEEDED)

    test.rerun()

    assert test.rounds == 0


@pytest.mark.parametrize("minimum", [0, None], ids=["zero", "unset"])
def test_without_a_minimum_stress_is_not_run_again(minimum, info_event):
    test = _RoundsLongevityTest(minimum=minimum)

    test.rerun()

    assert test.rounds == 0
    info_event.assert_not_called()


@pytest.mark.parametrize(
    "time_left, expected_rounds",
    [
        # a round of 10 minutes needs 600 seconds, and 300 more (STRESS_ROUND_MARGIN)
        pytest.param(880, 0, id="one-more-round-does-not-fit"),
        pytest.param(920, 1, id="one-more-round-fits"),
    ],
)
def test_stress_is_run_again_only_when_one_more_round_fits_in_the_test_duration(
    time_left, expected_rounds, info_event, framework_event
):
    # no nemesis ever ends: only the duration of the test stops the rounds
    test = _RoundsLongevityTest(minimum=5, during_round=lambda _: test.leave_time(100))
    test.leave_time(time_left)

    test.rerun(first_round_took=600)

    assert test.rounds == expected_rounds
    framework_event.return_value.publish.assert_called_once_with()
    assert framework_event.call_args.kwargs["severity"] == Severity.WARNING
    message = framework_event.call_args.kwargs["message"]
    assert message.startswith("Only 0 of the minimum of 5 nemeses were executed, and stress_cmd is not run again")
    if expected_rounds:  # the round that was run took no time, and even so the 100 seconds left are too few
        assert "the last round took 0 seconds, one more can take 300 seconds longer" in message
        assert _messages(info_event) == ["0 of the minimum of 5 nemeses were executed, running stress_cmd again"]
    else:
        assert "the last round took 600 seconds, one more can take 300 seconds longer" in message
        info_event.assert_not_called()


def test_stress_is_not_run_again_when_no_nemesis_thread_is_running(framework_event):
    test = _RoundsLongevityTest(minimum=5)
    test.runner.finish(NemesisStatus.SUCCEEDED)
    test.db_cluster.nemesis_threads = [SimpleNamespace(is_alive=lambda: False)]

    test.rerun()

    assert test.rounds == 0
    framework_event.return_value.publish.assert_called_once_with()
    assert framework_event.call_args.kwargs["severity"] == Severity.WARNING
    assert framework_event.call_args.kwargs["message"] == (
        "Only 1 of the minimum of 5 nemeses were executed, and stress_cmd is not run again:"
        " no nemesis thread is running"
    )


def test_stress_is_not_run_again_when_the_test_has_no_nemesis_thread(framework_event):
    test = _RoundsLongevityTest(minimum=5, runners=0)  # the nemesis stage is skipped

    test.rerun()

    assert test.rounds == 0
    framework_event.return_value.publish.assert_called_once_with()
    assert framework_event.call_args.kwargs["message"].endswith("no nemesis thread is running")


def test_a_stress_command_that_is_a_plain_thread_is_waited_for():
    # a python_thread command is a threading.Thread: it has no results_futures
    can_end, ended = threading.Event(), threading.Event()

    def run():
        can_end.wait(timeout=10)
        threading.Event().wait(0.2)
        ended.set()

    thread = threading.Thread(target=run, daemon=True)
    thread.failed = False
    thread.start()
    test = _RoundsLongevityTest(minimum=1, during_round=lambda _: test.runner.finish(NemesisStatus.SUCCEEDED))
    can_end.set()

    test.rerun(first_round=[thread])

    assert ended.is_set()  # the first round was waited for before the count was checked
    assert test.rounds == 1


def test_a_round_that_failed_is_not_run_again_for_the_minimum(info_event, framework_event):
    test = _RoundsLongevityTest(minimum=5, during_round=lambda _: "fail")

    stress_queue = test.rerun()

    assert test.rounds == 1
    assert len(stress_queue) == 2  # the failed round stays queued too, for the final verification
    framework_event.return_value.publish.assert_called_once_with()
    assert framework_event.call_args.kwargs["severity"] == Severity.ERROR
    assert framework_event.call_args.kwargs["message"] == "stress_cmd failed, so it is not run again"
    assert len(info_event.call_args_list) == 1  # no "running stress_cmd again" for the round that was not run


def test_without_a_stress_round_nothing_is_run_for_the_minimum():
    test = _RoundsLongevityTest(minimum=5)

    assert test.rerun(first_round=[]) == []
    assert test.rounds == 0


def _round_that_ends_after(event: threading.Event) -> list:
    """A stress round that ends shortly after `event` is set, so that what the event reports is done by then."""
    future = Future()

    def end():
        event.wait(timeout=10)
        threading.Event().wait(0.3)
        future.set_result(None)

    threading.Thread(target=end, daemon=True).start()
    return [SimpleNamespace(results_futures=[future], failed=False)]


def test_after_the_growth_stress_is_run_again_until_the_minimum_is_reached(info_event):
    # the growth ends during the first round; the nemeses end in the round on the full-size cluster and after it
    test = _RoundsLongevityTest(
        minimum=2, during_round=lambda number: test.runner.finish(*[NemesisStatus.SUCCEEDED] * (number - 1))
    )
    growth_returned = threading.Event()
    test._grow_cluster_to_target_size = lambda **_: growth_returned.set()
    first_round = _round_that_ends_after(growth_returned)

    test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    # the round on the full-size cluster, where no nemesis ended, then one with one nemesis, then one with two
    assert test.rounds == 3
    assert _messages(info_event) == [
        "The cluster reached its target size, running stress_cmd once more on the full-size cluster",
        "0 of the minimum of 2 nemeses were executed, running stress_cmd again",
        "1 of the minimum of 2 nemeses were executed, running stress_cmd again",
        "3 nemeses were executed, the minimum of 2 is reached",
    ]


def test_the_round_on_the_full_size_cluster_runs_even_when_the_minimum_is_reached_before_it(info_event):
    test = _RoundsLongevityTest(minimum=2)
    test.runner.finish(NemesisStatus.SUCCEEDED, NemesisStatus.SUCCEEDED)  # during the growth
    growth_returned = threading.Event()
    test._grow_cluster_to_target_size = lambda **_: growth_returned.set()
    first_round = _round_that_ends_after(growth_returned)

    test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    assert test.rounds == 1
    assert _messages(info_event) == [
        "The cluster reached its target size, running stress_cmd once more on the full-size cluster",
        "2 nemeses were executed, the minimum of 2 is reached",
    ]


def test_a_growth_failure_is_raised_and_no_round_is_run_for_the_minimum(info_event):
    test = _RoundsLongevityTest(minimum=2)
    growth_returned = threading.Event()

    def grow(**_):
        growth_returned.set()
        raise RuntimeError("add_nodes failed")

    test._grow_cluster_to_target_size = grow
    first_round = _round_that_ends_after(growth_returned)

    with pytest.raises(RuntimeError, match="add_nodes failed"):
        test._grow_cluster_under_stress(list(first_round), first_round, keyspace_num=1)

    assert test.rounds == 0
    info_event.assert_not_called()


# ------------------------------------------------------------------ where test_custom_time checks the minimum


class _FlowLongevityTest(LongevityTest):
    def __init__(self, params):  # skip the full ClusterTester.__init__
        self.params = params
        self.calls = []
        self.partitions_attrs = None
        self.validate_large_collections = False
        self.db_cluster = MagicMock()
        for name in (
            "get_nemesis_class",
            "download_artifacts_from_s3",
            "pre_create_alternator_tables",
            "run_pre_create_keyspace",
            "run_pre_create_schema",
            "kafka_configure",
            "run_prepare_write_cmd",
            "verify_stress_thread",
            "_grow_cluster_to_target_size",
        ):
            setattr(self, name, MagicMock())
        self._get_scan_operation_params = MagicMock(return_value=None)
        self._get_tombstone_gc_verification_params = MagicMock(return_value=None)

    def assemble_and_run_all_stress_cmd(self, stress_queue, stress_cmd, keyspace_num):
        stress_queue.append("stress_cmd round")

    def _run_all_stress_cmds(self, stress_queue, params):
        self.calls.append("stress_read_cmd")

    def _rerun_stress_cmd(self, stress_queue, stress_round, keyspace_num, growth=None, round_started=None):
        # the growth flow does not need the start of the first round: that round is never checked for the nemesis
        timed = round_started is not None and time.time() - round_started < 10
        self.calls.append(("rerun", list(stress_round), "growing" if growth else "not growing", timed))


@pytest.mark.parametrize(
    "overrides, expected_calls",
    [
        pytest.param(
            {"minimum_nemesis_to_execute": 3},
            # stress_read_cmd starts next to the first stress_cmd round, as it does without a minimum
            # and the start of that round is passed on, for the time that it took
            ["stress_read_cmd", ("rerun", ["stress_cmd round"], "not growing", True)],
            id="minimum",
        ),
        pytest.param({"minimum_nemesis_to_execute": 0}, ["stress_read_cmd"], id="no-minimum"),
        pytest.param(
            {"minimum_nemesis_to_execute": 3, "cluster_target_size": [4], "grow_cluster_during_stress": True},
            # the rounds for the minimum belong to the growth flow: they follow the round on the full-size cluster
            [("rerun", ["stress_cmd round"], "growing", False), "stress_read_cmd"],
            id="minimum-with-growth-under-stress",
        ),
    ],
)
def test_custom_time_runs_stress_again_for_the_minimum_once(monkeypatch, overrides, expected_calls):
    monkeypatch.setattr("longevity_test.skip_optional_stage", lambda stage: False)
    params = {
        "stress_cmd": ["cassandra-stress write duration=10m"],
        "stress_read_cmd": ["cassandra-stress read duration=10m"],
        "keyspace_num": 1,
        "nemesis_during_prepare": False,
    }
    test = _FlowLongevityTest(params | overrides)

    test.test_custom_time()

    assert test.calls == expected_calls
