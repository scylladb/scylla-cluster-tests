import pytest

from argus.common.enums import NemesisStatus
from sdcm.exceptions import KillNemesis, UnsupportedNemesis
from sdcm.sct_events import Severity


def test_execute_nemesis_success(nemesis, nemesis_runner, capsys):
    """Test execute_nemesis completes successfully."""
    nemesis_runner.execute_nemesis(nemesis)

    captured = capsys.readouterr()
    assert "Disrupting cluster" in captured.out
    nemesis_runner.cluster.check_cluster_health.assert_called_once()
    nemesis_runner.metrics_srv.event.assert_called_once_with("CustomTestNemesis")
    assert len(nemesis_runner.duration_list) == 1
    assert len(nemesis_runner.operation_log) == 1

    # Verify the DisruptionEvent was recorded correctly
    event = nemesis_runner.last_nemesis_event
    assert event is not None
    assert event.nemesis_name == "CustomTestNemesis"
    assert event.duration is not None
    assert event.duration >= 0
    assert event.severity == Severity.NORMAL
    assert event.nemesis_status == NemesisStatus.SUCCEEDED
    assert event.is_skipped is False
    assert not event.errors

    # Verify operation_log entry structure
    op = nemesis_runner.operation_log[0]
    assert op["operation"] == "CustomTestNemesis"
    assert op["subtype"] == NemesisStatus.SUCCEEDED.value
    assert "start" in op
    assert "end" in op
    assert "duration" in op
    assert op["end"] >= op["start"]
    assert op["duration"] == op["end"] - op["start"]
    assert "error" not in op
    assert "skip_reason" not in op


def test_execute_nemesis_failure(failing_nemesis, nemesis_runner):
    """Test execute_nemesis handles disruption failures."""
    nemesis_runner.execute_nemesis(failing_nemesis)

    nemesis_runner.cluster.check_cluster_health.assert_called_once()
    assert len(nemesis_runner.error_list) == 1
    assert "Intentional failure" in nemesis_runner.error_list[0]

    # Verify the DisruptionEvent captured the error
    event = nemesis_runner.last_nemesis_event
    assert event is not None
    assert event.severity == Severity.ERROR
    assert event.nemesis_status == NemesisStatus.FAILED
    assert event.is_skipped is False
    assert "Intentional failure" in event.errors_formatted
    assert event.full_traceback  # traceback should be recorded

    # Verify operation_log records the error
    op = nemesis_runner.operation_log[0]
    assert op["subtype"] == NemesisStatus.FAILED.value
    assert "error" in op
    assert "Intentional failure" in str(op["error"])


def test_execute_nemesis_skip(skipping_nemesis, nemesis_runner):
    """Test execute_nemesis handles nemesis skip."""

    with pytest.raises(UnsupportedNemesis, match="Intentional skip"):
        nemesis_runner.execute_nemesis(skipping_nemesis)

    nemesis_runner.cluster.check_cluster_health.assert_called_once()

    # Verify the DisruptionEvent was marked as skipped
    event = nemesis_runner.last_nemesis_event
    assert event is not None
    assert event.is_skipped is True
    assert event.skip_reason == "Intentional skip"
    assert event.nemesis_status == NemesisStatus.SKIPPED
    assert event.severity == Severity.NORMAL
    assert not event.errors

    # Verify operation_log records the skip
    op = nemesis_runner.operation_log[0]
    assert op["subtype"] == NemesisStatus.SKIPPED.value
    assert op["skip_reason"] == "Intentional skip"
    assert "error" not in op


def test_execute_nemesis_with_kill_nemesis_success(kill_nemesis, nemesis_runner):
    """Test execute_nemesis handles KillNemesis when no critical events."""
    nemesis_runner.tester.get_event_summary = dict

    with pytest.raises(KillNemesis) as excinfo:
        nemesis_runner.execute_nemesis(kill_nemesis)
    assert "Intentional kill" in str(excinfo.value)

    # Verify the DisruptionEvent was skipped with success message
    event = nemesis_runner.last_nemesis_event
    assert event is not None
    assert event.is_skipped is True
    assert event.skip_reason == "Killed by tearDown - test success"
    assert event.nemesis_status == NemesisStatus.SKIPPED
    assert event.severity == Severity.NORMAL
    assert not event.errors

    # Verify operation_log records the skip
    op = nemesis_runner.operation_log[0]
    assert op["subtype"] == NemesisStatus.SKIPPED.value
    assert op["skip_reason"] == "Killed by tearDown - test success"
    assert "error" not in op


def test_execute_nemesis_with_kill_nemesis_failure(kill_nemesis, nemesis_runner):
    """Test execute_nemesis handles KillNemesis when critical events exist."""
    nemesis_runner.tester.get_event_summary = lambda: {"CRITICAL": 1}

    with pytest.raises(KillNemesis) as excinfo:
        nemesis_runner.execute_nemesis(kill_nemesis)
    assert "Intentional kill" in str(excinfo.value)

    # Verify the DisruptionEvent recorded the failure
    event = nemesis_runner.last_nemesis_event
    assert event is not None
    assert event.is_skipped is False
    assert event.severity == Severity.ERROR
    assert event.nemesis_status == NemesisStatus.FAILED
    assert "Killed by tearDown - test fail" in event.errors_formatted
    assert event.full_traceback  # traceback should be recorded

    # Verify operation_log records the error
    op = nemesis_runner.operation_log[0]
    assert op["subtype"] == NemesisStatus.FAILED.value
    assert "error" in op
    assert "Killed by tearDown - test fail" in str(op["error"])


def test_execute_multiple_nemesis_in_sequence(nemesis, kill_nemesis, failing_nemesis, skipping_nemesis, nemesis_runner):
    """Test that running multiple nemesis in sequence doesn't cause interference.

    Verifies that each execution gets its own independent DisruptionEvent,
    operation_log entry, and duration record, and that a failure in one
    does not corrupt the state for subsequent executions.
    """

    # 1. Run a successful nemesis
    nemesis_runner.execute_nemesis(nemesis)

    assert len(nemesis_runner.duration_list) == 1
    assert len(nemesis_runner.operation_log) == 1
    assert len(nemesis_runner.error_list) == 0
    first_event = nemesis_runner.last_nemesis_event
    assert first_event.nemesis_status == NemesisStatus.SUCCEEDED
    # operation recorded for this run
    assert nemesis_runner.operation_log[-1]["operation"] == "CustomTestNemesis"
    assert nemesis_runner.operation_log[-1]["subtype"] == NemesisStatus.SUCCEEDED.value

    # 2. Run a failing nemesis — should not corrupt previous results
    nemesis_runner.execute_nemesis(failing_nemesis)

    assert len(nemesis_runner.duration_list) == 2
    assert len(nemesis_runner.operation_log) == 2
    assert len(nemesis_runner.error_list) == 1
    second_event = nemesis_runner.last_nemesis_event
    assert second_event.nemesis_status == NemesisStatus.FAILED
    assert second_event is not first_event  # each run gets its own event
    # operation recorded for this run
    assert nemesis_runner.operation_log[-1]["operation"] == "FailingTestNemesis"
    assert nemesis_runner.operation_log[-1]["subtype"] == NemesisStatus.FAILED.value
    assert "Intentional failure" in nemesis_runner.operation_log[-1]["error"]

    # 3. Run a skipping nemesis
    with pytest.raises(UnsupportedNemesis):
        nemesis_runner.execute_nemesis(skipping_nemesis)

    assert len(nemesis_runner.duration_list) == 3
    assert len(nemesis_runner.operation_log) == 3
    assert len(nemesis_runner.error_list) == 1  # unchanged from step 2
    third_event = nemesis_runner.last_nemesis_event
    assert third_event.nemesis_status == NemesisStatus.SKIPPED
    assert third_event is not second_event
    # operation recorded for this run
    assert nemesis_runner.operation_log[-1]["operation"] == "SkippingTestNemesis"
    assert nemesis_runner.operation_log[-1]["subtype"] == NemesisStatus.SKIPPED.value
    assert nemesis_runner.operation_log[-1]["skip_reason"] == "Intentional skip"

    # 4. Run another successful nemesis after failure and skip
    nemesis_runner.execute_nemesis(nemesis)

    assert len(nemesis_runner.duration_list) == 4
    assert len(nemesis_runner.operation_log) == 4
    assert len(nemesis_runner.error_list) == 1  # still only the one from step 2
    fourth_event = nemesis_runner.last_nemesis_event
    assert fourth_event.nemesis_status == NemesisStatus.SUCCEEDED
    assert fourth_event is not third_event
    # operation recorded for this run
    assert nemesis_runner.operation_log[-1]["operation"] == "CustomTestNemesis"
    assert nemesis_runner.operation_log[-1]["subtype"] == NemesisStatus.SUCCEEDED.value

    # Verify durations are non-negative and timestamps are monotonically ordered
    for i, op in enumerate(nemesis_runner.operation_log):
        assert op["duration"] >= 0, f"operation_log[{i}] has negative duration"
        assert op["end"] >= op["start"], f"operation_log[{i}] end before start"
    for i in range(1, len(nemesis_runner.operation_log)):
        assert nemesis_runner.operation_log[i]["start"] >= nemesis_runner.operation_log[i - 1]["start"], (
            f"operation_log[{i}] started before operation_log[{i - 1}]"
        )
