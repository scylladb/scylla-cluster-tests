"""
Unit tests for Argus submission in NemesisRunner.execute_nemesis
"""

from unittest.mock import MagicMock, ANY

import pytest

from argus.common.enums import NemesisStatus
from sdcm.exceptions import UnsupportedNemesis


@pytest.fixture
def argus_mock(nemesis_runner):
    """Helper function to set up Argus mock for a NemesisRunner."""
    argus_mock = MagicMock()
    nemesis_runner.cluster.test_config.argus_client = MagicMock(return_value=argus_mock)
    return argus_mock


def test_argus_submit_on_success(argus_mock, nemesis, nemesis_runner):
    """Test that successful nemesis submits correct data to Argus."""
    nemesis_runner.execute_nemesis(nemesis)

    # Verify submit_nemesis was called at the start
    argus_mock.submit_nemesis.assert_called_once_with(
        name="CustomTestNemesis",
        class_name="TestNemesisRunner",
        start_time=ANY,
        target_name="Node1",
        target_ip="127.0.0.1",
        target_shards=8,
    )

    # Verify finalize_nemesis was called at the end
    argus_mock.finalize_nemesis.assert_called_once_with(
        name="CustomTestNemesis",
        start_time=ANY,
        status=NemesisStatus.SUCCEEDED,
        message="",
    )


def test_argus_submit_on_failure(failing_nemesis, nemesis_runner):
    """Test that failing nemesis submits error information to Argus."""
    argus_mock = MagicMock()
    nemesis_runner.cluster.test_config.argus_client.return_value = argus_mock

    nemesis_runner.execute_nemesis(failing_nemesis)

    # Verify submit_nemesis was called
    argus_mock.submit_nemesis.assert_called_once_with(
        name="FailingTestNemesis",
        class_name="TestNemesisRunner",
        start_time=ANY,
        target_name="Node1",
        target_ip="127.0.0.1",
        target_shards=8,
    )

    # Verify finalize_nemesis was called with failure status and traceback
    finalize_args = argus_mock.finalize_nemesis.call_args
    assert finalize_args.kwargs["name"] == "FailingTestNemesis"
    assert finalize_args.kwargs["status"] == NemesisStatus.FAILED
    assert "Intentional failure" in finalize_args.kwargs["message"]
    assert "RuntimeError" in finalize_args.kwargs["message"]


def test_argus_submit_on_skip(argus_mock, skipping_nemesis, nemesis_runner):
    """Test that skipped nemesis submits skip reason to Argus."""

    with pytest.raises(UnsupportedNemesis):
        nemesis_runner.execute_nemesis(skipping_nemesis)

    # Verify submit_nemesis was called
    argus_mock.submit_nemesis.assert_called_once_with(
        name="SkippingTestNemesis",
        class_name="TestNemesisRunner",
        start_time=ANY,
        target_name="Node1",
        target_ip="127.0.0.1",
        target_shards=8,
    )

    argus_mock.finalize_nemesis.assert_called_once_with(
        status=NemesisStatus.SKIPPED, message="Intentional skip", name="SkippingTestNemesis", start_time=ANY
    )


def test_argus_submit_handles_submit_error_gracefully(nemesis, nemesis_runner):
    """Test that errors during Argus submit_nemesis don't break execution."""
    argus_mock = MagicMock()
    argus_mock.submit_nemesis.side_effect = Exception("Argus connection error")
    nemesis_runner.cluster.test_config.argus_client.return_value = argus_mock

    # Should complete successfully despite Argus error
    nemesis_runner.execute_nemesis(nemesis)

    # Verify the nemesis still completed
    assert len(nemesis_runner.duration_list) == 1
    assert len(nemesis_runner.operation_log) == 1
    event = nemesis_runner.last_nemesis_event
    assert event.nemesis_status == NemesisStatus.SUCCEEDED

    argus_mock.finalize_nemesis.assert_called_once()


def test_argus_submit_handles_finalize_error_gracefully(nemesis, nemesis_runner):
    """Test that errors during Argus finalize_nemesis don't break execution."""
    argus_mock = MagicMock()
    argus_mock.finalize_nemesis.side_effect = Exception("Argus finalize error")
    nemesis_runner.cluster.test_config.argus_client.return_value = argus_mock

    # Should complete successfully despite Argus error
    nemesis_runner.execute_nemesis(nemesis)

    # Verify the nemesis still completed
    assert len(nemesis_runner.duration_list) == 1
    assert len(nemesis_runner.operation_log) == 1
    event = nemesis_runner.last_nemesis_event
    assert event.nemesis_status == NemesisStatus.SUCCEEDED

    # Both submit and finalize should have been attempted
    argus_mock.submit_nemesis.assert_called_once()
    argus_mock.finalize_nemesis.assert_called_once()


def test_argus_submit_no_client_available(nemesis, nemesis_runner):
    """Test that nemesis executes successfully when Argus client is not available."""
    nemesis_runner.cluster.test_config.argus_client.side_effect = Exception("No Argus client")

    # Should complete successfully without Argus
    nemesis_runner.execute_nemesis(nemesis)

    # Verify the nemesis still completed
    assert len(nemesis_runner.duration_list) == 1
    assert len(nemesis_runner.operation_log) == 1
    event = nemesis_runner.last_nemesis_event
    assert event.nemesis_status == NemesisStatus.SUCCEEDED


def test_argus_submit_parameters_consistency_across_calls(nemesis, nemesis_runner):
    """Test that submit_nemesis and finalize_nemesis use consistent parameters."""
    argus_mock = MagicMock()
    nemesis_runner.cluster.test_config.argus_client.return_value = argus_mock

    nemesis_runner.execute_nemesis(nemesis)

    submit_args = argus_mock.submit_nemesis.call_args.kwargs
    finalize_args = argus_mock.finalize_nemesis.call_args.kwargs

    # Verify name is consistent between submit and finalize
    assert submit_args["name"] == finalize_args["name"]
    # start_time is int in submit but float in finalize - verify they're close
    assert int(submit_args["start_time"]) == int(finalize_args["start_time"])


def test_argus_submit_multiple_nemesis_separate_entries(nemesis, failing_nemesis, nemesis_runner):
    """Test that multiple nemesis executions create separate Argus entries."""
    argus_mock = MagicMock()
    nemesis_runner.cluster.test_config.argus_client.return_value = argus_mock

    # Run two nemesis operations
    nemesis_runner.execute_nemesis(nemesis)
    nemesis_runner.execute_nemesis(failing_nemesis)

    # Verify submit_nemesis was called twice
    assert argus_mock.submit_nemesis.call_count == 2

    # Verify finalize_nemesis was called twice
    assert argus_mock.finalize_nemesis.call_count == 2

    # Verify each call had different start times (or at least recorded)
    submit_calls = argus_mock.submit_nemesis.call_args_list
    # Times might be the same if execution is very fast, just verify both were called
    assert len(submit_calls) == 2

    # Verify first was success, second was failure
    finalize_calls = argus_mock.finalize_nemesis.call_args_list
    assert finalize_calls[0].kwargs["status"] == NemesisStatus.SUCCEEDED
    assert finalize_calls[1].kwargs["status"] == NemesisStatus.FAILED


def test_argus_submit_captures_target_node_info(nemesis, nemesis_runner):
    """Test that Argus submission captures target node information."""
    argus_mock = MagicMock()
    nemesis_runner.cluster.test_config.argus_client.return_value = argus_mock

    nemesis_runner.execute_nemesis(nemesis)

    # Verify all required fields are passed to submit_nemesis
    argus_mock.submit_nemesis.assert_called_once_with(
        name="CustomTestNemesis",
        class_name="TestNemesisRunner",
        start_time=ANY,
        target_name="Node1",
        target_ip="127.0.0.1",
        target_shards=8,
    )
