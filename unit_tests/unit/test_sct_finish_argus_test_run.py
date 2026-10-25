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

"""finish-argus-test-run: an ABORTED build with infra errors in its events becomes TEST_ERROR."""

from unittest.mock import MagicMock, patch

import pytest
from argus.common.enums import TestStatus
from click.testing import CliRunner

import sct
from sdcm.sct_events.events_device import EVENTS_LOG_DIR

TEST_ID = "a6e16e4b-ebdc-4d08-8fb9-7c58f9b78f5b"
SPOT_EVENT = "2026-01-01 10:00:00.000: (SpotTerminationEvent Severity.CRITICAL) node=n1 message=terminated"
UNRELATED_EVENT = "2026-01-01 10:00:00.000: (TestFrameworkEvent Severity.ERROR) source=Test.test_run() message=boom"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def test_config() -> MagicMock:
    config = MagicMock()
    config.base_logdir.return_value = "/base/logdir"
    with (
        patch("sct.SCTConfiguration") as sct_configuration,
        patch("sct.get_test_config", return_value=config),
    ):
        sct_configuration.return_value.get.side_effect = lambda key, *_: {"test_id": TEST_ID}.get(key)
        yield config


def _make_testrun_dir(tmp_path, critical_events: list[str], error_events: list[str]):
    events_dir = tmp_path / EVENTS_LOG_DIR
    events_dir.mkdir()
    for severity in ("critical", "error", "warning", "normal", "debug"):
        events = {"critical": critical_events, "error": error_events}.get(severity, [])
        (events_dir / f"{severity}.log").write_text("\n".join(events) + "\n")
    return tmp_path


def _run_finish(runner, *extra_args):
    return runner.invoke(sct.finish_argus_test_run, ["--jenkins-status", "ABORTED", *extra_args])


@pytest.mark.parametrize(
    "critical_events, error_events, expected_status",
    [
        pytest.param([SPOT_EVENT], [], TestStatus.TEST_ERROR, id="spot-termination-gives-test-error"),
        pytest.param([], [UNRELATED_EVENT], TestStatus.ABORTED, id="no-infra-error-stays-aborted"),
    ],
)
def test_aborted_status_from_events(runner, test_config, tmp_path, critical_events, error_events, expected_status):
    testrun_dir = _make_testrun_dir(tmp_path, critical_events, error_events)

    with patch("sct.get_testrun_dir", return_value=str(testrun_dir)) as get_testrun_dir:
        result = _run_finish(runner, "--logdir", "/custom/logdir")

    assert result.exit_code == 0, result.output
    get_testrun_dir.assert_called_once_with("/custom/logdir", TEST_ID)
    test_config.argus_client().set_sct_run_status.assert_called_once_with(expected_status)
    test_config.argus_client().finalize_sct_run.assert_called_once()


def test_aborted_without_testrun_dir_stays_aborted(runner, test_config) -> None:
    with patch("sct.get_testrun_dir", return_value=None) as get_testrun_dir:
        result = _run_finish(runner)

    assert result.exit_code == 0, result.output
    get_testrun_dir.assert_called_once_with("/base/logdir", TEST_ID)
    test_config.argus_client().set_sct_run_status.assert_called_once_with(TestStatus.ABORTED)
    test_config.argus_client().finalize_sct_run.assert_called_once()
