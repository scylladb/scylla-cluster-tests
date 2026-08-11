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

"""SCT-803 follow-up: `sct.py` CLI commands that can exercise ParallelObject/nemesis
code paths (which arm the process-global hard-exit flag) must route their exit
through `exit_process()`, not raw `sys.exit()`, or an armed hard exit falls through
to the untimed interpreter-shutdown join.
"""

from unittest.mock import Mock, patch

from click.testing import CliRunner

import sct


def test_unit_tests_command_routes_through_exit_process():
    runner = CliRunner()
    with (
        patch("sct.pytest.main", return_value=0) as mock_pytest_main,
        patch("sct.exit_process") as mock_exit_process,
    ):
        result = runner.invoke(sct.unit_tests, [])

    assert result.exit_code == 0, result.output
    mock_pytest_main.assert_called_once()
    mock_exit_process.assert_called_once_with(0)


def test_integration_tests_command_routes_through_exit_process():
    runner = CliRunner()
    with (
        patch("sct.get_test_config"),
        patch("sct.add_file_logger"),
        patch("sct.running_in_podman", return_value=True),
        patch("sct.pytest.main", return_value=0) as mock_pytest_main,
        patch("sct.exit_process") as mock_exit_process,
    ):
        result = runner.invoke(sct.integration_tests, [])

    assert result.exit_code == 0, result.output
    mock_pytest_main.assert_called_once()
    mock_exit_process.assert_called_once_with(0)


def test_run_pytest_calls_exit_process_when_reporting_raises(tmp_path):
    """A failure in the Argus/JUnit reporting block must not prevent
    `exit_process()` from being called: otherwise an already-armed hard exit
    would be stuck behind the propagating exception."""
    runner = CliRunner()
    junit_file = tmp_path / "junit.xml"
    junit_file.write_text("<testsuite></testsuite>")

    mock_test_config = Mock()
    mock_test_config.logdir.return_value = str(tmp_path)
    mock_test_config.init_argus_client.side_effect = RuntimeError("argus is down")

    with (
        patch("sct.get_test_config", return_value=mock_test_config),
        patch("sct.SCTConfiguration"),
        patch("sct.pytest.main", return_value=0) as mock_pytest_main,
        patch("sct.exit_process") as mock_exit_process,
    ):
        result = runner.invoke(sct.run_pytest, ["some_test.py"])

    assert result.exit_code == 0, result.output
    mock_pytest_main.assert_called_once()
    mock_test_config.init_argus_client.assert_called_once()
    mock_exit_process.assert_called_once_with(0)
