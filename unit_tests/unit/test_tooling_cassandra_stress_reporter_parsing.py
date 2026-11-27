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

"""Pytest tests for CassandraStressVersionReporter parsing and behavior."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.reporting.tooling_reporter import CassandraStressVersionReporter


@pytest.fixture
def make_runner():
    """Factory fixture to create a mocked runner with provided stdout."""

    def _make(stdout: str) -> MagicMock:
        mock_runner = MagicMock()
        mock_output = MagicMock()
        mock_output.stdout = stdout
        mock_runner.run.return_value = mock_output
        return mock_runner

    return _make


@pytest.mark.parametrize(
    "is_driver_4x,expected_additional",
    [
        (True, "java-driver-4x: 4.19.0.1"),
        (False, "java-driver: 3.11.5.11"),
    ],
)
def test_parsing_reports_driver_based_on_flag(make_runner, is_driver_4x, expected_additional):
    """Parses the c-s version and reports the 4.x or 3.x java-driver version per is_driver_4x."""
    stdout = "Version: 3.20.1\nscylla-java-driver: 3.11.5.11\nscylla-java-driver-4x: 4.19.0.1\n"
    runner = make_runner(stdout)
    reporter = CassandraStressVersionReporter(
        runner=runner, command_prefix="", argus_client=None, is_driver_4x=is_driver_4x
    )

    reporter._collect_version_info()

    assert reporter.version == "3.20.1"
    assert reporter.additional_data == expected_additional
    runner.run.assert_called_once_with(" cassandra-stress version")


def test_parsing_no_driver_info(make_runner):
    """Parses the c-s version and leaves additional_data unset when no driver line is printed."""
    runner = make_runner("Version: 3.12.0-SNAPSHOT\n")
    reporter = CassandraStressVersionReporter(runner=runner, command_prefix="", argus_client=None, is_driver_4x=False)

    reporter._collect_version_info()

    assert reporter.version == "3.12.0-SNAPSHOT"
    assert reporter.additional_data is None


def test_parsing_malformed_output(make_runner):
    """Extracts the version line and ignores the lines without a key: value format."""
    stdout = "Some random text\nNo colon here\nVersion: 3.12.0-SNAPSHOT\nAnother line without proper format\n"
    runner = make_runner(stdout)
    reporter = CassandraStressVersionReporter(runner=runner, command_prefix="", argus_client=None, is_driver_4x=False)

    reporter._collect_version_info()

    assert reporter.version == "3.12.0-SNAPSHOT"
    assert reporter.additional_data is None


def test_parsing_missing_version_reports_fallback(make_runner):
    """Reports #FAILED_CHECK_LOGS as the version when it is missing, and still the driver version."""
    runner = make_runner("scylla-java-driver: 3.11.5.11\n")
    reporter = CassandraStressVersionReporter(runner=runner, command_prefix="", argus_client=None, is_driver_4x=False)

    reporter._collect_version_info()

    assert reporter.version == "#FAILED_CHECK_LOGS"
    assert reporter.additional_data == "java-driver: 3.11.5.11"


def test_command_prefix_is_used_in_command(make_runner):
    """Runs cassandra-stress version behind the given command prefix."""
    stdout = "Version: 3.20.1\nscylla-java-driver: 3.11.5.11\nscylla-java-driver-4x: 4.19.0.1\n"
    runner = make_runner(stdout)
    reporter = CassandraStressVersionReporter(
        runner=runner,
        command_prefix="docker exec my-container",
        argus_client=None,
        is_driver_4x=True,
    )

    reporter._collect_version_info()

    runner.run.assert_called_once_with("docker exec my-container cassandra-stress version")


@patch("sdcm.reporting.tooling_reporter.CassandraStressJavaDriverVersionReporter")
def test_calls_java_driver_reporter_for_v4(mock_driver_reporter_class, make_runner):
    """Reports the 4.x java-driver version through a separate CassandraStressJavaDriverVersionReporter."""
    stdout = "Version: 3.20.1\nscylla-java-driver: 3.11.5.11\nscylla-java-driver-4x: 4.19.0.1\n"
    runner = make_runner(stdout)
    mock_instance = MagicMock()
    mock_driver_reporter_class.return_value = mock_instance

    reporter = CassandraStressVersionReporter(runner=runner, command_prefix="", argus_client=None, is_driver_4x=True)

    reporter._collect_version_info()

    mock_driver_reporter_class.assert_called_once_with(
        driver_version="4.19.0.1", command_prefix=None, runner=None, argus_client=None
    )
    mock_instance.report.assert_called_once()
