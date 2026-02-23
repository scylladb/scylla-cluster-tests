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

"""Pytest tests for YcsbVersionReporter parsing and behavior."""

from unittest.mock import MagicMock

import pytest

from sdcm.reporting.tooling_reporter import YcsbVersionReporter


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
    "jar_name,expected_version",
    [
        ("core-0.17.0.jar\n", "0.17.0"),
        ("core-1.0.0.jar\n", "1.0.0"),
        ("ycsb-core-0.17.0.jar\n", "0.17.0"),
        ("core-0.17.0-SNAPSHOT.jar\n", "0.17.0-SNAPSHOT"),
        ("  core-0.18.0.jar  \n", "0.18.0"),
    ],
)
def test_parsing_version_from_jar_names(make_runner, jar_name, expected_version):
    runner = make_runner(jar_name)
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.version == expected_version


def test_parsing_empty_output_returns_fallback(make_runner):
    runner = make_runner("")
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.version == "#FAILED_CHECK_LOGS"


def test_parsing_non_versioned_jar_returns_fallback(make_runner):
    runner = make_runner("some-library.jar\n")
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.version == "#FAILED_CHECK_LOGS"


def test_version_query_targets_ycsb_lib(make_runner):
    runner = make_runner("core-0.17.0.jar\n")
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    runner.run.assert_called_once()
    (cmd,) = runner.run.call_args.args
    assert YcsbVersionReporter._YCSB_HOME in cmd
    assert "core-*.jar" in cmd


def test_tool_name_is_ycsb():
    assert YcsbVersionReporter.TOOL_NAME == "ycsb"
