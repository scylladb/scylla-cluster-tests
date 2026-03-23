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

import json
from unittest.mock import MagicMock, patch

import pytest

from sdcm.reporting.tooling_reporter import (
    YcsbVersionReporter,
    YcsbAwsSdkVersionReporter,
    YcsbScyllaDriverVersionReporter,
)


SAMPLE_VERSION_JSON = json.dumps(
    {
        "version": "1.3.0-SNAPSHOT",
        "git_hash": "4c50691",
        "git_tag": "",
        "build_date": "2026-02-26T22:28:26+0100",
        "aws_sdk_version": "2.0.0",
        "scylla_driver_version": "3.10.2",
    }
)


@pytest.fixture
def make_runner():
    def _make(stdout: str) -> MagicMock:
        mock_runner = MagicMock()
        mock_output = MagicMock()
        mock_output.stdout = stdout
        mock_runner.run.return_value = mock_output
        return mock_runner

    return _make


def test_version_parsed_from_json(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.version == "1.3.0-SNAPSHOT"


def test_build_date_parsed_from_json(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.date == "2026-02-26T22:28:26+0100"


def test_git_hash_used_as_revision_id(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.revision_id == "4c50691"


def test_version_command_targets_ycsb_binary(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    runner.run.assert_called_once()
    (cmd,) = runner.run.call_args.args
    assert f"{YcsbVersionReporter._YCSB_HOME}/bin/ycsb" in cmd
    assert "--version" in cmd


def test_missing_version_key_falls_back_to_marker(make_runner):
    runner = make_runner(json.dumps({"git_hash": "abc", "build_date": "2026-01-01"}))
    reporter = YcsbVersionReporter(runner=runner, command_prefix="", argus_client=None)

    reporter._collect_version_info()

    assert reporter.version == "#FAILED_CHECK_LOGS"


def test_dynamodb_workload_reports_aws_sdk_version(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run dynamodb -P workloads/workloada -threads 5",
    )

    with patch.object(YcsbAwsSdkVersionReporter, "report") as mock_report:
        reporter._collect_version_info()

    mock_report.assert_called_once()


def test_dynamodb_aws_sdk_reporter_receives_correct_version(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run dynamodb -P workloads/workloada",
    )

    created_reporters = []
    original_init = YcsbAwsSdkVersionReporter.__init__

    def capture_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created_reporters.append(self)

    with (
        patch.object(YcsbAwsSdkVersionReporter, "__init__", capture_init),
        patch.object(YcsbAwsSdkVersionReporter, "report"),
    ):
        reporter._collect_version_info()

    assert len(created_reporters) == 1
    assert created_reporters[0].version == "2.0.0"


def test_scylla_workload_reports_scylla_driver_version(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run scylla -P workloads/workloada -threads 5",
    )

    with patch.object(YcsbScyllaDriverVersionReporter, "report") as mock_report:
        reporter._collect_version_info()

    mock_report.assert_called_once()


def test_scylla_driver_reporter_receives_correct_version(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run scylla -P workloads/workloada",
    )

    created_reporters = []
    original_init = YcsbScyllaDriverVersionReporter.__init__

    def capture_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created_reporters.append(self)

    with (
        patch.object(YcsbScyllaDriverVersionReporter, "__init__", capture_init),
        patch.object(YcsbScyllaDriverVersionReporter, "report"),
    ):
        reporter._collect_version_info()

    assert len(created_reporters) == 1
    assert created_reporters[0].version == "3.10.2"


def test_unrecognized_workload_reports_no_sub_reporters(make_runner):
    runner = make_runner(SAMPLE_VERSION_JSON)
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run mongodb -P workloads/workloada",
    )

    with (
        patch.object(YcsbAwsSdkVersionReporter, "report") as aws_report,
        patch.object(YcsbScyllaDriverVersionReporter, "report") as scylla_report,
    ):
        reporter._collect_version_info()

    aws_report.assert_not_called()
    scylla_report.assert_not_called()


def test_dynamodb_workload_without_aws_sdk_key_does_not_crash(make_runner):
    runner = make_runner(json.dumps({"version": "1.0.0", "git_hash": "abc", "build_date": "2026-01-01"}))
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run dynamodb -P workloads/workloada",
    )

    reporter._collect_version_info()

    assert reporter.version == "1.0.0"


def test_scylla_workload_without_driver_version_key_does_not_crash(make_runner):
    runner = make_runner(json.dumps({"version": "1.0.0", "git_hash": "abc", "build_date": "2026-01-01"}))
    reporter = YcsbVersionReporter(
        runner=runner,
        command_prefix="",
        argus_client=None,
        stress_cmd="bin/ycsb run scylla -P workloads/workloada",
    )

    reporter._collect_version_info()

    assert reporter.version == "1.0.0"


def test_aws_sdk_reporter_stores_version():
    reporter = YcsbAwsSdkVersionReporter(version="2.0.0")

    assert reporter.version == "2.0.0"


def test_scylla_driver_reporter_stores_version():
    reporter = YcsbScyllaDriverVersionReporter(version="3.10.2")

    assert reporter.version == "3.10.2"


def test_tool_name_is_ycsb():
    assert YcsbVersionReporter.TOOL_NAME == "ycsb"


def test_aws_sdk_reporter_tool_name():
    assert YcsbAwsSdkVersionReporter.TOOL_NAME == "ycsb-aws-sdk"


def test_scylla_driver_reporter_tool_name():
    assert YcsbScyllaDriverVersionReporter.TOOL_NAME == "ycsb-scylla-driver"
