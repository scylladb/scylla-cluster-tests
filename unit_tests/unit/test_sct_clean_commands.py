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
# Copyright (c) 2025 ScyllaDB

from unittest.mock import ANY, patch

import pytest
from click.testing import CliRunner

import sct
from sdcm.sct_config import SCTConfiguration


@pytest.fixture()
def cli_runner():
    """CliRunner that invokes the sct click commands."""
    return CliRunner()


@pytest.fixture()
def cloud_env(monkeypatch):
    """Set the region variables clean-resources reads from the environment."""
    for name, value in {
        "SCT_REGION_NAME": "us-east-1",
        "SCT_GCE_DATACENTER": "us-central1-a",
        "SCT_AZURE_REGION_NAME": "eastus",
    }.items():
        monkeypatch.setenv(name, value)


@patch("sct.clean_cloud_resources")
@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_resources_with_clean_runners_flag_and_user(
    mock_logger, mock_clean_runners, mock_clean_resources, cli_runner, cloud_env
):
    """clean-resources with --clean-runners and --user passes the user and force=True to clean_sct_runners."""
    mock_clean_resources.return_value = None
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(sct.clean_resources, ["--user", "test.user", "--clean-runners", "--dry-run"])

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_status="", test_runner_ip=None, backend=None, user="test.user", test_id=None, dry_run=True, force=True
    )
    mock_clean_resources.assert_called_once_with(
        {"RunByUser": "test.user", "CreatedBy": "SCT"}, config=ANY, dry_run=True
    )
    assert isinstance(mock_clean_resources.call_args.kwargs["config"], SCTConfiguration)


@patch("sct.clean_cloud_resources")
@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_resources_with_clean_runners_flag_and_test_id(
    mock_logger, mock_clean_runners, mock_clean_resources, cli_runner, cloud_env
):
    """clean-resources with --clean-runners and --test-id passes the test id and backend to clean_sct_runners."""
    mock_clean_resources.return_value = None
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(sct.clean_resources, ["--test-id", "test-123", "--clean-runners", "--backend", "aws"])

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_status="", test_runner_ip=None, backend="aws", user=None, test_id="test-123", dry_run=False, force=True
    )


@patch("sct.add_file_logger")
def test_clean_resources_clean_runners_requires_user_or_test_id(mock_logger, cli_runner):
    """clean-resources --clean-runners without --user or --test-id exits 1 with an error."""
    result = cli_runner.invoke(sct.clean_resources, ["--clean-runners"])
    assert result.exit_code == 1
    assert "--clean-runners requires --user and/or --test-id" in result.output


@patch("sct.clean_cloud_resources")
@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_resources_with_both_user_and_test_id(
    mock_logger, mock_clean_runners, mock_clean_resources, cli_runner, cloud_env
):
    """clean-resources with --user and --test-id forwards both to clean_sct_runners."""
    mock_clean_resources.return_value = None
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(
        sct.clean_resources,
        ["--user", "test.user", "--test-id", "test-123", "--clean-runners", "--backend", "gce"],
    )

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_status="",
        test_runner_ip=None,
        backend="gce",
        user="test.user",
        test_id="test-123",
        dry_run=False,
        force=True,
    )


@patch("sct.clean_sct_runners")
@patch("sct.clean_cloud_resources")
@patch("sct.add_file_logger")
def test_clean_resources_without_clean_runners_flag(
    mock_logger, mock_clean_resources, mock_clean_runners, cli_runner, cloud_env
):
    """clean-resources without --clean-runners never calls clean_sct_runners."""
    mock_clean_resources.return_value = None

    result = cli_runner.invoke(sct.clean_resources, ["--user", "test.user"])

    assert result.exit_code == 0
    mock_clean_resources.assert_called_once_with(
        {"RunByUser": "test.user", "CreatedBy": "SCT"}, config=ANY, dry_run=False
    )
    assert isinstance(mock_clean_resources.call_args.kwargs["config"], SCTConfiguration)
    mock_clean_runners.assert_not_called()


@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_runner_instances_with_user(mock_logger, mock_clean_runners, cli_runner):
    """clean-runner-instances --user forwards the user to clean_sct_runners."""
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(sct.clean_runner_instances, ["--user", "test.user"])

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_runner_ip="", test_status=None, backend=None, user="test.user", test_id=(), dry_run=False, force=False
    )


@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_runner_instances_with_test_id(mock_logger, mock_clean_runners, cli_runner):
    """clean-runner-instances --test-id forwards the test id as a tuple."""
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(sct.clean_runner_instances, ["--test-id", "test-123"])

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_runner_ip="",
        test_status=None,
        backend=None,
        user=None,
        test_id=("test-123",),
        dry_run=False,
        force=False,
    )


@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_runner_instances_with_runner_ip(mock_logger, mock_clean_runners, cli_runner):
    """clean-runner-instances --runner-ip forwards the runner IP to clean_sct_runners."""
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(sct.clean_runner_instances, ["--runner-ip", "1.2.3.4"])

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_runner_ip="1.2.3.4", test_status=None, backend=None, user=None, test_id=(), dry_run=False, force=False
    )


@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_runner_instances_force(mock_logger, mock_clean_runners, cli_runner):
    """clean-runner-instances --force sets force=True."""
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(sct.clean_runner_instances, ["--runner-ip", "1.2.3.4", "--force"])

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_runner_ip="1.2.3.4", test_status=None, backend=None, user=None, test_id=(), dry_run=False, force=True
    )


@patch("sct.clean_sct_runners")
@patch("sct.add_file_logger")
def test_clean_runner_instances_all_options(mock_logger, mock_clean_runners, cli_runner):
    """clean-runner-instances with every option forwards each value to clean_sct_runners."""
    mock_clean_runners.return_value = None

    result = cli_runner.invoke(
        sct.clean_runner_instances,
        [
            "--runner-ip",
            "1.2.3.4",
            "--test-status",
            "timeout",
            "--backend",
            "gce",
            "--user",
            "test.user",
            "--test-id",
            "test-123",
            "--dry-run",
            "--force",
        ],
    )

    assert result.exit_code == 0
    mock_clean_runners.assert_called_once_with(
        test_runner_ip="1.2.3.4",
        test_status="timeout",
        backend="gce",
        user="test.user",
        test_id=("test-123",),
        dry_run=True,
        force=True,
    )
