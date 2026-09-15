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

"""collect-logs on a local run: no sct-runner, and usually no Argus registration."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import sct

TEST_ID = "a6e16e4b-ebdc-4d08-8fb9-7c58f9b78f5b"

# is_minicloud_active() reads these from the environment, so any of them left over from a
# minishell or a run-minicloud-test.sh run would send collect_logs down the minicloud branch,
# which shells out to `docker logs` / `docker inspect` on the host. A unit test must not
# depend on what the developer happened to export.
MINICLOUD_ENV_VARS = ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL")


@pytest.fixture(autouse=True)
def no_minicloud_env(monkeypatch):
    for env_var in MINICLOUD_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _config(enable_argus: bool) -> MagicMock:
    """A stand-in SCTConfiguration that answers only what collect_logs asks of it."""
    config = MagicMock()
    config.get.side_effect = lambda key, *_: {"test_id": None, "enable_argus": enable_argus}.get(key)
    return config


def _collector(test_id: str | None) -> MagicMock:
    collector = MagicMock()
    collector.test_id = test_id
    collector.storage_dir = None
    collector.run.return_value = ({}, None)
    return collector


@patch("sct.update_sct_runner_tags")
@patch("sct.store_logs_in_argus")
@patch("sct.Collector")
@patch("sct.SCTConfiguration")
@patch("sct.add_file_logger")
def test_test_id_is_read_from_the_logdir_when_not_given(
    _logger, mock_config, mock_collector_cls, _argus, _tags, runner, tmp_path
):
    """--logdir alone has to be enough, as it is for the Jenkins step.

    The pipeline exports SCT_TEST_ID build-wide (vars/runCollectLogs.groovy), so --logdir
    alone works there. A local run has no such export, and without an id Collector leaves
    collector.test_id None - which used to surface much later as update_sct_runner_tags()
    raising about a runner a local run never had, after collection had already done its work.
    """
    (tmp_path / "test_id").write_text(TEST_ID, encoding="utf-8")
    mock_config.return_value = _config(enable_argus=False)
    mock_collector_cls.return_value = _collector(TEST_ID)

    result = runner.invoke(sct.collect_logs, ["--logdir", str(tmp_path), "--backend", "aws"])

    assert result.exit_code == 0, result.output
    assert mock_collector_cls.call_args.kwargs["test_id"] == TEST_ID


@patch("sct.update_sct_runner_tags")
@patch("sct.store_logs_in_argus")
@patch("sct.Collector")
@patch("sct.SCTConfiguration")
@patch("sct.add_file_logger")
def test_argus_submission_is_skipped_when_argus_is_disabled(
    _logger, mock_config, mock_collector_cls, mock_store, _tags, runner, tmp_path
):
    """A run with enable_argus off has no SCTTestRun to attach logs to.

    Submitting anyway ended an otherwise clean local run with a full traceback
    ("No SCTTestRun found matching ..."), which reads like a failure and is not one.
    """
    (tmp_path / "test_id").write_text(TEST_ID, encoding="utf-8")
    mock_config.return_value = _config(enable_argus=False)
    mock_collector_cls.return_value = _collector(TEST_ID)

    result = runner.invoke(sct.collect_logs, ["--logdir", str(tmp_path), "--backend", "aws"])

    assert result.exit_code == 0, result.output
    mock_store.assert_not_called()


@patch("sct.update_sct_runner_tags")
@patch("sct.store_logs_in_argus")
@patch("sct.Collector")
@patch("sct.SCTConfiguration")
@patch("sct.add_file_logger")
def test_argus_submission_still_happens_when_argus_is_enabled(
    _logger, mock_config, mock_collector_cls, mock_store, _tags, runner, tmp_path
):
    """The CI path must keep working: every pipeline run registers with Argus."""
    (tmp_path / "test_id").write_text(TEST_ID, encoding="utf-8")
    mock_config.return_value = _config(enable_argus=True)
    mock_collector_cls.return_value = _collector(TEST_ID)

    result = runner.invoke(sct.collect_logs, ["--logdir", str(tmp_path), "--backend", "aws"])

    assert result.exit_code == 0, result.output
    mock_store.assert_called_once()
