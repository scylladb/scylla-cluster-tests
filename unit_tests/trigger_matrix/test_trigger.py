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

import logging
from unittest.mock import MagicMock, patch

import jenkins as jenkins_lib
import pytest

from sdcm.utils.trigger_matrix import JenkinsTriggerError, trigger_jenkins_job, trigger_matrix


def test_dry_run_produces_output(sample_matrix_yaml, caplog):
    with caplog.at_level(logging.INFO):
        results = trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
            dry_run=True,
        )

    assert len(results["triggered"]) == 4
    assert len(results["failed"]) == 0
    assert any("[DRY-RUN]" in record.message for record in caplog.records)


def test_dry_run_with_label_filter(sample_matrix_yaml):
    results = trigger_matrix(
        matrix_file=str(sample_matrix_yaml),
        scylla_version="2025.4",
        labels_selector="weekly",
        dry_run=True,
    )
    assert len(results["triggered"]) == 2


def test_dry_run_with_backend_filter(sample_matrix_yaml):
    results = trigger_matrix(
        matrix_file=str(sample_matrix_yaml),
        scylla_version="2025.4",
        backend="aws",
        dry_run=True,
    )
    assert len(results["triggered"]) == 2


def test_dry_run_version_exclusion(sample_matrix_yaml):
    results = trigger_matrix(
        matrix_file=str(sample_matrix_yaml),
        scylla_version="master:latest",
        dry_run=True,
    )
    assert len(results["triggered"]) == 3


@patch("sdcm.utils.trigger_matrix.trigger_jenkins_job")
def test_full_flow_with_mocked_api(mock_trigger, sample_matrix_yaml):
    mock_trigger.return_value = True
    results = trigger_matrix(
        matrix_file=str(sample_matrix_yaml),
        scylla_version="2025.4",
    )
    assert mock_trigger.call_count == 4
    assert len(results["triggered"]) == 4


@patch("sdcm.utils.trigger_matrix.trigger_jenkins_job")
def test_failed_jobs_reported(mock_trigger, sample_matrix_yaml):
    mock_trigger.return_value = False
    results = trigger_matrix(
        matrix_file=str(sample_matrix_yaml),
        scylla_version="2025.4",
        labels_selector="weekly",
    )
    assert len(results["failed"]) == 2
    assert len(results["triggered"]) == 0


def test_missing_jenkins_url_raises(monkeypatch):
    monkeypatch.delenv("JENKINS_URL", raising=False)
    monkeypatch.delenv("JENKINS_API_TOKEN", raising=False)
    with patch("sdcm.keystore.KeyStore.get_json", side_effect=Exception("no keystore")):
        with pytest.raises(JenkinsTriggerError, match="Jenkins URL not set"):
            trigger_jenkins_job("test-job", {}, dry_run=False)


def test_missing_jenkins_token_raises(monkeypatch):
    monkeypatch.setenv("JENKINS_URL", "https://jenkins.example.com")
    monkeypatch.delenv("JENKINS_API_TOKEN", raising=False)
    with patch("sdcm.keystore.KeyStore.get_json", side_effect=Exception("no keystore")):
        with pytest.raises(JenkinsTriggerError, match="Jenkins API token not set"):
            trigger_jenkins_job("test-job", {}, dry_run=False)


@patch("sdcm.utils.trigger_matrix.time.sleep")
def test_jenkins_exception_returns_false(mock_sleep, monkeypatch):
    monkeypatch.setenv("JENKINS_URL", "https://jenkins.example.com")
    monkeypatch.setenv("JENKINS_API_TOKEN", "fake-token")

    mock_client = MagicMock()
    mock_client.build_job.side_effect = jenkins_lib.JenkinsException("Not Found")
    with patch(
        "sdcm.utils.trigger_matrix._get_jenkins_client", return_value=(mock_client, "https://jenkins.example.com")
    ):
        result = trigger_jenkins_job("test-job", {}, dry_run=False)
    assert result is False


@patch("sdcm.utils.trigger_matrix.time.sleep")
def test_jenkins_exception_retries_then_fails(mock_sleep, monkeypatch):
    monkeypatch.setenv("JENKINS_URL", "https://jenkins.example.com")
    monkeypatch.setenv("JENKINS_API_TOKEN", "fake-token")

    mock_client = MagicMock()
    mock_client.build_job.side_effect = jenkins_lib.JenkinsException("Service Unavailable")
    with patch(
        "sdcm.utils.trigger_matrix._get_jenkins_client", return_value=(mock_client, "https://jenkins.example.com")
    ):
        result = trigger_jenkins_job("test-job", {}, dry_run=False)
    assert result is False
    assert mock_client.build_job.call_count == 3


@patch("sdcm.utils.trigger_matrix.time.sleep")
def test_jenkins_exception_then_success(mock_sleep, monkeypatch):
    monkeypatch.setenv("JENKINS_URL", "https://jenkins.example.com")
    monkeypatch.setenv("JENKINS_API_TOKEN", "fake-token")

    mock_client = MagicMock()
    mock_client.build_job.side_effect = [jenkins_lib.JenkinsException("fail"), 42]
    with patch(
        "sdcm.utils.trigger_matrix._get_jenkins_client", return_value=(mock_client, "https://jenkins.example.com")
    ):
        result = trigger_jenkins_job("test-job", {}, dry_run=False)
    assert result is True
    assert mock_client.build_job.call_count == 2


def test_zero_match_logs_warning(sample_matrix_yaml, caplog):
    with caplog.at_level(logging.WARNING):
        trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
            labels_selector="nonexistent-label",
            dry_run=True,
        )
    assert any("No jobs matched" in record.message for record in caplog.records)


def test_unknown_skip_jobs_logs_warning(sample_matrix_yaml, caplog):
    with caplog.at_level(logging.WARNING):
        trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
            skip_jobs="nonexistent-job,another-fake",
            dry_run=True,
        )
    assert any("not found in matrix" in record.message for record in caplog.records)
