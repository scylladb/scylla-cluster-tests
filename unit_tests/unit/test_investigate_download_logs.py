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

"""Unit tests for the 'hydra investigate download-logs' CLI command."""

import datetime
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from sct import download_logs


@pytest.fixture
def mock_logs():
    """Sample log entries returned by list_logs_by_test_id."""
    return [
        {
            "file_path": "test-id-123/20250526_120000/sct-runner.tar.zst",
            "type": "sct",
            "link": "https://argus.scylladb.com/api/v1/tests/test-id-123/log/sct-runner.tar.zst/download",
            "s3_url": "https://cloudius-jenkins-test.s3.amazonaws.com/test-id-123/20250526_120000/sct-runner.tar.zst",
            "date": datetime.datetime(2025, 5, 26, 12, 0, 0),
        },
        {
            "file_path": "test-id-123/20250526_120000/db-cluster.tar.zst",
            "type": "db-cluster",
            "link": "https://argus.scylladb.com/api/v1/tests/test-id-123/log/db-cluster.tar.zst/download",
            "s3_url": "https://cloudius-jenkins-test.s3.amazonaws.com/test-id-123/20250526_120000/db-cluster.tar.zst",
            "date": datetime.datetime(2025, 5, 26, 12, 0, 0),
        },
        {
            "file_path": "test-id-123/20250526_120000/events.tar.zst",
            "type": "event",
            "link": "https://argus.scylladb.com/api/v1/tests/test-id-123/log/events.tar.zst/download",
            "s3_url": "https://cloudius-jenkins-test.s3.amazonaws.com/test-id-123/20250526_120000/events.tar.zst",
            "date": datetime.datetime(2025, 5, 26, 12, 0, 0),
        },
    ]


@pytest.fixture
def runner():
    """Click CLI test runner."""
    return CliRunner()


def test_download_logs_list_types_shows_available_types(runner, mock_logs):
    """Test --list-types shows available log types without downloading."""
    with patch("sct.list_logs_by_test_id", return_value=mock_logs):
        with patch("sct.add_file_logger"):
            result = runner.invoke(download_logs, ["test-id-123", "--list-types"])

    assert result.exit_code == 0
    assert "Available log types:" in result.output
    assert "db-cluster" in result.output
    assert "event" in result.output
    assert "sct" in result.output


def test_download_logs_no_logs_found_shows_message(runner):
    """Test shows appropriate message when no logs found."""
    with patch("sct.list_logs_by_test_id", return_value=[]):
        with patch("sct.add_file_logger"):
            result = runner.invoke(download_logs, ["nonexistent-test-id"])

    assert result.exit_code == 0
    assert "No logs found for test id: nonexistent-test-id" in result.output


def test_download_logs_filter_by_type(runner, mock_logs, tmp_path):
    """Test filtering downloads by log type."""
    mock_storage = MagicMock()
    mock_storage.download_file.return_value = str(tmp_path / "sct-runner.tar.zst")

    with patch("sct.list_logs_by_test_id", return_value=mock_logs):
        with patch("sct.S3Storage", return_value=mock_storage):
            with patch("sct.add_file_logger"):
                result = runner.invoke(download_logs, ["test-id-123", "-t", "sct", "-d", str(tmp_path)])

    assert result.exit_code == 0
    assert "Downloading 1 log file(s)" in result.output
    assert "Downloaded: 1/1 files" in result.output
    mock_storage.download_file.assert_called_once()


def test_download_logs_filter_by_multiple_types(runner, mock_logs, tmp_path):
    """Test filtering downloads by multiple log types."""
    mock_storage = MagicMock()
    mock_storage.download_file.side_effect = [
        str(tmp_path / "sct-runner.tar.zst"),
        str(tmp_path / "db-cluster.tar.zst"),
    ]

    with patch("sct.list_logs_by_test_id", return_value=mock_logs):
        with patch("sct.S3Storage", return_value=mock_storage):
            with patch("sct.add_file_logger"):
                result = runner.invoke(
                    download_logs, ["test-id-123", "-t", "sct", "-t", "db-cluster", "-d", str(tmp_path)]
                )

    assert result.exit_code == 0
    assert "Downloading 2 log file(s)" in result.output
    assert "Downloaded: 2/2 files" in result.output
    assert mock_storage.download_file.call_count == 2


def test_download_logs_filter_no_matching_type(runner, mock_logs):
    """Test shows message when no logs match the requested type."""
    with patch("sct.list_logs_by_test_id", return_value=mock_logs):
        with patch("sct.add_file_logger"):
            result = runner.invoke(download_logs, ["test-id-123", "-t", "nonexistent-type"])

    assert result.exit_code == 0
    assert "No logs found matching types: nonexistent-type" in result.output


def test_download_logs_all_files_success(runner, mock_logs, tmp_path):
    """Test downloading all log files successfully."""
    mock_storage = MagicMock()
    mock_storage.download_file.side_effect = [
        str(tmp_path / "sct-runner.tar.zst"),
        str(tmp_path / "db-cluster.tar.zst"),
        str(tmp_path / "events.tar.zst"),
    ]

    with patch("sct.list_logs_by_test_id", return_value=mock_logs):
        with patch("sct.S3Storage", return_value=mock_storage):
            with patch("sct.add_file_logger"):
                result = runner.invoke(download_logs, ["test-id-123", "-d", str(tmp_path)])

    assert result.exit_code == 0
    assert "Downloading 3 log file(s)" in result.output
    assert "Downloaded: 3/3 files" in result.output
    assert mock_storage.download_file.call_count == 3


def test_download_logs_partial_failure(runner, mock_logs, tmp_path):
    """Test reports partial download failures and exits non-zero."""
    mock_storage = MagicMock()
    mock_storage.download_file.side_effect = [
        str(tmp_path / "sct-runner.tar.zst"),
        RuntimeError("S3 download failed"),
        str(tmp_path / "events.tar.zst"),
    ]

    with patch("sct.list_logs_by_test_id", return_value=mock_logs):
        with patch("sct.S3Storage", return_value=mock_storage):
            with patch("sct.add_file_logger"):
                result = runner.invoke(download_logs, ["test-id-123", "-d", str(tmp_path)])

    # A failed download must surface as a non-zero exit so callers/CI can detect it.
    assert result.exit_code == 1
    assert "Downloaded: 2/3 files" in result.output
    assert "Failed: 1 files" in result.output
    assert "S3 download failed" in result.output


def test_download_logs_creates_dest_directory(runner, mock_logs, tmp_path):
    """Test creates destination directory if it doesn't exist."""
    new_dir = tmp_path / "new_subdir" / "logs"
    mock_storage = MagicMock()
    mock_storage.download_file.return_value = str(new_dir / "sct-runner.tar.zst")

    with patch("sct.list_logs_by_test_id", return_value=mock_logs[:1]):
        with patch("sct.S3Storage", return_value=mock_storage):
            with patch("sct.add_file_logger"):
                result = runner.invoke(download_logs, ["test-id-123", "-d", str(new_dir)])

    assert result.exit_code == 0
    assert new_dir.exists()
