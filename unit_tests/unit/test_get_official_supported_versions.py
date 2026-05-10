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

"""Unit tests for fetch_official_supported_versions and the CLI command."""

from unittest.mock import patch, MagicMock

from click.testing import CliRunner

from sct import get_official_supported_versions
from utils.get_supported_scylla_base_versions import fetch_official_supported_versions

SAMPLE_RESPONSE = {
    "data": [
        {"version": "ScyllaDB 2026.1", "status": "Supported"},
        {"version": "ScyllaDB 2025.4", "status": "Supported"},
        {"version": "ScyllaDB 2025.3", "status": " Not supported"},
        {"version": "ScyllaDB 2025.2", "status": "Not supported"},
        {"version": "ScyllaDB 2025.1 (LTS)", "status": "Supported"},
        {"version": "Enterprise 2024.2", "status": "Not supported"},
    ]
}


def _mock_session_get(url, timeout=30):
    resp = MagicMock()
    resp.json.return_value = SAMPLE_RESPONSE
    resp.raise_for_status = MagicMock()
    return resp


def test_fetch_official_supported_versions_filters_supported():
    """Only versions with status 'Supported' are returned."""
    mock_session = MagicMock()
    mock_session.get.side_effect = _mock_session_get

    with patch("utils.get_supported_scylla_base_versions.create_retry_session", return_value=mock_session):
        result = fetch_official_supported_versions()

    assert result == ["2026.1", "2025.4", "2025.1"]


def test_fetch_official_supported_versions_extracts_version_number():
    """Version numbers are extracted from strings like 'ScyllaDB 2025.1 (LTS)'."""
    mock_session = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {
        "data": [
            {"version": "ScyllaDB 2025.1 (LTS)", "status": "Supported"},
        ]
    }
    resp.raise_for_status = MagicMock()
    mock_session.get.return_value = resp

    with patch("utils.get_supported_scylla_base_versions.create_retry_session", return_value=mock_session):
        result = fetch_official_supported_versions()

    assert result == ["2025.1"]


def test_fetch_official_supported_versions_empty_data():
    """Returns empty list when no versions are supported."""
    mock_session = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {"data": []}
    resp.raise_for_status = MagicMock()
    mock_session.get.return_value = resp

    with patch("utils.get_supported_scylla_base_versions.create_retry_session", return_value=mock_session):
        result = fetch_official_supported_versions()

    assert result == []


def test_cli_get_official_supported_versions_output():
    """CLI command prints supported versions and exits 0."""
    with (
        patch(
            "sct.fetch_official_supported_versions",
            return_value=["2026.1", "2025.4", "2025.1"],
        ),
        patch("sct.add_file_logger"),
    ):
        runner = CliRunner()
        result = runner.invoke(get_official_supported_versions)

    assert result.exit_code == 0
    assert "2026.1" in result.output
    assert "2025.4" in result.output
    assert "2025.1" in result.output
