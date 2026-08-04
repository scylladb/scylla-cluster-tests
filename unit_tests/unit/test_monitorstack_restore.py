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

"""Unit tests for monitoring stack restore of annotations (SCT-432)."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.monitorstack.restore import restore_annotations_data, restore_grafana_dashboards_and_annotations


def _annotations_dir(tmp_path):
    addons_dir = tmp_path / "sct_monitoring_addons"
    addons_dir.mkdir()
    return addons_dir


@pytest.mark.parametrize(
    "make_annotations_file",
    [
        pytest.param(lambda addons_dir: None, id="missing"),
        pytest.param(lambda addons_dir: (addons_dir / "annotations.json").write_text(""), id="empty"),
        pytest.param(
            lambda addons_dir: (addons_dir / "annotations.json").write_text("{not valid json"), id="malformed"
        ),
    ],
)
def test_restore_annotations_data_returns_false_without_raising(tmp_path, make_annotations_file):
    """A missing, empty or malformed annotations.json must not raise, and must not upload anything."""
    addons_dir = _annotations_dir(tmp_path)
    make_annotations_file(addons_dir)

    with patch("sdcm.monitorstack.restore.requests.post") as mock_post:
        result = restore_annotations_data(str(tmp_path), grafana_docker_port=1234)

    assert result is False
    mock_post.assert_not_called()


def test_restore_annotations_data_uploads_valid_annotations(tmp_path):
    """A valid, non-empty annotations.json is loaded and each annotation is uploaded."""
    addons_dir = _annotations_dir(tmp_path)
    annotations_file = addons_dir / "annotations.json"
    annotations_file.write_text('[{"text": "annotation-1"}, {"text": "annotation-2"}]')

    mock_response = MagicMock(status_code=200)
    with patch("sdcm.monitorstack.restore.requests.post", return_value=mock_response) as mock_post:
        result = restore_annotations_data(str(tmp_path), grafana_docker_port=1234)

    assert result is True
    assert mock_post.call_count == 2


def test_restore_grafana_dashboards_and_annotations_succeeds_when_annotations_skipped():
    """restore_grafana_dashboards_and_annotations must not fail overall when annotations are skipped."""
    with patch("sdcm.monitorstack.restore.restore_sct_dashboards", return_value=True):
        with patch("sdcm.monitorstack.restore.restore_annotations_data", return_value=False):
            result = restore_grafana_dashboards_and_annotations(
                monitoring_dockers_dir="/some/dir", grafana_docker_port=1234, sct_dashboard_file="dashboard.json"
            )

    assert result is True
