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

"""Unit tests for monitoring stack restore: annotations, port selection and cleanup on failure."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.monitorstack import restore
from sdcm.monitorstack.restore import (
    pick_monitoring_stack_ports,
    restore_annotations_data,
    restore_grafana_dashboards_and_annotations,
    restore_monitoring_stack,
)


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
        pytest.param(lambda addons_dir: (addons_dir / "annotations.json").write_text("null"), id="null"),
        pytest.param(lambda addons_dir: (addons_dir / "annotations.json").write_text("{}"), id="object"),
        pytest.param(lambda addons_dir: (addons_dir / "annotations.json").write_text('"just a string"'), id="string"),
    ],
)
def test_restore_annotations_data_returns_false_without_raising(tmp_path, make_annotations_file):
    """A missing, empty, malformed, or non-list annotations.json must not raise, and must not upload anything."""
    addons_dir = _annotations_dir(tmp_path)
    make_annotations_file(addons_dir)

    with patch("sdcm.monitorstack.restore.create_retry_session") as mock_create_retry_session:
        result = restore_annotations_data(str(tmp_path), grafana_docker_port=1234)

    assert result is False
    mock_create_retry_session.assert_not_called()
    mock_create_retry_session.return_value.post.assert_not_called()


def test_restore_annotations_data_uploads_valid_annotations(tmp_path):
    """A valid, non-empty annotations.json is loaded and each annotation is uploaded."""
    addons_dir = _annotations_dir(tmp_path)
    annotations_file = addons_dir / "annotations.json"
    annotations_file.write_text('[{"text": "annotation-1"}, {"text": "annotation-2"}]')

    mock_response = MagicMock(status_code=200)
    mock_session = MagicMock(post=MagicMock(return_value=mock_response))
    with patch(
        "sdcm.monitorstack.restore.create_retry_session", return_value=mock_session
    ) as mock_create_retry_session:
        result = restore_annotations_data(str(tmp_path), grafana_docker_port=1234)

    assert result is True
    mock_create_retry_session.assert_called_once_with(retries=0)
    assert mock_session.post.call_count == 2
    for call in mock_session.post.call_args_list:
        assert call.kwargs["timeout"] == 30


def test_restore_grafana_dashboards_and_annotations_succeeds_when_annotations_skipped():
    """restore_grafana_dashboards_and_annotations must not fail overall when annotations are skipped."""
    with patch("sdcm.monitorstack.restore.restore_sct_dashboards", return_value=True):
        with patch("sdcm.monitorstack.restore.restore_annotations_data", return_value=False):
            result = restore_grafana_dashboards_and_annotations(
                monitoring_dockers_dir="/some/dir", grafana_docker_port=1234, sct_dashboard_file="dashboard.json"
            )

    assert result is True


@pytest.fixture
def default_ports(monkeypatch):
    """Pin the default ports (they are picked at import time) and fake port binding."""
    monkeypatch.setattr(restore, "GRAFANA_DOCKER_PORT", 3000)
    monkeypatch.setattr(restore, "ALERT_DOCKER_PORT", 6000)
    monkeypatch.setattr(restore, "PROMETHEUS_DOCKER_PORT", 9090)
    bound_elsewhere = set()
    random_ports = iter(range(40000, 40100))

    def fake_get_free_port(address="", ports_to_try=(0,)):
        for port in ports_to_try:
            if port == 0:
                return next(random_ports)
            if port not in bound_elsewhere:
                return port
        raise RuntimeError("Can't allocate a free port")

    monkeypatch.setattr(restore, "get_free_port", fake_get_free_port)
    return bound_elsewhere


def test_pick_monitoring_stack_ports_uses_defaults_when_nothing_runs(default_ports):
    assert pick_monitoring_stack_ports(tenants_number=1, occupied_ports=set()) == (3000, 6000, 9090)


def test_pick_monitoring_stack_ports_skips_half_started_stack(default_ports):
    """A concurrent restore that has created Prometheus and Alertmanager but not yet Grafana owns the slot."""
    ports = pick_monitoring_stack_ports(tenants_number=1, occupied_ports={6000, 9090})

    assert ports == (40000, 40001, 40002)


def test_pick_monitoring_stack_ports_skips_slot_with_port_bound_by_other_process(default_ports):
    default_ports.add(3000)

    assert pick_monitoring_stack_ports(tenants_number=1, occupied_ports=set()) == (40000, 40001, 40002)


def test_pick_monitoring_stack_ports_uses_next_tenant_slot(default_ports):
    assert pick_monitoring_stack_ports(tenants_number=2, occupied_ports={3000, 6000, 9090}) == (3001, 6001, 9091)


@pytest.fixture
def restore_until_dashboards(monkeypatch):
    """Stub everything restore_monitoring_stack() does before uploading dashboards, for two clusters."""
    clusters = {"cluster-1": "arch-1", "cluster-2": "arch-2"}
    started = iter(
        [
            {"grafana_docker_port": 3000, "alert_docker_port": 6000, "prometheus_docker_port": 9090},
            {"grafana_docker_port": 3001, "alert_docker_port": 6001, "prometheus_docker_port": 9091},
        ]
    )
    monkeypatch.setattr(restore, "is_docker_available", lambda: True)
    monkeypatch.setattr(restore, "get_monitoring_stack_archive", lambda *_: {"file_path": "f", "link": "l"})
    monkeypatch.setattr(restore, "S3Storage", MagicMock())
    monkeypatch.setattr(restore, "extract_monitoring_data_archive", lambda *_: dict(clusters))
    monkeypatch.setattr(restore, "extract_monitoring_stack_archive", lambda *_: dict(clusters))
    monkeypatch.setattr(restore, "create_monitoring_data_dir", lambda *_, **__: "/data")
    monkeypatch.setattr(restore, "create_monitoring_stack_dir", lambda *_: "/stack")
    monkeypatch.setattr(restore, "get_monitoring_stack_scylla_version", lambda *_: ("master", "master"))
    monkeypatch.setattr(restore, "run_monitoring_stack_containers", lambda *_, **__: next(started))
    monkeypatch.setattr(restore, "get_nemesis_dashboard_file_for_cluster", lambda **_: "dashboard.json")
    monkeypatch.setattr(restore, "verify_monitoring_stack", lambda **_: True)
    kill = MagicMock()
    monkeypatch.setattr(restore, "kill_running_monitoring_stack_services", kill)
    return kill


def test_restore_monitoring_stack_removes_only_its_own_containers_on_failure(monkeypatch, restore_until_dashboards):
    """When the second cluster fails, both stacks this restore started are removed, and nothing else."""
    monkeypatch.setattr(restore, "restore_grafana_dashboards_and_annotations", MagicMock(side_effect=[True, False]))

    assert restore_monitoring_stack("test-id") is False
    assert [call.kwargs["ports"]["grafana_docker_port"] for call in restore_until_dashboards.call_args_list] == [
        3000,
        3001,
    ]


def test_restore_monitoring_stack_keeps_containers_on_success(monkeypatch, restore_until_dashboards):
    monkeypatch.setattr(restore, "restore_grafana_dashboards_and_annotations", lambda *_, **__: True)

    assert set(restore_monitoring_stack("test-id")) == {"cluster-1", "cluster-2"}
    restore_until_dashboards.assert_not_called()
