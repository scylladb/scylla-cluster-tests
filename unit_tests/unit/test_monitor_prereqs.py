"""Tests for installing only the monitor prerequisites a host lacks.

Lab hosts already run Docker and other jobs, so the setup must not reinstall or restart what works.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseMonitorSet


def run_prereqs(missing: str) -> MagicMock:
    node = MagicMock()

    def run(cmd, **_):
        result = MagicMock()
        result.ok = False  # no ~/PREPARED-MONITOR
        result.stdout = missing
        return result

    node.remoter.run.side_effect = run
    monitor_set = MagicMock(is_formal_monitor_image=False)
    BaseMonitorSet.install_scylla_monitoring_prereqs(monitor_set, node)
    return node


@pytest.fixture(autouse=True)
def no_node_exporter_or_hub_login(monkeypatch):
    monkeypatch.setattr("sdcm.cluster.NodeExporterSetup", MagicMock())
    monkeypatch.setattr("sdcm.cluster.docker_hub_login", MagicMock())


def commands(node: MagicMock) -> list[str]:
    """Commands run after the PREPARED-MONITOR check and the probe."""
    return [call.args[0] if call.args else call.kwargs["cmd"] for call in node.remoter.run.call_args_list[2:]]


def test_host_with_everything_gets_no_install_and_no_docker_restart():
    node = run_prereqs("")

    node.update_repo_cache.assert_not_called()
    node.install_package.assert_not_called()
    node.remoter.sudo.assert_not_called()
    assert commands(node) == [], "only the marker check and the probe should run"


def test_only_missing_packages_are_installed():
    node = run_prereqs("unzip\n")

    node.install_package.assert_called_once_with("unzip")
    node.remoter.sudo.assert_not_called()


def test_missing_python_modules_are_pip_installed():
    node = run_prereqs("python-libs\npython3-pip\n")

    node.install_package.assert_called_once_with("python3-pip")
    assert "pyyaml psutil" in node.remoter.sudo.call_args.args[0]


def test_docker_usable_only_by_root_adds_the_user_to_the_group_without_restart():
    node = run_prereqs("docker-group\n")

    node.remoter.sudo.assert_not_called()
    assert commands(node) == ["sudo usermod -aG docker $USER"]


def test_missing_docker_is_installed_and_restarted():
    node = run_prereqs("docker\n")

    assert "get.docker.com" in node.remoter.sudo.call_args.args[0]
    assert commands(node) == ["sudo usermod -aG docker $USER", "sudo systemctl restart docker"]
