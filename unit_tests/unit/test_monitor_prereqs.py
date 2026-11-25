"""Tests for installing only the monitor prerequisites a host lacks.

Lab hosts already run Docker and other jobs, so the setup must not reinstall or restart what works.
"""

from unittest.mock import MagicMock

import pytest
from invoke.exceptions import UnexpectedExit

from sdcm.cluster import BaseMonitorSet, NodeSetupFailed


@pytest.fixture(autouse=True)
def no_node_exporter_or_hub_login(monkeypatch):
    """Stub the node exporter setup and Docker Hub login, which would reach other hosts."""
    monkeypatch.setattr("sdcm.cluster.NodeExporterSetup", MagicMock())
    monkeypatch.setattr("sdcm.cluster.docker_hub_login", MagicMock())


def run_prereqs(missing: str) -> MagicMock:
    """Run install_scylla_monitoring_prereqs on a node whose probe reports the missing items."""
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


def commands(node: MagicMock) -> list[str]:
    """Commands run after the PREPARED-MONITOR check and the probe."""
    return [call.args[0] if call.args else call.kwargs["cmd"] for call in node.remoter.run.call_args_list[2:]]


def shared_host_setup(monkeypatch, start_error=None) -> tuple[MagicMock, MagicMock]:
    """Run the monitor node setup on a RHEL-like baremetal host, with every step of its own mocked."""
    monkeypatch.setattr("sdcm.cluster.set_grafana_url", MagicMock())
    node = MagicMock(external_address="1.2.3.4")
    node.distro.is_rhel_like = True
    monitor_set = MagicMock(owns_host=False, params={"cluster_backend": "baremetal"})
    monitor_set.test_config.REUSE_CLUSTER = False
    monitor_set.start_scylla_monitoring.side_effect = start_error
    BaseMonitorSet.node_setup(monitor_set, node)
    return node, monitor_set


def test_host_with_everything_gets_no_install_and_no_docker_restart():
    """Nothing is installed or restarted when the host already has every prerequisite."""
    node = run_prereqs("")

    node.update_repo_cache.assert_not_called()
    node.install_package.assert_not_called()
    node.remoter.sudo.assert_not_called()
    assert commands(node) == [], "only the marker check and the probe should run"


def test_only_missing_packages_are_installed():
    """Only the missing package is installed, and the repo cache is not refreshed again."""
    node = run_prereqs("unzip\n")

    node.update_repo_cache.assert_not_called()  # node_setup already refreshed it
    node.install_package.assert_called_once_with("unzip")
    node.remoter.sudo.assert_not_called()


def test_missing_python_modules_are_pip_installed():
    """A missing python-libs prerequisite installs pip, then pyyaml and psutil through it."""
    node = run_prereqs("python-libs\npython3-pip\n")

    node.install_package.assert_called_once_with("python3-pip")
    assert "pyyaml psutil" in node.remoter.sudo.call_args.args[0]


def test_docker_usable_only_by_root_adds_the_user_to_the_group_without_restart():
    """A docker group the user lacks is fixed with usermod, without restarting Docker."""
    node = run_prereqs("docker-group\n")

    node.remoter.sudo.assert_not_called()
    assert commands(node) == ["sudo usermod -aG docker $USER"]


def test_missing_docker_is_installed():
    """Missing Docker is installed with get.docker.com and the user added to the docker group."""
    node = run_prereqs("docker\n")

    assert "get.docker.com" in node.remoter.sudo.call_args.args[0]
    assert commands(node) == ["sudo usermod -aG docker $USER"]


@pytest.mark.parametrize(
    "missing",
    [
        pytest.param("docker\n", id="docker"),
        pytest.param("docker-group\n", id="docker-group"),
    ],
)
def test_host_sct_does_not_own_fails_instead_of_changing_docker(missing):
    """A host SCT does not own fails instead of changing Docker, whatever is missing."""
    node = MagicMock()
    node.remoter.run.return_value = MagicMock(ok=False, stdout=missing)
    monitor_set = MagicMock(is_formal_monitor_image=False, owns_host=False)

    with pytest.raises(NodeSetupFailed, match="install and start Docker"):
        BaseMonitorSet.install_scylla_monitoring_prereqs(monitor_set, node)

    node.remoter.sudo.assert_not_called()
    assert commands(node) == [], "no Docker install, group change or restart"


def test_shared_host_keeps_its_firewall_and_ssh_packages(monkeypatch):
    """A shared baremetal host keeps its firewall and SSH packages untouched."""
    node, _ = shared_host_setup(monkeypatch)

    node.disable_firewall.assert_not_called()
    node.upgrade_ssh_packages.assert_not_called()


def test_shared_host_fails_a_stack_that_does_not_start_instead_of_restarting_it(monkeypatch):
    """A monitor stack that does not start on a shared host raises instead of being restarted."""
    error = UnexpectedExit(MagicMock(exited=1))

    with pytest.raises(UnexpectedExit):
        shared_host_setup(monkeypatch, start_error=error)
