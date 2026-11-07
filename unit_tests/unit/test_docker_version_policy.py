"""Tests for the Docker part of the loader and monitor preflight check.

SCT accepts any Docker Engine packaging at or above DOCKER_MIN_VERSION, and never upgrades it: on a shared host the
upgrade would restart other jobs' containers. On a shared lab host SCT doesn't install Docker either.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseLoaderSet, NodeSetupFailed, ensure_docker_version


def make_node(client_version: str, server_version: str, server_failed: bool = False) -> MagicMock:
    """A node whose `docker --version` prints `client_version`, and whose server query, run as the SSH user, prints
    `server_version` or fails."""
    node = MagicMock()
    node.remoter.run.side_effect = [
        MagicMock(failed=False, stdout=client_version),
        MagicMock(failed=server_failed, stdout=f"{server_version}\n"),
    ]
    return node


@pytest.mark.parametrize(
    "server_version",
    ["29.6.2", "27.3.1", "24.0.7", "20.10.21+dfsg1"],
    ids=["moby-engine", "docker-ce", "docker.io", "oldest-accepted"],
)
def test_supported_docker_is_left_alone(server_version):
    node = make_node("Docker version 29.6.2, build 1", server_version)

    ensure_docker_version(node)

    node.remoter.sudo.assert_not_called()


def test_too_old_docker_fails_naming_both_versions_and_is_never_upgraded():
    node = make_node("Docker version 19.03.13", "19.03.13")

    with pytest.raises(NodeSetupFailed, match=r"Docker 19\.03\.13 is older than 20\.10"):
        ensure_docker_version(node)

    node.remoter.sudo.assert_not_called()


def test_podman_shim_is_rejected():
    node = make_node("podman version 5.6.1", "5.6.1")

    with pytest.raises(NodeSetupFailed, match="Podman"):
        ensure_docker_version(node)


def test_missing_docker_fails_with_a_clear_message():
    node = MagicMock()
    node.remoter.run.return_value.failed = True

    with pytest.raises(NodeSetupFailed, match="Docker is not installed"):
        ensure_docker_version(node)


def test_docker_the_ssh_user_cannot_use_fails_naming_the_fix():
    node = make_node("Docker version 29.6.2, build 1", "", server_failed=True)

    with pytest.raises(NodeSetupFailed, match="add the user to the `docker` group"):
        ensure_docker_version(node)


def test_node_setup_failure_repr_names_the_node_and_the_error():
    assert repr(NodeSetupFailed(node="loader-1", error_msg="Docker is not installed")) == (
        "[loader-1] NodeSetupFailed: Docker is not installed"
    )


@pytest.mark.parametrize("owns_host", [True, False], ids=["vm-the-run-created", "shared-lab-host"])
def test_loader_installs_docker_only_on_a_host_it_owns(monkeypatch, owns_host):
    for name in ("TestConfig", "NodeExporterSetup", "docker_hub_login", "docker_host_preflight"):
        monkeypatch.setattr(f"sdcm.cluster.{name}", MagicMock())
    monkeypatch.setattr("sdcm.cluster.TestConfig.return_value.REUSE_CLUSTER", False)
    loader_set = MagicMock(owns_host=owns_host)
    loader_set.params.get.return_value = None
    node = MagicMock()
    node.distro.is_fedora = True

    BaseLoaderSet.node_setup(loader_set, node)

    group_changes = [call for call in node.remoter.run.call_args_list if "usermod" in str(call)]
    assert bool(group_changes) is owns_host
    assert loader_set._install_docker_on_fedora.called is owns_host
