"""Tests for the Docker part of the loader and monitor preflight check.

SCT accepts any Docker Engine packaging at or above DOCKER_MIN_VERSION, and never upgrades it: on a shared host the
upgrade would restart other jobs' containers.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import NodeSetupFailed, ensure_docker_version


def make_node(client_version: str, server_version: str) -> MagicMock:
    node = MagicMock()
    node.remoter.run.return_value.failed = False
    node.remoter.run.return_value.stdout = client_version
    node.remoter.sudo.return_value.stdout = f"{server_version}\n"
    return node


@pytest.mark.parametrize(
    "server_version",
    ["29.6.2", "27.3.1", "24.0.7", "20.10.21+dfsg1"],
    ids=["moby-engine", "docker-ce", "docker.io", "oldest-accepted"],
)
def test_supported_docker_is_left_alone(server_version):
    node = make_node("Docker version 29.6.2, build 1", server_version)

    ensure_docker_version(node)

    node.remoter.sudo.assert_called_once()  # only the version query


def test_too_old_docker_fails_naming_both_versions_and_is_never_upgraded():
    node = make_node("Docker version 19.03.13", "19.03.13")

    with pytest.raises(NodeSetupFailed, match=r"Docker 19\.03\.13 is older than 20\.10"):
        ensure_docker_version(node)

    node.remoter.sudo.assert_called_once()


def test_podman_shim_is_rejected():
    node = make_node("podman version 5.6.1", "5.6.1")

    with pytest.raises(NodeSetupFailed, match="Podman"):
        ensure_docker_version(node)


def test_missing_docker_fails_with_a_clear_message():
    node = make_node("", "")
    node.remoter.run.return_value.failed = True

    with pytest.raises(NodeSetupFailed, match="Docker is not installed"):
        ensure_docker_version(node)

    node.remoter.sudo.assert_not_called()


def test_node_setup_failure_repr_names_the_node_and_the_error():
    assert repr(NodeSetupFailed(node="loader-1", error_msg="Docker is not installed")) == (
        "[loader-1] NodeSetupFailed: Docker is not installed"
    )
