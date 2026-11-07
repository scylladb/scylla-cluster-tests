"""Tests for checking Docker's free disk before a loader or monitor pulls its images.

On a lab host in SCT-1215 a 15GB root filled up mid-pull and the run failed long after setup.
"""

from unittest.mock import MagicMock

import pytest
from invoke.exceptions import UnexpectedExit

from sdcm.cluster import NodeSetupFailed, docker_host_preflight, ensure_docker_free_space, register_docker_host_role

PRUNE = "docker image prune --all --force"


def make_node(store: str, *free_gb: int) -> MagicMock:
    """A node whose image store probe prints `store` and whose `df` reports `free_gb`, one value per call."""
    node = MagicMock()
    node.remoter.sudo.return_value.stdout = f"{store}\n"
    node.remoter.run.side_effect = [MagicMock(stdout=f"Avail\n  {gb}G\n") for gb in free_gb]
    return node


def sudo_commands(node: MagicMock) -> list[str]:
    return [call.args[0] for call in node.remoter.sudo.call_args_list]


def test_enough_space_removes_nothing():
    node = make_node("/var/lib/containerd", 48)

    ensure_docker_free_space(node, 10)

    assert PRUNE not in sudo_commands(node)
    assert node.remoter.run.call_args.args[0] == "df --output=avail -BG /var/lib/containerd"


def test_unused_images_are_removed_before_failing():
    node = make_node("/var/lib/docker", 2, 12)

    ensure_docker_free_space(node, 10)

    assert PRUNE in sudo_commands(node)


def test_still_short_fails_naming_the_store_and_both_sizes():
    node = make_node("/var/lib/containerd", 2, 4)

    with pytest.raises(NodeSetupFailed, match=r"/var/lib/containerd has 4GB free, the run needs 10GB"):
        ensure_docker_free_space(node, 10)


def test_zero_turns_the_check_off():
    node = MagicMock()

    ensure_docker_free_space(node, 0)

    node.remoter.run.assert_not_called()
    node.remoter.sudo.assert_not_called()


def test_a_store_df_cannot_read_fails_with_dfs_error():
    node = make_node("/var/lib/containerd")
    node.remoter.run.side_effect = UnexpectedExit(MagicMock(stderr="df: /var/lib/containerd: No such file"))

    with pytest.raises(UnexpectedExit):
        ensure_docker_free_space(node, 10)


PARAMS = {"docker_min_free_space_gb_loader": 3, "docker_min_free_space_gb_monitor": 10}


@pytest.mark.parametrize("first,second", [("loader", "monitor"), ("monitor", "loader")])
def test_a_host_with_two_roles_is_checked_once_against_their_sum(monkeypatch, first, second):
    """Whichever role reaches the host first checks the space of both, before either pulls an image."""
    monkeypatch.setattr("sdcm.cluster._DOCKER_HOST_CHECKS", {})
    version_check = MagicMock()
    monkeypatch.setattr("sdcm.cluster.ensure_docker_version", version_check)
    register_docker_host_role("10.0.0.7", "loader")
    register_docker_host_role("10.0.0.7", "monitor")
    first_node = make_node("/var/lib/containerd", 9, 9)  # enough for either role alone, short for 3 + 10
    second_node = make_node("/var/lib/containerd")
    first_node.private_ip_address = second_node.private_ip_address = "10.0.0.7"

    with pytest.raises(NodeSetupFailed, match=r"has 9GB free, the run needs 13GB"):
        docker_host_preflight(first_node, PARAMS, first)

    first_node = make_node("/var/lib/containerd", 13)
    first_node.private_ip_address = "10.0.0.7"
    docker_host_preflight(first_node, PARAMS, first)
    docker_host_preflight(second_node, PARAMS, second)

    second_node.remoter.run.assert_not_called()
    second_node.remoter.sudo.assert_not_called()


def test_a_host_with_one_role_needs_only_its_space(monkeypatch):
    monkeypatch.setattr("sdcm.cluster._DOCKER_HOST_CHECKS", {})
    monkeypatch.setattr("sdcm.cluster.ensure_docker_version", MagicMock())
    node = make_node("/var/lib/containerd", 9)
    node.private_ip_address = "10.0.0.8"

    docker_host_preflight(node, PARAMS, "loader")

    assert PRUNE not in sudo_commands(node)
