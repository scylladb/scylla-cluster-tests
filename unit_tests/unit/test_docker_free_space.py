"""Tests for checking Docker's free disk before a loader or monitor pulls its images.

On a lab host in SCT-1215 a 15GB root filled up mid-pull and the run failed long after setup.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import NodeSetupFailed, ensure_docker_free_space


def make_node(store: str, *free_gb: int) -> MagicMock:
    """A node whose image store probe prints `store` and whose `df` reports `free_gb`, one value per call."""
    node = MagicMock()
    node.remoter.sudo.return_value.stdout = f"{store}\n"
    node.remoter.run.side_effect = [MagicMock(stdout=f"  {gb}G\n") for gb in free_gb]
    return node


def sudo_commands(node: MagicMock) -> list[str]:
    return [call.args[0] for call in node.remoter.sudo.call_args_list]


def test_enough_space_removes_nothing():
    node = make_node("/var/lib/containerd", 48)

    ensure_docker_free_space(node, 10)

    assert "docker image prune --all --force" not in sudo_commands(node)
    assert "df --output=avail -BG /var/lib/containerd" in node.remoter.run.call_args.args[0]


def test_unused_images_are_removed_before_failing():
    node = make_node("/var/lib/docker", 2, 12)

    ensure_docker_free_space(node, 10)

    assert "docker image prune --all --force" in sudo_commands(node)


def test_still_short_fails_naming_the_store_and_both_sizes():
    node = make_node("/var/lib/containerd", 2, 4)

    with pytest.raises(NodeSetupFailed, match=r"/var/lib/containerd has 4GB free, the run needs 10GB"):
        ensure_docker_free_space(node, 10)


def test_zero_turns_the_check_off():
    node = MagicMock()

    ensure_docker_free_space(node, 0)

    node.remoter.run.assert_not_called()
    node.remoter.sudo.assert_not_called()
