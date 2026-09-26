"""Regression tests for the OCI repo cache failure.

On OCI the oracle-cloud-agent snap runs its own `apt update` right after boot (that agent
is part of what customers run, so it is not disabled). If `BaseNode.update_repo_cache()`
hits that lock it used to fail immediately and the failure was only logged, so the
following `apt-get install -y scylla` failed with a misleading
"E: Unable to locate package scylla".

These tests assert that every command of `update_repo_cache()` tolerates a busy package
manager (lock wait + retries) and that a failure is propagated instead of swallowed.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseNode, NodeSetupFailed
from sdcm.utils.apt import APT_LOCK_FILES


def _fake_node(*, is_rhel_like=False, is_sles=False):
    node = MagicMock()
    node.distro.is_rhel_like = is_rhel_like
    node.distro.is_sles = is_sles
    return node


def _sudo_calls(node):
    return [(call.args[0], call.kwargs) for call in node.remoter.sudo.call_args_list]


def test_update_repo_cache_debian_waits_for_apt_lock_and_retries():
    node = _fake_node()

    BaseNode.update_repo_cache(node)

    apt_calls = [(cmd, kwargs) for cmd, kwargs in _sudo_calls(node) if "apt-get" in cmd]
    assert len(apt_calls) == 2, f"expected 'apt-get clean' and 'apt-get update' calls, got: {apt_calls!r}"
    for cmd, kwargs in apt_calls:
        assert "DPkg::Lock::Timeout" in cmd, f"apt command missing dpkg lock-wait option: {cmd!r}"
        assert kwargs.get("retry") == 3, f"apt command missing retry=3: {cmd!r} {kwargs!r}"
        # 'clean' and 'update' fail immediately when the archives/lists locks are taken,
        # DPkg::Lock::Timeout makes apt wait for the dpkg locks only
        assert "fuser" in cmd, f"apt command does not wait for the apt locks: {cmd!r}"
        for lock_file in APT_LOCK_FILES:
            assert lock_file in cmd, f"apt lock wait does not cover {lock_file}: {cmd!r}"

    clean_cmd = apt_calls[0][0]
    assert "clean all" not in clean_cmd, f"expected 'apt-get clean' (no 'all' argument): {clean_cmd!r}"
    assert "Dpkg::Options" not in clean_cmd, f"'apt-get clean' never invokes dpkg, drop its options: {clean_cmd!r}"


def test_update_repo_cache_rhel_like_waits_for_rpm_lock_and_retries():
    node = _fake_node(is_rhel_like=True)

    BaseNode.update_repo_cache(node)

    yum_calls = [(cmd, kwargs) for cmd, kwargs in _sudo_calls(node) if "yum clean all" in cmd]
    assert len(yum_calls) == 1, f"expected exactly one 'yum clean all' call, got: {_sudo_calls(node)!r}"

    cmd, kwargs = yum_calls[0]
    assert "/var/lib/rpm/.rpm.lock" in cmd, f"'yum clean all' missing rpm lock wait: {cmd!r}"
    assert kwargs.get("retry") == 3, f"'yum clean all' missing retry=3: {kwargs!r}"


def test_update_repo_cache_sles_retries_cache_cleanup():
    node = _fake_node(is_sles=True)

    BaseNode.update_repo_cache(node)

    zypper_calls = [(cmd, kwargs) for cmd, kwargs in _sudo_calls(node) if cmd.startswith("zypper")]
    assert [cmd for cmd, _ in zypper_calls] == ["zypper clean all", "zypper refresh"], (
        f"unexpected zypper calls: {zypper_calls!r}"
    )
    for cmd, kwargs in zypper_calls:
        assert kwargs.get("retry") == 3, f"zypper command missing retry=3: {cmd!r} {kwargs!r}"


@pytest.mark.parametrize("distro", ["debian", "rhel_like", "sles"])
def test_update_repo_cache_failure_is_fatal(distro):
    node = _fake_node(is_rhel_like=distro == "rhel_like", is_sles=distro == "sles")
    node.remoter.sudo.side_effect = Exception("Could not get lock /var/cache/apt/archives/lock")

    with pytest.raises(NodeSetupFailed, match="Failed to update repo cache"):
        BaseNode.update_repo_cache(node)
