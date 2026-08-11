"""Regression tests for SCT-839: apt-get upgrade/install must wait for the dpkg lock.

If the rolling upgrade's ``apt-get dist-upgrade`` hits the SCT 600s SSH timeout while
still running remotely, the remoter retries the command. Without a dpkg lock-wait
option, the retried ``apt-get`` invocation immediately fails with
"Unable to acquire the dpkg frontend lock" because the previous remote process is
still holding it, instead of waiting for it to finish. These tests assert that
``UpgradeTest._upgrade_node`` (Debian/Ubuntu upgrade path) and
``UpgradeTest._rollback_node`` (Debian/Ubuntu rollback path) build their apt-get
commands via ``sdcm.utils.apt.apt_cmd()`` -- the same helper used elsewhere in the
codebase (e.g. ``BaseNode.install_package()`` / ``upgrade_manager_agent()``) -- so
that ``-o DPkg::Lock::Timeout=...`` is always present.
"""

from unittest.mock import MagicMock

import pytest

from upgrade_test import UpgradeTest


UpgradeTest.__test__ = False

pytestmark = pytest.mark.usefixtures("events")


def _run_side_effect(orig_ver, new_ver):
    """Return distinct ``scylla --version`` results across the two calls, empty stdout otherwise."""
    call_count = {"n": 0}

    def _side_effect(cmd, *args, **kwargs):  # noqa: ARG001
        if cmd == "scylla --version":
            call_count["n"] += 1
            stdout = orig_ver if call_count["n"] == 1 else new_ver
            return MagicMock(stdout=stdout, stderr="")
        return MagicMock(stdout="", stderr="")

    return _side_effect


def _package_manager_calls(fake_node):
    """Collect (cmd, kwargs) pairs for every apt-get invocation on ``fake_node.remoter``."""
    calls = list(fake_node.remoter.run.call_args_list) + list(fake_node.remoter.sudo.call_args_list)
    package_manager_calls = []
    for call in calls:
        cmd = call.args[0]
        if "apt-get" in cmd:
            package_manager_calls.append((cmd, call.kwargs))
    return package_manager_calls


def test_upgrade_node_dist_upgrade_waits_for_dpkg_lock():
    """The exact SCT-839 failure site: the Debian/Ubuntu 'apt-get dist-upgrade' call."""
    fake_self = MagicMock()
    fake_self.upgrade_rollback_mode = None
    params = {
        "upgrade_node_packages": None,
        "disable_raft": True,
        "enable_tablets_on_upgrade": False,
        "enable_views_with_tablets_on_upgrade": False,
        "use_preinstalled_scylla": False,
    }
    fake_self.params.get.side_effect = lambda key, *a, **kw: params.get(key)

    fake_node = MagicMock()
    fake_node.name = "node1"
    fake_node.distro.is_rhel_like = False
    fake_node.distro.is_sles = False
    fake_node.is_product_enterprise = False
    fake_node.remoter.run.side_effect = _run_side_effect("2026.1.10", "2026.3.0")

    UpgradeTest._upgrade_node(
        fake_self,
        node=fake_node,
        upgrade_sstables=False,
        new_scylla_repo="http://example.com/scylla.list",
        new_version="2026.3.0",
    )

    package_manager_calls = _package_manager_calls(fake_node)
    dist_upgrade_calls = [(cmd, kwargs) for cmd, kwargs in package_manager_calls if "dist-upgrade" in cmd]
    assert len(dist_upgrade_calls) == 1, f"expected exactly one dist-upgrade call, got: {package_manager_calls!r}"

    cmd, kwargs = dist_upgrade_calls[0]
    assert "DPkg::Lock::Timeout" in cmd, f"dist-upgrade command missing dpkg lock-wait option: {cmd!r}"
    assert kwargs.get("retry") == 3, f"dist-upgrade command missing retry=3: {kwargs!r}"
    assert kwargs.get("timeout") == 600, f"dist-upgrade command missing timeout=600: {kwargs!r}"

    # the 'apt-get update' preceding it should also go through the shared apt_cmd() helper
    update_calls = [(cmd, kwargs) for cmd, kwargs in package_manager_calls if cmd.split()[-1] == "update"]
    assert update_calls, f"expected an 'apt-get ... update' call, got: {package_manager_calls!r}"
    assert "DPkg::Lock::Timeout" in update_calls[0][0]


def test_rollback_node_apt_get_install_waits_for_dpkg_lock():
    """The Debian/Ubuntu rollback counterpart: 'apt-get install <scylla_pkg_ver>' must also wait for the lock."""
    fake_self = MagicMock()
    fake_self.upgrade_rollback_mode = None
    fake_self.orig_ver = "2026.3.0"
    fake_self.new_ver = "2026.1.10"
    fake_self.params.get.return_value = False

    fake_node = MagicMock()
    fake_node.name = "node1"
    fake_node.distro.is_rhel_like = False
    fake_node.scylla_pkg.return_value = "scylla"
    fake_node.remoter.run.side_effect = _run_side_effect("2026.3.0", "2026.1.10")

    UpgradeTest._rollback_node(fake_self, node=fake_node, upgrade_sstables=False)

    package_manager_calls = _package_manager_calls(fake_node)
    install_calls = [(cmd, kwargs) for cmd, kwargs in package_manager_calls if "install" in cmd]
    assert len(install_calls) == 1, f"expected exactly one install call, got: {package_manager_calls!r}"

    cmd, kwargs = install_calls[0]
    assert "DPkg::Lock::Timeout" in cmd, f"install command missing dpkg lock-wait option: {cmd!r}"
    assert kwargs.get("retry") == 3, f"install command missing retry=3: {kwargs!r}"
    assert kwargs.get("timeout") == 600, f"install command missing timeout=600: {kwargs!r}"
