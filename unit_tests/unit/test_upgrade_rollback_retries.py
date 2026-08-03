"""Regression tests for SCT-704: package-manager commands in rollback must retry.

A transient repo-metadata failure (e.g. Rocky 10 AppStream mirrorlist blip) causes
yum/apt-get to exit non-zero. Without ``retry=`` on the remoter call, the rollback
aborts immediately instead of retrying the transient failure. These tests exercise
``UpgradeTest._rollback_node`` directly (as an unbound method) with mocked node/self
objects and assert the package-manager commands carry ``retry=3`` (and ``timeout=600``
for downloads).
"""

from unittest import mock
from unittest.mock import MagicMock

import pytest

from upgrade_test import UpgradeTest


UpgradeTest.__test__ = False

pytestmark = pytest.mark.usefixtures("events")


def _build_fake_self(*, upgrade_rollback_mode, orig_ver, new_ver):
    """Build a MagicMock standing in for ``self`` in ``UpgradeTest._rollback_node``."""
    fake_self = MagicMock()
    fake_self.upgrade_rollback_mode = upgrade_rollback_mode
    fake_self.orig_ver = orig_ver
    fake_self.new_ver = new_ver
    fake_self.params.get.return_value = False
    return fake_self


def _run_side_effect(orig_ver, new_ver):
    """Return distinct ``scylla --version`` results across the two calls in ``_rollback_node``.

    The method calls ``scylla --version`` once before the downgrade (orig_ver) and
    once after (new_ver), then asserts they differ. Any other command gets a generic
    empty-stdout result.
    """
    call_count = {"n": 0}

    def _side_effect(cmd, *args, **kwargs):  # noqa: ARG001
        if cmd == "scylla --version":
            call_count["n"] += 1
            stdout = orig_ver if call_count["n"] == 1 else new_ver
            return MagicMock(stdout=stdout)
        return MagicMock(stdout="")

    return _side_effect


def _build_fake_node(*, is_rhel_like, orig_ver, new_ver):
    """Build a MagicMock standing in for ``node`` in ``UpgradeTest._rollback_node``."""
    fake_node = MagicMock()
    fake_node.name = "node1"
    fake_node.distro.is_rhel_like = is_rhel_like
    fake_node.scylla_pkg.return_value = "scylla"
    fake_node.remoter.run.side_effect = _run_side_effect(orig_ver, new_ver)
    return fake_node


def _package_manager_calls(fake_node):
    """Collect (cmd, kwargs) pairs for every yum/apt-get invocation on ``fake_node.remoter``."""
    calls = list(fake_node.remoter.run.call_args_list) + list(fake_node.remoter.sudo.call_args_list)
    package_manager_calls = []
    for call in calls:
        cmd = call.args[0]
        if "yum " in cmd or "apt-get " in cmd:
            package_manager_calls.append((cmd, call.kwargs))
    return package_manager_calls


def test_rollback_node_yum_downgrade_passes_retry_and_timeout():
    """The exact SCT-704 failure site: 'sudo yum downgrade scylla\\* -y' must retry."""
    fake_self = _build_fake_self(upgrade_rollback_mode=None, orig_ver="5.4.0", new_ver="6.0.0")
    fake_node = _build_fake_node(is_rhel_like=True, orig_ver="5.4.0", new_ver="6.0.0")

    UpgradeTest._rollback_node(fake_self, node=fake_node, upgrade_sstables=False)

    assert mock.call(r"sudo yum downgrade scylla\* -y", retry=3, timeout=600) in fake_node.remoter.run.call_args_list


@pytest.mark.parametrize(
    ("upgrade_rollback_mode", "is_rhel_like", "orig_ver", "new_ver"),
    [
        pytest.param(None, True, "5.4.0", "6.0.0", id="rhel-major"),
        pytest.param(None, True, "5.4.0", "5.4.5", id="rhel-minor"),
        pytest.param("reinstall", True, "5.4.0", "6.0.0", id="rhel-reinstall"),
    ],
)
def test_rollback_node_package_commands_are_retried(upgrade_rollback_mode, is_rhel_like, orig_ver, new_ver):
    """Every yum/apt-get command invoked by ``_rollback_node`` must carry retry>=3.

    Commands that download packages (install/downgrade) must additionally carry
    timeout=600; plain ``remove`` commands don't download anything and aren't
    expected to carry a timeout.
    """
    fake_self = _build_fake_self(upgrade_rollback_mode=upgrade_rollback_mode, orig_ver=orig_ver, new_ver=new_ver)
    fake_node = _build_fake_node(is_rhel_like=is_rhel_like, orig_ver=orig_ver, new_ver=new_ver)

    UpgradeTest._rollback_node(fake_self, node=fake_node, upgrade_sstables=False)

    package_manager_calls = _package_manager_calls(fake_node)
    assert package_manager_calls, "expected at least one yum/apt-get call to be exercised"
    for cmd, kwargs in package_manager_calls:
        assert kwargs.get("retry", 1) >= 3, f"command missing retry>=3: {cmd!r} (kwargs={kwargs!r})"
        if "remove" in cmd:
            continue
        assert kwargs.get("timeout") == 600, f"command missing timeout=600: {cmd!r} (kwargs={kwargs!r})"
