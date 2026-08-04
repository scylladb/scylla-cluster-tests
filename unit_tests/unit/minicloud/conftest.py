"""Shared fixtures and helpers for the minicloud unit-test package."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def no_atexit(monkeypatch):
    # start() registers an atexit teardown; in tests that would run a real
    # `docker rm -f` at interpreter exit, escaping the unit-test sandbox.
    monkeypatch.setattr("sdcm.utils.minicloud.manager.atexit.register", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    # MinicloudManager.set_env_overrides() writes straight into os.environ (that is its
    # job - it configures the SCT process). monkeypatch cannot undo what it never saw, so
    # snapshot and restore the whole environment: a leaked AWS_ENDPOINT_URL makes
    # is_minicloud_active() true for every test that runs afterwards in this process,
    # which silently disables AZ/region fallback and reds the provisioner suite.
    original_env = os.environ.copy()
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("SCT_MINICLOUD_ENDPOINT_URL", raising=False)
    # region resolution reads this; a developer's exported value must not decide what
    # these tests assert
    monkeypatch.delenv("SCT_REGION_NAME", raising=False)
    yield
    os.environ.clear()
    os.environ.update(original_env)


def _patch_probe_session(response=None, side_effect=None):
    """Patch the retry session used by check_minicloud_reachability's POST probe."""
    session = MagicMock()
    if side_effect is not None:
        session.post.side_effect = side_effect
    else:
        session.post.return_value = response
    return patch("sdcm.utils.minicloud.activation.create_retry_session", return_value=session), session


def _meminfo_path_patch(available_kib):
    """Patch sdcm.utils.minicloud.preflight.Path so /proc/meminfo reports the given MemAvailable."""
    mock_meminfo = MagicMock()
    mock_meminfo.exists.return_value = True
    mock_meminfo.read_text.return_value = f"MemTotal: 999999999 kB\nMemAvailable: {available_kib} kB\n"

    def path_side_effect(arg):
        if str(arg) == "/proc/meminfo":
            return mock_meminfo
        return Path(arg)

    return patch("sdcm.utils.minicloud.preflight.Path", side_effect=path_side_effect)
