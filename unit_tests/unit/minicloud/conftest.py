"""Shared fixtures and helpers for the minicloud unit-test package."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def no_atexit(monkeypatch):
    """start() registers no atexit teardown, so no test runs a real `docker rm -f` at interpreter exit."""
    monkeypatch.setattr("sdcm.utils.minicloud.manager.atexit.register", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def container_mode(monkeypatch):
    """Pin the minicloud default to container mode, so tests take the same path on macOS and Linux."""
    # Native-mode tests pass native=True.
    monkeypatch.setattr("sdcm.utils.minicloud.config.MINICLOUD_NATIVE_DEFAULT", False)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Clear the endpoint and region variables for each test and restore whatever the code under test sets."""
    # set_env_overrides() writes straight into os.environ. A setenv/delenv pair makes
    # monkeypatch record each variable even when it was absent, so it is restored afterwards.
    # A leaked AWS_ENDPOINT_URL would make is_minicloud_active() true for later tests.
    for name in (
        "AWS_ENDPOINT_URL",
        "GCE_ENDPOINT_URL",
        "SCT_MINICLOUD_ENDPOINT_URL",
        "AZURE_ENDPOINT_URL",
        "REQUESTS_CA_BUNDLE",
        # region resolution reads this; a developer's exported value must not decide what
        # these tests assert
        "SCT_REGION_NAME",
    ):
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)


@pytest.fixture(autouse=True)
def no_mem_in_use():
    """Pin the container's memory in use to zero, so no test calls a real docker daemon."""
    with patch("sdcm.utils.minicloud.preflight.container_mem_in_use_gib", return_value=0.0):
        yield


@pytest.fixture(autouse=True)
def no_running_minicloud():
    """Pin the running-minicloud lookup to none, so no test probes a real endpoint or docker daemon."""
    with patch("sdcm.utils.minicloud.manager.MinicloudManager.running_lightweight_cmd", return_value=None):
        yield


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
