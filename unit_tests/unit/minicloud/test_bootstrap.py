"""Tests for ensure_minicloud_ready: adopt-a-healthy-container vs auto-start bootstrap."""

import os
from unittest.mock import MagicMock, patch

from sdcm.utils.minicloud import ensure_minicloud_ready


def test_ensure_minicloud_ready_retries_then_succeeds(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")

    call_count = {"n": 0}

    def mock_check(endpoint=None, timeout=5):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise RuntimeError("not reachable")
        return True

    with patch("sdcm.utils.minicloud.bootstrap.check_minicloud_reachability", side_effect=mock_check):
        with patch("sdcm.utils.minicloud.bootstrap.time.sleep"):
            ensure_minicloud_ready()

    assert call_count["n"] == 3
    assert os.environ.get("AWS_ENDPOINT_URL") == "http://localhost:5000"


def test_ensure_minicloud_ready_healthy_path_sets_gce_endpoint(monkeypatch):
    """A pre-started container adopted by a GCE run must set GCE_ENDPOINT_URL too.

    Env changes from an earlier start-minicloud process never propagate to the next
    hydra invocation, so the healthy path has to set the full per-backend endpoint set.
    """
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    # a stale endpoint from an earlier container on a different port must be overwritten
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9999")

    with patch("sdcm.utils.minicloud.bootstrap.check_minicloud_reachability", return_value=True):
        ensure_minicloud_ready(backend="gce")

    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5000"
    assert os.environ["GCE_ENDPOINT_URL"] == "http://localhost:5000"


def test_ensure_minicloud_ready_falls_through_to_auto_start(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")

    def mock_check_always_fails(endpoint=None, timeout=5):
        raise RuntimeError("not reachable")

    mock_manager = MagicMock()

    with patch("sdcm.utils.minicloud.bootstrap.check_minicloud_reachability", side_effect=mock_check_always_fails):
        with patch("sdcm.utils.minicloud.bootstrap.time.sleep"):
            with patch("sdcm.utils.minicloud.bootstrap.MinicloudConfig.from_env"):
                with patch("sdcm.utils.minicloud.bootstrap.MinicloudManager", return_value=mock_manager):
                    ensure_minicloud_ready(backend="aws")

    # Full preflight: this path can be the FIRST container start (standalone run-test /
    # provision-resources), so credential validation must not be skipped here.
    mock_manager.preflight_check.assert_called_once_with(params=None)
    mock_manager.start.assert_called_once()
    mock_manager.prepare_regions.assert_called_once()
