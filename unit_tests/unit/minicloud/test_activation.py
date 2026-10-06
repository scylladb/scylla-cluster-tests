"""Tests for minicloud activation predicates, endpoint resolution and the health probe."""

from unittest.mock import MagicMock

import pytest
import requests

from sdcm.utils.minicloud import (
    MinicloudError,
    check_minicloud_reachability,
    get_minicloud_endpoint,
    is_minicloud_active,
    validate_minicloud_params,
)
from unit_tests.unit.minicloud.conftest import _patch_probe_session


def test_is_minicloud_active_from_aws_endpoint_url(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")
    assert is_minicloud_active() is True


def test_is_minicloud_active_from_sct_env(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    assert is_minicloud_active() is True


def test_minicloud_docker_env_does_not_activate(monkeypatch):
    """No bare MINICLOUD_* env var may redirect a run by itself — activation is explicit."""
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:latest")
    assert is_minicloud_active() is False


def test_is_minicloud_active_from_params():
    """A test-case yaml can only switch minicloud on through the param, not the environment."""
    assert is_minicloud_active(params={"minicloud_endpoint_url": "http://localhost:5000"}) is True
    assert get_minicloud_endpoint(params={"minicloud_endpoint_url": "http://localhost:6000"}) == "http://localhost:6000"


def test_is_minicloud_inactive_for_empty_params():
    assert is_minicloud_active(params={}) is False
    assert is_minicloud_active(params={"minicloud_endpoint_url": ""}) is False


def test_minicloud_endpoint_env_beats_params(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:7000")
    assert get_minicloud_endpoint(params={"minicloud_endpoint_url": "http://localhost:6000"}) == "http://localhost:7000"


def test_is_minicloud_inactive_by_default():
    assert is_minicloud_active() is False


def test_is_minicloud_inactive_for_real_aws_endpoint(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://ec2.us-east-1.amazonaws.com")
    assert is_minicloud_active() is False


def test_get_minicloud_endpoint_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9999")
    assert get_minicloud_endpoint() == "http://localhost:9999"


def test_get_minicloud_endpoint_ignores_real_cloud_aws_endpoint(monkeypatch):
    """Endpoint resolution must mirror activation: only a localhost SDK override counts.

    A host exporting a real-cloud AWS_ENDPOINT_URL would otherwise hijack the endpoint of
    a yaml-activated run, and the manager would probe/start the wrong port.
    """
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://ec2.us-east-1.amazonaws.com")
    params = {"minicloud_endpoint_url": "http://localhost:6000"}
    assert get_minicloud_endpoint(params=params) == "http://localhost:6000"


def test_get_minicloud_endpoint_from_gce_env(monkeypatch):
    """GCE_ENDPOINT_URL activates minicloud, so it must resolve as an endpoint too."""
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:9099")
    assert get_minicloud_endpoint() == "http://localhost:9099"


def test_get_minicloud_endpoint_default():
    assert get_minicloud_endpoint() == "http://localhost:5000"


def test_check_minicloud_reachability_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    patcher, session = _patch_probe_session(response=mock_response)
    with patcher:
        assert check_minicloud_reachability("http://localhost:5000") is True
    assert session.post.call_args.kwargs["data"]["Action"] == "DescribeVpcs"


def test_check_minicloud_reachability_400_is_unhealthy():
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "<Error><Code>UnsupportedOperation</Code></Error>"
    patcher, _ = _patch_probe_session(response=mock_response)
    with patcher:
        with pytest.raises(RuntimeError, match="HTTP 400"):
            check_minicloud_reachability("http://localhost:5000")


def test_check_minicloud_reachability_connection_error():
    patcher, _ = _patch_probe_session(side_effect=requests.ConnectionError("refused"))
    with patcher:
        with pytest.raises(RuntimeError, match="minicloud is not reachable"):
            check_minicloud_reachability("http://localhost:5000")


def test_check_minicloud_reachability_timeout():
    patcher, _ = _patch_probe_session(side_effect=requests.Timeout("timed out"))
    with patcher:
        with pytest.raises(RuntimeError, match="timed out"):
            check_minicloud_reachability("http://localhost:5000")


def test_check_minicloud_reachability_other_requests_errors_are_runtime_errors():
    """Every probe failure must surface as RuntimeError — callers retry on that alone.

    A escaping RequestException (InvalidURL from a malformed endpoint, TooManyRedirects)
    would bypass ensure_minicloud_ready's retry loop and abort the run.
    """
    patcher, _ = _patch_probe_session(side_effect=requests.exceptions.InvalidURL("bad url"))
    with patcher:
        with pytest.raises(RuntimeError, match="probe against"):
            check_minicloud_reachability("http://localhost:5000")


def test_validate_minicloud_params_accepts_overlay_values():
    validate_minicloud_params(
        params={
            "instance_provision": "on_demand",
            "ip_ssh_connections": "private",
            "enterprise_disable_kms": True,
            "force_run_iotune": False,
        }
    )


def test_validate_minicloud_params_rejects_missing_overlay():
    """Default aws params (spot, KMS on) mean the overlay was not layered — fail fast."""
    with pytest.raises(MinicloudError, match="configurations/minicloud.yaml"):
        validate_minicloud_params(
            params={
                "instance_provision": "spot",
                "ip_ssh_connections": "private",
                "enterprise_disable_kms": False,
            }
        )
