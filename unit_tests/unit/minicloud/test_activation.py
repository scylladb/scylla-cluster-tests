"""Tests for minicloud activation predicates, endpoint resolution and the health probe."""

import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from sdcm.utils.azure_utils import AzureService
from sdcm.utils.metaclasses import Singleton
from sdcm.utils.minicloud.activation import (
    check_minicloud_reachability,
    set_minicloud_endpoint_env,
    validate_minicloud_params,
)
from sdcm.utils.minicloud.config import MinicloudError
from sdcm.utils.minicloud.endpoint import MinicloudActivationError, get_minicloud_endpoint, is_minicloud_active
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


def test_set_endpoint_env_exports_azure_vars_for_azure_backend():
    set_minicloud_endpoint_env("http://localhost:5000", "azure", ca_bundle="/tmp/bundle.pem")
    assert os.environ["AZURE_ENDPOINT_URL"] == "https://localhost:5443"
    assert os.environ["REQUESTS_CA_BUNDLE"] == "/tmp/bundle.pem"
    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5000"


@pytest.fixture
def fresh_azure_service(monkeypatch):
    """AzureService is a process-wide singleton: start each test from a new one"""
    Singleton._instances.pop(AzureService, None)
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    yield
    Singleton._instances.pop(AzureService, None)


@pytest.mark.parametrize(
    "client_property, client_class",
    [
        ("compute", "ComputeManagementClient"),
        ("network", "NetworkManagementClient"),
        ("resource", "ResourceManagementClient"),
        ("keyvault", "KeyVaultManagementClient"),
        ("subscription", "SubscriptionClient"),
        ("resource_graph", "ResourceGraphClient"),
    ],
)
def test_every_arm_client_is_redirected(fresh_azure_service, monkeypatch, client_property, client_class):
    monkeypatch.setenv("AZURE_ENDPOINT_URL", "https://localhost:5443")
    with (
        patch("sdcm.utils.azure_utils.KeyStore") as keystore,
        patch("sdcm.utils.azure_utils.ClientSecretCredential") as credential,
        patch(f"sdcm.utils.azure_utils.{client_class}") as client,
    ):
        _ = getattr(AzureService(), client_property)
    keystore.assert_not_called()
    credential.assert_called_once_with(
        tenant_id="00000000-0000-0000-0000-000000000000",
        client_id="minicloud",
        client_secret="minicloud",
        authority="https://localhost:5443",
        disable_instance_discovery=True,
    )
    client.assert_called_once()
    assert client.call_args.kwargs["credential"] is credential.return_value
    assert client.call_args.kwargs["base_url"] == "https://localhost:5443"
    assert client.call_args.kwargs["credential_scopes"] == ["https://localhost:5443/.default"]


def test_guard_refuses_arm_clients_before_activation(fresh_azure_service, monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    with (
        patch("sdcm.utils.azure_utils.KeyStore") as keystore,
        pytest.raises(MinicloudActivationError, match="AZURE_ENDPOINT_URL"),
    ):
        _ = AzureService().compute
    keystore.assert_not_called()
