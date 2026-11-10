"""Unit tests for the GCP KMS client wrapper."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import AlreadyExists, NotFound

from sdcm.utils.gcp_kms import GcpKms


@pytest.fixture
def kms_client(monkeypatch):
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)
    keystore = MagicMock()
    keystore.get_gcp_kms_config.return_value = {"keyring_name": "test-ring"}
    with (
        patch("sdcm.utils.gcp_kms.KeyStore", return_value=keystore),
        patch("sdcm.utils.gcp_kms.service_account.Credentials.from_service_account_info"),
        patch("sdcm.utils.gcp_kms.kms.KeyManagementServiceClient") as client_cls,
    ):
        yield client_cls


def test_ensure_key_ring_creates_missing_ring(kms_client):
    kms_client.return_value.get_key_ring.side_effect = NotFound("no ring")

    GcpKms("proj", "us-east1", "scylla-key-1").ensure_key_ring()

    kms_client.return_value.create_key_ring.assert_called_once_with(
        parent="projects/proj/locations/us-east1", key_ring_id="test-ring", key_ring={}
    )


def test_ensure_key_ring_keeps_existing_ring(kms_client):
    GcpKms("proj", "us-east1", "scylla-key-1").ensure_key_ring()

    kms_client.return_value.get_key_ring.assert_called_once_with(
        name="projects/proj/locations/us-east1/keyRings/test-ring"
    )
    kms_client.return_value.create_key_ring.assert_not_called()


def test_ensure_key_ring_accepts_concurrently_created_ring(kms_client):
    kms_client.return_value.get_key_ring.side_effect = NotFound("no ring")
    kms_client.return_value.create_key_ring.side_effect = AlreadyExists("created by another run")

    GcpKms("proj", "us-east1", "scylla-key-1").ensure_key_ring()


def test_client_uses_rest_transport_on_custom_endpoint(kms_client, monkeypatch):
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:5000")

    GcpKms("proj", "us-east1", "scylla-key-1")

    kwargs = kms_client.call_args.kwargs
    assert kwargs["transport"] == "rest"
    assert kwargs["client_options"].api_endpoint == "http://localhost:5000"


def test_client_uses_default_transport_without_endpoint(kms_client):
    GcpKms("proj", "us-east1", "scylla-key-1")

    assert "transport" not in kms_client.call_args.kwargs
    assert "client_options" not in kms_client.call_args.kwargs
