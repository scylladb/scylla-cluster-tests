"""Tests for the GCE client plumbing that routes to minicloud via GCE_ENDPOINT_URL."""

from unittest.mock import patch

from google.api_core.client_options import ClientOptions

from sdcm.utils.gce_region import GceRegion
from sdcm.utils.gce_utils import _gce_client_options

_FAKE_GCP_INFO = {
    "project_id": "test-project",
    "type": "service_account",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "private_key_id": "key-id",
    "private_key": "",
    "client_id": "123",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


def _make_gce_region():
    """Build a GceRegion with the keystore and every GCP client patched out."""
    with (
        patch("sdcm.utils.gce_region.KeyStore") as mock_keystore_cls,
        patch("sdcm.utils.gce_region.service_account.Credentials.from_service_account_info"),
        patch("sdcm.utils.gce_region.build"),
        patch("sdcm.utils.gce_region.compute_v1.NetworksClient"),
        patch("sdcm.utils.gce_region.compute_v1.FirewallsClient"),
        patch("sdcm.utils.gce_region.compute_v1.SubnetworksClient"),
        patch("sdcm.utils.gce_region.compute_v1.RoutesClient"),
        patch("sdcm.utils.gce_region.storage.Client"),
    ):
        mock_keystore_cls.return_value.get_gcp_credentials.return_value = _FAKE_GCP_INFO
        return GceRegion("us-central1")


def test_gce_client_options_returns_client_options_when_endpoint_set(monkeypatch):
    """With GCE_ENDPOINT_URL set, the GCE client options point the API endpoint at it."""
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:9099")
    result = _gce_client_options()

    assert "client_options" in result
    assert isinstance(result["client_options"], ClientOptions)
    assert result["client_options"].api_endpoint == "http://localhost:9099"


def test_gce_client_options_returns_empty_dict_when_no_endpoint():
    """Without GCE_ENDPOINT_URL, the GCE client options are an empty dict."""
    result = _gce_client_options()

    assert result == {}


def test_gce_region_is_minicloud_true_when_endpoint_set(monkeypatch):
    """A GceRegion reports minicloud mode when GCE_ENDPOINT_URL is set."""
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:9099")

    region = _make_gce_region()
    assert region._is_minicloud is True


def test_gce_region_is_minicloud_false_when_no_endpoint():
    """A GceRegion reports non-minicloud mode when GCE_ENDPOINT_URL is unset."""
    region = _make_gce_region()
    assert region._is_minicloud is False
