"""Unit tests for Azure KMS Provider."""

from unittest.mock import MagicMock, patch
import pytest

from azure.core.exceptions import AzureError, HttpResponseError
from sdcm.provision.azure.kms_provider import AzureKmsProvider
from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.provisioner import ProvisionError, InstanceDefinition


@pytest.fixture
def kms_config():
    """Mock KMS configuration."""
    return {
        "resource_group": "test-rg",
        "identity_name": "test-identity",
        "managed_identity_principal_id": "test-principal-id",
        "sct_service_principal_id": "test-service-principal-id",
        "shared_vault_name": "test-vault",
        "num_of_keys": 3,
    }


@pytest.fixture
def azure_service_mock():
    """Mock Azure service."""
    service = MagicMock()
    service.subscription_id = "test-subscription-id"
    service.azure_credentials = {"tenant_id": "test-tenant-id"}
    return service


@pytest.fixture
def mock_vault():
    """Create a mock vault with standard properties."""
    vault = MagicMock()
    vault.properties.vault_uri = "https://test-vault.vault.azure.net/"
    return vault


@pytest.fixture
def mock_vault_poller(mock_vault):
    """Create a mock poller that returns a vault."""
    poller = MagicMock()
    poller.result.return_value = mock_vault
    return poller


@pytest.fixture
def mock_http_error_factory():
    """Factory for creating mock HttpResponseErrors with different error codes."""

    def _create_error(error_code: str) -> HttpResponseError:
        error = HttpResponseError()
        mock_error_obj = MagicMock()
        mock_error_obj.code = error_code
        error.error = mock_error_obj
        return error

    return _create_error


@pytest.fixture
def mock_ssh_key():
    """Create a mock SSH key for VM testing."""
    ssh_key = MagicMock()
    ssh_key.name = "test-key"
    ssh_key.public_key = b"ssh-rsa AAAAB3..."
    return ssh_key


@pytest.fixture
def mock_logger():
    """Mock LOGGER for all tests."""
    with patch("sdcm.provision.azure.kms_provider.LOGGER") as logger:
        yield logger


@pytest.fixture
def kms_provider(kms_config, azure_service_mock):
    """Create KMS provider with mocked dependencies."""
    with patch("sdcm.provision.azure.kms_provider.KeyStore") as mock_keystore:
        mock_keystore.return_value.get_azure_kms_config.return_value = kms_config
        provider = AzureKmsProvider(
            _resource_group_name="test-rg", _region="eastus", _az="1", _azure_service=azure_service_mock
        )
        yield provider


@pytest.fixture
def vm_provider():
    """Create VM provider with mocked dependencies."""
    azure_service = MagicMock()
    azure_service.compute.virtual_machines.list.return_value = []

    provider = VirtualMachineProvider(
        _resource_group_name="SCT-test-id-123",
        _region="eastus",
        _az="1",
        _enable_azure_kms=True,
        _azure_service=azure_service,
    )
    return provider


def test_get_or_create_keyvault_returns_none_on_error(mock_logger, kms_provider, azure_service_mock):
    """Test that get_or_create_keyvault_and_identity returns None on error."""
    azure_service_mock.keyvault.vaults.begin_create_or_update.side_effect = AzureError("Test error")

    result = kms_provider.get_or_create_keyvault_and_identity("test-id-123")

    assert result is None
    assert mock_logger.error.called


def test_create_keyvault_retries_conflict_errors(
    mock_logger, kms_provider, azure_service_mock, mock_http_error_factory, mock_vault_poller
):
    """Test that ConflictError is retried and eventually succeeds."""
    conflict_error = mock_http_error_factory("ConflictError")

    azure_service_mock.keyvault.vaults.begin_create_or_update.side_effect = [
        conflict_error,
        conflict_error,
        mock_vault_poller,
    ]
    azure_service_mock.get_vault_key.return_value = True

    result = kms_provider.get_or_create_keyvault_and_identity("test-id-123")

    assert result is not None
    assert result["vault_uri"] == "https://test-vault.vault.azure.net/"
    assert azure_service_mock.keyvault.vaults.begin_create_or_update.call_count == 3
    assert mock_logger.error.call_count == 2


def test_create_keyvault_fails_immediately_on_non_conflict_errors(
    mock_logger, kms_provider, azure_service_mock, mock_http_error_factory
):
    """Test that non-ConflictError errors fail immediately without retry."""
    non_conflict_error = mock_http_error_factory("ResourceNotFound")

    azure_service_mock.keyvault.vaults.begin_create_or_update.side_effect = non_conflict_error

    result = kms_provider.get_or_create_keyvault_and_identity("test-id-123")

    assert result is None
    assert azure_service_mock.keyvault.vaults.begin_create_or_update.call_count == 1
    assert mock_logger.error.called


def test_get_or_create_keyvault_success(mock_logger, kms_provider, azure_service_mock, mock_vault_poller):
    """Test successful vault creation."""
    azure_service_mock.keyvault.vaults.begin_create_or_update.return_value = mock_vault_poller
    azure_service_mock.get_vault_key.return_value = True

    result = kms_provider.get_or_create_keyvault_and_identity("test-id-123")

    assert result is not None
    assert "identity_id" in result
    assert "vault_uri" in result
    assert "key_uri" in result
    assert result["vault_uri"] == "https://test-vault.vault.azure.net/"


def test_vm_creation_fails_when_kms_returns_none(vm_provider, mock_ssh_key):
    """Test that VM creation fails with ProvisionError when KMS setup returns None."""
    with patch("sdcm.provision.azure.virtual_machine_provider.AzureKmsProvider") as mock_kms_class:
        mock_kms_instance = MagicMock()
        mock_kms_instance.get_or_create_keyvault_and_identity.return_value = None
        mock_kms_class.return_value = mock_kms_instance

        definition = InstanceDefinition(
            name="test-vm",
            image_id="test-image",
            type="Standard_D2_v5",
            user_name="ubuntu",
            ssh_key=mock_ssh_key,
            tags={},
            root_disk_size=30,
            user_data=None,
        )

        with pytest.raises(ProvisionError) as exc_info:
            vm_provider.get_or_create(definitions=[definition], nics_ids=["test-nic-id"], pricing_model=MagicMock())

        assert "Failed to setup Azure KMS" in str(exc_info.value)
        assert "Key Vault creation failed" in str(exc_info.value)
