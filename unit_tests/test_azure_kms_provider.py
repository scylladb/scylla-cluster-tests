"""Unit tests for Azure KMS Provider."""
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from azure.core.exceptions import AzureError, HttpResponseError
from sdcm.provision.azure.kms_provider import AzureKmsProvider
from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.provisioner import ProvisionError, InstanceDefinition


class MockError:
    """Mock error object for HttpResponseError."""
    def __init__(self, code):
        self.code = code


class TestAzureKmsProvider:
    """Test Azure KMS Provider error handling."""

    @pytest.fixture
    def kms_config(self):
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
    def azure_service_mock(self):
        """Mock Azure service."""
        service = MagicMock()
        service.subscription_id = "test-subscription-id"
        service.azure_credentials = {"tenant_id": "test-tenant-id"}
        return service

    @pytest.fixture
    def kms_provider(self, kms_config, azure_service_mock):
        """Create KMS provider with mocked dependencies."""
        with patch('sdcm.provision.azure.kms_provider.KeyStore') as mock_keystore:
            mock_keystore.return_value.get_azure_kms_config.return_value = kms_config
            provider = AzureKmsProvider(
                _resource_group_name="test-rg",
                _region="eastus",
                _az="1",
                _azure_service=azure_service_mock
            )
            yield provider

    def test_is_conflict_error_with_http_response_error(self, kms_provider):
        """Test conflict error detection with HttpResponseError."""
        # Test with error code
        error = HttpResponseError()
        error.error = MockError('ConflictError')
        assert kms_provider._is_conflict_error(error) is True

        # Test with different error code
        error = HttpResponseError()
        error.error = MockError('NotFoundError')
        assert kms_provider._is_conflict_error(error) is False

    def test_is_conflict_error_with_message(self, kms_provider):
        """Test conflict error detection from error message."""
        # Mock HttpResponseError with ConflictError in message
        error = HttpResponseError("A conflict occurred that prevented the operation")
        assert kms_provider._is_conflict_error(error) is True

    def test_is_conflict_error_with_regular_error(self, kms_provider):
        """Test conflict error detection with regular AzureError."""
        error = AzureError("Some other error")
        assert kms_provider._is_conflict_error(error) is False

    @patch('sdcm.provision.azure.kms_provider.LOGGER')
    def test_get_or_create_keyvault_returns_none_on_error(self, mock_logger, kms_provider, azure_service_mock):
        """Test that get_or_create_keyvault_and_identity returns None on error."""
        # Mock vault creation to raise an error
        azure_service_mock.keyvault.vaults.begin_create_or_update.side_effect = AzureError("Test error")
        
        result = kms_provider.get_or_create_keyvault_and_identity("test-id-123")
        
        assert result is None
        # Verify error was logged
        assert mock_logger.error.called

    @patch('sdcm.provision.azure.kms_provider.LOGGER')
    def test_get_or_create_keyvault_success(self, mock_logger, kms_provider, azure_service_mock):
        """Test successful vault creation."""
        # Mock successful vault creation
        mock_vault = MagicMock()
        mock_vault.properties.vault_uri = "https://test-vault.vault.azure.net/"
        
        mock_poller = MagicMock()
        mock_poller.result.return_value = mock_vault
        azure_service_mock.keyvault.vaults.begin_create_or_update.return_value = mock_poller
        azure_service_mock.get_vault_key.return_value = True  # Keys already exist
        
        result = kms_provider.get_or_create_keyvault_and_identity("test-id-123")
        
        assert result is not None
        assert "identity_id" in result
        assert "vault_uri" in result
        assert "key_uri" in result
        assert result["vault_uri"] == "https://test-vault.vault.azure.net/"


class TestVirtualMachineProviderKmsHandling:
    """Test Virtual Machine Provider KMS handling."""

    @pytest.fixture
    def vm_provider(self):
        """Create VM provider with mocked dependencies."""
        azure_service = MagicMock()
        azure_service.compute.virtual_machines.list.return_value = []
        
        provider = VirtualMachineProvider(
            _resource_group_name="SCT-test-id-123",
            _region="eastus",
            _az="1",
            _enable_azure_kms=True,
            _azure_service=azure_service
        )
        return provider

    def test_vm_creation_fails_when_kms_returns_none(self, vm_provider):
        """Test that VM creation fails with ProvisionError when KMS setup returns None."""
        # Mock KMS provider to return None
        with patch('sdcm.provision.azure.virtual_machine_provider.AzureKmsProvider') as mock_kms_class:
            mock_kms_instance = MagicMock()
            mock_kms_instance.get_or_create_keyvault_and_identity.return_value = None
            mock_kms_class.return_value = mock_kms_instance
            
            # Create a minimal instance definition
            definition = InstanceDefinition(
                name="test-vm",
                image_id="test-image",
                type="Standard_D2_v5",
                user_name="ubuntu",
                ssh_key=MagicMock(name="test-key"),
                tags={},
                root_disk_size=30,
                user_data=None
            )
            
            # Should raise ProvisionError when vault_info is None
            with pytest.raises(ProvisionError) as exc_info:
                vm_provider.get_or_create(
                    definitions=[definition],
                    nics_ids=["test-nic-id"],
                    pricing_model=MagicMock()
                )
            
            assert "Failed to setup Azure KMS" in str(exc_info.value)
            assert "Key Vault creation failed" in str(exc_info.value)

    def test_vm_creation_succeeds_with_valid_kms(self, vm_provider):
        """Test that VM creation succeeds when KMS setup returns valid vault_info."""
        with patch('sdcm.provision.azure.virtual_machine_provider.AzureKmsProvider') as mock_kms_class:
            mock_kms_instance = MagicMock()
            mock_kms_instance.get_or_create_keyvault_and_identity.return_value = {
                "identity_id": "/subscriptions/test/resourcegroups/test/providers/Microsoft.ManagedIdentity/userAssignedIdentities/test",
                "vault_uri": "https://test-vault.vault.azure.net/",
                "key_uri": "https://test-vault.vault.azure.net/scylla-key-1"
            }
            mock_kms_class.return_value = mock_kms_instance
            
            # Mock VM creation
            mock_poller = MagicMock()
            mock_poller.wait.return_value = None
            vm_provider._azure_service.compute.virtual_machines.begin_create_or_update.return_value = mock_poller
            
            mock_vm = MagicMock()
            mock_vm.name = "test-vm"
            mock_vm.instance_view = None
            vm_provider._azure_service.compute.virtual_machines.get.return_value = mock_vm
            
            # Create a minimal instance definition
            definition = InstanceDefinition(
                name="test-vm",
                image_id="test-image",
                type="Standard_D2_v5",
                user_name="ubuntu",
                ssh_key=MagicMock(name="test-key"),
                tags={},
                root_disk_size=30,
                user_data=None
            )
            
            # Should succeed
            result = vm_provider.get_or_create(
                definitions=[definition],
                nics_ids=["test-nic-id"],
                pricing_model=MagicMock()
            )
            
            assert len(result) == 1
            assert result[0].name == "test-vm"
