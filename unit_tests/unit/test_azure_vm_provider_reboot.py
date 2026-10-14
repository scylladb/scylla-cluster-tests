from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.provisioner import PricingModel


@pytest.fixture
def provider():
    provider = VirtualMachineProvider(
        _resource_group_name="test-rg",
        _region="eastus",
        _az="1",
        _azure_service=MagicMock(),
    )
    provider._cache = {"vm1": MagicMock(name="vm1")}
    return provider


@patch("sdcm.provision.azure.virtual_machine_provider.time.sleep")
@patch("sdcm.provision.azure.virtual_machine_provider.VirtualMachineProvider.run_command")
def test_reboot_soft_success(mock_run_command, mock_sleep, provider):
    provider._azure_service.compute.virtual_machines.instance_view.return_value.statuses = [
        MagicMock(display_status="VM running")
    ]
    provider.reboot("vm1", wait=True, hard=False)
    mock_run_command.assert_called_once_with("vm1", "reboot -f")
    provider._azure_service.compute.virtual_machines.begin_restart.assert_not_called()


@patch(
    "sdcm.provision.azure.virtual_machine_provider.VirtualMachineProvider.run_command", side_effect=Exception("fail")
)
def test_reboot_fallback_to_sdk(mock_run_command, provider):
    mock_task = MagicMock()
    provider._azure_service.compute.virtual_machines.begin_restart.return_value = mock_task
    provider._azure_service.compute.virtual_machines.instance_view.return_value.statuses = [
        MagicMock(display_status="VM running")
    ]
    provider.reboot("vm1", wait=True, hard=True)
    provider._azure_service.compute.virtual_machines.begin_restart.assert_called_once_with("test-rg", vm_name="vm1")
    mock_task.wait.assert_called_once()


@patch("sdcm.provision.azure.virtual_machine_provider.VirtualMachineProvider.run_command")
def test_reboot_no_wait(mock_run_command, provider):
    provider.reboot("vm1", wait=False, hard=False)
    mock_run_command.assert_called_once_with("vm1", "reboot -f")
    provider._azure_service.compute.virtual_machines.begin_restart.assert_not_called()


def test_starting_vm_is_not_provisioned_under_minicloud(provider, monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    provider._azure_service.compute.virtual_machines.get.return_value = MagicMock(
        provisioning_state="Succeeded",
        instance_view=MagicMock(statuses=[MagicMock(code="PowerState/starting")]),
    )
    assert provider._poll_provisioned_vm("vm1", PricingModel.ON_DEMAND) is None
