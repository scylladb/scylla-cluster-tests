from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider


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
