from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.provisioner import PricingModel, ProvisionError


@pytest.fixture
def provider():
    """VirtualMachineProvider in test-rg with one cached VM named vm1 and a mocked Azure service."""
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
    """Reboot runs ``reboot -f`` through walinuxagent and does not restart the VM through the SDK."""
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
    """Reboot restarts the VM through the SDK and waits for the task when walinuxagent fails."""
    mock_task = MagicMock()
    provider._azure_service.compute.virtual_machines.begin_restart.return_value = mock_task
    provider._azure_service.compute.virtual_machines.instance_view.return_value.statuses = [
        MagicMock(display_status="VM running")
    ]
    provider.reboot("vm1", wait=True, hard=True)
    provider._azure_service.compute.virtual_machines.begin_restart.assert_called_once_with("test-rg", vm_name="vm1")
    mock_task.wait.assert_called_once_with()


@patch("sdcm.provision.azure.virtual_machine_provider.VirtualMachineProvider.run_command")
def test_reboot_no_wait(mock_run_command, provider):
    """Reboot with wait=False triggers ``reboot -f`` and returns without an SDK restart."""
    provider.reboot("vm1", wait=False, hard=False)
    mock_run_command.assert_called_once_with("vm1", "reboot -f")
    provider._azure_service.compute.virtual_machines.begin_restart.assert_not_called()


@pytest.mark.parametrize(
    "power_state, provisioned",
    [
        pytest.param("PowerState/starting", False, id="starting"),
        pytest.param("PowerState/running", True, id="running"),
    ],
)
def test_minicloud_vm_is_provisioned_once_running(provider, monkeypatch, power_state, provisioned):
    """_poll_provisioned_vm returns the VM only once its power state is running."""
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    vm = MagicMock(provisioning_state="Succeeded", instance_view=MagicMock(statuses=[MagicMock(code=power_state)]))
    provider._azure_service.compute.virtual_machines.get.return_value = vm
    assert provider._poll_provisioned_vm("vm1", PricingModel.ON_DEMAND) is (vm if provisioned else None)


def test_minicloud_vm_that_failed_to_launch_raises(provider, monkeypatch):
    """_poll_provisioned_vm raises ProvisionError naming the state when the VM is deallocated."""
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    provider._azure_service.compute.virtual_machines.get.return_value = MagicMock(
        provisioning_state="Succeeded",
        instance_view=MagicMock(statuses=[MagicMock(code="PowerState/deallocated")]),
    )
    with pytest.raises(ProvisionError, match="deallocated"):
        provider._poll_provisioned_vm("vm1", PricingModel.ON_DEMAND)
