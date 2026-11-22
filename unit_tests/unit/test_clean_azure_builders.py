from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from utils.cloud_cleanup.azure import clean_azure

RG_ID = "/subscriptions/s/resourceGroups/SCT-builders-eastus/providers/Microsoft.Network"


def vm(name, **tags):
    created = datetime.now(timezone.utc) - timedelta(hours=200)
    v_m = MagicMock(tags={"creation_time": created.isoformat(sep=" ", timespec="seconds"), **tags})
    v_m.name = name
    v_m.network_profile.network_interfaces = [MagicMock(id=f"{RG_ID}/networkInterfaces/{name}NIC")]
    v_m.storage_profile.os_disk.name = f"{name}_OsDisk"
    return v_m


def group(name):
    resource_group = MagicMock(tags={"keep": "alive"})
    resource_group.name = name
    return resource_group


def test_keep_alive_group_loses_only_expired_builders_with_their_resources():
    groups = [group("SCT-builders-eastus"), group("SCT_IMAGES-eastus")]
    vms = {
        "SCT-builders-eastus": [
            vm("lost", NodeType="builder", keep="168"),
            vm("long-test", NodeType="builder", keep="504"),
        ],
        "SCT_IMAGES-eastus": [vm("not-a-builder")],
    }
    with (
        patch.object(clean_azure, "azure_service") as azure,
        patch.object(clean_azure, "update_argus_resource_status"),
    ):
        resources, compute, network = azure.resource, azure.compute, azure.network
        resources.resource_groups.list.return_value = groups
        compute.virtual_machines.list.side_effect = lambda group: vms[group]
        network.network_interfaces.get.return_value.ip_configurations = [
            MagicMock(public_ip_address=MagicMock(id=f"{RG_ID}/publicIPAddresses/lostIPName")),
            MagicMock(public_ip_address=None),
        ]
        clean_azure.clean_azure_instances()

    compute.virtual_machines.begin_delete.assert_called_once_with("SCT-builders-eastus", "lost")
    network.network_interfaces.begin_delete.assert_called_once_with("SCT-builders-eastus", "lostNIC")
    network.public_ip_addresses.begin_delete.assert_called_once_with("SCT-builders-eastus", "lostIPName")
    compute.disks.begin_delete.assert_called_once_with("SCT-builders-eastus", "lost_OsDisk")
    resources.resource_groups.begin_delete.assert_not_called()


def test_dry_run_deletes_nothing():
    with patch.object(clean_azure, "azure_service") as azure:
        compute, network = azure.compute, azure.network
        compute.virtual_machines.list.return_value = [vm("lost", NodeType="builder", keep="168")]
        clean_azure.clean_builder_vms("SCT-builders-eastus", dry_run=True)

    compute.virtual_machines.begin_delete.assert_not_called()
    network.network_interfaces.begin_delete.assert_not_called()
