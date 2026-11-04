"""SCT-345: GCE instances must be created with on_host_maintenance=TERMINATE and
automatic_restart=False to prevent live migration from disrupting tests.

Covers both creation paths: VirtualMachineProvider (DB/loader/monitor nodes) and
sdcm.utils.gce_utils.create_instance (SCT runner VM).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import compute_v1

from sdcm.provision.gce.instance_provider import VirtualMachineProvider
from sdcm.provision.provisioner import InstanceDefinition, PricingModel
from sdcm.utils.gce_utils import create_instance


def _scheduling_set_explicitly(instance: compute_v1.Instance, field: str) -> bool:
    """Confirm the field was explicitly set, not just proto-plus default."""
    return compute_v1.Scheduling.pb(instance.scheduling).HasField(field)


def _assert_terminate_no_restart(instance: compute_v1.Instance) -> None:
    assert instance.scheduling.on_host_maintenance == "TERMINATE"
    assert _scheduling_set_explicitly(instance, "automatic_restart")
    assert instance.scheduling.automatic_restart is False


@patch("sdcm.provision.gce.instance_provider.get_gce_service_accounts", return_value=None)
def _provider_insert_request(_mock_sa, tags: dict[str, str]) -> compute_v1.Instance:
    """Build one VirtualMachineProvider insert and return the instance it sends."""
    disk_provider = MagicMock()
    disk_provider.create_disks_config.return_value = [
        {
            "boot": True,
            "device_name": "boot",
            "initialize_params": {"source_image": "img", "disk_size_gb": 50, "disk_type": "pd-standard"},
        },
    ]
    network_provider = MagicMock()
    network_provider.get_network_tags.return_value = []
    network_provider.get_network_url.return_value = "global/networks/default"

    with patch(
        "sdcm.provision.gce.instance_provider.get_gce_compute_instances_client",
        return_value=(MagicMock(), {}),
    ):
        provider = VirtualMachineProvider(
            project_id="test-project",
            zone="us-central1-a",
            test_id="test-id",
            disk_provider=disk_provider,
            network_provider=network_provider,
        )
    definition = InstanceDefinition(
        name="test-node-1",
        image_id="projects/p/global/images/test-image",
        type="n2-standard-4",
        user_name="scyllaadm",
        ssh_key=SimpleNamespace(name="test-key", public_key=b"ssh-rsa AAA test@example.com"),
        tags=tags,
    )
    provider._build_and_insert_instance(
        definition=definition,
        pricing_model=PricingModel.ON_DEMAND,
        user_data="",
        startup_script="",
        normalized_name="test-node-1",
    )

    return provider._instances_client.insert.call_args.kwargs["request"].instance_resource


def test_instance_provider_default_branch_sets_terminate_and_no_auto_restart() -> None:
    _assert_terminate_no_restart(_provider_insert_request(tags={"env": "test"}))


def test_instance_provider_insert_carries_normalized_labels() -> None:
    instance = _provider_insert_request(tags={"NodeType": "scylla-db", "TestId": "Abc-1"})
    assert dict(instance.labels) == {"nodetype": "scylla-db", "testid": "abc-1"}


def _create_instance_insert_request(machine_type: str, disks: list[compute_v1.AttachedDisk], **kwargs):
    """Run `gce_utils.create_instance` against a mocked client and return the instance it sends."""
    instance_client = MagicMock()
    instance_client.get.return_value = compute_v1.Instance()

    with (
        patch("sdcm.utils.gce_utils.get_gce_compute_instances_client", return_value=(instance_client, {})),
        patch("sdcm.utils.gce_utils.wait_for_extended_operation"),
    ):
        create_instance(
            project_id="test-project",
            zone="us-central1-a",
            instance_name="test-runner",
            disks=disks,
            machine_type=machine_type,
            network_name="default",
            **kwargs,
        )

    return instance_client.insert.call_args.kwargs["request"].instance_resource


def test_create_instance_default_branch_sets_terminate_and_no_auto_restart() -> None:
    instance = _create_instance_insert_request(
        "n2-standard-1", [compute_v1.AttachedDisk(boot=True, device_name="boot")]
    )
    _assert_terminate_no_restart(instance)


@pytest.mark.parametrize("spot", [False, True], ids=["on-demand", "spot"])
@pytest.mark.parametrize(
    "machine_type",
    [
        "z3-highmem-8-highlssd",
        "z4d-highmem-16-highlssd",
        "z4d-highmem-16-standardlssd",
        "zones/us-central1-a/machineTypes/z4d-highmem-16-highlssd",
    ],
)
def test_create_instance_bundled_ssd_family_migrates_without_extra_local_ssds(machine_type, spot) -> None:
    """SCT-1410: GCE rejects z4d with TERMINATE, and rejects local SSDs requested on top of the bundled ones."""
    instance = _create_instance_insert_request(
        machine_type,
        [
            compute_v1.AttachedDisk(boot=True, device_name="boot"),
            compute_v1.AttachedDisk(device_name="test-runner-data-local-ssd-0"),
        ],
        spot=spot,
    )

    assert instance.scheduling.on_host_maintenance == "MIGRATE"
    assert not instance.scheduling.provisioning_model
    assert [disk.device_name for disk in instance.disks] == ["boot"]
