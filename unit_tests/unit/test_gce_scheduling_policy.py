"""SCT-345: GCE instances must be created with on_host_maintenance=TERMINATE and
automatic_restart=False to prevent live migration from disrupting tests.

Covers both creation paths: VirtualMachineProvider (DB/loader/monitor nodes) and
sdcm.utils.gce_utils.create_instance (SCT runner VM).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
def test_instance_provider_default_branch_sets_terminate_and_no_auto_restart(_mock_sa) -> None:
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
        tags={"env": "test"},
    )
    provider._build_and_insert_instance(
        definition=definition,
        pricing_model=PricingModel.ON_DEMAND,
        user_data="",
        startup_script="",
        normalized_name="test-node-1",
    )

    instance = provider._instances_client.insert.call_args.kwargs["request"].instance_resource
    _assert_terminate_no_restart(instance)


def test_create_instance_default_branch_sets_terminate_and_no_auto_restart() -> None:
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
            disks=[compute_v1.AttachedDisk(boot=True, device_name="boot")],
            machine_type="n2-standard-1",
            network_name="default",
        )

    instance = instance_client.insert.call_args.kwargs["request"].instance_resource
    _assert_terminate_no_restart(instance)
