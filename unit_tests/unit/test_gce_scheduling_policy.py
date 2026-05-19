"""SCT-345: GCE instances must be created with on_host_maintenance=TERMINATE and
automatic_restart=False to prevent live migration from disrupting tests.

On branch-2026.1 all GCE instances (DB/loader/monitor nodes and the SCT runner VM) are
created via sdcm.utils.gce_utils.create_instance, so covering that path covers them all.
"""

from unittest.mock import MagicMock, patch

from google.cloud import compute_v1

from sdcm.utils.gce_utils import create_instance


def _scheduling_set_explicitly(instance: compute_v1.Instance, field: str) -> bool:
    """Confirm the field was explicitly set, not just proto-plus default."""
    return compute_v1.Scheduling.pb(instance.scheduling).HasField(field)


def _create_instance_with_mocked_client(machine_type: str) -> compute_v1.Instance:
    instance_client = MagicMock()
    instance_client.get.return_value = compute_v1.Instance()

    with (
        patch("sdcm.utils.gce_utils.get_gce_compute_instances_client", return_value=(instance_client, {})),
        patch("sdcm.utils.gce_utils.wait_for_extended_operation"),
    ):
        create_instance(
            project_id="test-project",
            zone="us-central1-a",
            instance_name="test-node",
            disks=[compute_v1.AttachedDisk(boot=True, device_name="boot")],
            machine_type=machine_type,
            network_name="default",
        )

    return instance_client.insert.call_args.kwargs["request"].instance_resource


def test_create_instance_default_branch_sets_terminate_and_no_auto_restart() -> None:
    instance = _create_instance_with_mocked_client("n2-standard-1")
    assert instance.scheduling.on_host_maintenance == "TERMINATE"
    assert _scheduling_set_explicitly(instance, "automatic_restart")
    assert instance.scheduling.automatic_restart is False


def test_create_instance_e2_keeps_migrate() -> None:
    instance = _create_instance_with_mocked_client("e2-standard-4")
    assert instance.scheduling.on_host_maintenance == "MIGRATE"


def test_create_instance_z3_keeps_migrate() -> None:
    instance = _create_instance_with_mocked_client("z3-highmem-8")
    assert instance.scheduling.on_host_maintenance == "MIGRATE"
    assert not _scheduling_set_explicitly(instance, "automatic_restart")
