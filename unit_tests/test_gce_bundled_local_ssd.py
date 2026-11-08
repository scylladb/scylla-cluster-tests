"""SCT-1410: GCE machine types with bundled local SSDs (z3, z4d) are created with MIGRATE
and without extra local SSDs, in `sdcm.utils.gce_utils.create_instance`.
"""

from unittest.mock import MagicMock, patch

import pytest
from google.cloud import compute_v1

from sdcm.utils.gce_machine_types import bundles_local_ssds, is_bundled_local_ssd_family
from sdcm.utils.gce_utils import create_instance


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


def _disks() -> list[compute_v1.AttachedDisk]:
    return [
        compute_v1.AttachedDisk(boot=True, device_name="boot"),
        compute_v1.AttachedDisk(device_name="test-runner-data-local-ssd-0"),
    ]


@pytest.mark.parametrize(
    "machine_type, bundled_family, bundles",
    [
        ("z3-highmem-88", True, True),
        ("z3-highmem-8-highlssd", True, True),
        ("z4d-highmem-16-highlssd", True, True),
        ("z4d-highmem-16-standardlssd", True, True),
        ("zones/us-east4-a/machineTypes/z4d-highmem-16-highlssd", True, True),
        ("c4-standard-8-lssd", False, True),
        ("n2-highmem-8", False, False),
    ],
)
def test_machine_type_traits(machine_type, bundled_family, bundles) -> None:
    assert is_bundled_local_ssd_family(machine_type) is bundled_family
    assert bundles_local_ssds(machine_type) is bundles


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
    """GCE rejects z4d with TERMINATE, and rejects local SSDs requested on top of the bundled ones."""
    instance = _create_instance_insert_request(machine_type, _disks(), spot=spot)

    assert instance.scheduling.on_host_maintenance == "MIGRATE"
    assert not instance.scheduling.provisioning_model
    assert [disk.device_name for disk in instance.disks] == ["boot"]


def test_create_instance_other_machine_type_keeps_local_ssds_and_spot() -> None:
    instance = _create_instance_insert_request("n2-highmem-8", _disks(), spot=True)

    assert instance.scheduling.on_host_maintenance != "MIGRATE"
    assert instance.scheduling.provisioning_model == compute_v1.Scheduling.ProvisioningModel.SPOT.name
    assert [disk.device_name for disk in instance.disks] == ["boot", "test-runner-data-local-ssd-0"]
