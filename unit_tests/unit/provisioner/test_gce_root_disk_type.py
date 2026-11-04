# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.gce.constants import (
    DISK_TYPE_HYPERDISK_BALANCED,
    DISK_TYPE_LOCAL_SSD,
)
from sdcm.provision.gce.disk_provider import DiskProvider
from sdcm.provision.gce.instance_provider import VirtualMachineProvider
from sdcm.provision.gce.instance_provider import resolve_root_disk_type as _resolve_root_disk_type
from sdcm.provision.provisioner import DataDisk, InstanceDefinition, PricingModel
from sdcm.utils.gce_machine_types import bundles_local_ssds


@pytest.mark.parametrize(
    "instance_type",
    [
        "n4a-standard-4",
        "n4a-highcpu-8",
        "n4-standard-4",
        "c4a-standard-4",
        "c4d-highmem-8-lssd",
        "z3-highmem-8-highlssd",
        "z4d-highmem-16-highlssd",
    ],
)
@pytest.mark.parametrize("configured", ["pd-standard", "pd-balanced", "pd-ssd", None])
def test_hyperdisk_only_families_override_the_configured_root_disk_type(instance_type, configured):
    assert _resolve_root_disk_type(instance_type, configured) == DISK_TYPE_HYPERDISK_BALANCED


@pytest.mark.parametrize(
    "instance_type, configured, expected",
    [
        ("e2-standard-8", "pd-standard", "pd-standard"),
        ("e2-standard-8", None, "pd-standard"),
        ("n2-standard-8", "pd-ssd", "pd-ssd"),
        ("n2d-standard-8", "pd-balanced", "pd-balanced"),
    ],
)
def test_other_families_keep_the_configured_root_disk_type(instance_type, configured, expected):
    assert _resolve_root_disk_type(instance_type, configured) == expected


@pytest.mark.parametrize(
    "instance_type, expected",
    [
        ("z3-highmem-8-highlssd", True),
        ("z3-highmem-88-standardlssd", True),
        ("z3-highmem-88", True),
        ("z4d-highmem-16-highlssd", True),
        ("z4d-highmem-16-standardlssd", True),
        ("c4-highmem-8-lssd", True),
        ("c4-highmem-288-lssd-metal", True),
        ("c4a-highmem-4-lssd", True),
        ("c4d-highmem-96-lssd", True),
        ("c4-highmem-8", False),
        ("n2-highmem-32", False),
    ],
)
def test_bundles_local_ssds(instance_type, expected):
    assert bundles_local_ssds(instance_type) is expected


@pytest.mark.parametrize(
    "instance_type, expected_local_ssds",
    [
        ("c4-highmem-8-lssd", 0),
        ("c4d-highmem-8-lssd", 0),
        ("z3-highmem-88", 0),
        ("z4d-highmem-16-highlssd", 0),
        ("c4-highmem-8", 8),
    ],
)
@patch("sdcm.provision.gce.instance_provider.get_gce_service_accounts", return_value=None)
def test_requested_local_ssds_are_dropped_on_bundled_ssd_types(_mock_sa, instance_type, expected_local_ssds):
    """SCT-1408: GCE answers `should have [1] local SSD(s)` when SCT adds requested ones on top."""
    network_provider = MagicMock()
    network_provider.get_network_tags.return_value = []
    network_provider.get_network_url.return_value = "global/networks/default"
    with patch("sdcm.provision.gce.instance_provider.get_gce_compute_instances_client", return_value=(MagicMock(), {})):
        provider = VirtualMachineProvider(
            project_id="p",
            zone="us-central1-a",
            test_id="t",
            disk_provider=DiskProvider("p", "us-central1-a"),
            network_provider=network_provider,
        )
    provider._build_and_insert_instance(
        definition=InstanceDefinition(
            name="db-1",
            image_id="projects/p/global/images/i",
            type=instance_type,
            user_name="scyllaadm",
            ssh_key=SimpleNamespace(name="k", public_key=b"ssh-rsa AAA k"),
            tags={},
            data_disks=[DataDisk(type=DISK_TYPE_LOCAL_SSD, size=375, count=8)],
        ),
        pricing_model=PricingModel.ON_DEMAND,
        user_data="",
        startup_script="",
        normalized_name="db-1",
    )

    instance = provider._instances_client.insert.call_args.kwargs["request"].instance_resource
    assert sum("-data-local-ssd-" in d.device_name for d in instance.disks) == expected_local_ssds
