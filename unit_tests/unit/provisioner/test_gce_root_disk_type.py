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
from sdcm.provision.gce.instance_provider import VirtualMachineProvider, resolve_root_disk_type
from sdcm.provision.provisioner import DataDisk, InstanceDefinition, PricingModel
from sdcm.utils.gce_machine_types import bundles_local_ssds


@pytest.mark.parametrize(
    "instance_type",
    [
        pytest.param("n4a-standard-4", id="n4a-standard-4"),
        pytest.param("n4a-highcpu-8", id="n4a-highcpu-8"),
        pytest.param("n4-standard-4", id="n4-standard-4"),
        pytest.param("c4a-standard-4", id="c4a-standard-4"),
        pytest.param("c4d-highmem-8-lssd", id="c4d-highmem-8-lssd"),
        pytest.param("z3-highmem-8-highlssd", id="z3-highmem-8-highlssd"),
        pytest.param("z4d-highmem-16-highlssd", id="z4d-highmem-16-highlssd"),
    ],
)
@pytest.mark.parametrize(
    "configured",
    [
        pytest.param("pd-standard", id="pd-standard"),
        pytest.param("pd-balanced", id="pd-balanced"),
        pytest.param("pd-ssd", id="pd-ssd"),
        pytest.param(None, id="none"),
    ],
)
def test_hyperdisk_only_families_override_the_configured_root_disk_type(instance_type, configured):
    """resolve_root_disk_type returns hyperdisk-balanced for hyperdisk-only families whatever the configured type."""
    assert resolve_root_disk_type(instance_type, configured) == DISK_TYPE_HYPERDISK_BALANCED


@pytest.mark.parametrize(
    "instance_type, configured, expected",
    [
        pytest.param("e2-standard-8", "pd-standard", "pd-standard", id="e2-pd-standard"),
        pytest.param("e2-standard-8", None, "pd-standard", id="e2-default-root"),
        pytest.param("n2-standard-8", "pd-ssd", "pd-ssd", id="n2-pd-ssd"),
        pytest.param("n2d-standard-8", "pd-balanced", "pd-balanced", id="n2d-pd-balanced"),
    ],
)
def test_other_families_keep_the_configured_root_disk_type(instance_type, configured, expected):
    """resolve_root_disk_type keeps the configured root disk type for families without the hyperdisk-only restriction."""
    assert resolve_root_disk_type(instance_type, configured) == expected


@pytest.mark.parametrize(
    "instance_type, expected",
    [
        pytest.param("z3-highmem-8-highlssd", True, id="z3-highlssd"),
        pytest.param("z3-highmem-88-standardlssd", True, id="z3-standardlssd"),
        pytest.param("z3-highmem-88", True, id="z3-bundled"),
        pytest.param("z4d-highmem-16-highlssd", True, id="z4d-highlssd"),
        pytest.param("z4d-highmem-16-standardlssd", True, id="z4d-standardlssd"),
        pytest.param("c4-highmem-8-lssd", True, id="c4-lssd"),
        pytest.param("c4-highmem-288-lssd-metal", True, id="c4-lssd-metal"),
        pytest.param("c4a-highmem-4-lssd", True, id="c4a-lssd"),
        pytest.param("c4d-highmem-96-lssd", True, id="c4d-lssd"),
        pytest.param("c4-highmem-8", False, id="c4-no-lssd"),
        pytest.param("n2-highmem-32", False, id="n2-no-lssd"),
    ],
)
def test_bundles_local_ssds(instance_type, expected):
    """bundles_local_ssds is True only for machine types that include local SSDs in the bundle."""
    assert bundles_local_ssds(instance_type) is expected


@pytest.mark.parametrize(
    "instance_type, expected_local_ssds",
    [
        pytest.param("c4-highmem-8-lssd", 0, id="c4-lssd"),
        pytest.param("c4d-highmem-8-lssd", 0, id="c4d-lssd"),
        pytest.param("z3-highmem-88", 0, id="z3-bundled"),
        pytest.param("z4d-highmem-16-highlssd", 0, id="z4d-highlssd"),
        pytest.param("c4-highmem-8", 8, id="c4-no-lssd"),
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
