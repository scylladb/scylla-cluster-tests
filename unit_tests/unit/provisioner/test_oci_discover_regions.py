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

from sdcm.provision.oci import provisioner as oci_provisioner
from sdcm.provision.oci.provisioner import OciProvisioner
from sdcm.utils.oci_utils import SUPPORTED_REGIONS

TEST_ID = "bf18675a-e1f6-4d6e-a30b-e29bac62991d"


@pytest.fixture
def fake_vm_provider():
    """VirtualMachineProvider whose only test instance runs in us-phoenix-1, AD 3."""

    def make_provider(compartment_id, region, az, oci_service):
        provider = MagicMock(region=region)
        provider.list_instances.return_value = (
            [SimpleNamespace(availability_domain="ewbj:PHX-AD-3")] if region == "us-phoenix-1" else []
        )
        return provider

    with (
        patch.object(oci_provisioner, "VirtualMachineProvider", side_effect=make_provider) as provider_class,
        patch.object(oci_provisioner, "get_oci_compartment_id", return_value="compartment"),
    ):
        yield provider_class


def test_discover_regions_searches_all_supported_regions_by_default(fake_vm_provider):
    """Test that a test in us-phoenix-1 is found without naming its region, as Azure discovery does."""
    provisioners = OciProvisioner.discover_regions(test_id=TEST_ID, oci_service=MagicMock())

    searched = [call.args[1] for call in fake_vm_provider.call_args_list if call.args[2] == ""]
    assert searched == SUPPORTED_REGIONS
    assert [(p.region, p.availability_zone) for p in provisioners] == [("us-phoenix-1", "3")]


def test_discover_regions_searches_only_given_regions(fake_vm_provider):
    """Test that explicitly given regions limit the search, for example to us-ashburn-1 only."""
    provisioners = OciProvisioner.discover_regions(test_id=TEST_ID, regions=["us-ashburn-1"], oci_service=MagicMock())

    searched = [call.args[1] for call in fake_vm_provider.call_args_list if call.args[2] == ""]
    assert searched == ["us-ashburn-1"]
    assert provisioners == []
