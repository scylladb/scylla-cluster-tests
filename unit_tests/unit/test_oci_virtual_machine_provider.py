"""Unit tests for OCI virtual machine provider DNS and boot volume behavior."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.oci.virtual_machine_provider import VirtualMachineProvider
from sdcm.utils.oci_utils import MIN_BOOT_VOLUME_SIZE_IN_GBS, build_image_source_details


@patch("sdcm.utils.oci_utils.OciService.get_block_storage_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_identity_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_compute_client", return_value=MagicMock())
def test_build_primary_vnic_details_enables_private_dns(
    mock_compute,
    mock_identity,
    mock_network,
    mock_bs,
) -> None:
    """Test that primary VNIC details include private DNS record and valid hostname label."""
    provider = VirtualMachineProvider(
        _compartment_id="ocid1.compartment.oc1..example",
        _region="us-ashburn-1",
        _az="1",
    )
    definition = SimpleNamespace(name="test-oci-node-a", use_public_ip=False)

    vnic_details = provider._build_primary_vnic_details(
        definition=definition,
        subnet_id="ocid1.subnet.oc1..example",
        nsg_id="ocid1.networksecuritygroup.oc1..example",
    )

    assert vnic_details.subnet_id == "ocid1.subnet.oc1..example"
    assert vnic_details.assign_public_ip is False
    assert vnic_details.assign_private_dns_record is True
    assert vnic_details.hostname_label
    assert len(vnic_details.hostname_label) <= 63
    assert vnic_details.nsg_ids == ["ocid1.networksecuritygroup.oc1..example"]


@patch("oci.pagination.list_call_get_all_results", side_effect=lambda func, **kwargs: func(**kwargs))
@patch("sdcm.utils.oci_utils.OciService.get_block_storage_client", return_value=MagicMock())
@patch(
    "sdcm.utils.oci_utils.OciService.get_network_client",
    return_value=MagicMock(
        get_vnic=MagicMock(
            return_value=SimpleNamespace(
                data=SimpleNamespace(hostname_label="db-host", subnet_id="ocid1.subnet.oc1..example")
            )
        ),
        get_subnet=MagicMock(
            return_value=SimpleNamespace(data=SimpleNamespace(dns_label="sctprvtest", vcn_id="ocid1.vcn.oc1..example"))
        ),
        get_vcn=MagicMock(return_value=SimpleNamespace(data=SimpleNamespace(dns_label="sct2vcn"))),
    ),
)
@patch("sdcm.utils.oci_utils.OciService.get_identity_client", return_value=MagicMock())
@patch(
    "sdcm.utils.oci_utils.OciService.get_compute_client",
    return_value=MagicMock(
        list_vnic_attachments=MagicMock(
            return_value=SimpleNamespace(data=[SimpleNamespace(vnic_id="ocid1.vnic.oc1..example")])
        )
    ),
)
def test_get_private_dns_name_builds_fqdn_from_vnic_subnet_vcn_labels(
    mock_compute,
    mock_identity,
    mock_network,
    mock_bs,
    mock_pagination,
) -> None:
    """Test that private DNS FQDN is built from VNIC, subnet, and VCN DNS labels."""
    provider = VirtualMachineProvider(
        _compartment_id="ocid1.compartment.oc1..example",
        _region="us-ashburn-1",
        _az="1",
    )
    provider._raw_cache["db-node"] = SimpleNamespace(id="ocid1.instance.oc1..example")

    fqdn = provider.get_private_dns_name("db-node")

    assert fqdn == "db-host.sctprvtest.sct2vcn.oraclevcn.com"


# --- boot volume sizing ---
#
# LaunchInstanceDetails has no root disk field: a bare `image_id' makes OCI size the boot volume from
# the image and silently drop `root_disk_size_db' / `_loader' / `_monitor'. The size has to be carried
# in `source_details', and OCI refuses anything under 50G - which two of the oci_config defaults are.
# `build_image_source_details' is shared with the SCT runner, so these cover both call sites' sizing.


@pytest.mark.parametrize(
    "root_disk_size,expected_gb",
    [
        pytest.param(100, 100, id="honours-requested-size"),
        pytest.param(30, MIN_BOOT_VOLUME_SIZE_IN_GBS, id="db-default-30g-raised-to-oci-minimum"),
        pytest.param(20, MIN_BOOT_VOLUME_SIZE_IN_GBS, id="loader-default-20g-raised-to-oci-minimum"),
        pytest.param(50, 50, id="monitor-default-50g-is-already-the-minimum"),
        pytest.param(None, MIN_BOOT_VOLUME_SIZE_IN_GBS, id="unset-falls-back-to-oci-minimum"),
    ],
)
def test_build_image_source_details_sizes_boot_volume_within_oci_limits(root_disk_size, expected_gb):
    """Test that the requested root disk size becomes an explicit boot volume size, floored at the OCI minimum."""
    source_details = build_image_source_details(
        image_id="ocid1.image.oc1..example", root_disk_size_gb=root_disk_size, name="test-oci-node-a"
    )

    assert source_details.boot_volume_size_in_gbs == expected_gb


def test_build_image_source_details_carries_the_image() -> None:
    """Test that the image is carried by source_details rather than the deprecated image_id field."""
    source_details = build_image_source_details(
        image_id="ocid1.image.oc1..example", root_disk_size_gb=100, name="test-oci-node-a"
    )

    assert source_details.image_id == "ocid1.image.oc1..example"
    assert source_details.source_type == "image"


@patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=[]))
@patch("sdcm.utils.oci_utils.OciService.get_block_storage_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
@patch(
    "sdcm.utils.oci_utils.OciService.get_identity_client",
    return_value=MagicMock(
        list_availability_domains=MagicMock(
            return_value=SimpleNamespace(data=[SimpleNamespace(name="us-ashburn-1-AD-1")])
        )
    ),
)
@patch("sdcm.utils.oci_utils.OciService.get_compute_client", return_value=MagicMock())
def test_provision_instance_sends_boot_volume_size_to_oci(
    mock_compute,
    mock_identity,
    mock_network,
    mock_bs,
    mock_pagination,
) -> None:
    """Test that the sized source_details reaches launch_instance, leaving image_id unset.

    Guards the original defect, where root_disk_size was resolved into the InstanceDefinition and then
    never referenced when launch_details was assembled.
    """
    provider = VirtualMachineProvider(
        _compartment_id="ocid1.compartment.oc1..example",
        _region="us-ashburn-1",
        _az="us-ashburn-1-AD-1",
    )
    definition = SimpleNamespace(
        name="test-oci-node-a",
        image_id="ocid1.image.oc1..example",
        root_disk_size=100,
        type="VM.Standard.E4.Flex:2:8",
        tags={"NodeIndex": "1", "NodeType": "scylla-db"},
        rack_index=None,
        ssh_key=SimpleNamespace(name="test-key", public_key=b"ssh-ed25519 AAAATEST"),
        user_name="scyllaadm",
        user_data=None,
        use_public_ip=False,
    )

    with patch.object(VirtualMachineProvider, "_wait_for_state", return_value=MagicMock()):
        provider._provision_instance(oci_region=MagicMock(), definition=definition, pricing_model=MagicMock())

    launch_details = provider._compute_client.launch_instance.call_args.args[0]
    assert launch_details.source_details.boot_volume_size_in_gbs == 100
    assert launch_details.source_details.image_id == "ocid1.image.oc1..example"
    assert launch_details.image_id is None
