"""Unit tests for OCI virtual machine provider DNS behavior."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sdcm.provision.oci.virtual_machine_provider import VirtualMachineProvider


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
