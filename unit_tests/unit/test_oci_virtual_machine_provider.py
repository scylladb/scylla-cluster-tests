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


AVAILABILITY_DOMAINS = [
    SimpleNamespace(name="ewbj:US-ASHBURN-AD-1"),
    SimpleNamespace(name="ewbj:US-ASHBURN-AD-2"),
    SimpleNamespace(name="ewbj:US-ASHBURN-AD-3"),
]


def _oci_instance(display_name: str, availability_domain: str, test_id: str = "test-id") -> SimpleNamespace:
    return SimpleNamespace(
        display_name=display_name,
        availability_domain=availability_domain,
        lifecycle_state="RUNNING",
        defined_tags={"sct": {"TestId": test_id}},
    )


ALL_REGION_INSTANCES = [
    _oci_instance("db-node-1", "ewbj:US-ASHBURN-AD-1"),
    _oci_instance("db-node-2", "ewbj:US-ASHBURN-AD-2"),
    _oci_instance("db-node-3", "ewbj:US-ASHBURN-AD-3"),
    _oci_instance("monitor-node-1", "ewbj:US-ASHBURN-AD-1"),
]


def _make_provider(az: str) -> VirtualMachineProvider:
    provider = VirtualMachineProvider(
        _compartment_id="ocid1.compartment.oc1..example",
        _region="us-ashburn-1",
        _az=az,
    )
    provider._identity_client.list_availability_domains.return_value = SimpleNamespace(data=AVAILABILITY_DOMAINS)
    return provider


@pytest.mark.parametrize(
    "az, expected_names",
    [
        pytest.param("1", ["db-node-1", "monitor-node-1"], id="single-numeric-az"),
        pytest.param("b", ["db-node-2"], id="single-letter-az"),
        pytest.param("ewbj:US-ASHBURN-AD-3", ["db-node-3"], id="full-ad-name"),
        pytest.param("a,b,c", ["db-node-1", "db-node-2", "db-node-3", "monitor-node-1"], id="az-list"),
        pytest.param("", ["db-node-1", "db-node-2", "db-node-3", "monitor-node-1"], id="no-az-scope"),
    ],
)
@patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=ALL_REGION_INSTANCES))
@patch("sdcm.utils.oci_utils.OciService.get_block_storage_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_identity_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_compute_client", return_value=MagicMock())
def test_list_instances_is_scoped_to_the_provider_availability_domains(
    mock_compute,
    mock_identity,
    mock_network,
    mock_bs,
    mock_pagination,
    az,
    expected_names,
) -> None:
    """Test that instances of other availability domains of the same region are not reported.

    The OCI compute API is regional. Without an explicit AD filter each per-AD provisioner of a
    region reports every node of the test, and callers merging provisioners (i.e. the log collector)
    end up collecting each node once per AD in use, racing for the same remote archive paths.
    """
    provider = _make_provider(az)

    instances = provider.list_instances(test_id="test-id")

    assert [inst.display_name for inst in instances] == expected_names
    assert sorted(provider._raw_cache) == sorted(expected_names)


@patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=ALL_REGION_INSTANCES))
@patch("sdcm.utils.oci_utils.OciService.get_block_storage_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_identity_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_compute_client", return_value=MagicMock())
def test_per_availability_domain_providers_report_each_instance_once(
    mock_compute,
    mock_identity,
    mock_network,
    mock_bs,
    mock_pagination,
) -> None:
    """Test that providers of all the region's ADs together report every instance exactly once."""
    reported = [
        inst.display_name for az in ("1", "2", "3") for inst in _make_provider(az).list_instances(test_id="test-id")
    ]

    assert sorted(reported) == ["db-node-1", "db-node-2", "db-node-3", "monitor-node-1"]


@patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=ALL_REGION_INSTANCES))
@patch("sdcm.utils.oci_utils.OciService.get_block_storage_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_identity_client", return_value=MagicMock())
@patch("sdcm.utils.oci_utils.OciService.get_compute_client", return_value=MagicMock())
def test_list_instances_keeps_the_whole_region_when_az_cannot_be_resolved(
    mock_compute,
    mock_identity,
    mock_network,
    mock_bs,
    mock_pagination,
) -> None:
    """Test that an unresolvable AZ degrades to no filtering instead of hiding nodes from collection."""
    provider = _make_provider("no-such-zone")

    instances = provider.list_instances(test_id="test-id")

    assert len(instances) == len(ALL_REGION_INSTANCES)
