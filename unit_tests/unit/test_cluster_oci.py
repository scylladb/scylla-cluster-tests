"""Unit tests for OCI cluster node behavior."""

from unittest.mock import Mock, patch

from sdcm.cluster_oci import OciNode


MOCK_CREDENTIALS = Mock(key_file="/tmp/test_key")
MOCK_PARENT_CLUSTER = Mock(params={"simulated_regions": 0})


def _mock_cloud_instance(private_ip_address="10.0.4.4"):
    return Mock(
        private_ip_address=private_ip_address,
        private_dns_name=None,
        public_ip_address=None,
        instance_type="VM.Standard.E4.Flex",
        user_name="opc",
        region="us-ashburn-1",
    )


def _base_node_init(
    self,
    name,
    parent_cluster,
    ssh_login_info=None,
    base_logdir=None,
    node_prefix=None,
    dc_idx=0,
    rack=0,
    after_config=None,
):
    self.name = name
    self.test_config = Mock(IP_SSH_CONNECTIONS="private", INTRA_NODE_COMM_PUBLIC=False)
    self._private_ip_address_cached = None
    self._public_ip_address_cached = None
    self._ipv6_ip_address_cached = None
    self.log = Mock()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="node.internal")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_uses_metadata_hostname_when_available(mock_resolver) -> None:
    """Test that FQDN from OCI metadata is used directly without reverse DNS lookup."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="node.private.subnet.vcn.oraclevcn.com")

    assert oci_node.private_dns_name == "node.private.subnet.vcn.oraclevcn.com"

    mock_resolver.assert_not_called()
    oci_node.log.warning.assert_not_called()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="node.internal")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_falls_back_to_reverse_dns_when_metadata_query_fails(mock_resolver) -> None:
    """Test that reverse DNS lookup is used when OCI metadata query raises an exception."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    assert oci_node.private_dns_name == "node.internal"

    mock_resolver.assert_called_once_with("10.0.4.4")
    oci_node.log.warning.assert_called_once()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="db-node-short.subnet.vcn.oraclevcn.com")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_prefers_reverse_dns_fqdn_when_metadata_returns_short_label(mock_resolver) -> None:
    """Test that reverse DNS FQDN is preferred over a short hostname label from metadata."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="db-node-short")

    assert oci_node.private_dns_name == "db-node-short.subnet.vcn.oraclevcn.com"

    mock_resolver.assert_called_once_with("10.0.4.4")


@patch(
    "sdcm.cluster_oci.resolve_ip_to_dns",
    side_effect=ValueError("Unable to resolve IP: [Errno 1] Unknown host"),
)
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_falls_back_to_private_ip_when_reverse_dns_missing(mock_resolver) -> None:
    """Test that private IP is returned when both metadata and reverse DNS fail."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    assert oci_node.private_dns_name == "10.0.4.4"

    mock_resolver.assert_called_once_with("10.0.4.4")
    assert oci_node.log.warning.call_count == 2


@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_falls_back_to_node_name_when_private_ip_missing() -> None:
    """Test that node name is returned as last resort when private IP is also unavailable."""
    oci_node = OciNode(_mock_cloud_instance(None), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    dns_name = oci_node.private_dns_name

    oci_node.query_oci_metadata.assert_called_once_with("hostname")
    assert oci_node.log.warning.call_count == 2
    oci_node.log.warning.assert_any_call(
        "Failed to query OCI metadata hostname for node %s (%s). Falling back.",
        oci_node.name,
        oci_node.query_oci_metadata.side_effect,
    )
    oci_node.log.warning.assert_any_call(
        "Node %s has no private IP while resolving private DNS name. Falling back to node name.",
        oci_node.name,
    )
    assert dns_name == oci_node.name


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="db-node-short.subnet.vcn.oraclevcn.com")
@patch("sdcm.cluster_oci.create_certificate")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_create_node_certificate_includes_short_and_fqdn_dns_names(mock_create_cert, mock_resolver) -> None:
    """Test that node certificate includes both short hostname and FQDN as DNS names."""
    oci_node = OciNode(_mock_cloud_instance("10.0.3.13"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="db-node-short")

    oci_node.create_node_certificate("cert.pem", "key.pem")

    mock_create_cert.assert_called_once()
    dns_names = mock_create_cert.call_args.kwargs["dns_names"]
    assert "db-node-short" in dns_names
    assert "db-node-short.subnet.vcn.oraclevcn.com" in dns_names
