"""Shared test helpers for OCI unit tests."""

from unittest.mock import MagicMock, Mock, PropertyMock

from sdcm.cluster_oci import OciNode

MOCK_CREDENTIALS = Mock(key_file="/tmp/test_key")
MOCK_PARENT_CLUSTER = Mock(params={"simulated_regions": 0})


def make_cloud_instance(private_ip="10.0.4.4"):
    return Mock(
        name="oci-instance-mock",
        private_ip_address=private_ip,
        private_dns_name=None,
        public_ip_address=None,
        instance_type="VM.Standard.E4.Flex",
        user_name="opc",
        region="us-ashburn-1",
    )


def base_node_init(
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


def make_oci_node_mock(private_ip=None, public_ip=None, ipv6_ip=None, rpc_address=None, broadcast_rpc_address=None):
    """Create a lightweight MagicMock(spec=OciNode) for testing get_all_ip_addresses and IP mapping."""
    node = MagicMock(spec=OciNode)
    type(node).private_ip_address = PropertyMock(return_value=private_ip)
    type(node).public_ip_address = PropertyMock(return_value=public_ip)
    type(node).ipv6_ip_address = PropertyMock(return_value=ipv6_ip)
    node.scylla_network_configuration = MagicMock()
    node.scylla_network_configuration.rpc_address = rpc_address
    node.scylla_network_configuration.broadcast_rpc_address = broadcast_rpc_address
    return node
