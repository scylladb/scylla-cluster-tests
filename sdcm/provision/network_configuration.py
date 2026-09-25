from dataclasses import dataclass
from typing import Optional


class NetworkInterfaceNotFound(Exception):
    pass


@dataclass
class NetworkInterface:
    ipv4_public_address: Optional[str]
    # AWS allows to have a few public IPv6 addresses per interface.
    ipv6_public_addresses: [str]
    # AWS allows to have a few private IPv4 addresses per interface.
    ipv4_private_addresses: [str]
    ipv6_private_address: str
    dns_private_name: str
    dns_public_name: Optional[str]
    device_index: int
    device_name: Optional[str]
    mac_address: Optional[str]
    use_dns_names: bool = False


class ScyllaNetworkConfiguration:
    LISTEN_ALL = "0.0.0.0"

    def __init__(self, network_interfaces, scylla_network_config):
        self.network_interfaces = network_interfaces
        self.scylla_network_config = scylla_network_config

    def __bool__(self) -> bool:
        return bool(self.scylla_network_config)

    @property
    def _use_dns_names(self) -> bool:
        """Check if any network interface has use_dns_names enabled."""
        return any(iface.use_dns_names for iface in self.network_interfaces)

    def _get_dns_private_name(self, address_config: dict) -> str:
        """Get DNS private name for the interface specified in address_config."""
        if not (interface := [conf for conf in self.network_interfaces if address_config["nic"] == conf.device_index]):
            raise NetworkInterfaceNotFound(
                f"Not found network interface with device_index {address_config['nic']}. "
                f"Check 'scylla_network_config' definition in the test configuration"
            )
        return interface[0].dns_private_name

    @property
    def listen_address(self):
        listen_address_config = [conf for conf in self.scylla_network_config if conf["address"] == "listen_address"][0]
        if self._use_dns_names:
            return self._get_dns_private_name(listen_address_config)
        return self.get_ip_by_address_config(address_config=listen_address_config)

    @property
    def rpc_address(self):
        rpc_address_config = [conf for conf in self.scylla_network_config if conf["address"] == "rpc_address"][0]
        if self._use_dns_names:
            return self._get_dns_private_name(rpc_address_config)
        return self.get_ip_by_address_config(address_config=rpc_address_config)

    @property
    def broadcast_rpc_address(self):
        if broadcast_rpc_address_config := [
            conf for conf in self.scylla_network_config if conf["address"] == "broadcast_rpc_address"
        ]:
            if self._use_dns_names:
                return self._get_dns_private_name(broadcast_rpc_address_config[0])
            return self.get_ip_by_address_config(address_config=broadcast_rpc_address_config[0])
        else:
            return self.rpc_address

    @property
    def broadcast_address(self):
        if broadcast_address_config := [
            conf for conf in self.scylla_network_config if conf["address"] == "broadcast_address"
        ]:
            return self.get_ip_by_address_config(address_config=broadcast_address_config[0])

        return self.listen_address

    @property
    def dns_private_name(self):
        if broadcast_address_config := [
            conf for conf in self.scylla_network_config if conf["address"] == "broadcast_address"
        ]:
            if not (
                interface := [
                    conf for conf in self.network_interfaces if broadcast_address_config[0]["nic"] == conf.device_index
                ]
            ):
                raise NetworkInterfaceNotFound(
                    f"Not found network interface with device_index {broadcast_address_config[0]['nic']}. "
                    f"Check 'scylla_network_config' definition in the test configuration"
                )
            return interface[0].dns_private_name

        return self.network_interfaces[0].dns_private_name

    @property
    def dns_broadcast_rpc_name(self):
        """Return the DNS private name of the NIC used for broadcast_rpc_address.

        In split-network topologies, broadcast_rpc_address is on a different NIC
        than broadcast_address, so the client-facing certificate must include
        the DNS name of the RPC NIC for TLS hostname verification.
        """
        if broadcast_rpc_address_config := [
            conf for conf in self.scylla_network_config if conf["address"] == "broadcast_rpc_address"
        ]:
            if not (
                interface := [
                    conf
                    for conf in self.network_interfaces
                    if broadcast_rpc_address_config[0]["nic"] == conf.device_index
                ]
            ):
                raise NetworkInterfaceNotFound(
                    f"Not found network interface with device_index {broadcast_rpc_address_config[0]['nic']}. "
                    f"Check 'scylla_network_config' definition in the test configuration"
                )
            return interface[0].dns_private_name

        return self.dns_private_name

    @property
    def broadcast_address_ip_type(self):
        # If multiple network interface is defined on the node, private address in the `nodetool status` is IP that defined in
        # broadcast_address. Keep this output in correlation with `nodetool status`
        broadcast_address_config = [
            conf for conf in self.scylla_network_config if conf["address"] == "broadcast_address"
        ]
        if broadcast_address_config[0]["ip_type"] == "ipv6":
            return "ipv6"
        else:
            return "public" if broadcast_address_config[0]["public"] else "private"

    @property
    def test_communication(self):
        """
        IP is used to connect from test to DB/monitor instances
        """
        if test_communication_address_config := [
            conf for conf in self.scylla_network_config if conf["address"] == "test_communication"
        ]:
            return self.get_ip_by_address_config(address_config=test_communication_address_config[0])

        return self.broadcast_address

    @property
    def test_communication_nic(self) -> int:
        """Device index of the interface used to connect from test to DB/monitor instances."""
        if address_config := [conf for conf in self.scylla_network_config if conf["address"] == "test_communication"]:
            return address_config[0]["nic"]

        return 0

    @property
    def interface_ipv6_address(self):
        # Keep same as sdcm.cluster_aws.AWSNode._get_ipv6_ip_address for now: take first IP address for IPv6 from first interface
        if self.network_interfaces[0].ipv6_public_addresses:
            return self.network_interfaces[0].ipv6_public_addresses[0]
        else:
            return None

    @property
    def device(self):
        # Depend on network configuration:
        # if broadcast_address.device_name if found
        # else empty string.
        if address_config := [conf for conf in self.scylla_network_config if conf["address"] == "broadcast_address"]:
            return "".join(
                [ni.device_name for ni in self.network_interfaces if ni.device_index == address_config[0]["nic"]]
            )
        # Workaround for k8s, while it does not support `scylla_network_config`
        return (
            self.network_interfaces[0].device_name
            if self.network_interfaces and self.network_interfaces[0].device_name
            else ""
        )

    def get_ip_by_address_config(self, address_config: dict) -> str:
        if not (interface := [conf for conf in self.network_interfaces if address_config["nic"] == conf.device_index]):
            raise NetworkInterfaceNotFound(
                f"Not found network interface with device_index {address_config['nic']}. "
                f"Check 'scylla_network_config' definition in the test configuration"
            )

        interface = interface[0]

        # When multiple interfaces - Scylla should be listening on all interfaces
        if address_config.get("listen_all"):
            return self.LISTEN_ALL

        if address_config.get("use_dns"):
            return interface.dns_private_name

        match address_config["ip_type"]:
            case "ipv4":
                # AWS allows to have a few private IPv4 addresses per interface. For
                # now the first one address is picking, until we have anything else defined/implemented
                return (
                    interface.ipv4_public_address if address_config["public"] else interface.ipv4_private_addresses[0]
                )

            case "ipv6":
                # AWS allows to have a few IPv6 addresses per interface. For
                # now the first one address is picking, until we have anything else defined/implemented
                return (
                    interface.ipv6_public_addresses[0] if address_config["public"] else interface.ipv6_private_address
                )


def is_ip_ssh_connections_ipv6(params):
    return ssh_connection_ip_type(params) == "ipv6"


def network_interfaces_count(params):
    if params and (scylla_network_config := params.get("scylla_network_config")):
        nics = set()
        for interface in scylla_network_config:
            nics.add(interface["nic"])
        return len(nics)

    # For clusters where new network configuration is not used and "extra_network_interface" parameter is not defined
    # Another case - jenkins builder
    return 1


DEFAULT_AZURE_SUBNET_NAME = "default"
# Azure VNet IPv6 space must come from a private (ULA) range: an internet-routable IPv6 is a
# separate, billed Public IP resource attached to the ipConfiguration, never the VNet address.
AZURE_IPV6_ADDRESS_SPACE = "fd00:db8:5c7::/48"
AZURE_IPV6_SUBNET_PREFIX_TMPL = "fd00:db8:5c7:{index}::/64"  # Azure requires exactly a /64
AZURE_SECONDARY_SUBNET_NAME_TMPL = "nic{index}"


def azure_network_interfaces(params) -> list[dict]:
    """NIC specs of an Azure node, one dict per device index, derived from 'scylla_network_config'.

    That option already says how many interfaces the node has, which address family each one
    carries and which of those addresses must be reachable from outside the VNet, so the Azure
    layout follows from it - there is nothing left for a separate, Azure-only option to say:

    - subnet: positional. The primary NIC carries the public IPv4 and stays on the test VNet's
      default subnet; every other NIC gets its own 'nic<index>' subnet
    - public_ip: only the primary NIC. It is the node's outbound path and the address SCT falls
      back to, and a public IPv4 on a secondary NIC is rejected by validation anyway
    - ipv6 / public_ipv6: an IPv6 ipConfiguration wherever an address asks for 'ip_type: ipv6',
      and the billed IPv6 Public IP only where such an address is also 'public: true'

    Without 'scylla_network_config' this is a single public IPv4 NIC - what every Azure test had
    before multiple interfaces were supported.
    """
    scylla_network_config = (params.get("scylla_network_config") if params else None) or []
    ipv6_nics = {config["nic"] for config in scylla_network_config if config["ip_type"] == "ipv6"}
    public_ipv6_nics = {
        config["nic"] for config in scylla_network_config if config["ip_type"] == "ipv6" and config["public"]
    }
    return [
        {
            "subnet": DEFAULT_AZURE_SUBNET_NAME if index == 0 else AZURE_SECONDARY_SUBNET_NAME_TMPL.format(index=index),
            "public_ip": index == 0,
            "ipv6": index in ipv6_nics,
            "public_ipv6": index in public_ipv6_nics,
        }
        for index in range(network_interfaces_count(params))
    ]


def azure_ipv6_enabled(params) -> bool:
    """True when 'scylla_network_config' puts an 'ip_type: ipv6' address on any interface.

    An internet-routable Azure IPv6 is a billed Public IP resource, so IPv6 is strictly opt-in.
    """
    return any(interface["ipv6"] for interface in azure_network_interfaces(params))


def ssh_connection_ip_type(params):
    if scylla_network_config := params.get("scylla_network_config"):
        ssh_ip_type = [conf for conf in scylla_network_config if conf["address"] == "test_communication"][0]
        match ssh_ip_type["ip_type"]:
            case "ipv6":
                return "ipv6"
            case "ipv4":
                return "public" if ssh_ip_type["public"] else "private"

    # TODO: Keep the "ip_ssh_connections" parameter for clusters where new network configuration is not used
    return params.get("ip_ssh_connections") or "private"
