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

"""AzureNode network interface introspection."""

from unittest.mock import Mock, patch

import pytest

from sdcm.cluster_azure import AzureNode, Ipv6AddressNotFoundError
from sdcm.sct_runner import AzureSctRunner


def azure_ip_configuration(private_ip: str, version: str = "IPv4", public_ip: str = None):
    """Returns an Azure ipConfiguration mock with the given private IP, version and optional expanded Public IP."""
    return Mock(
        private_ip_address=private_ip,
        private_ip_address_version=version,
        public_ip_address=Mock(ip_address=public_ip) if public_ip else None,
    )


def azure_nic(mac: str, ip_configurations: list):
    """Returns an Azure NIC mock with the given MAC address and ipConfigurations."""
    return Mock(mac_address=mac, ip_configurations=ip_configurations)


def unexpanded_ip_configuration(private_ip: str, version: str = "IPv4"):
    """An ipConfiguration as Azure actually returns it.

    The Public IP comes back as a sub-resource reference: an id, but no `ipAddress` unless the NIC
    was fetched with `expand=IPConfigurations/PublicIPAddress`. `spec` is what makes the attribute
    genuinely absent rather than a Mock that answers to everything.
    """
    return Mock(
        private_ip_address=private_ip,
        private_ip_address_version=version,
        public_ip_address=Mock(spec=["id", "name"]),
    )


@pytest.fixture(name="node")
def fixture_node():
    """An AzureNode with just enough wired up to build its network interfaces."""
    node = AzureNode.__new__(AzureNode)
    node._cached_network_interfaces = None
    node.use_dns_names = False
    node.remoter = Mock()
    node._instance = Mock(name="vm", private_dns_name="vm.internal.cloudapp.net")
    node._instance.name = "vm"
    return node


def configure(node, nics: list, devices: dict):
    """Makes the provisioner return the given NICs and the node's ip-link see the given MAC-to-device map."""
    node._instance._provisioner = Mock(network_interfaces=Mock(return_value=nics))
    # BaseNode.network_configuration is a cached_property, so an instance attribute shadows it
    node.network_configuration = devices


def test_single_nic_is_reported_with_its_device_name(node):
    """A single NIC is reported as device 0 with its ip-link device name, private IPv4 and public IPv4."""
    configure(
        node,
        [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3")])],
        {"00:0d:3a:11:11:11": "eth0"},
    )

    (interface,) = node.network_interfaces
    assert interface.device_index == 0
    assert interface.device_name == "eth0"
    assert interface.ipv4_private_addresses == ["10.0.0.4"]
    assert interface.ipv4_public_address == "20.1.2.3"


def test_interfaces_keep_the_provisioner_device_order(node):
    """Interfaces keep the provisioner's NIC order for device index, device name and private addresses."""
    configure(
        node,
        [
            azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")]),
            azure_nic("00-0D-3A-22-22-22", [azure_ip_configuration("10.0.1.4")]),
        ],
        {"00:0d:3a:11:11:11": "eth0", "00:0d:3a:22:22:22": "eth1"},
    )

    interfaces = node.network_interfaces
    assert [interface.device_index for interface in interfaces] == [0, 1]
    assert [interface.device_name for interface in interfaces] == ["eth0", "eth1"]
    assert [interface.ipv4_private_addresses for interface in interfaces] == [["10.0.0.4"], ["10.0.1.4"]]


def test_mac_addresses_are_normalised_to_the_ip_link_format(node):
    """Azure reports '00-0D-3A-...', ip-link reports '00:0d:3a:...'."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    assert node.network_interfaces[0].mac_address == "00:0d:3a:11:11:11"


def test_ipv6_configuration_is_split_into_private_and_routable_addresses(node):
    """A VNet-local (ULA) IPv6 is private; only a Public IP resource is routable."""
    configure(
        node,
        [
            azure_nic(
                "00-0D-3A-11-11-11",
                [
                    azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3"),
                    azure_ip_configuration("fd00:db8:5c7::4", version="IPv6", public_ip="2603:1030::1"),
                ],
            )
        ],
        {},
    )

    (interface,) = node.network_interfaces
    assert interface.ipv4_private_addresses == ["10.0.0.4"]
    assert interface.ipv6_private_address == "fd00:db8:5c7::4"
    assert interface.ipv6_public_addresses == ["2603:1030::1"]


def test_ipv4_only_nic_reports_no_ipv6_address(node):
    """An IPv4-only NIC reports an empty IPv6 private address and an empty list of public IPv6 addresses."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    (interface,) = node.network_interfaces
    assert interface.ipv6_private_address == ""
    assert interface.ipv6_public_addresses == []


def test_public_addresses_come_from_the_ip_provider_when_the_nic_omits_them(node):
    """The NIC payload carries no address, so it has to be resolved through the IP provider."""
    configure(
        node,
        [
            azure_nic(
                "00-0D-3A-11-11-11",
                [
                    unexpanded_ip_configuration("10.0.0.4"),
                    unexpanded_ip_configuration("fd00:db8:5c7::4", version="IPv6"),
                ],
            )
        ],
        {},
    )
    addresses = {("IPV4", 0): "20.1.2.3", ("IPV6", 0): "2603:1030::1"}
    node._instance._provisioner.public_ip_address.side_effect = lambda name, version, index: addresses[(version, index)]

    (interface,) = node.network_interfaces
    assert interface.ipv4_public_address == "20.1.2.3"
    assert interface.ipv6_public_addresses == ["2603:1030::1"]


def test_an_interface_without_a_public_ip_asks_the_ip_provider_for_nothing(node):
    """An interface without a Public IP reports None as its public IPv4 without calling the IP provider."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    (interface,) = node.network_interfaces
    assert interface.ipv4_public_address is None
    node._instance._provisioner._ip_provider.get.assert_not_called()


def test_interfaces_are_cached_until_invalidated(node):
    """The interfaces are fetched from the provisioner once and again only after the cache is invalidated."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    node.network_interfaces
    node.network_interfaces
    assert node._instance._provisioner.network_interfaces.call_count == 1

    node._invalidate_network_interfaces_cache()
    node.network_interfaces
    assert node._instance._provisioner.network_interfaces.call_count == 2


class TestIpv6AddressResolution:
    """`AzureNode._get_ipv6_ip_address` while Azure has not published the address yet.

    With 'test_communication' set to 'ip_type: ipv6', this address is what SCT opens its SSH
    connection to, so it is resolved before the node has a remoter at all - the OS fallback
    cannot be reached and the Azure API is the only source.
    """

    IPV6_PARAMS = {
        "scylla_network_config": [{"address": "test_communication", "ip_type": "ipv6", "public": True, "nic": 0}]
    }

    @staticmethod
    def ipv6_nic():
        """Returns a NIC with an IPv4 and an IPv6 configuration, each with a Public IP."""
        return azure_nic(
            "00-0D-3A-11-11-11",
            [
                azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3"),
                azure_ip_configuration("fd00:db8:5c7::4", version="IPv6", public_ip="2603:1030::1"),
            ],
        )

    @staticmethod
    def ipv4_nic():
        """Returns a NIC with only an IPv4 configuration and its Public IP."""
        return azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3")])

    @pytest.fixture(name="ipv6_node")
    def fixture_ipv6_node(self, node):
        """The shared node, named and configured for an IPv6 test_communication address."""
        node.name = "azure-node-1"
        node.destroyed = False
        node.scylla_network_configuration = None
        node.parent_cluster = Mock(params=self.IPV6_PARAMS)
        return node

    def test_address_is_taken_from_the_api(self, ipv6_node):
        """The IPv6 address is the public IPv6 that the Azure API reports for the NIC."""
        configure(ipv6_node, [self.ipv6_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == "2603:1030::1"

    def test_the_address_is_found_without_a_remoter(self, ipv6_node):
        """The regression test: this used to raise AttributeError on the absent remoter."""
        ipv6_node.remoter = None
        configure(ipv6_node, [self.ipv6_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == "2603:1030::1"

    def test_a_missing_address_fails_loudly(self, ipv6_node):
        """An IPv6 run whose NIC has no IPv6 raises Ipv6AddressNotFoundError."""
        ipv6_node.remoter = None
        configure(ipv6_node, [self.ipv4_nic()], {})

        with pytest.raises(Ipv6AddressNotFoundError):
            ipv6_node._get_ipv6_ip_address()

    def test_an_ipv4_only_run_needs_no_address(self, ipv6_node):
        """No IPv6 in the configuration means no address and, above all, no waiting for one."""
        ipv6_node.remoter = None
        ipv6_node.parent_cluster = Mock(params={})
        configure(ipv6_node, [self.ipv4_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == ""

    def test_a_destroyed_node_reports_no_address(self, ipv6_node):
        """`destroy()` drops the remoter; log collection and Argus still read the addresses."""
        ipv6_node.remoter = None
        ipv6_node.destroyed = True
        configure(ipv6_node, [self.ipv4_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == ""

    def test_the_os_is_not_queried_without_a_remoter(self, ipv6_node):
        """Without a remoter the OS lookup returns an empty mapping."""
        ipv6_node.remoter = None

        assert ipv6_node._discover_ipv6_from_os() == {}


class TestAzureRunnerNetworkInterfaces:
    """The SCT runner gets a routable IPv6 only when the test configuration enables IPv6."""

    @staticmethod
    def runner(params: dict):
        """Returns an AzureSctRunner with the given params and nothing else set up."""
        runner = AzureSctRunner.__new__(AzureSctRunner)
        runner.params = params
        return runner

    def test_ipv4_only_run_gets_no_ipv6(self):
        """A run without IPv6 config gets one interface with a public IPv4 and IPv6 disabled."""
        (interface,) = self.runner({})._network_interfaces()

        assert interface == {"subnet": "default", "public_ip": True, "ipv6": False, "public_ipv6": False}

    def test_ipv6_run_gets_a_routable_ipv6(self):
        """An IPv6 test_communication config gives the runner an IPv6 that is routable from outside the subnet."""
        params = {
            "scylla_network_config": [{"address": "test_communication", "ip_type": "ipv6", "public": True, "nic": 0}]
        }

        (interface,) = self.runner(params)._network_interfaces()

        assert interface["ipv6"] is True
        assert interface["public_ipv6"] is True, "the runner reaches the nodes from outside their subnet"

    def test_runner_always_has_exactly_one_interface(self):
        """'scylla_network_config' describes the DB nodes; the runner never needs more than one."""
        params = {
            "scylla_network_config": [
                {"address": "listen_address", "ip_type": "ipv4", "public": False, "nic": nic} for nic in (0, 1, 2)
            ]
        }

        assert len(self.runner(params)._network_interfaces()) == 1


class TestOsIpv6Fallback:
    """The last resort when the API publishes nothing: what the OS itself holds."""

    @staticmethod
    def node_with(node, devices: dict, primary_device: str):
        """Prepares the node to return the given OS IPv6 devices, with primary_device as its first interface."""
        node.remoter = Mock()
        node._discover_ipv6_from_os = Mock(return_value=devices)
        node._cached_network_interfaces = [Mock(device_name=primary_device)]
        return node

    def test_the_primary_interface_is_reported(self, node):
        """A secondary NIC carries a ULA of its own, so the interface has to be chosen, not guessed."""
        devices = {"eth1": ["fd00:db8:5c7:1::4"], "eth0": ["fd00:db8:5c7::4"]}

        assert self.node_with(node, devices, "eth0")._os_ipv6_address() == "fd00:db8:5c7::4"

    def test_an_unknown_primary_device_falls_back_deterministically(self, node):
        """With an unknown primary device the address comes from the first device in sorted order (eth0)."""
        devices = {"eth1": ["fd00:db8:5c7:1::4"], "eth0": ["fd00:db8:5c7::4"]}

        assert self.node_with(node, devices, "")._os_ipv6_address() == "fd00:db8:5c7::4"

    def test_no_address_at_all_is_empty(self, node):
        """With no IPv6 address on any device the fallback returns an empty string."""
        assert self.node_with(node, {}, "eth0")._os_ipv6_address() == ""


@pytest.fixture(name="node_without_ptr")
def fixture_node_without_ptr(node, monkeypatch):
    """A node with a resolved private IP whose reverse DNS lookup fails (no PTR record)."""
    node.name = "db-node-1"
    node._private_ip_address_resolved = True
    node._private_ip_address_cached = "10.180.0.10"
    monkeypatch.setattr(
        "sdcm.cluster_azure.resolve_ip_to_dns",
        Mock(side_effect=ValueError("Unable to resolve IP: [Errno 1] Unknown host")),
    )
    return node


def test_private_dns_name_falls_back_to_the_node_name_without_a_ptr_record(node_without_ptr):
    """Without a PTR record the private DNS name falls back to the node name."""
    assert node_without_ptr.private_dns_name == "db-node-1"


def test_private_dns_name_without_a_ptr_record_raises_when_dns_names_are_on(node_without_ptr):
    """With use_dns_names on, the failed PTR lookup raises its ValueError."""
    node_without_ptr.use_dns_names = True
    with pytest.raises(ValueError, match="Unable to resolve IP"):
        _ = node_without_ptr.private_dns_name


@pytest.mark.parametrize("logs_transport,checked", [("vector", True), ("syslog-ng", False)])
def test_azure_node_checks_vector_only_for_the_vector_transport(logs_transport, checked):
    """configure_remote_logging checks that Vector is active only when logs_transport is vector."""
    node = AzureNode.__new__(AzureNode)
    node.parent_cluster = Mock(params={"logs_transport": logs_transport})

    with patch.object(AzureNode, "_ensure_vector_is_active") as ensure:
        node.configure_remote_logging()

    assert ensure.called is checked
