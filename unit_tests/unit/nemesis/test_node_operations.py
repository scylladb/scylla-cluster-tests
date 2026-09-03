"""Tests for sdcm.nemesis.utils.node_operations module."""

from unittest.mock import MagicMock

import pytest

from sdcm.nemesis.utils.node_operations import (
    block_loaders_payload_for_scylla_node,
    block_scylla_ports,
    is_node_destroyed,
    pause_scylla_with_sigstop,
)
from unit_tests.unit.nemesis import make_mock_node, sudo_commands

LOADER_IPS = "10.0.0.2,10.0.0.3"
CQL_PORTS = (9042, 9142, 19042, 19142)
GOSSIP_PORTS = (7000, 7001)


@pytest.fixture()
def scylla_node():
    """The live Scylla node that the context managers under test act on."""
    return make_mock_node("node1")


@pytest.fixture()
def loader_nodes():
    """Loaders whose payload gets blocked; their addresses make up ``LOADER_IPS``."""
    return [
        make_mock_node("loader1", ip_address="10.0.0.2"),
        make_mock_node("loader2", ip_address="10.0.0.3"),
    ]


def _loader_rules(commands, action):
    """Loader-blocking INPUT rules issued with the given iptables action (``A`` or ``D``)."""
    return [cmd for cmd in commands if cmd and f" -{action} INPUT -s {LOADER_IPS} " in cmd]


def _destroy(node):
    """Apply to ``node`` what ``BaseNode.destroy()`` does to a terminated instance."""
    node.remoter = None
    node.destroyed = True


# ---------------------------------------------------------------------------
# is_node_destroyed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "destroyed,has_remoter,expected",
    [
        pytest.param(False, True, False, id="live-node"),
        pytest.param(True, False, True, id="destroyed-node"),
        pytest.param(True, True, True, id="flag-set-before-remoter-dropped"),
        pytest.param(False, False, True, id="remoter-dropped-before-flag-set"),
    ],
)
def test_is_node_destroyed_detects_either_half_of_destroy(destroyed, has_remoter, expected):
    """``BaseNode.destroy()`` drops the remoter and raises the ``destroyed`` flag in two
    separate statements, so a reader can observe either one first. Both must count."""
    node = make_mock_node("node1")
    node.destroyed = destroyed
    node.remoter = MagicMock() if has_remoter else None

    assert is_node_destroyed(node) is expected


# ---------------------------------------------------------------------------
# block_loaders_payload_for_scylla_node
# ---------------------------------------------------------------------------


def test_block_loaders_payload_live_node_removes_every_rule(scylla_node, loader_nodes):
    """The happy path: every CQL port is DROPped for the loader IPs on enter and the
    matching rule is deleted on exit, after which the iptables service is stopped."""
    remoter = scylla_node.remoter

    with block_loaders_payload_for_scylla_node(scylla_node, loader_nodes=loader_nodes):
        pass

    commands = sudo_commands(remoter)
    for port in CQL_PORTS:
        rule = f"INPUT -s {LOADER_IPS} -p tcp --dport {port} -j DROP"
        assert commands.count(f"iptables -A {rule}") == 1
        assert commands.count(f"ip6tables -A {rule}") == 1
        assert commands.count(f"iptables -D {rule}") == 1
        assert commands.count(f"ip6tables -D {rule}") == 1
    scylla_node.install_package.assert_called_once_with("iptables")
    scylla_node.stop_service.assert_called_once_with("iptables", ignore_status=True)


def test_block_loaders_payload_destroyed_node_skips_cleanup(scylla_node, loader_nodes):
    """SCT-920: the node may be removed from the cluster and terminated inside the ``with``
    block. Cleanup must then skip rather than raise ``AttributeError: 'NoneType' object has
    no attribute 'is_up'`` on the dropped remoter."""
    remoter = scylla_node.remoter

    with block_loaders_payload_for_scylla_node(scylla_node, loader_nodes=loader_nodes):
        _destroy(scylla_node)

    commands = sudo_commands(remoter)
    assert len(_loader_rules(commands, "A")) == 2 * len(CQL_PORTS)  # iptables + ip6tables
    assert _loader_rules(commands, "D") == []
    scylla_node.stop_service.assert_not_called()


def test_block_loaders_payload_destroy_flag_only_skips_cleanup(scylla_node, loader_nodes):
    """``destroyed`` alone is enough to skip: the instance is gone, so its iptables rules
    went with it even if the remoter object happens to still be around."""
    with block_loaders_payload_for_scylla_node(scylla_node, loader_nodes=loader_nodes):
        scylla_node.destroyed = True

    assert _loader_rules(sudo_commands(scylla_node.remoter), "D") == []
    scylla_node.remoter.is_up.assert_not_called()
    scylla_node.stop_service.assert_not_called()


def test_block_loaders_payload_unreachable_node_skips_cleanup(scylla_node, loader_nodes):
    """Pre-existing behaviour, kept intact by the SCT-920 guard: a node that still has a
    remoter but is not reachable gets no cleanup commands either."""
    scylla_node.remoter.is_up.return_value = False

    with block_loaders_payload_for_scylla_node(scylla_node, loader_nodes=loader_nodes):
        pass

    assert _loader_rules(sudo_commands(scylla_node.remoter), "D") == []
    scylla_node.stop_service.assert_not_called()


# ---------------------------------------------------------------------------
# block_scylla_ports
# ---------------------------------------------------------------------------


def test_block_scylla_ports_live_node_removes_every_rule(scylla_node):
    """Every requested port is DROPped in both directions and both IP families on enter,
    and each rule is deleted again on exit."""
    remoter = scylla_node.remoter

    with block_scylla_ports(scylla_node, ports=list(GOSSIP_PORTS)):
        pass

    commands = sudo_commands(remoter)
    for port in GOSSIP_PORTS:
        for table in ("iptables", "ip6tables"):
            for chain in ("INPUT", "OUTPUT"):
                assert commands.count(f"{table} -A {chain} -p tcp --dport {port} -j DROP") == 1
                assert commands.count(f"{table} -D {chain} -p tcp --dport {port} -j DROP") == 1
    scylla_node.stop_service.assert_called_once_with("iptables", ignore_status=True)


def test_block_scylla_ports_destroyed_node_skips_cleanup(scylla_node):
    """Hardening against the SCT-920 failure mode: a caller that removes and terminates the
    node it just blocked must not make the unblock step raise on the dropped remoter."""
    remoter = scylla_node.remoter

    with block_scylla_ports(scylla_node, ports=list(GOSSIP_PORTS)):
        _destroy(scylla_node)

    assert [cmd for cmd in sudo_commands(remoter) if cmd and " -D " in cmd] == []
    scylla_node.stop_service.assert_not_called()


# ---------------------------------------------------------------------------
# pause_scylla_with_sigstop
# ---------------------------------------------------------------------------


def test_pause_scylla_with_sigstop_live_node_sends_sigcont(scylla_node):
    """The paused scylla process is resumed again once the block exits."""
    with pause_scylla_with_sigstop(scylla_node):
        pass

    assert sudo_commands(scylla_node.remoter) == [
        "pkill --signal SIGSTOP -e scylla",
        "pkill --signal SIGCONT -e scylla",
    ]


def test_pause_scylla_with_sigstop_destroyed_node_skips_sigcont(scylla_node):
    """Hardening against the SCT-920 failure mode: there is no process left to resume once
    the instance has been terminated."""
    remoter = scylla_node.remoter

    with pause_scylla_with_sigstop(scylla_node):
        _destroy(scylla_node)

    assert sudo_commands(remoter) == ["pkill --signal SIGSTOP -e scylla"]
