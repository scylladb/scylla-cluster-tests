"""Tests for sdcm.nemesis.monkey.network module."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.network import (
    BlockNetworkMonkey,
    RandomInterruptionNetworkMonkey,
    RejectInterNodeNetworkMonkey,
    RejectNodeExporterNetworkMonkey,
    RejectThriftNetworkMonkey,
    StopStartInterfacesNetworkMonkey,
    get_rate_limit_for_network_disruption,
    install_iptables,
    iptables_randomly_get_disrupting_target,
    iptables_randomly_get_random_matching_rule,
    run_commands_wait_and_cleanup,
)

_MODULE = "sdcm.nemesis.monkey.network"

pytestmark = pytest.mark.usefixtures("events")

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with network-nemesis-specific attributes."""
    base_runner.random.randrange.side_effect = lambda a, b: a
    base_runner.monitoring_set = MagicMock()
    base_runner.monitoring_set.nodes = []
    base_runner._is_it_on_kubernetes = MagicMock(return_value=False)
    base_runner.node_allocator = MagicMock()
    base_runner.tester.params = MagicMock(artifact_scylla_version=None)
    with patch("sdcm.utils.issues.SkipPerIssues.get_issue_details", return_value=None):
        yield base_runner


@pytest.fixture()
def node():
    """A generic mock node with a remoter for command execution."""
    return MagicMock()


@pytest.fixture()
def log():
    """A mock logger for capturing log calls."""
    return MagicMock()


# ---------------------------------------------------------------------------
# install_iptables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "is_ubuntu, should_install",
    [
        pytest.param(True, True, id="ubuntu"),
        pytest.param(False, False, id="non-ubuntu"),
    ],
)
def test_install_iptables(is_ubuntu, should_install):
    """Verify iptables is installed only on Ubuntu targets."""
    target = MagicMock()
    target.distro.is_ubuntu = is_ubuntu
    install_iptables(target)
    if should_install:
        target.install_package.assert_called_once_with("iptables")
    else:
        target.install_package.assert_not_called()


# ---------------------------------------------------------------------------
# iptables_randomly_get_random_matching_rule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "choices, expected_desc, expected_rule",
    [
        pytest.param(
            [""],
            "every packet",
            "",
            id="empty-match",
        ),
        pytest.param(
            ["limit", "second", 5],
            "string of 5 very first packets every second",
            "-m limit --limit 5/second",
            id="limit",
        ),
        pytest.param(
            ["statistic", "random", "0.3"],
            "randomly chosen packet with 0.3 probability",
            "-m statistic --mode random --probability 0.3",
            id="statistic-random",
        ),
        pytest.param(
            ["statistic", "nth", "8"],
            "every 8 packet",
            "-m statistic --mode nth --every 8 --packet 0",
            id="statistic-nth",
        ),
        pytest.param(
            ["connbytes", "800"],
            "every packet from connection that total byte counter exceeds 800",
            "-m connbytes --connbytes-mode bytes --connbytes-dir both --connbytes 800",
            id="connbytes",
        ),
    ],
)
def test_matching_rule(choices, expected_desc, expected_rule):
    """Verify each iptables matching rule branch produces correct description and rule string."""
    mock_rnd = MagicMock()
    mock_rnd.choice.side_effect = choices
    desc, rule = iptables_randomly_get_random_matching_rule(mock_rnd)
    assert desc == expected_desc
    assert rule == expected_rule


# ---------------------------------------------------------------------------
# iptables_randomly_get_disrupting_target
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "choices, expected_desc, expected_target",
    [
        pytest.param(["DROP"], "dropped", "DROP", id="drop"),
        pytest.param(
            ["REJECT", "icmp-port-unreachable"],
            "rejected with icmp-port-unreachable",
            "REJECT --reject-with icmp-port-unreachable",
            id="reject",
        ),
    ],
)
def test_disrupting_target(choices, expected_desc, expected_target):
    """Verify DROP and REJECT disrupting targets produce correct description and iptables target."""
    mock_rnd = MagicMock()
    mock_rnd.choice.side_effect = choices
    desc, target = iptables_randomly_get_disrupting_target(mock_rnd)
    assert desc == expected_desc
    assert target == expected_target


# ---------------------------------------------------------------------------
# run_commands_wait_and_cleanup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "start_cmds, cleanup_cmds, side_effects, expected_run_count, expect_error_logged",
    [
        pytest.param(
            ["cmd1", "cmd2"],
            ["cleanup1"],
            [None, None, None],
            3,
            False,
            id="all-succeed",
        ),
        pytest.param(
            ["bad", "good"],
            ["cleanup1"],
            [Exception("fail"), None, None],
            3,
            True,
            id="start-failure-continues",
        ),
        pytest.param(
            ["cmd1"],
            ["bad_cleanup"],
            [None, Exception("fail")],
            2,
            False,
            id="cleanup-failure-swallowed",
        ),
        pytest.param(
            [],
            ["cleanup1"],
            None,
            0,
            False,
            id="empty-start-skips-cleanup",
        ),
    ],
)
def test_run_commands(node, log, start_cmds, cleanup_cmds, side_effects, expected_run_count, expect_error_logged):
    """Verify run_commands_wait_and_cleanup handles success, failure, and empty command lists correctly."""
    if side_effects is not None:
        node.remoter.run.side_effect = side_effects
    run_commands_wait_and_cleanup(
        node,
        log,
        name="Test",
        start_commands=start_cmds,
        cleanup_commands=cleanup_cmds,
        wait_time=0,
    )
    assert node.remoter.run.call_count == expected_run_count
    if expect_error_logged:
        log.error.assert_called_once()


@patch(f"{_MODULE}.time")
def test_run_commands_waits_between_start_commands(mock_time, node, log):
    """Verify time.sleep is called with the configured wait_time between start and cleanup phases."""
    run_commands_wait_and_cleanup(
        node,
        log,
        name="Test",
        start_commands=["cmd1"],
        cleanup_commands=[],
        wait_time=30,
    )
    mock_time.sleep.assert_called_once_with(30)


# ---------------------------------------------------------------------------
# get_rate_limit_for_network_disruption
# ---------------------------------------------------------------------------


def test_rate_limit_returns_none_when_no_monitoring(runner):
    """Verify rate limit returns None when there are no monitoring nodes."""
    assert get_rate_limit_for_network_disruption(runner.monitoring_set, runner.target_node, runner.random) is None


@patch(f"{_MODULE}.PrometheusDBStats")
def test_rate_limit_returns_string_with_suffix(mock_prom_class, runner):
    """Verify rate limit queries Prometheus and returns a value with mbps or kbps suffix."""
    runner.random.randrange.side_effect = lambda a, b: 42
    runner.monitoring_set.nodes = [MagicMock()]
    mock_prom_class.return_value.query.return_value = [{"values": [(0, "0"), (600, str(100 * 1024 * 1024 * 600))]}]
    result = get_rate_limit_for_network_disruption(runner.monitoring_set, runner.target_node, runner.random)
    assert result is not None
    assert result.endswith(("mbps", "kbps"))


# ---------------------------------------------------------------------------
# UnsupportedNemesis guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "monkey_class",
    [
        pytest.param(RandomInterruptionNetworkMonkey, id="random-interruption"),
        pytest.param(BlockNetworkMonkey, id="block"),
        pytest.param(StopStartInterfacesNetworkMonkey, id="stop-start"),
    ],
)
def test_raises_when_no_extra_interface(runner, monkey_class):
    """Verify monkeys requiring extra_network_interface raise UnsupportedNemesis when it's absent."""
    runner.cluster.extra_network_interface = False
    with pytest.raises(UnsupportedNemesis, match="extra_network_interface"):
        monkey_class(runner).disrupt()


@pytest.mark.parametrize(
    "monkey_class",
    [
        pytest.param(RandomInterruptionNetworkMonkey, id="random-interruption"),
        pytest.param(BlockNetworkMonkey, id="block"),
    ],
)
def test_raises_when_tc_not_installed(runner, monkey_class):
    """Verify monkeys raise UnsupportedNemesis when traffic control installation fails."""
    runner.cluster.extra_network_interface = True
    runner.target_node.install_traffic_control.return_value = False
    with pytest.raises(UnsupportedNemesis, match="Traffic control"):
        monkey_class(runner).disrupt()


# ---------------------------------------------------------------------------
# iptables monkey disrupt() targets correct ports
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "monkey_class, expected_ports",
    [
        pytest.param(RejectInterNodeNetworkMonkey, ["7000", "7001"], id="inter-node"),
        pytest.param(RejectNodeExporterNetworkMonkey, ["9100"], id="node-exporter"),
        pytest.param(RejectThriftNetworkMonkey, ["9160"], id="thrift"),
    ],
)
@patch(f"{_MODULE}.run_commands_wait_and_cleanup")
@patch(f"{_MODULE}.iptables_randomly_get_disrupting_target", return_value=("dropped", "DROP"))
@patch(f"{_MODULE}.iptables_randomly_get_random_matching_rule", return_value=("every packet", ""))
@patch(f"{_MODULE}.install_iptables")
def test_iptables_monkey_targets_correct_ports(
    mock_install,
    mock_rule,
    mock_target,
    mock_run,
    runner,
    monkey_class,
    expected_ports,
):
    """Verify each iptables-based monkey generates start commands targeting its specific ports."""
    monkey_class(runner).disrupt()

    mock_install.assert_called_once_with(runner.target_node)
    mock_rule.assert_called_once_with(rnd=runner.random)
    mock_target.assert_called_once_with(rnd=runner.random)
    start_cmds = mock_run.call_args.kwargs["start_commands"]
    for port in expected_ports:
        assert any(port in cmd for cmd in start_cmds)
