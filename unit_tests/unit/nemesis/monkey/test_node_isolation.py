"""Tests for sdcm.nemesis.monkey.node_isolation module."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import KillNemesis, UnsupportedNemesis, WaitForTimeoutError
from sdcm.nemesis.monkey.node_isolation import (
    IsolateNodeWithIptableRuleNemesis,
    IsolateNodeWithProcessSignalNemesis,
    is_single_node_in_rack,
    refuse_connection_from_banned_node,
    switch_target_node_to_another_rack,
)

pytestmark = pytest.mark.usefixtures("events")


def _node(name="node1", rack="rack1", dc_idx=0):
    node = MagicMock()
    node.name = name
    node.rack = rack
    node.dc_idx = dc_idx
    return node


def _sudo_commands(node):
    """Flatten node.remoter.sudo call args/kwargs into the plain command strings issued."""
    return [call.args[0] if call.args else call.kwargs.get("cmd") for call in node.remoter.sudo.call_args_list]


def _working_node(runner):
    return runner.node_allocator.run_nemesis.return_value.__enter__.return_value


def _instant_wait_for(func, step=1, text=None, timeout=None, throw_exc=True, stop_event=None, **kwargs):
    """A same-contract, non-blocking stand-in for sdcm.wait.wait_for: evaluates ``func``
    once instead of retrying for real (up to 600s) wall-clock seconds, so tests that
    exercise the timeout/failure branch don't have to actually wait it out."""
    result = func(**kwargs)
    if not result and throw_exc:
        raise WaitForTimeoutError(text or "mock timeout")
    return result


# ---------------------------------------------------------------------------
# is_single_node_in_rack / switch_target_node_to_another_rack
# ---------------------------------------------------------------------------


def test_is_single_node_in_rack(base_runner):
    """A node counts as alone only when no other data node shares both its rack and dc."""
    target = _node("node1", rack="rack1", dc_idx=0)
    peer_same_rack = _node("node2", rack="rack1", dc_idx=0)
    peer_other_dc = _node("node3", rack="rack1", dc_idx=1)
    base_runner.cluster.data_nodes = [target, peer_same_rack, peer_other_dc]

    assert not is_single_node_in_rack(base_runner, target)

    base_runner.cluster.data_nodes = [target, peer_other_dc]
    assert is_single_node_in_rack(base_runner, target)


def test_switch_target_node_to_another_rack(base_runner):
    """The target node moves to a rack different from the loader's rack."""
    base_runner.cluster.params = {"rack_aware_loader": True}
    base_runner.target_node.parent_cluster.racks_count = 2
    base_runner.loaders = MagicMock()
    base_runner.loaders.nodes = [_node("loader1", rack="rack1")]
    base_runner.set_target_node = MagicMock()
    base_runner.cluster.nodes = [
        _node("node1", rack="rack1"),
        _node("node2", rack="rack2"),
    ]

    switch_target_node_to_another_rack(base_runner)

    base_runner.set_target_node.assert_called_once_with(rack="rack2")


# ---------------------------------------------------------------------------
# refuse_connection_from_banned_node
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` wired for a successful node-ban walk-through: two
    same-rack/dc nodes, a working (verification) node that reports the target
    as DOWN then REMOVED, and the GitHub issue lookup (SkipPerIssues) stubbed
    out so no real network call is made.
    """
    target = base_runner.target_node
    peer = base_runner.cluster.data_nodes[1]
    target.dc_idx = 0
    peer.dc_idx = 0

    target.raft = MagicMock(is_consistent_topology_changes_enabled=True)
    target.host_id = "host-1"
    target.ip_address = "10.0.0.1"
    target.follow_system_log.return_value = iter(["received notification of being banned from the cluster"])
    target.db_up.return_value = False
    target.remoter.run.return_value = MagicMock(ok=False, stdout="")  # scylla no longer running once banned
    target.parent_cluster.get_nodetool_status.return_value = {"dc1": {}}  # target no longer listed -> removed

    base_runner._is_it_on_kubernetes = MagicMock(return_value=False)
    base_runner._remove_node_add_node = MagicMock()
    base_runner.cluster.params = MagicMock(artifact_scylla_version=None)
    base_runner.cluster.params.get.return_value = None  # rack_aware_loader off -> no rack switch
    base_runner.loaders = MagicMock()
    base_runner.loaders.nodes = []

    base_runner.node_allocator = MagicMock()
    working_node = base_runner.node_allocator.run_nemesis.return_value.__enter__.return_value
    working_node.parent_cluster.get_nodetool_status.return_value = {target.ip_address: {"state": "DN"}}
    base_runner.node_allocator.run_nemesis.return_value.__exit__.return_value = False

    with patch("sdcm.utils.issues.SkipPerIssues.get_issue_details", return_value=None):
        yield base_runner


# ---------------------------------------------------------------------------
# refuse_connection_from_banned_node — guard clauses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("nemesis_class", [IsolateNodeWithProcessSignalNemesis, IsolateNodeWithIptableRuleNemesis])
def test_precheck_skips_when_raft_topology_changes_disabled(runner, nemesis_class):
    """precheck() must return a skip reason when consistent-topology-changes raft feature is off,
    for both the SIGSTOP and iptables variants, since they share the same guard."""
    runner.target_node.raft.is_consistent_topology_changes_enabled = False

    reason = nemesis_class(runner).precheck(runner.target_node)

    assert reason == "Raft feature: consistent-topology-changes is not enabled"


@pytest.mark.parametrize("nemesis_class", [IsolateNodeWithProcessSignalNemesis, IsolateNodeWithIptableRuleNemesis])
def test_precheck_skips_on_kubernetes(runner, nemesis_class):
    """precheck() must return a skip reason on kubernetes, which isn't supported yet,
    for both the SIGSTOP and iptables variants, since they share the same guard."""
    runner._is_it_on_kubernetes.return_value = True

    reason = nemesis_class(runner).precheck(runner.target_node)

    assert reason == "Skip test for K8S because no supported yet"


@pytest.mark.parametrize("nemesis_class", [IsolateNodeWithProcessSignalNemesis, IsolateNodeWithIptableRuleNemesis])
def test_precheck_passes_when_conditions_are_met(runner, nemesis_class):
    """precheck() must return None once raft topology-changes are enabled and it's not k8s."""
    assert nemesis_class(runner).precheck(runner.target_node) is None


def test_raises_when_alone_in_rack(runner):
    """UnsupportedNemesis must be raised when the target is the only node in its rack."""
    runner.cluster.data_nodes = [runner.target_node]

    with pytest.raises(UnsupportedNemesis, match="alone in its rack"):
        refuse_connection_from_banned_node(runner)


# ---------------------------------------------------------------------------
# refuse_connection_from_banned_node — open scylla-drivers#95 issue
# ---------------------------------------------------------------------------


@patch("sdcm.nemesis.monkey.node_isolation.SkipPerIssues")
def test_open_driver_issue_triggers_rack_switch(mock_skip_per_issues, runner):
    """When SkipPerIssues("scylladb/scylla-drivers#95", ...) is truthy, the target node
    must be switched to another rack before the ban sequence proceeds. NOTE: the
    surrounding inline comment ("we should disable the target node switching") reads as
    the opposite of what this branch does (it performs the switch) — that mismatch
    predates this extraction (introduced in 7ba6cec3c) and is called out for pehala to
    clarify rather than resolved here; this test only pins down the current behavior."""
    mock_skip_per_issues.return_value = True  # issue still open

    runner.cluster.params.get.return_value = True  # rack_aware_loader on
    runner.target_node.parent_cluster.racks_count = 2
    loader = _node("loader1", rack="rack1")
    loader.ip_address = "10.0.0.2"
    runner.loaders.nodes = [loader]
    runner.cluster.nodes = [runner.target_node, _node("node-other-rack", rack="rack2")]
    runner.set_target_node = MagicMock()

    refuse_connection_from_banned_node(runner, use_iptables=True)

    runner.set_target_node.assert_called_once_with(rack="rack2")


# ---------------------------------------------------------------------------
# refuse_connection_from_banned_node — finalizer cleanup: KillNemesis vs. failure
# ---------------------------------------------------------------------------


def test_kill_nemesis_during_removal_skips_finalizer_cleanup(runner):
    """If KillNemesis propagates out of `nodetool removenode` (raised at test teardown
    when the nemesis thread is killed), the ExitStack finalizer must NOT call
    _remove_node_add_node, and KillNemesis must still propagate uncaught.

    Contrast with test_generic_exception_during_removal_still_runs_finalizer_cleanup_and_reraises,
    which raises a different exception type from the same call site and gets the opposite
    cleanup behavior — that's what proves the `is not KillNemesis` branch actually matters,
    rather than both tests merely observing "the finalizer never ran"."""
    working_node = _working_node(runner)
    working_node.run_nodetool.side_effect = KillNemesis()

    with pytest.raises(KillNemesis):
        refuse_connection_from_banned_node(runner, use_iptables=True)

    runner._remove_node_add_node.assert_not_called()


def test_generic_exception_during_removal_still_runs_finalizer_cleanup_and_reraises(runner):
    """Same injection point as the KillNemesis test above (`nodetool removenode` raising),
    but with a plain exception: the finalizer must still perform its best-effort cleanup
    via _remove_node_add_node, and the original exception must still propagate."""
    working_node = _working_node(runner)
    working_node.run_nodetool.side_effect = RuntimeError("removenode failed unexpectedly")

    with pytest.raises(RuntimeError, match="removenode failed unexpectedly"):
        refuse_connection_from_banned_node(runner, use_iptables=True)

    runner._remove_node_add_node.assert_called_once_with(
        verification_node=working_node,
        node_to_remove=runner.target_node,
        remove_node_host_id=runner.target_node.host_id,
    )


def test_raises_when_node_not_removed_from_cluster(runner):
    """The `is_node_removed_from_cluster` assertion must fail — a distinct failure
    point from the two above — when the target is still listed as part of the cluster
    after `nodetool removenode` genuinely fails to remove it."""
    runner.target_node.parent_cluster.get_nodetool_status.return_value = {
        "dc1": {runner.target_node.ip_address: {"state": "UN"}}
    }

    with pytest.raises(AssertionError, match="was not removed"):
        refuse_connection_from_banned_node(runner, use_iptables=True)


# ---------------------------------------------------------------------------
# refuse_connection_from_banned_node — ban-verification assertions
# ---------------------------------------------------------------------------


def test_raises_when_scylla_still_running_after_ban(runner):
    """The closing assertion must fail if the target's scylla process is still
    running after the ban/removal sequence otherwise completes successfully."""
    runner.target_node.remoter.run.return_value = MagicMock(ok=True, stdout="12345\n")

    with pytest.raises(AssertionError):
        refuse_connection_from_banned_node(runner, use_iptables=True)


@patch("sdcm.nemesis.monkey.node_isolation.wait_for")
def test_raises_when_ban_notification_pattern_not_found(mock_wait_for, runner):
    """The closing assertion must fail when the target's system log never emits the
    'received notification of being banned' pattern, even though the node was
    otherwise successfully banned and removed."""
    mock_wait_for.side_effect = _instant_wait_for
    runner.target_node.follow_system_log.return_value = iter([])  # no ban notification ever logged

    with pytest.raises(AssertionError, match="Ban notification patterns were not found"):
        refuse_connection_from_banned_node(runner, use_iptables=True)


# ---------------------------------------------------------------------------
# refuse_connection_from_banned_node — actual iptables / SIGSTOP rules
# ---------------------------------------------------------------------------


def test_iptables_path_blocks_and_unblocks_every_scylla_port(runner):
    """``IsolateNodeWithIptableRuleNemesis`` (use_iptables=True) must DROP every
    Scylla port with iptables/ip6tables, run `nodetool removenode` while blocked,
    then remove all the rules once the node is confirmed banned."""
    IsolateNodeWithIptableRuleNemesis(runner).disrupt()

    runner.node_allocator.run_nemesis.assert_called_once_with(nemesis_label="block_scylla_ports")

    commands = _sudo_commands(runner.target_node)
    for port in (7000, 7001, 9042, 9142, 19042, 19142):
        for table, action in (("iptables", "A"), ("iptables", "D"), ("ip6tables", "A"), ("ip6tables", "D")):
            assert commands.count(f"{table} -{action} INPUT -p tcp --dport {port} -j DROP") == 1

    working_node = _working_node(runner)
    working_node.run_nodetool.assert_called_once_with("removenode host-1", retry=0, long_running=True)
    runner._remove_node_add_node.assert_called_once_with(
        verification_node=working_node,
        node_to_remove=runner.target_node,
        remove_node_host_id="host-1",
    )


def test_sigstop_path_pauses_scylla_and_only_blocks_gossip_ports_during_removal(runner):
    """``IsolateNodeWithProcessSignalNemesis`` (use_iptables=False) must SIGSTOP/SIGCONT
    the scylla process, and additionally DROP only the inter-node ports (7000/7001)
    while `nodetool removenode` runs, so raft barriers can't stick to the still-open
    socket of the paused process."""
    IsolateNodeWithProcessSignalNemesis(runner).disrupt()

    runner.node_allocator.run_nemesis.assert_called_once_with(nemesis_label="pause_scylla_with_sigstop")

    commands = _sudo_commands(runner.target_node)
    assert commands.count("pkill --signal SIGSTOP -e scylla") == 1
    assert commands.count("pkill --signal SIGCONT -e scylla") == 1

    for port in (7000, 7001):
        assert commands.count(f"iptables -A INPUT -p tcp --dport {port} -j DROP") == 1
        assert commands.count(f"iptables -D INPUT -p tcp --dport {port} -j DROP") == 1
    for port in (9042, 9142, 19042, 19142):
        assert f"iptables -A INPUT -p tcp --dport {port} -j DROP" not in commands

    working_node = _working_node(runner)
    working_node.run_nodetool.assert_called_once_with("removenode host-1", retry=0, long_running=True)
