"""Tests for NemesisRunner.disrupt_multiple_hard_reboot_node (SCT-953)."""

from unittest.mock import MagicMock

import pytest

from sdcm.nemesis import NemesisRunner
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from sdcm.utils.metaclasses import Singleton
from unit_tests.unit.nemesis import make_mock_node

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture(autouse=True)
def clear_node_allocator_singleton():
    """Drop the NemesisNodeAllocator Singleton after each test to prevent cross-test pollution."""
    yield
    Singleton._instances.pop(NemesisNodeAllocator, None)


@pytest.fixture()
def instant_reboot_loop(monkeypatch):
    """Run the reboot loop a fixed number of times without sleeping between iterations.

    ``disrupt_multiple_hard_reboot_node`` draws both its reboot count and its inter-reboot
    sleep from ``random.randint``, so the factory pins the count and neutralises the sleep.

    Returns:
        A factory taking the desired number of reboots.
    """

    def _setup(reboots):
        monkeypatch.setattr("sdcm.nemesis.random.randint", lambda *_: reboots)
        monkeypatch.setattr("sdcm.nemesis.time.sleep", lambda *_: None)

    return _setup


@pytest.fixture()
def runner(base_runner):
    """Build a runner with a mocked allocator, for asserting on how it is called.

    The target node's system-log followers report no CDC error, so the reboot loop runs
    to completion instead of diverting into the CDC-failure branch.
    """
    base_runner.node_allocator = MagicMock()
    base_runner.reboot_node = MagicMock()
    base_runner.target_node = make_mock_node(name="node1", rack="rack1")
    base_runner.target_node.follow_system_log.return_value = []
    base_runner.cluster.nodes = [base_runner.target_node, base_runner.cluster.data_nodes[1]]
    return base_runner


@pytest.fixture()
def verification_node(runner):
    """The node the mocked allocator yields from its ``run_nemesis`` context."""
    return runner.node_allocator.run_nemesis.return_value.__enter__.return_value


@pytest.fixture()
def real_allocator_runner(base_runner):
    """Build a runner driven by a real NemesisNodeAllocator over a three-node pool.

    The target is reserved the way ``NemesisRunner.set_target_node`` reserves it, so the
    allocator is obliged to hand out one of the other two nodes for verification.
    """
    nodes = [make_mock_node(name=f"node{i}") for i in range(1, 4)]
    for node in nodes:
        # a bare MagicMock attribute is truthy, which would empty the data_nodes pool
        node._is_zero_token_node = False
        node.follow_system_log.return_value = []

    tester = MagicMock()
    tester.all_db_nodes = nodes
    allocator = NemesisNodeAllocator(tester)
    assert allocator.set_running_nemesis(nodes[0], "MultipleHardRebootNodeMonkey")

    base_runner.node_allocator = allocator
    base_runner.target_node = nodes[0]
    base_runner.reboot_node = MagicMock()
    base_runner.cluster.data_nodes = nodes
    base_runner.cluster.nodes = nodes
    return base_runner


def test_multiple_hard_reboot_verifies_through_an_allocated_peer(runner, verification_node, instant_reboot_loop):
    """The rebooted node must never be asked to confirm its own UN state."""
    instant_reboot_loop(reboots=2)

    NemesisRunner.disrupt_multiple_hard_reboot_node(runner)

    calls = runner.cluster.wait_for_nodes_up_and_normal.call_args_list
    assert len(calls) == 2, "one up-and-normal check per reboot"
    for call in calls:
        assert call.kwargs["nodes"] == [runner.target_node]
        assert call.kwargs["verification_node"] is verification_node
        assert call.kwargs["verification_node"] is not runner.target_node


def test_multiple_hard_reboot_allocates_a_verification_node_per_reboot(runner, instant_reboot_loop):
    """The peer is reserved and released around each check, not held for the whole loop."""
    instant_reboot_loop(reboots=3)

    NemesisRunner.disrupt_multiple_hard_reboot_node(runner)

    assert runner.node_allocator.run_nemesis.call_count == 3
    for call in runner.node_allocator.run_nemesis.call_args_list:
        assert call.kwargs == {"nemesis_label": "MultipleHardRebootNode verification"}
    # the context manager is exited each iteration, so the peer is handed back
    assert runner.node_allocator.run_nemesis.return_value.__exit__.call_count == 3


def test_multiple_hard_reboot_real_allocator_never_hands_back_the_target(real_allocator_runner, instant_reboot_loop):
    """A real allocator holding the target reserved hands out only other nodes."""
    instant_reboot_loop(reboots=4)
    runner = real_allocator_runner
    target = runner.target_node
    peers = [node for node in runner.cluster.data_nodes if node is not target]

    NemesisRunner.disrupt_multiple_hard_reboot_node(runner)

    used = [call.kwargs["verification_node"] for call in runner.cluster.wait_for_nodes_up_and_normal.call_args_list]
    assert len(used) == 4
    assert target not in used, "the rebooting node must never verify itself"
    assert set(used) <= set(peers)
    # every reservation was released, leaving only the disruption's own target held
    assert runner.node_allocator.active_nemesis_on_nodes == {target: "MultipleHardRebootNodeMonkey"}
