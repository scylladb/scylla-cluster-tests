"""
Module containing all node ban/isolation nemesis classes.

Simulates a banned node trying to reconnect to the cluster after removal,
by refusing connections from it. Node unavailability is simulated either by
blocking Scylla ports with iptables or by pausing the Scylla process with
SIGSTOP.
"""

from contextlib import ExitStack

from sdcm.cluster import BaseNode
from sdcm.exceptions import KillNemesis, UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, target_all_nodes
from sdcm.nemesis.utils import node_operations
from sdcm.sct_events.group_common_events import ignore_raft_topology_cmd_failing
from sdcm.utils.issues import SkipPerIssues
from sdcm.wait import wait_for


def switch_target_node_to_another_rack(runner) -> None:
    """
    Switches the target node to a rack different than loader node rack.

    This method selects a node from a different rack than the loader node rack
    and sets it as the new target node. It is useful for testing rack-aware scenarios.
    """
    if runner.cluster.params.get("rack_aware_loader") and runner.target_node.parent_cluster.racks_count > 1:
        loader_rack = runner.loaders.nodes[0].rack
        target_node_rack = [node.rack for node in runner.cluster.nodes if node.rack != loader_rack][0]
        runner.set_target_node(rack=target_node_rack)
        runner.log.info("Target node rack %s, loader rack %s", runner.target_node.rack, loader_rack)


def is_single_node_in_rack(runner, node: BaseNode) -> bool:
    return len([n for n in runner.cluster.data_nodes if (n.rack == node.rack and n.dc_idx == node.dc_idx)]) == 1


def refuse_connection_from_banned_node(runner, use_iptables: bool = False) -> None:
    """Banned node could not connect with rest nodes in cluster

    If node was removed from cluster for any reason, even if removed node
    become alive and try to communicate with rest node in cluster, all connections
    from it should be refused by other nodes in cluster
    1. on target node block any scylladb process
    1.1 Pause process
    1.2 Block with iptables port 9100/10000
    2. Wait and remove target node from cluster
    3. start scylla on target node
    4. Create exclusive connection to target node
    5. Execute cql command on target node and validate that no operation
    from target node passed to cluster
    """
    if SkipPerIssues("scylladb/scylla-drivers#95", runner.cluster.params):
        # until https://github.com/scylladb/scylla-drivers/issues/95 would be solved
        # we should disable the target node switching
        switch_target_node_to_another_rack(runner)
    if is_single_node_in_rack(runner, runner.target_node):
        raise UnsupportedNemesis(f"Target node {runner.target_node.name} is alone in its rack, cannot remove it.")

    def is_scylla_running(node: BaseNode) -> bool:
        result = node.remoter.run("ps -C scylla -o pid --no-headers", ignore_status=True)
        return result.ok and bool(result.stdout.strip())

    simulate_node_unavailability = (
        node_operations.block_scylla_ports if use_iptables else node_operations.pause_scylla_with_sigstop
    )
    with (
        runner.node_allocator.run_nemesis(nemesis_label=f"{simulate_node_unavailability.__name__}") as working_node,
        ExitStack() as stack,
    ):
        stack.enter_context(
            node_operations.block_loaders_payload_for_scylla_node(runner.target_node, loader_nodes=runner.loaders.nodes)
        )
        target_host_id = runner.target_node.host_id

        def _finalizer(exc_type, *_):
            if exc_type is not KillNemesis:
                runner._remove_node_add_node(
                    verification_node=working_node,
                    node_to_remove=runner.target_node,
                    remove_node_host_id=target_host_id,
                )
            return False

        stack.push(_finalizer)

        pattern = ["received notification of being banned from the cluster"]
        follower = runner.target_node.follow_system_log(
            patterns=pattern,
        )

        with simulate_node_unavailability(runner.target_node):
            # target node stopped by Contextmanger. Wait while its status will be updated
            runner.actions_log.info(
                f"Blocked {runner.target_node.name} node with {simulate_node_unavailability.__name__}"
            )
            wait_for(
                node_operations.is_node_seen_as_down,
                step=5,
                timeout=600,
                throw_exc=True,
                down_node=runner.target_node,
                verification_node=working_node,
                text=f"Wait other nodes see {runner.target_node.name} as DOWN...",
            )
            runner.log.debug(
                "Remove node %s : hostid: %s with blocked scylla from cluster",
                runner.target_node.name,
                target_host_id,
            )
            runner.actions_log.info(f"Remove {runner.target_node.name} node from cluster")
            # For process paused with SIGSTOP signal, network sockets are still open,
            # so already running raft barriers could stuck. To avoid that
            # we need to block scylla ports on target node.
            with ignore_raft_topology_cmd_failing():
                if simulate_node_unavailability == node_operations.pause_scylla_with_sigstop:
                    with node_operations.block_scylla_ports(runner.target_node, ports=[7000, 7001]):
                        working_node.run_nodetool(f"removenode {target_host_id}", retry=0, long_running=True)
                else:
                    working_node.run_nodetool(f"removenode {target_host_id}", retry=0, long_running=True)

            assert node_operations.is_node_removed_from_cluster(
                removed_node=runner.target_node, verification_node=working_node
            ), f"Node {runner.target_node.name} with host id {target_host_id} was not removed. See log errors"

        wait_for(
            lambda: not runner.target_node.db_up(),
            step=5,
            timeout=60,
            throw_exc=True,
            text=f"Wait banned node {runner.target_node.name} to terminate after ban notification...",
        )

        assert wait_for(
            func=lambda: list(follower),
            timeout=30,
            text="Waiting for ban notification patterns in log",
            throw_exc=False,
        ), "Ban notification patterns were not found in system log"

        runner.actions_log.info(
            "Banned node %s has NOTIFY_BANNED and terminating messages in logs",
            runner.target_node.name,
        )

        assert not is_scylla_running(runner.target_node)


@target_all_nodes
class IsolateNodeWithProcessSignalNemesis(NemesisBaseClass):
    disruptive = True
    topology_changes = True
    supports_high_disk_utilization = False  # Runs nodetool removenode

    def precheck(self, node: BaseNode) -> str | None:
        if not node.raft.is_consistent_topology_changes_enabled:
            return "Raft feature: consistent-topology-changes is not enabled"
        if self.runner._is_it_on_kubernetes():
            return "Skip test for K8S because no supported yet"
        return None

    def disrupt(self):
        refuse_connection_from_banned_node(self.runner, use_iptables=False)


@target_all_nodes
class IsolateNodeWithIptableRuleNemesis(IsolateNodeWithProcessSignalNemesis):
    def disrupt(self):
        refuse_connection_from_banned_node(self.runner, use_iptables=True)
