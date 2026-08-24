"""
Module containing the cluster grow/shrink nemesis classes.

Covers scaling the cluster up and down (adding then decommissioning data
nodes), doing the same within a brand new rack, adding and removing a
zero-token node, and removing a node from the cluster and adding a
replacement for it.

The grow/shrink primitives live in ``sdcm.nemesis.utils.topology_ops`` so
other nemesis that orchestrate topology operations (e.g.
``NemesisSequence``) can reuse them as well.
"""

import time

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, target_all_nodes, target_data_nodes
from sdcm.nemesis.utils.topology_ops import grow_cluster, shrink_cluster
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.version_utils import scylla_versions


@latency_calculator_decorator(legend="Doubling cluster load")
def _double_cluster_load(runner, duration: int) -> None:
    duration = 30
    stress_cmd = runner.tester.stress_cmd
    if isinstance(stress_cmd, list):
        stress_cmd = stress_cmd[0]
    runner.log.info("Doubling the load on the cluster for %s minutes", duration)
    stress_queue = runner.tester.run_stress_thread(
        stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False, duration=duration
    )
    results = runner.tester.verify_stress_thread(
        thread_pool=stress_queue, error_handler=runner._nemesis_stress_failure_handler
    )
    runner.log.info(f"Double load results: {results}")


@target_data_nodes
class GrowShrinkClusterNemesis(NemesisBaseClass):
    """Add nodes to the cluster, optionally double the load, then decommission them back."""

    disruptive = True
    kubernetes = True
    topology_changes = True

    def disrupt(self):
        sleep_time_between_ops = self.runner.cluster.params.get("nemesis_sequence_sleep_between_ops")
        if not self.runner.has_steady_run and sleep_time_between_ops:
            self.runner.steady_state_latency()
            self.runner.has_steady_run = True
        new_nodes = grow_cluster(self.runner, rack=None)

        # pass on the exact nodes only if we have specific types for them
        new_nodes = new_nodes if self.runner.tester.params.get("nemesis_grow_shrink_instance_type") else None
        if duration := self.runner.tester.params.get("nemesis_double_load_during_grow_shrink_duration"):
            with self.runner.action_log_scope("Double load after grow cluster"):
                _double_cluster_load(self.runner, duration)
        shrink_cluster(self.runner, rack=None, new_nodes=new_nodes)


class AddRemoveRackNemesis(NemesisBaseClass):
    """Add a new rack to a Kubernetes cluster and decommission it back."""

    disruptive = True
    kubernetes = True
    config_changes = True

    @property
    def cluster(self):
        """Cluster under test, required by the ``scylla_versions`` decorator."""
        return self.runner.cluster

    # NOTE: version limitation is caused by the following:
    #       - https://github.com/scylladb/scylla-enterprise/issues/3211
    #       - https://github.com/scylladb/scylladb/issues/14184
    @scylla_versions(("5.2.7", None), ("2023.1.1", None))
    def disrupt(self):
        if not self.runner._is_it_on_kubernetes():
            raise UnsupportedNemesis("Adding new rack is not supported for non-k8s Scylla clusters")
        rack = max(self.runner.cluster.racks) + 1
        grow_cluster(self.runner, rack)
        shrink_cluster(self.runner, rack)


@target_all_nodes
class GrowShrinkZeroTokenNode(NemesisBaseClass):
    """Add a zero-token node to the target node datacenter and decommission one back."""

    disruptive = True
    zero_node_changes = True

    def disrupt(self):
        """Add/remove znodes to same dc where target node. The target node could be any node"""
        if not self.runner.cluster.params.get("use_zero_nodes"):
            raise UnsupportedNemesis("The zero tokens support is not enabled")

        duration_with_znode = 300
        new_znode = self.runner._add_and_init_new_cluster_nodes(count=1, is_zero_node=True)[0]
        self.runner.log.debug("Run with zero-token node %s for %ds", new_znode.name, duration_with_znode)
        time.sleep(duration_with_znode)
        znode = self.runner.random.choice(
            [node for node in self.runner.cluster.zero_nodes if node.dc_idx == self.runner.target_node.dc_idx]
        )
        self.runner.decommission_nodes(nodes=[znode])


@target_all_nodes
class TerminateAndRemoveNodeMonkey(NemesisBaseClass):
    """Remove a Node from a Scylla Cluster (Down Scale)"""

    disruptive = True
    # It should not be run on kubernetes, since it is a manual procedure
    # While on kubernetes we put it all on scylla-operator
    kubernetes = False
    topology_changes = True
    supports_high_disk_utilization = False  # Removing a node consumes disk space

    def disrupt(self):
        """
        https://docs.scylladb.com/operating-scylla/procedures/cluster-management/remove_node/

        1. Terminate node
        2. Run full repair
        3. Nodetool removenode, if removenode rejected, because removing node is UN in gossiper,
           repeat operation in 5 second
        4. Add new node
        5. Run nodetool cleanup (on each node) for each keyspace
        """
        if self.runner.cluster.params.get("db_type") == "cloud_scylla":
            raise UnsupportedNemesis(
                "Skipping this nemesis due the replace node option that supported by Cloud "
                "is tested by CloudReplaceNonResponsiveNode nemesis"
            )

        node_to_remove = self.runner.target_node
        up_normal_nodes = self.runner.cluster.get_nodes_up_and_normal(verification_node=node_to_remove)

        with self.runner.node_allocator.run_nemesis(
            nemesis_label="RemoveNodeAddNode", node_list=up_normal_nodes
        ) as verification_node:
            self.runner._remove_node_add_node(verification_node=verification_node, node_to_remove=node_to_remove)
