from invoke import UnexpectedExit

from sdcm.cluster import NodeCleanedAfterDecommissionAborted, NodeStayInClusterAfterDecommission
from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, target_data_nodes
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit as Libssh2UnexpectedExit
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.utils.tasks import wait_for_tasks
from sdcm.utils.parallel_object import ParallelObject
from sdcm.wait import wait_for

# How long to wait for the node to recover to UN after a successful abort.
# A node may land in a transitional state (UJ / DN) for up to several minutes
# after the decommission stream is interrupted, so 300 s gives enough headroom.
_RECOVERY_TIMEOUT_S = 300


@target_data_nodes
class AbortDecommissionMonkey(NemesisBaseClass):
    disruptive = True

    def decommission_target_node(self):
        try:
            with EventsSeverityChangerFilter(
                new_severity=Severity.WARNING,
                event_class=DatabaseLogEvent,
                extra_time_to_expiration=30,
                regex=r".*Decommission failed. See earlier errors \(aborted on user request\).*",
            ):
                self.runner.target_node.run_nodetool(
                    sub_cmd="decommission",
                    warning_event_on_exception=(UnexpectedExit, Libssh2UnexpectedExit),
                    long_running=True,
                    retry=0,
                )
        except (UnexpectedExit, Libssh2UnexpectedExit) as ex:
            if "Decommission failed. See earlier errors (aborted on user request)" in ex.stdout + ex.stderr:
                self.runner.actions_log.info("Decommission was aborted as expected")
            else:
                raise

    def abort_decommission_task(self):
        task_id = wait_for_tasks(
            self.runner.target_node,
            module="node_ops",
            timeout=60,
            filter={"entity": self.runner.target_node.host_id, "type": "decommission"},
        )[0]["task_id"]

        # In order to ensure streaming has started, we follow the system log for a message
        # indicating that SSTable streaming has finished for at least one sstable.
        # Once the `log_follower` yields a line, we know streaming has started.
        log_follower = self.runner.target_node.follow_system_log(
            patterns=[r"stream_blob - stream_sstables\[[0-9a-f-]+\] Finished sending sstable_nr"]
        )

        _streaming_started = lambda: len(list(log_follower)) > 0

        wait_for(_streaming_started, timeout=60)
        # Abort the decommission task after streaming has started
        self.runner.target_node.run_nodetool(
            f"tasks abort {task_id}", warning_event_on_exception=(UnexpectedExit, Libssh2UnexpectedExit), retry=0
        )
        # This will wait until either abort finishes, or, if abort fails, until decommission finishes
        self.runner.target_node.run_nodetool(f"tasks wait {task_id}")

    def disrupt(self):
        """
        Start decommission on target node and abort it after streaming starts.

        Outcome is determined by cluster.verify_decommission() which inspects
        gossip and Raft group0:

        - NodeStayInClusterAfterDecommission: abort worked, node is still in
          the token ring (possibly in a transient UJ/DN state).  Wait up to
          _RECOVERY_TIMEOUT_S seconds for the node to return to UN and exit
          without adding a replacement.

        - NodeCleanedAfterDecommissionAborted: decommission was partially
          completed — the node was removed from the token ring but left a
          group0 entry.  verify_decommission already terminated the node and
          cleaned Raft group0.  Add a replacement node.

        - Returns normally: full decommission succeeded despite the abort.
          verify_decommission already terminated the node.  Add a replacement.
        """
        if len([n for n in self.runner.cluster.data_nodes if n.rack == self.runner.target_node.rack]) == 1:
            raise UnsupportedNemesis(
                f"Target node {self.runner.target_node.name} is the only one in rack {self.runner.target_node.rack}, cannot decommission it."
            )

        # save info of the target node, as it will not be available if decommission succeeds
        target_is_seed = self.runner.target_node.is_seed
        target_name = self.runner.target_node.name

        # Run decommission and abort in parallel, since we want to abort as soon as streaming starts, without waiting for decommission to finish
        ParallelObject(
            objects=[self.decommission_target_node, self.abort_decommission_task], timeout=600
        ).call_objects()

        # verify_decommission checks gossip + Raft to determine the actual outcome.
        # It also calls terminate_node() internally when the node has left the ring,
        # which removes the node from cluster.nodes and prevents ghost-node pollution.
        try:
            self.runner.cluster.verify_decommission(self.runner.target_node)
        except NodeStayInClusterAfterDecommission:
            # Abort succeeded: node is still in the ring (possibly in a transitional
            # state such as UJ or DN).  Wait for it to settle back to UN.
            self.runner.log.info(
                "Abort decommission succeeded for node %s. Waiting up to %d s for recovery.",
                target_name,
                _RECOVERY_TIMEOUT_S,
            )
            self.runner.cluster.wait_for_nodes_up_and_normal(
                nodes=[self.runner.target_node], timeout=_RECOVERY_TIMEOUT_S
            )
            return
        except NodeCleanedAfterDecommissionAborted:
            # Partial decommission: node was removed from the token ring and terminated
            # by verify_decommission; Raft group0 has been cleaned.  Add a replacement.
            self.runner.log.warning(
                "Node %s was partially decommissioned (cleaned from group0). Adding a replacement.",
                target_name,
            )
        else:
            # Full decommission: node was terminated by verify_decommission.
            self.runner.log.warning(
                "Node %s was fully decommissioned despite abort. Adding a replacement.",
                target_name,
            )

        # Either full or partial decommission: the node is already gone.  Add a replacement.
        new_node = self.runner.add_new_nodes(count=1, rack=self.runner.target_node.rack)[0]
        if new_node.is_seed != target_is_seed:
            new_node.set_seed_flag(target_is_seed)
            self.runner.cluster.update_seed_provider()
        self.runner.log.info("Added new node %s to replace decommissioned node %s", new_node.name, target_name)
        self.runner.monitoring_set.reconfigure_scylla_monitoring()
