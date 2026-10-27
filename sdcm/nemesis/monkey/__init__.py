"""
Module containing all disruptions logic/classes
Classes can be used in nemesis_selector
"""

from sdcm.nemesis import target_all_nodes, NemesisBaseClass, target_data_nodes


@target_all_nodes
class SslHotReloadingNemesis(NemesisBaseClass):
    """Replace the internode TLS certificate on every node and verify Scylla hot-reloads it."""

    disruptive = False
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_hot_reloading_internode_certificate()


@target_all_nodes
class PauseLdapNemesis(NemesisBaseClass):
    """Pause the LDAP container for 3 minutes, then resume it."""

    disruptive = False
    limited = True

    additional_configs = ["configurations/ldap-authorization.yaml", "configurations/auth_cassandra.yaml"]

    def disrupt(self):
        self.runner.disrupt_ldap_connection_toggle()


@target_all_nodes
class ToggleLdapConfiguration(NemesisBaseClass):
    """Remove the LDAP configuration from every node and pause LDAP for 10 minutes, then restore both."""

    disruptive = True
    limited = True

    additional_configs = ["configurations/ldap-authorization.yaml", "configurations/auth_cassandra.yaml"]

    def disrupt(self):
        self.runner.disrupt_disable_enable_ldap_authorization()


class AddRemoveDcNemesis(NemesisBaseClass):
    """
    https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cluster-management/add-dc-to-existing-dc.html
    https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cluster-management/decommissioning-data-center.html

    1. Create and populate a new keyspace
    2. Add some new nodes in a new datacenter
    3. Ensure relevant keyspaces are using NetworkTopologyStrategy
    4. Alter the keyspaces to add the new datacenter with the appropriate replication factor
    5. Run rebuild on the new datacenter nodes, if vnodes
    6. Run a full cluster repair
    7. Run some writes and reads to verify the new datacenter is working
    8. Alter the keyspaces to set replication factor to 0 for the new datacenter
    9. Decommission the new datacenter nodes
    10. Verify the keyspace is healthy after decommissioning the new datacenter
    """

    disruptive = True
    limited = True
    topology_changes = True

    def precheck(self, node) -> str | None:
        if self.runner.cluster.test_config.MULTI_REGION:
            return "Skipped for multi-dc scenario (https://github.com/scylladb/scylla-cluster-tests/issues/5369)"
        return None

    def disrupt(self):
        self.runner.disrupt_add_remove_dc()


@target_all_nodes
class StopWaitStartMonkey(NemesisBaseClass):
    """Stop Scylla on the target node, wait 10 minutes and start it back."""

    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    zero_node_changes = True

    def disrupt(self):
        self.runner.disrupt_stop_wait_start_scylla_server(600)


@target_all_nodes
class StopStartMonkey(NemesisBaseClass):
    """Stop Scylla on the target node and start it right back."""

    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_stop_start_scylla_server()


@target_all_nodes
class RestartThenRepairNodeMonkey(NemesisBaseClass):
    """Restart the target node, wait for it to fully start and run a full repair."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_restart_then_repair_node()


@target_all_nodes
class MultipleHardRebootNodeMonkey(NemesisBaseClass):
    """Hard reboot the target node 2 to 10 times in a row with random pauses, verifying CDC generation updates."""

    disruptive = True
    kubernetes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_multiple_hard_reboot_node()


@target_all_nodes
class HardRebootNodeMonkey(NemesisBaseClass):
    """Hard reboot the target node and wait for it to fully start."""

    disruptive = True
    kubernetes = True
    limited = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_hard_reboot_node()


@target_all_nodes
class SoftRebootNodeMonkey(NemesisBaseClass):
    """Soft reboot the target node and wait for it to fully start."""

    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_soft_reboot_node()


@target_all_nodes
class DrainerMonkey(NemesisBaseClass):
    """Run nodetool drain on the target node, then stop and start Scylla to bring it back."""

    disruptive = True
    kubernetes = True
    limited = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_nodetool_drain()


class CorruptThenRepairMonkey(NemesisBaseClass):
    """Repair the cluster, delete half of the target node's tombstone-free sstables and repair the data back."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_destroy_data_then_repair()


class CorruptThenRebuildMonkey(NemesisBaseClass):
    """Stop Scylla on the target node, delete half of its sstables, restart it and rebuild the data back."""

    disruptive = True
    kubernetes = True
    xcloud = True

    def disrupt(self):
        self.runner.disrupt_destroy_data_then_rebuild()


@target_all_nodes
class DecommissionMonkey(NemesisBaseClass):
    """Decommission the target node, add a replacement node in the same rack and run cleanup on all nodes."""

    disruptive = True
    limited = True
    topology_changes = True
    supports_high_disk_utilization = (
        False  # Decommissioning a node cause increase of disk space across rest of the nodes
    )

    def disrupt(self):
        self.runner.disrupt_nodetool_decommission()


@target_all_nodes
class DecommissionSeedNode(NemesisBaseClass):
    """Decommission a seed node, add a replacement node in the same rack and promote it to seed."""

    disruptive = True
    topology_changes = True
    supports_high_disk_utilization = (
        False  # Decommissioning a node cause increase of disk space across rest of the nodes
    )

    def disrupt(self):
        self.runner.disrupt_nodetool_seed_decommission()


class NoCorruptRepairMonkey(NemesisBaseClass):
    """Run a repair on the target node while dropping 10 freshly created test tables one by one."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_no_corrupt_repair()


class MajorCompactionMonkey(NemesisBaseClass):
    """Run nodetool compact on the target node."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_major_compaction()


@target_all_nodes
class RefreshMonkey(NemesisBaseClass):
    """Upload a small pre-built sstable to every data node and load it with nodetool refresh, verifying resharding."""

    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_nodetool_refresh(big_sstable=False)


@target_data_nodes
class LoadAndStreamMonkey(NemesisBaseClass):
    """Upload pre-built sstables to every data node and load them with nodetool refresh --load-and-stream."""

    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_load_and_stream()


@target_all_nodes
class RefreshBigMonkey(NemesisBaseClass):
    """Upload a big pre-built sstable to every data node and load it with nodetool refresh, verifying resharding."""

    disruptive = False
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_nodetool_refresh(big_sstable=True)


@target_all_nodes
class EnospcMonkey(NemesisBaseClass):
    """Fill the data disk of the target node until ENOSPC, then free the space and restart Scylla."""

    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    enospc = True

    def disrupt(self):
        self.runner.disrupt_nodetool_enospc()


@target_all_nodes
class EnospcAllNodesMonkey(NemesisBaseClass):
    """Fill the data disk of every data node until ENOSPC, then free the space and restart Scylla on each of them."""

    disruptive = True
    kubernetes = True
    xcloud = True
    enospc = True

    def disrupt(self):
        self.runner.disrupt_nodetool_enospc(all_nodes=True)


@target_all_nodes
class NodeToolCleanupMonkey(NemesisBaseClass):
    """Run nodetool cleanup on all nodes in parallel."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_nodetool_cleanup()


class ToggleTableIcsMonkey(NemesisBaseClass):
    """Alter the compaction strategy of a random non-system table between ICS and another strategy."""

    kubernetes = True
    xcloud = True
    schema_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_table_ics()


class ToggleGcModeMonkey(NemesisBaseClass):
    """Alter the tombstone_gc mode of a random non-system table between 'repair' and 'timeout'."""

    kubernetes = True
    xcloud = True
    disruptive = False
    schema_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_table_gc_mode()


class AbortRepairMonkey(NemesisBaseClass):
    """Start a repair on the target node, abort its streaming through the REST API, then run a full repair."""

    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_abort_repair()


@target_all_nodes
class NodeTerminateAndReplace(NemesisBaseClass):
    """Terminate the target node, wait 5 minutes, then add a replacement with the replace-dead-node procedure."""

    disruptive = True
    # It should not be run on kubernetes, since it is a manual procedure
    # While on kubernetes we put it all on scylla-operator
    kubernetes = False
    topology_changes = True
    zero_node_changes = True

    def disrupt(self):
        self.runner.disrupt_terminate_and_replace_node()


class DrainKubernetesNodeThenReplaceScyllaNode(NemesisBaseClass):
    """Drain the Kubernetes host of the target pod, then replace the Scylla node through scylla-operator."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_drain_kubernetes_node_then_replace_scylla_node()


class TerminateKubernetesHostThenReplaceScyllaNode(NemesisBaseClass):
    """Terminate the Kubernetes host of the target pod, then replace the Scylla node through scylla-operator."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_terminate_kubernetes_host_then_replace_scylla_node()


class DrainKubernetesNodeThenDecommissionAndAddScyllaNode(NemesisBaseClass):
    """Drain the Kubernetes host of the target pod, decommission the Scylla node and add a new one in its rack."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_drain_kubernetes_node_then_decommission_and_add_scylla_node()


class TerminateKubernetesHostThenDecommissionAndAddScyllaNode(NemesisBaseClass):
    """Terminate the Kubernetes host of the target pod, decommission the Scylla node and add a new one in its rack."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_terminate_kubernetes_host_then_decommission_and_add_scylla_node()


class OperatorNodeReplace(NemesisBaseClass):
    """Mark the target Scylla pod for replacement and wait for scylla-operator to recreate it."""

    disruptive = True
    kubernetes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_replace_scylla_node_on_kubernetes()


class OperatorNodetoolFlushAndReshard(NemesisBaseClass):
    """Flush keyspace1, then change the Scylla pod CPU limit on EKS to trigger resharding and revert it."""

    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_nodetool_flush_and_reshard_on_kubernetes()


@target_all_nodes
class ScyllaKillMonkey(NemesisBaseClass):
    """Kill the Scylla process on the target node with SIGKILL and wait for it to come back up."""

    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_kill_scylla()


@target_data_nodes
class ValidateHintedHandoffShortDowntime(NemesisBaseClass):
    """Stop Scylla on the target node for 10 seconds, start it and verify hints arrive without drops or errors."""

    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_validate_hh_short_downtime()


@target_all_nodes
class SnapshotOperations(NemesisBaseClass):
    """Take a nodetool snapshot of all, one or a few keyspaces or a few tables, validate it and clear it."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_snapshot_operations()


class NodeRestartWithResharding(NemesisBaseClass):
    """Restart the target node with changed murmur3_partitioner_ignore_msb_bits to reshard, then restore it."""

    disruptive = True
    topology_changes = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_restart_with_resharding()


@target_all_nodes
class ClusterRollingRestart(NemesisBaseClass):
    """Restart Scylla on every node of the cluster one by one, in cluster order."""

    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_rolling_restart_cluster(random_order=False)


@target_all_nodes
class RollingRestartConfigChangeInternodeCompression(NemesisBaseClass):
    """Switch internode_compression to a different value on every node with a rolling restart."""

    disruptive = True
    full_cluster_restart = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_rolling_config_change_internode_compression()


@target_all_nodes
class ClusterRollingRestartRandomOrder(NemesisBaseClass):
    """Restart Scylla on every node of the cluster one by one, in random order."""

    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_rolling_restart_cluster(random_order=True)


class SwitchBetweenPasswordAuthAndSaslauthdAuth(NemesisBaseClass):
    """Switch between PasswordAuthenticator and SaslauthdAuthenticator, run a short workload and switch back."""

    disruptive = True  # the nemesis has rolling restart
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back()


@target_all_nodes
class TopPartitions(NemesisBaseClass):
    """Run nodetool toppartitions on the target node and verify its output."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_show_toppartitions()


class NemesisSequence(NemesisBaseClass):
    """Run a fixed sequence: manager repair, grow the cluster, terminate and replace a node, then shrink the cluster."""

    disruptive = True
    networking = False

    def disrupt(self):
        self.runner.disrupt_run_unique_sequence()


class ToggleCDCMonkey(NemesisBaseClass):
    """Toggle a random CDC property on a random table with CDC enabled and verify the new settings."""

    disruptive = False
    schema_changes = True
    config_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_cdc_feature_properties_on_table()


class CDCStressorMonkey(NemesisBaseClass):
    """Run the CDC log reader stress tool against a random table with CDC enabled."""

    disruptive = False
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_run_cdcstressor_tool()


class DecommissionStreamingErrMonkey(NemesisBaseClass):
    """Start a decommission, hard reboot the node once unbootstrapping starts, then rebuild or re-add the node."""

    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_decommission_streaming_err()


class RebuildStreamingErrMonkey(NemesisBaseClass):
    """Delete sstables on the target node, start nodetool rebuild, hard reboot it mid-stream and rebuild again."""

    disruptive = True

    def disrupt(self):
        self.runner.disrupt_rebuild_streaming_err()


class RepairStreamingErrMonkey(NemesisBaseClass):
    """Delete sstables on the target node, start nodetool repair, hard reboot it mid-stream and rebuild or repair it."""

    disruptive = True

    def disrupt(self):
        self.runner.disrupt_repair_streaming_err()


@target_data_nodes
class CorruptThenScrubMonkey(NemesisBaseClass):
    """Run nodetool scrub --skip-corrupted on every test keyspace of the target node and clear the snapshots."""

    disruptive = False
    supports_high_disk_utilization = False  # Failed for: https://github.com/scylladb/scylladb/issues/22088

    def disrupt(self):
        self.runner.disrupt_corrupt_then_scrub()


@target_all_nodes
class MemoryStressMonkey(NemesisBaseClass):
    """Run stress-ng on the target node to allocate 90% of its memory and push Scylla into swapping."""

    disruptive = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_memory_stress()


@target_all_nodes
class ResetLocalSchemaMonkey(NemesisBaseClass):
    disruptive = False
    config_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_resetlocalschema()


class StartStopMajorCompaction(NemesisBaseClass):
    """Start a major compaction on a non-system table and stop it with nodetool stop COMPACTION once it is logged."""

    disruptive = False

    def disrupt(self):
        self.runner.disrupt_start_stop_major_compaction()


class StartStopScrubCompaction(NemesisBaseClass):
    """Start a scrub compaction on a non-system table and stop it with nodetool stop SCRUB once it is logged."""

    disruptive = False

    def disrupt(self):
        self.runner.disrupt_start_stop_scrub_compaction()


class StartStopCleanupCompaction(NemesisBaseClass):
    """Start a cleanup compaction on a non-system table and stop it with nodetool stop CLEANUP once it is logged."""

    disruptive = False

    def disrupt(self):
        self.runner.disrupt_start_stop_cleanup_compaction()


class StartStopValidationCompaction(NemesisBaseClass):
    """Start a validation compaction on a non-system table and stop it with nodetool stop SCRUB once it is logged."""

    disruptive = False
    supports_high_disk_utilization = False  # Failed for: https://github.com/scylladb/scylladb/issues/22088

    def disrupt(self):
        self.runner.disrupt_start_stop_validation_compaction()


class ToggleAuditNemesisSyslog(NemesisBaseClass):
    """Enable audit logging to syslog for all categories, verify it, narrow the categories and verify DDL logs."""

    disruptive = True
    schema_changes = True
    config_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_audit_syslog()


@target_data_nodes
class BootstrapStreamingErrorNemesis(NemesisBaseClass):
    """Add a node, abort its bootstrap mid-streaming, clean group0, re-bootstrap it and decommission it."""

    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_bootstrap_streaming_error()


class DisableBinaryGossipExecuteMajorCompaction(NemesisBaseClass):
    """Disable binary and gossip on the target node, run a major compaction, enable both back and verify CQL works."""

    disruptive = True
    kubernetes = True
    xcloud = True

    def disrupt(self):
        self.runner.disrupt_disable_binary_gossip_execute_major_compaction()


@target_all_nodes
class EndOfQuotaNemesis(NemesisBaseClass):
    """Set a disk quota for the scylla user on the target node, fill it past the limit, lift it and restart Scylla."""

    disruptive = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_end_of_quota_nemesis()


@target_all_nodes
class SerialRestartOfElectedTopologyCoordinatorNemesis(NemesisBaseClass):
    """Stop the elected topology coordinator, verify a new one is elected and start it back, repeated randomly."""

    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_serial_restart_elected_topology_coordinator()


@target_all_nodes
class SplitMergeTabletsWithAlter(NemesisBaseClass):
    """Alter min_tablet_count on every tablets-enabled table and view to trigger a tablet split or merge."""

    schema_changes = True

    def disrupt(self):
        self.runner.disrupt_trigger_split_merge_tablets_with_alter()
