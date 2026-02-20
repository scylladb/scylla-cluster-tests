"""
Module containing all disruptions logic/classes
Classes can be used in nemesis_selector
"""

from sdcm.nemesis import target_all_nodes, NemesisBaseClass, target_data_nodes
from sdcm.nemesis.utils import NEMESIS_TARGET_POOLS


@target_all_nodes
class SslHotReloadingNemesis(NemesisBaseClass):
    disruptive = False
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_hot_reloading_internode_certificate()


@target_all_nodes
class PauseLdapNemesis(NemesisBaseClass):
    disruptive = False
    limited = True

    additional_configs = ["configurations/ldap-authorization.yaml"]

    def disrupt(self):
        self.runner.disrupt_ldap_connection_toggle()


@target_all_nodes
class ToggleLdapConfiguration(NemesisBaseClass):
    disruptive = True
    limited = True

    additional_configs = ["configurations/ldap-authorization.yaml"]

    def disrupt(self):
        self.runner.disrupt_disable_enable_ldap_authorization()


class AddRemoveDcNemesis(NemesisBaseClass):
    disruptive = True
    limited = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_add_remove_dc()


@target_data_nodes
class GrowShrinkClusterNemesis(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_grow_shrink_cluster()


class AddRemoveRackNemesis(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_grow_shrink_new_rack()


@target_all_nodes
class StopWaitStartMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    zero_node_changes = True

    def disrupt(self):
        self.runner.disrupt_stop_wait_start_scylla_server(600)


@target_all_nodes
class StopStartMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_stop_start_scylla_server()


@target_all_nodes
class EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = False  # Enable it when EKS SCT code starts supporting the KMS service

    def disrupt(self):
        self.runner.disrupt_enable_disable_table_encryption_aws_kms_provider_with_rotation()


@target_all_nodes
class EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = False  # Enable it when EKS SCT code starts supporting the KMS service

    target_pool = NEMESIS_TARGET_POOLS.all_nodes

    def disrupt(self):
        self.runner.disrupt_enable_disable_table_encryption_aws_kms_provider_without_rotation()


@target_all_nodes
class RestartThenRepairNodeMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_restart_then_repair_node()


@target_all_nodes
class MultipleHardRebootNodeMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_multiple_hard_reboot_node()


@target_all_nodes
class HardRebootNodeMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    limited = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_hard_reboot_node()


@target_all_nodes
class SoftRebootNodeMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_soft_reboot_node()


@target_all_nodes
class DrainerMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    limited = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_nodetool_drain()


class CorruptThenRepairMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_destroy_data_then_repair()


class CorruptThenRebuildMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True

    def disrupt(self):
        self.runner.disrupt_destroy_data_then_rebuild()


@target_all_nodes
class DecommissionMonkey(NemesisBaseClass):
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
    disruptive = True
    topology_changes = True
    supports_high_disk_utilization = (
        False  # Decommissioning a node cause increase of disk space across rest of the nodes
    )

    def disrupt(self):
        self.runner.disrupt_nodetool_seed_decommission()


class NoCorruptRepairMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_no_corrupt_repair()


class MajorCompactionMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_major_compaction()


@target_all_nodes
class RefreshMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_nodetool_refresh(big_sstable=False)


@target_data_nodes
class LoadAndStreamMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_load_and_stream()


@target_all_nodes
class RefreshBigMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_nodetool_refresh(big_sstable=True)


class RemoveServiceLevelMonkey(NemesisBaseClass):
    disruptive = True
    sla = True

    def disrupt(self):
        self.runner.disrupt_remove_service_level_while_load()


@target_all_nodes
class EnospcMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    enospc = True

    def disrupt(self):
        self.runner.disrupt_nodetool_enospc()


@target_all_nodes
class EnospcAllNodesMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    enospc = True

    def disrupt(self):
        self.runner.disrupt_nodetool_enospc(all_nodes=True)


@target_all_nodes
class NodeToolCleanupMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_nodetool_cleanup()


class TruncateMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_truncate()


class TruncateLargeParititionMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_truncate_large_partition()


class DeleteByPartitionsMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    free_tier_set = True
    delete_rows = True

    def disrupt(self):
        self.runner.disrupt_delete_10_full_partitions()


class DeleteByRowsRangeMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    free_tier_set = True
    delete_rows = True

    def disrupt(self):
        self.runner.disrupt_delete_by_rows_range()


class DeleteOverlappingRowRangesMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    free_tier_set = True
    delete_rows = True

    def disrupt(self):
        self.runner.disrupt_delete_overlapping_row_ranges()


class AddDropColumnMonkey(NemesisBaseClass):
    disruptive = False
    networking = False
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_add_drop_column()


class ToggleTableIcsMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    schema_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_table_ics()


class ToggleGcModeMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    disruptive = False
    schema_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_table_gc_mode()


@target_data_nodes
class MgmtBackup(NemesisBaseClass):
    manager_operation = True
    disruptive = False
    limited = True
    supports_high_disk_utilization = False  # Snapshot/Restore operations consume disk space

    def disrupt(self):
        self.runner.disrupt_mgmt_backup()


@target_data_nodes
class MgmtBackupSpecificKeyspaces(NemesisBaseClass):
    manager_operation = True
    disruptive = False
    limited = True
    supports_high_disk_utilization = False  # Snapshot/Restore operations consume disk space

    def disrupt(self):
        self.runner.disrupt_mgmt_backup_specific_keyspaces()


@target_data_nodes
class MgmtRestore(NemesisBaseClass):
    manager_operation = True
    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    supports_high_disk_utilization = False  # Snapshot/Restore operations consume disk space

    def disrupt(self):
        self.runner.disrupt_mgmt_restore()


@target_data_nodes
class MgmtRepair(NemesisBaseClass):
    manager_operation = True
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.log.info("disrupt_mgmt_repair_cli Nemesis begin")
        self.runner.disrupt_mgmt_repair_cli()
        self.runner.log.info("disrupt_mgmt_repair_cli Nemesis end")
        # For Manager APIs test, use: self.runner.disrupt_mgmt_repair_api()


@target_data_nodes
class MgmtCorruptThenRepair(NemesisBaseClass):
    manager_operation = True
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_mgmt_corrupt_then_repair()


class AbortRepairMonkey(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_abort_repair()


@target_all_nodes
class NodeTerminateAndReplace(NemesisBaseClass):
    disruptive = True
    # It should not be run on kubernetes, since it is a manual procedure
    # While on kubernetes we put it all on scylla-operator
    kubernetes = False
    topology_changes = True
    zero_node_changes = True

    def disrupt(self):
        self.runner.disrupt_terminate_and_replace_node()


class DrainKubernetesNodeThenReplaceScyllaNode(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_drain_kubernetes_node_then_replace_scylla_node()


class TerminateKubernetesHostThenReplaceScyllaNode(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_terminate_kubernetes_host_then_replace_scylla_node()


class DrainKubernetesNodeThenDecommissionAndAddScyllaNode(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_drain_kubernetes_node_then_decommission_and_add_scylla_node()


class TerminateKubernetesHostThenDecommissionAndAddScyllaNode(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_terminate_kubernetes_host_then_decommission_and_add_scylla_node()


class OperatorNodeReplace(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_replace_scylla_node_on_kubernetes()


class OperatorNodetoolFlushAndReshard(NemesisBaseClass):
    disruptive = True
    kubernetes = True

    def disrupt(self):
        self.runner.disrupt_nodetool_flush_and_reshard_on_kubernetes()


@target_all_nodes
class ScyllaKillMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_kill_scylla()


@target_data_nodes
class ValidateHintedHandoffShortDowntime(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_validate_hh_short_downtime()


@target_all_nodes
class SnapshotOperations(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_snapshot_operations()


class NodeRestartWithResharding(NemesisBaseClass):
    disruptive = True
    topology_changes = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_restart_with_resharding()


@target_all_nodes
class ClusterRollingRestart(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_rolling_restart_cluster(random_order=False)


@target_all_nodes
class RollingRestartConfigChangeInternodeCompression(NemesisBaseClass):
    disruptive = True
    full_cluster_restart = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_rolling_config_change_internode_compression()


@target_all_nodes
class ClusterRollingRestartRandomOrder(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_rolling_restart_cluster(random_order=True)


class SwitchBetweenPasswordAuthAndSaslauthdAuth(NemesisBaseClass):
    disruptive = True  # the nemesis has rolling restart
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back()


@target_all_nodes
class TopPartitions(NemesisBaseClass):
    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True

    def disrupt(self):
        self.runner.disrupt_show_toppartitions()


@target_all_nodes
class RandomInterruptionNetworkMonkey(NemesisBaseClass):
    disruptive = True
    networking = True
    kubernetes = True

    additional_configs = ["configurations/network_config/two_interfaces.yaml"]
    # TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
    #  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
    #  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
    additional_params = {"ip_ssh_connections": "public"}

    def disrupt(self):
        self.runner.disrupt_network_random_interruptions()


@target_all_nodes
class BlockNetworkMonkey(NemesisBaseClass):
    disruptive = True
    networking = True
    kubernetes = True

    additional_configs = ["configurations/network_config/two_interfaces.yaml"]
    # TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
    #  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
    #  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
    additional_params = {"ip_ssh_connections": "public"}

    def disrupt(self):
        self.runner.disrupt_network_block()


@target_all_nodes
class RejectInterNodeNetworkMonkey(NemesisBaseClass):
    disruptive = True
    networking = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_network_reject_inter_node_communication()


@target_all_nodes
class RejectNodeExporterNetworkMonkey(NemesisBaseClass):
    disruptive = True
    networking = True

    def disrupt(self):
        self.runner.disrupt_network_reject_node_exporter()


@target_all_nodes
class RejectThriftNetworkMonkey(NemesisBaseClass):
    disruptive = True
    networking = True

    def disrupt(self):
        self.runner.disrupt_network_reject_thrift()


@target_all_nodes
class StopStartInterfacesNetworkMonkey(NemesisBaseClass):
    disruptive = True
    networking = True

    additional_configs = ["configurations/network_config/two_interfaces.yaml"]
    # TODO: this definition should be removed when network configuration new mechanism will be supported by all backends.
    #  Now "ip_ssh_connections" is not supported for AWS and it is ignored.
    #  Test communication address (ip_ssh_connections) is defined as "public" for the relevant pipelines in "two_interfaces.yaml"
    additional_params = {"ip_ssh_connections": "public"}

    def disrupt(self):
        self.runner.disrupt_network_start_stop_interface()


class NemesisSequence(NemesisBaseClass):
    disruptive = True
    networking = False

    def disrupt(self):
        self.runner.disrupt_run_unique_sequence()


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
        self.runner.disrupt_remove_node_then_add_node()


class ToggleCDCMonkey(NemesisBaseClass):
    disruptive = False
    schema_changes = True
    config_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_cdc_feature_properties_on_table()


class CDCStressorMonkey(NemesisBaseClass):
    disruptive = False
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_run_cdcstressor_tool()


class DecommissionStreamingErrMonkey(NemesisBaseClass):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_decommission_streaming_err()


class RebuildStreamingErrMonkey(NemesisBaseClass):
    disruptive = True

    def disrupt(self):
        self.runner.disrupt_rebuild_streaming_err()


class RepairStreamingErrMonkey(NemesisBaseClass):
    disruptive = True

    def disrupt(self):
        self.runner.disrupt_repair_streaming_err()


@target_data_nodes
class CorruptThenScrubMonkey(NemesisBaseClass):
    disruptive = False
    supports_high_disk_utilization = False  # Failed for: https://github.com/scylladb/scylladb/issues/22088

    def disrupt(self):
        self.runner.disrupt_corrupt_then_scrub()


@target_all_nodes
class MemoryStressMonkey(NemesisBaseClass):
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
    disruptive = False

    def disrupt(self):
        self.runner.disrupt_start_stop_major_compaction()


class StartStopScrubCompaction(NemesisBaseClass):
    disruptive = False

    def disrupt(self):
        self.runner.disrupt_start_stop_scrub_compaction()


class StartStopCleanupCompaction(NemesisBaseClass):
    disruptive = False

    def disrupt(self):
        self.runner.disrupt_start_stop_cleanup_compaction()


class StartStopValidationCompaction(NemesisBaseClass):
    disruptive = False
    supports_high_disk_utilization = False  # Failed for: https://github.com/scylladb/scylladb/issues/22088

    def disrupt(self):
        self.runner.disrupt_start_stop_validation_compaction()


class SlaIncreaseSharesDuringLoad(NemesisBaseClass):
    disruptive = False
    sla = True

    additional_configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    def disrupt(self):
        self.runner.disrupt_sla_increase_shares_during_load()


class SlaDecreaseSharesDuringLoad(NemesisBaseClass):
    disruptive = False
    sla = True

    additional_configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    def disrupt(self):
        self.runner.disrupt_sla_decrease_shares_during_load()


class SlaReplaceUsingDetachDuringLoad(NemesisBaseClass):
    # TODO: This SLA nemesis uses binary disable/enable workaround that in a test with parallel nemeses can cause to the errors and
    #  failures that is not a problem of Scylla. The option "disruptive" was set to True to prevent irrelevant failures. Should be changed
    #  to False when the issue https://github.com/scylladb/scylla-enterprise/issues/2572 will be fixed.
    disruptive = True
    sla = True

    additional_configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    def disrupt(self):
        self.runner.disrupt_replace_service_level_using_detach_during_load()


class SlaReplaceUsingDropDuringLoad(NemesisBaseClass):
    # TODO: This SLA nemesis uses binary disable/enable workaround that in a test with parallel nemeses can cause to the errors and
    #  failures that is not a problem of Scylla. The option "disruptive" was set to True to prevent irrelevant failures. Should be changed
    #  to False when the issue https://github.com/scylladb/scylla-enterprise/issues/2572 will be fixed.
    disruptive = True
    sla = True

    additional_configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    def disrupt(self):
        self.runner.disrupt_replace_service_level_using_drop_during_load()


class SlaIncreaseSharesByAttachAnotherSlDuringLoad(NemesisBaseClass):
    # TODO: This SLA nemesis uses binary disable/enable workaround that in a test with parallel nemeses can cause to the errors and
    #  failures that is not a problem of Scylla. The option "disruptive" was set to True to prevent irrelevant failures. Should be changed
    #  to False when the issue https://github.com/scylladb/scylla-enterprise/issues/2572 will be fixed.
    disruptive = True
    sla = True

    additional_configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    def disrupt(self):
        self.runner.disrupt_increase_shares_by_attach_another_sl_during_load()


class SlaMaximumAllowedSlsWithMaxSharesDuringLoad(NemesisBaseClass):
    disruptive = False
    sla = True

    additional_configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    def disrupt(self):
        self.runner.disrupt_maximum_allowed_sls_with_max_shares_during_load()


@target_data_nodes
class CreateIndexNemesis(NemesisBaseClass):
    disruptive = False
    schema_changes = True
    free_tier_set = True
    supports_high_disk_utilization = False  # Creating an Index consumes disk space

    def disrupt(self):
        self.runner.disrupt_create_index()


@target_data_nodes
class AddRemoveMvNemesis(NemesisBaseClass):
    disruptive = True
    schema_changes = True
    free_tier_set = True
    supports_high_disk_utilization = False  # Creating an MV consumes disk space

    def disrupt(self):
        self.runner.disrupt_add_remove_mv()


class ToggleAuditNemesisSyslog(NemesisBaseClass):
    disruptive = True
    schema_changes = True
    config_changes = True
    free_tier_set = True

    def disrupt(self):
        self.runner.disrupt_toggle_audit_syslog()


@target_data_nodes
class BootstrapStreamingErrorNemesis(NemesisBaseClass):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_bootstrap_streaming_error()


class DisableBinaryGossipExecuteMajorCompaction(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    xcloud = True

    def disrupt(self):
        self.runner.disrupt_disable_binary_gossip_execute_major_compaction()


@target_all_nodes
class EndOfQuotaNemesis(NemesisBaseClass):
    disruptive = True
    config_changes = True

    def disrupt(self):
        self.runner.disrupt_end_of_quota_nemesis()


@target_all_nodes
class GrowShrinkZeroTokenNode(NemesisBaseClass):
    disruptive = True
    zero_node_changes = True

    def disrupt(self):
        self.runner.disrupt_grow_shrink_zero_nodes()


@target_all_nodes
class SerialRestartOfElectedTopologyCoordinatorNemesis(NemesisBaseClass):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_serial_restart_elected_topology_coordinator()


@target_all_nodes
class IsolateNodeWithProcessSignalNemesis(NemesisBaseClass):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_refuse_connection_with_send_sigstop_signal_to_scylla_on_banned_node()


@target_all_nodes
class IsolateNodeWithIptableRuleNemesis(NemesisBaseClass):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_refuse_connection_with_block_scylla_ports_on_banned_node()


@target_all_nodes
class KillMVBuildingCoordinator(NemesisBaseClass):
    disruptive = True
    topology_changes = True

    def disrupt(self):
        self.runner.disrupt_kill_mv_building_coordinator()


class ModifyTableTwcsWindowSizeMonkey(NemesisBaseClass):
    disruptive = True
    kubernetes = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_twcs_window_size()


class ModifyTableCommentMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_comment()


class ModifyTableGcGraceTimeMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_gc_grace_time()


class ModifyTableCachingMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_caching()


class ModifyTableBloomFilterFpChanceMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_bloom_filter_fp_chance()


class ModifyTableCompactionMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_compaction()


class ModifyTableCompressionMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_compression()


class ModifyTableCrcCheckChanceMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_crc_check_chance()


class ModifyTableDclocalReadRepairChanceMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_dclocal_read_repair_chance()


class ModifyTableDefaultTimeToLiveMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_default_time_to_live()


class ModifyTableMaxIndexIntervalMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_max_index_interval()


class ModifyTableMinIndexIntervalMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_min_index_interval()


class ModifyTableMemtableFlushPeriodInMsMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_memtable_flush_period_in_ms()


class ModifyTableReadRepairChanceMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_read_repair_chance()


class ModifyTableSpeculativeRetryMonkey(NemesisBaseClass):
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.runner.disrupt_modify_table_speculative_retry()
