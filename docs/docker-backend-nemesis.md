# Individual Nemesis cases status on Docker backend
| Nemesis | Status | Comment
| :------ | :----- | :------
| AbortRepairMonkey | Pass |
| AddDropColumnMonkey | Pass |
| AddRemoveDcNemesis  | Fail | [scylla-cluster-tests/issues/7351](https://github.com/scylladb/scylla-cluster-tests/issues/7351)
| AddRemoveMvNemesis | Pass |
| AddRemoveRackNemesis | Pass |
| BlockNetworkMonkey | Pass |
| BootstrapStreamingErrorNemesis | Pass |
| CDCStressorMonkey | Pass |
| ClusterRollingRestartRandomOrder | Pass |
| ClusterRollingRestart | Pass |
| CorruptThenRebuildMonkey | Pass |
| CorruptThenRepairMonkey | Pass |
| CorruptThenScrubMonkey | Pass |
| CreateIndexNemesis | Pass |
| DecommissionMonkey | Pass |
| DecommissionSeedNode | Fail | [scylla-cluster-tests/issues/7328](https://github.com/scylladb/scylla-cluster-tests/issues/7328)
| DecommissionStreamingErrMonkey | Pass |
| DeleteByPartitionsMonkey | Pass |
| DeleteByRowsRangeMonkey | Pass |
| DeleteOverlappingRowRangesMonkey | Pass |
| DisableBinaryGossipExecuteMajorCompaction | Pass |
| DrainerMonkey | Pass |
| DrainKubernetesNodeThenDecommissionAndAddScyllaNode | Skipped | Not a k8s backend
| DrainKubernetesNodeThenReplaceScyllaNode | Skipped | Not a k8s backend
| EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey | Skipped | Not an AWS backend
| EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey    | Skipped | Not an AWS backend
| EndOfQuotaNemesis | Skipped | Should be skipped for docker backend
| EnospcAllNodesMonkey | Pass |
| EnospcMonkey | Pass |
| GrowShrinkClusterNemesis | Pass |
| HardRebootNodeMonkey | Skipped | Backend doesn't support hard reboot
| LoadAndStreamMonkey | Pass |
| MajorCompactionMonkey | Pass |
| MemoryStressMonkey | Pass |
| MgmtBackup | Skipped | Currently the backend can't run manager server
| MgmtBackupSpecificKeyspaces | Skipped | Currently the backend can't run manager server
| MgmtCorruptThenRepair | Skipped | Currently the backend can't run manager server
| MgmtRepair | Skipped | Currently the backend can't run manager server
| MgmtRestore | Skipped | Currently the backend can't run manager server
| ModifyTableMonkey | Pass |
| MultipleHardRebootNodeMonkey | Skipped | Backend doesn't support hard reboot
| NemesisSequence | Pass |
| NoCorruptRepairMonkey | Pass |
| NodeRestartWithResharding | Fail | [scylla-cluster-tests/issues/7330](https://github.com/scylladb/scylla-cluster-tests/issues/7330)
| NodeTerminateAndReplace | Pass |
| NodeToolCleanupMonkey | Pass |
| OperatorNodeReplace | Skipped | Should be skipped for docker backend
| OperatorNodetoolFlushAndReshard | Pass |
| PauseLdapNemesis | Pass | Should be executed on enterprise
| RandomInterruptionNetworkMonkey | Pass |
| RebuildStreamingErrMonkey | Skipped | Backend doesn't support hard reboot
| RefreshBigMonkey | Pass |
| RefreshMonkey | Pass |
| RejectNodeExporterNetworkMonkey | Fail | [scylla-cluster-tests/issues/7329](https://github.com/scylladb/scylla-cluster-tests/issues/7329)
| RejectThriftNetworkMonkey | Fail | [scylla-cluster-tests/issues/7329](https://github.com/scylladb/scylla-cluster-tests/issues/7329)
| RemoveServiceLevelMonkey | Pass |
| RepairStreamingErrMonkey | Skipped | Backend doesn't support hard reboot
| ResetLocalSchemaMonkey | Pass |
| RollingRestartConfigChangeInternodeCompression | Pass |
| ScyllaKillMonkey | Pass |
| SlaDecreaseSharesDuringLoad | Pass |
| SlaIncreaseSharesByAttachAnotherSlDuringLoad | Pass |
| SlaIncreaseSharesDuringLoad | Pass |
| SlaMaximumAllowedSlsWithMaxSharesDuringLoad | Pass |
| SlaReplaceUsingDetachDuringLoad | Pass |
| SlaReplaceUsingDropDuringLoad | Pass |
| SnapshotOperations | Pass |
| SoftRebootNodeMonkey | Fail | [scylla-cluster-tests/issues/7330](https://github.com/scylladb/scylla-cluster-tests/issues/7330)
| SslHotReloadingNemesis | Pass |
| StartStopCleanupCompaction | Pass |
| StartStopMajorCompaction | Pass |
| StartStopScrubCompaction | Pass |
| StartStopValidationCompaction | Pass |
| StopStartInterfacesNetworkMonkey | Pass |
| StopStartMonkey | Pass |
| StopWaitStartMonkey | Pass |
| SwitchBetweenPasswordAuthAndSaslauthdAuth | Pass |
| TerminateAndRemoveNodeMonkey | Fail | [scylla-cluster-tests/issues/7331](https://github.com/scylladb/scylla-cluster-tests/issues/7331)
| TerminateKubernetesHostThenDecommissionAndAddScyllaNode | Skipped | Not a k8s backend
| TerminateKubernetesHostThenReplaceScyllaNode | Skipped | Not a k8s backend
| ToggleAuditNemesisSyslog | Pass | Should be executed on enterprise
| ToggleCDCMonkey | Pass |
| ToggleGcModeMonkey | Pass |
| ToggleLdapConfiguration | Pass | Should be executed on enterprise
| ToggleTableIcsMonkey | Pass |
| TopPartitions | Pass |
| TruncateLargeParititionMonkey | Pass |
| TruncateMonkey | Pass |
