# Nemesis Extraction Plan — Phase 3: Split Existing Nemesis Into Modules

## Problem Statement

[sdcm/nemesis/__init__.py](sdcm/nemesis/__init__.py) is 6,067 lines long and [sdcm/nemesis/monkey/__init__.py](sdcm/nemesis/monkey/__init__.py) is 978 lines. Both files contain dozens of unrelated nemesis operations mixed together, making the codebase hard to navigate, review, and maintain.

Commit `ab662e3` demonstrated the extraction pattern by moving `ModifyTable*` nemesis into [sdcm/nemesis/monkey/modify_table.py](sdcm/nemesis/monkey/modify_table.py) (566 lines), removing ~569 lines from the two `__init__.py` files. This plan continues that work by identifying the remaining groups of nemesis that should be extracted, ordered by impact (lines removed from `__init__.py`).

## Current State

### File Sizes (post `ab662e3`)

| File | Lines |
|------|-------|
| [sdcm/nemesis/__init__.py](sdcm/nemesis/__init__.py) | 6,067 |
| [sdcm/nemesis/monkey/__init__.py](sdcm/nemesis/monkey/__init__.py) | 978 |
| [sdcm/nemesis/monkey/modify_table.py](sdcm/nemesis/monkey/modify_table.py) | 566 (already extracted) |
| [sdcm/nemesis/monkey/runners.py](sdcm/nemesis/monkey/runners.py) | 230 (already extracted) |

### Architecture

- **`NemesisRunner`** (in `__init__.py`): The God-class that holds all `disrupt_*` methods and their private helpers.
- **`NemesisBaseClass`** (in `__init__.py`): Abstract base class for individual nemesis monkey classes.
- **`monkey/__init__.py`**: Contains ~95 thin Monkey classes that delegate to `NemesisRunner.disrupt_*` methods.
- **`monkey/modify_table.py`**: First extracted module — self-contained `ModifyTable*` monkey classes with shared `modify_table_property()` helper.

### Extraction Pattern (from `ab662e3`)

1. Create `sdcm/nemesis/monkey/<group_name>.py`
2. Move the `disrupt_*` methods from `NemesisRunner` into the new monkey class(es) as `disrupt()` methods
3. Move associated private helper methods and instance variables that are **only used by that group**
4. Move corresponding Monkey classes from `monkey/__init__.py`
5. Shared helpers that are used by multiple groups move to the **`topology/` package** (see Phase 8), which acts as the central package for node lifecycle operations. Other modules that need these helpers inherit from `TopologyBaseMonkey` or import from `topology/node_ops.py`.
6. When a group of related nemesis is large enough, extract it as a **sub-package** (directory with `__init__.py`) rather than a flat module. Sub-packages can contain a base class with shared helpers, plus submodules for specific nemesis categories.

## Goals

1. **Reduce `nemesis/__init__.py` from ~6,067 lines to ~800 lines** by extracting ~5,200 lines of `disrupt_*` methods, their helpers, AND shared node-operation helpers
2. **Reduce `monkey/__init__.py` from ~978 lines to ~300 lines** by co-locating monkey classes with their implementation
3. **Group related nemesis into cohesive, self-contained modules** for easier maintenance
4. **Create a `topology/` package** that owns all node lifecycle helpers and contains sub-groups (streaming, grow/shrink) that build on them
5. **Preserve backward compatibility** — no changes to nemesis names, selectors, or behavior

## Implementation Phases

Phases are ordered by **lines removed from `__init__.py`** (descending), so each PR delivers maximum impact. Line counts include private helper methods that are exclusive to the group.

---

### Phase 1: `network.py` — Network Disruption Nemesis (~750 lines)

**Target file**: `sdcm/nemesis/monkey/network.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_network_random_interruptions` | 92 | L3476-L3567 |
| `disrupt_network_block` | 43 | L3568-L3610 |
| `disrupt_network_reject_inter_node_communication` | 39 | L3752-L3790 |
| `disrupt_network_reject_node_exporter` | 25 | L3791-L3815 |
| `disrupt_network_reject_thrift` | 138 | L3816-L3953 |
| `disrupt_network_start_stop_interface` | 216 | L3954-L4169 |
| `get_rate_limit_for_network_disruption` | helper | L3400 |
| `_install_iptables` | helper | L3841 |
| `_iptables_randomly_get_random_matching_rule` | helper | L3846 |
| `_iptables_randomly_get_disrupting_target` | helper | L3882 |
| `_run_commands_wait_and_cleanup` | helper | L3902 |
| `_disrupt_network_block_k8s` | helper | L3558 |
| `_disrupt_network_random_interruptions_k8s` (if exists) | helper | — |

**Monkey classes to move from `monkey/__init__.py`**:
- `RandomInterruptionNetworkMonkey` (L614)
- `BlockNetworkMonkey` (L630)
- `RejectInterNodeNetworkMonkey` (L646)
- `RejectNodeExporterNetworkMonkey` (L656)
- `RejectThriftNetworkMonkey` (L665)
- `StopStartInterfacesNetworkMonkey` (L674)

**Estimated removal**: ~750 lines from `__init__.py`, ~70 lines from `monkey/__init__.py`

**Definition of Done**:
- All network nemesis live in `monkey/network.py`
- Shared iptables helpers are either in the new module or extracted to a utils
- All unit tests pass; nemesis selector resolution unchanged

---

### Phase 2: `sla.py` — SLA / Service Level Nemesis (~450 lines)

**Target file**: `sdcm/nemesis/monkey/sla.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_remove_service_level_while_load` | 226 | L1890-L2115 |
| `disrupt_sla_increase_shares_during_load` | 28 | L5012-L5039 |
| `disrupt_sla_decrease_shares_during_load` | 28 | L5040-L5067 |
| `disrupt_replace_service_level_using_detach_during_load` | 28 | L5068-L5095 |
| `disrupt_replace_service_level_using_drop_during_load` | 29 | L5096-L5124 |
| `disrupt_increase_shares_by_attach_another_sl_during_load` | 29 | L5125-L5153 |
| `disrupt_maximum_allowed_sls_with_max_shares_during_load` | 43 | L5154-L5196 |
| `format_error_for_sla_test_and_raise` | helper | L5191 |
| `get_cassandra_stress_write_cmds` | helper | L4986 |
| `get_cassandra_stress_definition` | helper | L5000 |

**Monkey classes to move from `monkey/__init__.py`**:
- `RemoveServiceLevelMonkey` (L256)
- `SlaIncreaseSharesDuringLoad` (L808)
- `SlaDecreaseSharesDuringLoad` (L818)
- `SlaReplaceUsingDetachDuringLoad` (L828)
- `SlaReplaceUsingDropDuringLoad` (L841)
- `SlaIncreaseSharesByAttachAnotherSlDuringLoad` (L854)
- `SlaMaximumAllowedSlsWithMaxSharesDuringLoad` (L867)

**Notes**: All 6 `disrupt_sla_*` methods share an identical validation preamble (~15 lines each) that can be refactored into a shared `_validate_sla_preconditions()` helper in the new module.

**Estimated removal**: ~450 lines from `__init__.py`, ~80 lines from `monkey/__init__.py`

**Definition of Done**:
- All SLA nemesis live in `monkey/sla.py`
- Shared SLA validation boilerplate consolidated into one helper
- All unit tests pass

---

### Phase 3: `data_operations.py` — Delete / Truncate / Data Manipulation Nemesis (~850 lines)

**Target file**: `sdcm/nemesis/monkey/data_operations.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_truncate` | 19 | L2160-L2178 |
| `disrupt_truncate_large_partition` | 438 | L2179-L2616 |
| `disrupt_delete_10_full_partitions` | 19 | L2617-L2635 |
| `disrupt_delete_overlapping_row_ranges` | 27 | L2636-L2662 |
| `disrupt_delete_by_rows_range` | 15 | L2663-L2677 |
| `disrupt_add_drop_column` | 106 | L2678-L2783 |
| `verify_initial_inputs_for_delete_nemesis` | helper | L2371 |
| `choose_partitions_for_delete` | helper | L2382 |
| `get_random_timestamp_from_partition` | helper | L2446 |
| `run_deletions` | helper | L2492 |
| `_verify_using_timestamp_deletions` | helper | L2500 |
| `delete_half_partition` | helper | L2525 |
| `delete_by_range_using_timestamp` | helper | L2544 |
| `delete_range_in_few_partitions` | helper | L2578 |
| `_get_all_tables_with_no_compact_storage` | helper | L2213 |
| `_add_drop_column_*` (6 methods) | helpers | L2252-L2365 |
| `_add_drop_column_run_in_cycle` | helper | L2365 |
| `_prepare_test_table` | helper | L2119 |
| `_truncate_cmd_timeout_suffix` (2 versions) | helpers | L2151-L2156 |

**Monkey classes to move from `monkey/__init__.py`**:
- `TruncateMonkey` (L298)
- `TruncateLargeParititionMonkey` (L309)
- `DeleteTenFullPartitionsMonkey` (L319 — if exists)
- `DeleteOverlappingRowRangesMonkey` (if exists)
- `DeleteByRowsRangeMonkey` (if exists)
- `AddDropColumnMonkey` (if exists)

**Estimated removal**: ~850 lines from `__init__.py`, ~40 lines from `monkey/__init__.py`

**Definition of Done**:
- All data manipulation nemesis live in `monkey/data_operations.py`
- Delete helpers are co-located with delete nemesis
- All unit tests pass

---

### Phase 4: `manager.py` — Scylla Manager Nemesis (~450 lines)

**Target file**: `sdcm/nemesis/monkey/manager.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_manager_backup` | 18 | L2815-L2832 |
| `disrupt_mgmt_backup_specific_keyspaces` | 3 | L2833-L2835 |
| `disrupt_mgmt_backup` | 3 | L2836-L2838 |
| `disrupt_mgmt_restore` | 237 | L2839-L3075 |
| `disrupt_mgmt_repair_cli` | 5 | L3076-L3080 |
| `disrupt_mgmt_corrupt_then_repair` | 6 | L3081-L3086 |
| `get_manager_tool` | helper | L2830 |
| `_run_manager_backup` | helper | L2790 |
| `_manager_backup_and_report` | helper | L2797 |
| `_delete_existing_backups` | helper | L3011 |
| `_mgmt_backup` | helper | L3026 |

**Monkey classes to move from `monkey/__init__.py`**:
- `MgmtBackupMonkey` (if exists)
- `MgmtBackupSpecificKeyspacesMonkey` (if exists)
- `MgmtRepairCliMonkey` (if exists)
- `MgmtCorruptThenRepairMonkey` (if exists)
- `MgmtRestoreMonkey` (if exists)

**Estimated removal**: ~300 lines from `__init__.py`, ~30 lines from `monkey/__init__.py`

**Note**: `disrupt_mgmt_corrupt_then_repair` calls `_destroy_data_and_restart_scylla()`. After Phase 8 (topology package), this helper lives on `TopologyBaseMonkey`. The manager monkey class for this nemesis should **inherit from `TopologyBaseMonkey`** to access it, or import the standalone function from `topology/node_ops.py`.

**Definition of Done**:
- All manager nemesis live in `monkey/manager.py`
- `MgmtCorruptThenRepairMonkey` inherits from `TopologyBaseMonkey` (or imports from `topology/node_ops.py`)
- All unit tests pass

---

### Phase 5: `kubernetes.py` — Kubernetes-Specific Nemesis (~350 lines)

**Target file**: `sdcm/nemesis/monkey/kubernetes.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_nodetool_flush_and_reshard_on_kubernetes` | 37 | L1451-L1487 |
| `disrupt_drain_kubernetes_node_then_replace_scylla_node` | 3 | L1488-L1490 |
| `disrupt_terminate_kubernetes_host_then_replace_scylla_node` | 54 | L1491-L1544 |
| `disrupt_drain_kubernetes_node_then_decommission_and_add_scylla_node` | 3 | L1545-L1547 |
| `disrupt_terminate_kubernetes_host_then_decommission_and_add_scylla_node` | 59 | L1548-L1606 |
| `disrupt_replace_scylla_node_on_kubernetes` | 15 | L1607-L1621 |
| `_disrupt_kubernetes_then_replace_scylla_node` | helper | L1498 |
| `_disrupt_kubernetes_then_decommission_and_add_scylla_node` | helper | L1555 |
| `_get_neighbour_scylla_pods` | helper | L1596 |
| `_kubernetes_wait_till_node_up_after_been_recreated` | helper | L1617 |
| `_verify_resharding_on_k8s` | helper | L1380 |

**Monkey classes to move from `monkey/__init__.py`**:
- `FlushAndReshardOnK8sMonkey` (if exists)
- `DrainK8sNodeThenReplace` (if exists)
- `TerminateK8sHostThenReplace` (if exists)
- `DrainK8sNodeThenDecommissionAndAdd` (if exists)
- `TerminateK8sHostThenDecommissionAndAdd` (if exists)
- `ReplaceScyllaNodeOnK8s` (if exists)

**Estimated removal**: ~350 lines from `__init__.py`, ~30 lines from `monkey/__init__.py`

**Note**: K8s decommission-and-add nemesis call `_add_and_init_new_cluster_nodes()` and `add_new_nodes()`. After Phase 8 (topology package), these helpers live on `TopologyBaseMonkey`. K8s monkey classes that perform node add/decommission should **inherit from `TopologyBaseMonkey`** to access them.

**Definition of Done**:
- All K8s-specific nemesis live in `monkey/kubernetes.py`
- K8s monkey classes that use node lifecycle helpers inherit from `TopologyBaseMonkey`
- All unit tests pass

---

### Phase 6: `cdc.py` — CDC Nemesis (~200 lines)

**Target file**: `sdcm/nemesis/monkey/cdc.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_toggle_cdc_feature_properties_on_table` | 34 | L4728-L4761 |
| `disrupt_run_cdcstressor_tool` | 145 | L4762-L4906 |
| `_run_cdc_stressor_tool` | helper | L4771 |
| `_alter_table_with_cdc_properties` | helper | L4784 |
| `_verify_cdc_feature_status` | helper | L4809 |

**Monkey classes to move from `monkey/__init__.py`**:
- `ToggleCDCMonkey` (L711)
- `CDCStressorMonkey` (L721)

**Estimated removal**: ~200 lines from `__init__.py`, ~20 lines from `monkey/__init__.py`

**Definition of Done**:
- All CDC nemesis live in `monkey/cdc.py`
- All unit tests pass

---

### Phase 7: `encryption.py` — Table Encryption Nemesis (~200 lines)

**Target file**: `sdcm/nemesis/monkey/encryption.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_enable_disable_table_encryption_aws_kms_provider_without_rotation` | 6 | L4395-L4400 |
| `disrupt_enable_disable_table_encryption_aws_kms_provider_with_rotation` | 164 | L4401-L4564 |
| `_enable_disable_table_encryption` | helper (bulk of logic) | L4408 |

**Monkey classes to move from `monkey/__init__.py`**:
- `EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey` (L93)
- `EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey` (L102)

**Estimated removal**: ~170 lines from `__init__.py`, ~20 lines from `monkey/__init__.py`

**Definition of Done**:
- All encryption nemesis live in `monkey/encryption.py`
- All unit tests pass

---

### Phase 8: `topology/` — Topology & Node Operations Package (~900 lines)

**Target directory**: `sdcm/nemesis/monkey/topology/`

This phase creates a **package** rather than a single module. Streaming error nemesis are grouped under topology because they fundamentally test streaming behavior during topology changes (decommission, rebuild, repair). Shared node lifecycle helpers that were previously scattered across `NemesisRunner` are consolidated into `TopologyBaseMonkey`, which serves as the base class for all topology-related monkey classes.

**Package structure**:
```
monkey/topology/
    __init__.py          — TopologyBaseMonkey base class with shared node lifecycle helpers (~250 lines)
    grow_shrink.py       — Grow/shrink cluster, add/remove DC/rack nemesis (~350 lines)
    streaming.py         — Streaming error nemesis (~300 lines)
```

#### `topology/__init__.py` — `TopologyBaseMonkey` Base Class

Contains `TopologyBaseMonkey(NemesisBaseClass, abc.ABC)` — a base class (following the `ModifyTableBaseMonkey` pattern from `modify_table.py`) that provides shared node lifecycle helpers. All monkey classes in `grow_shrink.py` and `streaming.py` inherit from this class. Other extracted modules (k8s, manager) that need these helpers also inherit from it.

**Shared helpers to extract from `NemesisRunner` into `TopologyBaseMonkey`**:

| Method | Lines | Location | Currently called by |
|--------|-------|----------|---------------------|
| `_add_and_init_new_cluster_nodes` | ~50 | L1319+ | streaming, grow_shrink, k8s, decommission, remove+add |
| `add_new_nodes` | ~5 | L4232 | grow_shrink, k8s |
| `decommission_nodes` | ~5 | L4239 | grow_shrink |
| `_decommission_nodes` | ~30 | L4251 | grow_shrink (via `_shrink_cluster`) |
| `_nodetool_decommission` | ~30 | L1319 | decommission nemesis (currently in `__init__.py`) |
| `_terminate_cluster_node` | ~20 | L1370+ | decommission, remove+add |
| `_replace_cluster_node` | ~30 | L1375+ | replace nemesis |
| `reboot_node` | ~20 | L807+ | streaming, standalone reboot nemesis |
| `rebuild_or_repair` | ~10 | L4090+ | streaming |
| `_destroy_data_and_restart_scylla` | ~10 | L1107+ | streaming, manager, standalone |
| `run_repair` | ~15 | L785+ | streaming, compaction, manager, standalone, many others |

**Total**: ~225 lines of shared helpers move from `NemesisRunner` to `TopologyBaseMonkey`.

**Design rationale**: These helpers form a cohesive set of "node lifecycle operations" — adding, removing, rebooting, repairing, and replacing nodes. Even though some (like `run_repair`) are used beyond topology contexts, they are fundamentally node-level operations. Consolidating them in one base class avoids scattering them across unrelated modules and gives every monkey class a clear import path.

**Access from other modules**: Monkey classes in other extracted modules (k8s, manager, etc.) that need node lifecycle helpers inherit from `TopologyBaseMonkey`:
```python
# Example: monkey/kubernetes.py
from sdcm.nemesis.monkey.topology import TopologyBaseMonkey

class TerminateK8sHostThenDecommissionAndAddMonkey(TopologyBaseMonkey):
    def disrupt(self):
        ...  # can call self._add_and_init_new_cluster_nodes(), self.reboot_node(), etc.
```

**Impact on NemesisRunner residual**: After this extraction, several `disrupt_*` methods remaining in `__init__.py` (e.g., `disrupt_hard_reboot_node`, `disrupt_destroy_data_then_repair`, `disrupt_restart_then_repair_node`, `disrupt_nodetool_decommission`) currently call `self.reboot_node()`, `self.run_repair()`, etc. These must be updated to either:
1. **Also be extracted** into the topology package or their own modules (preferred — maximizes `__init__.py` reduction)
2. **Import from topology**: `from sdcm.nemesis.monkey.topology import TopologyBaseMonkey` and call the helpers as standalone functions or via a helper instance

**Recommended approach for residual methods**: Extract `disrupt_nodetool_decommission` and `disrupt_nodetool_seed_decommission` into `topology/decommission.py` (they ARE topology operations). For truly standalone nemesis like `disrupt_hard_reboot_node` and `disrupt_destroy_data_then_repair`, extract them into `topology/node_ops.py` — they are node operations that naturally belong in the topology package.

---

#### `topology/grow_shrink.py` — Grow/Shrink Cluster Nemesis

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Type | Location |
|--------|------|----------|
| `disrupt_grow_shrink_cluster` | disrupt | L4302-L4319 |
| `disrupt_grow_shrink_new_rack` | disrupt | L4320-L4394 |
| `disrupt_grow_shrink_zero_nodes` | disrupt | L5537-L5548 |
| `disrupt_add_remove_dc` | disrupt | L4907-L5011 |
| `disrupt_remove_node_then_add_node` | disrupt | L3611-L3751 |
| `_grow_cluster` | helper | L4327 |
| `_shrink_cluster` | helper | L4351 |
| `_double_cluster_load` | helper | L4291 |
| `_add_new_node_in_new_dc` | helper | L4825 |
| `_write_read_data_to_multi_dc_keyspace` | helper | L4856 |
| `_verify_multi_dc_keyspace_data` | helper | L4873 |
| `_switch_to_network_replication_strategy` | helper | L4882 |
| `_remove_node_add_node` | helper | L3636 |

**Monkey classes to move from `monkey/__init__.py`**:
- `GrowShrinkClusterNemesis` (L51)
- `AddRemoveRackNemesis` (L60)
- `AddRemoveDcNemesis` (L41)
- `GrowShrinkZeroTokenNode` (L937)
- `RemoveNodeThenAddNodeMonkey` (if exists)

All monkey classes in this file inherit from `TopologyBaseMonkey` and use the shared helpers via `self._add_and_init_new_cluster_nodes()`, `self.decommission_nodes()`, etc.

---

#### `topology/streaming.py` — Streaming Error Nemesis

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Type | Location |
|--------|------|----------|
| `disrupt_decommission_streaming_err` | disrupt | L4170-L4177 |
| `disrupt_rebuild_streaming_err` | disrupt | L4178-L4190 |
| `disrupt_repair_streaming_err` | disrupt | L4191-L4214 |
| `disrupt_bootstrap_streaming_error` | disrupt | L5442-L5507 |
| `start_and_interrupt_decommission_streaming` | helper | L4013 |
| `start_and_interrupt_repair_streaming` | helper | L4098 |
| `start_and_interrupt_rebuild_streaming` | helper | L4132 |
| `_call_disrupt_func_after_expression_logged` | helper (streaming-exclusive) | L3973 |

**Monkey classes to move from `monkey/__init__.py`**:
- `DecommissionStreamingErrMonkey` (L729)
- `RebuildStreamingErrMonkey` (L737)
- `RepairStreamingErrMonkey` (L744)
- `BootstrapStreamingErrorNemesis` (L910)

All monkey classes in this file inherit from `TopologyBaseMonkey` and use shared helpers via `self._add_and_init_new_cluster_nodes()`, `self.reboot_node()`, `self._destroy_data_and_restart_scylla()`, `self.rebuild_or_repair()`, `self.run_repair()`. The streaming-exclusive helper `_call_disrupt_func_after_expression_logged` lives in this file (not on `TopologyBaseMonkey`).

---

#### Phase 8 Summary

**Estimated removal**: ~900 lines from `__init__.py` (~225 shared helpers + ~350 grow/shrink + ~300 streaming + ~25 disrupt_remove_node_then_add_node overhead), ~70 lines from `monkey/__init__.py`

**Definition of Done**:
- `monkey/topology/` package exists with `__init__.py`, `grow_shrink.py`, `streaming.py`
- `TopologyBaseMonkey` base class contains all shared node lifecycle helpers
- All grow/shrink and streaming nemesis monkey classes inherit from `TopologyBaseMonkey`
- `_call_disrupt_func_after_expression_logged` lives in `streaming.py` (exclusive to streaming)
- `NemesisRunner` no longer contains ANY of the extracted helpers or disrupt methods
- K8s (Phase 5) and manager (Phase 4) monkey classes updated to inherit from `TopologyBaseMonkey`
- All unit tests pass; nemesis selector resolution unchanged

---

### Phase 9: `views_indexes.py` — Materialized View / Index Nemesis (~470 lines)

**Target file**: `sdcm/nemesis/monkey/views_indexes.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_create_index` | 59 | L5197-L5255 |
| `disrupt_add_remove_mv` | 61 | L5256-L5316 |
| `disrupt_kill_mv_building_coordinator` | 351 | L5717-L6067 |

**Monkey classes to move from `monkey/__init__.py`**:
- `CreateIndexNemesis` (L878)
- `AddRemoveMvNemesis` (L889)
- `KillMVBuildingCoordinator` (L973)

**Estimated removal**: ~470 lines from `__init__.py`, ~30 lines from `monkey/__init__.py`

**Definition of Done**:
- All MV/Index nemesis live in `monkey/views_indexes.py`
- All unit tests pass

---

### Phase 10: `compaction.py` — Compaction Nemesis (~250 lines)

**Target file**: `sdcm/nemesis/monkey/compaction.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_start_stop_major_compaction` | 57 | L576-L632 |
| `disrupt_start_stop_scrub_compaction` | 44 | L633-L676 |
| `disrupt_start_stop_cleanup_compaction` | 45 | L677-L721 |
| `disrupt_start_stop_validation_compaction` | 46 | L722-L767 |
| `disrupt_major_compaction` | 3 | L1709-L1711 |
| `disrupt_corrupt_then_scrub` | 87 | L4215-L4301 |
| `_handle_start_stop_compaction_results` | helper | L533 |
| `_prepare_start_stop_compaction` | helper | L562 |
| `_get_random_non_system_ks_cf` | helper | L547 |
| `clear_snapshots` | helper | L628 |
| `_major_compaction` | helper | L1702 |
| `_corrupt_data_file` | helper | L4202 |

**Monkey classes to move from `monkey/__init__.py`**:
- `StartStopMajorCompaction` (L779)
- `StartStopScrubCompaction` (L786)
- `StartStopCleanupCompaction` (L793)
- `StartStopValidationCompaction` (L800)
- `MajorCompactionMonkey` (L217)
- `CorruptThenScrubMonkey` (L752)

**Estimated removal**: ~250 lines from `__init__.py`, ~40 lines from `monkey/__init__.py`

**Definition of Done**:
- All compaction nemesis live in `monkey/compaction.py`
- All unit tests pass

---

### Phase 11: `audit.py` — Audit Log Nemesis (~130 lines)

**Target file**: `sdcm/nemesis/monkey/audit.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_toggle_audit_syslog` | 125 | L5317-L5441 |
| `_disrupt_toggle_audit` | helper (bulk) | L5321 |

**Monkey classes to move from `monkey/__init__.py`**:
- `ToggleAuditNemesisSyslog` (L899)

**Estimated removal**: ~130 lines from `__init__.py`, ~10 lines from `monkey/__init__.py`

**Definition of Done**:
- All audit nemesis live in `monkey/audit.py`
- All unit tests pass

---

### Phase 12: `auth_ldap.py` — LDAP / Authentication Nemesis (~270 lines)

**Target file**: `sdcm/nemesis/monkey/auth_ldap.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_ldap_connection_toggle` | 15 | L1141-L1155 |
| `disrupt_disable_enable_ldap_authorization` | 194 | L1156-L1349 |
| `disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back` | 41 | L871-L911 |
| `remove_ldap_configuration_from_node` | helper | L1170 |
| `add_ldap_configuration_to_node` | helper | L1190 |

**Monkey classes to move from `monkey/__init__.py`**:
- `PauseLdapNemesis` (L20)
- `ToggleLdapConfiguration` (L31)
- `SwitchBetweenPasswordAuthAndSaslauthdAuth` (L594)

**Estimated removal**: ~270 lines from `__init__.py`, ~25 lines from `monkey/__init__.py`

**Definition of Done**:
- All LDAP/auth nemesis live in `monkey/auth_ldap.py`
- All unit tests pass

---

### Phase 13: `node_isolation.py` — Node Ban / Isolation Nemesis (~180 lines)

**Target file**: `sdcm/nemesis/monkey/node_isolation.py`

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Lines | Location |
|--------|-------|----------|
| `disrupt_refuse_connection_with_block_scylla_ports_on_banned_node` | 3 | L5587-L5589 |
| `disrupt_refuse_connection_with_send_sigstop_signal_to_scylla_on_banned_node` | 127 | L5590-L5716 |
| `_refuse_connection_from_banned_node` | helper (bulk) | L5606 |
| `switch_target_node_to_another_rack` | helper | L5593 |

**Monkey classes to move from `monkey/__init__.py`**:
- `IsolateNodeWithProcessSignalNemesis` (L955)
- `IsolateNodeWithIptableRuleNemesis` (L964)

**Estimated removal**: ~180 lines from `__init__.py`, ~15 lines from `monkey/__init__.py`

**Definition of Done**:
- All node isolation nemesis live in `monkey/node_isolation.py`
- All unit tests pass

---

## Summary Table

| Phase | Module | Lines from `__init__.py` | Lines from `monkey/__init__.py` | Total removed |
|-------|--------|--------------------------|----------------------------------|---------------|
| 1 | `network.py` | ~750 | ~70 | ~820 |
| 2 | `sla.py` | ~450 | ~80 | ~530 |
| 3 | `data_operations.py` | ~850 | ~40 | ~890 |
| 4 | `manager.py` | ~300 | ~30 | ~330 |
| 5 | `kubernetes.py` | ~350 | ~30 | ~380 |
| 6 | `cdc.py` | ~200 | ~20 | ~220 |
| 7 | `encryption.py` | ~170 | ~20 | ~190 |
| 8 | **`topology/`** (package) | **~900** | **~70** | **~970** |
|   | └ `__init__.py` (shared helpers) | ~225 | — | ~225 |
|   | └ `grow_shrink.py` | ~350 | ~40 | ~390 |
|   | └ `streaming.py` | ~325 | ~30 | ~355 |
| 9 | `views_indexes.py` | ~470 | ~30 | ~500 |
| 10 | `compaction.py` | ~250 | ~40 | ~290 |
| 11 | `audit.py` | ~130 | ~10 | ~140 |
| 12 | `auth_ldap.py` | ~270 | ~25 | ~295 |
| 13 | `node_isolation.py` | ~180 | ~15 | ~195 |
| **Total** | | **~5,270** | **~480** | **~5,750** |

After all phases, the expected residual in `__init__.py` would be ~800 lines containing:
- `NemesisRunner` core (init, run, report, node selection, event handling): ~300 lines
- `NemesisBaseClass`, `NemesisFlags`, decorators: ~50 lines
- Remaining small/standalone disrupts not worth their own module: ~50 lines
- Imports, constants, `disrupt_method_wrapper`: ~200 lines
- No shared helpers remain — all moved to `topology/` package

## Testing Requirements

**For each phase**:

1. **Unit tests**: Run `uv run sct.py unit-tests` — all existing tests must pass without modification (or with import updates)
2. **Pre-commit**: Run `uv run sct.py pre-commit` — linting and formatting must be clean
3. **Nemesis discovery**: Verify that `NemesisRegistry` auto-discovers all moved nemesis classes (existing auto-import mechanism handles this)
4. **Selector resolution**: Verify nemesis selectors like `"NetworkMonkey|BlockNetworkMonkey"` still work

**Manual testing** (once per 3-4 phases):
- Run a short longevity test with SisyphusMonkey on Docker backend to verify end-to-end nemesis execution
- Verify Argus reporting correctly maps nemesis names

## Success Criteria

1. `nemesis/__init__.py` reduced to ≤1,000 lines (shared helpers also extracted)
2. `monkey/__init__.py` reduced to ≤400 lines
3. All tests pass (unit, integration, pre-commit)
4. No changes to nemesis names, selectors, or user-facing behavior
5. Each extracted module is self-contained and readable (<700 lines)
6. `topology/` package is the single source of truth for node lifecycle operations

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| **Circular imports** | `topology/__init__.py` imports from `sdcm.nemesis` (for `NemesisBaseClass`). `sdcm/nemesis/__init__.py` must NOT import from `topology/`. Residual NemesisRunner methods that need helpers either get extracted too, or use late imports. |
| **Cross-module dependency on `TopologyBaseMonkey`** | K8s and manager monkey classes inherit from `TopologyBaseMonkey`. This is semantically correct (they perform node lifecycle operations) but creates a dependency. If this coupling becomes problematic, extract shared helpers to standalone functions in `topology/node_ops.py` instead. |
| **Merge conflicts with parallel development** | Each phase is a separate PR touching distinct line ranges. Phases are independent after Phase 1. Process phases in priority order. |
| **Import cycles** | Follow the existing pattern from `modify_table.py`: monkey modules import from `sdcm.nemesis` (the `NemesisBaseClass`), not the other way around. The `topology/` package deepens this pattern but doesn't change the direction. |
| **Large PRs** | Phase 8 (topology package) is the largest single phase (~970 lines). Split into sub-PRs: 8a (`__init__.py` + `grow_shrink.py`), 8b (`streaming.py`). |
| **Residual NemesisRunner methods needing helpers** | After Phase 8, methods like `disrupt_hard_reboot_node` still need `reboot_node`. Plan recommends extracting these into `topology/` sub-modules (e.g., `topology/node_ops.py`). Alternatively, keep thin proxy methods on NemesisRunner that delegate to topology. |

## Open Questions

1. **`snapshot_operations`, `show_toppartitions`, `validate_hh_short_downtime`**: These standalone nemesis (~300 lines total) are not strongly related to any group. Should they get their own module or stay in `__init__.py`? **Recommendation**: group them as `misc.py` if desired, but low priority.
2. **Standalone nemesis using topology helpers**: After Phase 8, `disrupt_hard_reboot_node` (5 lines, calls `reboot_node`), `disrupt_destroy_data_then_repair` (15 lines, calls `_destroy_data_and_restart_scylla` + `run_repair`), `disrupt_restart_then_repair_node` (20 lines, calls `run_repair`), and `disrupt_abort_repair` (70 lines, calls `run_repair`) all need helpers that moved to `TopologyBaseMonkey`. Options: (a) extract them to `topology/node_ops.py`, (b) make them standalone monkey classes inheriting from `TopologyBaseMonkey`, (c) keep thin proxy methods on NemesisRunner. **Recommendation**: (a) — they ARE node operations.
3. **Decommission nemesis**: `disrupt_nodetool_decommission` and `disrupt_nodetool_seed_decommission` use `_nodetool_decommission`, `_add_and_init_new_cluster_nodes`, `_terminate_cluster_node`, `_replace_cluster_node` — all moved to `TopologyBaseMonkey`. Should they go into `topology/decommission.py`? **Recommendation**: yes, they are topology operations. This would further reduce `__init__.py` by ~80 lines.
4. **`NemesisRegistry` scanning depth**: Currently `generator.py` scans `monkey/` for submodules. Does it recurse into sub-packages like `monkey/topology/`? Verify before Phase 8 implementation and update if needed.
