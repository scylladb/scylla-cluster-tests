# Nemesis Extraction Plan — Phase 3: Split Existing Nemesis Into Modules

## Problem Statement

[sdcm/nemesis/__init__.py](sdcm/nemesis/__init__.py) is 6,067 lines long and [sdcm/nemesis/monkey/__init__.py](sdcm/nemesis/monkey/__init__.py) is 978 lines. Both files contain dozens of unrelated nemesis operations mixed together, making the codebase hard to navigate, review, and maintain.

The `ModifyTable*` nemesis have already been extracted into [sdcm/nemesis/monkey/modify_table.py](sdcm/nemesis/monkey/modify_table.py) (566 lines), removing ~569 lines from the two `__init__.py` files. This plan continues that work by identifying the remaining groups of nemesis that should be extracted, ordered by impact (lines removed from `__init__.py`).

## Current State

### File Sizes

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

### Extraction Pattern

1. Create `sdcm/nemesis/monkey/<group_name>.py`
2. Move the `disrupt_*` methods from `NemesisRunner` into the new monkey class(es) as `disrupt()` methods
3. Move associated private helper methods and instance variables that are **only used by that group**
4. Move corresponding Monkey classes from `monkey/__init__.py`
5. Shared helpers that are used by multiple groups are extracted **incrementally** — each phase extracts the shared helpers it needs, the first time they are needed. They are split into two files depending on their nature:
   - **Node operation helpers** (add/remove/decommission/replace — operations that change cluster membership) move to `sdcm/nemesis/utils/topology_ops.py`. These are stateless functions that accept a cluster/node plus parameters and operate on them.
   - **General-purpose helpers** that don't fit a neat topological grouping (reboot, repair, data destruction, test table setup, snapshot cleanup) move to `sdcm/nemesis/utils/common_ops.py`. These are cross-cutting utilities used by many unrelated nemesis groups.
   - Both are imported as plain functions by any monkey module that needs them — no base class inheritance required.
   - Later phases that need the same helper simply import it from the utils file where an earlier phase already placed it.
6. When a group of related nemesis is large enough, extract it as a **sub-package** (directory with `__init__.py`) rather than a flat module. Sub-packages can contain shared helpers in a base module, plus submodules for specific nemesis categories.
7. Add unit tests in `unit_tests/nemesis/monkeys/test_<group_name>.py` to verify the migration: monkey class instantiation, helper function behavior with mocked objects, import path resolution, and `NemesisRegistry` discovery.

## Goals

1. **Reduce `nemesis/__init__.py` from ~6,067 lines to ~750 lines** by extracting ALL `disrupt_*` methods, their helpers, AND shared node-operation helpers. No disruption logic remains in `__init__.py`.
2. **Reduce `monkey/__init__.py` from ~978 lines to ~350 lines** by co-locating monkey classes with their implementation; small/standalone monkey classes stay in `monkey/__init__.py` with their `disrupt()` logic inlined directly
3. **Group related nemesis into cohesive, self-contained modules** for easier maintenance
4. **Extract shared helpers into `sdcm/nemesis/utils/`** — node operation helpers into `topology_ops.py`, cross-cutting utilities into `common_ops.py`, extending the existing utils package
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
| `_truncate_cmd_timeout_suffix` (2 versions) | helpers | L2151-L2156 |

**Shared helper to extract to `utils/common_ops.py`** (first use — `disrupt_truncate` calls it):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `_prepare_test_table` | `prepare_test_table(tester, ks, table)` | ~15 |

This is the first phase that needs `_prepare_test_table`. It is a cross-group utility (also used by auth/ldap, repair, streaming, refresh nemesis), so it goes to `sdcm/nemesis/utils/common_ops.py`. Later phases import it from there.

**Monkey classes to move from `monkey/__init__.py`**:
- `TruncateMonkey` (L298)
- `TruncateLargeParititionMonkey` (L309)
- `DeleteTenFullPartitionsMonkey` (L319 — if exists)
- `DeleteOverlappingRowRangesMonkey` (if exists)
- `DeleteByRowsRangeMonkey` (if exists)
- `AddDropColumnMonkey` (if exists)

**Estimated removal**: ~865 lines from `__init__.py` (~850 group-specific + ~15 `_prepare_test_table`), ~40 lines from `monkey/__init__.py`

**Definition of Done**:
- All data manipulation nemesis live in `monkey/data_operations.py`
- `prepare_test_table` extracted to `sdcm/nemesis/utils/common_ops.py` as a standalone function
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

**Estimated removal**: ~355 lines from `__init__.py` (~300 group-specific + ~55 `_destroy_data_and_restart_scylla`), ~30 lines from `monkey/__init__.py`

**Shared helper to extract to `utils/common_ops.py`** (first use — `disrupt_mgmt_corrupt_then_repair` calls it):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `_destroy_data_and_restart_scylla` | `destroy_data_and_restart_scylla(target_node, ...)` | ~55 |

This is the first phase that needs `_destroy_data_and_restart_scylla`. It is a cross-group utility (also used by data-destroy nemesis and streaming nemesis), so it goes to `sdcm/nemesis/utils/common_ops.py`. Later phases import it from there.

**Definition of Done**:
- All manager nemesis live in `monkey/manager.py`
- `destroy_data_and_restart_scylla` extracted to `sdcm/nemesis/utils/common_ops.py` as a standalone function
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

**Estimated removal**: ~405 lines from `__init__.py` (~350 group-specific + ~55 topology helpers), ~30 lines from `monkey/__init__.py`

**Shared helpers to extract to `utils/topology_ops.py`** (first use — K8s decommission-and-add nemesis call them):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `_add_and_init_new_cluster_nodes` | `add_and_init_new_cluster_nodes(cluster, count, ...)` | ~50 |
| `add_new_nodes` | `add_new_nodes(cluster, count, ...)` | ~5 |

This is the first phase that needs these node operation helpers. They are also used by grow/shrink and streaming nemesis (Phase 8), so they go to `sdcm/nemesis/utils/topology_ops.py`. Each function takes a `cluster` as its first parameter instead of relying on `self`. Later phases import them from there.

**Definition of Done**:
- All K8s-specific nemesis live in `monkey/kubernetes.py`
- `add_and_init_new_cluster_nodes` and `add_new_nodes` extracted to `sdcm/nemesis/utils/topology_ops.py` as standalone functions
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

### Phase 8a: Shared Helper Extraction for Topology Nemesis (~75 lines)

**Target files**: `sdcm/nemesis/utils/topology_ops.py`, `sdcm/nemesis/utils/common_ops.py`

This phase extracts the remaining shared helpers that are needed by the topology nemesis (Phases 8b and 8c) and other residual nemesis in `__init__.py`. It also creates the `monkey/topology/` package skeleton.

By this point, earlier phases have already extracted some shared helpers into `sdcm/nemesis/utils/`:
- Phase 3 → `prepare_test_table` in `common_ops.py`
- Phase 4 → `destroy_data_and_restart_scylla` in `common_ops.py`
- Phase 5 → `add_and_init_new_cluster_nodes`, `add_new_nodes` in `topology_ops.py`

**Shared helpers to extract to `utils/topology_ops.py`** (first use in this phase):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `decommission_nodes` | `decommission_nodes(cluster, nodes)` | ~10 |
| `_decommission_nodes` | `decommission_nodes_by_criteria(cluster, nodes_number, ...)` | ~30 |
| `_terminate_cluster_node` | `terminate_cluster_node(cluster, node)` | ~5 |

**Shared helpers to extract to `utils/common_ops.py`** (first use in this phase):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `reboot_node` | `reboot_node(target_node, hard=True, verify_ssh=True)` | ~5 |
| `run_repair` | `run_repair(cluster, target_node, ignore_down_hosts=False)` | ~15 |
| `rebuild_or_repair` | `rebuild_or_repair(cluster, target_node, reason="")` | ~10 |

**Design**: Each function takes a `cluster` (and optionally `target_node`, `tester`) as its first parameter instead of relying on `self`. The latency-measurement decorators that existed on some of these methods (e.g., `decommission_nodes`) are preserved as decorators on the standalone functions.

**Package skeleton to create**:
```
monkey/topology/
    __init__.py          — re-exports for discovery (~5 lines)
```

The topology package is a **pure nemesis package** — it will contain only `disrupt_*` implementations and their group-specific helpers (added in 8b, 8c, and 8d). No shared base class is needed; monkey classes inherit directly from `NemesisBaseClass` and import helpers from `sdcm.nemesis.utils.topology_ops` and `sdcm.nemesis.utils.common_ops`.

**Estimated removal**: ~75 lines from `__init__.py` (shared helpers only)

**Definition of Done**:
- Shared helpers (`decommission_nodes`, `decommission_nodes_by_criteria`, `terminate_cluster_node`, `reboot_node`, `run_repair`, `rebuild_or_repair`) extracted to respective utils files
- `monkey/topology/__init__.py` exists (empty package skeleton)
- All call sites in `NemesisRunner` updated to import from `sdcm.nemesis.utils.*`
- All unit tests pass

---

### Phase 8b: `topology/grow_shrink.py` — Grow/Shrink Cluster Nemesis (~350 lines)

**Target file**: `sdcm/nemesis/monkey/topology/grow_shrink.py`

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

These monkey classes import helpers from `sdcm.nemesis.utils.topology_ops` (placed there by Phase 5 and Phase 8a).

**Estimated removal**: ~350 lines from `__init__.py`, ~35 lines from `monkey/__init__.py`

**Definition of Done**:
- All grow/shrink and DC/rack nemesis live in `monkey/topology/grow_shrink.py`
- Monkey classes inherit from `NemesisBaseClass` and import helpers as standalone functions
- All unit tests pass; nemesis selector resolution unchanged

---

### Phase 8c: `topology/sequence.py` — Unique Sequence Nemesis (~75 lines)

**Target file**: `sdcm/nemesis/monkey/topology/sequence.py`

This is a composite nemesis that orchestrates a fixed sequence of topology operations: steady-state latency capture → manager repair → grow → terminate-and-replace → shrink. It depends on helpers extracted in earlier phases (8a, 8b) and imports `run_repair_manager` from `monkey/manager.py` (Phase 4).

**Methods to extract from `NemesisRunner` (`__init__.py`)**:

| Method | Type | Location |
|--------|------|----------|
| `disrupt_run_unique_sequence` | disrupt | L5016-L5042 |
| `steady_state_latency` | helper (shared with 8b) | L5008-L5014 |
| `_terminate_and_replace_node` | helper (shared with `disrupt_terminate_and_replace_node`) | L1624-L1670 |

**Shared helper to extract to `utils/common_ops.py`** (first use in this phase):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `steady_state_latency` | `steady_state_latency(cluster, sleep_time=None)` | ~7 |
| `_terminate_and_replace_node` | `terminate_and_replace_node(cluster, target_node, ...)` | ~47 |

`steady_state_latency` is also used by `disrupt_grow_shrink_cluster` (Phase 8b). `_terminate_and_replace_node` is also used by `disrupt_terminate_and_replace_node` (a standalone nemesis that stays in `monkey/__init__.py`). Both become standalone functions in `utils/common_ops.py` and are imported by all callers.

**Monkey class to move from `monkey/__init__.py`**:
- `NemesisSequence` (L688)

**Estimated removal**: ~75 lines from `__init__.py` (~27 disrupt + ~47 `_terminate_and_replace_node` + ~7 `steady_state_latency` - ~6 monkey class stays as import), ~6 lines from `monkey/__init__.py`

**Definition of Done**:
- `NemesisSequence` lives in `monkey/topology/sequence.py` with `disrupt()` logic inlined
- `steady_state_latency` and `terminate_and_replace_node` extracted to `sdcm/nemesis/utils/common_ops.py` as standalone functions
- `disrupt_grow_shrink_cluster` (Phase 8b) and `disrupt_terminate_and_replace_node` (in `monkey/__init__.py`) import from utils
- All unit tests pass; nemesis selector resolution unchanged

---

### Phase 8d: `topology/streaming.py` — Streaming Error Nemesis (~300 lines)

**Target file**: `sdcm/nemesis/monkey/topology/streaming.py`

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

These monkey classes import shared helpers from both utils modules:
```python
from sdcm.nemesis.utils.topology_ops import add_and_init_new_cluster_nodes  # placed by Phase 5
from sdcm.nemesis.utils.common_ops import (  # placed by Phases 4 and 8a
    reboot_node, destroy_data_and_restart_scylla, rebuild_or_repair, run_repair,
)
```
The streaming-exclusive helper `_call_disrupt_func_after_expression_logged` stays in `streaming.py`.

**Estimated removal**: ~300 lines from `__init__.py`, ~35 lines from `monkey/__init__.py`

**Definition of Done**:
- All streaming error nemesis live in `monkey/topology/streaming.py`
- `_call_disrupt_func_after_expression_logged` lives in `streaming.py` (exclusive to streaming)
- `NemesisRunner` no longer contains ANY of the topology-related helpers or disrupt methods
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

### Phase 10: `compaction.py` — Compaction Nemesis (~255 lines)

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
| `_major_compaction` | helper | L1702 |
| `_corrupt_data_file` | helper | L4202 |

**Shared helper to extract to `utils/common_ops.py`** (first use — `disrupt_start_stop_major_compaction` and `disrupt_corrupt_then_scrub` call it):

| Method on NemesisRunner | Standalone function name | Lines |
|-------------------------|--------------------------|-------|
| `clear_snapshots` | `clear_snapshots(target_node)` | ~5 |

This is the first phase that needs `clear_snapshots`. It is a cross-group utility (also used by snapshot-operations nemesis), so it goes to `sdcm/nemesis/utils/common_ops.py`. Later phases or residual nemesis in `__init__.py` import it from there.

**Monkey classes to move from `monkey/__init__.py`**:
- `StartStopMajorCompaction` (L779)
- `StartStopScrubCompaction` (L786)
- `StartStopCleanupCompaction` (L793)
- `StartStopValidationCompaction` (L800)
- `MajorCompactionMonkey` (L217)
- `CorruptThenScrubMonkey` (L752)

**Estimated removal**: ~255 lines from `__init__.py` (~250 group-specific + ~5 `clear_snapshots`), ~40 lines from `monkey/__init__.py`

**Definition of Done**:
- All compaction nemesis live in `monkey/compaction.py`
- `clear_snapshots` extracted to `sdcm/nemesis/utils/common_ops.py` as a standalone function
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
| 3 | `data_operations.py` + `utils/common_ops.py` (`prepare_test_table`) | ~865 | ~40 | ~905 |
| 4 | `manager.py` + `utils/common_ops.py` (`destroy_data_and_restart_scylla`) | ~355 | ~30 | ~385 |
| 5 | `kubernetes.py` + `utils/topology_ops.py` (`add_and_init_new_cluster_nodes`, `add_new_nodes`) | ~405 | ~30 | ~435 |
| 6 | `cdc.py` | ~200 | ~20 | ~220 |
| 7 | `encryption.py` | ~170 | ~20 | ~190 |
| 8a | `utils/topology_ops.py` + `utils/common_ops.py` (remaining shared helpers) + `topology/` skeleton | ~75 | — | ~75 |
| 8b | `topology/grow_shrink.py` | ~350 | ~35 | ~385 |
| 8c | `topology/sequence.py` + `utils/common_ops.py` (`steady_state_latency`, `terminate_and_replace_node`) | ~75 | ~6 | ~81 |
| 8d | `topology/streaming.py` | ~300 | ~35 | ~335 |
| 9 | `views_indexes.py` | ~470 | ~30 | ~500 |
| 10 | `compaction.py` + `utils/common_ops.py` (`clear_snapshots`) | ~255 | ~40 | ~295 |
| 11 | `audit.py` | ~130 | ~10 | ~140 |
| 12 | `auth_ldap.py` | ~270 | ~25 | ~295 |
| 13 | `node_isolation.py` | ~180 | ~15 | ~195 |
| **Total** | | **~5,300** | **~486** | **~5,786** |

**Shared helper extraction is distributed across phases** (no standalone "extract utils" phase):
- Phase 3 creates `utils/common_ops.py` with `prepare_test_table` (~15 lines)
- Phase 4 adds `destroy_data_and_restart_scylla` to `utils/common_ops.py` (~55 lines)
- Phase 5 creates `utils/topology_ops.py` with `add_and_init_new_cluster_nodes`, `add_new_nodes` (~55 lines)
- Phase 8a adds `decommission_nodes`, `decommission_nodes_by_criteria`, `terminate_cluster_node` to `utils/topology_ops.py` (~45 lines); adds `reboot_node`, `run_repair`, `rebuild_or_repair` to `utils/common_ops.py` (~30 lines)
- Phase 8c adds `steady_state_latency`, `terminate_and_replace_node` to `utils/common_ops.py` (~54 lines)
- Phase 10 adds `clear_snapshots` to `utils/common_ops.py` (~5 lines)

After all phases, the expected residual in `__init__.py` would be ~750 lines containing:
- `NemesisRunner` core (init, run, report, node selection, event handling): ~300 lines
- `NemesisBaseClass`, `NemesisFlags`, decorators: ~50 lines
- Imports, constants, `disrupt_method_wrapper`: ~200 lines
- No `disrupt_*` method implementations remain — all moved into monkey classes
- No shared helpers remain — all moved to `sdcm/nemesis/utils/` package

**Important invariant**: No `disrupt_*` method code stays in `nemesis/__init__.py`. Small/standalone nemesis that don't warrant their own file have their `disrupt()` logic inlined directly into the monkey class in `monkey/__init__.py`. This keeps `NemesisRunner` free of any disruption logic.

## Testing Requirements

**For each phase**:

1. **Existing unit tests**: Run `uv run sct.py unit-tests` — all existing tests must pass without modification (or with import updates)
2. **New unit tests for extracted nemesis**: Each phase must add unit tests for the migrated nemesis to verify the extraction preserved behavior. Tests should cover:
   - **Instantiation**: The monkey class can be instantiated and its `disrupt()` method is callable
   - **Functionality**: The monkey class with mocked Scylla cluster performs the operations we expect
   - **Failure states**: The monkey class with mocked Scylla cluster upon failure does not leave cluster in inconsistent state
   - **Helper functions**: Extracted standalone helpers in `utils/topology_ops.py` and `utils/common_ops.py` are independently testable with mocked cluster/node objects
3. **Pre-commit**: Run `uv run sct.py pre-commit` — linting and formatting must be clean
4. **Nemesis discovery**: Verify that `NemesisRegistry` auto-discovers all moved nemesis classes (existing auto-import mechanism handles this)
5. **Selector resolution**: Verify nemesis selectors like `"NetworkMonkey|BlockNetworkMonkey"` still work

**New test file convention**: Unit tests for extracted modules go in `unit_tests/nemesis/monkeys/test_<module_name>.py` (e.g., `unit_tests/nemesis/monkeys/test_network.py` for Phase 1). Tests for shared utils go in `unit_tests/nemesis/monkeys/test_topology_ops.py` and `unit_tests/nemesis/monkeys/test_common_ops.py`.

**Manual testing** (once per 3-4 phases):
- Run a short longevity test with SisyphusMonkey on Docker backend to verify end-to-end nemesis execution
- Verify Argus reporting correctly maps nemesis names

## Success Criteria

1. `nemesis/__init__.py` reduced to ≤800 lines — zero `disrupt_*` method implementations remain
2. `monkey/__init__.py` reduced to ≤400 lines (small/standalone monkey classes with inlined `disrupt()` stay here)
3. All tests pass (unit, integration, pre-commit)
4. Each extracted phase includes new unit tests verifying migration correctness (instantiation, helper behavior, import paths, registry discovery)
5. No changes to nemesis names, selectors, or user-facing behavior
6. Each extracted module is self-contained and readable (<700 lines)
7. `sdcm/nemesis/utils/topology_ops.py` is the single source for node operations; `sdcm/nemesis/utils/common_ops.py` is the single source for cross-cutting nemesis utilities

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| **Circular imports** | `utils/topology_ops.py` and `utils/common_ops.py` import from `sdcm.cluster` (for `BaseNode`, `BaseCluster`) but NOT from `sdcm.nemesis`. Monkey modules import from `sdcm.nemesis` (for `NemesisBaseClass`) and from `sdcm.nemesis.utils`. No circular path exists. |
| **Function signature changes** | Extracting methods to standalone functions requires converting `self.cluster`, `self.target_node`, `self.tester` to explicit parameters. This is a mechanical refactor — each call site passes the previously-implicit attributes. Careful review of each call site is needed. |
| **Merge conflicts with parallel development** | Each phase is a separate PR. Phases that only extract nemesis into new files touch distinct line ranges and can be parallelized. However, phases that add shared helpers to `utils/common_ops.py` or `utils/topology_ops.py` (Phases 3, 4, 5, 8a, 8c, 10) can collide — these must be merged sequentially. Process phases in priority order; rebase later phases after earlier ones land. |
| **Import cycles** | Follow the existing pattern: monkey modules import from `sdcm.nemesis` (the `NemesisBaseClass`), not the other way around. Utils modules import only from `sdcm.cluster` and standard library. |
| **Large PRs** | Phase 8 is split into four sub-phases: 8a (shared helper extraction + package skeleton, ~75 lines), 8b (`topology/grow_shrink.py`, ~385 lines), 8c (`topology/sequence.py`, ~81 lines), and 8d (`topology/streaming.py`, ~335 lines). Each sub-phase is an independent PR of manageable size. |
| **Residual NemesisRunner methods needing helpers** | As each phase extracts shared helpers incrementally, later phases and residual `__init__.py` methods simply import from `sdcm.nemesis.utils.common_ops` or `sdcm.nemesis.utils.topology_ops`. This is a simple function import — no inheritance or base class coupling required. |
| **Backporting** | After extraction, nemesis logic lives in new file locations (`monkey/<module>.py`, `utils/*.py`) with different structure (`self.runner` delegation, standalone helper functions). Bug fixes backported to older branches that still have the monolithic `nemesis/__init__.py` layout will require manual conflict resolution since the code structure differs, not just the file location. This is a known cost of the refactor. |
