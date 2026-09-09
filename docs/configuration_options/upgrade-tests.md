# Upgrade tests

[← All configuration options](configuration_options.md)

Rolling upgrade and rollback scenarios: target versions and the load applied across the upgrade.

**20 options.**


## **disable_raft** / SCT_DISABLE_RAFT

Flag to disable Raft consensus for LWT operations.

**default:** True

**type:** bool


## **enable_tablets_on_upgrade** / SCT_ENABLE_TABLETS_ON_UPGRADE

By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade

**default:** False

**type:** bool


## **enable_truncate_checks_on_node_upgrade** / SCT_ENABLE_TRUNCATE_CHECKS_ON_NODE_UPGRADE

Enables or disables truncate checks on each node upgrade and rollback

**default:** True

**type:** bool


## **enable_views_with_tablets_on_upgrade** / SCT_ENABLE_VIEWS_WITH_TABLETS_ON_UPGRADE

Enables creating materialized views in keyspaces using tablets by adding an experimental feature.It should not be used when upgrading to versions before 2025.1 and it should be used for upgradeswhere we create such views.

**default:** False

**type:** bool


## **large_partition_stress_during_upgrade** / SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE

Stress command to be run during rolling upgrade while nodes are being upgraded. This workload cannot use CL=ALL as not all nodes may be available during the upgrade.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **new_scylla_repo** / SCT_NEW_SCYLLA_REPO

URL to the Scylla repository for new versions.

**default:** N/A

**type:** str (appendable)


## **new_version** / SCT_NEW_VERSION

Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1

**default:** N/A

**type:** str (appendable)


## **num_nodes_to_rollback** / SCT_NUM_NODES_TO_ROLLBACK

Number of nodes to upgrade and rollback in test_generic_cluster_upgrade

**default:** N/A

**type:** int


## **run_gemini_in_rolling_upgrade** / SCT_RUN_GEMINI_IN_ROLLING_UPGRADE

Enable running Gemini workload during rolling upgrade test. Default is false.

**default:** False

**type:** bool


## **stress_after_cluster_upgrade** / SCT_STRESS_AFTER_CLUSTER_UPGRADE

Stress command to be run after full upgrade - usually used to read the dataset for verification

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_before_upgrade** / SCT_STRESS_BEFORE_UPGRADE

Stress command to be run before upgrade starts (preload/validation stage). This workload runs before any nodes are upgraded and can use CL=ALL for data validation.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **stress_during_entire_upgrade** / SCT_STRESS_DURING_ENTIRE_UPGRADE

Stress command to be run during the upgrade - user should take care for suitable duration

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **target_upgrade_version** / SCT_TARGET_UPGRADE_VERSION

The target version to upgrade Scylla to.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_packages** / SCT_UPGRADE_NODE_PACKAGES

Specifies the packages to be upgraded on the node.

**default:** N/A

**type:** str (appendable)


## **upgrade_node_system** / SCT_UPGRADE_NODE_SYSTEM

Upgrade system packages on nodes before upgrading Scylla. Enabled by default.

**default:** True

**type:** bool


## **upgrade_sstables** / SCT_UPGRADE_SSTABLES

Whether to upgrade sstables as part of upgrade_node or not

**default:** N/A

**type:** bool


## **verify_data_after_entire_test** / SCT_VERIFY_DATA_AFTER_ENTIRE_TEST

Stress command to verify data integrity after the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_cluster_upgrade** / SCT_VERIFY_STRESS_AFTER_CLUSTER_UPGRADE

Stress command(s) run after every node has been upgraded, to verify the upgraded cluster. See [`stress_cmd`](stress-commands-and-load-generation.md#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **verify_stress_after_migration** / SCT_VERIFY_STRESS_AFTER_MIGRATION

Stress command to verify data after migration

**default:** N/A

**type:** str (appendable)


## **write_stress_during_entire_test** / SCT_WRITE_STRESS_DURING_ENTIRE_TEST

Stress command to perform write operations throughout the entire test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)
