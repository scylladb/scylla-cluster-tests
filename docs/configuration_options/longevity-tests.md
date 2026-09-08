# Longevity tests

[← All configuration options](configuration_options.md)

Options specific to long-running longevity test scenarios.

**13 options.** Jump to: [cluster_target_size](#cluster_target_size) · [compaction_strategy](#compaction_strategy) · [data_validation](#data_validation) · [post_prepare_cql_cmds](#post_prepare_cql_cmds) · [pre_create_keyspace](#pre_create_keyspace) · [pre_create_schema](#pre_create_schema) · [run_commit_log_check_thread](#run_commit_log_check_thread) · [run_full_partition_scan](#run_full_partition_scan) · [run_fullscan](#run_fullscan) · [run_tombstone_gc_verification](#run_tombstone_gc_verification) · [space_node_threshold](#space_node_threshold) · [sstable_size](#sstable_size) · [validate_large_collections](#validate_large_collections)


## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Compaction strategy to use for pre-created schema

**default:** IncrementalCompactionStrategy

**type:** str (appendable)


## **data_validation** / SCT_DATA_VALIDATION

Specify the type of data validation to perform

**default:** N/A

**type:** str (appendable)


## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keyspace to be pre-created before running workload

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **pre_create_schema** / SCT_PRE_CREATE_SCHEMA

Enable or disable pre-creation of schema before running workload

**default:** False

**type:** bool


## **run_commit_log_check_thread** / SCT_RUN_COMMIT_LOG_CHECK_THREAD

Flag to run a thread that checks commit logs

**default:** True

**type:** bool

**backend overrides:**
- `False`: xcloud


## **run_full_partition_scan** / SCT_RUN_FULL_PARTITION_SCAN

Enable or disable running full partition scans during tests

**default:** N/A

**type:** str (appendable)


## **run_fullscan** / SCT_RUN_FULLSCAN

Enable or disable running full scans during tests

**default:** []

**type:** list


## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Enable or disable tombstone garbage collection verification during tests

**default:** N/A

**type:** str (appendable)


## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** 0

**type:** int


## **sstable_size** / SCT_SSTABLE_SIZE

Configure sstable size for pre-create-schema mode

**default:** N/A

**type:** int


## **validate_large_collections** / SCT_VALIDATE_LARGE_COLLECTIONS

Flag to validate large collections in the database

**default:** False

**type:** bool
