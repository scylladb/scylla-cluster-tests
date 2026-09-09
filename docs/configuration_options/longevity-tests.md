# Longevity tests

[← All configuration options](../configuration_options.md)

Options specific to long-running longevity test scenarios.

**13 options.**


<a id="cluster_target_size"></a>

## **cluster_target_size** / SCT_CLUSTER_TARGET_SIZE

Used for scale test: max size of the cluster

**default:** N/A

**type:** int | list[int] | space-separated ints → list[int]


<a id="compaction_strategy"></a>

## **compaction_strategy** / SCT_COMPACTION_STRATEGY

Compaction strategy to use for pre-created schema

**default:** IncrementalCompactionStrategy

**type:** str (appendable)


<a id="data_validation"></a>

## **data_validation** / SCT_DATA_VALIDATION

Specify the type of data validation to perform

**default:** N/A

**type:** str (appendable)


<a id="post_prepare_cql_cmds"></a>

## **post_prepare_cql_cmds** / SCT_POST_PREPARE_CQL_CMDS

CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="pre_create_keyspace"></a>

## **pre_create_keyspace** / SCT_PRE_CREATE_KEYSPACE

Command to create keyspace to be pre-created before running workload

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="pre_create_schema"></a>

## **pre_create_schema** / SCT_PRE_CREATE_SCHEMA

Enable or disable pre-creation of schema before running workload

**default:** False

**type:** bool


<a id="run_commit_log_check_thread"></a>

## **run_commit_log_check_thread** / SCT_RUN_COMMIT_LOG_CHECK_THREAD

Flag to run a thread that checks commit logs

**default:** True

**type:** bool

**backend overrides:**
- `False`: xcloud


<a id="run_full_partition_scan"></a>

## **run_full_partition_scan** / SCT_RUN_FULL_PARTITION_SCAN

Enable or disable running full partition scans during tests

**default:** N/A

**type:** str (appendable)


<a id="run_fullscan"></a>

## **run_fullscan** / SCT_RUN_FULLSCAN

Enable or disable running full scans during tests

**default:** []

**type:** list


<a id="run_tombstone_gc_verification"></a>

## **run_tombstone_gc_verification** / SCT_RUN_TOMBSTONE_GC_VERIFICATION

Enable or disable tombstone garbage collection verification during tests

**default:** N/A

**type:** str (appendable)


<a id="space_node_threshold"></a>

## **space_node_threshold** / SCT_SPACE_NODE_THRESHOLD

Space node threshold before starting nemesis (bytes)<br>The default value is 6GB (6x1024^3 bytes)<br>This value is supposed to reproduce<br>https://github.com/scylladb/scylla/issues/1140

**default:** 0

**type:** int


<a id="sstable_size"></a>

## **sstable_size** / SCT_SSTABLE_SIZE

Configure sstable size for pre-create-schema mode

**default:** N/A

**type:** int


<a id="validate_large_collections"></a>

## **validate_large_collections** / SCT_VALIDATE_LARGE_COLLECTIONS

Flag to validate large collections in the database

**default:** False

**type:** bool
