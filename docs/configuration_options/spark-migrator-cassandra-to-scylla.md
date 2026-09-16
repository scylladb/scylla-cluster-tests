# Spark migrator (Cassandra to Scylla)

[← All configuration options](../configuration_options.md)

The migration job itself: source and target keyspaces/tables and validation.

**9 options.**


<a id="migrator_run_validator"></a>

## **migrator_run_validator** / SCT_MIGRATOR_RUN_VALIDATOR

Run the spark-migrator validator after migration to do a row-by-row comparison

**default:** N/A

**type:** bool


<a id="migrator_source_hosts"></a>

## **migrator_source_hosts** / SCT_MIGRATOR_SOURCE_HOSTS

CQL contact-point IPs for the source Cassandra/Scylla cluster. Mutually exclusive with [`migrator_source_test_id`](#migrator_source_test_id).

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="migrator_source_keyspace"></a>

## **migrator_source_keyspace** / SCT_MIGRATOR_SOURCE_KEYSPACE

Keyspace to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


<a id="migrator_source_table"></a>

## **migrator_source_table** / SCT_MIGRATOR_SOURCE_TABLE

Table to migrate from on the source cluster

**default:** N/A

**type:** str (appendable)


<a id="migrator_source_test_id"></a>

## **migrator_source_test_id** / SCT_MIGRATOR_SOURCE_TEST_ID

SCT [`test_id`](general-and-provisioning.md#test_id) of a running source cluster. When set, source host IPs are auto-discovered via EC2 tags (NodeType=cs-db). Mutually exclusive with [`migrator_source_hosts`](#migrator_source_hosts).

**default:** N/A

**type:** str (appendable)


<a id="migrator_step_timeout_minutes"></a>

## **migrator_step_timeout_minutes** / SCT_MIGRATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator migration EMR step. Default 360.

**default:** N/A

**type:** int

**backend overrides:**
- `360`: aws


<a id="migrator_target_keyspace"></a>

## **migrator_target_keyspace** / SCT_MIGRATOR_TARGET_KEYSPACE

Keyspace to migrate into on the target Scylla cluster. Defaults to [`migrator_source_keyspace`](#migrator_source_keyspace).

**default:** N/A

**type:** str (appendable)


<a id="migrator_target_table"></a>

## **migrator_target_table** / SCT_MIGRATOR_TARGET_TABLE

Table to migrate into on the target Scylla cluster. Defaults to [`migrator_source_table`](#migrator_source_table).

**default:** N/A

**type:** str (appendable)


<a id="validator_step_timeout_minutes"></a>

## **validator_step_timeout_minutes** / SCT_VALIDATOR_STEP_TIMEOUT_MINUTES

Time in minutes to wait for the spark-migrator validator EMR step. Default 60.

**default:** N/A

**type:** int

**backend overrides:**
- `60`: aws
