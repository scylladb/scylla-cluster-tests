### **Guide: Using Full-Scan and Full-Partition-Scan in Scylla Cluster Test (SCT)**

**Overview**
In Scylla Cluster Test (SCT) Longevity, `full-scan` and `full-partition-scan` are background threads used for testing long-duration select-queries. These operations are configured in test YAML files and help ensure that the database’s tables and partitions are consistent and can be read without unexpected Scylla errors.

This can be tested during or after various disruptive nemesis.

* **Full-scan**: Its primary goal is to verify that Scylla can handle a comprehensive scan of an entire table without significant performance issues or failures.
  * Variants:
    * A simple “select \*” from a table.
    * An aggregation mode of “select count(\*)”
* **Full-partition-scan**: Particularly relevant for large-partition test configurations, this scan focuses on counting the number of rows per partition.
  * variants:
    * Read all partition data
    * Query and compare a partition data in both Ascending and Descending order  (by the parameter of "validate\_data")

**Configuration in YAML**
The full-scan operation can be customized by specifying parameters like mode, keyspace, table, and intervals. The two main modes are:

* **Full Table Scan (`table`)**: This scans all the rows in the specified table.
* **Full Partition Scan (`partition`)**: This scans partitions, particularly useful for testing large partitions or checking partition key ranges.

You can configure full-scan operations in your test YAML by defining a `run_fullscan` parameter with a set of attributes. Below is an example of a typical YAML configuration:

run\_fullscan: \['{"mode": "table\_and\_aggregate", "ks\_cf": "keyspace1.standard1", "interval": 10}'\]

This example performs a full table scan and aggregates the results from the keyspace `keyspace1` and table `standard1` every 10 seconds.

#### **Common Parameters:**

1. **`mode`**: Defines the type of scan. Can be `table`, `aggregate,` `partition`, `random`, or `table_and_aggregate`.
   * `table`: Scans the entire table.
   * `aggregate`: Scans the entire table using “count”.
   * `partition`: Scans partition ranges.
   * `table_and_aggregate`: randomly choose one of "table", "aggregate".
   * `random`: randomly choose one of "table", "aggregate" and "partition".
2. **`ks_cf`**: Defines the keyspace and column family (table) that will be scanned. Can be set to a specific table like `keyspace1.standard1` or use `"random"` for randomly selected keyspaces tables.
3. **`interval`**: Time in seconds between consecutive scans. This is useful for scheduling recurring scans throughout a test's duration.
4. **`pk_name` (Optional)**: Defines the partition key name for full partition scans.
5. **`rows_count` (Optional)**: Specifies how many rows should be scanned in a partition scan, typically used for limiting large partition scans.
6. **`validate_data` (Optional)**: A boolean flag to enable or disable a reversed-query comparison during the scan. This queries the partition in both “Ascending” and “Decending” order, and verifies output data is the same.

**Example Use Cases in YAML Files**

* **Random Full Scan**
  This configuration selects a random mode for the given table, running a scan every 5 seconds:

run\_fullscan: \['{"mode": "random", "ks\_cf": "keyspace1.standard1", "interval": 5}'\]

**Table and Aggregate Scan**
This scans a random table and aggregates the results every 2 minutes:

run\_fullscan: \['{"mode": "table\_and\_aggregate", "ks\_cf": "random", "interval": 120}'\]

**Partition Scan**
This scans the `scylla_bench.test` table's partitions every 5 minutes, focusing on specific partition key ranges:

run\_fullscan: \['{"mode": "partition", "ks\_cf": "scylla\_bench.test", "interval": 300, "pk\_name":"pk", "rows\_count": 5555, "validate\_data": true}'\]

**Key Considerations**

* Adjust the `interval` depending on the duration and intensity of your test. For short tests, smaller intervals (e.g., 5 minutes) might be more appropriate, while longer tests could use larger intervals.
* The `ks_cf` (keyspace.column\_family) should be chosen carefully based on the keyspaces and tables involved in the test.

**Example scan output**

```plaintext
c:FullPartitionScanOperation p:DEBUG > Randomly formed normal query is: select pk,ck from scylla_bench.test where pk = 437 and ck < 178089 and ck > 122918
c:FullPartitionScanOperation p:DEBUG > [scan: 118, type: lt_and_gt] Randomly formed reversed query is: select pk,ck from scylla_bench.test where pk = 437 and ck < 178089 and ck > 122918 order by ck asc limit 46728
c:FullPartitionScanOperation p:DEBUG > Will run command "select pk,ck from scylla_bench.test where pk = 437 and ck < 178089 and ck > 122918 order by ck asc limit 46728 "
c:FullPartitionScanOperation p:DEBUG > Will fetch up to 100 result pages.."
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 0
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 1
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 2
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 3
c:FullPartitionScanOperation p:DEBUG > Fetched a total of 4 pages
c:FullPartitionScanOperation p:DEBUG > Average lt_and_gt scans duration of 17 executions is: 233.56022893681245
c:FullPartitionScanOperation p:DEBUG > Executing the normal query: select pk,ck from scylla_bench.test where pk = 437 and ck < 178089 and ck > 122918
c:FullPartitionScanOperation p:DEBUG > Will run command "select pk,ck from scylla_bench.test where pk = 437 and ck < 178089 and ck > 122918 "
c:FullPartitionScanOperation p:DEBUG > Will fetch up to 100 result pages.."
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 0
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 1
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 2
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 3
c:PagedResultHandler   p:DEBUG > Will fetch the next page: 4
c:FullPartitionScanOperation p:DEBUG > Fetched a total of 5 pages
c:FullPartitionScanOperation p:DEBUG > Comparing scan queries output files by: diff -y --suppress-common-lines  /tmp/tmp6h_exdfn /tmp/tmpt77xta5n
c:FullPartitionScanOperation p:WARNING > Normal and reversed queries output differs: output results in /home/ubuntu/sct-results/20241028-071611-231345/fullscans/partition_range_scan_diff_2024_10_28-12_42_54.log
c:FullPartitionScanOperation p:DEBUG > ls -alh  /tmp/tmp6h_exdfn /tmp/tmpt77xta5n command output is:
c:FullPartitionScanOperation p:DEBUG > -rw------- 1 ubuntu ubuntu 457K Oct 28 12:42 /tmp/tmp6h_exdfn
c:FullPartitionScanOperation p:DEBUG > -rw------- 1 ubuntu ubuntu    0 Oct 28 12:37 /tmp/tmpt77xta5n
c:FullPartitionScanOperation p:DEBUG > head  /tmp/tmp6h_exdfn /tmp/tmpt77xta5n command output is:
c:FullPartitionScanOperation p:DEBUG > ==> /tmp/tmp6h_exdfn <==
c:FullPartitionScanOperation p:DEBUG > 437178088
c:FullPartitionScanOperation p:DEBUG > 437178087
c:FullPartitionScanOperation p:DEBUG > 437178086
c:FullPartitionScanOperation p:DEBUG > 437178085
c:FullPartitionScanOperation p:DEBUG > 437178084
c:FullPartitionScanOperation p:DEBUG > 437178083
c:FullPartitionScanOperation p:DEBUG > 437178082
c:FullPartitionScanOperation p:DEBUG > 437178081
c:FullPartitionScanOperation p:DEBUG > 437178080
c:FullPartitionScanOperation p:DEBUG > 437178079
c:FullPartitionScanOperation p:DEBUG >
c:FullPartitionScanOperation p:DEBUG > ==> /tmp/tmpt77xta5n <==
```

**Scan Statistics**

* A statistic class (OperationThreadStats) collects and formats scan result statistics in a table.

* Example statistics output:

```plaintext
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > Thread stats:
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > +-------------------------------------+---------------------+------------+------------------+----------------+---------+------------------------------------
----------------------------------------------------------------------------+
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > |               op_type               |       duration      | exceptions | nemesis_at_start | nemesis_at_end | success |                                                      cmd                                                       |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > +-------------------------------------+---------------------+------------+------------------+----------------+---------+----------------------------------------------------------------------------------------------------------------+
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > | FullPartitionScanReversedOrderEvent |  0.286191463470459  |            |       None       |      None      |   True  |                select pk,ck from scylla_bench.test where pk = 167 order by ck asc limit 138231                 |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > |        FullPartitionScanEvent       | 0.20973515510559082 |            |       None       |      None      |   True  |                              select pk,ck from scylla_bench.test where pk = 167                                |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > | FullPartitionScanReversedOrderEvent |  1.1589272022247314 |            |       None       |      None      |   True  | select pk,ck from scylla_bench.test where pk = 577 and ck < 117341 and ck > 77752 order by ck asc limit 33343  |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > |        FullPartitionScanEvent       |  1.1932871341705322 |            |       None       |      None      |   True  |               select pk,ck from scylla_bench.test where pk = 577 and ck < 117341 and ck > 77752                |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > | FullPartitionScanReversedOrderEvent |  18.678792715072632 |            |       None       |      None      |   True  |              select pk,ck from scylla_bench.test where pk = 1007 and ck > 66456 order by ck asc                |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > |        FullPartitionScanEvent       |  19.784258604049683 |            |       None       |      None      |   True  |                      select pk,ck from scylla_bench.test where pk = 1007 and ck > 66456                        |
< t:2024-10-28 07:49:01,702 f:operations_thread.py l:156  c:ScanOperationThread  p:DEBUG > +-------------------------------------+---------------------+------------+------------------+----------------+---------+----------------------------------------------------------------------------------------------------------------+
```
