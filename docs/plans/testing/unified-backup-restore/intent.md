# One configurable test for backup and restore

**Jira:** SCYLLADB-4542

## Problem

The backup and restore scenario of a test is fixed in code. To change the
method, the dataset, or the number of backups, an author must write a new
test method, add options, and add jobs.

SCT has two sets of backup and restore tests, and they do not share code:

- The Scylla Manager benchmarks in `mgmt_cli_test.py`
  (`ManagerHelperTests.test_prepare_backup_snapshot`,
  `ManagerRestoreBenchmarkTests`). They cover the `rclone` and the `native`
  methods of `sctool`.
- The Scylla API tests in `scylla_api_backup_restore_tests.py` on the
  `single-copy` and `frequent-backups` branches. They cover the cluster-wide
  single copy backup and the tablet-aware restore. An earlier version of
  `single-copy` also had a per node backup. The SCYLLADB-3585 table below
  shows its results.

The result:

- A comparison of the Manager methods and the Scylla API methods on one
  dataset needs two test files, two sets of options, and two sets of jobs.
  Today the comparison is put together by hand from separate runs.
- The frequent backup scenario is one more test method,
  `test_frequent_backups`. It writes new rows between the backups only. It
  cannot run a backup while a load continues, and it cannot overwrite
  existing rows. The Frequent Backups test plan needs both.
- The dataset is always one keyspace, `keyspace1`. The Frequent Backups test
  plan needs 10 keyspaces.
- Only the last backup of a frequent backup run gets a snapshot entry. A
  restore of an earlier backup is not possible.
- A frequent backup run cannot use Scylla Manager. When Manager supports
  frequent backups, each test needs a new method again.
- The Scylla API tests inherit `ManagerTestFunctionsMixIn` and reuse the
  `mgmt_*` options, but they do not use Manager.
  `user_profile_table_count`, a generic longevity option, sets the number
  of tables.
- A snapshot entry does not record which method made it. SCT cannot find
  a restore method that cannot read the backup before it provisions the
  cluster.
- The backup and restore code is in test classes that need a `ClusterTester`.
  A nemesis cannot use it, as `MgmtBackup` uses the Manager backup today.

## Who it affects

- The storage and Manager teams, who compare the backup and restore methods
  for the single copy epic (SCYLLADB-2465) and the frequent backups epic
  (SCYLLADB-3585).
- The owner of the Frequent Backups test plan. Its test tasks wait on this
  change.
- The owners of the Tablets Aware Restore and the Single Copy test plans,
  which compare the tablet-aware restore with `native` and `rclone`.
- Every author of a new backup or restore method. Scylla Manager will drive
  the cluster-wide backup and the tablet-aware restore later (SCYLLADB-5774,
  CLOUD-3460).

## Evidence

The Scylla API tests on the `single-copy` branch, commit `9b2c6fe70`:

```
scylla_api_backup_restore_tests.py:47:class ScyllaApiBackupRestoreTests(ManagerTestFunctionsMixIn):
scylla_api_backup_restore_tests.py:569:    def test_backup(self):
scylla_api_backup_restore_tests.py:577:    def test_restore(self):
scylla_api_backup_restore_tests.py:594:    def test_abort_backup_restore(self):
```

The `frequent-backups` branch, commit `90e34d04b`, adds a test method, two
options, a test-case file, and a job for one more scenario:

```
scylla_api_backup_restore_tests.py:655:    def test_frequent_backups(self):
```

```
mgmt_frequent_backup_cycles: int = SctField(
    ge=1,
    description="Number of `mgmt_frequent_backup_chunk_size` GB chunks of new data written by the Scylla API "
    "frequent backups test, each followed by incremental repair and a backup",
)
```

```
test-cases/manager/scylla_api_frequent_backup.yaml
jenkins-pipelines/manager/scylla_api_backup_restore/frequent-backup.jenkinsfile
```

The keyspace is a constant, and the table count is a generic option:

```
scylla_api_backup_restore_tests.py:45:KEYSPACE = "keyspace1"  # of TABLE_PROFILE_TEMPLATE
scylla_api_backup_restore_tests.py:88:        tables = [f"standard{i}" for i in range(1, self.params.get("user_profile_table_count") + 1)]
```

Only the last backup of `test_frequent_backups` gets a snapshot entry:

```
        for cycle in range(1, self.params.get("mgmt_frequent_backup_cycles") + 1):
            self.log.info("Write chunk %s of %s GB", cycle, chunk_size)
            self.run_and_verify_stress_in_threads(
                cs_cmds=self._cs_write_cmds(last_row + 1, last_row + chunk_rows), stop_on_failure=True
            )
            last_row += chunk_rows
            tag, backup_time = self._backup_cycle(f"cycle_{cycle}", prefix)
        self._send_snapshot_details(tag, prefix, backup_time, num_of_rows=last_row)
```

The snapshot entry has no field for the method that made it:

```
        entry = {
            "tag": tag,
            "locations": [self.location],
            "prefix": prefix,
            "exp_timeout": HOUR_IN_SEC * 8,
            "scylla_version": self.api_node.scylla_version,
            "number_of_nodes": len(self.db_cluster.data_nodes),
            "prohibit_verification_read": False,
            "dataset": {
                "schema": {ks: [{table: table_size} for table in ks_tables] for ks, ks_tables in tables.items()},
                "num_of_rows": num_of_rows,  # per table, read by the verification
            },
        }
```

The Manager benchmarks on master:

```
mgmt_cli_test.py:1067:class ManagerHelperTests(ManagerTestFunctionsMixIn):
mgmt_cli_test.py:1082:    def test_prepare_backup_snapshot(self):  # pylint: disable=too-many-locals  # noqa: PLR0914
mgmt_cli_test.py:1325:class ManagerRestoreBenchmarkTests(ManagerTestFunctionsMixIn):
```

The Manager backup nemesis on master:

```
sdcm/nemesis/monkey/manager.py:319:class MgmtBackup(ManagerMonkeyBase):
```

The cross-method comparison, put together by hand from separate runs.
SCYLLADB-3585, comment of 2026-10-29:

```
| Method                | Repair % | Size on Disk | Size on Bucket | Duration |
|-----------------------|----------|--------------|----------------|----------|
| API - cluster backup  |      100 |        3,138 |        1049.24 | 00:25:00 |
| API - cluster backup  |       50 |        3,115 |        2070.62 | 01:18:40 |
| API - cluster backup  |        0 |        3,032 |        3033.18 | 01:32:55 |
| API - per node backup |        0 |        3,050 |        3096.88 | 01:35:25 |
| Scylla Manager        |        0 |        3,029 |        3023.95 | 01:36:59 |
```

The frequent backup test tasks wait on this change. Each one "is blocked
by" SCYLLADB-4542:

```
SCYLLADB-4559  Blocked  [Test] Frequent backup under continuous load
SCYLLADB-4560  Blocked  Frequent backup of multiple tables
SCYLLADB-4561  Blocked  [Test] Frequent backup listing and restore of one backup
SCYLLADB-4562  Blocked  [Test] Mixed-instance-type frequent backup
```

Scylla Manager does not drive frequent backups yet:

```
CLOUD-3460  New  Integrate frequent backups with manager
```

The
[Frequent Backups Test Plan](https://scylladb.atlassian.net/wiki/spaces/RND/pages/450461715/Frequent+Backups+Test+Plan)
needs a load that overwrites rows and runs during the backups:

> Generate representative load by overwriting or updating approximately 100
> GB ("delta-new-data-size") of data between backup cycles.

> Start continuous background `cassandra-stress` mixed workload.

A load that writes new rows before the backup:

> writing a fixed additional new delta-data of 100GB just once. (not a
> background load, not updates of existing partitions).

A dataset in many keyspaces:

> the initial dataset of 500GB is spread across all 10 tables. - Writing to
> 10 different keyspaces.

A restore of one backup of the cycle:

> Select the last (third) backup. Restore it (schema + data), using
> Tablet-Aware mode.

> Validate the restored data size is 600GB (500GB initial dataset + 100GB
> delta-new-data)

The measurements:

> Backup duration per cycle
>
> Uploaded backup data size

The method pairs that cannot work, from
[Integration with tablet aware backups](https://scylladb.atlassian.net/wiki/spaces/RND/pages/439058455/Integration+with+tablet+aware+backups):

> Backups made with scylla tablet aware backups can be restored only with
> tablet aware restore (rclone and native restore rely on knowing the backup
> dir layout)

The plans that use the same pieces:
[Tablets Aware Restore Test Plan](https://scylladb.atlassian.net/wiki/spaces/RND/pages/318963720/Tablets+Aware+Restore+Test+Plan),
[Single Copy Backup/Restore Test Plan Document](https://scylladb.atlassian.net/wiki/spaces/RND/pages/446431593/Single+Copy+Backup+Restore+Test+Plan+Document).

## Definition of done

Method:

- The configuration selects the backup method: Manager `rclone`, Manager
  `native`, Scylla cluster-wide, or Scylla per node.
- The configuration selects the restore method: Manager `rclone`, Manager
  `native`, or Scylla tablet-aware.
- A new method, for example Manager frequent backups, needs no new test
  method and no new test-case file.
- Two jobs that differ only in the method use the same test name and the
  same test-case file. When both methods can read one snapshot, they use
  the same snapshot.

Backup cycles:

- The configuration selects the number of backups and the size of the data
  written before each one. All the backups go to the same location. For
  example, a list of sizes: `[500, 100, 100]` writes 500 GB and backs it
  up, then two times writes 100 GB and backs up again. `[500]` is one
  backup.
- The configuration selects how the data after the first write changes:
  new rows, or overwrites of existing rows.
- The configuration selects when this data is written: before each backup,
  or by a continuous load at a fixed rate. With a continuous load, a backup
  starts when the load has written the size of the cycle, and the load
  continues during the backup.
- The configuration selects whether an incremental repair runs before each
  backup.

Dataset:

- The configuration selects the number of keyspaces, the number of tables,
  and the repaired share of the first write. The data is split evenly
  between the tables.
- A new dataset shape needs no new test method.

Results:

- For each backup, the run reports the backup duration, the repair
  duration, the uploaded size, the size on the bucket, and the data written
  since the previous backup.
- For each restore, the run reports the restore duration and the repair
  duration after it.
- Every method reports to the same Argus tables, so the results of the
  methods compare side by side.

Snapshots and restore:

- Each backup of a cycle gets its own snapshot entry. The entry records the
  method that made it.
- A restore can restore any of these snapshots, for example the third one,
  and checks the data that the snapshot holds.
- A restore without a snapshot name does a round trip: write, backup,
  truncate, restore.
- A restore can check the rows on each node with CL=ONE reads.
- A test aborts a running backup and a running restore, and checks that
  both complete when they run again.

Configuration:

- A method pair that cannot work, or a method on a backend that it does not
  support, fails when SCT loads the configuration, before it provisions a
  resource. `lint-pipelines` reports the same failure.
- The new test uses new options, for all methods, to select the method,
  the dataset, the backup cycles, and the snapshot. It does not read the
  `mgmt_*` options or `user_profile_table_count` for them. The Manager
  setup options, such as `mgmt_docker_image`, stay.
- A run that uses only the Scylla API needs no Manager server.
- The test runs on AWS and GCE.

Code:

- The backup and restore code does not need a `ClusterTester`. A nemesis can
  use it, as `MgmtBackup` uses the Manager backup today.
- The existing Manager tests, jobs, and options work as today. The new test
  duplicates their code where it needs to.

## Out of scope

- Frequent backups driven by Scylla Manager, and the listing of backups by
  Manager (CLOUD-3460, SCYLLADB-5774). The method selection must allow them
  later.
- A nemesis that runs backups, and frequent backups during topology changes
  or disruptive nemeses (SCYLLADB-4563, SCYLLADB-4564).
- A cluster with mixed instance types in one DC. SCYLLADB-4562 needs it.
- The move of the existing Manager tests and jobs to the new test, and the
  removal of the old tests.
- OCI and Azure.
- Multi-DC backup locations (SCYLLADB-225, SCYLLADB-4546).
- A restore under a background load.
- The tablet-aware restore of a backup in the Manager layout.
- The `nodetool refresh` restore and the 1-1 restore.
- Scylla Cloud Manager.
- The Manager tests that are not benchmarks: purge, WORM, rate limit,
  ENOSPC, alternator. They stay in `mgmt_cli_test.py`.
