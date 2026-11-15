#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import json
import string
import tempfile
import textwrap
import threading
import time
from functools import cached_property
from pathlib import Path
from typing import Literal

import yaml

from sdcm import sct_abs_path, wait
from sdcm.argus_results import (
    ManagerRestoreBenchmarkResult,
    send_manager_benchmark_results_to_argus,
    send_manager_snapshot_details_to_argus,
)
from sdcm.cluster import HOUR_IN_SEC
from sdcm.mgmt.operations import ManagerTestFunctionsMixIn, SnapshotData
from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.cluster_tools import flush_nodes
from sdcm.utils.replication_strategy_utils import ReplicationStrategy
from sdcm.utils.time_utils import ExecutionTimer

TASK_TIMEOUT = 200000
TABLE_PROFILE_TEMPLATE = "data_dir/cs_scylla_api_backup_restore_table.yaml"
KEYSPACE = "keyspace1"  # of TABLE_PROFILE_TEMPLATE
CS_THREADS_PER_LOADER = 500


class ScyllaApiBackupRestoreTests(ManagerTestFunctionsMixIn):
    """Single copy (tablet-aware, deduplicated) backup and restore, driven via Scylla REST API only.

    Backup tests write `mgmt_prepare_snapshot_size` GB, `mgmt_backup_repaired_percent` of it repaired by incremental
    repair, so the backup holds repaired sstables (uploaded from one replica only) and unrepaired ones (uploaded from
    every replica). The data is split between `user_profile_table_count` tables of the c-s user profile
    `TABLE_PROFILE_TEMPLATE`. They log the snapshot entry for `defaults/manager_restore_benchmark_snapshots.yaml`.
    `test_restore` restores the snapshot set in `mgmt_reuse_backup_snapshot_name`.
    Requires a Scylla build with cluster backup and tablet-aware restore of its layout (scylladb#32306).
    """

    @cached_property
    def api_node(self):
        return self.db_cluster.data_nodes[0]

    @cached_property
    def location(self) -> str:
        return self.locations[0]

    @cached_property
    def bucket(self) -> str:
        return self.location.split(":")[-1]

    @property
    def keyspaces(self) -> list[str]:
        return self.db_cluster.get_test_keyspaces()

    @cached_property
    def snapshot(self) -> SnapshotData:
        snapshot_name = self.params.get("mgmt_reuse_backup_snapshot_name")
        assert snapshot_name, "`test_restore` requires `mgmt_reuse_backup_snapshot_name`"
        snapshot = self.get_snapshot_data(snapshot_name)
        assert snapshot.prefix, f"Snapshot {snapshot_name} has no `prefix`, it is not a Scylla API backup"
        return snapshot

    @cached_property
    def table_profiles(self) -> dict[str, str]:
        """C-S user profile file of each of the `user_profile_table_count` tables written by backup tests."""
        # named like the default cassandra-stress table
        tables = [f"standard{i}" for i in range(1, self.params.get("user_profile_table_count") + 1)]
        return {table: self._write_table_profile(table) for table in tables}

    @cached_property
    def rows_per_table(self) -> int:
        """`mgmt_prepare_snapshot_size` GB of 1 KB rows, split evenly between `user_profile_table_count` tables."""
        size = self.params.get("mgmt_prepare_snapshot_size")
        assert size, "Backup tests require `mgmt_prepare_snapshot_size`"
        return size * 1024 * 1024 // self.params.get("user_profile_table_count")

    def _table_profile(self, table: str) -> str:
        """C-S user profile of a `TABLE_PROFILE_TEMPLATE` table."""
        template = string.Template(Path(sct_abs_path(TABLE_PROFILE_TEMPLATE)).read_text(encoding="utf-8"))
        return template.substitute(table_name=table)

    def _write_table_profile(self, table: str) -> str:
        # stress thread uploads a profile from /tmp to the same path on the loader, see `get_profile_content`
        with tempfile.NamedTemporaryFile(
            mode="w", dir="/tmp", prefix=f"cs_scylla_api_{table}_", suffix=".yaml", delete=False, encoding="utf-8"
        ) as profile:
            profile.write(self._table_profile(table))
        return profile.name

    def _create_tables(self, tables: list[str]) -> None:
        """Create `TABLE_PROFILE_TEMPLATE` tables with CQL, before any c-s command writes them."""
        with self.db_cluster.cql_connection_patient(self.api_node) as session:
            for table in tables:
                profile = yaml.safe_load(self._table_profile(table))
                session.execute(profile["keyspace_definition"])
                session.execute(profile["table_definition"])

    def _cs_threads(self, cmd_count: int) -> int:
        """Threads per c-s command, about `CS_THREADS_PER_LOADER` per loader in total (c-s commands go round robin)."""
        return max(1, CS_THREADS_PER_LOADER * self._get_total_loaders() // cmd_count)

    def _scylla_api(self, method: Literal["GET", "POST"], path: str, params: dict | None = None, data=None, node=None):
        """Call Scylla REST API, `path` starts with the API endpoint, e.g. `storage_service/tablets/backup`."""
        endpoint, _, path = path.partition("/")
        client = RemoteCurlClient(host="localhost:10000", endpoint=endpoint, node=node or self.api_node)
        stdout = client.run_remoter_curl(method=method, path=path, params=params, data=data, retry=3).stdout.strip()
        if not stdout:
            return None
        # curl retries on HTTP 500 and concatenates the error responses, take the first one
        response, _ = json.JSONDecoder().raw_decode(stdout)
        # errors come back as {"message": ..., "code": ...} while curl exits with 0
        if isinstance(response, dict) and {"message", "code"} <= response.keys():
            raise AssertionError(f"Scylla API {method} {endpoint}/{path} {params} failed: {response}")
        return response

    def _get_task_status(self, task_id: str, node=None) -> dict:
        status = self._scylla_api("GET", f"task_manager/task_status/{task_id}", node=node)
        self.log.debug("Task %s status: %s", task_id, status)
        return status

    def _wait_for_tasks(self, tasks: dict, timeout: int = TASK_TIMEOUT, step: int = 30) -> dict[str, dict]:
        """Wait for the tasks (task id -> node, None for `api_node`) to finish, return their final statuses.

        All tasks are polled together, as Scylla forgets a finished task after `user_task_ttl_in_seconds` (1h).
        """
        statuses = {}
        task_lost = threading.Event()

        def _all_finished():
            for task_id, node in tasks.items():
                if task_id in statuses:
                    continue
                try:
                    status = self._get_task_status(task_id, node=node)
                except AssertionError as error:
                    if "not found" in str(error):
                        task_lost.set()  # a lost task never shows up again, stop waiting and raise the error
                    raise
                if status["state"] in ("done", "failed"):
                    statuses[task_id] = status
            return len(statuses) == len(tasks)

        wait.wait_for(
            _all_finished,
            step=step,
            timeout=timeout,
            stop_event=task_lost,
            text=f"Waiting for Scylla tasks {list(tasks)}",
        )
        return statuses

    def _assert_tasks_done(self, tasks: dict, timeout: int = TASK_TIMEOUT) -> dict[str, dict]:
        statuses = self._wait_for_tasks(tasks, timeout=timeout)
        failed = {task_id: status.get("error") for task_id, status in statuses.items() if status["state"] != "done"}
        assert not failed, f"Tasks failed: {failed}"
        return statuses

    def _wait_for_task(self, task_id: str, node=None, timeout: int = TASK_TIMEOUT, step: int = 30) -> dict:
        return self._wait_for_tasks({task_id: node}, timeout=timeout, step=step)[task_id]

    def _assert_task_done(self, task_id: str, node=None, timeout: int = TASK_TIMEOUT) -> dict:
        return self._assert_tasks_done({task_id: node}, timeout=timeout)[task_id]

    def _report_error(self, message: str) -> None:
        """Publish an error event: fails the test without stopping it, so the following checks still run."""
        TestFrameworkEvent(source=self.__class__.__name__, message=message).publish()

    def _abort_task_in_progress(self, task_id: str) -> None:
        """Wait until the task makes progress, abort it and check it stops early with an abort error."""

        def _in_progress_or_finished_status():
            status = self._get_task_status(task_id)
            finished = status["state"] in ("done", "failed")
            return status if finished or status["progress_completed"] > 0 else None

        status = wait.wait_for(
            _in_progress_or_finished_status,
            step=5,
            timeout=HOUR_IN_SEC,
            text=f"Waiting for task {task_id} to make progress",
        )
        assert status["state"] == "running", f"Task {task_id} finished before abort: {status}"
        assert status["is_abortable"], f"Task {task_id} is not abortable: {status}"

        self.log.info(
            "Abort task %s at progress %s/%s", task_id, status["progress_completed"], status["progress_total"]
        )
        self._scylla_api("POST", f"task_manager/abort_task/{task_id}")

        status = self._wait_for_task(task_id, timeout=HOUR_IN_SEC, step=5)
        if status["state"] != "failed":
            self._report_error(f"Task {task_id} is expected to fail after abort: {status}")
        # not just "abort": a backup error starts with "Abortable task wait failed:" whatever the cause
        elif not any(cause in status["error"].lower() for cause in ("abort requested", "aborted")):
            self._report_error(f"Task {task_id} failed with unexpected error: {status['error']}")
        if status["progress_completed"] >= status["progress_total"]:
            self._report_error(f"Task {task_id} completed despite abort: {status}")

    def _get_object_storage_endpoint(self) -> str:
        """Name of the object storage endpoint configured in scylla.yaml."""
        endpoints = (self.params.get("append_scylla_yaml") or {}).get("object_storage_endpoints")
        assert endpoints, "Backup to object storage requires `object_storage_endpoints` in `append_scylla_yaml`"
        # `{region}` is filled with the first test region when writing scylla.yaml, see `sdcm/cluster.py`
        return endpoints[0]["name"].format(region=next(iter(self.params.region_names), ""))

    def _get_tables(self, keyspace: str) -> list[str]:
        with self.db_cluster.cql_connection_patient(self.api_node) as session:
            rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", (keyspace,))
            return [row.table_name for row in rows]

    def _get_rf(self, keyspace: str) -> int:
        return sum(ReplicationStrategy.get(self.api_node, keyspace).replication_factors)

    def _new_prefix(self, name: str) -> str:
        return f"scylla_api_backup/{self.test_config.test_id()}/{name}"

    def _get_prefix_objects(self, prefix: str) -> dict[str, int] | None:
        """Size of each object under the prefix, by its path relative to the prefix. None if the provider is not supported."""
        provider = self.location.split(":")[-2]
        if (lister := self._bucket_object_listers.get(provider)) is None:
            self.log.info("Listing bucket objects is not implemented for '%s' backend, skipping", provider)
            return None
        return {key.removeprefix(f"{prefix}/"): size for key, size in lister(self.bucket, [f"{prefix}/"])}

    def _get_prefix_size_on_bucket(self, prefix: str) -> float | None:
        """Total size of objects under the prefix, in GiB. None if the provider is not supported."""
        if (objects := self._get_prefix_objects(prefix)) is None:
            return None
        size_gib = round(sum(objects.values()) / 1024**3, 3)
        self.log.info("Size on bucket under %s: %s GiB", prefix, size_gib)
        return size_gib

    def _check_aborted_backup_is_partial(self, aborted_prefix: str, full_prefix: str) -> None:
        """Compare the objects of an aborted backup with a full backup of the same data, taken after it.

        The aborted backup must have uploaded no snapshot metadata (manifests) and only part of the sstables,
        each of them complete, i.e. present in the full backup with the same size.
        """
        if (aborted := self._get_prefix_objects(aborted_prefix)) is None:
            return

        def sstable_components(objects: dict[str, int]) -> dict[str, int]:
            # skip the empty `sstables/<id>/refs/snapshot-<tag>/<gen>` objects, named after each backup's tag
            return {key: size for key, size in objects.items() if key.startswith("sstables/") and "/refs/" not in key}

        aborted_sstables = sstable_components(aborted)
        full_sstables = sstable_components(self._get_prefix_objects(full_prefix))
        self.log.info(
            "Aborted backup uploaded %s sstable objects of %s GiB, the full backup has %s of %s GiB",
            len(aborted_sstables),
            round(sum(aborted_sstables.values()) / 1024**3, 3),
            len(full_sstables),
            round(sum(full_sstables.values()) / 1024**3, 3),
        )
        if metadata := sorted(key for key in aborted if not key.startswith("sstables/")):
            self._report_error(f"Aborted backup uploaded snapshot metadata: {metadata}")
        if unexpected := sorted(aborted_sstables.items() - full_sstables.items()):
            self._report_error(
                f"Aborted backup uploaded {len(unexpected)} sstable objects missing from the full backup "
                f"or of different size, e.g. {unexpected[:5]}"
            )
        if len(aborted_sstables) >= len(full_sstables):
            self._report_error(f"Aborted backup uploaded all {len(full_sstables)} sstable objects of the full backup")

    def _repair(self) -> None:
        for keyspace in self.keyspaces:
            self.api_node.run_nodetool(
                sub_cmd="cluster repair",
                args=f"--incremental-mode incremental {keyspace}",
                timeout=HOUR_IN_SEC * 6,
                long_running=True,
                retry=0,
            )

    def _assert_tablets_repaired(self) -> None:
        """Single copy dedup relies on incremental repair setting the per-tablet repair watermark."""
        with self.db_cluster.cql_connection_patient(self.api_node) as session:
            for keyspace in self.keyspaces:
                rows = list(
                    session.execute(
                        f"SELECT table_name, sstables_repaired_at FROM system.tablets "
                        f"WHERE keyspace_name = '{keyspace}' ALLOW FILTERING"
                    )
                )
                assert rows, f"Keyspace {keyspace} has no tablets, single copy backup requires tablets"
                unrepaired = {row.table_name for row in rows if not row.sstables_repaired_at}
                assert not unrepaired, f"Tablets of {keyspace} tables {unrepaired} have no repair watermark"

    def _repair_written_data(self) -> None:
        self.log.info("Run incremental repair")
        self._repair()
        self._assert_tablets_repaired()

    def _flush_and_wait_for_compactions(self) -> None:
        if not self.params.get("mgmt_flush_and_wait_for_compactions"):
            return
        # flush first, otherwise the backup snapshot flushes memtables and triggers new compactions
        for keyspace in self.keyspaces:
            flush_nodes(cluster=self.db_cluster, keyspace=keyspace)
        self.wait_no_compactions_running(n=240)

    def _write_data(self) -> None:
        """Write `mgmt_prepare_snapshot_size` GB, the first `mgmt_backup_repaired_percent` of it repaired.

        Data is split evenly between `user_profile_table_count` tables, created first, each one repaired the same way.
        Then, if `mgmt_flush_and_wait_for_compactions` is set, flush and wait for compactions to settle,
        so they don't slow down the backup.
        """
        size, num_of_rows = self.params.get("mgmt_prepare_snapshot_size"), self.rows_per_table
        repaired_percent = self.params.get("mgmt_backup_repaired_percent")
        self._create_tables(list(self.table_profiles))
        repaired_rows = num_of_rows * repaired_percent // 100
        if repaired_rows:
            self.log.info("Write %s%% of %s GB, to be repaired", repaired_percent, size)
            self.run_and_verify_stress_in_threads(cs_cmds=self._cs_write_cmds(1, repaired_rows), stop_on_failure=True)
            self._repair_written_data()
        if repaired_rows < num_of_rows:
            self.log.info("Write %s%% of %s GB, left unrepaired", 100 - repaired_percent, size)
            self.run_and_verify_stress_in_threads(
                cs_cmds=self._cs_write_cmds(repaired_rows + 1, num_of_rows), stop_on_failure=True
            )
        self._flush_and_wait_for_compactions()

    def _cs_write_cmds(self, first_row: int, last_row: int) -> list[str]:
        """C-S write commands of the rows (inclusive) of every table.

        The rows of a table are split between `loaders // tables` commands, so one table still uses every loader.
        """
        chunks = max(1, self._get_total_loaders() // len(self.table_profiles))
        threads = self._cs_threads(chunks * len(self.table_profiles))
        rows = last_row - first_row + 1
        cmds = []
        for profile in self.table_profiles.values():
            for chunk in range(chunks):
                start = first_row + rows * chunk // chunks
                end = first_row + rows * (chunk + 1) // chunks - 1
                cmds.append(
                    f"cassandra-stress user profile={profile} 'ops(insert=1)' cl=QUORUM n={end - start + 1} "
                    f"-mode cql3 native -rate threads={threads} -pop seq={start}..{end}"
                )
        return cmds

    def _start_cluster_backup(self, snapshot_tag: str, prefix: str) -> str:
        """Start cluster backup, the snapshot is created by Scylla. Return the task id."""
        endpoint = self._get_object_storage_endpoint()
        backup_location = [
            {"datacenter": dc, "endpoint": endpoint, "bucket": self.bucket, "prefix": prefix}
            for dc in sorted(self.get_all_dcs_names())
        ]
        # backup locations are read from the request body, despite the API doc listing them as a query param
        task_id = self._scylla_api(
            "POST",
            "storage_service/tablets/backup",
            params={"keyspace": ",".join(self.keyspaces), "snapshot": snapshot_tag},
            data=backup_location,
        )
        self.log.info("Cluster backup task %s started to %s/%s", task_id, self.bucket, prefix)
        return task_id

    def _cluster_backup(self, name: str = "backup") -> tuple[str, str, int]:
        """Run cluster backup to a new prefix. Return snapshot tag, prefix and duration in seconds."""
        snapshot_tag = f"sct_scylla_api_{name}_{int(time.time())}"
        prefix = self._new_prefix(name)
        with ExecutionTimer() as timer:
            self._assert_task_done(self._start_cluster_backup(snapshot_tag, prefix))
        duration = int(timer.duration.total_seconds())
        self.log.info("Cluster backup took %s seconds", duration)
        return snapshot_tag, prefix, duration

    def _clear_snapshots(self) -> None:
        """Clear all snapshots of the test keyspaces on every node, to free the disk."""
        for node in self.db_cluster.data_nodes:
            node.run_nodetool("clearsnapshot", args=" ".join(self.keyspaces))

    def _create_snapshot_schema(self) -> None:
        """Restore requires existing tables and the backup holds no schema: create them from the profile template."""
        keyspaces = list(self.snapshot.ks_tables_map)
        assert keyspaces == [KEYSPACE], f"Snapshot keyspaces {keyspaces} are not {KEYSPACE} of {TABLE_PROFILE_TEMPLATE}"
        self._create_tables(self.snapshot.ks_tables_map[KEYSPACE])

    def _start_restore(self, keyspace: str, table: str) -> str:
        endpoint = self._get_object_storage_endpoint()
        bucket = self.snapshot.locations[0].split(":")[-1]
        # manifest keys are full paths in the bucket, hence the empty prefix
        restore_location = [
            {
                "datacenter": dc,
                "endpoint": endpoint,
                "bucket": bucket,
                "prefix": "",
                "manifests": [f"{self.snapshot.prefix}/snapshots/{keyspace}/{table}/{self.snapshot.tag}/manifest.json"],
            }
            for dc in sorted(self.get_all_dcs_names())
        ]
        task_id = self._scylla_api(
            "POST",
            "storage_service/tablets/restore",
            params={"keyspace": keyspace, "table": table, "snapshot": self.snapshot.tag},
            data=restore_location,
        )
        self.log.info("Restore task %s of %s.%s started", task_id, keyspace, table)
        return task_id

    def _start_restore_all_tables(self) -> list[str]:
        return [
            self._start_restore(keyspace, table)
            for keyspace, tables in self.snapshot.ks_tables_map.items()
            for table in tables
        ]

    def _restore(self) -> int:
        """Start the restore of every snapshot table and wait for all of them. Return duration in seconds.

        Scylla runs the restores one table at a time, in no particular order.
        """
        with ExecutionTimer() as timer:
            task_ids = self._start_restore_all_tables()
            statuses = self._assert_tasks_done(dict.fromkeys(task_ids), timeout=self.snapshot.exp_timeout)
        for task_id, status in statuses.items():
            assert 0 < status["progress_completed"] == status["progress_total"], (
                f"Restore task {task_id} did not restore everything: {status}"
            )
        duration = int(timer.duration.total_seconds())
        self.log.info("Restore took %s seconds", duration)
        return duration

    def _verify_data_on_each_node(self) -> None:
        """Read and validate every table of the snapshot with CL=ONE from each node only, in parallel."""
        if self.snapshot.prohibit_verification_read:
            self.log.info("Skip verification read, prohibited by the snapshot")
            return
        num_of_rows = self.snapshot.dataset["num_of_rows"]  # per table
        profiles = [self._write_table_profile(table) for table in self.snapshot.ks_tables_map[KEYSPACE]]
        nodes = self.db_cluster.data_nodes
        threads = self._cs_threads(len(nodes) * len(profiles))
        stress_queue = [
            self.run_stress_thread(
                stress_cmd=f"cassandra-stress user profile={profile} 'ops(validate=1)' cl=ONE n={num_of_rows} "
                f"-mode cql3 native -rate threads={threads} -pop seq=1..{num_of_rows} "
                f"-node whitelist {node.cql_address}",
                round_robin=True,
                node_list=[node],
            )
            for node in nodes
            for profile in profiles
        ]
        for _thread in stress_queue:
            assert self.verify_stress_thread(_thread), "Stress thread verification failed"

    def _log_snapshot_entry(self, tag: str, prefix: str, size: int) -> SnapshotData:
        """Log the snapshot entry to add to `defaults/manager_restore_benchmark_snapshots.yaml`, return its data."""
        tables = {keyspace: self._get_tables(keyspace) for keyspace in self.keyspaces}
        # dataset size, single replica
        dataset_size = size // self._get_rf(self.keyspaces[0])
        table_size = dataset_size // sum(len(ks_tables) for ks_tables in tables.values())
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
                "num_of_rows": self.rows_per_table,  # per table, read by the verification
            },
        }
        name = f"scylla_api_{dataset_size}gb_{prefix.rsplit('/', 1)[-1]}_{int(time.time())}"
        self.log.info(
            "Snapshot entry for defaults/manager_restore_benchmark_snapshots.yaml (under `sizes:`):\n%s",
            textwrap.indent(yaml.safe_dump({name: entry}, sort_keys=False), "  "),
        )
        return SnapshotData(
            locations=entry["locations"],
            tag=tag,
            exp_timeout=entry["exp_timeout"],
            dataset=entry["dataset"],
            keyspaces=list(tables),
            ks_tables_map=tables,
            cs_read_cmd_template="",  # reads use the c-s user profile, see `_verify_data_on_each_node`
            prohibit_verification_read=False,
            node_ids=None,
            one_one_restore_params=None,
            prefix=prefix,
        )

    def _send_snapshot_details(self, tag: str, prefix: str, duration: int) -> SnapshotData:
        """Send the backup details to Argus and log its snapshot entry. Return the snapshot data."""
        size = self.get_keyspaces_disk_size_gb(self.keyspaces)
        snapshot = self._log_snapshot_entry(tag, prefix, size)
        snapshot_details = {
            "tag": tag,
            "size": size,
            "duration": duration,
            "locations": f"{self.location}/{prefix}",
            "ks_name": ",".join(self.keyspaces),
            "scylla_version": self.params.get_version_based_on_conf()[0],
        }
        if (size_on_bucket := self._get_prefix_size_on_bucket(prefix)) is not None:
            snapshot_details["size_on_bucket"] = size_on_bucket
        self.log.info("Send snapshot details to Argus: %s", snapshot_details)
        send_manager_snapshot_details_to_argus(
            argus_client=self.test_config.argus_client(), snapshot_details=snapshot_details
        )
        return snapshot

    def _post_restore_repair(self) -> int:
        """Run incremental repair after restore. Return duration in seconds."""
        self.log.info("Run post restore incremental repair")
        with ExecutionTimer() as timer:
            self._repair()
        return int(timer.duration.total_seconds())

    def _send_restore_results(self, restore_time: int, repair_time: int, label: str) -> None:
        results = {
            "restore time": restore_time,
            "repair time": repair_time,
            "total": restore_time + repair_time,
            # dataset size, single replica
            "size": self.get_keyspaces_disk_size_gb(self.keyspaces) // self._get_rf(self.keyspaces[0]),
        }
        self.log.info("Send restore results to Argus: %s", results)
        send_manager_benchmark_results_to_argus(
            argus_client=self.test_config.argus_client(),
            result=results,
            result_table=ManagerRestoreBenchmarkResult(),
            row_name=label,
        )

    def _restore_and_verify(self, label: str) -> None:
        """Restore all tables, run incremental repair, send results to Argus, read all data from each node."""
        restore_time = self._restore()
        repair_time = self._post_restore_repair()
        self._send_restore_results(restore_time, repair_time, label=label)
        self._verify_data_on_each_node()

    def test_backup(self):
        """Single copy backup: compare keyspace size on disk with size on bucket and measure backup duration.

        Repaired data (`mgmt_backup_repaired_percent`) is expected to be uploaded from one replica only,
        unrepaired data from every replica.
        """
        self._write_data()
        repaired_percent = self.params.get("mgmt_backup_repaired_percent")
        snapshot_tag, prefix, duration = self._cluster_backup(name=f"backup_{repaired_percent}_repaired")
        self._send_snapshot_details(snapshot_tag, prefix, duration)

    def test_restore(self):
        """Restore a single copy backup, verify every replica got the data.

        Steps:
        1. Create the schema of the snapshot.
        2. Restore all tables via `/storage_service/tablets/restore`.
        3. Run incremental repair and send restore results to Argus.
        4. Read all data with CL=ONE from each node only.
        """
        self._create_snapshot_schema()
        self._restore_and_verify(label="single copy")

    def test_abort_backup_restore(self):
        """Abort a running backup and a running restore of it, check both succeed when run again.

        Steps:
        1. Write the dataset, abort a backup mid-upload, run a full backup.
        2. Check the aborted backup uploaded part of the full backup's sstables and no snapshot metadata.
        3. Truncate the tables, restore the full backup and abort the restore of one table mid-download.
        4. Restore again without truncating: sstables downloaded by the aborted restore are marked in
           `system_distributed.snapshot_sstables` and skipped.
        5. Run incremental repair and read all data with CL=ONE from each node only.
        """
        self._write_data()

        snapshot_tag = f"sct_scylla_api_aborted_{int(time.time())}"
        aborted_prefix = self._new_prefix("aborted")
        self._abort_task_in_progress(self._start_cluster_backup(snapshot_tag, aborted_prefix))
        self._clear_snapshots()

        snapshot_tag, prefix, duration = self._cluster_backup()
        # restore this backup instead of `mgmt_reuse_backup_snapshot_name`
        self.snapshot = self._send_snapshot_details(snapshot_tag, prefix, duration)
        self._check_aborted_backup_is_partial(aborted_prefix, prefix)

        self.truncate_tables(ks_tables_map=self.snapshot.ks_tables_map)
        # backup and truncate snapshots hold the old sstables, free the disk for the restore
        self._clear_snapshots()
        task_ids = self._start_restore_all_tables()
        self._abort_task_in_progress(task_ids[0])
        self._wait_for_tasks(dict.fromkeys(task_ids[1:]), timeout=self.snapshot.exp_timeout)
        self._restore_and_verify(label="after abort")
