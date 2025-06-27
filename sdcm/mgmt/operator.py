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
# Copyright (c) 2021 ScyllaDB

import logging
from typing import List, Optional
from dataclasses import dataclass, asdict, fields

from sdcm.mgmt.cli import (
    BackupTask,
    HealthcheckTask,
    ManagerCluster,
    RepairTask,
    ScyllaManagerTool,
    ManagerTask,
)
from sdcm.mgmt.common import TaskStatus
from sdcm.wait import wait_for


LOGGER = logging.getLogger(__name__)


@dataclass
class BaseClass:
    @staticmethod
    def _keys_to_values(data: dict) -> dict:
        return {value: key for key, value in data.items() if value is not None}

    @staticmethod
    def _map_dict(data: dict, mapping: dict) -> dict:
        return {mapping[key]: value for key, value in data.items()
                if value is not None and mapping.get(key, None) is not None}

    def to_dict(self, remove_defaults=True) -> dict:
        result = asdict(self)
        if remove_defaults:
            return {key: val for key, val in result.items() if val is not None}
        return result

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        field_names = [field.name for field in fields(cls)]
        return cls(**{key: val for key, val in data.items() if key in field_names}, **kwargs)


@dataclass
class ScyllaOperatorTaskBaseClass(BaseClass):
    name: 'str'
    # StartDate specifies the task start date expressed in the RFC3339 format or now[+duration],
    # e.g. now+3d2h10m, valid units are d, h, m, s (default "now").
    start_date: str = None
    # Interval task schedule interval e.g. 3d2h10m, valid units are d, h, m, s (default "0").
    interval: str = None
    # crontab time to determine the start time/schedule of the task, like so: 5 0 * 8 *.
    # for more info: https://crontab.guru/
    cron: list = None
    # num_retries the number of times a scheduled task will retry to run before failing (default 3).
    num_retries: int = None


@dataclass
class ScyllaOperatorRepairTask(ScyllaOperatorTaskBaseClass):
    # DC list of datacenter glob patterns, e.g. 'dc1', '!otherdc*' used to specify the DCs
    # to include or exclude from backup.
    dc: List[str] = None
    # fail_fast stop repair on first error.
    fail_fast: bool = None
    # Intensity integer >= 1 or a decimal between (0,1), higher values may result in higher speed and cluster load.
    # 0 value means repair at maximum intensity.
    intensity: float = None
    # Parallel The maximum number of repair jobs to run in parallel, each node can participate in at most one repair
    # at any given time. Default is means system will repair at maximum parallelism.
    parallel: int = None
    # Keyspace a list of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'
    # used to include or exclude keyspaces from repair.
    keyspace: List[str] = None
    # small_table_threshold enable small table optimization for tables of size lower than given threshold.
    # Supported units [B, MiB, GiB, TiB] (default "1GiB").
    small_table_threshold: str = None


@dataclass
class ScyllaOperatorRepairTaskStatus(ScyllaOperatorRepairTask):
    # These statuses are not available at scylla-operator 1.0
    # You have to relay on task status in manager

    STARTED = 'STARTED'
    COMPLETED = 'COMPLETED'
    SCHEDULED = 'SCHEDULED'
    ERROR = 'ERROR'
    IN_PROGRESS = 'IN PROGRESS'

    error: str = None
    status: str = None
    name: str = None
    id: str = None


@dataclass
class ScyllaOperatorBackupTask(ScyllaOperatorTaskBaseClass):
    # DC list of datacenter glob patterns, e.g. 'dc1', '!otherdc*' used to specify the DCs
    # to include or exclude from backup.
    dc: List[str] = None
    # fail_fast stop repair on first error.
    fail_fast: bool = None
    # Intensity integer >= 1 or a decimal between (0,1), higher values may result in higher speed and cluster load.
    # 0 value means repair at maximum intensity.
    intensity: int = None
    # Parallel The maximum number of repair jobs to run in parallel, each node can participate in at most one repair
    # at any given time. Default is means system will repair at maximum parallelism.
    parallel: int = None
    # Keyspace a list of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'
    # used to include or exclude keyspaces from repair.
    keyspace: List[str] = None
    # small_table_threshold enable small table optimization for tables of size lower than given threshold.
    # Supported units [B, MiB, GiB, TiB] (default "1GiB").
    small_table_threshold: str = None
    # List of locations where backup is going to be stored, location is string in following format:
    # <provider>:<path> , where provider could be gcs or s3
    location: List[str] = None
    # RateLimit a list of megabytes (MiB) per second rate limits expressed in the format [<dc>:]<limit>.
    # The <dc>: part is optional and only needed when different datacenters need different upload limits.
    # Set to 0 for no limit (default 100).
    rate_limit: List[str] = None
    # Retention The number of backups which are to be stored (default 3).
    retention: int = None
    # SnapshotParallel a list of snapshot parallelism limits in the format [<dc>:]<limit>.
    # The <dc>: part is optional and allows for specifying different limits in selected datacenters.
    # If The <dc>: part is not set, the limit is global (e.g. 'dc1:2,5') the runs are parallel in n nodes (2 in dc1)
    # and n nodes in all the other datacenters.
    snapshot_parallel: List[str] = None
    # UploadParallel a list of upload parallelism limits in the format [<dc>:]<limit>.
    # The <dc>: part is optional and allows for specifying different limits in selected datacenters.
    # If The <dc>: part is not set the limit is global (e.g. 'dc1:2,5') the runs are parallel in n nodes (2 in dc1)
    # and n nodes in all the other datacenters.
    upload_parallel: List[str] = None


@dataclass
class ScyllaOperatorBackupTaskStatus(ScyllaOperatorBackupTask):
    # These statuses are not available at scylla-operator 1.0
    # You have to relay on task status in manager

    STARTED = 'STARTED'
    COMPLETED = 'COMPLETED'
    SCHEDULED = 'SCHEDULED'
    ERROR = 'ERROR'
    IN_PROGRESS = 'IN PROGRESS'

    error: str = None
    status: str = None
    name: str = None
    id: str = None


class OperatorManagerCluster(ManagerCluster):
    scylla_cluster = None
    _id = None

    def __init__(self, manager_node, cluster_id=None, client_encrypt=False,
                 cluster_name: str = None, scylla_cluster=None):
        self.cluster_name = cluster_name
        self.scylla_cluster = scylla_cluster
        super().__init__(manager_node=manager_node, cluster_id=cluster_id,
                         client_encrypt=client_encrypt)

    @staticmethod
    def _pick_original_name(basic_name, names):
        current_name = basic_name
        idx = 1
        while current_name in names:
            current_name = f'{basic_name}-{idx}'
            idx += 1
        return current_name

    @property
    def id(self):
        if self._id is None:
            self._id = wait_for(
                self.get_cluster_id_by_name, cluster_name=self.cluster_name, timeout=120,
                text='Waiting manager cluster to appear', throw_exc=True)
        return self._id

    @id.setter
    def id(self, value: str):
        self._id = value

    def set_scylla_cluster(self, scylla_cluster):
        self.scylla_cluster = scylla_cluster

    def set_cluster_name(self, name: str):
        self.cluster_name = name

    def are_healthchecks_done(self):
        # NOTE: operator has 3 healthchecks which we should check:
        # +---------------------------------+-----------+-------------------------------+--------+
        # | Task                            | Arguments | Next run                      | Status |
        # +---------------------------------+-----------+-------------------------------+--------+
        # | healthcheck/uuid-foo            |           | 06 May 22 12:27:45 UTC (+15s) | DONE   |
        # | healthcheck_alternator/uuid-bar |           | 06 May 22 12:27:45 UTC (+15s) | DONE   |
        # | healthcheck_rest/uuid-quuz      |           | 06 May 22 12:28:30 UTC (+1m)  | DONE   |
        # +---------------------------------+-----------+-------------------------------+--------+
        healthchecks = self._get_task_list_filtered(prefix="healthcheck", task_class=HealthcheckTask)
        return all((healthcheck.status == TaskStatus.DONE for healthcheck in healthchecks))

    def wait_for_healthchecks(self):
        wait_for(
            func=self.are_healthchecks_done,
            text="Waiting for the healthchecks to have 'DONE' status",
            step=3,
            timeout=300,
            throw_exc=True,
        )

    def _create_operator_backup_task(self, dc_list=None, interval=None, keyspace_list=None, location_list=None,  # noqa: PLR0913
                                     num_retries=None, rate_limit_list=None, retention=None, cron=None,
                                     snapshot_parallel_list=None, start_date=None, upload_parallel_list=None,
                                     name=None) -> ScyllaOperatorBackupTask:

        if name is None:
            name = self._pick_original_name(
                'default-backup-task-name', [so_task.name for so_task in self.operator_backup_tasks])

        so_backup_task = ScyllaOperatorBackupTask(
            name=name,
            dc=dc_list,
            interval=interval,
            keyspace=keyspace_list,
            cron=cron,
            location=location_list,
            num_retries=num_retries,
            rate_limit=rate_limit_list,
            retention=retention,
            snapshot_parallel=snapshot_parallel_list,
            start_date=start_date,
            upload_parallel=upload_parallel_list
        )
        try:
            self.scylla_cluster.add_scylla_cluster_value('/spec/backups', so_backup_task.to_dict())
        except Exception as exc:
            LOGGER.error('Failed to submit repair task:\n%s\ndue to the %s', so_backup_task.to_dict(), exc)
            raise
        return so_backup_task

    def create_backup_task(  # noqa: PLR0913
            self,
            dc_list=None,
            dry_run=None,
            interval=None,
            keyspace_list=None,
            cron=None,
            location_list=None,
            num_retries=None,
            rate_limit_list=None,
            retention=None,
            show_tables=None,
            snapshot_parallel_list=None,
            start_date=None,
            upload_parallel_list=None,
            legacy_args=None) -> BackupTask:

        # NOTE: wait for the 'healthcheck' tasks be 'DONE' before starting the backup one.
        self.wait_for_healthchecks()

        so_task = self._create_operator_backup_task(
            dc_list=dc_list,
            interval=interval,
            keyspace_list=keyspace_list,
            cron=cron,
            location_list=location_list,
            num_retries=num_retries,
            rate_limit_list=rate_limit_list,
            retention=retention,
            snapshot_parallel_list=snapshot_parallel_list,
            start_date=start_date,
            upload_parallel_list=upload_parallel_list,
        )
        return wait_for(lambda: self.get_mgr_backup_task(so_task), step=2, timeout=300)

    def _create_scylla_operator_repair_task(self, dc_list=None, keyspace=None, interval=None, num_retries=None,
                                            fail_fast=None, intensity=None, parallel=None,
                                            name=None) -> ScyllaOperatorRepairTask:

        # TODO: add support for the "ignore_down_hosts" manager parameter
        #       when following scylla-operator bug gets fixed:
        #       https://github.com/scylladb/scylla-operator/issues/2730

        if name is None:
            name = self._pick_original_name(
                'default-repair-task-name', [so_task.name for so_task in self.operator_repair_tasks])
        so_repair_task = ScyllaOperatorRepairTask(
            name=name,
            dc=dc_list,
            fail_fast=fail_fast,
            intensity=intensity,
            parallel=parallel,
            keyspace=keyspace,
            interval=interval,
            num_retries=num_retries,
        )
        try:
            self.scylla_cluster.add_scylla_cluster_value('/spec/repairs', so_repair_task.to_dict())
        except Exception as exc:
            LOGGER.error('Failed to submit repair task:\n%s\ndue to the %s', so_repair_task.to_dict(), exc)
            raise
        return so_repair_task

    def create_repair_task(self, dc_list=None,
                           keyspace=None, interval=None, num_retries=None, fail_fast=None,
                           intensity=None, parallel=None, name=None) -> RepairTask:
        # NOTE: wait for the 'healthcheck' tasks be 'DONE' before starting the repair one.
        self.wait_for_healthchecks()

        # TBD: After https://github.com/scylladb/scylla-operator/issues/272 is solved,
        #   replace RepairTask with ScyllaOperatorRepairTask and move relate logic there
        so_task = self._create_scylla_operator_repair_task(dc_list=dc_list, keyspace=keyspace, interval=interval,
                                                           num_retries=num_retries, fail_fast=fail_fast,
                                                           intensity=intensity, parallel=parallel, name=name)
        return wait_for(lambda: self.get_mgr_repair_task(so_task), step=2, timeout=300)

    def get_mgr_repair_task(self, so_repair_task: ScyllaOperatorRepairTask) -> Optional[RepairTask]:
        so_repair_task_status = wait_for(
            func=self.get_operator_repair_task_status,
            text=f"Waiting until operator repair task: {so_repair_task.name} get it's status",
            step=2,
            timeout=300,
            task_name=so_repair_task.name,
            throw_exc=True)
        for mgr_task in self.repair_task_list:
            if mgr_task.id.split("/", 1)[-1] in (so_repair_task_status.name, so_repair_task_status.id):
                return mgr_task
        return None

    def get_mgr_backup_task(self, so_backup_task: ScyllaOperatorBackupTask) -> Optional[BackupTask]:
        so_backup_task_status = wait_for(
            func=self.get_operator_backup_task_status,
            text=f"Waiting until operator backup task '{so_backup_task.name}' get it's status",
            step=2,
            timeout=600,
            task_name=so_backup_task.name,
            throw_exc=True)
        for mgr_task in self.backup_task_list:
            if mgr_task.id.split("/", 1)[-1] in (so_backup_task_status.name, so_backup_task_status.id):
                return mgr_task
        return None

    def get_operator_repair_task_status(self, task_name: str) -> Optional[ScyllaOperatorRepairTaskStatus]:
        for task_status in self.operator_repair_task_statuses:
            if task_status.name == task_name:
                return task_status
        return None

    def get_operator_backup_task_status(self, task_name: str) -> Optional[ScyllaOperatorBackupTaskStatus]:
        for task_status in self.operator_backup_task_statuses:
            if task_status.name == task_name:
                return task_status
        return None

    def _get_list_of_entities_from_operator(self, path, entity_class):
        repair_task_status_infos = self.scylla_cluster.get_scylla_cluster_value(path)
        if not repair_task_status_infos:
            return []
        return [entity_class.from_dict(task_status) for task_status in repair_task_status_infos]

    @property
    def operator_repair_task_statuses(self) -> List[ScyllaOperatorRepairTaskStatus]:
        return self._get_list_of_entities_from_operator('/status/repairs', ScyllaOperatorRepairTaskStatus)

    @property
    def operator_backup_task_statuses(self) -> List[ScyllaOperatorBackupTaskStatus]:
        return self._get_list_of_entities_from_operator('/status/backups', ScyllaOperatorBackupTaskStatus)

    @property
    def operator_repair_tasks(self) -> List[ScyllaOperatorRepairTask]:
        return self._get_list_of_entities_from_operator('/spec/repairs', ScyllaOperatorRepairTask)

    @property
    def operator_backup_tasks(self) -> List[ScyllaOperatorBackupTask]:
        return self._get_list_of_entities_from_operator('/spec/backups', ScyllaOperatorBackupTask)

    def update(self, name=None, host=None, client_encrypt=None, force_non_ssl_session_port=False):
        raise NotImplementedError()

    def delete_task(self, task: ManagerTask) -> None:
        LOGGER.debug("deleting task [%s - %s]", task.__class__.__name__, task.id)
        name = task.id.split('/')[-1]
        if isinstance(task, RepairTask):
            self.scylla_cluster.remove_scylla_cluster_value('/spec/repairs', element_name=name)
        if isinstance(task, BackupTask):
            self.scylla_cluster.remove_scylla_cluster_value('/spec/backups', element_name=name)


class ScyllaManagerToolOperator(ScyllaManagerTool):
    clusterClass = OperatorManagerCluster

    def _initial_wait(self, seconds: int):
        pass

    def get_cluster(self, cluster_name):
        cluster = super().get_cluster(cluster_name)
        if cluster is None:
            cluster = self.clusterClass(
                manager_node=self.manager_node, cluster_name=cluster_name, scylla_cluster=self.scylla_cluster)
        else:
            cluster.set_scylla_cluster(self.scylla_cluster)
            cluster.set_cluster_name(cluster_name)
        return cluster

    def __init__(self, manager_node, scylla_cluster):
        self.scylla_cluster = scylla_cluster
        super().__init__(manager_node)

    def rollback_upgrade(self, scylla_mgmt_address):
        raise NotImplementedError()

    def add_cluster(self, name, host=None, db_cluster=None, client_encrypt=None, disable_automatic_repair=True,
                    auth_token=None, credentials=None, force_non_ssl_session_port=False):
        raise NotImplementedError()

    def upgrade(self, scylla_mgmt_upgrade_to_repo):
        raise NotImplementedError()
