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


import json
import time
import logging
import datetime
import re
from pathlib import Path
from re import findall
from textwrap import dedent
from statistics import mean
from contextlib import contextmanager
from distutils.version import LooseVersion

import requests
from invoke.exceptions import Failure as InvokeFailure

from sdcm.remote.libssh2_client.exceptions import Failure as Libssh2Failure
from sdcm import wait
from sdcm.mgmt.common import \
    TaskStatus, ScyllaManagerError, HostStatus, HostSsl, HostRestStatus, duration_to_timedelta, DEFAULT_TASK_TIMEOUT
from sdcm.provision.helpers.certificate import TLSAssets
from sdcm.utils.context_managers import DbNodeLogger
from sdcm.wait import WaitForTimeoutError

LOGGER = logging.getLogger(__name__)

STATUS_DONE = 'done'
STATUS_ERROR = 'error'
SSL_CONF_DIR = Path('/tmp/ssl_conf')
SSL_USER_CERT_FILE = SSL_CONF_DIR / TLSAssets.CLIENT_CERT
SSL_USER_KEY_FILE = SSL_CONF_DIR / TLSAssets.CLIENT_KEY
REPAIR_TIMEOUT_SEC = 7200  # 2 hours


new_command_structure_minimum_version = LooseVersion("3.0")
forcing_tls_minimum_version = LooseVersion("3.2.6")

# TODO: remove these checks once manager 2.6 is no longer supported


class ScyllaManagerBase:

    def __init__(self, id, manager_node):
        self.id = id
        self.manager_node = manager_node
        self.sctool = SCTool(manager_node=manager_node)

    def get_property(self, parsed_table, column_name):
        return self.sctool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=self.id)


class ManagerTask:

    def __init__(self, task_id, cluster_id, manager_node):
        self.manager_node = manager_node
        self.sctool = SCTool(manager_node=manager_node)
        self.id = task_id
        self.cluster_id = cluster_id

    def get_property(self, parsed_table, column_name):
        return self.sctool.get_table_value(parsed_table=parsed_table, column_name=column_name, identifier=self.id)

    def stop(self):
        if self.sctool.is_v3_cli:
            cmd = "stop {} -c {}".format(self.id, self.cluster_id)
        else:
            cmd = "task stop {} -c {}".format(self.id, self.cluster_id)
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return self.wait_and_get_final_status(timeout=30, step=3)

    def start(self, continue_task=True):
        if self.sctool.is_v3_cli:
            cmd = "start {} -c {}".format(self.id, self.cluster_id)
        else:
            cmd = "task start {} -c {}".format(self.id, self.cluster_id)
        if not continue_task:
            cmd += " --no-continue"
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @staticmethod
    def _add_kwargs_to_cmd(cmd, **kwargs):
        for key, value in kwargs.items():
            cmd += ' --{}={}'.format(key, value)
        return cmd

    def get_task_info_dict(self):
        # Output example:
        # Name:      healthcheck/cql
        #             Cron:     @every 15s
        #             Tz:       UTC
        #
        #             Properties:
        #             - mode: cql
        #
        #             ╭──────────────────────────────────────┬────────────────────────┬──────────┬────────╮
        #             │ ID                                   │ Start time             │ Duration │ Status │
        #             ├──────────────────────────────────────┼────────────────────────┼──────────┼────────┤
        #             │ 13814000-1dd2-11b2-a009-02c33d089f9b │ 07 Jan 23 23:08:59 UTC │ 0s       │ DONE   │
        #             ╰──────────────────────────────────────┴────────────────────────┴──────────┴────────╯
        info_dict = {}
        cmd = "info {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        info_lines = [line[0] for line in res if len(line) == 1]
        for line in info_lines:
            if ":" in line:
                name, value = [string.strip() for string in line.split(":", maxsplit=1)]
                if name.startswith("-"):
                    name = name[2:]
                info_dict[name] = value
        history_table_lines = [line for line in res if len(line) > 1]
        # The info command returns some unnecessary values: task_name, cron, retry
        # The number of the extra info is not set, so I just search the for the lines that its length is larger than
        # 1, which are the history table (See above)
        info_dict["history"] = history_table_lines
        return info_dict

    @property
    def history(self):
        """
        Gets the task's history table
        """
        # ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬───────╮
        # │ id                                   │ start time             │ end time               │ duration │ status│
        # ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼───────┤
        # │ e4f70414-ebe7-11e8-82c4-12c0dad619c2 │ 19 Nov 18 10:43:04 UTC │ 19 Nov 18 10:43:04 UTC │ 0s       │ NEW   │
        # │ 7f564891-ebe6-11e8-82c3-12c0dad619c2 │ 19 Nov 18 10:33:04 UTC │ 19 Nov 18 10:33:04 UTC │ 0s       │ NEW   │
        # │ 19b58cb3-ebe5-11e8-82c2-12c0dad619c2 │ 19 Nov 18 10:23:04 UTC │ 19 Nov 18 10:23:04 UTC │ 0s       │ NEW   │
        # │ b414cde5-ebe3-11e8-82c1-12c0dad619c2 │ 19 Nov 18 10:13:04 UTC │ 19 Nov 18 10:13:04 UTC │ 0s       │ NEW   │
        # │ 4e741c3d-ebe2-11e8-82c0-12c0dad619c2 │ 19 Nov 18 10:03:04 UTC │ 19 Nov 18 10:03:04 UTC │ 0s       │ NEW   │
        # ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴───────╯

        # In 3.0:

        # Name:      repair/1baef128-ea0a-47b8-bf4e-612e1095f7b5
        # Cron:     1d
        # Retry:    3
        #
        # ╭──────────────────────────────────────┬────────────────────────┬──────────┬────────╮
        # │ ID                                   │ Start time             │ Duration │ Status │
        # ├──────────────────────────────────────┼────────────────────────┼──────────┼────────┤
        # │ e2f6e5ea-9879-11ec-af1b-02cd01a36b8f │ 28 Feb 22 09:36:20 UTC │ 11s      │ …      │
        # ╰──────────────────────────────────────┴────────────────────────┴──────────┴────────╯
        #
        # or
        #
        # Name:      backup/8e955997-2dd0-4f6e-8526-375fd1902c94
        # Cron:     1d
        # Retry:    3
        #
        # Properties:
        # - keyspace: keyspace1
        # - location: s3:manager-backup-tests-us-east-1
        #
        # ╭──────────────────────────────────────┬────────────────────────┬──────────┬────────╮
        # │ ID                                   │ Start time             │ Duration │ Status │
        # ├──────────────────────────────────────┼────────────────────────┼──────────┼────────┤
        # │ 3e32bcc3-c5c1-11ec-85ad-02f351adfaf7 │ 27 Apr 22 00:30:30 UTC │ 15s      │ DONE   │
        # ╰──────────────────────────────────────┴────────────────────────┴──────────┴────────╯
        if self.sctool.is_v3_cli:
            return self.get_task_info_dict()["history"]
        cmd = "task history {} -c {}".format(self.id, self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        return res  # or can be specified like: self.get_property(parsed_table=res, column_name='status')

    @property
    def next_run(self):
        """
        Gets the task's next run value
        """
        # ╭──────────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                             │ next run                      │ ret. │ properties │ status │
        # ├──────────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ healthcheck/7fb6f1a7-aafc-4950-90eb-dc64729e8ecb │ 18 Nov 18 20:32:08 UTC (+15s) │ 0    │            │ NEW    │
        # │ repair/22b68423-4332-443d-b8b4-713005ea6049      │ 19 Nov 18 00:00:00 UTC (+7d)  │ 3    │            │ NEW    │
        # ╰──────────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        if self.sctool.is_v3_cli:
            cmd = "tasks -c {}".format(self.cluster_id)
        else:
            cmd = "task list -c {}".format(self.cluster_id)
        res = self.sctool.run(cmd=cmd, is_verify_errorless_result=True)
        if self.sctool.is_v3_cli:
            return self.get_property(parsed_table=res, column_name='Next')
        return self.get_property(parsed_table=res, column_name='next run')

    @property
    def latest_run_id(self):
        history = self.history
        all_dates = self.sctool.get_all_column_values_from_table(history, "start time")
        latest_run_date = self.get_max_date(all_dates)
        latest_run_id = self.sctool.get_table_value(parsed_table=history, column_name="id",
                                                    identifier=latest_run_date)
        return latest_run_id

    @staticmethod
    def get_max_date(date_list):
        """
        Receives a list of date strings and returns the string of the latest date string
        """
        time_format = "%d %b %y %H:%M:%S"
        converted_max_date = datetime.datetime(1970, 1, 3, 0, 0, 0)
        timezone = date_list[0][date_list[0].rindex(" ") + 1:]
        for date_string in date_list:
            converted_date = datetime.datetime.strptime(date_string[:date_string.rindex(" ")], time_format)
            converted_max_date = max(converted_max_date, converted_date)
        return f"{datetime.datetime.strftime(converted_max_date, time_format)} {timezone}"

    @property
    def status(self) -> str:
        """
        Gets the task's status
        """
        if self.sctool.is_v3_cli:
            cmd = "tasks -c {}".format(self.cluster_id)
        else:
            cmd = "task list -c {}".format(self.cluster_id)
        # expecting output of:
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 05 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ repair/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        res = self.sctool.run(cmd=cmd)
        str_status = self.get_property(parsed_table=res, column_name='status')
        # The manager will sometimes retry a task a few times if it's defined this way, and so in the case of
        # a failure in the task the manager can present the task's status as 'ERROR (#/4)'
        tmp = str_status.split()
        # We don't examine the whole string, since sometimes error messages can appear after the status
        if ' '.join(tmp[0:2]) == 'ERROR (4/4)':
            return TaskStatus.ERROR_FINAL
        return TaskStatus.from_str(tmp[0])

    @property
    def arguments(self) -> str:
        """
        Gets the task's arguments
        """
        res = self.progress_string()

        arguments_string = ""  # If arguments parameter doesn't exist, there were no arguments in this task
        # Output example:
        #
        # Status:           RUNNING (uploading data)
        # Start time:       20 Feb 22 15:02:40 UTC
        # Duration: 1m1s
        # Progress: 99%
        # Snapshot Tag:     sm_20220220150242UTC
        # Datacenters:
        #   - us-eastscylla_node_east
        #   - us-west-2scylla_node_west
        #
        # ╭───────────────┬──────────┬──────────┬──────────┬──────────────┬────────╮
        # │ Host          │ Progress │     Size │  Success │ Deduplicated │ Failed │
        # ├───────────────┼──────────┼──────────┼──────────┼──────────────┼────────┤
        # │ 3.239.214.188 │     100% │ 944.676M │ 944.676M │            0 │      0 │
        # │ 35.86.127.236 │      99% │ 944.800M │ 944.764M │            0 │      0 │
        # │ 44.200.32.210 │     100% │ 944.777M │ 944.777M │            0 │      0 │
        # ╰───────────────┴──────────┴──────────┴──────────┴──────────────┴────────╯
        for task_property in res:
            if task_property[0].startswith("Arguments"):
                arguments_string = task_property[0].split(':', maxsplit=1)[1].strip()
                break
        return arguments_string

    def progress_string(self, **kwargs):
        """
        The function executes the progress command for the current task
        and returns its output string (or parsed table if stated in the kwargs
        """
        # Progress output:
        #
        # Run:              2634559a-925e-11ec-9837-0286f7bd10db
        # Status:           RUNNING (uploading data)
        # Start time:       20 Feb 22 15:02:40 UTC
        # Duration: 1m1s
        # Progress: 99%
        # Snapshot Tag:     sm_20220220150242UTC
        # Datacenters:
        #   - us-eastscylla_node_east
        #   - us-west-2scylla_node_west
        #
        # ╭───────────────┬──────────┬──────────┬──────────┬──────────────┬────────╮
        # │ Host          │ Progress │     Size │  Success │ Deduplicated │ Failed │
        # ├───────────────┼──────────┼──────────┼──────────┼──────────────┼────────┤
        # │ 3.239.214.188 │     100% │ 944.676M │ 944.676M │            0 │      0 │
        # │ 35.86.127.236 │      99% │ 944.800M │ 944.764M │            0 │      0 │
        # │ 44.200.32.210 │     100% │ 944.777M │ 944.777M │            0 │      0 │
        # ╰───────────────┴──────────┴──────────┴──────────┴──────────────┴────────╯
        if self.sctool.is_v3_cli:
            cmd = f" -c {self.cluster_id} progress {self.id}"
        else:
            cmd = f" -c {self.cluster_id} task progress {self.id}"
        res = self.sctool.run(cmd=cmd, **kwargs)
        return res

    @property
    def progress(self) -> str:
        """
        Gets the repair task's progress
        """
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"
        res = self.progress_string()
        # expecting output of:
        #  Status:           RUNNING
        #  Start time:       26 Mar 19 19:40:21 UTC
        #  Duration: 6s
        #  Progress: 0.12%
        #  Datacenters:
        #    - us-eastscylla_node_east
        #  ╭────────────────────┬───────╮
        #  │ system_auth        │ 0.47% │
        #  │ system_distributed │ 0.00% │
        #  │ system_traces      │ 0.00% │
        #  │ keyspace1          │ 0.00% │
        #  ╰────────────────────┴───────╯
        # [['Status: RUNNING'], ['Start time: 26 Mar 19 19:40:21 UTC'], ['Duration: 6s'], ['Progress: 0.12%'], ... ]
        progress = "N/A"
        for task_property in res:
            if task_property[0].startswith("Progress"):
                progress = task_property[0].split(':')[1]
                break
        return progress

    @property
    def detailed_progress(self):
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return " 0%"

        parsed_progress_table = self.progress_string(parse_table_res=True, is_multiple_tables=True)
        # expecting output of:
        # ...
        # ╭────────────────────┬────────────────────────┬──────────┬──────────╮
        # │ Keyspace           │                  Table │ Progress │ Duration │
        # ├────────────────────┼────────────────────────┼──────────┼──────────┤
        # │ keyspace1          │              standard1 │ 0%       │ 0s       │
        # ├────────────────────┼────────────────────────┼──────────┼──────────┤
        # │ system_auth        │           role_members │ 100%     │ 21s      │
        # │ system_auth        │                  roles │ 100%     │ 23s      │
        # ├────────────────────┼────────────────────────┼──────────┼──────────┤
        # │ system_distributed │        cdc_generations │ 0%       │ 1s       │
        # │ system_distributed │            cdc_streams │ 0%       │ 0s       │
        # │ system_distributed │      view_build_status │ 0%       │ 0s       │
        # ├────────────────────┼────────────────────────┼──────────┼──────────┤
        # │ system_traces      │                 events │ 0%       │ 0s       │
        # │ system_traces      │          node_slow_log │ 100%     │ 7s       │
        # │ system_traces      │ node_slow_log_time_idx │ 0%       │ 0s       │
        # │ system_traces      │               sessions │ 100%     │ 7s       │
        # │ system_traces      │      sessions_time_idx │ 100%     │ 7s       │
        # ╰────────────────────┴────────────────────────┴──────────┴──────────╯
        relevant_key = [key for key in parsed_progress_table.keys() if parsed_progress_table[key]][0]
        return parsed_progress_table[relevant_key]

    @property
    def duration(self) -> datetime.timedelta:
        if self.status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return duration_to_timedelta(duration_string="0")

        res = self.progress_string()
        duration_string = "0"
        for task_property in res:
            if task_property[0].startswith("Duration"):
                duration_string = task_property[0].split(':')[1]
                break
        duration_timedelta = duration_to_timedelta(duration_string=duration_string)
        return duration_timedelta

    def is_status_in_list(self, list_status, check_task_progress=False):
        """
        Check if the status of a given task is in list
        :param list_status:
        :param check_task_progress:
        :return:
        """
        status = self.status
        if not check_task_progress or status in [TaskStatus.NEW, TaskStatus.STARTING]:
            return status in list_status
        # check progress for all statuses except 'NEW' / 'STARTING'
        ###
        # The reasons for the below (un-used) assignment are:
        # * check that progress command works on various task statuses (that was how manager bug #856 found).
        # * print the progress to log in cases needed for failures/performance analysis.
        ###
        progress = self.progress  # noqa: F841
        return self.status in list_status

    def wait_for_status(self, list_status, check_task_progress=True, timeout=3600, step=120):
        text = "Waiting until task: {} reaches status of: {}".format(self.id, list_status)
        try:
            return wait.wait_for(func=self.is_status_in_list, step=step, throw_exc=True,
                                 text=text, list_status=list_status, check_task_progress=check_task_progress,
                                 timeout=timeout)
        except WaitForTimeoutError as ex:
            raise WaitForTimeoutError(
                "Failed on waiting until task: {} reaches status of {}: current task status {}: {}".format(
                    self.id, list_status, self.status, str(ex))) from ex

    def wait_for_percentage(self, minimum_percentage, timeout=3600, step=10):
        text = f"Waiting until task: {self.id} reaches at least {minimum_percentage}% progress"
        is_percentage_reached = wait.wait_for(func=self.has_progress_reached_percentage,
                                              minimum_percentage=minimum_percentage,
                                              step=step, text=text, timeout=timeout, throw_exc=True)
        return is_percentage_reached

    def has_progress_reached_percentage(self, minimum_percentage):
        """
        The function receives an expected percentage number, whether int or float (between 0 and 100)
        and the function will return True if the current progress percentage of the task is at least the
        minimum_percentage, and False otherwise.
        """
        progress_string = self.progress.strip()
        if progress_string == "N/A":
            return 0.0
        current_progress_percentage = float(progress_string.replace("%", ""))
        return current_progress_percentage >= minimum_percentage

    def wait_and_get_final_status(self, timeout=DEFAULT_TASK_TIMEOUT, step=60, only_final=False):
        """
        1) Wait for task to reach a 'final' status. meaning one of: done/error/stopped
        2) return the final status.
        :return:
        """
        if only_final:
            list_final_status = [TaskStatus.ERROR_FINAL, TaskStatus.DONE]
        else:
            list_final_status = [
                TaskStatus.ERROR, TaskStatus.ERROR_FINAL, TaskStatus.STOPPED, TaskStatus.DONE, TaskStatus.ABORTED]
        LOGGER.debug("Waiting for task: {} getting to a final status ({})..".format(self.id, str(list_final_status)))
        res = self.wait_for_status(list_status=list_final_status, timeout=timeout, step=step)
        if not res:
            raise ScyllaManagerError("Unexpected result on waiting for task {} status".format(self.id))
        return self.status


class RepairTask(ManagerTask):
    @property
    def per_keyspace_progress(self) -> dict[str, float]:
        """
        Since, as of now, the progress table of a repair task shows the progress of every table,
        this function will create an average for each keyspace and return the progress of all of the keyspaces in a dict
        """
        inclusive_table_progress_dict = {}
        for keyspace_name, _, progress_percentage, _ in self.detailed_progress[1:]:  # skip headers
            inclusive_table_progress_dict.setdefault(keyspace_name, []).append(int(progress_percentage.strip()[:-1]))
        return {keyspace_name: mean(progress) for keyspace_name, progress in inclusive_table_progress_dict.items()}


class HealthcheckTask(ManagerTask):
    def __init__(self, task_id, cluster_id, manager_node):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, manager_node=manager_node)

    def progress_string(self, **kwargs):
        # progress command does not support healthcheck tasks
        return ""


class BackupTask(ManagerTask):
    def __init__(self, task_id, cluster_id, manager_node):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, manager_node=manager_node)

    def get_snapshot_tag(self, snapshot_index=0):
        res = self.progress_string(parse_table_res=False, is_verify_errorless_result=True)
        snapshot_line = [line for line in res.stdout.splitlines() if "snapshot tag" in line.lower()]
        # Returns the following:
        # Snapshot Tag:	sm_20200106093455UTC
        # (when executed manually, the title and value is separated by \t instead
        if snapshot_index >= len(snapshot_line):
            snapshot_index = -1
        snapshot_tag = snapshot_line[snapshot_index].split(":")[1].strip()
        return snapshot_tag

    def is_task_in_uploading_stage(self):
        full_progress_string = self.progress_string(parse_table_res=False,
                                                    is_verify_errorless_result=True).stdout.lower()
        return "uploading data" in full_progress_string.lower()

    def wait_for_uploading_stage(self, timeout=1440, step=10):
        text = "Waiting until backup task: {} starts to upload snapshots".format(self.id)
        is_status_reached = wait.wait_for(func=self.is_task_in_uploading_stage, step=step,
                                          text=text, timeout=timeout, throw_exc=True)
        return is_status_reached

    def delete_backup_snapshot(self):
        if self.status == TaskStatus.DONE:
            snapshot_tag = self.get_snapshot_tag()
            command = f" -c {self.cluster_id} backup delete --snapshot-tag {snapshot_tag}"
            self.sctool.run(command, parse_table_res=False, is_verify_errorless_result=True)
        else:
            LOGGER.warning("Did not delete the snapshot of task {}, since the status of said task is {}, and the "
                           "manager can only delete snapshots of finished tasks".format(self.id, str(self.status)))


class RestoreTask(ManagerTask):
    def __init__(self, task_id, cluster_id, manager_node):
        ManagerTask.__init__(self, task_id=task_id, cluster_id=cluster_id, manager_node=manager_node)

    @property
    def download_bw(self) -> float | None:
        """Restore download phase bandwidth in MiB/s/shard"""
        # ...
        # Bandwidth:
        #   - Download:    22.313MiB/s/shard
        #   - Load&stream: 3.556MiB/s/shard
        #
        # ...
        res = self.progress_string()

        try:
            download_bandwidth_str = res[res.index(['Bandwidth:']) + 1][0]
        except ValueError:
            LOGGER.warning("Failed to extract Download bandwidth from the sctool restore progress output."
                           "Check Manager version, bandwidth metrics are supported starting from 3.4.")
            return None

        download_bandwidth_match = re.search(r"(\d+\.\d+)", download_bandwidth_str)
        if download_bandwidth_match:
            # Numerical value found, return it as a float
            return float(download_bandwidth_match.group(1))
        else:
            # Non-numeric value found (e.g., 'unknown')
            LOGGER.warning(f"Download bandwidth is non-numeric: {download_bandwidth_str.strip()}. Returning None.")
            return None

    @property
    def load_and_stream_bw(self) -> float | None:
        """Restore load&stream phase bandwidth in MiB/s/shard"""
        # ...
        # Bandwidth:
        #   - Download:    22.313MiB/s/shard
        #   - Load&stream: 3.556MiB/s/shard
        #
        # ...
        res = self.progress_string()

        try:
            las_bandwidth_str = res[res.index(['Bandwidth:']) + 2][0]
        except ValueError:
            LOGGER.warning("Failed to extract Load&Stream bandwidth from the sctool restore progress output."
                           "Check Manager version, bandwidth metrics are supported starting from 3.4.")
            return None

        las_bandwidth_match = re.search(r"(\d+\.\d+)", las_bandwidth_str)
        if las_bandwidth_match:
            # Numerical value found, return it as a float
            return float(las_bandwidth_match.group(1))
        else:
            # Non-numeric value found (e.g., 'unknown'). Log warning and return None.
            LOGGER.warning(f"Load&Stream bandwidth is non-numeric: {las_bandwidth_str.strip()}. Returning None.")
            return None

    @property
    def post_restore_repair_duration(self) -> datetime.timedelta:
        """Restore task consists of two parts and includes two duration marks:
        - overall restore duration
        - post restore repair duration

        This function returns the post-restore repair duration
        """
        res = self.progress_string()

        try:
            repair_res = res[res.index(['Post-restore repair progress']):]
        except ValueError:
            return duration_to_timedelta(duration_string="0")

        if self.sctool.parsed_client_version >= LooseVersion("3.4.0"):
            for task_property in repair_res:
                if task_property[0].startswith("Duration"):
                    return duration_to_timedelta(task_property[0].split(':')[-1])
        else:
            # Overall repair duration is defined as a sum of all the durations (last column) in the table
            # because of the issue https://github.com/scylladb/scylla-manager/issues/4046
            repair_by_tables = repair_res[repair_res.index(['Keyspace', 'Table', 'Progress', 'Duration']) + 1:]
            per_table_duration_timedelta = [duration_to_timedelta(table[-1]) for table in repair_by_tables]
            duration_timedelta = sum(per_table_duration_timedelta, datetime.timedelta(0))
            return duration_timedelta


class ManagerCluster(ScyllaManagerBase):

    def __init__(self, manager_node, cluster_id=None, client_encrypt=False):
        if not manager_node:
            raise ScyllaManagerError("Cannot create a Manager Cluster where no 'manager tool' parameter is given")
        ScyllaManagerBase.__init__(self, id=cluster_id, manager_node=manager_node)
        self.client_encrypt = client_encrypt

    def get_cluster_id_by_name(self, cluster_name: str):
        try:
            cluster_list = self.sctool.run(cmd="cluster list", is_verify_errorless_result=True)
            column_to_search = "ID"
            if cluster_list:
                column_names = cluster_list[0]
                if "cluster id" in column_names:
                    column_to_search = "cluster id"
            return self.sctool.get_table_value(
                parsed_table=cluster_list, column_name=column_to_search, identifier=cluster_name)
        except ScyllaManagerError as ex:
            LOGGER.warning("Cluster name not found in Scylla-Manager: {}".format(ex))
            return None

    def set_cluster_id(self, value: str):
        self.id = value

    def create_restore_task(self, restore_schema=False, restore_data=False, location_list=None, snapshot_tag=None,
                            dc_mapping=None, object_storage_method=None, extra_params=None):
        cmd = f"restore -c {self.id}"
        if restore_schema:
            cmd += " --restore-schema"
        if restore_data:
            cmd += " --restore-tables"
        if location_list:
            locations_names = ','.join(location_list)
            cmd += " --location {} ".format(locations_names)
        if snapshot_tag:
            cmd += f" --snapshot-tag {snapshot_tag}"
        if dc_mapping and restore_data:
            # --dc-mapping flag is applicable only for --restore-tables mode
            # https://manager.docs.scylladb.com/stable/sctool/restore.html#dc-mapping
            cmd += f" --dc-mapping {dc_mapping}"
        if object_storage_method is not None:
            cmd += " --method {} ".format(object_storage_method.value)
        if extra_params:
            cmd += f" {extra_params}"

        res = self.sctool.run(cmd=cmd, parse_table_res=False)
        task_id = res.stdout.strip()
        LOGGER.debug("Created task id is: {}".format(task_id))
        return RestoreTask(task_id=task_id, cluster_id=self.id, manager_node=self.manager_node)

    def create_backup_task(self, dc_list=None,  # noqa: PLR0913
                           dry_run=None, interval=None, keyspace_list=None, cron=None,
                           location_list=None, num_retries=None, rate_limit_list=None, retention=None, show_tables=None,
                           snapshot_parallel_list=None, start_date=None, upload_parallel_list=None, transfers=None,
                           object_storage_upload_mode=None, legacy_args=None):
        cmd = "backup -c {}".format(self.id)

        if dc_list is not None:
            dc_names = ','.join(dc_list)
            cmd += " --dc {} ".format(dc_names)
        if dry_run is not None:
            cmd += " --dry-run"
        if interval is not None:
            cmd += " --interval {}".format(interval)
        if keyspace_list is not None:
            keyspaces_names = ','.join(keyspace_list)
            cmd += " --keyspace {} ".format(keyspaces_names)
        if location_list is not None:
            locations_names = ','.join(location_list)
            cmd += " --location {} ".format(locations_names)
        if num_retries is not None:
            cmd += " --num-retries {}".format(num_retries)
        if rate_limit_list is not None:
            rate_limit_string = ','.join(rate_limit_list)
            cmd += " --rate-limit {} ".format(rate_limit_string)
        if retention is not None:
            cmd += " --retention {} ".format(retention)
        if show_tables is not None:
            cmd += " --show-tables {} ".format(show_tables)
        if snapshot_parallel_list is not None:
            snapshot_parallel_string = ','.join(snapshot_parallel_list)
            cmd += " --snapshot-parallel {} ".format(snapshot_parallel_string)
        if transfers is not None:
            cmd += " --transfers {} ".format(transfers)
        if start_date is not None:
            cmd += " --start-date {} ".format(start_date)
        # Since currently we support both manager 2.6 and 3.0, I left the start-date parameter in,
        # even though it's deprecated in 3.0
        # TODO: remove start-date and interval once 2.6 is no longer supported
        if cron is not None:
            cmd += " --cron '{}' ".format(" ".join(cron))
        if upload_parallel_list is not None:
            upload_parallel_string = ','.join(upload_parallel_list)
            cmd += " --upload-parallel {} ".format(upload_parallel_string)
        if object_storage_upload_mode is not None:
            cmd += " --method {} ".format(object_storage_upload_mode.value)
        if legacy_args:
            cmd += f" {legacy_args}"
        res = self.sctool.run(cmd=cmd, parse_table_res=False)

        task_id = res.stdout.strip()
        LOGGER.debug("Created task id is: {}".format(task_id))
        return BackupTask(task_id=task_id, cluster_id=self.id, manager_node=self.manager_node)

    def create_repair_task(self, dc_list=None,
                           keyspace=None, interval=None, num_retries=None, fail_fast=None,
                           intensity=None, parallel=None, cron=None, start_date=None, ignore_down_hosts=False):
        # the interval string:
        # Amount of time after which a successfully completed task would be run again. Supported time units include:
        #
        # d - days,
        # h - hours,
        # m - minutes,
        # s - seconds.
        cmd = "repair -c {}".format(self.id)
        if dc_list is not None:
            dc_names = ','.join(dc_list)
            cmd += " --dc {} ".format(dc_names)
        if keyspace is not None:
            cmd += " --keyspace {} ".format(keyspace)
        if interval is not None:
            cmd += " --interval {}".format(interval)
        if num_retries is not None:
            cmd += " --num-retries {}".format(num_retries)
        if fail_fast is not None:
            cmd += " --fail-fast"
        if intensity is not None:
            cmd += f" --intensity {intensity}"
        if parallel is not None:
            cmd += f" --parallel {parallel}"
        if start_date is not None:
            cmd += " --start-date {} ".format(start_date)
        # Since currently we support both manager 2.6 and 3.0, I left the start-date parameter in, even though it's
        # deprecated in 3.0
        # TODO: remove start-date once 2.6 is no longer supported
        if cron is not None:
            cmd += " --cron '{}' ".format(" ".join(cron))
        if ignore_down_hosts:
            cmd += " --ignore-down-hosts"

        with DbNodeLogger([self.manager_node], f"start scylla-manager task {cmd}", target_node=self.manager_node):
            res = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not res:
            raise ScyllaManagerError("Unknown failure for sctool {} command".format(cmd))

        if "no matching units found" in res.stderr:
            raise ScyllaManagerError("Manager cannot run repair where no keyspace exists.")

        # expected result output is to have a format of: "repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d"
        if 'repair' not in res.stdout:
            LOGGER.error("Encountered an error on '{}' command response".format(cmd))
            raise ScyllaManagerError(res.stderr)

        task_id = res.stdout.split('\n')[0]
        LOGGER.debug("Created task id is: {}".format(task_id))

        return RepairTask(task_id=task_id, cluster_id=self.id,
                          manager_node=self.manager_node)  # return the manager's object with new repair-task-id

    def control_repair(self, intensity=None, parallel=None):
        cmd = " repair control -c {} ".format(self.id)
        if intensity is not None:
            cmd += f" --intensity {intensity}"
        if parallel is not None:
            cmd += f" --parallel {parallel}"

        res = self.sctool.run(cmd=cmd, parse_table_res=False)
        if not res:
            raise ScyllaManagerError("Unknown failure for sctool {} command".format(cmd))

    def get_backup_files_dict(self, snapshot_tag, location=None, all_clusters=None):
        location_flag = f" --location {location}" if location else ""
        all_clusters_flag = "--all-clusters" if all_clusters else ""
        command = f" -c {self.id} backup files --snapshot-tag {snapshot_tag} {location_flag} {all_clusters_flag}"
        # The sctool backup files command prints the s3 paths of all of the files that are required to restore the
        # cluster from the backup
        snapshot_files = self.sctool.run(command)
        snapshot_file_list = [file_path_list[0] for file_path_list in snapshot_files]
        # sctool.run returns a list of lists, each of them is a 1 length list that contains the row.
        # This list comprehension turns the list into a list of strings (rows) instead
        return self.snapshot_files_to_dict(snapshot_file_list)

    @staticmethod
    def snapshot_files_to_dict(snapshot_file_lines):
        per_node_keyspaces_and_tables_backup_files = {}
        for line in snapshot_file_lines:
            s3_file_path, keyspace_and_table = [string.strip() for string in line.split(' ')]
            node_id = s3_file_path[s3_file_path.find("/node/") + len("/node/"):s3_file_path.find("/keyspace")]
            keyspace, table = keyspace_and_table.split('/')
            if node_id not in per_node_keyspaces_and_tables_backup_files:
                per_node_keyspaces_and_tables_backup_files[node_id] = {}
            if keyspace not in per_node_keyspaces_and_tables_backup_files[node_id]:
                per_node_keyspaces_and_tables_backup_files[node_id][keyspace] = {}
            if table not in per_node_keyspaces_and_tables_backup_files[node_id][keyspace]:
                per_node_keyspaces_and_tables_backup_files[node_id][keyspace][table] = []
            per_node_keyspaces_and_tables_backup_files[node_id][keyspace][table].append(s3_file_path)
        return per_node_keyspaces_and_tables_backup_files

    def delete(self):
        """
        $ sctool cluster delete
        """

        cmd = "cluster delete -c {}".format(self.id)
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def update(self, name=None, host=None, client_encrypt=None, force_non_ssl_session_port=False):
        """
        $ sctool cluster update --help
        Modify a cluster

        Usage:
          sctool cluster update [flags]

        Flags:
          -h, --help                     help for update
              --host string              hostname or IP of one of the cluster nodes
          -n, --name alias               alias you can give to your cluster
        """
        cmd = "cluster update -c {}".format(self.id)
        if name:
            cmd += " --name={}".format(name)
        if host:
            cmd += " --host={}".format(host)
        if client_encrypt:
            cmd += " --ssl-user-cert-file {} --ssl-user-key-file {}".format(SSL_USER_CERT_FILE, SSL_USER_KEY_FILE)
        if force_non_ssl_session_port:
            cmd += "  --force-non-ssl-session-port"
        self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def delete_task(self, task: ManagerTask):
        task_id = task.id
        if self.sctool.is_v3_cli:
            cmd = "stop --delete {} -c {}".format(task_id, self.id)
        else:
            cmd = "-c {} task delete {}".format(self.id, task_id)
        LOGGER.debug("Task Delete command to execute is: {}".format(cmd))
        self.sctool.run(cmd=cmd, parse_table_res=False)
        LOGGER.debug("Deleted the task '{}' successfully!". format(task_id))

    def delete_automatic_repair_task(self):
        repair_tasks_list = self.repair_task_list
        if repair_tasks_list:
            automatic_repair_task = repair_tasks_list[0]
            self.delete_task(automatic_repair_task)

    @property
    def _cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def name(self):
        """
        Gets the Cluster name as represented in Manager
        """
        # expecting output of:
        # ╭──────────────────────────────────────┬──────┬─────────────╮
        # │ cluster id                           │ name │ host        │
        # ├──────────────────────────────────────┼──────┼─────────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ sce2 │ 10.142.0.25 │
        # ╰──────────────────────────────────────┴──────┴─────────────╯
        return self.get_property(parsed_table=self._cluster_list, column_name='name')

    def _get_task_list(self):
        if self.sctool.is_v3_cli:
            cmd = "tasks -c {}".format(self.id)
        else:
            cmd = "task list -c {}".format(self.id)
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def _get_task_list_filtered(self, prefix, task_class):
        """
        Gets the Cluster's  Task list
        """
        # ╭─────────────────────────────────────────────┬───────────────────────────────┬──────┬────────────┬────────╮
        # │ task                                        │ next run                      │ ret. │ properties │ status │
        # ├─────────────────────────────────────────────┼───────────────────────────────┼──────┼────────────┼────────┤
        # │ repair/2a4125d6-5d5a-45b9-9d8d-dec038b3732d │ 26 Nov 18 00:00 UTC (+7 days) │ 3    │            │ DONE   │
        # │ backup/dd98f6ae-bcf4-4c98-8949-573d533bb789 │                               │ 3    │            │ DONE   │
        # ╰─────────────────────────────────────────────┴───────────────────────────────┴──────┴────────────┴────────╯
        task_list = []
        table_res = self._get_task_list()
        if len(table_res) > 1:
            task_row_list = [row for row in table_res[1:] if row[0].startswith(f"{prefix}")]
            for row in task_row_list:
                task_list.append(task_class(task_id=row[0], cluster_id=self.id, manager_node=self.manager_node))
        return task_list

    @property
    def repair_task_list(self):
        return self._get_task_list_filtered('repair/', RepairTask)

    @property
    def backup_task_list(self):
        return self._get_task_list_filtered('backup/', BackupTask)

    def get_healthcheck_task(self):
        healthcheck_id = self.sctool.get_table_value(parsed_table=self._get_task_list(), column_name="task",
                                                     identifier="healthcheck/", is_search_substring=True)
        return HealthcheckTask(task_id=healthcheck_id, cluster_id=self.id,
                               manager_node=self.manager_node)  # return the manager's health-check-task object with the found id

    def get_hosts_health(self):  # noqa: PLR0914
        """
        Gets the Manager's Cluster Nodes status
        """

        # $ sctool status -c bla
        # Datacenter: dc1
        # ╭────┬─────────────────────────┬───────────┬────────────────┬──────────────────────────────────────╮
        # │    │ CQL                     │ REST      │ Host           │ Host ID                              │
        # ├────┼─────────────────────────┼───────────┼────────────────┼──────────────────────────────────────┤
        # │ UN │ UP SSL (58ms)           │ UP (2ms)  │ 192.168.100.11 │ a2b4200a-4157-4b47-9c10-b102246fe7ff │
        # │ UN │ UP SSL (60ms)           │ UP (3ms)  │ 192.168.100.12 │ aa1d8329-a66e-4500-bb38-ed9c4f236a0d │
        # │ UN │ DOWN SSL (40ms)         │ UP (11ms) │ 192.168.100.13 │ b583255c-4029-4207-8237-e40996985f29 │
        # ╰────┴─────────────────────────┴───────────┴────────────────┴──────────────────────────────────────╯
        # Datacenter: dc2
        # ╭────┬─────────────────────────┬───────────────────┬────────────────┬──────────────────────────────────────╮
        # │    │ CQL                     │ REST              │ Host           │ Host ID                              │
        # ├────┼─────────────────────────┼───────────────────┼────────────────┼──────────────────────────────────────┤
        # │ UN │ TIMEOUT SSL             │ UP (4ms)          │ 192.168.100.21 │ 9b91b800-f74d-47ed-973c-7a8ef8088c77 │
        # │ UN │ TIMEOUT SSL             │ TIMEOUT           │ 192.168.100.22 │ 56d2f4c0-9327-487e-b115-c96d3e5c014b │
        # │ UN │ UP SSL (40ms)           │ HTTP (503) (7ms)  │ 192.168.100.23 │ 08152d3d-ed30-469e-bc19-5ab9f4248e9a │
        # ╰────┴─────────────────────────┴───────────────────┴────────────────┴──────────────────────────────────────╯
        cmd = "status -c {}".format(self.id)
        dict_status_tables = self.sctool.run(cmd=cmd, is_verify_errorless_result=True, is_multiple_tables=True)

        dict_hosts_health = {}
        for dc_name, hosts_table in dict_status_tables.items():
            if len(hosts_table) < 2:
                LOGGER.debug("Cluster: {} - {} has no hosts health report".format(self.id, dc_name))
            else:
                list_titles_row = hosts_table[0]
                host_col_idx = list_titles_row.index("Address")
                cql_status_col_idx = list_titles_row.index("CQL")
                rest_col_idx = list_titles_row.index("REST")

                for line in hosts_table[1:]:
                    host = line[host_col_idx]
                    list_cql = line[cql_status_col_idx].split()
                    status = list_cql[0]
                    rtt = self._extract_value_with_regex(string=list_cql[-1], regex_pattern=r"\(([^)]+ms)")
                    rest_value = line[rest_col_idx]
                    if rest_value == '-':
                        rest_status = rest_value
                    else:
                        rest_status = rest_value[:rest_value.find("(")].strip()
                    rest_rtt = self._extract_value_with_regex(string=rest_value, regex_pattern=r"\(([^)]+ms)")
                    rest_http_status_code = self._extract_value_with_regex(string=rest_value,
                                                                           regex_pattern=r"\(([0-9]*?)\)")
                    ssl = line[cql_status_col_idx]
                    # Whether or not SSL is on is now described in the cql column
                    # If SSL is on the column value will include "SSL" in it, and if not it will not.
                    dict_hosts_health[host] = self._HostHealth(status=HostStatus.from_str(status), rtt=rtt,
                                                               rest_status=HostRestStatus.from_str(rest_status),
                                                               rest_rtt=rest_rtt, ssl=HostSsl.from_str(ssl),
                                                               rest_http_status_code=rest_http_status_code)
            LOGGER.debug("Cluster {} Hosts Health is:".format(self.id))
            for ip, health in dict_hosts_health.items():
                LOGGER.debug("{}: {},{},{},{},{}".format(ip, health.status, health.rtt,
                                                         health.rest_status, health.rest_rtt, health.ssl))
        return dict_hosts_health

    class _HostHealth():
        def __init__(self, status, rtt, ssl, rest_status, rest_rtt, rest_http_status_code=None):
            self.status = status
            self.rtt = rtt
            self.rest_status = rest_status
            self.rest_rtt = rest_rtt
            self.ssl = ssl
            self.rest_http_status_code = rest_http_status_code

    @staticmethod
    def _extract_value_with_regex(string, regex_pattern, default_value="N/A"):
        value_list = findall(pattern=regex_pattern, string=string)
        if len(value_list) == 1:
            return value_list[0]
        return default_value

    def suspend(self, on_resume_start_tasks=False, duration=None):
        cmd = f"suspend -c {self.id}"
        if on_resume_start_tasks:
            cmd += " --on-resume-start-tasks"
        if duration is not None:
            cmd += f" --duration {duration}"
        self.sctool.run(cmd=cmd)

    def resume(self, start_tasks=True):
        cmd = f"resume -c {self.id}"
        if start_tasks:
            cmd += " --start-tasks"
        self.sctool.run(cmd=cmd)

    @contextmanager
    def suspend_manager_then_resume(self, start_tasks=True, start_tasks_in_advance=False, duration=None):
        self.suspend(on_resume_start_tasks=start_tasks_in_advance, duration=duration)
        try:
            yield
        finally:
            self.resume(start_tasks=start_tasks)


def verify_errorless_result(cmd, res):
    if res.exited != 0:
        raise ScyllaManagerError("Encountered an error on '{}' command response {}\ncommand exit code:{}\nstderr:{}".format(
            cmd, res, res.exited, res.stderr))
    if res.stderr:
        LOGGER.error("Encountered an error on '{}' stderr: {}".format(cmd, str(res.stderr)))  # TODO: just for checking


class ScyllaManagerTool(ScyllaManagerBase):
    clusterClass = ManagerCluster

    """
    Provides communication with scylla-manager, operating sctool commands
    """

    def __init__(self, manager_node):
        ScyllaManagerBase.__init__(self, id="MANAGER", manager_node=manager_node)
        self._initial_wait(20)
        LOGGER.info("Initiating Scylla-Manager, version: {}".format(self.sctool.version))
        self.default_user = "centos"
        if not (manager_node.distro.is_debian_like or manager_node.distro.is_rhel_like):
            raise ScyllaManagerError(
                "Non-Manager-supported Distro found on Monitoring Node: {}".format(manager_node.distro))

    @staticmethod
    def _initial_wait(seconds: int):
        LOGGER.debug('Sleep %s seconds, waiting for manager service ready to respond', seconds)
        time.sleep(seconds)

    @property
    def cluster_list(self):
        """
        Gets the Manager's Cluster list
        """
        cmd = "cluster list"
        return self.sctool.run(cmd=cmd, is_verify_errorless_result=True)

    def get_cluster(self, cluster_name):
        """
        Returns Manager Cluster object by a given name if exist, else returns none.
        """
        # ╭──────────────────────────────────────┬──────────╮
        # │ cluster id                           │ name     │
        # ├──────────────────────────────────────┼──────────┤
        # │ 1de39a6b-ce64-41be-a671-a7c621035c0f │ Dev_Test │
        # │ bf6571ef-21d9-4cf1-9f67-9d05bc07b32e │ Prod     │
        # ╰──────────────────────────────────────┴──────────╯
        cluster = self.clusterClass(manager_node=self.manager_node)
        cluster_id = cluster.get_cluster_id_by_name(cluster_name)
        if cluster_id is None:
            return None
        cluster.set_cluster_id(cluster_id)
        return cluster

    @staticmethod
    def get_cluster_hosts_ip(db_cluster):
        return [node_data[1] for node_data in ScyllaManagerTool.get_cluster_hosts_with_ips(db_cluster)]

    @staticmethod
    def get_cluster_hosts_with_ips(db_cluster):
        return [[n, n.ip_address] for n in db_cluster.nodes]

    def add_cluster(self, name, host=None, db_cluster=None, client_encrypt=None, disable_automatic_repair=True,
                    auth_token=None, credentials=None, force_non_ssl_session_port=False, alternator_credentials=None):
        """
        :param name: cluster name
        :param host: cluster node IP
        :param db_cluster: scylla cluster
        :param client_encrypt: is TSL client encryption enable/disable
        :param disable_automatic_repair: when a cluster is added to the manager, a repair task is created automatically.
         This param removes that task.
        :param auth_token: a token used to authenticate requests to the Agent
        :param credentials: a tuple of the username and password that are used to access the cluster.
        :param force_non_ssl_session_port: force SM to always use the non-SSL port for TLS-enabled cluster CQL sessions.
        :param alternator_credentials: a tuple of the alternator access key and secret key that are used to access the Alternator API.
        :return: ManagerCluster

        Add a cluster to manager

        Usage:
          sctool cluster add --host <IP> [--name <alias>] [--auth-token <token>] [flags]

        Scylla Docs:
          https://manager.docs.scylladb.com/stable/add-a-cluster.html
          https://manager.docs.scylladb.com/stable/sctool#cluster-add
        """

        if not any([host, db_cluster]):
            raise ScyllaManagerError("Neither host or db_cluster parameter were given to Manager add_cluster")

        host = host or self.get_cluster_hosts_ip(db_cluster=db_cluster)[0]

        cmd = 'cluster add --host {} --name {} --auth-token {}'.format(host, name, auth_token)

        if force_non_ssl_session_port:
            cmd += " --force-non-ssl-session-port"

        # Adding client-encryption parameters if required
        if client_encrypt:
            if not db_cluster:
                LOGGER.warning("db_cluster is not given. Scylla-Manager connection to cluster may "
                               "fail since not using client-encryption parameters.")
            else:  # check if scylla-node has client-encrypt
                db_node, _ip = self.get_cluster_hosts_with_ips(db_cluster=db_cluster)[0]
                if db_node.is_client_encrypt:
                    cmd += " --ssl-user-cert-file {} --ssl-user-key-file {}".format(SSL_USER_CERT_FILE,
                                                                                    SSL_USER_KEY_FILE)

        if credentials:
            username, password = credentials
            cmd += f" --username {username} --password {password}"

        if alternator_credentials:
            access_key_id, secret_access_key = alternator_credentials
            cmd += f" --alternator-access-key-id='{access_key_id}' --alternator-secret-access-key='{secret_access_key}'"

        res_cluster_add = self.sctool.run(cmd, parse_table_res=False)
        if not res_cluster_add or 'Cluster added' not in res_cluster_add.stderr:
            raise ScyllaManagerError("Encountered an error on 'sctool cluster add' command response: {}".format(
                res_cluster_add))
        cluster_id = res_cluster_add.stdout.split('\n')[0]

        # return ManagerCluster instance with the manager's new cluster-id
        manager_cluster = self.clusterClass(manager_node=self.manager_node, cluster_id=cluster_id,
                                            client_encrypt=client_encrypt)
        if disable_automatic_repair:
            manager_cluster.delete_automatic_repair_task()
        return manager_cluster

    def upgrade(self, scylla_mgmt_upgrade_to_repo):
        manager_from_version = self.sctool.version
        LOGGER.debug('Running Manager upgrade from: {} to version in repo: {}'.format(
            manager_from_version, scylla_mgmt_upgrade_to_repo))
        self.manager_node.upgrade_mgmt(scylla_mgmt_address=scylla_mgmt_upgrade_to_repo)
        new_manager_version = self.sctool.version
        LOGGER.debug('The Manager version after upgrade is: {}'.format(new_manager_version))
        return new_manager_version

    def rollback_upgrade(self, scylla_mgmt_address):
        raise NotImplementedError

    @staticmethod
    def is_force_non_ssl_session_port(db_cluster) -> bool:
        _node = db_cluster.nodes[0]
        return _node.is_client_encrypt and not _node.is_native_transport_port_ssl


class ScyllaManagerToolRedhatLike(ScyllaManagerTool):

    def __init__(self, manager_node):
        ScyllaManagerTool.__init__(self, manager_node=manager_node)
        self.manager_repo_path = '/etc/yum.repos.d/scylla-manager.repo'

    def rollback_upgrade(self, scylla_mgmt_address):

        remove_post_upgrade_repo = dedent("""
                        sudo systemctl stop scylla-manager
                        cqlsh -e 'DROP KEYSPACE scylla_manager'
                        sudo rm -rf {}
                        sudo yum clean all
                        sudo rm -rf /var/cache/yum
                    """.format(self.manager_repo_path))
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % remove_post_upgrade_repo)

        # Downgrade to pre-upgrade scylla-manager repository
        self.manager_node.download_scylla_manager_repo(scylla_mgmt_address)

        downgrade_to_pre_upgrade_repo = dedent("""
                                sudo yum downgrade scylla-manager* -y
                                sleep 2
                                sudo systemctl daemon-reload
                                sudo systemctl restart scylla-manager
                                sleep 3
                            """)
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % downgrade_to_pre_upgrade_repo)

        # Rollback the Scylla Manager database???


class ScyllaManagerToolNonRedhat(ScyllaManagerTool):
    def __init__(self, manager_node):
        ScyllaManagerTool.__init__(self, manager_node=manager_node)
        self.manager_repo_path = '/etc/apt/sources.list.d/scylla-manager.list'

    def rollback_upgrade(self, scylla_mgmt_address):
        manager_from_version = self.sctool.version[0]
        remove_post_upgrade_repo = dedent("""
                        cqlsh -e 'DROP KEYSPACE scylla_manager'
                        sudo systemctl stop scylla-manager
                        sudo systemctl stop scylla-server.service
                        sudo apt-get remove scylla-manager -y
                        sudo apt-get remove scylla-manager-server -y
                        sudo apt-get remove scylla-manager-client -y
                        sudo rm -rf {}
                        sudo apt-get clean
                    """.format(self.manager_repo_path))  # +" /var/lib/scylla-manager/*"))
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % remove_post_upgrade_repo)
        self.manager_node.remoter.run('sudo apt-get update', ignore_status=True)

        # Downgrade to pre-upgrade scylla-manager repository
        self.manager_node.download_scylla_manager_repo(scylla_mgmt_address)
        res = self.manager_node.remoter.run('sudo apt-get update', ignore_status=True)
        res = self.manager_node.remoter.run('apt-cache  show scylla-manager-client | grep Version:')
        rollback_to_version = res.stdout.split()[1]
        LOGGER.debug("Rolling back manager version from: {} to: {}".format(manager_from_version, rollback_to_version))
        # self.manager_node.install_mgmt(scylla_mgmt_address=scylla_mgmt_address)
        downgrade_to_pre_upgrade_repo = dedent("""

                                sudo apt-get install scylla-manager -y
                                sudo systemctl unmask scylla-manager.service
                                sudo systemctl enable scylla-manager
                                echo yes| sudo scyllamgr_setup
                                sudo systemctl restart scylla-manager.service
                                sleep 25
                            """)
        self.manager_node.remoter.run('sudo bash -cxe "%s"' % downgrade_to_pre_upgrade_repo)

        # Rollback the Scylla Manager database???


class SCTool:
    def __init__(self, manager_node):
        self.manager_node = manager_node

    def run(self,
            cmd,
            is_verify_errorless_result=False,
            parse_table_res=True,
            is_multiple_tables=False,
            replace_broken_unicode_values=True):
        LOGGER.debug("Issuing: 'sctool %s'", cmd)
        try:
            res = self.manager_node.remoter.sudo(f"sctool {cmd}")
            LOGGER.debug("sctool output: %s", res.stdout)
        except (InvokeFailure, Libssh2Failure) as ex:
            raise ScyllaManagerError(f"Encountered an error on sctool command: {cmd}: {ex}") from ex

        if replace_broken_unicode_values:
            res.stdout = self.replace_broken_unicode_values(res.stdout)
            # Minor band-aid to fix a unique error with the output of some sctool command
            # (So far - specifically cluster status)

        if is_verify_errorless_result:
            verify_errorless_result(cmd=cmd, res=res)
        if parse_table_res:
            res = self.parse_result_table(res=res)
            if is_multiple_tables:
                dict_res_tables = self.parse_result_multiple_tables(res=res)
                return dict_res_tables
        LOGGER.debug("sctool res after parsing: %s", res)
        return res

    @staticmethod
    def replace_chars_with_line_character(string, chars_to_replace_index_range):
        replaced_string = string[:chars_to_replace_index_range[0]] + '│' + string[chars_to_replace_index_range[1] + 1:]
        return replaced_string

    def replace_broken_unicode_values(self, string):
        while string.find("�") != -1:
            first_broken_char_index = string.find("�")
            for index in range(first_broken_char_index + 1, len(string)):
                if string[index] != "�":
                    break
            broken_character_index_range = [first_broken_char_index, index - 1]
            string = self.replace_chars_with_line_character(string, broken_character_index_range)
        return string

    @staticmethod
    def parse_result_table(res):
        parsed_table = []
        lines = res.stdout.split('\n')
        filtered_lines = [line for line in lines if line]
        if filtered_lines:
            if '╭' in res.stdout:
                filtered_lines = [line.replace('│', "|") for line in filtered_lines if
                                  not line.startswith(('╭', '├', '╰'))]  # filter out the dashes lines
            else:
                filtered_lines = [line for line in filtered_lines if
                                  not line.startswith('+')]  # filter out the dashes lines
        for line in filtered_lines:
            list_line = [s if s else 'EMPTY' for s in line.split("|")]  # filter out spaces and "|" column seperators
            list_line_no_spaces = [s.split() for s in list_line if s != 'EMPTY']
            list_line_multiple_words_join = []
            for words in list_line_no_spaces:
                list_line_multiple_words_join.append(" ".join(words))
            if list_line_multiple_words_join:
                parsed_table.append(list_line_multiple_words_join)
        return parsed_table

    @staticmethod
    def parse_result_multiple_tables(res):
        """

        # Datacenter: us-eastscylla_node_east
        # ╭──────────┬────────────────╮
        # │ CQL      │ Host           │
        # ├──────────┼────────────────┤
        # │ UP (1ms) │ 18.233.164.181 │
        # ╰──────────┴────────────────╯
        # Datacenter: us-west-2scylla_node_west
        # ╭────────────┬───────────────╮
        # │ CQL        │ Host          │
        # ├────────────┼───────────────┤
        # │ UP (180ms) │ 54.245.183.30 │
        # ╰────────────┴───────────────╯
        # the above output example was translated to a single table that includes both 2 DC's values:
        # [['Datacenter: us-eastscylla_node_east'],
        #  ['CQL', 'Host'],
        #  ['UP (1ms)', '18.233.164.181'],
        #  ['Datacenter: us-west-2scylla_node_west'],
        #  ['CQL', 'Host'],
        #  ['UP (180ms)', '54.245.183.30']]
        :param res:
        :return:
        """
        if not any(len(line) == 1 for line in res):  # "1" means a table title like DC-name is found.
            return {"single_table": res}

        dict_res_tables = {}
        cur_table = None
        for line in res:
            if len(line) == 1:  # "1" means it is the table title like DC name.
                cur_table = line[0]
                dict_res_tables[cur_table] = []
            else:
                dict_res_tables[cur_table].append(line)
        return dict_res_tables

    def get_table_value(self, parsed_table, identifier, column_name=None, is_search_substring=False):
        """

        :param parsed_table:
        :param column_name:
        :param identifier:
        :return:
        """

        # example expected parsed_table input is:
        # [['Host', 'Status', 'RTT'],
        #  ['18.234.77.216', 'UP', '0.92761'],
        #  ['54.203.234.42', 'DOWN', '0']]
        # usage flow example: mgr_cluster1.host -> mgr_cluster1.get_property -> get_table_value

        if not parsed_table or not self._is_found_in_table(parsed_table=parsed_table, identifier=identifier,
                                                           is_search_substring=is_search_substring):
            raise ScyllaManagerError(
                "Encountered an error retrieving sctool table value: {} not found in: {}".format(identifier,
                                                                                                 str(parsed_table)))
        column_titles = [title.upper() for title in
                         parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError("Column name: {} not found in table: {}".format(column_name, parsed_table))
        column_name_index = column_titles.index(
            # "1" is used in a case like "task progress" where no column names exist.
            column_name.upper()) if column_name else 1
        ret_val = 'N/A'
        for row in parsed_table:
            if is_search_substring:
                if any(identifier in cur_str for cur_str in row):
                    ret_val = row[column_name_index]
                    break
            elif identifier in row:
                ret_val = row[column_name_index]
                break
        LOGGER.debug("{} {} value is:{}".format(identifier, column_name, ret_val))
        return ret_val

    @staticmethod
    def get_all_column_values_from_table(parsed_table, column_name):
        """
        Receives a parsed table of an sctool command output and a column name that appears in the table
        and returns a list of all of the values of said column in the table
        """
        column_titles = [title.upper() for title in
                         parsed_table[0]]  # get all table column titles capital (for comparison)
        if column_name and column_name.upper() not in column_titles:
            raise ScyllaManagerError("Column name: {} not found in table: {}".format(column_name, parsed_table))

        column_name_index = column_titles.index(column_name.upper())
        column_values = [row[column_name_index] for row in parsed_table[1:]]
        return column_values

    @staticmethod
    def _is_found_in_table(parsed_table, identifier, is_search_substring=False):
        full_rows_list = []
        for row in parsed_table:
            full_rows_list += row
        if is_search_substring:
            return any(identifier in cur_str for cur_str in full_rows_list)

        return identifier in full_rows_list

    @property
    def version(self):
        cmd = "version"
        return self.run(cmd=cmd, is_verify_errorless_result=True)

    @property
    def client_version(self):
        return self.version[0][0].strip("Client version: ")

    @property
    def parsed_client_version(self):
        return LooseVersion(self.client_version)

    @property
    def client_version_timestamp(self) -> int:
        """Gets the timestamp of the client version

        Example, for client version `3.3.3-0.20240912.924034e0d` the timestamp is `1726099200` (2024-09-12 00:00:00)
        """
        date_str = self.client_version.split('.')[-2]
        date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
        timestamp = int(date_obj.timestamp())
        return timestamp

    @property
    def is_v3_cli(self):
        return self.parsed_client_version >= new_command_structure_minimum_version


class ScyllaMgmt:
    """
    Provides communication with scylla-manager via REST API
    """

    def __init__(self, server, port=9090):
        self._url = 'http://{}:{}/api/v1/'.format(server, port)

    def get(self, path, params=None):
        if not params:
            params = {}
        resp = requests.get(url=self._url + path, params=params)
        if resp.status_code not in [200, 201, 202]:
            err_msg = 'GET request to scylla-manager failed! error: {}'.format(resp.content)
            LOGGER.error(err_msg)
            raise Exception(err_msg)
        try:
            return json.loads(resp.content)
        except Exception as ex:  # noqa: BLE001
            LOGGER.error('Failed load data from json %s, error: %s', resp.content, ex)
        return resp.content

    def post(self, path, data):
        resp = requests.post(url=self._url + path, data=json.dumps(data))
        if resp.status_code not in [200, 201]:
            err_msg = 'POST request to scylla-manager failed! error: {}'.format(resp.content)
            LOGGER.error(err_msg)
            raise Exception(err_msg)
        return resp

    def put(self, path, data=None):
        if not data:
            data = {}
        resp = requests.put(url=self._url + path, data=json.dumps(data))
        if resp.status_code not in [200, 201]:
            err_msg = 'PUT request to scylla-manager failed! error: {}'.format(resp.content)
            LOGGER.error(err_msg)
            raise Exception(err_msg)
        return resp

    def delete(self, path):
        resp = requests.delete(url=self._url + path)
        if resp.status_code not in [200, 204]:
            err_msg = 'DELETE request to scylla-manager failed! error: {}'.format(resp.content)
            LOGGER.error(err_msg)
            raise Exception(err_msg)

    def get_cluster(self, cluster_name):
        """
        Get cluster by name
        :param cluster_name: cluster name
        :return: cluster id if found, otherwise None
        """
        resp = self.get('clusters', params={'name': cluster_name})

        if not resp:
            LOGGER.debug('Cluster %s not found in scylla-manager', cluster_name)
            return None
        return resp[0]['id']

    def add_cluster(self, cluster_name, hosts, shard_count=16):
        """
        Add cluster to management
        :param cluster_name: cluster name
        :param hosts: list of cluster node IP-s
        :param shard_count: number of shards in nodes
        :return: cluster id
        """
        cluster_obj = {'name': cluster_name, 'hosts': hosts, 'shard_count': shard_count}
        resp = self.post('clusters', cluster_obj)
        return resp.headers['Location'].split('/')[-1]

    def delete_cluster(self, cluster_id):
        """
        Remove cluster from management
        :param cluster_id: cluster id/name
        :return: nothing
        """
        self.delete('cluster/{}'.format(cluster_id))

    def get_schedule_task(self, cluster_id):
        """
        Find auto scheduling repair task created automatically on cluster creation
        :param cluster_id: cluster id
        :return: task dict
        """
        resp = []
        while not resp:
            resp = self.get(path='cluster/{}/tasks'.format(cluster_id), params={'type': 'repair_auto_schedule'})
        return resp[0]

    def disable_task_schedule(self, cluster_id, task):
        start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)
        task['schedule'] = {'start_date': start_time.isoformat() + 'Z',
                            'interval_days': 0,
                            'num_retries': 0}
        task['enabled'] = True

        self.put(path='cluster/{}/task/repair_auto_schedule/{}'.format(cluster_id, task['id']), data=task)

    def start_repair_task(self, cluster_id, task_id, task_type='repair'):
        self.put(path='cluster/{}/task/{}/{}/start'.format(cluster_id, task_type, task_id))

    def get_repair_tasks(self, cluster_id):
        """
        Get cluster repair tasks(repair units per cluster keyspace)
        :param cluster_id: cluster id
        :return: repair tasks dict with unit id as key, task id as value
        """
        resp = []
        while not resp:
            resp = self.get(path='cluster/{}/tasks'.format(cluster_id), params={'type': 'repair'})
        tasks = {}
        for task in resp:
            unit_id = task['properties']['unit_id']
            if unit_id not in tasks and 'status' not in task:
                tasks[unit_id] = task['id']
        return tasks

    def get_task_progress(self, cluster_id, repair_unit):
        try:
            return self.get(path='cluster/{}/repair/unit/{}/progress'.format(cluster_id, repair_unit))
        except Exception as ex:
            LOGGER.exception('Failed to get repair progress: %s', ex)
        return None

    def run_repair(self, cluster_id, timeout=0):
        """
        Run repair for cluster
        :param cluster_id: cluster id
        :param timeout: timeout in seconds to wait for repair done
        :return: repair status(True/False)
        """
        sched_task = self.get_schedule_task(cluster_id)

        if sched_task['schedule']['interval_days'] > 0:
            self.disable_task_schedule(cluster_id, sched_task)

        self.start_repair_task(cluster_id, sched_task['id'], 'repair_auto_schedule')

        tasks = self.get_repair_tasks(cluster_id)

        status = True
        # start repair tasks one by one:
        # currently scylla-manager cannot execute tasks simultaneously, only one can run
        LOGGER.info('Start repair tasks per cluster %s keyspace', cluster_id)
        unit_to = timeout / len(tasks)
        start_time = time.time()
        for unit, task in tasks.items():
            self.start_repair_task(cluster_id, task, 'repair')
            task_status = self.wait_for_repair_done(cluster_id, unit, unit_to)
            if task_status['status'] != STATUS_DONE or task_status['error']:
                LOGGER.error('Repair unit %s failed, status: %s, error count: %s', unit,
                             task_status['status'], task_status['error'])
                status = False

        LOGGER.debug('Repair finished with status: %s, time elapsed: %s', status, time.time() - start_time)
        return status

    def wait_for_repair_done(self, cluster_id, unit, timeout=0):
        """
        Wait for repair unit task finished
        :param cluster_id: cluster id
        :param unit: repair unit id
        :param timeout: timeout in seconds to wait for repair unit done
        :return: task status dict
        """
        done = False
        status = {'status': 'unknown', 'percent': 0, 'error': 0}
        interval = 10
        wait_time = 0

        LOGGER.info('Wait for repair unit done: %s', unit)
        while not done and wait_time <= timeout:
            time.sleep(interval)
            resp = self.get_task_progress(cluster_id, unit)
            if not resp:
                break
            status['status'] = resp['status']
            status['percent'] = resp['percent_complete']
            if resp['error']:
                status['error'] = resp['error']
            if status['status'] in [STATUS_DONE, STATUS_ERROR]:
                done = True
            LOGGER.debug('Repair status: %s', status)
            if timeout:
                wait_time += interval
                if wait_time > timeout:
                    LOGGER.error('Waiting for repair unit %s: timeout expired: %s', unit, timeout)
        return status
