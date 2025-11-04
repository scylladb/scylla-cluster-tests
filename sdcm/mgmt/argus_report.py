from enum import Enum

from docker.errors import InvalidArgument

from argus.client.generic_result import Status
from sdcm import mgmt
from sdcm.argus_results import ManagerBackupReadResult, ManagerBackupBenchmarkResult, submit_results_to_argus
from sdcm.mgmt.cli import BackupTask, ManagerCluster
from sdcm.mgmt.common import get_backup_size


class ManagerReportType(Enum):
    READ = 1
    BACKUP = 2


def report_to_argus(monitors, test_config, report_type: ManagerReportType, data: dict, label: str):
    timestamp = mgmt.get_scylla_manager_tool(manager_node=monitors.nodes[0]).sctool.client_version_timestamp
    if report_type == ManagerReportType.READ:
        table = ManagerBackupReadResult(sut_timestamp=timestamp)
    elif report_type == ManagerReportType.BACKUP:
        table = ManagerBackupBenchmarkResult(sut_timestamp=timestamp)
    else:
        raise InvalidArgument("Unknown report type")

    for key, value in data.items():
        table.add_result(column=key, value=value, row=label, status=Status.UNSET)
    submit_results_to_argus(test_config.argus_client(), table)


def report_manager_backup_results_to_argus(monitors, test_config, label: str, task: BackupTask,
                                           mgr_cluster: ManagerCluster) -> None:
    report = {
        "Size": get_backup_size(mgr_cluster, task.id),
        "Time": int(task.duration.total_seconds()),
    }
    report_to_argus(monitors, test_config, ManagerReportType.BACKUP, report, label)
