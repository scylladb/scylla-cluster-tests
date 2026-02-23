import re
from datetime import datetime, timedelta
from enum import Enum
import logging
import yaml
from typing import Optional, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from sdcm.mgmt.cli import ManagerTask

from sdcm.utils.distro import Distro
from sdcm.utils.version_utils import find_scylla_repo

DEFAULT_TASK_TIMEOUT = 7200  # 2 hours
LOGGER = logging.getLogger(__name__)
# Regex to extract the backup size from the `sctool progress` command output
# Example output line:
# ╭─────────────┬──────────┬────────────┬────────────┬──────────────┬────────╮
# │ Host        │ Progress │       Size │    Success │ Deduplicated │ Failed │
# ├─────────────┼──────────┼────────────┼────────────┼──────────────┼────────┤
# │ 10.12.4.125 │     100% │ 422.434GiB │ 422.434GiB │           0B │     0B │
# │ 10.12.4.25  │     100% │ 422.413GiB │ 422.413GiB │           0B │     0B │
BACKUP_SIZE_REGEX = re.compile(r".+100% │ (.*?) │ ", re.MULTILINE)

MANAGER_REPO_PATTERNS = {
    "rhel": "https://downloads.scylladb.com/rpm/centos/scylladb-manager-{version}.repo",
    "debian": "https://downloads.scylladb.com/deb/debian/scylladb-manager-{version}.list",
}
MANAGER_REPO_MASTER_LATEST = {
    "rhel": "https://downloads.scylladb.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo",
    "debian": "https://downloads.scylladb.com/manager/deb/unstable/unified-deb/master/latest/scylla-manager.list",
}


def get_persistent_snapshots():  # Snapshot sizes (dict keys) are in GB
    with open("defaults/manager_persistent_snapshots.yaml", encoding="utf-8") as mgmt_snapshot_yaml:
        persistent_manager_snapshots_dict = yaml.safe_load(mgmt_snapshot_yaml)
    return persistent_manager_snapshots_dict


def get_distro_name(distro_object: Distro) -> str:
    if distro_object.is_debian_like:
        return "debian"
    if distro_object.is_rhel_like:
        return "rhel"

    raise ValueError(f"Unsupported distribution for installing manager: {distro_object}")


def duration_to_timedelta(duration_string):
    total_seconds = 0
    if "d" in duration_string:
        total_seconds += int(duration_string[: duration_string.find("d")]) * 86400
        duration_string = duration_string[duration_string.find("d") + 1 :]
    if "h" in duration_string:
        total_seconds += int(duration_string[: duration_string.find("h")]) * 3600
        duration_string = duration_string[duration_string.find("h") + 1 :]
    if "m" in duration_string:
        total_seconds += int(duration_string[: duration_string.find("m")]) * 60
        duration_string = duration_string[duration_string.find("m") + 1 :]
    if "s" in duration_string:
        total_seconds += int(duration_string[: duration_string.find("s")])
    return timedelta(seconds=total_seconds)


def create_cron_list_from_timedelta(minutes=0, hours=0):
    destined_time = datetime.now() + timedelta(hours=hours, minutes=minutes)
    cron_list = [str(destined_time.minute), str(destined_time.hour), "*", "*", "*"]
    return cron_list


def calculate_task_end_time(start_time: str, duration: str) -> datetime:
    """Calculate the end time of Manager task by adding a duration to a start time.

    Args:
        start_time: Start time in format "%d %b %y %H:%M:%S %Z" (e.g., "14 Jun 23 15:41:00 UTC")
        duration: Duration string in format like "2d3h15m30s" where d=days, h=hours, m=minutes, s=seconds
    """
    duration = duration.strip().lower()

    delta = duration_to_timedelta(duration_string=duration)

    base_time = datetime.strptime(start_time, "%d %b %y %H:%M:%S %Z")
    return base_time + delta


class TaskRunDetails(BaseModel):
    """Details of a Manager task run.

    Attributes:
        next_run: The datetime of the next scheduled run
        latest_run_id: The ID of the latest run
        start_time: The start time string from task history
        end_time: The calculated end time as datetime
        duration: The duration string (e.g., "2d3h15m30s")
    """

    next_run: datetime
    latest_run_id: str
    start_time: str
    end_time: datetime
    duration: str


def get_task_run_details(task: "ManagerTask", wait: bool = True, timeout: int = 1000, step: int = 10) -> TaskRunDetails:
    """Get details of the latest task run.

    Args:
        task: The manager task object to get details from
        wait: Whether to wait for task completion before retrieving details
        timeout: Maximum time to wait for task completion (seconds)
        step: Poll interval when waiting (seconds)

    Returns:
        TaskRunDetails object containing task run details
    """
    if wait:
        task.wait_and_get_final_status(timeout=timeout, step=step)

    task_history = task.history
    latest_run_id = task.latest_run_id
    start_time = task.sctool.get_table_value(
        parsed_table=task_history, column_name="start time", identifier=latest_run_id
    )
    next_run_time = datetime.strptime(task.next_run, "%d %b %y %H:%M:%S %Z")  # from `03 Feb 26 16:35:00 UTC`
    duration = task.sctool.get_table_value(parsed_table=task_history, column_name="duration", identifier=latest_run_id)
    end_time = calculate_task_end_time(duration=duration, start_time=start_time)

    task_details = TaskRunDetails(
        next_run=next_run_time,
        latest_run_id=latest_run_id,
        start_time=start_time,
        end_time=end_time,
        duration=duration,
    )
    LOGGER.debug("Task %s details: %s", task.id, task_details)
    return task_details


def get_manager_repo(manager_version: str, distro: Distro) -> str:
    """Build the Scylla Manager repo URL for the given version and distro.

    Constructs the URL from a pattern based on the distro family (RHEL or Debian).
    The special value "master_latest" resolves to the latest unstable build URL.
    Patch version components are stripped (e.g. "3.8.1" -> "3.8").

    Args:
        manager_version: Manager version string, e.g. "3.8", "3.8.1", or "master_latest".
        distro: Distro object representing the target OS.

    Returns:
        Full URL to the repo file.
    """
    # If the version is a patch version, we need to remove the patch part
    if len(version_parts := manager_version.split(".")) == 3:
        manager_version = f"{version_parts[0]}.{version_parts[1]}"

    distro_name = get_distro_name(distro)

    if manager_version == "master_latest":
        return MANAGER_REPO_MASTER_LATEST[distro_name]

    return MANAGER_REPO_PATTERNS[distro_name].format(version=manager_version)


def get_manager_scylla_backend(scylla_backend_version_name: str, distro: Distro) -> str:
    """Find the Scylla backend repo URL for the given version and distro.

    Args:
        scylla_backend_version_name: Scylla version string, e.g. "2025.4" or "2025.4.1".
        distro: Distro object representing the target OS.

    Returns:
        Full URL to the repo file.
    """
    distro_name = get_distro_name(distro)
    return find_scylla_repo(scylla_version=scylla_backend_version_name, dist_type=distro_name)


def reconfigure_scylla_manager(manager_node, logger, values_to_update=(), values_to_remove=()):
    with manager_node.remote_manager_yaml() as scylla_manager_yaml:
        for value in values_to_update:
            scylla_manager_yaml.update(value)
        for value in values_to_remove:
            del scylla_manager_yaml[value]
        logger.info("The new Scylla Manager is:\n%s", scylla_manager_yaml)
    manager_node.restart_manager_server()


class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """


class HostSsl(Enum):
    ON = "ON"
    OFF = "OFF"

    @classmethod
    def from_str(cls, output_str):
        if "SSL" in output_str:
            return HostSsl.ON
        return HostSsl.OFF


class HostStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError as err:
            raise ScyllaManagerError("Could not recognize returned host status: {}".format(output_str)) from err


class HostRestStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"
    UNAUTHORIZED = "UNAUTHORIZED"
    HTTP = "HTTP"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError as err:
            raise ScyllaManagerError("Could not recognize returned host rest status: {}".format(output_str)) from err


class TaskStatus:
    NEW = "NEW"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"
    ERROR_FINAL = "ERROR (4/4)"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    WAITING = "WAITING"
    STARTING = "STARTING"
    ABORTED = "ABORTED"
    SKIPPED = "SKIPPED"

    @classmethod
    def from_str(cls, output_str) -> str:
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError as err:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str)) from err

    @classmethod
    def all_statuses(cls):
        return set(getattr(cls, name) for name in dir(cls) if name.isupper())


class AgentBackupParameters(BaseModel):
    checkers: Optional[int] = 100
    transfers: Optional[int] = 2
    low_level_retries: Optional[int] = 20

    model_config = ConfigDict(arbitrary_types_allowed=False)


def get_backup_size(mgr_cluster, task_id):
    """
    Returns the generated backup size of a given Manager backup Task.
    """
    res = mgr_cluster.sctool.run(cmd=f"progress {task_id} -c {mgr_cluster.id}", parse_table_res=False)
    match = BACKUP_SIZE_REGEX.search(res.stdout)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Backup size not found in the output in {res.stdout}")


class ObjectStorageUploadMode(str, Enum):
    AUTO = "auto"
    RCLONE = "rclone"
    NATIVE = "native"
