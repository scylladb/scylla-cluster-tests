"""Manager backup operations module.

This module provides backup functionality for Scylla Manager clusters
without circular import dependencies.
"""

from sdcm.mgmt import TaskStatus
from sdcm.sct_events.system import InfoEvent


def run_manager_backup(mgr_cluster, locations, object_storage_upload_mode=None, timeout=7200):
    """
    Perform a Manager backup task and wait for its completion.

    Args:
        mgr_cluster: The ManagerCluster object.
        locations: List of backup locations.
        object_storage_upload_mode: The upload mode (e.g., RCLONE or NATIVE).
        timeout: Timeout for the backup task.

    Returns:
        The completed backup task.
    """
    InfoEvent(message=f"Starting a Manager backup (Object Storage Upload Mode: {object_storage_upload_mode})").publish()
    task = mgr_cluster.create_backup_task(
        location_list=locations, rate_limit_list=["0"], object_storage_upload_mode=object_storage_upload_mode
    )
    backup_status = task.wait_and_get_final_status(timeout=timeout)
    assert backup_status == TaskStatus.DONE, "Backup upload has failed!"
    return task
