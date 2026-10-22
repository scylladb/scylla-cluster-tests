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

import logging
import time

from sdcm.cluster_cloud import ScyllaCloudCluster, ScyllaCloudError
from sdcm.utils.decorators import retrying, Retry

LOGGER = logging.getLogger(__name__)


class ScaleOutFailedException(ScyllaCloudError):
    pass


def wait_for_cluster_disk_utilization(
    cluster: ScyllaCloudCluster, target_utilization: int, check_interval: int = 30, wait_timeout: int = 3600
) -> None:
    """Wait until cluster disk utilization reaches the target percentage."""
    total_storage = cluster.api_client.get_cluster_total_storage(
        account_id=cluster.account_id, cluster_id=cluster.cluster_id
    )

    LOGGER.debug("Waiting for cluster disk utilization to reach %d%%...", target_utilization)
    start_time = time.time()
    while time.time() - start_time < wait_timeout:
        used_storage = cluster.api_client.get_cluster_used_storage(
            account_id=cluster.account_id, cluster_id=cluster.cluster_id
        )
        current_disk_utilization = (used_storage / total_storage) * 100
        if current_disk_utilization >= target_utilization:
            LOGGER.info("Target disk utilization of %d%% reached", target_utilization)
            return

        LOGGER.debug("Current disk utilization: %.2f%%, waiting...", current_disk_utilization)
        time.sleep(check_interval)

    raise TimeoutError(f"Disk utilization did not reach {target_utilization}% within {wait_timeout}s")


@retrying(n=20, sleep_time=60, allowed_exceptions=(Retry,))
def wait_for_resize_request(cluster: ScyllaCloudCluster) -> int:
    requests = cluster.api_client.get_cluster_requests(account_id=cluster.account_id, cluster_id=cluster.cluster_id)
    resize_request = next((r for r in requests if r["requestType"] == "RESIZE_CLUSTER_V3"), None)
    if resize_request and resize_request["status"] == "IN_PROGRESS":
        return resize_request["id"]
    raise Retry("Resize request not found")


@retrying(n=180, sleep_time=60, allowed_exceptions=(AssertionError,))
def wait_for_cluster_scale_out(cluster: ScyllaCloudCluster, request_id: int) -> None:
    request = cluster.api_client.get_cluster_request_details(account_id=cluster.account_id, request_id=request_id)
    status = request["status"]
    if status in ("FAILED", "CANCELLED"):
        raise ScaleOutFailedException("Cluster scale out failed")
    assert status == "COMPLETED", f"Cluster resize is not completed yet. Current status: {status}"
