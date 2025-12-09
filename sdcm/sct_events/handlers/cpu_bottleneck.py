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
# Copyright (c) 2025 ScyllaDB
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING

from sdcm.sct_events.handlers import EventHandler
from sdcm.sct_events.loaders import CassandraStressLogEvent
from sdcm.utils.argus import create_proxy_argus_s3_url
from sdcm.cluster import BaseNode
from sdcm.logcollector import CollectingNode
from sdcm.utils.common import S3Storage
from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3

if TYPE_CHECKING:
    from sdcm.tester import ClusterTester

LOGGER = logging.getLogger(__name__)


def upload_to_s3(node: CollectingNode | BaseNode, test_id: str, files: list | None = None, public: bool = True):
    """
    Uploads specified files from the node to S3 storage.
    """

    snapshot_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info, files, s3_bucket=S3Storage.bucket_name,
            s3_key=f"{test_id}/{snapshot_date}/perf.report-{snapshot_date}-{node.name}.tar.gz",
            max_size_gb=400, public_read_acl=public)
        if s3_link:
            LOGGER.info("Successfully uploaded files from node %s ", node.name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Error while getting and uploading files: %s", exc, exc_info=exc)
        s3_link = ""
    return s3_link


class CpuBottleneckHandler(EventHandler):
    """Handles CPU bottleneck events by running perf on the affected node."""

    # Track last run time per node
    _last_run = {}
    _min_interval_secs = 300  # 5 minutes in seconds
    _perf_sleep_duration_secs = 30  # Duration for perf record sleep in seconds

    def handle(self, event: CassandraStressLogEvent, tester_obj: ClusterTester):  # noqa: F821
        # create new event with details to make them visible in error events log/argus

        for node in list(tester_obj.db_cluster.nodes):
            node: BaseNode
            if event.node == str(node):
                continue

            now = time.time()
            last_run = self._last_run.get(node.name, 0)
            if now - last_run < self._min_interval_secs:
                LOGGER.info("Skipping perf on node %s: last run was %.1f seconds ago", node.name, now - last_run)
                continue
            self._last_run[node.name] = now

            if backend := tester_obj.params.get('xcloud_provider') or tester_obj.params.get('cluster_backend'):
                if backend not in ['aws', 'azure', 'gce']:
                    LOGGER.warning("Skipping %s", )
                    continue  # Skip unsupported backends
                backend = backend.replace('gce', 'gcp')
                node.install_package(f"linux-tools-{backend}")

            LOGGER.info("CPU bottleneck detected on node: %s. Running perf...", node.name)
            try:
                cmd_record = f'perf record -a --call-graph=dwarf -F 99 -p $(pidof scylla) sleep {self._perf_sleep_duration_secs}'
                result_record = node.remoter.sudo(cmd_record, timeout=40)
                LOGGER.debug("perf record completed on node %s: %s", node.name, result_record)

                cmd_report = 'perf report -f --no-children --stdio > perf.report.txt'
                result_report = node.remoter.sudo(cmd_report, timeout=30)
                LOGGER.debug("perf report completed on node %s: %s", node.name, result_report)

                # Upload perf.data and perf.report.txt to s3
                try:
                    link = upload_to_s3(node, tester_obj.test_id, files=['perf.data', 'perf.report.txt'], public=False)
                    event.known_issue = create_proxy_argus_s3_url(link, True)
                except Exception as upload_exc:  # noqa: BLE001
                    LOGGER.error("Failed to upload perf files on node %s: %s", node.name, upload_exc)
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Failed to run perf on node %s: %s", node.name, exc)
