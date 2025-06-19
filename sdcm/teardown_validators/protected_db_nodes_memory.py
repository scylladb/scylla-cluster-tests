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

import time
import logging
from pathlib import Path
from datetime import datetime, timezone

from sdcm.cluster import BaseNode
from argus.client.sct.types import LogLink

from sdcm.utils.common import S3Storage
from sdcm.logcollector import GrafanaScreenShot
from sdcm.monitorstack.ui import DetailedLsaTotalMemory
from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent
from sdcm.teardown_validators.base import TeardownValidator


LOG = logging.getLogger(__name__)


class ProtectedDbNodesMemoryValidator(TeardownValidator):
    validator_name = "protected_db_nodes_memory"

    def validate(self):
        """
        Validate memory usage for protected_db_nodes using Prometheus.
        """
        protected_nodes = self.params.get("protected_db_nodes")
        if not protected_nodes:
            LOG.info("No protected_db_nodes configured, skipping memory validation.")
            return

        # Assume protected_nodes is a list of node indexes
        # Map node indexes to node names/ips as needed
        nodes = [node for node in self.tester.db_cluster.nodes if node.is_protected]
        for node in nodes:
            mem_usage = self._get_memory_usage(node)
            LOG.info(f"Node {node} memory usage: {mem_usage} bytes")
            self.take_grafana_memory_screenshot(node)

    def _get_memory_usage(self, node: BaseNode) -> int:
        """
        This method queries Prometheus for the scylla_memory_unspooled_dirty_bytes metric
        for the given node, summing the values over the test run period.
        Returns the total memory usage in bytes as an integer.
        """
        query = 'sum(scylla_memory_unspooled_dirty_bytes{instance="%s"})' % node.ip_address

        if results := self.tester.prometheus_db.query(query=query, start=self.tester.start_time, end=time.time()):
            values = [int(float(value[1])) for value in results[0]["values"]] if results[0]["values"] else []
            if values:
                mean = sum(values) / len(values)
                max_val = max(values)
                min_val = min(values)
                stddev = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
                spike_detected = any(abs(v - mean) > 3 * stddev for v in values)
                if spike_detected:
                    LOG.warning(
                        f"Memory usage anomaly detected on node {node}: mean={mean}, stddev={stddev}, max={max_val}, min={min_val}")
                    ValidatorEvent(
                        message=f"Memory usage anomaly detected on node {node}: mean={mean}, stddev={stddev}, max={max_val}, min={min_val}",
                        severity=Severity.ERROR,
                    ).publish()
                return sum(values)
        return 0

    def take_grafana_memory_screenshot(self, node: BaseNode):
        """
        Takes a screenshot of the Grafana dashboard for the given node, saves it, uploads it to S3,
        and submits the screenshot and its link to Argus.
        """
        LOG.info(f"Taking Grafana screenshot for node {node.ip_address}")

        date_time = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

        screenshot_collector = GrafanaScreenShot(
            name="grafana-screenshot",
            test_start_time=self.tester.start_time,
            extra_params_dict={"var_node", node.ip_address},
        )
        screenshot_collector.grafana_dashboards = [DetailedLsaTotalMemory,]
        screenshot_files = screenshot_collector.collect(self.tester.monitors.nodes[0], self.tester.logdir)
        client = self.tester.test_config.argus_client()

        for screenshot in screenshot_files:
            s3_path = "{test_id}/{date}".format(test_id=self.tester.test_config.test_id(), date=date_time)
            file_url = S3Storage().upload_file(screenshot, s3_path)

            client.submit_sct_logs([LogLink(log_name=Path(screenshot).name, log_link=file_url)])
            client.submit_screenshots([file_url])
