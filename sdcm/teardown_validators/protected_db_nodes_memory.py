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
from datetime import datetime, timezone
from typing import List, Tuple, Union

from sdcm.cluster import BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent
from sdcm.teardown_validators.base import TeardownValidator
from sdcm.utils.validators.anomalies_detection import detect_isolation_forest_anomalies, bytes_to_readable
from sdcm.utils.decorators import silence


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
            self.get_memory_usage(node)

    def detect_anomalies(self, prom_values: List[Tuple[int, Union[str, int, float]]], node: BaseNode, metric_name: str):
        """
        Detect anomalies in memory usage data using Isolation Forest.
        """
        start = datetime.fromtimestamp(prom_values[0][0], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        end = datetime.fromtimestamp(prom_values[-1][0], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        LOG.debug(f"Analyzing memory usage ({metric_name}) from {start} to {end}...")

        anomalies = detect_isolation_forest_anomalies(
            prom_values,
            filter_score=self.configuration.get('filter_score', 0.70),
            window_seconds=self.configuration.get('window_seconds', 600),
            deviation_threshold=self.configuration.get('deviation_threshold', 0.20),
            ignore_first_minutes=self.configuration.get('ignore_first_minutes', 10),
        )

        if anomalies:
            LOG.warning("Detected memory usage anomalies:")
            event_message = f"Memory usage anomalies detected for {metric_name} on node {node}:\n"
            for index, (timestamp, value), kind, score in anomalies:
                formatted_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
                line = f" - At {index}: {formatted_timestamp} - {bytes_to_readable(value)} ({kind} - score: {score})"
                LOG.warning(line)
                event_message += line + "\n"

            ValidatorEvent(
                message=event_message,
                severity=Severity.ERROR,
            ).publish()

        else:
            LOG.debug("No anomalies detected.")

    @silence(verbose=True)
    def get_memory_usage(self, node: BaseNode) -> None:
        """
        This method queries Prometheus for the scylla_lsa_total_space_bytes metric
        for the given node, summing the values over the test run period.
        """
        query = 'sum(scylla_lsa_total_space_bytes{instance="%s"})' % node.ip_address

        if results := self.tester.prometheus_db.query(query=query, start=self.tester.start_time, end=time.time()):
            self.detect_anomalies(results[0]['values'],
                                  metric_name="scylla_lsa_total_space_bytes", node=node)
