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

import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Union

from argus.client.sct.types import LogLink

from sdcm.cluster import BaseNode
from sdcm.utils.common import S3Storage
from sdcm.logcollector import GrafanaScreenShot
from sdcm.monitorstack.ui import DetailedLsaTotalMemory
from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent
from sdcm.teardown_validators.base import TeardownValidator
from sdcm.utils.validators.anomalies_detection import detect_isolation_forest_anomalies, bytes_to_readable
from sdcm.utils.decorators import silence
from sdcm.sct_events.file_logger import get_events_grouped_by_category


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
            self.take_grafana_memory_screenshot(node)

    def get_active_nemesis_names(self, anomaly_timestamps: List[int]) -> dict:
        """
        Returns a dict mapping anomaly timestamps to comma-separated nemesis names active at those times.
        Parses both begin and end event strings to extract nemesis name and timestamp.
        Matches if anomaly timestamp is between begin and end for the same event_id.
        """
        events_by_category = get_events_grouped_by_category(
            _registry=getattr(self.tester, 'events_processes_registry', None))
        begin_events = {}
        end_events = {}
        for event_str in events_by_category.get('NORMAL', []):
            if 'DisruptionEvent' in event_str and 'period_type=begin' in event_str:
                m = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+):.*period_type=begin event_id=([\w-]+): nemesis_name=([\w-]+)", event_str)
                if m:
                    dt_str = m.group(1)
                    event_id = m.group(2)
                    nemesis_name = m.group(3)
                    try:
                        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                        ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
                        begin_events[event_id] = (ts, nemesis_name)
                    except Exception:  # noqa: BLE001
                        continue
            if 'DisruptionEvent' in event_str and 'period_type=end' in event_str:
                m = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+):.*period_type=end event_id=([\w-]+) duration=[^:]*: nemesis_name=([\w-]+)", event_str)
                if m:
                    dt_str = m.group(1)
                    event_id = m.group(2)
                    nemesis_name = m.group(3)
                    try:
                        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                        ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
                        end_events[event_id] = (ts, nemesis_name)
                    except Exception:  # noqa: BLE001
                        continue
        for event_str in events_by_category.get('ERROR', []):
            if 'DisruptionEvent' in event_str and 'period_type=end' in event_str:
                m = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+):.*period_type=end event_id=([\w-]+) duration=[^:]*: nemesis_name=([\w-]+)", event_str)
                if m:
                    dt_str = m.group(1)
                    event_id = m.group(2)
                    nemesis_name = m.group(3)
                    try:
                        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                        ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
                        end_events[event_id] = (ts, nemesis_name)
                    except Exception:  # noqa: BLE001
                        continue
        # For each anomaly timestamp, find active nemesis
        result = {}
        for anomaly_timestamp in anomaly_timestamps:
            active_nemesis = []
            for event_id, (begin_ts, nemesis_name) in begin_events.items():
                end_ts = end_events.get(event_id, (None, None))[0]
                if end_ts:
                    if begin_ts <= anomaly_timestamp <= end_ts:
                        active_nemesis.append(nemesis_name)
                elif begin_ts <= anomaly_timestamp <= begin_ts:
                    active_nemesis.append(nemesis_name)
            result[anomaly_timestamp] = ', '.join(set(active_nemesis)) if active_nemesis else "No nemesis running"
        return result

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
            anomaly_timestamps = [timestamp for _, (timestamp, _), _, _ in anomalies]
            nemesis_map = self.get_active_nemesis_names(anomaly_timestamps)
            for index, (timestamp, value), kind, score in anomalies:
                formatted_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
                nemesis_names = nemesis_map.get(timestamp, "No nemesis running")
                line = f" - At {index}: {formatted_timestamp} - {bytes_to_readable(value)} ({kind} - score: {score}) | Nemesis: {nemesis_names}"
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

    @silence(verbose=True)
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
            extra_params_dict={
                "var-node": node.ip_address,
                "var-by": 'instance',
                "var-cluster": "my-cluster",
                "var-dc": "$__all",
                "var-shard": "$__all",
                "var-func": "sum"
            },
        )
        screenshot_collector.grafana_dashboards = [DetailedLsaTotalMemory,]
        screenshot_files = screenshot_collector.collect(self.tester.monitors.nodes[0], self.tester.logdir)
        client = self.tester.test_config.argus_client()

        for screenshot in screenshot_files:
            s3_path = "{test_id}/{date}".format(test_id=self.tester.test_config.test_id(), date=date_time)
            file_url = S3Storage().upload_file(screenshot, s3_path)

            client.submit_sct_logs([LogLink(log_name=Path(screenshot).name, log_link=file_url)])
            client.submit_screenshots([file_url])
