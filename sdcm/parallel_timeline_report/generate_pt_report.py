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
import logging
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import List
from enum import Enum
from jinja2 import Environment, FileSystemLoader


LOGGER = logging.getLogger(__name__)

env = Environment(
    loader=FileSystemLoader(Path(__file__).parent.resolve() / 'templates'),
    autoescape=True
)


class EventGroup(Enum):
    NODES_RELATED_EVENTS = ["ScyllaServerStatusEvent", "RepairEvent", "JMXServiceEvent", "DatabaseLogEvent",
                            "BootstrapEvent", "NodetoolEvent", "DisruptionEvent", "InstanceStatusEvent",
                            "CompactionEvent"]
    PROMETHEUS_EVENTS = ["PrometheusAlertManagerEvent"]
    SCT_EVENTS = ["InfoEvent", "ClusterHealthValidatorEvent"]
    STRESS_EVENTS = ["CassandraStressEvent", "CassandraStressLogEvent"]


@dataclass
class Event:
    event_dict: dict
    base: str = field(init=False)
    event_id: str = field(init=False)
    original_node_name: str = field(init=False)
    period_type: str = field(init=False)
    shard: int = field(init=False)
    nemesis_name: str = field(init=False)
    alert_name: str = field(init=False)
    stress_cmd: str = field(init=False)
    message: str = field(init=False)
    nodetool_command: str = field(init=False)
    type: str = field(init=False)
    table: str = field(init=False)
    begin_timestamp: float = field(init=False)
    end_timestamp: float = field(init=False)
    node_name: str = field(init=False)
    cluster_name: str = field(init=False)
    chart_label: str = field(init=False)
    chart_value: str = field(init=False)

    def __post_init__(self):
        self.base = self.event_dict["base"]
        self.event_id = self.event_dict["event_id"]
        self.original_node_name = self.event_dict.get("node")
        self.period_type = self.event_dict.get("period_type")
        self.shard = self.event_dict.get("shard")
        self.nemesis_name = self.event_dict.get("nemesis_name")
        self.alert_name = self.event_dict.get("alert_name")
        self.stress_cmd = self.event_dict.get("stress_cmd")
        self.message = self.event_dict.get("message")
        self.nodetool_command = self.event_dict.get("nodetool_command")
        self.type = self.event_dict["type"]
        self.table = self.event_dict.get("table")
        if self.event_dict.get("period_type") in ["begin", "end"]:
            event_begin_timestamp = self._convert_to_milliseconds(timestamp=self.event_dict.get("begin_timestamp"))
            event_end_timestamp = self._convert_to_milliseconds(timestamp=self.event_dict.get("end_timestamp"))
        else:
            event_begin_timestamp = self._convert_to_milliseconds(timestamp=self.event_dict.get("event_timestamp"))
            event_end_timestamp = event_begin_timestamp
        self.begin_timestamp = event_begin_timestamp
        self.end_timestamp = event_end_timestamp
        self.node_name, self.cluster_name = self._parse_node_name(name_to_parse=self.event_dict.get("node"))
        self.chart_label = self._create_chart_label()
        self.chart_value = self._create_chart_value()

    @staticmethod
    def _convert_to_milliseconds(timestamp: float | None) -> float | None:
        return timestamp * 1000 if timestamp else timestamp

    @staticmethod
    def _parse_node_name(name_to_parse: str | None) -> tuple[str, str] | tuple[None, None]:
        """
        The node names may look like this
        'Node longevity-100gb-4h-master-db-node-6fb3995d-3 [13.49.80.25 | 10.0.1.221] (seed: True)'
        or this
        'longevity-100gb-4h-master-db-node-6fb3995d-3'
        They are split with regex into 3 groups:
        1) is skipped
        2) 'longevity-100gb-4h-master-db-node-6fb3995d' becomes the cluster name
        3) '-3' is used to create new node name like this: 'node-3'
        """
        if name_to_parse:
            if result := re.match(r"(.*\s)?(.*)(-\d+)", name_to_parse):
                node_name = f"node{result.group(3)}"
                cluster_name = result.group(2).replace("node", "cluster")
                return node_name, cluster_name
        return name_to_parse, None

    def _create_chart_label(self) -> str:
        """
        Creates labels for the chart
        """
        if self.event_dict["base"] in ["RepairEvent", "CompactionEvent"]:
            label_string = f"{self.event_dict['base']}, shard: {self.event_dict['shard']}"
        elif self.event_dict["base"] == "DisruptionEvent":
            label_string = f"{self.event_dict['base']}, nemesis: {self.event_dict['nemesis_name']}"
        elif self.event_dict["base"] == "PrometheusAlertManagerEvent":
            label_string = f"node: {self.event_dict['node']}, alert: {self.event_dict['alert_name']}"
        elif self.event_dict["base"] == "CassandraStressEvent":
            label_string = f"cmd: {self.event_dict['stress_cmd']}, node: {self.event_dict['node']}"
        elif self.event_dict["base"] == "CassandraStressLogEvent":
            label_string = self.event_dict['node']
        else:
            label_string = self.event_dict['base']
        return label_string

    def _create_chart_value(self) -> str:
        """
        Creates values for chart's tooltips
        """
        if self.event_dict["base"] == "InfoEvent":
            label_string = f"message: {self.event_dict['message']}"
        elif self.event_dict["base"] == "NodetoolEvent":
            label_string = f"nodetool_command: {self.event_dict['nodetool_command']}"
        elif self.event_dict["base"] == "DisruptionEvent":
            label_string = f"nemesis: {self.event_dict['nemesis_name']}"
        elif self.event_dict["base"] in ["DatabaseLogEvent", "InstanceStatusEvent"]:
            label_string = f"type: {self.event_dict['type']}"
        elif self.event_dict["base"] == "CompactionEvent":
            label_string = f"table: {self.event_dict['table']}"
        else:
            label_string = self.event_dict['base']
        return label_string


class ParallelTimelinesReportGenerator:
    def __init__(self, events_file):
        self.events_file = Path(events_file)
        self.test_id = ""
        self.cluster_name = ""
        self.max_end_timestamp = 0
        self.events = []
        self.chart_data = []
        self.template = "pt_report_template.html"
        self.default_report_file_name = "parallel-timelines-report.html"

    def read_events_file(self) -> None:
        if not self.events_file.exists():
            LOGGER.critical("File \"%s\" not found!", self.events_file)
            sys.exit(1)
        LOGGER.info("Starting to read file \"%s\"...", self.events_file)
        with self.events_file.open(encoding="utf-8") as file:
            for line in file:
                event = Event(event_dict=json.loads(line))
                if event.end_timestamp and event.end_timestamp > self.max_end_timestamp:
                    self.max_end_timestamp = event.end_timestamp
                if not self.cluster_name and event.cluster_name:
                    self.cluster_name = event.cluster_name
                # Getting test_id from the line like this "test_id=fe9c9218-367f-47ba-b59f-0d06c0e81c30"
                if not self.test_id and event.base == "InfoEvent" and "TEST_START" in event.message:
                    self.test_id = event.message.split("=")[-1]
                self.events.append(event)
            LOGGER.info("File \"%s\" has been read successfully. %d rows have been processed.",
                        self.events_file, len(self.events))

    def prepare_scylla_nodes_event_data(self) -> None:
        scylla_nodes_events = self._process_raw_data(events_to_process=EventGroup.NODES_RELATED_EVENTS.value)
        if scylla_nodes_events:
            LOGGER.info("Preparing Scylla node-related event data...")
            scylla_nodes_events_sorted = sorted(scylla_nodes_events,
                                                key=lambda x: (int(x.node_name.split("-")[1]), x.base))

            group_dict = {}
            stat_dict = {}
            for event in scylla_nodes_events_sorted:
                group_dict.setdefault(event.node_name, []).append(event)
                stat_dict[event.base] = stat_dict.setdefault(event.base, 0) + 1

            for key, value in group_dict.items():
                self._process_chart_data(event_list=value, group_name=key)
            stat_string = ', '.join([f"{key}={value}" for key, value in stat_dict.items()])
            LOGGER.info("All Scylla node-related event data have been successfully prepared")
            LOGGER.info("Total number of node-related events processed: %s", stat_string)

    def prepare_prometheus_event_data(self) -> None:
        prometheus_events_data = self._process_raw_data(events_to_process=EventGroup.PROMETHEUS_EVENTS.value)
        if prometheus_events_data:
            prometheus_events_data_sorted = sorted(prometheus_events_data, key=lambda x: (x.original_node_name,
                                                                                          x.alert_name))
            self._process_chart_data(event_list=prometheus_events_data_sorted, group_name="Prometheus events")

    def prepare_sct_event_data(self) -> None:
        sct_events_data = self._process_raw_data(events_to_process=EventGroup.SCT_EVENTS.value)
        if sct_events_data:
            sct_events_data_sorted = sorted(sct_events_data,
                                            key=lambda x: (x.base, x.original_node_name, x.nemesis_name))
            self._process_chart_data(event_list=sct_events_data_sorted, group_name="SCT events")

    def prepare_stress_event_data(self) -> None:
        stress_events_data = self._process_raw_data(events_to_process=EventGroup.STRESS_EVENTS.value)
        if stress_events_data:
            stress_events_data_sorted = sorted(stress_events_data, key=lambda x: (x.base, x.original_node_name,
                                                                                  x.stress_cmd))
            self._process_chart_data(event_list=stress_events_data_sorted, group_name="Stress events")

    def _process_chart_data(self, event_list: List[Event], group_name: str) -> None:
        """
        The structure of self.chart_data should look like this:
        [
            {group: "group1name",
             data: [
                     {label: "label1name",
                      data: [
                                {timeRange: [<date>, <date>],
                                 val: <val: number (continuous dataScale) or string (ordinal dataScale)>},
                                {timeRange: [<date>, <date>],
                                 val: <val>},
                                (...)
                            ]},
                     {label: "label2name",
                      data: [...]},
                     (...)
                   ]},
            {group: "group2name",
             data: [...]},
             (...)
        ]
        Firstly, we create here an appropriate structure of self.chart_data for the given group of events.
        Then we populate created group structure with events' data by calling append_group_data().
        """
        LOGGER.info("Preparing %s data...", group_name)
        stat_dict = {}

        # Prepare self.chart_data structure. Add new group dictionary.
        self.chart_data.append({"group": group_name,
                                "data": []})
        current_chart_group = self.chart_data[-1]["data"]

        # Fill self.chart_data with data
        for event in event_list:
            if event.chart_label not in [item["label"] for item in current_chart_group]:
                current_chart_group.append({"label": event.chart_label,
                                            "data": []})
            self._append_group_data(group_name=group_name,
                                    event_data=event)
            stat_dict[event.base] = stat_dict.setdefault(event.base, 0) + 1
        stat_string = ', '.join([f"{key}={value}" for key, value in stat_dict.items()])
        LOGGER.info("All %s data have been successfully prepared.", group_name)
        LOGGER.info("Number of events processed: %s", stat_string)

    def _append_group_data(self, group_name: str, event_data: Event) -> None:
        group_by_name_list = list(filter(lambda x: x['group'] == group_name, self.chart_data))
        for data in group_by_name_list[0]["data"]:
            if event_data.chart_label == data["label"]:
                data["data"].append({"timeRange": [event_data.begin_timestamp,
                                                   event_data.end_timestamp],
                                     "val": event_data.chart_value})

    def _process_raw_data(self, events_to_process: list) -> List[Event]:
        """
        Finds continuous events with 'begin' records only and evaluates 'end_timestamp' for them.
        If continuous event has both 'begin' and 'end' records, then only 'end' record will be processed.
        """
        processed_events = []
        end_event_id_set = set()
        begin_event_id_set = set()
        begin_events = []
        events_string = ', '.join(events_to_process)
        LOGGER.info("Processing raw data for events: %s.", events_string)
        selected_events = list(filter(lambda x: x.base in events_to_process, self.events))
        for event in selected_events:
            # Exclude DisruptionEvents with nemesis=RunUniqueSequence from processing
            if event.base == "DisruptionEvent" and event.nemesis_name == "RunUniqueSequence":
                continue
            if not event.begin_timestamp:
                LOGGER.warning("Empty begin_timestamp for event name=%s, id=%s", event.base, event.event_id)
            elif not event.end_timestamp and event.period_type == 'end':
                LOGGER.warning("Empty end_timestamp when period_type=end for event name=%s, id=%s", event.base,
                               event.event_id)
            # Processing of event records with period_type = None or period_type in ["end", "one-time"]
            elif event.period_type != "begin":
                if event.period_type == "end":
                    end_event_id_set.add(event.event_id)
                processed_events.append(event)
            # Save the continuous events with period_type = "begin" and their IDs
            else:
                begin_event_id_set.add(event.event_id)
                begin_events.append(event)
        # Check if there are event IDs with period_type = "begin" and without period_type = "end"
        endless_event_id_set = begin_event_id_set.difference(end_event_id_set)
        if endless_event_id_set:
            # If endless events exist, evaluate end_timestamp for them
            endless_events = list(filter(lambda x: x.event_id in endless_event_id_set, begin_events))
            for event in endless_events:
                if event.base in ["ScyllaServerStatusEvent", "JMXServiceEvent"]:
                    event.end_timestamp = self.max_end_timestamp
                else:
                    event.end_timestamp = event.begin_timestamp
                processed_events.append(event)
        return processed_events

    def create_report_file(self) -> None:
        if self.cluster_name:
            report_file_name = self.cluster_name.replace("-db-cluster", "") + "-" + self.default_report_file_name
        else:
            report_file_name = self.default_report_file_name
        report_file = self.events_file.parent / report_file_name
        LOGGER.info("Creating report file \"%s\"", report_file)
        max_line_height = 20
        label_count = 0
        for group in self.chart_data:
            label_count += len(group["data"])
        max_height = max_line_height * label_count + 200
        template = env.get_template(self.template)
        rendered_template = template.render(chart_data=self.chart_data, max_height=max_height,
                                            max_line_height=max_line_height, test_id=self.test_id,
                                            cluster_name=self.cluster_name)
        with report_file.open("w", encoding="utf-8") as file:
            file.write(rendered_template)
        LOGGER.info("Report file has been successfully created")

    def generate_full_report(self):
        self.read_events_file()
        self.prepare_scylla_nodes_event_data()
        self.prepare_prometheus_event_data()
        self.prepare_sct_event_data()
        self.prepare_stress_event_data()
        self.create_report_file()


def setup_logging():
    log_dir = Path("logs")
    log_level = "INFO"
    if not log_dir.exists():
        log_dir.mkdir()
    formatter = logging.Formatter("[%(asctime)s] - (%(levelname)s): %(message)s")
    file_handler = logging.FileHandler(f"{log_dir}/pt_report_generator_"
                                       f"{datetime.now().strftime('%d%m%Y_%H%M%S')}.log")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.setLevel(log_level)


def main():
    setup_logging()
    pt_report_generator = ParallelTimelinesReportGenerator(events_file="raw_events.log")
    pt_report_generator.generate_full_report()


if __name__ == "__main__":
    main()
