import json
import logging
import re

from pathlib import Path
from collections import defaultdict


LOGGER = logging.getLogger(__name__)
DECODED_STALLS_DIR_NAME = "operations_reactor_stalls"
REACTOR_STALL_MS_REGEXP = re.compile(r" (\d+) ms")


class ReactorStallExtractor:  # pylint: disable=too-few-public-methods

    DISRUPTION_EVENT = "DisruptionEvent"
    INFO_EVENT = "InfoEvent"
    STEADY_STATE = "SteadyState"

    def __init__(self,
                 working_dir: str | Path,
                 raw_events_file: str | Path):
        base_dir = self._convert_str_to_path(working_dir)
        self.raw_events_file = self._convert_str_to_path(raw_events_file)
        self.store_dir = base_dir.joinpath(DECODED_STALLS_DIR_NAME)
        self.reactor_stalls_per_operation = defaultdict(dict)

    @staticmethod
    def _convert_str_to_path(value: str | Path) -> Path:
        if isinstance(value, str):
            return Path(value)
        return value

    def _extract_reactor_stalls_events_by_nemesis(self):
        with open(self.raw_events_file, 'r', encoding='utf-8') as raw_events_file_content:
            current_operation = self.STEADY_STATE
            reactor_stall_per_node = defaultdict(list)
            for line in raw_events_file_content:
                event = json.loads(line)

                if (event['base'] == 'InfoEvent' and 'TEST_END' in event['message']) \
                   or (event['base'] == self.DISRUPTION_EVENT
                        and event['nemesis_name'] == "RunUniqueSequence"
                       and event["period_type"] == "end"):
                    if current_operation in self.reactor_stalls_per_operation:
                        self.reactor_stalls_per_operation[current_operation].update(reactor_stall_per_node)
                    else:
                        self.reactor_stalls_per_operation[current_operation] = reactor_stall_per_node
                    break

                if event['base'] == self.DISRUPTION_EVENT:
                    self.reactor_stalls_per_operation[current_operation] = reactor_stall_per_node
                    reactor_stall_per_node = defaultdict(list)
                    if event['period_type'] == 'begin':
                        current_operation = event['nemesis_name']
                    if event['period_type'] == 'end':
                        current_operation = f"{self.STEADY_STATE}_after_{event['nemesis_name']}"
                    continue

                if event['base'] == 'DatabaseLogEvent' and event['type'].strip() == 'REACTOR_STALLED':
                    node_name = self._parse_node_name(event['node'])
                    reactor_stall_per_node[node_name].append(event['line'])

    @staticmethod
    def _parse_node_name(name: str):
        try:
            return name.split(" ")[1]
        except IndexError:
            return name

    def _save_per_operation_by_node(self):
        for operation, node_data in self.reactor_stalls_per_operation.items():
            operation_dir = self.store_dir.joinpath(operation)
            operation_dir.mkdir(exist_ok=True, parents=True)
            for node in node_data:
                node_file = operation_dir.joinpath(node)
                if not node_data[node]:
                    continue
                with open(node_file, 'w', encoding='utf-8') as encoded_stall_file:
                    encoded_stall_file.write('\n'.join(node_data[node]))

    def extract_reactor_stall_by_nemesis(self):
        self._extract_reactor_stalls_events_by_nemesis()
        self._save_per_operation_by_node()


class ReactorStallCounter:  # pylint: disable=too-few-public-methods
    stall_ms_intervals = [15, 30, 50, 100]

    def __init__(self, working_dir: str | Path):
        self.working_dir = Path(working_dir)
        self._stats = {}

    def count(self):
        reactor_stall_dir = self.working_dir / DECODED_STALLS_DIR_NAME
        for operation_dir in reactor_stall_dir.iterdir():
            self._stats[operation_dir.name] = {}

            for node_path in operation_dir.iterdir():
                with open(node_path, encoding="utf-8") as node_file:
                    for line in node_file:
                        if match := REACTOR_STALL_MS_REGEXP.search(line):
                            reactor_stall_value = int(match.group(1))
                            self._check_ms_interval(operation_dir.name, reactor_stall_value)
        self._sort()
        return self._stats

    def _check_ms_interval(self, operation: str, stall_ms: int):
        for up_border in self.stall_ms_intervals:
            if stall_ms < up_border:
                if up_border not in self._stats[operation]:
                    self._stats[operation][up_border] = 1
                self._stats[operation][up_border] += 1
                break
        else:
            if stall_ms not in self._stats[operation]:
                self._stats[operation][stall_ms] = 1
            self._stats[operation][stall_ms] += 1

    def _sort(self):
        sorted_stats = {}
        for operation, data in self._stats.items():
            if not data:
                continue
            keys = sorted(data.keys())
            sorted_stats[operation] = {key: data[key] for key in keys}
        self._stats = sorted_stats


def count_reactor_stalls_per_operation(working_dir: str | Path, raw_events_file: str | Path):
    ReactorStallExtractor(working_dir, raw_events_file).extract_reactor_stall_by_nemesis()
    return ReactorStallCounter(working_dir).count()
