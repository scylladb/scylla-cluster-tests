from __future__ import annotations

import logging
import re
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from re import Pattern
from typing import Any

from prettytable import PrettyTable

LOGGER = logging.getLogger(__name__)


@dataclass
class NoSQLBenchSummaryMetric(ABC):
    field_pattern: str
    metrics: str = None
    sanitized_name: str = None

    def __post_init__(self):
        self.sanitized_name = self.field_pattern.replace('.', '_').replace('-', '_').replace('\\', '_')


@dataclass
class Histogram(NoSQLBenchSummaryMetric):
    def __post_init__(self):
        super().__post_init__()
        self.metrics = fr"?(count = (?P<{self.sanitized_name}_count>[\d]+))\n\s+" \
                       fr"?(min = (?P<{self.sanitized_name}_min>[\d\.]+))\n\s+" \
                       fr"?(max = (?P<{self.sanitized_name}_max>[\d\.]+))\n\s+" \
                       fr"?(mean = (?P<{self.sanitized_name}_mean>[\d\.]+))\n\s+" \
                       fr"?(stddev = (?P<{self.sanitized_name}_stddev>[\d\.]+))\n\s+" \
                       fr"?(median = (?P<{self.sanitized_name}_median>[\d\.]+))\n\s+" \
                       fr"?(75% <= (?P<{self.sanitized_name}_p75>[\d\.]+))\n\s+" \
                       fr"?(95% <= (?P<{self.sanitized_name}_p95>[\d\.]+))\n\s+" \
                       fr"?(98% <= (?P<{self.sanitized_name}_p98>[\d\.]+))\n\s+" \
                       fr"?(99% <= (?P<{self.sanitized_name}_p99>[\d\.]+))\n\s+" \
                       fr"?(99.9% <= (?P<{self.sanitized_name}_p999>[\d\.]+))"


@dataclass
class Gauge(NoSQLBenchSummaryMetric):
    def __post_init__(self):
        super().__post_init__()
        self.metrics = fr"?(value = (?P<{self.sanitized_name}>[\d\.]+)\n)"


@dataclass
class Counter(NoSQLBenchSummaryMetric):
    def __post_init__(self):
        super().__post_init__()
        self.metrics = fr"?(count = (?P<{self.sanitized_name}>[\d\.]+)\n)"


@dataclass
class Meter(NoSQLBenchSummaryMetric):
    def __post_init__(self):
        super().__post_init__()
        self.metrics = fr"?(count = (?P<{self.sanitized_name}_count>[\d]+))\n\s+" \
                       fr"?(mean rate = (?P<{self.sanitized_name}_mean_rate>[\d\.]+) events/second)\n\s+" \
                       fr"?(1-minute rate = (?P<{self.sanitized_name}_1_minute_rate>[\d\.]+) events/second)\n\s+" \
                       fr"?(5-minute rate = (?P<{self.sanitized_name}_5_minute_rate>[\d\.]+) events/second)\n\s+" \
                       fr"?(15-minute rate = (?P<{self.sanitized_name}_15_minute_rate>[\d\.]+) events/second)"


@dataclass
class Timer(NoSQLBenchSummaryMetric):
    def __post_init__(self):
        super().__post_init__()
        self.metrics = fr"?(count = (?P<{self.sanitized_name}_count>[\d]+)\n\s+)" \
                       fr"?(mean rate = (?P<{self.sanitized_name}_mean_rate>[\d\.]+) calls/second)\n\s+" \
                       fr"?(1-minute rate = (?P<{self.sanitized_name}_1_minute_rate>[\d\.]+) calls/second)\n\s+" \
                       fr"?(5-minute rate = (?P<{self.sanitized_name}_5_minute_rate>[\d\.]+) calls/second)\n\s+" \
                       fr"?(15-minute rate = (?P<{self.sanitized_name}_15_minute_rate>[\d\.]+) calls/second)\n\s+" \
                       fr"?(min = (?P<{self.sanitized_name}_min>[\d\.]+) microseconds)\n\s+" \
                       fr"?(max = (?P<{self.sanitized_name}_max>[\d\.]+) microseconds)\n\s+" \
                       fr"?(mean = (?P<{self.sanitized_name}_mean>[\d\.]+) microseconds)\n\s+" \
                       fr"?(stddev = (?P<{self.sanitized_name}_stddev>[\d\.]+) microseconds)\n\s+" \
                       fr"?(median = (?P<{self.sanitized_name}_median>[\d\.]*) microseconds)\n\s+" \
                       fr"?(75% <= (?P<{self.sanitized_name}_p75>[\d\.]+) microseconds)\n\s+" \
                       fr"?(95% <= (?P<{self.sanitized_name}_p95>[\d\.]+) microseconds)\n\s+" \
                       fr"?(98% <= (?P<{self.sanitized_name}_p98>[\d\.]+) microseconds)\n\s+" \
                       fr"?(99% <= (?P<{self.sanitized_name}_p99>[\d\.]+) microseconds)\n\s+" \
                       fr"?(99.9% <= (?P<{self.sanitized_name}_p999>[\d\.]+) microseconds)"


all_gauges = [
    Gauge(field_pattern=r"main.cycles.config.burstrate"),
    Gauge(field_pattern=r"main.cycles.config.cyclerate"),
    Gauge(field_pattern=r"main.cycles.waittime"),
    Gauge(field_pattern=r"rampup.cycles.config.burstrate"),
    Gauge(field_pattern=r"rampup.cycles.config.cyclerate"),
    Gauge(field_pattern=r"rampup.cycles.waittime"),
    Gauge(field_pattern=r"schema.cycles.config.burstrate"),
    Gauge(field_pattern=r"schema.cycles.config.cyclerate"),
    Gauge(field_pattern=r"schema.cycles.waittime")
]

all_counters = [
    Counter(field_pattern="main.optracker_blocked"),
    Counter(field_pattern="main.pending_ops"),
    Counter(field_pattern="rampup.optracker_blocked"),
    Counter(field_pattern="rampup.pending_ops"),
    Counter(field_pattern="schema.optracker_blocked"),
    Counter(field_pattern="schema.pending_ops")
]

all_histograms = [
        Histogram(field_pattern=r"main.main-read--main-select--resultset-size"),
        Histogram(field_pattern=r"main.main-read--main-select-01--resultset-size"),
        Histogram(field_pattern=r"main.main-read--main-select-0123--resultset-size"),
        Histogram(field_pattern=r"main.main-read--main-select-0246--resultset-size"),
        Histogram(field_pattern=r"main.main-read--main-select-1357--resultset-size"),
        Histogram(field_pattern=r"main.main-read--main-select-4567--resultset-size"),
        Histogram(field_pattern=r"main.main-read--main-select-all--resultset-size"),
        Histogram(field_pattern=r"main.main-write--main-write--resultset-size"),
        Histogram(field_pattern=r"main.resultset-size"),
        Histogram(field_pattern=r"main.skipped-tokens"),
        Histogram(field_pattern=r"main.tries"),
        Histogram(field_pattern=r"rampup.rampup--rampup-insert--resultset-size"),
        Histogram(field_pattern=r"rampup.resultset-size"),
        Histogram(field_pattern=r"rampup.skipped-tokens"),
        Histogram(field_pattern=r"rampup.tries"),
        Histogram(field_pattern=r"schema.resultset-size"),
        Histogram(field_pattern=r"schema.schema--create-keyspace--resultset-size"),
        Histogram(field_pattern=r"schema.schema--create-table--resultset-size"),
        Histogram(field_pattern=r"schema.skipped-tokens"),
        Histogram(field_pattern=r"schema.tries")
]

histograms = [
    Histogram(field_pattern=r"main.main-read--main-select-all--resultset-size"),
    Histogram(field_pattern=r"main.main-write--main-write--resultset-size"),
    Histogram(field_pattern=r"main.resultset-size"),
    Histogram(field_pattern=r"main.skipped-tokens"),
    Histogram(field_pattern=r"main.tries"),
    Histogram(field_pattern=r"schema.schema--create-keyspace--resultset-size"),
    Histogram(field_pattern=r"schema.schema--create-table--resultset-size")
]

all_meters = [
    Meter(field_pattern=r"main.rows"),
    Meter(field_pattern=r"rampup.rows"),
    Meter(field_pattern=r"schema.rows")
]

meters = [
    Meter(field_pattern=r"main.rows"),
    Meter(field_pattern=r"rampup.rows")
]

all_timers = [
    Timer(field_pattern=r"main.bind"),
    Timer(field_pattern=r"main.cycles.responsetime"),
    Timer(field_pattern=r"main.cycles.servicetime"),
    Timer(field_pattern=r"main.execute"),
    Timer(field_pattern=r"main.main-read--main-select--error"),
    Timer(field_pattern=r"main.main-read--main-select--success"),
    Timer(field_pattern=r"main.main-read--main-select-01--error"),
    Timer(field_pattern=r"main.main-read--main-select-01--success"),
    Timer(field_pattern=r"main.main-read--main-select-0123--error"),
    Timer(field_pattern=r"main.main-read--main-select-0123--success"),
    Timer(field_pattern=r"main.main-read--main-select-0246--error"),
    Timer(field_pattern=r"main.main-read--main-select-0246--success"),
    Timer(field_pattern=r"main.main-read--main-select-1357--error"),
    Timer(field_pattern=r"main.main-read--main-select-1357--success"),
    Timer(field_pattern=r"main.main-read--main-select-4567--error"),
    Timer(field_pattern=r"main.main-read--main-select-4567--success"),
    Timer(field_pattern=r"main.main-read--main-select-all--error"),
    Timer(field_pattern=r"main.main-read--main-select-all--success"),
    Timer(field_pattern=r"main.main-write--main-write--error"),
    Timer(field_pattern=r"main.main-write--main-write--success"),
    Timer(field_pattern=r"main.pages"),
    Timer(field_pattern=r"main.read_input"),
    Timer(field_pattern=r"main.result-success"),
    Timer(field_pattern=r"main.retry-delay"),
    Timer(field_pattern=r"main.strides.servicetime"),
    Timer(field_pattern=r"main.tokenfiller"),
    Timer(field_pattern=r"rampup.bind"),
    Timer(field_pattern=r"rampup.cycles.responsetime"),
    Timer(field_pattern=r"rampup.cycles.servicetime"),
    Timer(field_pattern=r"rampup.execute"),
    Timer(field_pattern=r"rampup.pages"),
    Timer(field_pattern=r"rampup.rampup--rampup-insert--error"),
    Timer(field_pattern=r"rampup.rampup--rampup-insert--success"),
    Timer(field_pattern=r"rampup.read_input"),
    Timer(field_pattern=r"rampup.result"),
    Timer(field_pattern=r"rampup.result-success"),
    Timer(field_pattern=r"rampup.retry-delay"),
    Timer(field_pattern=r"rampup.strides.servicetime"),
    Timer(field_pattern=r"rampup.tokenfiller"),
    Timer(field_pattern=r"schema.bind"),
    Timer(field_pattern=r"schema.cycles.responsetime"),
    Timer(field_pattern=r"schema.cycles.servicetime"),
    Timer(field_pattern=r"schema.execute"),
    Timer(field_pattern=r"schema.pages"),
    Timer(field_pattern=r"schema.read_input"),
    Timer(field_pattern=r"schema.result"),
    Timer(field_pattern=r"schema.result-success"),
    Timer(field_pattern=r"schema.retry-delay"),
    Timer(field_pattern=r"schema.schema--create-keyspace--error"),
    Timer(field_pattern=r"schema.schema--create-keyspace--success"),
    Timer(field_pattern=r"schema.schema--create-table--error"),
    Timer(field_pattern=r"schema.schema--create-table--success"),
    Timer(field_pattern=r"schema.strides.servicetime"),
    Timer(field_pattern=r"schema.tokenfiller")
]

timers = [
    Timer(field_pattern=r"main.bind"),
    Timer(field_pattern=r"main.cycles.servicetime"),
    Timer(field_pattern=r"main.execute"),
    Timer(field_pattern=r"main.main-read--main-select--error"),
    Timer(field_pattern=r"main.main-read--main-select--success"),
    Timer(field_pattern=r"main.main-read--main-select-all--error"),
    Timer(field_pattern=r"main.main-read--main-select-all--success"),
    Timer(field_pattern=r"main.main-write--main-write--error"),
    Timer(field_pattern=r"main.main-write--main-write--success"),
    Timer(field_pattern=r"main.pages"),
    Timer(field_pattern=r"main.read_input"),
    Timer(field_pattern=r"main.result-success"),
    Timer(field_pattern=r"main.retry-delay"),
    Timer(field_pattern=r"main.strides.servicetime")
]


class NoSQLBenchSummaryReportBuilder:
    def __init__(self, summary_file_path: [str | Path]):
        self._path = summary_file_path if isinstance(summary_file_path, Path) else Path(summary_file_path)
        self._raw_summary = None

    def build_summary_report(self) -> list[PrettyTable]:
        with open(self._path, mode="r") as infile:
            self._raw_summary = "".join(infile.readlines())
            summary_report = self._make_pretty_tables(self._split_into_sections())

        LOGGER.info("Final NoSQLBench Summary Report:\n")
        for pt in summary_report:
            LOGGER.info(pt)

        return summary_report

    @staticmethod
    def make_patterns(metrics: list[NoSQLBenchSummaryMetric]) -> Pattern:
        return re.compile(
            ".*".join([f"({metric.field_pattern}).*{metric.metrics}" for metric in metrics]),
            flags=re.DOTALL
        )

    @staticmethod
    def _make_pretty_tables(metrics: dict[str, Any]) -> list[PrettyTable]:
        pretty_tables = []
        for key in metrics.keys():
            table = PrettyTable()
            pretty_tables.append(table)
            table.title = key
            table.add_column("metric", [])
            table.add_column("value", [])
            if metrics[key]:
                for sub_key, sub_value in metrics[key].items():
                    table.add_row([sub_key, sub_value])

        return pretty_tables

    def _split_into_sections(self) -> dict[str, dict[str, str]]:
        groups = {
            "counters": self.make_patterns(all_counters).search(self._raw_summary),
            "histograms": self.make_patterns(histograms).search(self._raw_summary),
            "meters": self.make_patterns(meters).search(self._raw_summary),
            "timers": self.make_patterns(timers).search(self._raw_summary)
        }

        for group in groups:
            if groups[group]:
                g_dict = groups[group].groupdict()
                groups.update({group: g_dict})

        return groups
