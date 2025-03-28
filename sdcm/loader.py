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
# Copyright (c) 2016 ScyllaDB

import os
import re
from abc import abstractmethod, ABCMeta
import time
import logging
from typing import NamedTuple

from sdcm.prometheus import NemesisMetrics
from sdcm.utils.common import FileFollowerThread, convert_metric_to_ms
from sdcm.utils.hdrhistogram import (
    make_hdrhistogram_summary_from_log_line,
)

LOGGER = logging.getLogger(__name__)


class MetricsPosition(NamedTuple):
    ops: int
    lat_mean: int
    lat_med: int
    lat_perc_95: int
    lat_perc_99: int
    lat_perc_999: int
    lat_max: int
    errors: int


class HDRPositions(NamedTuple):
    lat_perc_50: int
    lat_perc_90: int
    lat_perc_99: int
    lat_perc_999: int
    lat_perc_9999: int


# pylint: disable=too-many-instance-attributes
class StressExporter(FileFollowerThread, metaclass=ABCMeta):
    METRICS_GAUGES = {}
    METRIC_NAMES = ['lat_mean', 'lat_med', 'lat_perc_95', 'lat_perc_99', 'lat_perc_999', 'lat_max']

    # pylint: disable=too-many-arguments
    def __init__(self, instance_name: str, metrics: NemesisMetrics, stress_operation: str,
                 stress_log_filename: str, loader_idx: int, cpu_idx: int = 1, keyspace: str = ''):
        super().__init__()
        self.metrics = metrics
        self.stress_operation = stress_operation
        self.stress_log_filename = stress_log_filename
        gauge_name = self.create_metrix_gauge()
        self.stress_metric = self.METRICS_GAUGES[gauge_name]
        self.instance_name = instance_name
        self.loader_idx = loader_idx
        self.cpu_idx = cpu_idx
        self.metrics_positions = self.metrics_position_in_log()
        self.keyspace = keyspace
        self.init()

    def init(self):
        pass

    @abstractmethod
    def metrics_position_in_log(self) -> MetricsPosition:
        ...

    @abstractmethod
    def create_metrix_gauge(self) -> str:
        ...

    def set_metric(self, name: str, value: float) -> None:
        self.stress_metric.labels(0, self.instance_name, self.loader_idx, self.cpu_idx, name, self.keyspace).set(value)

    def clear_metrics(self) -> None:
        if self.stress_metric:
            for metric_name in self.metrics_positions._fields:
                self.set_metric(metric_name, 0.0)

    @staticmethod
    @abstractmethod
    def skip_line(line: str) -> bool:
        ...

    @staticmethod
    @abstractmethod
    def split_line(line: str) -> list:
        ...

    def get_metric_value(self, columns: list, metric_name: str) -> str:
        try:
            value = columns[getattr(self.metrics_positions, metric_name)]
        except AttributeError:
            value = ''
        except (ValueError, IndexError) as exc:
            value = ''
            LOGGER.warning("Failed to get %s metric value. Error: %s", metric_name, str(exc))

        return value

    def run(self):
        while not self.stopped():
            exists = os.path.isfile(self.stress_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line in self.follow_file(self.stress_log_filename):
                if self.stopped():
                    break

                if self.skip_line(line=line):
                    continue

                cols = self.split_line(line=line)

                for metric in self.METRIC_NAMES:
                    if metric_value := self.get_metric_value(columns=cols, metric_name=metric):
                        self.set_metric(metric, convert_metric_to_ms(str(metric_value)))

                if ops := self.get_metric_value(columns=cols, metric_name='ops'):
                    self.set_metric('ops', float(ops))

                if errors := self.get_metric_value(columns=cols, metric_name='errors'):
                    self.set_metric('errors', int(errors))


class CassandraStressExporter(StressExporter):
    def init(self):
        self.keyspace_regex = re.compile(r'.*Keyspace:\s(.*?)$')

    def create_metrix_gauge(self):
        gauge_name = f'sct_cassandra_stress_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for cassandra stress metrics',
                [f'cassandra_stress_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name

    def metrics_position_in_log(self) -> MetricsPosition:
        return MetricsPosition(ops=2, lat_mean=5, lat_med=6, lat_perc_95=7, lat_perc_99=8, lat_perc_999=9,
                               lat_max=10, errors=13)

    def skip_line(self, line: str) -> bool:
        if not self.keyspace:
            if 'Keyspace:' in line:
                self.keyspace = self.keyspace_regex.match(line).groups()[0]
                return True
        # If line starts with 'total,' - skip this line
        return not 'total,' in line

    @staticmethod
    def split_line(line: str) -> list:
        return [element.strip() for element in line.split(',')]


class LatteKeyspaceHolder:
    """Allows to reuse Keyspace for the Latte exporters."""

    def __init__(self):
        self._value = ''

    def set_value(self, v):
        self._value = v

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"LatteKeyspaceHolder({self._value!r})"

    def __getitem__(self, index):
        return self._value[index]

    def __len__(self):
        return len(self._value)

    def __iter__(self):
        return iter(self._value)


class LatteExporter(StressExporter):
    def init(self):
        self.keyspace_regex = re.compile(r'.*Keyspace:\s(.*?)$')

    def create_metrix_gauge(self):
        gauge_name = f'sct_latte_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for latte metrics',
                [f'latte_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name

    def metrics_position_in_log(self) -> MetricsPosition:
        # Example:
        #  Time  Cycles  Errors  Thrpt.  ───────────────────── Latency [ms/op] ─────────────────
        #   [s]    [op]    [op]  [op/s]     Min     50     75     90     95     99   99.9    Max
        # 1.001     299       0     299   0.202  1.626  1.934  2.269  2.427  2.632  3.369  3.369
        return MetricsPosition(
            ops=3,
            # NOTE: latte doesn't print out the 'mean' values.
            # So, set the 'lat_mean' column to store 'P90' value and consider it as a 'placeholder'
            lat_mean=7,
            lat_med=5,
            lat_perc_95=8,
            lat_perc_99=9,
            lat_perc_999=10,
            lat_max=11,
            errors=2,
        )

    def skip_line(self, line: str) -> bool:
        if not self.keyspace and self.keyspace_regex.match(line):
            ks = self.keyspace_regex.match(line).groups()[0].strip()
            LOGGER.debug("Found following keyspace in the latte command: '%s'", ks)
            self.keyspace.set_value(ks)
            return True

        # NOTE: all latency data lines consist of digits only.
        if columns := line.split():
            for column in columns:
                try:
                    float(column)
                except ValueError:
                    return True
        else:
            return True

        # NOTE: 'Keyspace: foo' line print depends on the rune script implementation.
        #       So, if it is absent then set it to be 'unknown'.
        if not self.keyspace:
            self.keyspace.set_value('unknown')
        return False

    @staticmethod
    def split_line(line: str) -> list:
        ret = line.split()
        if len(ret) != 12:
            LOGGER.error(
                "'%s' line got splitted in the following unexpected list: %s",
                line, ret)
        return ret


class CassandraStressHDRExporter(StressExporter):
    METRIC_NAMES = ['lat_perc_50', 'lat_perc_90', 'lat_perc_99', 'lat_perc_999', "lat_perc_9999"]

    def __init__(self, hdr_tags: list[str], instance_name: str, metrics: NemesisMetrics, stress_operation: str,
                 stress_log_filename: str, loader_idx: int, cpu_idx: int = 1, keyspace: str = ''):
        super().__init__(
            instance_name, metrics, stress_operation, stress_log_filename, loader_idx, cpu_idx, keyspace)
        self.log_start_time = 0
        self.hdr_tags = hdr_tags
        self.current_line_hdr_tag = ''

    def create_metrix_gauge(self):
        gauge_name = f'collectd_cassandra_stress_hdr_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for cassandra stress hdr percentiles',
                [f'cassandra_stress_hdr_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', "keyspace"])
        return gauge_name

    def metrics_position_in_log(self) -> HDRPositions:
        return HDRPositions(lat_perc_50=3, lat_perc_90=4,
                            lat_perc_99=6, lat_perc_999=7, lat_perc_9999=8)

    def skip_line(self, line: str) -> bool:
        if match := re.match(r"^#\[StartTime:\s(\d+)", line):
            self.log_start_time = int(match.group(1))
        for hdr_tag in self.hdr_tags:
            if line.startswith(f"Tag={hdr_tag}"):
                return False
        return True

    def set_metric(self, name: str, value: float) -> None:
        self.stress_metric.labels(self.current_line_hdr_tag, self.instance_name, self.loader_idx,
                                  self.cpu_idx, name, self.keyspace).set(value)

    def split_line(self, line: str) -> list:
        summary_data = make_hdrhistogram_summary_from_log_line(
            hdr_tags=self.hdr_tags, stress_operation=self.stress_operation,
            log_line=line, hst_log_start_time=self.log_start_time)
        self.current_line_hdr_tag, percentiles = summary_data.popitem()
        return list(percentiles.values())


class LatteHDRExporter(CassandraStressHDRExporter):
    def create_metrix_gauge(self):
        gauge_name = f'collectd_latte_hdr_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for latte hdr percentiles',
                [f'latte_hdr_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name


class CqlStressHDRExporter(CassandraStressHDRExporter):
    def create_metrix_gauge(self):
        gauge_name = f'collectd_cql_stress_hdr_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for cql-stress hdr percentiles',
                [f'cql_stress_hdr_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name


class CqlStressCassandraStressExporter(StressExporter):
    # Lines containing any of these should be skipped. These are the logs emitted by the `tracing` crate.
    TRACING_LOGS = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR']

    # pylint: disable=too-many-arguments

    def __init__(self, instance_name: str, metrics: NemesisMetrics, stress_operation: str, stress_log_filename: str,
                 loader_idx: int, cpu_idx: int = 1):

        self.keyspace_regex = re.compile(r'.*Keyspace:\s(.*?)$')
        self.do_skip = True
        super().__init__(instance_name, metrics, stress_operation, stress_log_filename, loader_idx,
                         cpu_idx)

    def create_metrix_gauge(self):
        gauge_name = f'sct_cql_stress_cassandra_stress_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for cql-stress cassandra stress metrics',
                [f'cql_stress_cassandra_stress_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name

    def metrics_position_in_log(self) -> MetricsPosition:
        """
        total ops ,    op/s,    mean,     med,     .95,     .99,    .999,     max,   time, errors
            30645,   30602,     0.3,     0.2,     0.7,     1.1,     1.7,     2.4,    1.0,      0
            62965,   32336,     0.3,     0.3,     0.6,     0.7,     0.9,     1.3,    2.0,      0
            99175,   36203,     0.3,     0.2,     0.5,     0.7,     0.9,     1.2,    3.0,      0
        """
        return MetricsPosition(ops=1, lat_mean=2, lat_med=3, lat_perc_95=4, lat_perc_99=5, lat_perc_999=6,
                               lat_max=7, errors=9)

    def skip_line(self, line: str) -> bool:
        if not self.keyspace:
            if 'Keyspace:' in line:
                self.keyspace = self.keyspace_regex.match(line).groups()[0]

        if "total ops ," in line:
            # Stats header has been printed - start collecting the metrics.
            self.do_skip = False
            return True

        for tracing_log in self.TRACING_LOGS:
            if tracing_log in line:
                return True

        if "Results:" in line:
            # Stress test finished.
            self.do_skip = True

        return self.do_skip

    @staticmethod
    def split_line(line: str) -> list:
        return [element.strip() for element in line.split(',')]


class ScyllaBenchStressExporter(StressExporter):

    def create_metrix_gauge(self) -> str:
        gauge_name = f'sct_scylla_bench_stress_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for scylla-bench stress metrics',
                [f'scylla_bench_stress_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name

    # pylint: disable=line-too-long
    def metrics_position_in_log(self) -> MetricsPosition:
        # Enumerate stress metric position in the log. Example:
        # time  operations/s    rows/s   errors  max   99.9th   99th      95th     90th       median        mean
        # 1.033603151s    3439    34390    0  71.434239ms   70.713343ms   62.685183ms    2.818047ms  1.867775ms 1.048575ms  2.947276ms

        return MetricsPosition(ops=1, lat_mean=10, lat_med=9, lat_perc_95=7, lat_perc_99=6, lat_perc_999=5,
                               lat_max=4, errors=3)

    # pylint: disable=line-too-long
    def skip_line(self, line) -> bool:
        # If line is not starts with numeric ended by "s" - skip this line.
        # Example:
        #    Client compression:  true
        #    1.004777157s       2891    28910     0  67.829759ms   64.290815ms    58.327039ms    4.653055ms   3.244031µs   1.376255µs

        line_splitted = (line or '').split()
        if not line_splitted or not line_splitted[0].endswith('s'):
            return True  # skip the line

        try:
            _ = float(line_splitted[0][:-1])
            return False  # The line hold the metrics, don't skip the line
        except ValueError:
            return True  # skip the line

    @staticmethod
    def split_line(line: str) -> list:
        return [element.strip() for element in line.split()]


class CassandraHarryStressExporter(StressExporter):

    # pylint: disable=too-many-arguments,useless-super-delegation
    def __init__(self, instance_name: str, metrics: NemesisMetrics, stress_operation: str, stress_log_filename: str,
                 loader_idx: int, cpu_idx: int = 1):

        super().__init__(instance_name, metrics, stress_operation, stress_log_filename,
                         loader_idx, cpu_idx)

    def create_metrix_gauge(self) -> str:
        gauge_name = f'sct_cassandra_harry_stress_{self.stress_operation}_gauge'
        if gauge_name not in self.METRICS_GAUGES:
            self.METRICS_GAUGES[gauge_name] = self.metrics.create_gauge(
                gauge_name,
                'Gauge for scylla-bench stress metrics',
                [f'scylla_bench_stress_{self.stress_operation}', 'instance', 'loader_idx', 'cpu_idx', 'type', 'keyspace'])
        return gauge_name

    def metrics_position_in_log(self) -> MetricsPosition:
        pass

    def skip_line(self, line) -> bool:
        return not 'Reorder buffer size has grown up to' in line

    @staticmethod
    def split_line(line: str) -> list:
        return line.split()
