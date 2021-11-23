from __future__ import annotations

from abc import ABC
from dataclasses import dataclass


@dataclass
class NoSQLBenchSummaryMetric(ABC):
    field_pattern: str
    metrics: str = None
    sanitized_name: str = None

    def __post_init__(self):
        self.sanitized_name = self.field_pattern\
            .replace(r'hdr-[\w]+', '') \
            .replace('.', '_')\
            .replace('-', '_')\
            .replace('\\', '_')


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
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-01--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-0123--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-0246--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-1357--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-4567--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-all--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-write--main-write--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.skipped-tokens"),
    Histogram(field_pattern=r"hdr-[\w]+main.tries"),
    Histogram(field_pattern=r"hdr-[\w]+rampup.rampup--rampup-insert--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+rampup.resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+rampup.skipped-tokens"),
    Histogram(field_pattern=r"hdr-[\w]+rampup.tries"),
    Histogram(field_pattern=r"hdr-[\w]+schema.resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+schema.schema--create-keyspace--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+schema.schema--create-table--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+schema.skipped-tokens"),
    Histogram(field_pattern=r"hdr-[\w]+schema.tries")
]

histograms = [
    Histogram(field_pattern=r"hdr-[\w]+main.main-read--main-select-all--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.main-write--main-write--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+main.skipped-tokens"),
    Histogram(field_pattern=r"hdr-[\w]+main.tries"),
    Histogram(field_pattern=r"hdr-[\w]+schema.schema--create-keyspace--resultset-size"),
    Histogram(field_pattern=r"hdr-[\w]+schema.schema--create-table--resultset-size")
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
    Timer(field_pattern=r"hdr-[\w]+main.bind"),
    Timer(field_pattern=r"hdr-[\w]+main.cycles.responsetime"),
    Timer(field_pattern=r"hdr-[\w]+main.cycles.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+main.execute"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-01--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-01--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-0123--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-0123--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-0246--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-0246--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-1357--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-1357--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-4567--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-4567--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-all--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select-all--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-write--main-write--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-write--main-write--success"),
    Timer(field_pattern=r"hdr-[\w]+main.pages"),
    Timer(field_pattern=r"hdr-[\w]+main.read_input"),
    Timer(field_pattern=r"hdr-[\w]+main.result"),
    Timer(field_pattern=r"hdr-[\w]+main.result-success"),
    Timer(field_pattern=r"hdr-[\w]+main.retry-delay"),
    Timer(field_pattern=r"hdr-[\w]+main.strides.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+main.tokenfiller"),
    Timer(field_pattern=r"hdr-[\w]+rampup.bind"),
    Timer(field_pattern=r"hdr-[\w]+rampup.cycles.responsetime"),
    Timer(field_pattern=r"hdr-[\w]+rampup.cycles.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+rampup.execute"),
    Timer(field_pattern=r"hdr-[\w]+rampup.pages"),
    Timer(field_pattern=r"hdr-[\w]+rampup.rampup--rampup-insert--error"),
    Timer(field_pattern=r"hdr-[\w]+rampup.rampup--rampup-insert--success"),
    Timer(field_pattern=r"hdr-[\w]+rampup.read_input"),
    Timer(field_pattern=r"hdr-[\w]+rampup.result"),
    Timer(field_pattern=r"hdr-[\w]+rampup.result-success"),
    Timer(field_pattern=r"hdr-[\w]+rampup.retry-delay"),
    Timer(field_pattern=r"hdr-[\w]+rampup.strides.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+rampup.tokenfiller"),
    Timer(field_pattern=r"hdr-[\w]+schema.bind"),
    Timer(field_pattern=r"hdr-[\w]+schema.cycles.responsetime"),
    Timer(field_pattern=r"hdr-[\w]+schema.cycles.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+schema.execute"),
    Timer(field_pattern=r"hdr-[\w]+schema.pages"),
    Timer(field_pattern=r"hdr-[\w]+schema.read_input"),
    Timer(field_pattern=r"hdr-[\w]+schema.result"),
    Timer(field_pattern=r"hdr-[\w]+schema.result-success"),
    Timer(field_pattern=r"hdr-[\w]+schema.retry-delay"),
    Timer(field_pattern=r"hdr-[\w]+schema.schema--create-keyspace--error"),
    Timer(field_pattern=r"hdr-[\w]+schema.schema--create-keyspace--success"),
    Timer(field_pattern=r"hdr-[\w]+schema.schema--create-table--error"),
    Timer(field_pattern=r"hdr-[\w]+schema.schema--create-table--success"),
    Timer(field_pattern=r"hdr-[\w]+schema.strides.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+schema.tokenfiller")
]

timers = [
    Timer(field_pattern=r"hdr-[\w]+main.cycles.servicetime"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-read--main-select--success"),
    Timer(field_pattern=r"hdr-[\w]+main.main-write--main-write--error"),
    Timer(field_pattern=r"hdr-[\w]+main.main-write--main-write--success"),
    Timer(field_pattern=r"hdr-[\w]+main.read_input"),
    Timer(field_pattern=r"hdr-[\w]+main.result"),
    Timer(field_pattern=r"hdr-[\w]+main.result-success"),
    Timer(field_pattern=r"hdr-[\w]+main.strides.servicetime")
]

abridged_meters = ["main_rows",
                   "rampup_rows"]

abridged_timers = ["main_cycles_servicetime",
                   "main_strides_servicetime",
                   "main_main_read__main_select__error",
                   "main_main_read__main_select__success",
                   "main_main_write__main_write__error",
                   "main_main_write__main_write__success",
                   "main_result",
                   "main_result_success",
                   ]

abridged_histograms = ["main_main_read__main_select_all__resultset_size",
                       "main.main-write--main-write--resultset-size",
                       "main.resultset-size",
                       "main.tries"]
