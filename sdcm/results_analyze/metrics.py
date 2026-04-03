from .base import ClassBase


class MetricBase(ClassBase):
    name = None
    subtype: str = None
    value: float = None
    inverted_betterness: bool = False

    def load_from_es_data(self, es_data):
        if isinstance(es_data, float):
            self.value = es_data

    def save_to_es_data(self):
        return self.value

    def better(self, other):
        if not self._same_type(other):
            raise ValueError("Can be compared only to exact same metric")
        return self.betterness > other.betterness

    def rdiff(self, other, name=None):
        if not self._same_type(other):
            raise ValueError("Can be compared only to exact same metric")
        if name is None:
            return MetricRelativeDiffBase(
                base_value=self.value,
                other_value=other.value,
                inverted_betterness=self.inverted_betterness,
            )
        return MetricRelativeDiffBase(
            base_value=self.value, other_value=other.value, inverted_betterness=self.inverted_betterness, name=name
        )

    def diff(self, other, name=None):
        if not self._same_type(other):
            raise ValueError("Can be compared only to exact same metric")
        if name is None:
            return MetricRelativeDiffBase(
                base_value=self.value,
                other_value=other.value,
                inverted_betterness=self.inverted_betterness,
            )
        return MetricRelativeDiffBase(
            base_value=self.value, other_value=other.value, inverted_betterness=self.inverted_betterness, name=name
        )

    def rate(self, other, name=None):
        if not self._same_type(other):
            raise ValueError("Can be compared only to exact same metric")
        if name is None:
            return MetricRateDiffBase(
                base_value=self.value,
                other_value=other.value,
                inverted_betterness=self.inverted_betterness,
            )
        return MetricRateDiffBase(
            base_value=self.value, other_value=other.value, inverted_betterness=self.inverted_betterness, name=name
        )

    @property
    def betterness(self):
        if self.inverted_betterness:
            return -self.value
        return self.value

    def _same_type(self, other):
        return self.__class__ is other.__class__ and self.subtype == other.subtype

    def is_valid(self):
        return self.value is not None


class MetricDiffBase(MetricBase):
    base_value: float = None
    other_value: float = None
    inverted_betterness: bool = False
    name: str = "diff"
    units = "%"

    @property
    def value(self):
        return self.base_value - self.other_value

    @property
    def abs(self):
        return abs(self.value)

    @property
    def decimal(self):
        return self.base_value - self.other_value

    @property
    def sign(self):
        if self.value >= 0:
            return "+"
        return "-"


class MetricRelativeDiffBase(MetricBase):
    base_value: float = None
    other_value: float = None
    inverted_betterness: bool = False
    name: str = "diff"
    units = "%"

    @property
    def value(self):
        return 100 * (self.base_value - self.other_value) / abs(self.base_value)

    @property
    def abs(self):
        return abs(self.value)

    @property
    def decimal(self):
        return (self.base_value - self.other_value) / abs(self.base_value)

    @property
    def sign(self):
        if self.value >= 0:
            return "+"
        return "-"


class MetricRateDiffBase(MetricBase):
    base_value: float = None
    other_value: float = None
    inverted_betterness: bool = False
    name: str = "diff"
    units = "%"

    @property
    def value(self):
        return 100 * self.other_value / self.base_value

    @property
    def abs(self):
        return abs(self.value)

    @property
    def decimal(self):
        return self.other_value / self.base_value

    @property
    def sign(self):
        if self.value >= 0:
            return "+"
        return "-"


class LatencyMetricBase(MetricBase):
    name = "latency"
    units = "us"
    inverted_betterness = True


class ThroughputMetricBase(MetricBase):
    units = "op/s"


class ThroughputMinScyllaMetric(ThroughputMetricBase):
    name = "scylla throughput (min)"


class ThroughputAvgScyllaMetric(ThroughputMetricBase):
    name = "scylla throughput (avg)"


class ThroughputMaxScyllaMetric(ThroughputMetricBase):
    name = "scylla throughput (max)"


class ThroughputStdevScyllaMetric(ThroughputMetricBase):
    name = "scylla throughput (stdev)"


class ReadLatency99MinScyllaMetric(LatencyMetricBase):
    name = "scylla read latency 99% (min)"


class ReadLatency99AvgScyllaMetric(LatencyMetricBase):
    name = "scylla read latency 99% (avg)"


class ReadLatency99MaxScyllaMetric(LatencyMetricBase):
    name = "scylla read latency 99% (max)"


class ReadLatency99StdevScyllaMetric(LatencyMetricBase):
    name = "scylla read latency 99% (stdev)"


class WriteLatency99MinScyllaMetric(LatencyMetricBase):
    name = "scylla write latency 99% (min)"


class WriteLatency99AvgScyllaMetric(LatencyMetricBase):
    name = "scylla write latency 99% (avg)"


class WriteLatency99MaxScyllaMetric(LatencyMetricBase):
    name = "scylla write latency 99% (max)"


class WriteLatency99StdevScyllaMetric(LatencyMetricBase):
    name = "scylla write latency 99% (stdev)"


class Latency99CassandraStressMetric(LatencyMetricBase):
    """
    Metric read from cassandra stress command results
    """

    name = "c-s latency 99%"


class LatencyMeanCassandraStressMetric(LatencyMetricBase):
    """
    Metric read from cassandra stress command results
    """

    name = "c-s latency mean"


class ThroughputCassandraStressMetric(ThroughputMetricBase):
    """
    Metric read from cassandra stress command results
    """

    name = "c-s throughput"


class MetricsBase(ClassBase):
    _es_data_mapping = {}
    _data_type = None

    def _apply_data(self, data_name, data_type, value):
        if not isinstance(value, float):
            return
        setattr(self, data_name, data_type(value, subtype=data_name))


class ReadLatency99ScyllaMetrics(MetricsBase):
    min: ReadLatency99MinScyllaMetric = None
    avg: ReadLatency99AvgScyllaMetric = None
    max: ReadLatency99MaxScyllaMetric = None
    stdev: ReadLatency99StdevScyllaMetric = None


class WriteLatency99ScyllaMetrics(MetricsBase):
    min: WriteLatency99MinScyllaMetric = None
    avg: WriteLatency99AvgScyllaMetric = None
    max: WriteLatency99MaxScyllaMetric = None
    stdev: WriteLatency99StdevScyllaMetric = None


class ThroughputScyllaMetrics(MetricsBase):
    min: ThroughputMinScyllaMetric = None
    avg: ThroughputAvgScyllaMetric = None
    max: ThroughputMaxScyllaMetric = None
    stdev: ThroughputStdevScyllaMetric = None


class ThroughputCassandraStressMetrics(MetricsBase):
    avg: ThroughputCassandraStressMetric = None


class ScyllaMetrics(ClassBase):
    _es_data_mapping = {"read_latency_99": "latency_read_99", "write_latency_99": "latency_write_99"}
    throughput: ThroughputScyllaMetrics = None
    read_latency_99: ReadLatency99ScyllaMetrics = None
    write_latency_99: WriteLatency99ScyllaMetrics = None
    short_name = "scylla metrics"
    long_name = "Scylla Metrics"
    description = "Metrics gathered directly from scylla server"


class CassandraStressMetrics(ClassBase):
    _es_data_mapping = {
        "latency_99": "stats_average.latency 99th percentile",
        "latency_mean": "stats_average.latency mean",
        "throughput": "stats_average.op rate",
    }
    latency_99: Latency99CassandraStressMetric = None
    latency_mean: LatencyMeanCassandraStressMetric = None
    throughput: ThroughputCassandraStressMetric = None
    short_name = "c-s metrics"
    long_name = "Cassandra Stress Metrics"
    description = "Metrics that produced by cassandra-stress tool"


class ScyllaTestMetrics(ClassBase):
    scylla_metrics: ScyllaMetrics = None
    cs_metrics: CassandraStressMetrics = None
    _es_data_mapping = {
        "scylla_metrics": "results",
        "cs_metrics": "results",
    }

    def is_valid(self):
        return self.scylla_metrics.is_valid() or self.cs_metrics.is_valid()
