from typing import TypedDict

class RawHDRHistogram(TypedDict):
    start_time: int
    percentile_90: float
    percentile_50: float
    percentile_99_999: float
    percentile_95: float
    end_time: float
    percentile_99_99: float
    percentile_99: float
    stddev: float
    percentile_99_9: float

class GeminiResultsRequest(TypedDict):
    oracle_nodes_count: int
    oracle_node_ami_id: str
    oracle_node_instance_type: str
    oracle_node_scylla_version: str
    gemini_command: str
    gemini_version: str
    gemini_status: str
    gemini_seed: str
    gemini_write_ops: int
    gemini_write_errors: int
    gemini_read_ops: int
    gemini_read_errors: int

class PerformanceResultsRequest(TypedDict):
    test_name: str
    stress_cmd: str
    perf_op_rate_average: float
    perf_op_rate_total: float
    perf_avg_latency_99th: float
    perf_avg_latency_mean: float
    perf_total_errors: str

    histograms: list[dict[str, RawHDRHistogram]] | None


class InstanceInfoUpdateRequest(TypedDict):
    provider: str
    region: str
    public_ip: str
    private_ip: str
    dc_name: str
    rack_name: str
    creation_time: int
    termination_time: int
    termination_reason: str
    shards_amount: int


class ResourceUpdateRequest(TypedDict):
    state: str
    instance_info: InstanceInfoUpdateRequest
