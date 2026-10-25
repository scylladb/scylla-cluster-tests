"""Unit tests for sizing the gradual test's dataset from the DB nodes' RAM and data disk."""

import logging
from fractions import Fraction
from types import SimpleNamespace

import pytest
import yaml

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.utils.dataset_sizing import (
    GIB,
    SEQ_RANGE,
    DatasetScale,
    DatasetSizingConfig,
    dataset_scale,
    scale_dataset_params,
    scale_stress_command,
)

TEST_CASE = "test-cases/performance/perf-regression-predefined-throughput-steps.yaml"
CONFIG = DatasetSizingConfig()
# What the reference instance types report: MemTotal is below the nominal 128 GiB, the XFS data disk a bit
# below the nominal 3750 GB.
I4I_4XLARGE = (int(125.6 * GIB), int(3.74e12))


def _test_case_params():
    with open(TEST_CASE, encoding="utf-8") as test_case:
        return yaml.safe_load(test_case)


def _ranges(commands):
    return [tuple(int(bound) for bound in SEQ_RANGE.search(cmd).groups()) for cmd in commands]


@pytest.mark.parametrize(
    "ram_bytes, disk_bytes, expected",
    [
        pytest.param(*I4I_4XLARGE, (1, 1, 1), id="reference_instance_rounds_to_1"),
        pytest.param(int(251 * GIB), int(7.48e12), (2, 2, 2), id="twice_the_reference"),
        pytest.param(int(503 * GIB), int(3.74e12), (4, 1, 1), id="ram_heavy_preload_capped_by_disk"),
        pytest.param(int(62 * GIB), int(7.48e12), (Fraction(1, 2), 2, Fraction(1, 2)), id="disk_heavy"),
        pytest.param(int(15 * GIB), int(0.47e12), (Fraction(1, 4), Fraction(1, 4), Fraction(1, 4)), id="minimum_step"),
    ],
)
def test_dataset_scale(ram_bytes, disk_bytes, expected):
    scale = dataset_scale(ram_bytes=ram_bytes, disk_bytes=disk_bytes, config=CONFIG)
    assert (scale.ram, scale.disk, scale.preload) == expected


def test_reference_instance_keeps_the_test_case_dataset_exactly():
    params = _test_case_params()
    scale = dataset_scale(*I4I_4XLARGE, config=CONFIG)
    scaled = scale_dataset_params(params, scale)

    assert set(scaled) == {
        "stress_cmd_r",
        "stress_cmd_m",
        "stress_cmd_cache_warmup",
        "prepare_write_cmd",
        "stress_cmd_read_disk",
        "stress_cmd_w",
    }
    assert all(scaled[param] == params[param] for param in scaled)


@pytest.mark.parametrize("factor", [Fraction(1, 2), Fraction(5, 4), 2, 4])
@pytest.mark.parametrize("param", ["prepare_write_cmd", "stress_cmd_w", "stress_cmd_r", "stress_cmd_read_disk"])
def test_scaled_ranges_stay_contiguous_and_cover_the_scaled_total(param, factor):
    commands = _test_case_params()[param]
    scaled = [scale_stress_command(cmd, Fraction(factor)) for cmd in commands]

    ranges = _ranges(scaled)
    assert ranges[0][0] == 1
    assert all(next_start == end + 1 for (_, end), (next_start, _) in zip(ranges, ranges[1:]))
    assert ranges[-1][1] == int(_ranges(commands)[-1][1] * Fraction(factor))


def test_row_count_follows_the_scaled_range():
    cmd = (
        "cassandra-stress write cl=ALL n=162500001 -mode connectionsPerHost=8 cql3 native "
        "-col 'size=FIXED(1024) n=FIXED(1)' -pop seq=162500002..325000002"
    )
    scaled = scale_stress_command(cmd, Fraction(2))
    assert "-pop seq=325000003..650000004" in scaled
    assert " n=325000002 " in scaled
    # The column spec is not a row count.
    assert "n=FIXED(1)" in scaled
    assert "connectionsPerHost=8" in scaled


@pytest.mark.parametrize(
    "cmd, expected",
    [
        pytest.param("cassandra-stress write n=1000 -rate threads=10", "cassandra-stress write n=500 -rate threads=10"),
        pytest.param(
            "cassandra-stress read duration=$duration -pop seq=1..5000000",
            "cassandra-stress read duration=$duration -pop seq=1..2500000",
        ),
        pytest.param("latte run --duration 30m workload.rn", "latte run --duration 30m workload.rn"),
    ],
)
def test_commands_without_a_range_or_a_row_count(cmd, expected):
    assert scale_stress_command(cmd, Fraction(1, 2)) == expected


def test_scale_for_each_command():
    scale = DatasetScale(ram=Fraction(4), disk=Fraction(1), preload=Fraction(1))
    assert [scale.for_param(param) for param in ("stress_cmd_r", "prepare_write_cmd", "stress_cmd_w")] == [4, 1, 1]


@pytest.mark.parametrize(
    "sizing_params, expected",
    [
        pytest.param(None, None, id="not_set"),
        pytest.param({}, DatasetSizingConfig(), id="defaults"),
        pytest.param({"scale_step": 0.5}, DatasetSizingConfig(scale_step=0.5), id="override"),
    ],
)
def test_config_from_params(sizing_params, expected):
    assert DatasetSizingConfig.from_params(sizing_params) == expected


def test_test_class_sizes_every_command_from_the_smallest_node():
    def node(ram_kib, disk_bytes):
        def run(cmd):
            return SimpleNamespace(stdout=f"{ram_kib}\n" if "meminfo" in cmd else f"{disk_bytes}\n")

        return SimpleNamespace(remoter=SimpleNamespace(run=run))

    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    params = _test_case_params()
    instance = SimpleNamespace(
        params=params,
        log=logging.getLogger(__name__),
        # The smallest node has twice the reference RAM and disk.
        db_cluster=SimpleNamespace(
            data_nodes=[node(int(251 * GIB / 1024), int(7.48e12)), node(int(503 * GIB / 1024), int(14.9e12))]
        ),
        _node_memory_bytes=test_class._node_memory_bytes,
        _node_data_disk_bytes=test_class._node_data_disk_bytes,
    )
    sized_params = (
        "stress_cmd_r",
        "stress_cmd_m",
        "stress_cmd_cache_warmup",
        "prepare_write_cmd",
        "stress_cmd_read_disk",
        "stress_cmd_w",
    )
    original = {param: list(params[param]) for param in sized_params}

    scale = test_class.size_dataset(instance, DatasetSizingConfig())

    assert (scale.ram, scale.disk, scale.preload) == (2, 2, 2)
    for param in sized_params:
        assert _ranges(params[param])[-1][1] == 2 * _ranges(original[param])[-1][1], param
