"""Unit tests for sizing the gradual test's dataset from the DB nodes' RAM and data disk."""

import logging
from fractions import Fraction
from types import SimpleNamespace

import pytest
import yaml

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.utils.dataset_sizing import (
    GIB,
    LATTE_CYCLES,
    LATTE_END,
    LATTE_OFFSET,
    LATTE_ROW_COUNT,
    LATTE_START,
    SEQ_RANGE,
    DatasetScale,
    DatasetSizingConfig,
    dataset_scale,
    scale_dataset_params,
    scale_stress_command,
    scale_stress_rate,
    split_stress_commands,
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


@pytest.mark.parametrize(
    "factor, threads, throttle",
    [
        pytest.param(Fraction(6), 300, 225000, id="24xlarge"),
        pytest.param(Fraction(1, 4), 13, 9375, id="minimum_step"),
        pytest.param(Fraction(1), 50, 37500, id="reference"),
    ],
)
def test_preload_rate_scales_with_its_rows(factor, threads, throttle):
    params = _test_case_params()
    scale = DatasetScale(ram=factor, disk=factor, preload=factor)
    scaled = scale_dataset_params(params, scale)

    for cmd in scaled["prepare_write_cmd"]:
        assert f"-rate threads={threads} throttle={throttle}/s" in cmd
    # The measured steps keep their placeholders.
    assert all("'threads=$threads $throttle'" in cmd for cmd in scaled["stress_cmd_w"] + scaled["stress_cmd_r"])
    assert all("'threads=$threads'" in cmd for cmd in scaled["stress_cmd_cache_warmup"])


def test_scale_stress_rate_leaves_placeholders_and_other_options():
    cmd = "cassandra-stress write n=10 -mode connectionsPerHost=8 cql3 native -rate 'threads=$threads $throttle'"
    assert scale_stress_rate(cmd, Fraction(2)) == cmd


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


@pytest.mark.parametrize("parts", [1, 2, 3, 6, 8])
@pytest.mark.parametrize(
    "param", ["stress_cmd_w", "stress_cmd_r", "stress_cmd_m", "stress_cmd_read_disk", "stress_cmd_cache_warmup"]
)
def test_split_commands_cover_the_same_range_in_equal_parts(param, parts):
    commands = _test_case_params()[param]
    split = split_stress_commands(commands, parts)

    assert len(split) == parts
    ranges = _ranges(split)
    assert ranges[0][0] == _ranges(commands)[0][0]
    assert ranges[-1][1] == _ranges(commands)[-1][1]
    assert all(next_start == end + 1 for (_, end), (next_start, _) in zip(ranges, ranges[1:]))
    lengths = [end - start + 1 for start, end in ranges]
    assert max(lengths) - min(lengths) <= 1
    # Only the range and the row count change.
    assert {SEQ_RANGE.sub("", cmd).split(" n=")[0] for cmd in split} == {SEQ_RANGE.sub("", commands[0]).split(" n=")[0]}


def test_split_row_count_follows_the_range():
    commands = _test_case_params()["stress_cmd_w"]
    split = split_stress_commands(commands, 8)

    assert "n=201326592 " in split[0]
    assert "-pop seq=1..201326592" in split[0]
    assert "-pop seq=1409286145..1610612736" in split[-1]
    assert all("n=FIXED(1)" in cmd for cmd in split)


def test_split_keeps_the_commands_when_they_already_match_the_loaders():
    commands = _test_case_params()["stress_cmd_r"]
    assert split_stress_commands(commands, 4) == commands


@pytest.mark.parametrize(
    "commands",
    [
        pytest.param(
            ["cassandra-stress read -pop seq=1..10", "cassandra-stress mixed -pop seq=11..20"], id="different_commands"
        ),
        pytest.param(["cassandra-stress read -pop seq=1..10", "cassandra-stress read -pop seq=12..20"], id="gap"),
        pytest.param(["latte run --duration 30m workload.rn"], id="no_range"),
        pytest.param(["cassandra-stress read -pop seq=1..2"], id="more_parts_than_rows"),
    ],
)
def test_commands_that_cant_be_split(commands):
    assert split_stress_commands(commands, 3) is None


def _split_per_loader(cs_cmd_tmpl, cs_cmd_warm_up, num_loaders):
    workload = gradual_grow_module.Workload(
        workload_type="read",
        cs_cmd_tmpl=cs_cmd_tmpl,
        cs_cmd_warm_up=cs_cmd_warm_up,
        num_threads=1,
        throttle_steps=[],
        preload_data=True,
        drop_keyspace=False,
        wait_no_compactions=True,
        step_duration="15m",
        prepare_schema=False,
    )
    instance = SimpleNamespace(log=logging.getLogger(__name__))
    return gradual_grow_module.PerformanceRegressionPredefinedStepsTest.split_commands_per_loader(
        instance, workload=workload, num_loaders=num_loaders
    )


@pytest.mark.parametrize("num_loaders", [2, 6])
def test_test_class_gives_every_loader_one_command(num_loaders):
    params = _test_case_params()
    workload = _split_per_loader(params["stress_cmd_r"], params["stress_cmd_cache_warmup"], num_loaders)

    assert len(workload.cs_cmd_tmpl) == num_loaders
    assert len(workload.cs_cmd_warm_up) == num_loaders


def test_test_class_keeps_commands_it_cant_split(caplog):
    commands = ["cassandra-stress read -pop seq=1..10", "cassandra-stress mixed -pop seq=11..20"]
    workload = _split_per_loader(commands, None, 4)

    assert workload.cs_cmd_tmpl == commands
    assert workload.cs_cmd_warm_up is None
    assert "Can't split" in caplog.text


LATTE_TEST_CASE = "test-cases/performance/latte-perf-regression-predefined-throughput-steps-tablets.yaml"
LATTE_TIME_BOUND_WRITE = "configurations/performance/best-throughput/latte_time_bound_write.yaml"


def _latte_params():
    with open(LATTE_TEST_CASE, encoding="utf-8") as test_case:
        params = yaml.safe_load(test_case)
    with open(LATTE_TIME_BOUND_WRITE, encoding="utf-8") as overlay:
        return params | {"stress_cmd_w_time_bound": yaml.safe_load(overlay)["stress_cmd_w"]}


def _latte_values(cmd):
    return {
        name: int(match.group(1)) if (match := pattern.search(cmd)) else None
        for name, pattern in (
            ("start", LATTE_START),
            ("end", LATTE_END),
            ("row_count", LATTE_ROW_COUNT),
            ("offset", LATTE_OFFSET),
            ("cycles", LATTE_CYCLES),
        )
    }


LATTE_PARAMS = [
    "prepare_write_cmd",
    "stress_cmd_w",
    "stress_cmd_w_time_bound",
    "stress_cmd_r",
    "stress_cmd_m",
    "stress_cmd_cache_warmup",
    "stress_cmd_read_disk",
]


def test_latte_reference_instance_keeps_the_test_case_dataset_exactly():
    params = _latte_params()
    scaled = scale_dataset_params(params, dataset_scale(*I4I_4XLARGE, config=CONFIG))
    assert all(scaled[param] == params[param] for param in scaled)


@pytest.mark.parametrize("factor", [Fraction(1, 2), Fraction(5, 4), 2])
@pytest.mark.parametrize("param", LATTE_PARAMS)
def test_latte_ranges_offsets_and_row_counts_scale_together(param, factor):
    commands = _latte_params()[param]
    original = [_latte_values(cmd) for cmd in commands]
    scaled = [_latte_values(scale_stress_command(cmd, factor)) for cmd in commands]

    assert scaled[0]["start"] == 1
    assert all(after["start"] == before["end"] + 1 for before, after in zip(scaled, scaled[1:]))
    assert scaled[-1]["end"] == int(original[-1]["end"] * factor)
    for before, after in zip(original, scaled):
        assert after["offset"] == after["start"] - 1, "the offset is still the first cycle minus 1"
        assert after["row_count"] == int(before["row_count"] * factor)
        if before["cycles"] is not None:
            assert after["cycles"] == after["end"] - after["start"] + 1, "a whole-range duration follows the range"


def test_latte_time_bound_commands_keep_their_duration():
    (cmd, *_) = _latte_params()["stress_cmd_r"]
    scaled = scale_stress_command(cmd, Fraction(1, 2))
    assert "--duration $duration" in scaled
    assert "--threads $threads --concurrency $concurrency $throttle" in scaled


@pytest.mark.parametrize("factor, threads, rate", [(Fraction(1, 2), 30, 46875), (2, 120, 187500)])
def test_latte_preload_rate_scales_with_its_rows(factor, threads, rate):
    (cmd, *_) = _latte_params()["prepare_write_cmd"]
    scaled = scale_stress_rate(cmd, factor)
    assert f"--threads {threads} " in scaled
    assert f"--rate {rate}" in scaled
    assert "--concurrency 15" in scaled


@pytest.mark.parametrize("parts", [2, 3, 8])
@pytest.mark.parametrize("param", ["stress_cmd_w", "stress_cmd_w_time_bound", "stress_cmd_r", "stress_cmd_read_disk"])
def test_latte_split_commands_cover_the_same_range_in_equal_parts(param, parts):
    commands = _latte_params()[param]
    split = split_stress_commands(commands, parts)

    values = [_latte_values(cmd) for cmd in split]
    assert len(split) == parts
    assert values[0]["start"] == _latte_values(commands[0])["start"]
    assert values[-1]["end"] == _latte_values(commands[-1])["end"]
    assert all(after["start"] == before["end"] + 1 for before, after in zip(values, values[1:]))
    assert max(v["end"] - v["start"] for v in values) - min(v["end"] - v["start"] for v in values) <= 1
    for value in values:
        assert value["offset"] == value["start"] - 1
        assert value["row_count"] == _latte_values(commands[0])["row_count"]
        if value["cycles"] is not None:
            assert value["cycles"] == value["end"] - value["start"] + 1


def test_latte_commands_with_another_offset_cant_be_split():
    commands = _latte_params()["stress_cmd_r"]
    shifted = [commands[0].replace("-P offset=0", "-P offset=7"), *commands[1:]]
    assert split_stress_commands(shifted, 2) is None
