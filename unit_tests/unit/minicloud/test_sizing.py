"""Tests for the guest sizes derived from the host (the `auto` value of the minicloud size knobs)."""

import dataclasses
import json
from unittest.mock import patch

import pytest

from sdcm.utils.minicloud.config import AUTO_SIZE_PARAMS, MinicloudConfig, MinicloudError
from sdcm.utils.minicloud.preflight import MemoryBudget
from sdcm.utils.minicloud.sizing import (
    derive_db_memory,
    derive_db_vcpus,
    derive_loader_memory,
    derive_loader_vcpus,
    load_sizing_record,
    resolve_auto_sizing,
)
from unit_tests.unit.minicloud.conftest import _meminfo_path_patch

PARAMS = {"n_db_nodes": 6, "n_loaders": 1, "n_monitor_nodes": 1}


@pytest.fixture
def minicloud_config(tmp_path):
    return MinicloudConfig(
        state_dir=str(tmp_path), lightweight=True, lightweight_memory_loader="2GiB", lightweight_memory_monitor="2GiB"
    )


def _budget(gib):
    return MemoryBudget(gib=gib, source="the test host", headroom_gib=10.0)  # 2GiB + 8 guests


def test_derive_db_memory_steps_down_and_keeps_the_margin(minicloud_config):
    assert derive_db_memory(minicloud_config, PARAMS, 8, _budget(45.0), db_vcpus=2) == "4GiB"


def test_derive_db_memory_grows_up_to_a_raised_ceiling(minicloud_config):
    # 6 x 9 + 2 + 2 + 10 headroom + 2 margin = 70GiB; 10GiB would need 76
    params = {**PARAMS, "minicloud_auto_memory_max": "16GiB"}
    assert derive_db_memory(minicloud_config, params, 8, _budget(70.0), db_vcpus=2) == "9GiB"


def test_derive_db_memory_fails_with_the_numbers_below_the_floor(minicloud_config):
    with pytest.raises(MinicloudError, match=r"4GiB db guests.*40\.0GiB needed.*39\.0GiB.*1\.0GiB short"):
        derive_db_memory(minicloud_config, PARAMS, 8, _budget(39.0), db_vcpus=2)


def test_derive_db_vcpus_grows_up_to_a_raised_ceiling_while_the_host_has_threads(minicloud_config):
    params = {**PARAMS, "minicloud_auto_vcpus_max": 4, "minicloud_auto_memory_max": "16GiB"}
    # 6 db x 4 + monitor 4 + loader 2 = 30 threads; at 3 vCPUs 23 would still not fit 16
    assert derive_db_vcpus(minicloud_config, params, list(AUTO_SIZE_PARAMS), 8, _budget(100.0), host_threads=32) == 4
    assert derive_db_vcpus(minicloud_config, params, list(AUTO_SIZE_PARAMS), 8, _budget(100.0), host_threads=16) == 2


def test_derive_db_vcpus_counts_an_explicit_loader_size(minicloud_config):
    params = {**PARAMS, "minicloud_auto_vcpus_max": 4, "minicloud_auto_memory_max": "16GiB"}
    params["minicloud_lightweight_vcpus_loader"] = 8
    derived = ["minicloud_lightweight_memory", "minicloud_lightweight_vcpus"]
    # 6 db x 4 + monitor 4 + loader 8 = 36 threads; at 3 vCPUs 29 fit 32
    assert derive_db_vcpus(minicloud_config, params, derived, 8, _budget(100.0), host_threads=32) == 3


def test_derive_loader_memory_counts_a_db_size_that_is_not_whole_gib(minicloud_config):
    # 6 x 4.5 + loader 3 + monitor 2 + 10 headroom + 2 margin = 44GiB; a 4GiB loader would need 45
    assert derive_loader_memory(minicloud_config, PARAMS, 8, _budget(44.0), "4.5GiB") == "3GiB"


def test_derive_loader_vcpus_never_goes_below_two():
    assert derive_loader_vcpus(host_threads=12, db_guests=6, db_vcpus=2, loaders=1, monitors=1, monitor_vcpus=1) == 2


AUTO_PARAMS = {
    "minicloud_lightweight_memory": "auto",
    "minicloud_lightweight_vcpus": "auto",
    "minicloud_lightweight_vcpus_loader": "auto",
    "minicloud_lightweight_memory_loader": "2GiB",
    "minicloud_lightweight_memory_monitor": "2GiB",
    "n_db_nodes": 6,
    "n_loaders": 1,
    "n_monitor_nodes": 1,
}


@pytest.fixture
def resolve(tmp_path):
    def _resolve(running_cmd=None, **overrides):
        params = {**AUTO_PARAMS, "minicloud_state_dir": str(tmp_path), **overrides}
        record = resolve_auto_sizing(params, MinicloudConfig.from_env(params=params), running_cmd)
        return params, record

    return _resolve


def _sizes(params):
    return tuple(params[knob] for knob in AUTO_SIZE_PARAMS)


def test_resolve_adopts_the_sizes_of_a_running_minicloud(resolve, tmp_path):
    cmd = [
        "--lightweight",
        "--lightweight-memory",
        "4GiB,NodeType=loader=2GiB,NodeType=monitor=2GiB",
        "--lightweight-vcpus",
        "2,NodeType=loader=3,NodeType=monitor=1",
    ]
    with patch("sdcm.utils.minicloud.sizing.host_memory_budget") as budget:
        params, record = resolve(running_cmd=cmd)
    budget.assert_not_called()
    assert record is None
    assert _sizes(params) == ("4GiB", 2, 3, "2GiB")
    assert not (tmp_path / "sizing.json").exists()


def test_resolve_derives_and_records_when_none_is_running(resolve, tmp_path):
    with (
        _meminfo_path_patch(int(42.1 * 1024 * 1024)),
        patch("sdcm.utils.minicloud.sizing.os.cpu_count", return_value=32),
    ):
        params, record = resolve()
    # 32 threads leave the loader 4 vCPUs, but its 2GiB carries 2
    assert _sizes(params) == ("4GiB", 2, 2, "2GiB")
    assert record["sizes"] == {"db_memory": "4GiB", "db_vcpus": 2, "loader_vcpus": 2, "loader_memory": "2GiB"}
    assert record["guests"] == {"total": 8, "db": 6, "loaders": 1, "monitors": 1, "nemesis_growth": 0}
    assert record["flags"] == ["4GiB,NodeType=loader=2GiB,NodeType=monitor=2GiB", "2,NodeType=loader=2"]
    assert json.loads((tmp_path / "sizing.json").read_text()) == record


def test_resolve_counts_only_the_container_cpus(resolve):
    with (
        _meminfo_path_patch(int(42.1 * 1024 * 1024)),
        patch("sdcm.utils.minicloud.sizing.os.cpu_count", return_value=32),
    ):
        params, record = resolve(minicloud_container_cpus="12.5")
    assert _sizes(params) == ("4GiB", 2, 2, "2GiB")
    assert record["host"]["cpu_threads"] == 12


def test_resolve_sizes_the_loader_memory_first_then_its_vcpus(resolve):
    # minipc5: 6 x 4GiB + loader 4GiB + monitor 2GiB + 10GiB headroom + 2GiB margin = 42GiB of 42.8GiB
    with (
        _meminfo_path_patch(int(42.8 * 1024 * 1024)),
        patch("sdcm.utils.minicloud.sizing.os.cpu_count", return_value=16),
    ):
        params, _ = resolve(minicloud_lightweight_memory_loader="auto", minicloud_lightweight_vcpus_monitor=1)
    assert _sizes(params) == ("4GiB", 2, 3, "4GiB")


def test_resolve_takes_the_smallest_sizes_when_the_memory_check_is_skipped(resolve):
    with (
        _meminfo_path_patch(20 * 1024 * 1024),
        patch("sdcm.utils.minicloud.sizing.os.cpu_count", return_value=16),
    ):
        params, record = resolve(minicloud_skip_memory_check=True, minicloud_lightweight_memory_loader="auto")
    assert _sizes(params) == ("4GiB", 2, 2, "2GiB")
    assert record["host"]["budget_source"] == "skipped: minicloud_skip_memory_check"


def test_resolve_writes_numbers_into_a_real_sct_config(params):
    for knob in AUTO_SIZE_PARAMS:
        params[knob] = "auto"
    cmd = ["--lightweight", "--lightweight-memory", "6GiB", "--lightweight-vcpus", "2"]
    resolve_auto_sizing(params, MinicloudConfig.from_env(params=params), cmd)
    assert _sizes(params) == ("6GiB", 2, None, None)
    assert params.model_dump()["minicloud_lightweight_vcpus"] == 2  # what Argus stores


RECORD = {
    "host": {"name": "host-a", "cpu_threads": 16, "mem_total_gib": 44.0, "budget_gib": 42.1, "budget_source": "x"},
    "guests": {"total": 8, "db": 6, "loaders": 1, "monitors": 1, "nemesis_growth": 0},
    "sizes": {"db_memory": "4GiB", "db_vcpus": 2, "loader_vcpus": 3},
    "derived": list(AUTO_SIZE_PARAMS),
    "flags": ["4GiB", "2,NodeType=loader=3"],
}


def test_load_sizing_record_ignores_a_record_of_other_sizes(tmp_path):
    (tmp_path / "sizing.json").write_text(json.dumps(RECORD))
    same = MinicloudConfig(
        state_dir=str(tmp_path), lightweight_memory="4GiB", lightweight_vcpus=2, lightweight_vcpus_loader=3
    )
    other = dataclasses.replace(same, lightweight_memory="6GiB")
    assert load_sizing_record(same) == RECORD
    assert load_sizing_record(other) is None
