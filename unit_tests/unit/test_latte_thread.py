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

import os
import re
import types

import pytest
import yaml

from sdcm import sct_abs_path
from sdcm.stress import latte_thread
from sdcm.stress.latte_thread import (
    LatteStressThread,
    find_latte_fn_names,
    find_latte_tags,
    get_latte_operation_type,
)

pytestmark = [
    pytest.mark.usefixtures("events"),
]

COMPLEX_CONFIG_FILES = (
    "test-cases/upgrades/rolling-upgrade.yaml",
    "configurations/azure/azure_rolling_upgrade.yaml",
    "configurations/minicloud/rolling-upgrade.yaml",
    "unit_tests/test_data/test_scylla_yaml_builders/rolling-upgrade.yaml",
)
COMPLEX_SCHEMA_SCRIPT = "data_dir/latte/complex_schema.rn"
COMPLEX_CMD_EXPECTATIONS = {
    "stress_cmd_complex_prepare": ("write", ["insert"]),
    "stress_cmd_complex_verify_read": ("read", ["read_by_key", "read_by_email"]),
    "stress_cmd_complex_verify_more": (
        "mixed",
        [
            "read_by_key",
            "read_by_ck",
            "read_by_email",
            "update_static",
            "update_ttl",
            "update_diff1_ts",
            "update_diff2_ts",
            "update_same1_ts",
            "update_same2_ts",
        ],
    ),
    "stress_cmd_complex_verify_delete": ("write", ["delete_row"]),
}


def test_05_latte_parse_final_output():
    latte = LatteStressThread(
        loader_set=["fake-loader"],
        stress_cmd="fake",
        timeout=1,
        node_list=["fake-db-node-1"],
        params={"cluster_backend": "aws"},
    )
    with open(sct_abs_path("data_dir/latte_stress_output.log"), "r", encoding="utf-8") as latte_output:
        stress_result = type("FakeStressResult", (), {"stdout": latte_output.read()})

    parsed_output = latte.parse_final_output(stress_result)

    assert isinstance(parsed_output, dict)
    assert "latency 99th percentile" in parsed_output
    assert parsed_output["latency 99th percentile"] == "6.206"
    assert "latency mean" in parsed_output
    assert parsed_output["latency mean"] == "2.272"
    assert "op rate" in parsed_output
    assert parsed_output["op rate"] == "160100"


@pytest.mark.parametrize(
    "cmd,items",
    (
        ("latte run /foo/bar.rn %swrite -q -r 500", ["write"]),
        ("latte run /foo/bar.rn %sread -q -r 500", ["read"]),
        ("latte run /foo/bar.rn %scustom -q -r 500", ["custom"]),
        ("latte run /foo/bar.rn %sread,write -q -r 500", ["read", "write"]),
        ("latte run /foo/bar.rn %sread,custom,write,user -q -r 500", ["read", "custom", "write", "user"]),
        ("latte run /foo/bar.rn %sfoo_bar:1,quuz_tea:2 -q -r 500", ["foo_bar", "quuz_tea"]),
    ),
)
def test_find_latte_fn_names(cmd, items):
    fn_params = ("-f ", "-f=", "--function ", "--function=", "--functions ", "--functions=")
    for fn_param in fn_params:
        result = find_latte_fn_names(cmd % fn_param)
        assert len(result) > 0
        assert len(result) == len(items), f"Expected: {items}, Actual: {result}"
        for item in items:
            assert item in result


@pytest.mark.parametrize(
    "cmd,expected_operation_type",
    (
        ("latte run /foo/bar.rn %swrite -q -r 500", "write"),
        ("latte run /foo/bar.rn %swrite_batch -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_write -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_batch -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_insert -q -r 500", "write"),
        ("latte run /foo/bar.rn %supdate -q -r 500", "write"),
        ("latte run /foo/bar.rn %supdate_batch -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_update -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_foo,update_bar -q -r 500", "write"),
        ("latte run /foo/bar.rn %sdelete -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_delete -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_delete_by_one -q -r 500", "write"),
        ("latte run /foo/bar.rn %scounter_write -q -r 500", "counter_write"),
        ("latte run /foo/bar.rn %sread -q -r 500", "read"),
        ("latte run /foo/bar.rn %sread_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_read -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_read_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %scount -q -r 500", "read"),
        ("latte run /foo/bar.rn %sselect -q -r 500", "read"),
        ("latte run /foo/bar.rn %sselect_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_select -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_select_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %smulti_get -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_get_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget_all,get_single -q -r 500", "read"),
        ("latte run /foo/bar.rn %ssearch -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_search -q -r 500", "read"),
        ("latte run /foo/bar.rn %scounter_read -q -r 500", "counter_read"),
        ("latte run /foo/bar.rn %sread,write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite:1,read:2 -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sbatch_insert:1,read_all:2,get_bar:0.5 -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sread,counter_write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sread,counter_read -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite,counter_read -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite,counter_write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %scustom -q -r 500", "user"),
        ("latte run /foo/bar.rn %suser_profile -q -r 500", "user"),
        ("latte run /foo/bar.rn %sfoo_bar:1,quuz_tea:2 -q -r 500", "user"),
        ("latte run /foo/bar.rn %sread,write,custom -q -r 500", "user"),
    ),
)
def test_get_latte_operation_type(cmd, expected_operation_type):
    fn_params = ("-f ", "-f=", "--function ", "--function=", "--functions ", "--functions=")
    for fn_param in fn_params:
        result = get_latte_operation_type(cmd % fn_param)
        assert expected_operation_type == result


@pytest.mark.parametrize(
    "cmd,items",
    (
        ("%s --tag=latte-prepare-01 -q -r 500", ["latte-prepare-01"]),
        ("%s --tag latte-main-01 -q -r 500", ["latte-main-01"]),
        ("%s --tag  latte-prepare-01,write  -q -r 500", ["latte-prepare-01", "write"]),
        ("%s --tag=latte-main-01,read    -q -r 500", ["latte-main-01", "read"]),
        ("%s  --tag=latte-main-01  --tag   write,table1    -q -r 500", ["latte-main-01", "write", "table1"]),
        ("%s --tag=latte-main-01,read  --tag table2  -q -r 500", ["latte-main-01", "read", "table2"]),
        ("%s --tag=latte-main-01,read -q -r 500 --tag table2", ["latte-main-01", "read", "table2"]),
    ),
)
def test_find_latte_tags(cmd, items):
    result = find_latte_tags(cmd % "latte run /foo/bar.rn")
    assert len(result) > 0
    assert len(result) == len(items), f"Expected: {items}, Actual: {result}"
    for item in items:
        assert item in result


def _load_complex_cmd(config_file, param_name):
    with open(sct_abs_path(config_file), encoding="utf-8") as config:
        return yaml.safe_load(config)[param_name]


@pytest.mark.parametrize("config_file", COMPLEX_CONFIG_FILES)
@pytest.mark.parametrize("param_name", sorted(COMPLEX_CMD_EXPECTATIONS))
def test_complex_schema_cmds_are_classified(config_file, param_name):
    """Each complex command must run the rune script and map onto the right latte metrics."""
    cmd = _load_complex_cmd(config_file, param_name)
    expected_operation_type, expected_fn_names = COMPLEX_CMD_EXPECTATIONS[param_name]

    assert COMPLEX_SCHEMA_SCRIPT in cmd
    assert find_latte_fn_names(cmd) == expected_fn_names
    assert get_latte_operation_type(cmd) == expected_operation_type


@pytest.mark.parametrize("config_file", COMPLEX_CONFIG_FILES)
@pytest.mark.parametrize("param_name", ("stress_cmd_complex_prepare", "stress_cmd_complex_verify_read"))
def test_complex_schema_cmds_cover_every_row(config_file, param_name):
    """The workload writes one row per cycle, so these steps must run exactly 'row_count' cycles."""
    cmd = _load_complex_cmd(config_file, param_name)

    cycles = re.search(r"-d (\d+)", cmd)
    row_count = re.search(r"-P row_count=(\d+)", cmd)
    assert cycles and row_count, f"missing -d or -P row_count in: {cmd}"
    assert cycles.group(1) == row_count.group(1)


def _staging_thread(files, backend="docker", **kwargs):
    loader_set = types.SimpleNamespace(
        get_db_auth=lambda: None,
        test_config=types.SimpleNamespace(MULTI_REGION=False, tester_obj=object),
    )
    return LatteStressThread(
        loader_set=loader_set,
        stress_cmd="latte run -f load data_dir/latte/fts_search/fts.rn -d 100",
        timeout=60,
        node_list=["fake-db-node-1"],
        params={"cluster_backend": backend, "client_encrypt": False, "latte_schema_parameters": {}},
        extra_files_to_stage=files,
        **kwargs,
    )


SHARD = ("/local/ds/shards/documents_000.tsv", "/tmp/fts/ds/shards/documents_000.tsv")


def test_a_staged_file_needs_an_absolute_container_path():
    with pytest.raises(ValueError, match="not absolute"):
        _staging_thread([("/local/documents.tsv", "documents.tsv")])


def test_staged_files_need_one_command_per_loader():
    """Two commands on one loader would share the file, and the first to finish would remove it."""
    with pytest.raises(ValueError, match="needs stress_num=1"):
        _staging_thread([SHARD], stress_num=2)


def test_round_robin_stages_files_whatever_stress_num_says():
    """Round-robin runs a single command per call, so the file is never shared."""
    assert _staging_thread([SHARD], stress_num=2, round_robin=True).extra_files_to_stage == [SHARD]


def test_staged_files_need_a_docker_loader():
    with pytest.raises(ValueError, match="k8s loaders lack"):
        _staging_thread([SHARD], backend="k8s-eks")


def _hdr_thread(stress_cmd, use_hdrhistogram=True, **kwargs):
    return LatteStressThread(
        loader_set=types.SimpleNamespace(
            get_db_auth=lambda: None,
            test_config=types.SimpleNamespace(MULTI_REGION=False, tester_obj=object),
        ),
        stress_cmd=stress_cmd,
        timeout=60,
        node_list=["fake-db-node-1"],
        params={"cluster_backend": "docker", "use_hdrhistogram": use_hdrhistogram},
        **kwargs,
    )


def test_a_run_records_hdr_when_use_hdrhistogram_is_on():
    assert _hdr_thread("latte run -f search data_dir/latte/x.rn").records_hdr()


@pytest.mark.parametrize(
    "stress_cmd, use_hdrhistogram, kwargs",
    [
        pytest.param("latte run -f load data_dir/latte/x.rn", True, {"record_hdr": False}, id="opted-out"),
        pytest.param("latte schema data_dir/latte/x.rn", True, {}, id="schema"),
        pytest.param("latte run -f search data_dir/latte/x.rn", False, {}, id="hdr-off"),
    ],
)
def test_no_hdr_is_recorded(stress_cmd, use_hdrhistogram, kwargs):
    assert not _hdr_thread(stress_cmd, use_hdrhistogram, **kwargs).records_hdr()


# The script directory is staged once per loader, not copied into every container


class _FakeRemoter:
    """Records the commands run on the loader host and the files sent to it."""

    def __init__(self):
        self.commands = []
        self.sent = []

    def run(self, cmd, **_):
        self.commands.append(cmd)
        return types.SimpleNamespace(stdout="/home/loader\n" if cmd == "echo $HOME" else "")

    def send_files(self, src, dst, **_):
        self.sent.append((os.path.basename(src), dst))
        return True


def _loader(name="loader-1"):
    return types.SimpleNamespace(name=name, remoter=_FakeRemoter())


def _thread(stress_cmd="latte run -f load data_dir/latte/bench/bench.rn -d 100"):
    return LatteStressThread(
        loader_set=types.SimpleNamespace(
            get_db_auth=lambda: None,
            test_config=types.SimpleNamespace(MULTI_REGION=False, tester_obj=object),
        ),
        stress_cmd=stress_cmd,
        timeout=60,
        node_list=["fake-db-node-1"],
        params={"cluster_backend": "docker"},
    )


@pytest.fixture(autouse=True)
def nothing_sent_yet(monkeypatch):
    monkeypatch.setattr(latte_thread, "_SENT_TO_LOADERS", {})
    monkeypatch.setattr(latte_thread, "_SEND_LOCKS", {})
    monkeypatch.setattr(latte_thread, "_LOADER_HOMES", {})
    monkeypatch.setattr(latte_thread, "_STAGING_RUN", "run")


@pytest.fixture
def script_dir(tmp_path, monkeypatch):
    """A rune script directory under a fake SCT root, with a subdirectory that must not be sent."""
    directory = tmp_path / "data_dir" / "latte" / "bench"
    (directory / "corpus").mkdir(parents=True)
    for name in ("bench.rn", "helpers.rn", "plan.yaml"):
        (directory / name).write_text(name, encoding="utf-8")
    (directory / "corpus" / "documents.tsv").write_text("too big to ship", encoding="utf-8")
    monkeypatch.setattr(latte_thread, "get_sct_root_path", lambda: str(tmp_path))
    return directory


def test_the_script_directory_is_sent_once_per_loader(script_dir):
    loader = _loader()

    first = _thread()._script_dir_mount(loader)
    second = _thread("latte run -f search data_dir/latte/bench/bench.rn -d 10s")._script_dir_mount(loader)

    host_dir = "/home/loader/latte-staging/run/loader-1/scripts/data_dir/latte/bench"
    assert first == second == f"-v {host_dir}:/data_dir/latte/bench:ro,z"
    assert loader.remoter.sent == [
        ("bench.rn", f"{host_dir}/bench.rn"),
        ("helpers.rn", f"{host_dir}/helpers.rn"),
        ("plan.yaml", f"{host_dir}/plan.yaml"),
    ], "top-level files only, each sent once -- the second call reuses the copy"
    assert loader.remoter.commands.count("echo $HOME") == 1


def test_another_run_on_the_same_host_stages_apart(script_dir, monkeypatch):
    """Parallel runs on one docker host share its home and their loaders' names."""
    loader = _loader()
    _thread()._script_dir_mount(loader)
    monkeypatch.setattr(latte_thread, "_STAGING_RUN", "other-run")
    monkeypatch.setattr(latte_thread, "_SENT_TO_LOADERS", {})

    _thread()._script_dir_mount(loader)

    first, second = loader.remoter.sent[:3], loader.remoter.sent[3:]
    assert {dst for _, dst in first}.isdisjoint(dst for _, dst in second)


def test_every_loader_gets_a_copy_of_its_own(script_dir):
    first, second = _loader("loader-1"), _loader("loader-2")

    _thread()._script_dir_mount(first)
    _thread()._script_dir_mount(second)

    # Docker loaders share one host, so the copies must not share a directory either.
    assert len(first.remoter.sent) == len(second.remoter.sent) == 3
    assert {dst for _, dst in first.remoter.sent}.isdisjoint(dst for _, dst in second.remoter.sent)


def test_a_changed_file_is_sent_again(script_dir):
    loader = _loader()
    _thread()._script_dir_mount(loader)

    edited = script_dir / "bench.rn"
    edited.write_text("BENCH.RN", encoding="utf-8")  # the same size: only the mtime tells
    os.utime(edited, ns=(edited.stat().st_atime_ns, edited.stat().st_mtime_ns + 1))
    _thread()._script_dir_mount(loader)

    assert [name for name, _ in loader.remoter.sent] == ["bench.rn", "helpers.rn", "plan.yaml", "bench.rn"]


def test_a_failed_send_fails_the_call_and_is_retried(script_dir):
    loader = _loader()
    loader.remoter.send_files = lambda src, dst, **_: False
    with pytest.raises(RuntimeError, match="Failed to send"):
        _thread()._script_dir_mount(loader)

    loader.remoter = _FakeRemoter()
    _thread()._script_dir_mount(loader)

    assert len(loader.remoter.sent) == 3, "a failed send must not count as sent"


def test_host_paths_are_quoted(script_dir):
    loader = _loader("loader 1")

    mount = _thread()._script_dir_mount(loader)

    assert (
        mount == "-v '/home/loader/latte-staging/run/loader 1/scripts/data_dir/latte/bench:/data_dir/latte/bench:ro,z'"
    )
    assert (
        loader.remoter.commands[-1] == "mkdir -p '/home/loader/latte-staging/run/loader 1/scripts/data_dir/latte/bench'"
    )


# Staged files go to the loader's host, are mounted read-only and removed afterwards


@pytest.fixture
def shard(tmp_path):
    path = tmp_path / "documents_000.tsv"
    path.write_text("doc_0\tbody\n", encoding="utf-8")
    return str(path), SHARD[1]


HOST_SHARD = "/home/loader/latte-staging/run/loader-1/files/tmp/fts/ds/shards/documents_000.tsv"


def test_a_staged_file_goes_to_the_loaders_host_and_is_mounted_read_only(shard):
    loader = _loader()

    with _staging_thread([shard])._staged_files(loader) as mounts:
        assert loader.remoter.sent == [("documents_000.tsv", HOST_SHARD)]
        assert mounts == f" -v {HOST_SHARD}:{SHARD[1]}:ro,z"

    assert loader.remoter.commands[-1] == f"rm -f {HOST_SHARD}"


def test_nothing_is_staged_by_default():
    """The default has to stay empty: every existing caller stages nothing, and pays nothing for it."""
    loader = _loader()

    with _staging_thread(None)._staged_files(loader) as mounts:
        assert mounts == ""

    assert loader.remoter.commands == loader.remoter.sent == []


def test_a_staged_file_is_removed_even_when_the_command_fails(shard):
    loader = _loader()

    with pytest.raises(RuntimeError, match="latte failed"), _staging_thread([shard])._staged_files(loader):
        raise RuntimeError("latte failed")

    assert loader.remoter.commands[-1] == f"rm -f {HOST_SHARD}"


def test_a_file_that_failed_to_send_is_removed_too(shard):
    """A send that fails partway can leave a partial file behind."""
    loader = _loader()
    loader.remoter.send_files = lambda src, dst, **_: False

    with pytest.raises(RuntimeError, match="Failed to send"), _staging_thread([shard])._staged_files(loader):
        pytest.fail("the command must not run without its file")

    assert loader.remoter.commands[-1] == f"rm -f {HOST_SHARD}"


def test_a_failed_removal_does_not_replace_the_commands_outcome(shard):
    """A connection dropped during the cleanup must not fail a load that succeeded."""
    loader = _loader()
    run = loader.remoter.run

    def rm_drops_the_connection(cmd, **kwargs):
        if cmd.startswith("rm -f "):
            raise ConnectionError("connection lost")
        return run(cmd, **kwargs)

    loader.remoter.run = rm_drops_the_connection

    with _staging_thread([shard])._staged_files(loader):
        pass


def test_staging_stops_when_the_home_cannot_be_resolved(shard):
    loader = _loader()
    loader.remoter.run = lambda cmd, **_: types.SimpleNamespace(stdout="\n")

    with pytest.raises(RuntimeError, match="Cannot resolve the home"), _staging_thread([shard])._staged_files(loader):
        pytest.fail("the command must not run without its file")

    assert loader.remoter.sent == []


def test_a_file_sent_ahead_is_not_sent_again(shard):
    """A caller may send a file ahead of the command that stages it."""
    loader = _loader()
    latte_thread.send_to_loader(loader, [(shard[0], latte_thread.staged_file_path(loader, shard[1]))])

    with _staging_thread([shard])._staged_files(loader):
        assert len(loader.remoter.sent) == 1


def test_a_removed_file_is_sent_again(shard):
    loader = _loader()
    with _staging_thread([shard])._staged_files(loader):
        pass

    with _staging_thread([shard])._staged_files(loader):
        assert len(loader.remoter.sent) == 2


def test_docker_loaders_stage_apart(shard):
    """Docker loaders share one host: one finishing must not remove the other's file."""
    first, second = _loader("loader-1"), _loader("loader-2")

    with _staging_thread([shard])._staged_files(first), _staging_thread([shard])._staged_files(second):
        assert first.remoter.sent[0][1] != second.remoter.sent[0][1]


def test_a_staged_file_mount_is_quoted(shard):
    loader = _loader("loader 1")

    with _staging_thread([shard])._staged_files(loader) as mounts:
        host_path = HOST_SHARD.replace("loader-1", "loader 1")
        assert mounts == f" -v '{host_path}:{SHARD[1]}:ro,z'"

    assert loader.remoter.commands[-1] == f"rm -f '{host_path}'"
