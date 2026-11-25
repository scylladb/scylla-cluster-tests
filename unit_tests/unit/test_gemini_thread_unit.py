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
# Copyright (c) 2024 ScyllaDB

import json
from unittest.mock import MagicMock, patch

import pytest

from sdcm.gemini_thread import GeminiStressThread, GeminiWorkerFiles
from sdcm.sct_events import Severity
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy


class MockCluster:
    """Minimal cluster stub providing the CQL IPs that GeminiStressThread needs."""

    def __init__(self, ips=None):
        self.nodes = []
        self._ips = ips or ["10.0.0.1"]

    def get_node_cql_ips(self):
        return self._ips


STRESS_CMD = "--duration=1m --mode=write --concurrency=5 --warmup=0"
WORKER_FILES = GeminiWorkerFiles.for_worker("worker")


@pytest.fixture(name="gemini_thread_unit")
def fixture_gemini_thread_unit(params):
    """Create a GeminiStressThread without touching real Docker (no integration mark)."""
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = MockCluster(["10.0.0.1"])

    with patch("sdcm.utils.docker_remote.RemoteDocker.pull_image"):
        thread = GeminiStressThread(
            loaders=loader_set,
            stress_cmd=STRESS_CMD,
            test_cluster=test_cluster,
            oracle_cluster=None,
            timeout=60,
            params=params,
        )
    return thread


def test_docker_image_name_is_not_none(gemini_thread_unit):
    """GeminiStressThread.docker_image_name must never be None.

    Regression test: a params.copy() path in build_gemini_thread was dropping
    the stress_image dict, causing RemoteDocker to receive None as image_name
    which produced ``docker run ... None tail -f /dev/null``.
    """
    assert gemini_thread_unit.docker_image_name is not None, (
        "docker_image_name is None — SCTConfiguration.stress_image.gemini was not resolved. "
        "Check that load_docker_images_defaults() runs and that params is an SCTConfiguration "
        "(not a plain dict) when passed to GeminiStressThread."
    )
    assert "gemini" in gemini_thread_unit.docker_image_name.lower(), (
        f"Expected 'gemini' in docker_image_name, got: {gemini_thread_unit.docker_image_name!r}"
    )


@pytest.mark.parametrize(
    "oracle_ips,expect_oracle_flag",
    [
        pytest.param(["10.0.0.2"], True, id="with_oracle"),
        pytest.param(None, False, id="without_oracle"),
    ],
)
def test_generate_gemini_command_oracle_flag(params, oracle_ips, expect_oracle_flag):
    """--oracle-cluster flag must be present iff an oracle cluster is provided."""
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = MockCluster(["10.0.0.1"])
    oracle_cluster = MockCluster(oracle_ips) if oracle_ips else None

    with patch("sdcm.utils.docker_remote.RemoteDocker.pull_image"):
        thread = GeminiStressThread(
            loaders=loader_set,
            stress_cmd=STRESS_CMD,
            test_cluster=test_cluster,
            oracle_cluster=oracle_cluster,
            timeout=60,
            params=params,
        )

    cmd = thread.generate_gemini_command(WORKER_FILES)
    if expect_oracle_flag:
        assert "--oracle-cluster=" in cmd, f"Expected --oracle-cluster in command: {cmd}"
    else:
        assert "--oracle-cluster" not in cmd, f"--oracle-cluster must not appear without oracle: {cmd}"


def test_generate_gemini_command_required_flags(gemini_thread_unit):
    """generate_gemini_command must include the essential gemini flags."""
    cmd = gemini_thread_unit.generate_gemini_command(WORKER_FILES)

    assert "--test-cluster=" in cmd, f"Missing --test-cluster in: {cmd}"
    assert "--outfile=/" in cmd, f"Missing --outfile in: {cmd}"
    assert "--summary-file=/" in cmd, f"Missing --summary-file in: {cmd}"
    assert "--seed=" in cmd, f"Missing --seed in: {cmd}"


def test_generate_gemini_command_stress_cmd_flags_forwarded(gemini_thread_unit):
    """Flags from stress_cmd must appear verbatim in the generated command."""
    cmd = gemini_thread_unit.generate_gemini_command(WORKER_FILES)

    assert "--duration=1m" in cmd, f"--duration not forwarded: {cmd}"
    assert "--mode=write" in cmd, f"--mode not forwarded: {cmd}"
    assert "--concurrency=5" in cmd, f"--concurrency not forwarded: {cmd}"


def test_generate_gemini_command_schema_path_injected(params):
    """When schema_path is supplied, --schema must appear in the command."""
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = MockCluster(["10.0.0.1"])

    with patch("sdcm.utils.docker_remote.RemoteDocker.pull_image"):
        thread = GeminiStressThread(
            loaders=loader_set,
            stress_cmd=STRESS_CMD,
            test_cluster=test_cluster,
            oracle_cluster=None,
            timeout=60,
            params=params,
        )

    cmd = thread.generate_gemini_command(WORKER_FILES, schema_path="/tmp/my_schema.json")
    assert '--schema="/tmp/my_schema.json"' in cmd, f"--schema flag not injected: {cmd}"


def test_generate_gemini_command_no_schema_by_default(gemini_thread_unit):
    """Without schema_path, --schema must not appear in the generated command."""
    cmd = gemini_thread_unit.generate_gemini_command(WORKER_FILES)
    assert "--schema=" not in cmd, f"--schema should not appear by default: {cmd}"


class _FakeDockerNode:
    def __init__(self, name):
        self.name = name


class _FakeDocker:
    def __init__(self, name):
        self.node = _FakeDockerNode(name)


def _gemini_summary(tmp_path, name, result):
    path = tmp_path / name
    path.write_text(json.dumps({"result": result}), encoding="utf-8")
    return str(path)


def test_parse_results_reports_gemini_error_counters(gemini_thread_unit, tmp_path):
    summary = _gemini_summary(tmp_path, "clean.json", {"write_errors": 3, "read_errors": 0, "errors": []})

    with patch.object(GeminiStressThread, "get_results", return_value=[(_FakeDocker("loader-1"), None, summary)]):
        results, errors = gemini_thread_unit.parse_results()

    assert results == [{"write_errors": 3, "read_errors": 0, "errors": []}]
    assert [msg for _, msg in errors["loader-1"]] == ["gemini write_errors: 3"]
    assert all(severity == Severity.ERROR for severity, _ in errors["loader-1"])


def test_parse_results_passes_a_clean_gemini_run(gemini_thread_unit, tmp_path):
    summary = _gemini_summary(tmp_path, "ok.json", {"write_errors": 0, "read_errors": 0, "errors": []})

    with patch.object(GeminiStressThread, "get_results", return_value=[(_FakeDocker("loader-1"), None, summary)]):
        results, errors = gemini_thread_unit.parse_results()

    assert results and not errors


def test_parse_results_reports_a_missing_summary_file(gemini_thread_unit):
    with patch.object(GeminiStressThread, "get_results", return_value=[(_FakeDocker("loader-1"), None, "")]):
        results, errors = gemini_thread_unit.parse_results()

    assert not results
    assert errors["loader-1"] == [(Severity.ERROR, "gemini results aren't available")]


def test_parse_results_reports_an_unparsable_summary(gemini_thread_unit, tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("not json at all", encoding="utf-8")

    with patch.object(GeminiStressThread, "get_results", return_value=[(_FakeDocker("loader-1"), None, str(path))]):
        results, errors = gemini_thread_unit.parse_results()

    assert not results
    assert "could not parse gemini summary" in errors["loader-1"][0][1]


def test_generate_gemini_command_keeps_the_gemini_defaults(gemini_thread_unit):
    """Only stress_cmd sets the gemini tuning flags. Gemini uses its own default for each flag it omits."""
    cmd = gemini_thread_unit.generate_gemini_command(WORKER_FILES)

    for flag in ("--level=", "--request-timeout=", "--max-errors-to-store=", "--partition-count=", "--max-columns="):
        assert flag not in cmd, f"{flag} is not in stress_cmd, so it must not be in the command: {cmd}"
    assert cmd.endswith(STRESS_CMD)


class _ContainerStarted(Exception):
    """Stops _run_stress once RemoteDocker gets its options."""


@pytest.mark.parametrize("stress_num", [1, 2])
def test_run_stress_container_gets_every_docker_option(gemini_thread_unit, stress_num):
    """The gemini container gets the host network, the shell marker and the output file mounts,
    also when stress_num > 1 pins it to one CPU.
    """
    gemini_thread_unit.stress_num = stress_num
    with patch("sdcm.gemini_thread.RemoteDocker", side_effect=_ContainerStarted) as remote_docker:
        with pytest.raises(_ContainerStarted):
            gemini_thread_unit._run_stress(MagicMock(), loader_idx=0, cpu_idx=1)

    docker_opts = remote_docker.call_args.kwargs["extra_docker_opts"]
    assert ('--cpuset-cpus="1"' in docker_opts) == (stress_num > 1)
    assert "--network=host" in docker_opts
    assert f"--label shell_marker={gemini_thread_unit.shell_marker}" in docker_opts
    for file_name in gemini_thread_unit.worker_files(loader_idx=0, cpu_idx=1):
        assert f"/{file_name}:/{file_name}:z" in docker_opts


def test_worker_files_are_unique_per_worker(gemini_thread_unit):
    """The workers of one loader share its home, so each worker writes its own output files."""
    workers = [(0, 0), (0, 1), (1, 0)]
    files = [set(gemini_thread_unit.worker_files(loader_idx, cpu_idx)) for loader_idx, cpu_idx in workers]

    assert all(len(worker_files) == len(GeminiWorkerFiles._fields) for worker_files in files)
    assert len(set.union(*files)) == len(workers) * len(GeminiWorkerFiles._fields)


def test_generate_gemini_command_writes_to_the_worker_files(gemini_thread_unit):
    cmd = gemini_thread_unit.generate_gemini_command(WORKER_FILES)

    assert f"--outfile=/{WORKER_FILES.result}" in cmd
    assert f"--summary-file=/{WORKER_FILES.summary}" in cmd
    assert f"--test-statement-log-file=/{WORKER_FILES.test_statements}" in cmd
    assert f"--oracle-statement-log-file=/{WORKER_FILES.oracle_statements}" in cmd


@pytest.mark.parametrize(("cpu_idx", "metrics_port", "profiling_port"), [(0, 2112, 6060), (1, 2113, 6061)])
def test_generate_gemini_command_gives_each_worker_its_own_ports(
    gemini_thread_unit, cpu_idx, metrics_port, profiling_port
):
    """The workers of one loader share the host network, so each worker binds its own ports."""
    cmd = gemini_thread_unit.generate_gemini_command(WORKER_FILES, cpu_idx=cpu_idx)

    assert f"--bind=0.0.0.0:{metrics_port}" in cmd
    assert f"--profiling-port={profiling_port}" in cmd
