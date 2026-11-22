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
# Copyright (c) 2026 ScyllaDB

"""Tests for the microbenchmarks that boot a full scylla server in process (perf-cql-raw, perf-alternator).

They check the command line each test runs, the server config it writes, and that the node's
scylla-server is started again whatever happens to the benchmark.
"""

from unittest.mock import MagicMock

import pytest
import yaml

from microbenchmarking_test import InProcessServerMicrobenchmarkTest, PerfAlternatorTest, PerfCqlRawTest

PerfAlternatorTest.__test__ = False
PerfCqlRawTest.__test__ = False

BASE_CONFIG = {
    "seed_provider": [
        {"class_name": "org.apache.cassandra.locator.SimpleSeedProvider", "parameters": [{"seeds": "127.0.0.1"}]}
    ],
    "listen_address": "localhost",
    "rpc_address": "localhost",
    "api_address": "127.0.0.1",
    "endpoint_snitch": "SimpleSnitch",
}

# The perf-cql-raw config and command lines are pinned: its Argus history depends on them, so they
# must not change.
PERF_CQL_RAW_CONFIG = BASE_CONFIG

PERF_ALTERNATOR_CONFIG = BASE_CONFIG | {
    "alternator_port": 8000,
    "alternator_address": "127.0.0.1",
    "alternator_write_isolation": "forbid",
    "tablets_mode_for_new_keyspaces": "enabled",
}


def expected_command(benchmark: str, workload: str) -> str:
    workdir = f"/tmp/scylla-{benchmark}-workdir"
    return (
        f"/usr/bin/scylla {benchmark} --json-result {benchmark}-result.json"
        f" --workdir {workdir}"
        f" --options-file {workdir}/conf/scylla.yaml"
        " --io-properties-file /etc/scylla.d/io_properties.yaml"
        f" --smp 2 --cpus 0,1 -m 3G --workload {workload} --duration 60"
    )


BENCHMARKS = [
    pytest.param(PerfCqlRawTest, "perf-cql-raw", "Perf CQL Raw", PERF_CQL_RAW_CONFIG, id="perf-cql-raw"),
    pytest.param(
        PerfAlternatorTest, "perf-alternator", "Perf Alternator", PERF_ALTERNATOR_CONFIG, id="perf-alternator"
    ),
]


def make_test(test_class, fail_at=None):
    """A test instance with a fake DB node, without running ClusterTester's setup.

    `fail_at` makes one step raise: "stop", "tool" or "submit". The returned list records the steps.
    """
    steps = []
    node = MagicMock()
    node.add_install_prefix.side_effect = lambda path: path

    def step(name):
        steps.append(name)
        if fail_at == name:
            raise RuntimeError(f"fail at {name}")

    def run(cmd, *_args, **_kwargs):
        if cmd.startswith("/usr/bin/scylla "):
            step("tool")
        return MagicMock(ok=True)

    node.remoter.run.side_effect = run
    node.stop_scylla_server.side_effect = lambda **_kwargs: step("stop")
    node.start_scylla_server.side_effect = lambda: step("start")
    test = test_class.__new__(test_class)
    test.db_cluster = MagicMock(nodes=[node])
    test.submit_results = MagicMock(side_effect=lambda *_args, **_kwargs: step("submit"))
    return test, node, steps


def benchmark_commands(node):
    return [call.args[0] for call in node.remoter.run.call_args_list if call.args[0].startswith("/usr/bin/scylla ")]


def written_config(node) -> dict:
    """The scylla.yaml the test writes into its work directory with a heredoc."""
    [heredoc] = [call.args[0] for call in node.remoter.run.call_args_list if "<<'SCYLLA_CONFIG'" in call.args[0]]
    body = heredoc.split("<<'SCYLLA_CONFIG'\n", 1)[1].removesuffix("SCYLLA_CONFIG")
    return yaml.safe_load(body)


@pytest.mark.parametrize("workload", ["read", "write"])
@pytest.mark.parametrize("test_class,benchmark,benchmark_name,config", BENCHMARKS)
def test_command_config_and_result(test_class, benchmark, benchmark_name, config, workload):
    """The whole config is pinned: a new key, such as a storage directory, must be a deliberate change."""
    test, node, _ = make_test(test_class)

    getattr(test, f"test_{workload}")()

    assert benchmark_commands(node) == [expected_command(benchmark, workload)]
    assert written_config(node) == config
    test.submit_results.assert_called_once_with(node, f"{benchmark}-result.json", benchmark_name=benchmark_name)


@pytest.mark.parametrize("test_class", [PerfCqlRawTest, PerfAlternatorTest])
@pytest.mark.parametrize(
    "fail_at,expected_steps",
    [
        (None, ["stop", "tool", "submit", "start"]),
        ("stop", ["stop", "start"]),
        ("tool", ["stop", "tool", "start"]),
        ("submit", ["stop", "tool", "submit", "start"]),
    ],
)
def test_scylla_server_is_down_for_the_run_and_starts_again(test_class, fail_at, expected_steps):
    test, node, steps = make_test(test_class, fail_at=fail_at)

    if fail_at:
        with pytest.raises(RuntimeError, match=f"fail at {fail_at}"):
            test.test_read()
    else:
        test.test_read()

    assert steps == expected_steps
    node.stop_scylla_server.assert_called_once_with(ignore_status=True)


@pytest.mark.parametrize("attributes", [{"BENCHMARK": "perf-x"}, {"BENCHMARK_NAME": "Perf X"}], ids=str)
def test_a_subclass_that_misses_an_attribute_fails_before_the_stop(attributes):
    """BENCHMARK and BENCHMARK_NAME have no default, so a subclass that misses one never stops scylla-server."""
    test_class = type("PerfXTest", (InProcessServerMicrobenchmarkTest,), {"__test__": False, **attributes})
    test, node, steps = make_test(test_class)

    with pytest.raises(AttributeError):
        test.run_workload("read")

    assert steps == []
    node.remoter.run.assert_not_called()
