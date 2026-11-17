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

from microbenchmarking_test import PerfAlternatorTest, PerfCqlRawTest

PerfAlternatorTest.__test__ = False
PerfCqlRawTest.__test__ = False

# The perf-cql-raw command line before the in-process server base class was extracted. Its Argus
# history depends on it, so it must not change.
PERF_CQL_RAW_WRITE_COMMAND = (
    "/usr/bin/scylla perf-cql-raw --json-result perf-cql-raw-result.json"
    " --workdir /tmp/scylla-perf-cql-raw-workdir"
    " --options-file /tmp/scylla-perf-cql-raw-workdir/conf/scylla.yaml"
    " --io-properties-file /etc/scylla.d/io_properties.yaml"
    " --smp 2 --cpus 0,1 -m 3G --workload write --duration 60"
)

PERF_ALTERNATOR_READ_COMMAND = (
    "/usr/bin/scylla perf-alternator --json-result perf-alternator-result.json"
    " --workdir /tmp/scylla-perf-alternator-workdir"
    " --options-file /tmp/scylla-perf-alternator-workdir/conf/scylla.yaml"
    " --io-properties-file /etc/scylla.d/io_properties.yaml"
    " --smp 2 --cpus 0,1 -m 3G --workload read --duration 60"
)


def make_test(test_class, benchmark_ok=True, benchmark_error=None):
    """A test instance with a fake DB node, without running ClusterTester's setup."""
    node = MagicMock()
    node.add_install_prefix.side_effect = lambda path: path

    def run(cmd, *args, **kwargs):  # noqa: ARG001
        if cmd.startswith("/usr/bin/scylla "):
            if benchmark_error:
                raise benchmark_error
            return MagicMock(ok=benchmark_ok)
        return MagicMock(ok=True)

    node.remoter.run.side_effect = run
    test = test_class.__new__(test_class)
    test.db_cluster = MagicMock(nodes=[node])
    test.submit_results = MagicMock()
    return test, node


def benchmark_commands(node):
    return [call.args[0] for call in node.remoter.run.call_args_list if call.args[0].startswith("/usr/bin/scylla ")]


def written_config(node) -> dict:
    """The scylla.yaml the test writes into its work directory with a heredoc."""
    [heredoc] = [call.args[0] for call in node.remoter.run.call_args_list if "<<'SCYLLA_CONFIG'" in call.args[0]]
    body = heredoc.split("<<'SCYLLA_CONFIG'\n", 1)[1].removesuffix("SCYLLA_CONFIG")
    return yaml.safe_load(body)


def test_perf_cql_raw_command_is_unchanged():
    test, node = make_test(PerfCqlRawTest)

    test.test_write()

    assert benchmark_commands(node) == [PERF_CQL_RAW_WRITE_COMMAND]
    test.submit_results.assert_called_once_with(node, "perf-cql-raw-result.json", benchmark_name="Perf CQL Raw")


def test_perf_cql_raw_config_has_no_alternator():
    test, node = make_test(PerfCqlRawTest)

    test.test_read()

    config = written_config(node)
    assert "alternator_port" not in config
    assert config["api_address"] == "127.0.0.1"


def test_perf_alternator_command_and_result():
    test, node = make_test(PerfAlternatorTest)

    test.test_read()

    assert benchmark_commands(node) == [PERF_ALTERNATOR_READ_COMMAND]
    test.submit_results.assert_called_once_with(node, "perf-alternator-result.json", benchmark_name="Perf Alternator")


def test_perf_alternator_config_turns_alternator_on():
    test, node = make_test(PerfAlternatorTest)

    test.test_write()

    config = written_config(node)
    assert config["alternator_port"] == 8000
    assert config["alternator_write_isolation"] == "forbid"
    # No storage directory may be set, or --workdir would not move it off the live server's data.
    assert not {key for key in config if key.endswith("_directory") or key == "data_file_directories"}


@pytest.mark.parametrize("test_class", [PerfCqlRawTest, PerfAlternatorTest])
def test_scylla_server_restarts_when_the_benchmark_fails(test_class):
    test, node = make_test(test_class, benchmark_error=RuntimeError("perf tool crashed"))

    with pytest.raises(RuntimeError, match="perf tool crashed"):
        test.test_read()

    node.stop_scylla_server.assert_called_once_with(ignore_status=True)
    node.start_scylla_server.assert_called_once_with()
    test.submit_results.assert_not_called()


@pytest.mark.parametrize("test_class", [PerfCqlRawTest, PerfAlternatorTest])
def test_no_result_is_sent_when_the_benchmark_is_not_ok(test_class):
    test, node = make_test(test_class, benchmark_ok=False)

    test.test_write()

    test.submit_results.assert_not_called()
    node.start_scylla_server.assert_called_once_with()
