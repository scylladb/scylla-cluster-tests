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
# Copyright (c) 2023 ScyllaDB
import json

from sdcm.argus_results import send_microbenchmark_result_to_argus
from sdcm.tester import ClusterTester


class MicrobenchmarkTest(ClusterTester):
    """
    Base for the scylla microbenchmark tests, each of which runs one `scylla perf-*` tool on a
    single DB node and reports the JSON it writes to Argus.

    Note for the tools' command lines: options must be passed as "--name value", never
    "--name=value". A tool that strips its own options out of argv before handing the rest to
    scylla (perf-cql-raw does) compares each argument verbatim, so the "=" form is not recognised,
    stays in argv and reaches scylla as an unknown option.
    """

    def submit_results(self, node, result_file: str, benchmark_name: str) -> None:
        results = json.loads(node.remoter.run(f"cat {result_file}").stdout)
        send_microbenchmark_result_to_argus(
            argus_client=self.test_config.argus_client(),
            result=results,
            error_thresholds=self.params.get("latency_decorator_error_thresholds"),
            benchmark_name=benchmark_name,
        )

    def update_test_with_errors(self):
        self.log.info("update_test_with_errors: Using Argus for performance results")


class PerfSimpleQueryTest(MicrobenchmarkTest):
    """
    Run the `scylla perf-simple-query` microbenchmark.

    The tool drives the query processor in process, against a cql_test_env: it builds its own
    db::config so it reads no config file, it starts no CQL server so it binds no ports, and its
    data goes to a throwaway temp directory. Nothing about it collides with the scylla-server that
    SCT runs on the node, so it needs no special handling here - unlike perf-cql-raw, see
    PerfCqlRawTest.

    The read workload is the default; `perf_simple_query_extra_command` passes `--write` for the
    write workload.
    """

    def test_perf_simple_query(self):
        extra_command = self.params.get("perf_simple_query_extra_command") or ""
        node = self.db_cluster.nodes[0]
        scylla_bin = node.add_install_prefix("/usr/bin/scylla")
        result_file = "perf-simple-query-result.txt"
        result = node.remoter.run(
            f"{scylla_bin} perf-simple-query --json-result {result_file} --smp 1 -m 1G {extra_command}"
        )
        if result.ok:
            self.submit_results(node, result_file, benchmark_name="Perf Simple Query")


class PerfCqlRawTest(MicrobenchmarkTest):
    """
    Run the `scylla perf-cql-raw` microbenchmark, which exercises the full networking and protocol
    parsing path using handcrafted CQL binary frames.

    Unlike perf-simple-query, this tool boots a full scylla server in process - it calls
    scylla_main and then speaks the CQL protocol to itself over a real socket - so it needs
    everything a server needs, and each of those needs is handled below:

    * A config file. scylla resolves its config as <cwd>/conf/scylla.yaml, and the test does not
      run from a scylla source tree the way the tool's developers do, so it gets SCYLLA_CONFIG
      written into its work directory and passed as --options-file.
    * A data directory. --workdir keeps it off the live /var/lib/scylla. Note this only works
      because SCYLLA_CONFIG lists no storage directories: scylla moves a directory under --workdir
      only when the config file leaves it unset, so reusing the node's /etc/scylla/scylla.yaml
      would silently put the benchmark on the running server's data.
    * The standard ports, plus memory and cpus. The node's own scylla-server already holds
      native_transport_port 9042 and api_port 10000, so it is stopped for the duration of the run
      and started again afterwards.

    The command line matches what the tool's developers run, so results stay comparable with
    theirs. Changing RESOURCE_OPTIONS or DURATION invalidates the Argus baseline, which is why
    they are constants here rather than config options.
    """

    WORKDIR = "/tmp/scylla-perf-cql-raw-workdir"
    RESOURCE_OPTIONS = "--smp 2 --cpus 0,1 -m 2G --developer-mode 1"
    DURATION = 60

    # Only long standing options are set, so this stays valid across the scylla versions the
    # weekly jobs run against; everything else falls back to scylla's built-in defaults. Storage
    # directories are deliberately absent, so that --workdir relocates all of them.
    SCYLLA_CONFIG = """\
seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          - seeds: "127.0.0.1"
listen_address: localhost
rpc_address: localhost
api_address: 127.0.0.1
endpoint_snitch: SimpleSnitch
"""

    def run_workload(self, workload: str) -> None:
        node = self.db_cluster.nodes[0]
        scylla_bin = node.add_install_prefix("/usr/bin/scylla")
        result_file = "perf-cql-raw-result.txt"
        conf_file = f"{self.WORKDIR}/conf/scylla.yaml"

        node.remoter.run(f"rm -rf {self.WORKDIR}")
        node.remoter.run(f"mkdir -p {self.WORKDIR}/conf")
        node.remoter.run(f"cat > {conf_file} <<'SCYLLA_CONFIG'\n{self.SCYLLA_CONFIG}SCYLLA_CONFIG")

        # The stop only has to free the ports, so a failing `systemctl stop` (the service is
        # already down) is not a reason to abort the benchmark - ignore_status lets the run
        # continue, and verify_down still confirms the ports are actually free. It is inside the
        # try so that anything it does raise still goes through the finally below and leaves the
        # node's scylla-server running.
        try:
            node.stop_scylla_server(ignore_status=True)
            results = node.remoter.run(
                f"{scylla_bin} perf-cql-raw --json-result {result_file}"
                f" --workdir {self.WORKDIR} --options-file {conf_file}"
                f" {self.RESOURCE_OPTIONS} --workload {workload} --duration {self.DURATION}"
            )
            if results.ok:
                self.submit_results(node, result_file, benchmark_name="Perf CQL Raw")
        finally:
            node.start_scylla_server()

    def test_read(self):
        self.run_workload("read")

    def test_write(self):
        self.run_workload("write")
