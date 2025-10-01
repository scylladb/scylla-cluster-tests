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
# Copyright (c) 2020 ScyllaDB

import logging
import os
import uuid
import random
import json
import time

from sdcm.cluster import BaseCluster, BaseScyllaCluster
from sdcm.cluster_aws import CassandraAWSCluster, ScyllaAWSCluster
from sdcm.sct_events import Severity
from sdcm.utils.common import FileFollowerThread
from sdcm.sct_events.loaders import GeminiStressEvent, GeminiStressLogEvent
from sdcm.stress_thread import DockerBasedStressThread
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.reporting.tooling_reporter import GeminiVersionReporter

LOGGER = logging.getLogger(__name__)


class NotGeminiErrorResult:
    def __init__(self, error):
        self.exited = 1
        self.stdout = "n/a"
        self.stderr = str(error)


class GeminiEventsPublisher(FileFollowerThread):
    def __init__(self, node, gemini_log_filename, verbose=False, event_id=None):
        super().__init__()
        self.gemini_log_filename = gemini_log_filename
        self.node = str(node)
        self.verbose = verbose
        self.event_id = event_id

    def run(self):
        while not self.stopped():
            if not os.path.isfile(self.gemini_log_filename):
                time.sleep(0.5)
                continue
            for line_number, line in enumerate(self.follow_file(self.gemini_log_filename), start=1):
                gemini_event = GeminiStressLogEvent.GeminiEvent(verbose=self.verbose)
                gemini_event.add_info(node=self.node, line=line, line_number=line_number)
                gemini_event.event_id = self.event_id
                gemini_event.publish(warn_not_ready=False)

                if self.stopped():
                    break


class GeminiStressThread(DockerBasedStressThread):
    DOCKER_IMAGE_PARAM_NAME = "stress_image.gemini"

    def __init__(self, test_cluster: BaseCluster | BaseScyllaCluster, oracle_cluster: ScyllaAWSCluster | CassandraAWSCluster | None, loaders, stress_cmd: str, timeout=None, params=None):
        super().__init__(loader_set=loaders, stress_cmd=stress_cmd, timeout=timeout, params=params)
        self.test_cluster = test_cluster
        self.oracle_cluster = oracle_cluster
        self.gemini_commands = []
        self.unique_id = uuid.uuid4()
        self.gemini_default_flags = {
            "level": "info",
            "request-timeout": "3s",
            "connect-timeout": "60s",
            "consistency": "QUORUM",
            "async-objects-stabilization-backoff": "10ms",
            "async-objects-stabilization-attempts": 10,
            "max-mutation-retries-backoff": "10ms",
            "max-mutation-retries": 10,
            "dataset-size": "large",
            "oracle-host-selection-policy": "token-aware",
            "test-host-selection-policy": "token-aware",
            "drop-schema": "true",
            "cql-features": "normal",
            "materialized-views": "false",
            "use-server-timestamps": "false",
            "use-lwt": "false",
            "use-counters": "false",
            "max-tables": 1,
            "max-columns": 16,
            "min-columns": 8,
            "max-partition-keys": 6,
            "min-partition-keys": 2,
            "max-clustering-keys": 4,
            "min-clustering-keys": 2,
            "partition-key-distribution": "uniform",  # Distribution for hitting the partition
            "token-range-slices": 10000,
            "partition-key-buffer-reuse-size": 128,
            "statement-log-file-compression": "zstd",
            "max-errors-to-store": 1000,  # Number of error to make gemini fail, after N error, gemini will stop immediately with error
        }

        self.gemini_oracle_statements_file = f"gemini_oracle_statements_{self.unique_id}.log"
        self.gemini_test_statements_file = f"gemini_test_statements_{self.unique_id}.log"
        self.gemini_result_file = f"gemini_result_{self.unique_id}.log"

    def _generate_gemini_command(self):
        seed = self.params.get("gemini_seed")
        if seed is None:
            seed = random.randint(1, 100)

        table_options = self.params.get("gemini_table_options")
        log_statements = self.params.get("gemini_log_cql_statements")
        if log_statements is None:
            log_statements = True

        test_nodes = ",".join(self.test_cluster.get_node_cql_ips())

        cmd = f"gemini \
                --test-cluster=\"{test_nodes}\" \
                --seed={seed} \
                --schema-seed={seed} \
                --profiling-port=6060 \
                --bind=0.0.0.0:2112 \
                --outfile=/{self.gemini_result_file} \
                --replication-strategy=\"{{'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}}\" \
                --oracle-replication-strategy=\"{{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}}\" "

        if self.oracle_cluster is not None:
            oracle_nodes = ",".join(self.oracle_cluster.get_node_cql_ips())
            cmd += f'--oracle-cluster="{oracle_nodes}" '

        if log_statements:
            cmd += f"--test-statement-log-file=/{self.gemini_test_statements_file} \
                    --oracle-statement-log-file=/{self.gemini_oracle_statements_file} "

        credentials = self.loader_set.get_db_auth()

        if credentials and "--test-username" not in cmd:
            cmd += f"--test-username={credentials[0]} \
                --test-password={credentials[1]} \
                --oracle-username={credentials[0]} \
                --oracle-password={credentials[1]} "

        if table_options:
            cmd += " ".join([f'--table-options="{table_opt}"' for table_opt in table_options])

        stress_cmd = self.stress_cmd.replace("\n", " ").strip()

        cmd += " " + " ".join(f"--{key}={value}" for key, value in self.gemini_default_flags.items() if
                              key not in stress_cmd) + " " + stress_cmd

        self.gemini_commands.append(cmd)
        return cmd

    def _run_stress(self, loader, loader_idx, cpu_idx):
        for file_name in [self.gemini_result_file, self.gemini_test_statements_file, self.gemini_oracle_statements_file]:
            loader.remoter.run(f"touch $HOME/{file_name}", ignore_status=True, verbose=False)

        docker = cleanup_context = RemoteDocker(
            loader,
            self.docker_image_name,
            extra_docker_opts=f'--cpuset-cpus="{cpu_idx}" '
            if self.stress_num > 1
            else ""
            "--network=host "
            "--security-opt seccomp=unconfined "
            '--entrypoint="" '
            f"--label shell_marker={self.shell_marker} "
            f"-v $HOME/{self.gemini_result_file}:/{self.gemini_result_file} "
            f"-v $HOME/{self.gemini_test_statements_file}:/{self.gemini_test_statements_file} "
            f"-v $HOME/{self.gemini_oracle_statements_file}:/{self.gemini_oracle_statements_file} ",
        )

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, "gemini-l%s-c%s-%s.log" % (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug("gemini local log: %s", log_file_name)

        gemini_cmd = self._generate_gemini_command()
        try:
            prefix, *_ = gemini_cmd.split("gemini", maxsplit=1)
            reporter = GeminiVersionReporter(docker, prefix, loader.parent_cluster.test_config.argus_client())
            reporter.report()
        except Exception:  # noqa: BLE001
            LOGGER.info("Failed to collect scylla-bench version information", exc_info=True)

        with cleanup_context, GeminiEventsPublisher(node=loader, gemini_log_filename=log_file_name) as publisher, GeminiStressEvent(node=loader, cmd=gemini_cmd, log_file_name=log_file_name) as gemini_stress_event:
            try:
                publisher.event_id = gemini_stress_event.event_id
                gemini_stress_event.log_file_name = log_file_name
                result = docker.run(
                    cmd=gemini_cmd,
                    timeout=self.timeout,
                    ignore_status=False,
                    log_file=log_file_name,
                    retry=0,
                    timestamp_logs=True,
                )
                # sleep to gather all latest log messages
                time.sleep(5)
            except Exception as details:  # noqa: BLE001
                LOGGER.error(details)
                result = getattr(details, "result", NotGeminiErrorResult(details))

            if result.exited:
                gemini_stress_event.add_result(result=result)
                gemini_stress_event.severity = Severity.ERROR
            else:  # noqa: PLR5501
                if result.stderr:
                    gemini_stress_event.add_result(result=result)
                    gemini_stress_event.severity = Severity.WARNING

            local_gemini_result_file = os.path.join(docker.node.logdir, os.path.basename(self.gemini_result_file))
            results_copied = docker.receive_files(src=self.gemini_result_file, dst=local_gemini_result_file)
            assert results_copied, "gemini results aren't available, did gemini even run ?"

            local_gemini_test_statements_file = os.path.join(
                docker.node.logdir, os.path.basename(self.gemini_test_statements_file))
            local_gemini_oracle_statements_file = os.path.join(
                docker.node.logdir, os.path.basename(self.gemini_oracle_statements_file))
            docker.receive_files(src=self.gemini_test_statements_file, dst=local_gemini_test_statements_file)
            docker.receive_files(src=self.gemini_oracle_statements_file, dst=local_gemini_oracle_statements_file)

        return docker, result, local_gemini_result_file

    def get_gemini_results(self):
        parsed_results = []
        raw_results = self.get_results()
        for _, _, local_gemini_result_file in raw_results:
            assert local_gemini_result_file, "gemini results aren't available, did gemini even run ?"
            with open(local_gemini_result_file, encoding="utf-8") as local_file:
                content = local_file.read()
                res = self._parse_gemini_summary_json(content)
                if res:
                    parsed_results.append(res)

        return parsed_results

    @staticmethod
    def verify_gemini_results(results):
        stats = {"results": [], "errors": {}}
        if not results:
            LOGGER.error("Gemini results are not found")
            stats["status"] = "FAILED"
        else:
            for res in results:
                stats["results"].append(res)
                for err_type in ["write_errors", "read_errors", "errors"]:
                    if res.get(err_type, None):
                        LOGGER.error("Gemini {} errors: {}".format(err_type, res[err_type]))
                        stats["status"] = "FAILED"
                        stats["errors"][err_type] = res[err_type]
        if not stats.get("status"):
            stats["status"] = "PASSED"

        return stats

    @staticmethod
    def _parse_gemini_summary_json(json_str):
        results = {"result": {}}
        try:
            results = json.loads(json_str)

        except Exception as details:  # noqa: BLE001
            LOGGER.error("Invalid json document {}".format(details))

        return results.get("result")

    @staticmethod
    def _parse_gemini_summary(lines):
        results = {}
        enable_parse = False

        for line in lines:
            line.strip()
            if "Results:" in line:
                enable_parse = True
                continue
            if "run completed" in line:
                enable_parse = False
                continue
            if not enable_parse:
                continue

            split_idx = line.index(":")
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
            results[key] = int(value)
        return results
