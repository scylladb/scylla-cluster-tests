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

import json
import logging
import os
import random
import time
import uuid
from pathlib import Path
from typing import NamedTuple

from sdcm.cluster import BaseCluster, BaseScyllaCluster
from sdcm.reporting.tooling_reporter import GeminiVersionReporter
from sdcm.sct_events import Severity
from sdcm.sct_events.loaders import GeminiStressEvent, GeminiStressLogEvent
from sdcm.stress.base import ParseResults
from sdcm.stress_thread import DockerBasedStressThread
from sdcm.utils.argus import report_stress_command
from sdcm.utils.common import FileFollowerThread
from sdcm.utils.curl import curl_with_retry
from sdcm.utils.docker_remote import RemoteDocker

LOGGER = logging.getLogger(__name__)

# The monitor scrapes the metrics of the first gemini worker of each loader on this port.
GEMINI_METRICS_PORT = 2112
GEMINI_PROFILING_PORT = 6060


class GeminiWorkerFiles(NamedTuple):
    """The output files of one gemini worker, in the loader home and in the root of its container."""

    result: str
    test_statements: str
    oracle_statements: str
    summary: str

    @classmethod
    def for_worker(cls, worker_id: str) -> "GeminiWorkerFiles":
        return cls(
            result=f"gemini_result_{worker_id}.log",
            test_statements=f"gemini_test_statements_{worker_id}.log",
            oracle_statements=f"gemini_oracle_statements_{worker_id}.log",
            summary=f"gemini-summary-{worker_id}.txt",
        )


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

    def __init__(  # noqa: PLR0913
        self,
        test_cluster: BaseCluster | BaseScyllaCluster,
        oracle_cluster: BaseCluster | None,
        loaders,
        stress_cmd: str,
        timeout=None,
        params=None,
        stress_num=1,
        round_robin=False,
        stop_test_on_failure=True,
    ):
        super().__init__(
            loader_set=loaders,
            stress_cmd=stress_cmd,
            timeout=timeout,
            params=params,
            stress_num=stress_num,
            round_robin=round_robin,
            stop_test_on_failure=stop_test_on_failure,
        )
        self.test_cluster = test_cluster
        self.oracle_cluster = oracle_cluster
        self.gemini_commands = []
        self.unique_id = uuid.uuid4()

        self.gemini_schema_file = f"gemini_schema_{self.unique_id}.json"

    def worker_files(self, loader_idx: int, cpu_idx: int) -> GeminiWorkerFiles:
        """Return the output files of the worker on CPU *cpu_idx* of loader *loader_idx*.

        Every worker writes its own files, because the workers of one loader share its home.
        """
        return GeminiWorkerFiles.for_worker(f"{self.unique_id}-l{loader_idx}-c{cpu_idx}")

    def generate_gemini_command(self, files: GeminiWorkerFiles, schema_path=None, cpu_idx: int = 0):
        """Return the gemini command of the worker on CPU *cpu_idx*.

        The containers of one loader share the host network, and gemini exits when it cannot bind
        a port. So each worker listens on the base metrics and profiling ports plus *cpu_idx*.
        """
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
                --profiling-port={GEMINI_PROFILING_PORT + cpu_idx} \
                --bind=0.0.0.0:{GEMINI_METRICS_PORT + cpu_idx} \
                --outfile=/{files.result} \
                --summary-file=/{files.summary} \
                --replication-strategy=\"{{'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}}\" \
                --oracle-replication-strategy=\"{{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}}\" "

        if self.oracle_cluster is not None:
            oracle_nodes = ",".join(self.oracle_cluster.get_node_cql_ips())
            cmd += f'--oracle-cluster="{oracle_nodes}" '

        # schema_path is only set when a local file was uploaded and mounted into Docker
        if schema_path:
            cmd += f'--schema="{schema_path}" '

        if log_statements:
            cmd += f"--test-statement-log-file=/{files.test_statements} \
                    --oracle-statement-log-file=/{files.oracle_statements} "

        credentials = self.loader_set.get_db_auth()

        if credentials and "--test-username" not in cmd:
            cmd += f"--test-username={credentials[0]} \
                --test-password={credentials[1]} \
                --oracle-username={credentials[0]} \
                --oracle-password={credentials[1]} "

        if table_options:
            cmd += " ".join([f'--table-options="{table_opt}"' for table_opt in table_options])

        cmd += " " + self.stress_cmd.replace("\n", " ").strip()

        self.gemini_commands.append(cmd)
        return cmd

    def _run_stress(self, loader, loader_idx, cpu_idx):
        schema_url = self.params.get("gemini_schema_url")
        schema_mount = ""
        schema_path = None
        if schema_url:
            if schema_url.startswith(("http://", "https://")):
                LOGGER.info("gemini_schema_url %r is a remote URL; downloading to loader.", schema_url)
                loader.remoter.run(
                    curl_with_retry(
                        schema_url,
                        output=f"$HOME/{self.gemini_schema_file}",
                        silent=True,
                        follow_redirects=True,
                        fail_early=True,
                    ),
                    verbose=False,
                )
            else:
                if not Path(schema_url).exists():
                    raise FileNotFoundError(
                        f"gemini_schema_url points to a local path that does not exist: {schema_url!r}"
                    )
                loader.remoter.send_files(src=schema_url, dst=f"$HOME/{self.gemini_schema_file}")
            schema_path = f"/{self.gemini_schema_file}"
            schema_mount = f"-v $HOME/{self.gemini_schema_file}:/{self.gemini_schema_file}:z "

        files = self.worker_files(loader_idx, cpu_idx)
        for file_name in files:
            loader.remoter.run(f"touch $HOME/{file_name}", ignore_status=True, verbose=False)

        docker = cleanup_context = RemoteDocker(
            loader,
            self.docker_image_name,
            extra_docker_opts=(f'--cpuset-cpus="{cpu_idx}" ' if self.stress_num > 1 else "")
            + (
                "--network=host "
                "--security-opt seccomp=unconfined "
                '--entrypoint="" '
                f"--label shell_marker={self.shell_marker} "
            )
            + "".join(f"-v $HOME/{file_name}:/{file_name}:z " for file_name in files)
            + schema_mount,
        )

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, "gemini-l%s-c%s-%s.log" % (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug("gemini local log: %s", log_file_name)

        gemini_cmd = self.generate_gemini_command(files, schema_path=schema_path, cpu_idx=cpu_idx)
        try:
            prefix, *_ = gemini_cmd.split("gemini", maxsplit=1)
            reporter = GeminiVersionReporter(docker, prefix, loader.parent_cluster.test_config.argus_client())
            reporter.report()
        except Exception:  # noqa: BLE001
            LOGGER.info("Failed to collect scylla-bench version information", exc_info=True)

        report_stress_command(
            client=loader.parent_cluster.test_config.argus_client(),
            stress_cmd=gemini_cmd,
            log_path=log_file_name,
            loader_name=loader.name,
        )
        with (
            cleanup_context,
            GeminiEventsPublisher(node=loader, gemini_log_filename=log_file_name) as publisher,
            GeminiStressEvent(node=loader, cmd=gemini_cmd, log_file_name=log_file_name) as gemini_stress_event,
        ):
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

            local_files = GeminiWorkerFiles(*(os.path.join(docker.node.logdir, file_name) for file_name in files))
            assert docker.receive_files(src=files.result, dst=local_files.result), (
                "gemini results aren't available, did gemini even run ?"
            )
            for src, dst in zip(files[1:], local_files[1:]):
                docker.receive_files(src=src, dst=dst)

        return docker, result, local_files.result

    def get_gemini_results(self):
        parsed_results = []
        raw_results = self.get_results()
        for _, _, local_gemini_result_file in raw_results:
            assert local_gemini_result_file, "gemini results aren't available, did gemini even run ?"
            with open(local_gemini_result_file, encoding="utf-8") as local_file:
                content = local_file.read()
                res = self.parse_gemini_summary_json(content)
                if res:
                    parsed_results.append(res)

        return parsed_results

    def parse_results(self) -> ParseResults:
        results = []
        errors = {}

        for docker, _, local_gemini_result_file in self.get_results():
            loader_name = docker.node.name
            if not local_gemini_result_file:
                errors.setdefault(loader_name, []).append((Severity.ERROR, "gemini results aren't available"))
                continue
            with open(local_gemini_result_file, encoding="utf-8") as local_file:
                summary = self.parse_gemini_summary_json(local_file.read())
            if not summary:
                errors.setdefault(loader_name, []).append(
                    (Severity.ERROR, f"could not parse gemini summary from {local_gemini_result_file}")
                )
                continue
            results.append(summary)
            for error_type in ("write_errors", "read_errors", "errors"):
                if reported := summary.get(error_type):
                    errors.setdefault(loader_name, []).append((Severity.ERROR, f"gemini {error_type}: {reported}"))

        return results, errors

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
                        LOGGER.error(f"Gemini {err_type} errors: {res[err_type]}")
                        stats["status"] = "FAILED"
                        stats["errors"][err_type] = res[err_type]
        if not stats.get("status"):
            stats["status"] = "PASSED"

        return stats

    @staticmethod
    def parse_gemini_summary_json(json_str):
        results = {"result": {}}
        try:
            results = json.loads(json_str)

        except Exception as details:  # noqa: BLE001
            LOGGER.error(f"Invalid json document {details}")

        return results.get("result")

    @staticmethod
    def parse_gemini_summary(lines):
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
            value = line[split_idx + 1 :].split()[0]
            results[key] = int(value)
        return results
