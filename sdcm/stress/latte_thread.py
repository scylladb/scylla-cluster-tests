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

import os
import re
import time
import uuid
import logging
from pathlib import Path

from sdcm.prometheus import nemesis_metrics_obj
from sdcm.provision.helpers.certificate import SCYLLA_SSL_CONF_DIR, TLSAssets
from sdcm.remote.libssh2_client.exceptions import Failure
from sdcm.sct_events.loaders import LatteStressEvent
from sdcm.sct_events import Severity
from sdcm.stress.base import DockerBasedStressThread
from sdcm.utils.common import (
    FileFollowerThread,
    generate_random_string,
    get_sct_root_path,
)
from sdcm.utils.docker_remote import RemoteDocker

LOGGER = logging.getLogger(__name__)


class LatteStatsPublisher(FileFollowerThread):
    METRICS = {}

    def __init__(self, loader_node, loader_idx, latte_log_filename, operation):
        super().__init__()
        self.loader_node = loader_node
        self.loader_idx = loader_idx
        self.latte_log_filename = latte_log_filename
        self.uuid = generate_random_string(10)
        self.operation = operation

        gauge_name = self.gauge_name(self.operation)
        if gauge_name not in self.METRICS:
            metrics = nemesis_metrics_obj()
            self.METRICS[gauge_name] = metrics.create_gauge(gauge_name,
                                                            'Gauge for latte metrics',
                                                            ['instance', 'loader_idx', 'uuid', 'type'])

    @staticmethod
    def gauge_name(operation):
        return 'sct_latte_%s_gauge' % operation.replace('-', '_')

    def set_metric(self, operation, name, value):
        metric = self.METRICS[self.gauge_name(operation)]
        metric.labels(self.loader_node.ip_address, self.loader_idx, self.uuid, name).set(value)

    def run(self):
        regex = re.compile(r"""
        \s*(?P<secoands>\d*\.\d*)
        \s*(?P<ops>\d*)
        \s*(?P<reqs>\d*)
        \s*(?P<min>\d*\.\d*)
        \s*(?P<p25>\d*\.\d*)
        \s*(?P<p50>\d*\.\d*)
        \s*(?P<p75>\d*\.\d*)
        \s*(?P<p90>\d*\.\d*)
        \s*(?P<p95>\d*\.\d*)
        \s*(?P<p99>\d*\.\d*)
        \s*(?P<p999>\d*\.\d*)
        \s*(?P<max>\d*\.\d*)\s*
        """, re.VERBOSE)

        while not self.stopped():
            exists = os.path.isfile(self.latte_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line in self.follow_file(self.latte_log_filename):
                if self.stopped():
                    break
                try:
                    match = regex.search(line)
                    if match:
                        for key, _value in match.groupdict().items():
                            value = float(_value)
                            self.set_metric(self.operation, key, value)

                except Exception:  # pylint: disable=broad-except
                    LOGGER.exception("fail to send metric")


class LatteStressThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes

    DOCKER_IMAGE_PARAM_NAME = "stress_image.latte"

    def build_stress_cmd(self, cmd_runner, loader):  # pylint: disable=too-many-locals
        hosts = " ".join([i.cql_address for i in self.node_list])

        # extract the script so we know which files to mount into the docker image
        script_name_regx = re.compile(r'([/\w-]*\.rn)')
        script_name = script_name_regx.search(self.stress_cmd).group(0)

        for src_file in (Path(get_sct_root_path()) / script_name).parent.iterdir():
            cmd_runner.send_files(str(src_file), str(Path(script_name).parent / src_file.name))

        ssl_config = ''
        if self.params['client_encrypt']:
            for ssl_file in loader.ssl_conf_dir.iterdir():
                if ssl_file.is_file():
                    cmd_runner.send_files(str(ssl_file),
                                          str(SCYLLA_SSL_CONF_DIR / ssl_file.name),
                                          verbose=True)

            ssl_config += (f' --ssl --ssl-ca {SCYLLA_SSL_CONF_DIR}/{TLSAssets.CA_CERT} '
                           f'--ssl-cert {SCYLLA_SSL_CONF_DIR}/{TLSAssets.CLIENT_CERT} '
                           f'--ssl-key {SCYLLA_SSL_CONF_DIR}/{TLSAssets.CLIENT_KEY}')

        auth_config = ''
        if credentials := self.loader_set.get_db_auth():
            auth_config = f' --user {credentials[0]} --password {credentials[1]}'

        datacenter = ""
        if self.loader_set.test_config.MULTI_REGION:
            # The datacenter name can be received from "nodetool status" output. It's possible for DB nodes only,
            # not for loader nodes. So call next function for DB nodes
            datacenter_name_per_region = self.loader_set.get_datacenter_name_per_region(db_nodes=self.node_list)
            if loader_dc := datacenter_name_per_region.get(loader.region):
                datacenter = f"--datacenter {loader_dc}"
            else:
                LOGGER.error(
                    "Not found datacenter for loader region '%s'. Datacenter per loader dict: %s",
                    loader.region, datacenter_name_per_region)

        custom_schema_params = ""
        if latte_schema_parameters := self.params['latte_schema_parameters']:
            # NOTE: string parameters in latte must be wrapped into escaped double-quotes: foo="\"bar\""
            for k, v in latte_schema_parameters.items():
                processed_v = v
                try:
                    processed_v = int(v)
                except Exception:  # pylint: disable=broad-except  # noqa: BLE001
                    if v not in ('true', 'false'):
                        processed_v = r"\"%s\"" % v
                custom_schema_params += " -P {k}={v}".format(k=k, v=processed_v)
        cmd_runner.run(
            cmd=f'latte schema {script_name} {ssl_config} {auth_config}{custom_schema_params} -- {hosts}',
            timeout=self.timeout,
            retry=0,
        )
        stress_cmd = f'{self.stress_cmd} {ssl_config} {auth_config} {datacenter} -q -- {hosts} '

        return stress_cmd

    @staticmethod
    def function_name(stress_cmd):
        function_name_regex = re.compile(r'.*--function\s*(.*?\S)\s')
        if match := function_name_regex.match(stress_cmd):
            return match.group(1)
        else:
            return 'read'

    @staticmethod
    def parse_final_output(result):
        """
        parse latte final results to match what we get out of cassandra-stress
        latencies returned in milliseconds

        :param result: output of latte stats
        :return: dict
        """
        ops_regex = re.compile(r'\s*Throughput(.*?)\[op\/s\]\s*(?P<op_rate>\d*)\s')
        latency_99_regex = re.compile(r'\s* 99 \s*(?P<latency_99th_percentile>\d*\.\d*)\s')
        latency_mean_regex = re.compile(
            r'\s*(?:Mean resp\. time|Request latency)\s*(?:\[(ms|s)\])?\s*(?P<latency_mean>\d+\.\d+)')

        output = {'latency 99th percentile': 0,
                  'latency mean': 0,
                  'op rate': 0
                  }
        for line in result.stdout.split("SUMMARY STATS")[-1].splitlines():
            if match := ops_regex.match(line):
                output['op rate'] = match.groupdict()['op_rate']
                continue
            if match := latency_99_regex.match(line):
                output['latency 99th percentile'] = float(match.groupdict()['latency_99th_percentile'])
                continue
            if match := latency_mean_regex.match(line):
                output['latency mean'] = float(match.groupdict()['latency_mean'])
                continue

        # output back to strings
        output = {k: str(v) for k, v in output.items()}
        return output

    def _run_stress(self, loader, loader_idx, cpu_idx):
        cpu_options = ""

        if self.stress_num > 1:
            cpu_options = f'--cpuset-cpus="{cpu_idx}"'

        cmd_runner = cleanup_context = RemoteDocker(
            loader, self.docker_image_name,
            command_line="-c 'tail -f /dev/null'",
            extra_docker_opts=f'--entrypoint /bin/bash {cpu_options} --label shell_marker={self.shell_marker}')
        stress_cmd = self.build_stress_cmd(cmd_runner, loader)

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, 'latte-l%s-c%s-%s.log' %
                                     (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('latter-stress local log: %s', log_file_name)

        LOGGER.debug("running: %s", stress_cmd)

        operation = self.function_name(stress_cmd)

        result = {}
        with cleanup_context, \
                LatteStatsPublisher(loader, loader_idx, latte_log_filename=log_file_name,
                                    operation=operation), \
                LatteStressEvent(node=loader,
                                 stress_cmd=stress_cmd,
                                 log_file_name=log_file_name,
                                 ) as latte_stress_event:
            try:
                result = cmd_runner.run(
                    cmd=stress_cmd,
                    timeout=self.timeout + self.shutdown_timeout,
                    log_file=log_file_name,
                    retry=0,
                )
                result = self.parse_final_output(result)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                self.configure_event_on_failure(stress_event=latte_stress_event, exc=exc)

        return loader, result, latte_stress_event
        # TODOs:
        # 1) take back the report workload..3.0.8.p128.t1.c1.20231025.220812.json

    def configure_event_on_failure(self, stress_event: LatteStressEvent, exc: Exception | Failure):
        error_msg = format_stress_cmd_error(exc)
        if (hasattr(exc, "result") and exc.result.failed) and exc.result.exited == 137:
            error_msg = f"Stress killed by test/teardown\n{error_msg}"
            stress_event.severity = Severity.WARNING
        elif self.stop_test_on_failure:
            stress_event.severity = Severity.CRITICAL
        else:
            stress_event.severity = Severity.ERROR
        stress_event.add_error(errors=[error_msg])

    def get_results(self) -> list:
        return [result for _, result, _ in super().get_results()]


def format_stress_cmd_error(exc: Exception) -> str:
    """Format nicely the exception from a stress command failure."""

    if hasattr(exc, "result") and exc.result.failed:
        # NOTE: print only last 2 lines in common case or whole 'panic' message
        last_n_lines, line_index = exc.result.stderr.splitlines()[-30:], -2
        for current_line_index, line in enumerate(last_n_lines):
            if "panicked at" in line:
                line_index = current_line_index
                break
        message = "\n".join(last_n_lines[line_index:])
        return f"Stress command completed with bad status {exc.result.exited}: {message}"
    return f"Stress command execution failed with: {exc}"
