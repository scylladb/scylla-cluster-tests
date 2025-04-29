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
import uuid
import pprint
from pathlib import Path
from typing import List, Dict

from sdcm.sct_events.loaders import CDCReaderStressEvent
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.stress.base import format_stress_cmd_error, DockerBasedStressThread
from sdcm.utils.cdc.options import CDC_LOGTABLE_SUFFIX


LOGGER = logging.getLogger(__name__)

PP = pprint.PrettyPrinter(indent=2)


class CDCLogReaderThread(DockerBasedStressThread):
    DOCKER_IMAGE_PARAM_NAME = "stress_image.cdc-stresser"

    def __init__(self, *args, **kwargs):

        self.keyspace = kwargs.pop("keyspace_name")
        self.cdc_log_table = kwargs.pop("base_table_name") + CDC_LOGTABLE_SUFFIX
        self.batching = kwargs.pop("enable_batching")
        super().__init__(*args, **kwargs)

    def build_stress_command(self, worker_id, worker_count):
        node_ips = ",".join([node.ip_address for node in self.node_list])
        shards_per_node = self.node_list[0].cpu_cores if self.batching else 1
        self.stress_cmd = f"{self.stress_cmd} -keyspace {self.keyspace} -table {self.cdc_log_table} \
                            -nodes {node_ips} -group-size {shards_per_node} \
                            -worker-id {worker_id} -worker-count {worker_count}"

    def _run_stress(self, loader, loader_idx, cpu_idx):
        loader_node_logdir = Path(loader.logdir)
        if not loader_node_logdir.exists():
            loader_node_logdir.mkdir()

        worker_count = self.max_workers
        worker_id = loader_idx * self.stress_num + cpu_idx
        log_file_name = loader_node_logdir.joinpath(f'cdclogreader-l{loader_idx}-{worker_id}-{uuid.uuid4()}.log')
        LOGGER.debug('cdc-stressor local log: %s', log_file_name)

        self.build_stress_command(worker_id, worker_count)

        LOGGER.info(self.stress_cmd)
        docker = cleanup_context = RemoteDocker(loader, self.docker_image_name,
                                                extra_docker_opts=f'--network=host --label shell_marker={self.shell_marker}')

        node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {self.stress_cmd}'

        CDCReaderStressEvent.start(node=loader, stress_cmd=self.stress_cmd).publish()

        try:
            with cleanup_context:
                result = docker.run(cmd=node_cmd,
                                    timeout=self.timeout + self.shutdown_timeout,
                                    ignore_status=True,
                                    log_file=log_file_name,
                                    verbose=True,
                                    retry=0
                                    )
                if not result.ok:
                    CDCReaderStressEvent.error(node=loader,
                                               stress_cmd=self.stress_cmd,
                                               errors=result.stderr.split("\n")).publish()
                return result
        except Exception as exc:  # noqa: BLE001
            CDCReaderStressEvent.failure(node=loader,
                                         stress_cmd=self.stress_cmd,
                                         errors=[format_stress_cmd_error(exc), ]).publish()
        finally:
            CDCReaderStressEvent.finish(node=loader, stress_cmd=self.stress_cmd).publish()
        return None

    @staticmethod
    def _parse_cdcreaderstressor_results(lines: List[str]) -> Dict:
        """parse result of cdcreader results
        lines:
            Results:
            num rows read:  95185
            rows read/s:    528.805556/s
            polls/s:        3039.144444/s
            idle polls:     529041/547046 (96.708686%)
            latency min:    0.524288 ms
            latency avg:    11.493153 ms
            latency median: 8.978431 ms
            latency 90%:    22.151167 ms
            latency 99%:    56.328191 ms
            latency 99.9%:  88.604671 ms
            latency max:    156.762111 ms

        return
            {
                "op rate": "1000",
                "latency min": "0.5",
                "latency max": "10",
                "latency mean": "4",
                ...
            }

        """
        cdcreader_cs_keys_map = {
            # {"num rows read": ["num rows read"]},
            "rows read/s": ["partition rate", "row rate"],
            "polls/s": ["op rate"],
            "latency min": ["latency min"],
            "latency avg": ["latency mean"],
            "latency median": ["latency median"],
            "latency 90%": ["latency 90th percentile"],
            "latency 99%": ["latency 99th percentile"],
            "latency 99.9%": ["latency 99.9th percentile"],
            "latency max": ["latency max"],
        }
        result = {}
        parse_enable = False
        for line in lines:
            if line.startswith("Results:"):
                parse_enable = True
            if not parse_enable:
                continue
            res = line.split(":")
            if len(res) < 2:
                continue
            name = res[0].strip()
            value = res[1].strip()
            if name in cdcreader_cs_keys_map:
                if name in ["rows read/s", "polls/s"]:
                    for replace_name in cdcreader_cs_keys_map[name]:
                        result[replace_name] = value.split("/")[0]
                else:
                    for replace_name in cdcreader_cs_keys_map[name]:
                        result[replace_name] = value.split(" ")[0]
        LOGGER.debug(result)
        return result

    def get_results(self) -> List[Dict]:
        """Return results of cdclog readers

        return list of dicts:
        [
            {
                "op rate": "1000",
                "latency min": "0.5",
                "latency max": "10",
                "latency mean": "4",
                ...
            },
            {
                "op rate": "1000",
                "latency min": "0.5",
                "latency max": "10",
                "latency mean": "4",
                ...
            }
        ]
        """
        results = []
        res_stats = []

        results = super().get_results()
        LOGGER.debug(PP.pformat(results))

        for result in results:
            res = self._parse_cdcreaderstressor_results(result.stdout.splitlines())

            if not res:
                LOGGER.warning("Result is empty")
                continue

            res_stats.append(res)
        self.kill()
        return res_stats
