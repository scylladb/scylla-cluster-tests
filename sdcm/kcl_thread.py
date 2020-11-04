import os
import time
import random
import logging
import uuid
import threading

from sdcm.stress_thread import format_stress_cmd_error, DockerBasedStressThread
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.sct_events import StressEvent, InfoEvent, Severity
from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class KclStressEvent(StressEvent):
    pass


class KclStressThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes

    def run(self):
        _self = super().run()
        # wait for the KCL thread to create the tables, so the YCSB thread beat this one, and start failing
        time.sleep(120)
        return _self

    def build_stress_cmd(self):
        if self.params.get('alternator_use_dns_routing'):
            target_address = 'alternator'
        else:
            if getattr(self.node_list[0], 'parent_cluster'):
                target_address = self.node_list[0].parent_cluster.get_node().ip_address
            else:
                target_address = self.node_list[0].ip_address
        stress_cmd = f"./gradlew run --args=\' {self.stress_cmd.replace('hydra-kcl', '')} -e http://{target_address}:{self.params.get('alternator_port')} \'"
        return stress_cmd

    def _run_stress(self, loader, loader_idx, cpu_idx):
        dns_options = ""
        if self.params.get('alternator_use_dns_routing'):
            dns = RemoteDocker(loader, "scylladb/hydra-loaders:alternator-dns-0.2",
                               command_line=f'python3 /dns_server.py {self.db_node_to_query(loader)} '
                                            f'{self.params.get("alternator_port")}',
                               extra_docker_opts=f'--label shell_marker={self.shell_marker}')
            dns_options += f'--dns {dns.internal_ip_address} --dns-option use-vc'
        docker = RemoteDocker(loader, "scylladb/hydra-loaders:kcl-jdk8-20201104",
                              extra_docker_opts=f'{dns_options} --label shell_marker={self.shell_marker}')
        stress_cmd = self.build_stress_cmd()

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, 'kcl-l%s-c%s-%s.log' %
                                     (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('kcl-stress local log: %s', log_file_name)

        LOGGER.debug("'running: %s", stress_cmd)

        if self.stress_num > 1:
            node_cmd = 'taskset -c %s bash -c "%s"' % (cpu_idx, stress_cmd)
        else:
            node_cmd = stress_cmd

        node_cmd = 'cd /hydra-kcl && {}'.format(node_cmd)

        KclStressEvent('start', node=loader, stress_cmd=stress_cmd)

        try:
            result = docker.run(cmd=node_cmd,
                                timeout=self.timeout + self.shutdown_timeout,
                                log_file=log_file_name,
                                )

            return result

        except Exception as exc:  # pylint: disable=broad-except
            errors_str = format_stress_cmd_error(exc)
            KclStressEvent(type='failure', node=str(loader), stress_cmd=self.stress_cmd,
                           log_file_name=log_file_name, severity=Severity.ERROR,
                           errors=[errors_str])
            raise
        finally:
            KclStressEvent('finish', node=loader, stress_cmd=stress_cmd, log_file_name=log_file_name)


class CompareTablesSizesThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def kill(self):
        self._stop_event.set()

    def db_node_to_query(self, loader):
        """Select DB node in the same region as loader node to query"""
        db_nodes = [db_node for db_node in self.node_list]  # if not db_node.running_nemesis]
        assert db_nodes, "No node to query, nemesis runs on all DB nodes!"
        node_to_query = random.choice(db_nodes)
        LOGGER.debug(f"Selected '{node_to_query}' to query for local nodes")
        return node_to_query

    def _run_stress(self, loader, loader_idx, cpu_idx):
        KclStressEvent('start', node=loader, stress_cmd=self.stress_cmd)
        try:
            options_str = self.stress_cmd.replace('table_compare', '').strip()
            options = dict(item.strip().split("=") for item in options_str.split(";"))
            interval = int(options.get('interval', 20))
            src_table = options.get('src_table')
            dst_table = options.get('dst_table')

            while not self._stop_event.is_set():
                node: BaseNode = self.db_node_to_query(loader)
                node.run_nodetool('flush')

                src_size = node.get_cfstats(src_table)['Number of partitions (estimate)']
                dst_size = node.get_cfstats(dst_table)['Number of partitions (estimate)']

                status = f"== CompareTablesSizesThread: dst table/src table number of partitions: {dst_size}/{src_size} =="
                LOGGER.info(status)
                InfoEvent(status)

                if src_size == 0:
                    continue
                if dst_size >= src_size:
                    InfoEvent("== CompareTablesSizesThread: Done ==")
                    break
                time.sleep(interval)
            return None

        except Exception as exc:  # pylint: disable=broad-except
            errors_str = format_stress_cmd_error(exc)
            KclStressEvent(type='failure', node=str(loader), stress_cmd=self.stress_cmd,
                           severity=Severity.ERROR,
                           errors=[errors_str])
            raise
        finally:
            KclStressEvent('finish', node=loader)
