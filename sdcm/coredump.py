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

from threading import Thread, Event
from typing import List, Optional
from logging import getLogger
from dataclasses import dataclass
import re
import os
from time import mktime
from datetime import datetime


from sdcm.sct_events import raise_event_on_failure, CoreDumpEvent
from sdcm.utils.decorators import retrying
from sdcm.remote import NETWORK_EXCEPTIONS


@dataclass
class CoreDumpInfo:
    pid: str
    node: 'BaseNode' = None
    corefile: str = ''
    timestamp: Optional[float] = None
    coredump_info: str = ''
    download_instructions: str = ''
    download_url: str = ''
    command_line: str = ''
    executable: str = ''

    def publish_event(self):
        CoreDumpEvent(
            corefile_url=self.download_url,
            download_instructions=self.download_instructions,
            backtrace=self.coredump_info,
            node=self.node,
            timestamp=self.timestamp
        )

    def __str__(self):
        if self.corefile:
            return f'CoreDump[{self.pid}, {self.corefile}]'
        return f'CoreDump[{self.pid}]'


class CoredumpExportThread(Thread):
    log = getLogger(__file__)

    def __init__(self, node: 'BaseNode', max_core_limit: int):
        self.node = node
        self.termination_event = Event()
        self.max_core_limit = max_core_limit
        self.core_dump_found: List[CoreDumpInfo] = []
        self.core_dump_in_progress: List[CoreDumpInfo] = []
        self.core_dump_completed: List[CoreDumpInfo] = []
        super().__init__(daemon=False)

    def stop(self):
        self.termination_event.set()

    def start(self):
        if self.node.is_ubuntu14():
            # fixme: ubuntu14 doesn't has coredumpctl, skip it.
            return
        super().start()

    @raise_event_on_failure
    def run(self):
        """
        Keep reporting new coredumps found, every 30 seconds.
        """
        while not self.termination_event.wait(30) or self.core_dump_in_progress:
            if not self.node.wait_ssh_up(verbose=False):
                continue
            self._process_coredumps(self.core_dump_in_progress, self.core_dump_completed)
            new_cores = self.extract_info_from_core_pids(self.get_core_pids(), exclude_cores=self.core_dump_found)
            self.push_new_cores_to_process(new_cores)

    def push_new_cores_to_process(self, new_cores: List[CoreDumpInfo]):
        self.core_dump_found.extend(new_cores)
        for core_dump in new_cores:
            if 'bash' in core_dump.executable:
                continue
            self.log_coredump(core_dump)
            if len(self.core_dump_completed) + len(self.core_dump_in_progress) < self.max_core_limit:
                self.core_dump_in_progress.append(core_dump)

    def process_coredumps(self):
        self._process_coredumps(self.core_dump_in_progress, self.core_dump_completed)

    def _process_coredumps(self, in_progress: List[CoreDumpInfo], completed: List[CoreDumpInfo]):
        """
        Get core files from node and report them
        """
        if not in_progress:
            return
        for core_info in in_progress.copy():
            try:
                result = self.upload_coredump(core_info)
                completed.append(core_info)
                in_progress.remove(core_info)
                if result:
                    self.publish_event(core_info)
            except:  # pylint: disable=bare-except
                pass

    @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on getting pid of cores")
    def get_core_pids(self) -> Optional[List[str]]:
        result = self.node.remoter.run(
            'sudo coredumpctl --no-pager --no-legend 2>&1', verbose=False, ignore_status=True)
        if "No coredumps found" in result.stdout or result.exit_status == 127:
            # exit_status 127: coredumpctl command not found
            return None
        pids_list = []
        result = result.stdout + result.stderr
        # Extracting PIDs from coredumpctl output as such:
        #     TIME                            PID   UID   GID SIG COREFILE  EXE
        #     Tue 2020-01-14 16:16:39 +07    3530  1000  1000   3 present   /usr/bin/scylla
        #     Tue 2020-01-14 16:18:56 +07    6321  1000  1000   3 present   /usr/bin/scylla
        #     Tue 2020-01-14 16:31:39 +07   18554  1000  1000   3 present   /usr/bin/scylla
        for line in result.splitlines():
            if "bash" in line:
                # Remove bash core which generated by scylla_setup for internal test purpose.
                # Workaround for https://github.com/scylladb/scylla/issues/6159
                # TIME                            PID   UID   GID SIG PRESENT EXE
                # Sun 2020-04-19 12:27:29 UTC   28987     0     0  11 * /usr/bin/bash
                continue

            columns = re.split(r'[ ]{2,}', line)
            if len(columns) < 2:
                continue
            pid = columns[1]
            if re.findall(r'[^0-9]', pid):
                self.log.error("PID pattern matched non-numerical value. Looks like coredumpctl changed it's output")
                continue
            pids_list.append(pid)
        return pids_list

    def publish_event(self, core_info: CoreDumpInfo):
        try:
            core_info.publish_event()
        except Exception as exc:  # pylint: disable=bare-except
            self.log.error(f"Failed to publish coredump event due to the: {str(exc)}")

    def extract_info_from_core_pids(self, pids: List[str], exclude_cores: List[CoreDumpInfo]) -> List[CoreDumpInfo]:
        output = []
        for pid in pids:
            found = False
            for core_info in exclude_cores:
                if core_info.pid == pid:
                    found = True
                    break
            if found:
                continue
            try:
                core_info = self.extract_coredump_info(pid)
            except Exception as exc:  # pylint: disable=bare-except
                self.log.error(f"Failed to extract coredump information for {pid} due to the: {str(exc)}")
                continue
            self.publish_event(core_info)
            output.append(core_info)
        return output

    def extract_coredump_info(self, pid: str) -> Optional[CoreDumpInfo]:
        coredump_info = self._get_coredump_info(pid)
        corefile = ''
        executable = ''
        command_line = ''
        timestamp = None
        # Extracting Coredump and Timestamp from coredumpctl output:
        #            PID: 37349 (scylla)
        #            UID: 996 (scylla)
        #            GID: 1001 (scylla)
        #         Signal: 3 (QUIT)
        #      Timestamp: Tue 2020-01-14 10:40:25 UTC (6min ago)
        #   Command Line: /usr/bin/scylla --blocked-reactor-notify-ms 500 --abort-on-lsa-bad-alloc 1
        #       --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --log-to-syslog 1 --log-to-stdout 0
        #       --default-log-level info --network-stack posix --io-properties-file=/etc/scylla.d/io_properties.yaml
        #       --cpuset 1-7,9-15
        #     Executable: /opt/scylladb/libexec/scylla
        #  Control Group: /scylla.slice/scylla-server.slice/scylla-server.service
        #           Unit: scylla-server.service
        #          Slice: scylla-server.slice
        #        Boot ID: 0dc7f4137d5f47a3bda07dd046937fc2
        #     Machine ID: df877a200226bc47d06f26dae0736ec9
        #       Hostname: longevity-10gb-3h-dkropachev-db-node-b9ebdfb0-1
        #       Coredump: /var/lib/systemd/coredump/core.scylla.996.0dc7f4137d5f47a3bda07dd046937fc2.37349.1578998425000000
        #        Message: Process 37349 (scylla) of user 996 dumped core.
        #
        #                 Stack trace of thread 37349:
        #                 #0  0x00007ffc2e724704 n/a (linux-vdso.so.1)
        #                 #1  0x00007ffc2e724992 __vdso_clock_gettime (linux-vdso.so.1)
        #                 #2  0x00007f251c3082c3 __clock_gettime (libc.so.6)
        #                 #3  0x00007f251c5f3b85 _ZNSt6chrono3_V212steady_clock3nowEv (libstdc++.so.6)
        #                 #4  0x0000000002ab94e5 _ZN7seastar7reactor3runEv (scylla)
        #                 #5  0x00000000029fc1ed _ZN7seastar12app_template14run_deprecatedEiPPcOSt8functionIFvvEE (scylla)
        #                 #6  0x00000000029fcedf _ZN7seastar12app_template3runEiPPcOSt8functionIFNS_6futureIJiEEEvEE (scylla)
        #                 #7  0x0000000000794222 main (scylla)
        #
        # Coredump could be absent when file was removed
        for line in coredump_info.splitlines():
            line = line.strip()
            if line.startswith('Executable:'):
                executable = line[12:].strip()
            elif line.startswith('Command Line:'):
                command_line = line[14:].strip()
            elif line.startswith('Coredump:') or line.startswith('Storage:'):
                if "(inaccessible)" in line:
                    # Ignore inaccessible cores
                    #       Storage: /var/lib/systemd/coredump/core.vi.1000.6c4de4c206a0476e88444e5ebaaac482.18554.1578994298000000.lz4 (inaccessible)
                    continue
                corefile = line[line.find(':') + 1:]
            elif line.startswith('Timestamp:'):
                try:
                    # Converting time string "Tue 2020-01-14 10:40:25 UTC (6min ago)" to timestamp
                    tmp = re.search(r'Timestamp: ([^\(]+)(\([^\)]+\)|)', line).group(1)
                    fmt = "%a %Y-%m-%d %H:%M:%S" if len(tmp.split()) == 3 else "%a %Y-%m-%d %H:%M:%S %Z"
                    timestamp = mktime(datetime.strptime(tmp.strip(' '), fmt).timetuple())
                except Exception as exc:  # pylint: disable=broad-except
                    self.log.error(f"Failed to convert date '{line}', due to error: {str(exc)}")
        return CoreDumpInfo(pid=pid, executable=executable, command_line=command_line, corefile=corefile,
                            timestamp=timestamp, coredump_info=coredump_info)

    def upload_coredump(self, core_info: CoreDumpInfo):
        if core_info.download_url:
            return False
        if not core_info.corefile:
            self.log.error(f"{str(core_info)} has inaccessible corefile, can't upload it")
            return False
        try:
            self.log.debug(f'Start uploading file: {core_info.corefile}')
            core_info.download_instructions = 'Coredump upload in progress'
            core_info.download_url, core_info.download_instructions = self._upload_coredump(core_info.corefile)
            return True
        except Exception as exc:  # pylint: disable=broad-except
            core_info.download_instructions = 'failed to upload core'
            self.log.error(f"Following error occurred during uploading coredump {core_info.corefile}: {str(exc)}")
            raise

    def log_coredump(self, core_info: CoreDumpInfo):
        if not core_info.coredump_info:
            return
        log_file = os.path.join(self.node.logdir, 'coredump.log')
        with open(log_file, 'a') as log_file_obj:
            log_file_obj.write(core_info.coredump_info)
        for line in core_info.coredump_info.splitlines():
            self.log.error(line)

    @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on getting coredump backtrace")
    def _get_coredump_info(self, pid):
        """
        Get coredump backtraces.

        :param pid: PID of the core.
        :return: fabric.Result output
        """
        output = self.node.remoter.run(
            f'sudo coredumpctl info --no-pager --no-legend {pid}', verbose=False, ignore_status=False)
        return output.stdout + output.stderr

    @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on uploading coredump")
    def _upload_coredump(self, coredump):
        coredump = self._pack_coredump(coredump)
        base_upload_url = 'upload.scylladb.com/%s/%s'
        coredump_id = os.path.basename(coredump)[:-3]
        upload_url = base_upload_url % (coredump_id, os.path.basename(coredump))
        self.log.info('Uploading coredump %s to %s' % (coredump, upload_url))
        self.node.remoter.run("sudo curl --request PUT --upload-file "
                              "'%s' '%s'" % (coredump, upload_url))
        download_url = 'https://storage.cloud.google.com/%s' % upload_url
        self.log.info("You can download it by %s (available for ScyllaDB employee)", download_url)
        download_instructions = 'gsutil cp gs://%s .\ngunzip %s' % (upload_url, coredump)
        return download_url, download_instructions

    def _pack_coredump(self, coredump: str) -> str:
        extensions = ['.lz4', '.zip', '.gz', '.gzip']
        for extension in extensions:
            if coredump.endswith(extension):
                return coredump
        try:  # pylint: disable=unreachable
            if self.node.is_debian() or self.node.is_ubuntu():
                self.node.remoter.run('sudo apt-get install -y pigz')
            else:
                self.node.remoter.run('sudo yum install -y pigz')
            self.node.remoter.run('sudo pigz --fast --keep {}'.format(coredump))
            coredump += '.gz'
        except NETWORK_EXCEPTIONS:  # pylint: disable=try-except-raise
            raise
        except Exception as ex:  # pylint: disable=broad-except
            self.log.warning("Failed to compress coredump '%s': %s", coredump, ex)
        return coredump

    @property
    def n_coredumps(self) -> int:
        return len(self.core_dump_found)
