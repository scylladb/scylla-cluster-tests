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

import os
import re
import time
import json
from abc import abstractmethod
from typing import List, Optional, Dict
from datetime import datetime
from functools import cached_property
from threading import Thread, Event
from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager

from sdcm.log import SDCMAdapter
from sdcm.remote import NETWORK_EXCEPTIONS
from sdcm.utils.decorators import timeout
from sdcm.utils.version_utils import get_systemd_version
from sdcm.sct_events.system import CoreDumpEvent
from sdcm.sct_events.decorators import raise_event_on_failure


# pylint: disable=too-many-instance-attributes
@dataclass
class CoreDumpInfo:
    pid: str
    node: 'BaseNode' = None
    corefile: str = ''
    source_timestamp: Optional[float] = None
    coredump_info: str = ''
    download_instructions: str = ''
    download_url: str = ''
    command_line: str = ''
    executable: str = ''
    process_retry: int = 0

    def publish_event(self):
        CoreDumpEvent(
            node=self.node,
            corefile_url=self.download_url,
            backtrace=self.coredump_info,
            download_instructions=self.download_instructions,
            source_timestamp=self.source_timestamp
        ).publish()

    def __str__(self):
        if self.corefile:
            return f'CoreDump[{self.pid}, {self.corefile}]'
        return f'CoreDump[{self.pid}]'

    # pylint: disable=too-many-arguments
    def update(self,
               node: 'BaseNode' = None,
               corefile: str = None,
               source_timestamp: Optional[float] = None,
               coredump_info: str = None,
               download_instructions: str = None,
               download_url: str = None,
               command_line: str = None,
               executable: str = None,
               process_retry: int = None):
        for attr_name, attr_value in {
            'node': node,
            'corefile': corefile,
            'source_timestamp': source_timestamp,
            'coredump_info': coredump_info,
            'download_instructions': download_instructions,
            'download_url': download_url,
            'command_line': command_line,
            'executable': executable,
            'process_retry': process_retry,
        }.items():
            if attr_value is not None:
                setattr(self, attr_name, attr_value)


class CoredumpThreadBase(Thread):  # pylint: disable=too-many-instance-attributes
    lookup_period = 30
    upload_retry_limit = 3
    max_coredump_thread_exceptions = 10

    def __init__(self, node: 'BaseNode', max_core_upload_limit: int):
        self.node = node
        self.log = SDCMAdapter(node.log, extra={"prefix": self.__class__.__name__})
        self.max_core_upload_limit = max_core_upload_limit
        self.found: List[CoreDumpInfo] = []
        self.in_progress: List[CoreDumpInfo] = []
        self.completed: List[CoreDumpInfo] = []
        self.uploaded: List[CoreDumpInfo] = []
        self.termination_event = Event()
        self.exception = None
        super().__init__(daemon=True)

    def stop(self):
        self.termination_event.set()

    @raise_event_on_failure
    def run(self):
        """
        Keep reporting new coredumps found, every 30 seconds.
        """
        exceptions_count = 0
        while not self.termination_event.wait(self.lookup_period) or self.in_progress:
            try:
                self.main_cycle_body()
                exceptions_count = 0
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                self.log.error("Following error occurred: %s", exc)
                exceptions_count += 1
                if exceptions_count == self.max_coredump_thread_exceptions:
                    self.exception = exc
                    raise

    def main_cycle_body(self):
        if not self.node.remoter.is_up(timeout=60):
            return
        self._process_coredumps(self.in_progress, self.completed, self.uploaded)
        new_cores = self.extract_info_from_core_pids(self.get_list_of_cores(), exclude_cores=self.found)
        self.push_new_cores_to_process(new_cores)

    def push_new_cores_to_process(self, new_cores: List[CoreDumpInfo]):
        self.found.extend(new_cores)
        for core_dump in new_cores:
            if 'bash' in core_dump.executable:
                continue
            self.log_coredump(core_dump)
            if not self.is_limit_reached():
                self.in_progress.append(core_dump)

    def is_limit_reached(self):
        return len(self.uploaded) >= self.max_core_upload_limit

    def process_coredumps(self):
        self._process_coredumps(self.in_progress, self.completed, self.uploaded)

    def _process_coredumps(
            self,
            in_progress: List[CoreDumpInfo],
            completed: List[CoreDumpInfo],
            uploaded: List[CoreDumpInfo]
    ):
        """
        Get core files from node and report them
        """
        if not in_progress:
            return
        for core_info in in_progress.copy():
            if self.is_limit_reached():
                in_progress.remove(core_info)
                continue
            try:
                core_info.process_retry += 1
                if self.upload_retry_limit < core_info.process_retry:
                    self.log.error(f"Maximum retry uploading is reached for core {str(core_info)}")
                    in_progress.remove(core_info)
                    completed.append(core_info)
                    continue
                self.update_coredump_info_with_more_information(core_info)
                result = self.upload_coredump(core_info)
                completed.append(core_info)
                in_progress.remove(core_info)
                if result:
                    uploaded.append(core_info)
                    self.publish_event(core_info)
            except Exception:  # pylint: disable=broad-except  # noqa: BLE001
                pass

    @abstractmethod
    def get_list_of_cores(self) -> Optional[List[CoreDumpInfo]]:
        ...

    def publish_event(self, core_info: CoreDumpInfo):
        try:
            core_info.publish_event()
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error(f"Failed to publish coredump event due to the: {str(exc)}")

    def extract_info_from_core_pids(
            self, new_cores: Optional[List[CoreDumpInfo]], exclude_cores: List[CoreDumpInfo]) -> List[CoreDumpInfo]:
        output = []
        for new_core_info in new_cores:
            found = False
            for e_core_info in exclude_cores:
                if e_core_info.pid == new_core_info.pid:
                    found = True
                    break
            if found:
                continue
            self.publish_event(new_core_info)
            output.append(new_core_info)
        return output

    # @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on uploading coredump")
    def _upload_coredump(self, core_info: CoreDumpInfo):
        coredump = core_info.corefile
        coredump = self._pack_coredump(coredump)
        coredump_id = os.path.basename(coredump)[:-3]
        upload_url = f'upload.scylladb.com/{coredump_id}/{os.path.basename(coredump)}'
        self.log.info('Uploading coredump %s to %s', coredump, upload_url)
        self.node.remoter.run("sudo curl --request PUT --fail --show-error --upload-file "
                              "'%s' 'https://%s'" % (coredump, upload_url))
        download_url = 'https://storage.cloud.google.com/%s' % upload_url
        self.log.info("You can download it by %s (available for ScyllaDB employee)", download_url)
        download_instructions = f'gsutil cp gs://{upload_url} .'

        coredump = Path(coredump)
        if coredump.suffix == '.zst':
            download_instructions += f'\nunzstd {coredump.name}'
        elif coredump.suffix in ('.gzip', '.gz'):
            download_instructions += f'\ngunzip {coredump.name}'
        elif coredump.suffix == '.lz4':
            download_instructions += f'\nunlz4 {coredump.name}'
        core_info.download_url, core_info.download_instructions = download_url, download_instructions

    @contextmanager
    def hard_link_corefile(self, corefile):  # pylint: disable=unused-argument,no-self-use
        yield

    def upload_coredump(self, core_info: CoreDumpInfo):
        if core_info.download_url:
            return False
        if not core_info.corefile:
            self.log.error(f"{str(core_info)} has inaccessible corefile, can't upload it")
            return False
        try:
            self.log.debug(f'Start uploading file: {core_info.corefile}')
            core_info.download_instructions = 'Coredump upload in progress'
            with self.hard_link_corefile(core_info.corefile) as hard_link:
                if hard_link:
                    core_info.corefile = str(hard_link)
                self._upload_coredump(core_info)
            return True
        except Exception as exc:  # pylint: disable=broad-except
            core_info.download_instructions = 'failed to upload core'
            self.log.error(f"Following error occurred during uploading coredump {core_info.corefile}: {str(exc)}")
            raise

    @cached_property
    def _is_pigz_installed(self):
        if self.node.distro.is_rhel_like:
            return self.node.remoter.run('yum list installed | grep pigz', ignore_status=True).ok
        if self.node.distro.is_ubuntu or self.node.distro.is_debian:
            return self.node.remoter.run('apt list --installed | grep pigz', ignore_status=True).ok
        raise RuntimeError("Distro is not supported")

    def _install_pigz(self):
        if self.node.distro.is_rhel_like:
            self.node.remoter.sudo('yum install -y pigz')
            self.__dict__['is_pigz_installed'] = True
        elif self.node.distro.is_ubuntu or self.node.distro.is_debian:
            self.node.remoter.sudo('apt install -y pigz')
            self.__dict__['is_pigz_installed'] = True
        else:
            raise RuntimeError("Distro is not supported")

    def _pack_coredump(self, coredump: str) -> str:
        extensions = ['.lz4', '.zip', '.gz', '.gzip', '.zst']
        for extension in extensions:
            if coredump.endswith(extension):
                return coredump
        if not self._is_pigz_installed:
            self._install_pigz()
        try:  # pylint: disable=unreachable
            if not self.node.remoter.run(f'sudo ls {coredump}.gz', verbose=False, ignore_status=True).ok:
                self.node.remoter.run(f'sudo pigz --fast --keep {coredump}')
            coredump += '.gz'
        except NETWORK_EXCEPTIONS:  # pylint: disable=try-except-raise
            raise
        except Exception as ex:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.warning("Failed to compress coredump '%s': %s", coredump, ex)
        return coredump

    def log_coredump(self, core_info: CoreDumpInfo):
        if not core_info.coredump_info:
            return
        log_file = os.path.join(self.node.logdir, 'coredump.log')
        with open(log_file, 'a', encoding="utf-8") as log_file_obj:
            log_file_obj.write(core_info.coredump_info)
        for line in core_info.coredump_info.splitlines():
            self.log.error(line)

    @property
    def n_coredumps(self) -> int:
        return len(self.found)

    @abstractmethod
    def update_coredump_info_with_more_information(self, core_info: CoreDumpInfo):
        pass


class CoredumpExportSystemdThread(CoredumpThreadBase):
    """
    Thread that monitor coredumps on the target host, decode them and upload.
    Relay on coredumpctl on the host-side to do all things
    """

    @cached_property
    def systemd_version(self):
        systemd_version = 0
        try:
            systemd_version = get_systemd_version(self.node.remoter.run(
                "systemctl --version", ignore_status=True).stdout)
        except Exception:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.warning("failed to get systemd version:", exc_info=True)
        return systemd_version

    @contextmanager
    def hard_link_corefile(self, corefile):
        hard_links_path = Path(corefile).parent / 'hardlinks'
        link_path = hard_links_path / Path(corefile).name
        self.node.remoter.sudo(f'mkdir -p {hard_links_path}', ignore_status=True)
        self.log.debug(f'doing: ln {corefile} {link_path}')
        self.node.remoter.sudo(f'ln {corefile} {link_path}', ignore_status=True)
        yield link_path
        self.node.remoter.sudo(f'rm -f {link_path}', ignore_status=True)

    def get_list_of_cores_json(self) -> Optional[List[CoreDumpInfo]]:
        result = self.node.remoter.run(
            'sudo coredumpctl -q --json=short', verbose=False, ignore_status=True)
        if not result.ok:
            return []

        # example of json output
        # [{"time":1669548359983740,"pid":11218,"uid":0,"gid":0,"sig":11,
        #   "corefile":"missing","exe":"/usr/bin/bash","size":null},
        # {"time":1669585690901244,"pid":6755,"uid":112,"gid":118,"sig":6,
        #  "corefile":"present","exe":"/opt/scylladb/libexec/scylla","size":61750136832}]
        coredump_infos = []
        try:
            coredump_infos = json.loads(result.stdout)
        except json.JSONDecodeError:
            self.log.warning("couldn't parse:\n %s", result.stdout)

        pid_list = []
        for dump in coredump_infos:
            if "bash" not in dump.get('exe', "") and "fwupd" not in dump.get('exe', ''):
                pid_list.append(CoreDumpInfo(pid=str(dump['pid']), node=self.node))
        return pid_list

    def get_list_of_cores(self) -> Optional[List[CoreDumpInfo]]:
        if self.systemd_version >= 248:
            # since systemd/systemd@0689cfd we have option to get
            # the coredump information in json format
            return self.get_list_of_cores_json()

        result = self.node.remoter.run(
            'sudo coredumpctl --no-pager --no-legend 2>&1', verbose=False, ignore_status=True)
        if "No coredumps found" in result.stdout or not result.ok:
            return []
        pids_list = []
        result = result.stdout + result.stderr
        # Extracting PIDs from coredumpctl output as such:
        #     TIME                            PID   UID   GID SIG COREFILE  EXE
        #     Tue 2020-01-14 16:16:39 +07    3530  1000  1000   3 present   /usr/bin/scylla
        #     Tue 2020-01-14 16:18:56 +07    6321  1000  1000   3 present   /usr/bin/scylla
        #     Tue 2020-01-14 16:31:39 +07   18554  1000  1000   3 present   /usr/bin/scylla
        for line in result.splitlines():
            if "bash" in line or "fwupd" in line:
                # Remove bash core which generated by scylla_setup for internal test purpose.
                # Workaround for https://github.com/scylladb/scylla/issues/6159
                # TIME                            PID   UID   GID SIG PRESENT EXE
                # Sun 2020-04-19 12:27:29 UTC   28987     0     0  11 * /usr/bin/bash
                # fwupd: opened an issue https://github.com/fwupd/fwupd/issues/4432
                continue

            columns = re.split(r'[ ]{2,}', line)
            if len(columns) < 2:
                continue
            pid = columns[1]
            if re.findall(r'[^0-9]', pid):
                self.log.error("PID pattern matched non-numerical value. Looks like coredumpctl changed it's output")
                continue
            pids_list.append(CoreDumpInfo(pid=pid, node=self.node))
        return pids_list

    def update_coredump_info_with_more_information(self, core_info: CoreDumpInfo):
        # pylint: disable=too-many-branches
        coredump_info = self._get_coredumpctl_info(core_info)
        corefile = ''
        executable = ''
        command_line = ''
        event_timestamp = None
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
        for line in coredump_info:
            line = line.strip()  # noqa: PLW2901
            if line.startswith('Executable:'):
                executable = line[12:].strip()
            elif line.startswith('Command Line:'):
                command_line = line[14:].strip()
            elif line.startswith('Coredump:') or line.startswith('Storage:'):
                # Ignore inaccessible cores
                # Storage: /var/lib/systemd/coredump/core.vi.1000.6c4de4c206a0476e88444e5ebaaac482.18554.1578994298000000.lz4 (inaccessible)
                if "inaccessible" in line:
                    continue
                line = line.replace('(present)', '')  # noqa: PLW2901
                corefile = line[line.find(':') + 1:].strip()
            elif line.startswith('Timestamp:'):
                timestring = None
                try:
                    # Converting time string "Tue 2020-01-14 10:40:25 UTC (6min ago)" to timestamp
                    timestring = re.search(r'Timestamp: ([^\(]+)(\([^\)]+\)|)', line).group(1).strip()
                    time_spat = timestring.split()
                    if len(time_spat) == 3:
                        fmt = "%a %Y-%m-%d %H:%M:%S"
                    elif len(time_spat) == 4:
                        timezone = time_spat[3].strip()
                        if re.search(r'^[+-][0-9]{2}$', timezone):
                            # On some systems two digit timezone is not recognized as correct timezone
                            time_spat[3] = f'{timezone}00'
                            timestring = ' '.join(time_spat)
                        if timezone.upper() == "UTC":
                            time_spat[3] = '+00:00'
                            timestring = ' '.join(time_spat)
                        fmt = "%a %Y-%m-%d %H:%M:%S %z"
                    else:
                        raise ValueError(f'Date has unknown format: {timestring}')
                    event_timestamp = datetime.strptime(timestring, fmt).timestamp()
                except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                    self.log.error(f"Failed to convert date '{line}' ({timestring}), due to error: {str(exc)}")
        core_info.update(executable=executable, command_line=command_line, corefile=corefile,
                         source_timestamp=event_timestamp, coredump_info="\n".join(coredump_info)+"\n")

    def _filter_out_modules_info(self, core_info: str) -> List[str]:
        """filters all lines between 'Found module' and 'Stack trace of' lines."""
        lines = core_info.splitlines()
        removed_lines = []
        filtered_lines = []
        skip = False
        for line in lines:
            if 'Found module' in line:
                skip = True
            if 'Stack trace of' in line:
                skip = False
            if not skip:
                filtered_lines.append(line)
            else:
                removed_lines.append(line)
        self.log.debug("Coredump Modules info:\n %s", "\n".join(removed_lines))
        return filtered_lines

    def _get_coredumpctl_info(self, core_info: CoreDumpInfo) -> List[str]:
        """
        Get coredump info with filtered out unnecessary data as list of lines.

        :param core_info: CoreDumpInfo containing PID of the core.
        :return: list[str]
        """
        output = self.node.remoter.run(
            f'sudo coredumpctl info --no-pager --no-legend {core_info.pid}', verbose=False, ignore_status=False)
        return self._filter_out_modules_info(output.stdout + output.stderr)


class CoredumpExportFileThread(CoredumpThreadBase):
    """
    Thread that reads cores from the source directories as binary dumps,
    feed them to coredumpctl to extract core information, packs them and upload

    """
    checkup_time_core_to_complete = 1

    def __init__(self, node: 'BaseNode', max_core_upload_limit: int, coredump_directories: List[str]):
        self.coredumps_directories = coredump_directories
        super().__init__(node=node, max_core_upload_limit=max_core_upload_limit)

    @property
    def _is_file_installed(self):
        return self.node.remoter.run('file', ignore_status=True).exited in [0, 1]

    def _install_file(self):
        if self.node.distro.is_rhel_like:
            self.node.remoter.sudo('yum install -y file')
        elif self.node.distro.is_ubuntu or self.node.distro.is_debian:
            self.node.remoter.sudo('apt install -y file')
        else:
            raise RuntimeError("Distro is not supported")

    @staticmethod
    def _extract_core_info_from_file_name(corefile: str) -> Dict[str, str]:
        data = os.path.splitext(corefile)[0].split('-')
        return {
            'host': data[0],
            'pid': data[1],
            'u': data[2],
            'g': data[3],
            's': data[4],
            'source_timestamp': data[5],
        }

    def get_list_of_cores(self) -> Optional[List[CoreDumpInfo]]:
        output = []
        for directory in self.coredumps_directories:
            for corefile in self.node.remoter.sudo(f'ls {directory}', verbose=False, ignore_status=True).stdout.split():
                self.log.debug(f'Found core file at {corefile}')
                core_data = self._extract_core_info_from_file_name(os.path.basename(corefile))
                output.append(
                    CoreDumpInfo(
                        pid=core_data['pid'],
                        source_timestamp=float(core_data['source_timestamp']),
                        corefile=os.path.join(directory, os.path.basename(corefile)),
                        node=self.node
                    )
                )
        if output:
            if not self._is_file_installed:
                self._install_file()
        return output

    @timeout(timeout=600, message='Wait till core is fully dumped')
    def wait_till_core_is_completed(self, core_info: CoreDumpInfo):
        initial_size = self.node.remoter.run(f'stat -c %s {core_info.corefile}', verbose=False).stdout
        if not initial_size:
            raise RuntimeError(f"Can't get size of the file {core_info.corefile}")
        time.sleep(self.checkup_time_core_to_complete)
        final_size = self.node.remoter.run(f'stat -c %s {core_info.corefile}', verbose=False).stdout
        if not final_size:
            raise RuntimeError(f"Can't get size of the file {core_info.corefile}")
        if initial_size != final_size:
            raise RuntimeError(f"Core is still in the process of dumping {core_info.corefile}")

    def update_coredump_info_with_more_information(self, core_info: CoreDumpInfo):
        self.wait_till_core_is_completed(core_info)
        # /var/lib/scylla/coredumps/45d8a24d50d3-5711-0-0-6-1600105104.core: ELF 64-bit LSB core file, x86-64,
        # version 1 (SYSV), SVR4-style, from '/usr/bin/scylla --log-to-syslog 0 --log-to-stdout 1
        # --default-log-level info --', real uid: 0, effective uid: 0, real gid: 0, effective gid: 0,
        # execfn: '/opt/scylladb/libexec/scylla', platform: 'x86_64'
        for chunk in self.node.remoter.sudo(f'file {core_info.corefile}', retry=3).stdout.split(', '):
            if chunk.startswith("from '"):
                core_info.update(command_line=chunk[6:-1])
