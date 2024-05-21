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
# Copyright (c) 2021 ScyllaDB

import abc

from sdcm.provision.common.builders import AttrBuilder
from sdcm.provision.common.utils import (
    configure_sshd_script,
    restart_sshd_service,
    install_syslogng_service,
    configure_syslogng_target_script,
    restart_syslogng_service,
    configure_ssh_accept_rsa,
    install_syslogng_exporter,
)
from sdcm.provision.user_data import CLOUD_INIT_SCRIPTS_PATH

SYSLOGNG_SSH_TUNNEL_LOCAL_PORT = 5000
SYSLOGNG_LOG_THROTTLE_PER_SECOND = 10000


class ConfigurationScriptBuilder(AttrBuilder, metaclass=abc.ABCMeta):
    syslog_host_port: tuple[str, int] = None
    logs_transport: str = 'syslog-ng'
    configure_sshd: bool = True
    hostname: str = ''

    def to_string(self) -> str:
        script = self._start_script()
        script += self._script_body()
        script += self._end_script()
        return script

    @staticmethod
    def _wait_before_running_script() -> str:
        return ''

    @staticmethod
    def _skip_if_already_run() -> str:
        """syslog-ng requires restart to retrigger sending logs in case it was configured before sct-runner"""
        return f'if [ -f {CLOUD_INIT_SCRIPTS_PATH}/done ]; then sudo systemctl restart syslog-ng; exit 0; fi\n'

    @staticmethod
    def _mark_script_as_done() -> str:
        return f"mkdir -p {CLOUD_INIT_SCRIPTS_PATH} && touch {CLOUD_INIT_SCRIPTS_PATH}/done"

    def _start_script(self) -> str:
        script = '#!/bin/bash\n'
        script += 'set -x\n'
        script += self._wait_before_running_script()
        script += self._skip_if_already_run()
        return script

    def _end_script(self) -> str:
        script = ""
        script += self._mark_script_as_done()
        return script

    def _script_body(self) -> str:
        # Whenever you change it please keep in mind that:
        # 1. scylla image is running it with -e key, which means it will stop on very first error
        # 2. scylla image is running it with -B key, which means you can use "{1..3}", you need to do "1 2 3"
        # 3. scylla image is running it in such mode that "echo -e" is not working
        # 4. There is race condition between sct and boot script, disable ssh to mitigate it
        # 5. Make sure that whenever you use "cat <<EOF >>/file", make sure that EOF has no spaces in front of it
        script = ''
        if self.logs_transport == 'syslog-ng':
            script += install_syslogng_service()
            script += configure_syslogng_target_script(
                host=self.syslog_host_port[0],
                port=self.syslog_host_port[1],
                throttle_per_second=SYSLOGNG_LOG_THROTTLE_PER_SECOND,
                hostname=self.hostname,
            )
            script += restart_syslogng_service()
            script += install_syslogng_exporter()

        if self.configure_sshd:
            script += configure_sshd_script()
            script += configure_ssh_accept_rsa()
            script += restart_sshd_service()

        return script
