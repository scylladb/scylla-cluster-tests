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
from textwrap import dedent
from typing import Any

from sdcm.provision.common.builders import AttrBuilder
from sdcm.provision.common.utils import (
    configure_sshd_script,
    restart_sshd_service,
    configure_backoff_timeout,
    update_repo_cache,
    install_syslogng_service,
    configure_syslogng_target_script,
    restart_syslogng_service,
    install_syslogng_exporter,
    disable_daily_apt_triggers,
    configure_syslogng_destination_conf,
    configure_syslogng_file_source,
    install_vector_service,
    configure_vector_target_script,
    install_docker_service,
)
from sdcm.provision.user_data import CLOUD_INIT_SCRIPTS_PATH

SYSLOGNG_SSH_TUNNEL_LOCAL_PORT = 5000
SYSLOGNG_LOG_THROTTLE_PER_SECOND = 10000


class ConfigurationScriptBuilder(AttrBuilder, metaclass=abc.ABCMeta):
    syslog_host_port: tuple[str, int] | None = None
    logs_transport: str = 'vector'
    configure_sshd: bool = True
    hostname: str = ''
    log_file: str = ''
    test_config: Any | None = None
    install_docker: bool = False

    def to_string(self) -> str:
        script = self._start_script()
        script += self._script_body()
        script += self._end_script()
        return script

    @staticmethod
    def _wait_before_running_script() -> str:
        return ''

    @staticmethod
    def _skip_if_already_run_syslogng() -> str:
        """
        If a node was configured before sct-runner, skip syslog-ng installation. Just ensure
        that logging destination is updated in the configuration and the service is
        restarted, to retrigger sending logs.
        """
        return dedent(f"""
        if [ -f {CLOUD_INIT_SCRIPTS_PATH}/done ] && command -v syslog-ng >/dev/null 2>&1; then
            write_syslog_ng_destination
            sudo systemctl restart syslog-ng
            exit 0
        fi
        """)

    @staticmethod
    def _skip_if_already_run_vector() -> str:
        """
        If a node was configured before sct-runner, skip vector installation. Just ensure
        that logging destination is updated in the configuration and the service is
        restarted, to retrigger sending logs.
        """
        return dedent(f"""
        if [ -f {CLOUD_INIT_SCRIPTS_PATH}/done ] && command -v vector >/dev/null 2>&1; then
            sudo systemctl restart vector
            exit 0
        fi
        """)

    @staticmethod
    def _mark_script_as_done() -> str:
        return f"mkdir -p {CLOUD_INIT_SCRIPTS_PATH} && touch {CLOUD_INIT_SCRIPTS_PATH}/done"

    def _start_script(self) -> str:
        script = '#!/bin/bash\n'
        script += 'set -x\n'
        script += self._wait_before_running_script()
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

        script += configure_backoff_timeout()
        if self.logs_transport == 'syslog-ng':
            script += configure_syslogng_destination_conf(
                host=self.syslog_host_port[0],
                port=self.syslog_host_port[1],
                throttle_per_second=SYSLOGNG_LOG_THROTTLE_PER_SECOND)
            script += self._skip_if_already_run_syslogng()
        if self.logs_transport == "vector":
            script += self._skip_if_already_run_vector()
        script += disable_daily_apt_triggers()
        if self.logs_transport == 'syslog-ng':
            script += update_repo_cache()
            script += install_syslogng_service()
            script += configure_syslogng_target_script(hostname=self.hostname)
            if self.log_file:
                script += configure_syslogng_file_source(log_file=self.log_file)
            script += restart_syslogng_service()
            script += install_syslogng_exporter()

        if self.logs_transport == 'vector':
            script += update_repo_cache()
            script += install_vector_service()
            host, port = self.syslog_host_port
            script += configure_vector_target_script(host=host, port=port)

        if self.configure_sshd:
            script += configure_sshd_script()
            script += restart_sshd_service()

        if self.install_docker:
            script += install_docker_service()

        return script
