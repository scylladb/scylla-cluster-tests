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
from typing import Tuple

from sdcm.provision.common.builders import AttrBuilder
from sdcm.provision.common.utils import (
    configure_rsyslog_target_script, configure_sshd_script, restart_sshd_service, restart_rsyslog_service, install_rsyslog)


class ConfigurationScriptBuilder(AttrBuilder, metaclass=abc.ABCMeta):
    rsyslog_host_port: Tuple[str, int] = None

    def to_string(self) -> str:
        script = '#!/bin/bash\n'
        script += configure_sshd_script()
        script += restart_sshd_service()
        if self.rsyslog_host_port:
            script += install_rsyslog()
            script += configure_rsyslog_target_script(host=self.rsyslog_host_port[0], port=self.rsyslog_host_port[1])
            script += restart_rsyslog_service()
        return script
