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
# Copyright (c) 2022 ScyllaDB
from dataclasses import dataclass

from sdcm.provision.common.configuration_script import SYSLOGNG_LOG_THROTTLE_PER_SECOND
from sdcm.provision.common.utils import configure_syslogng_target_script, restart_syslogng_service
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class SyslogNgUserDataObject(SctUserDataObject):
    @property
    def is_applicable(self) -> bool:
        return self.params.get("logs_transport") == "syslog-ng"

    @property
    def packages_to_install(self) -> set[str]:
        return {"syslog-ng"}

    @property
    def script_to_run(self) -> str:
        host, port = self.test_config.get_logging_service_host_port()
        script = configure_syslogng_target_script(
            host=host, port=port, throttle_per_second=SYSLOGNG_LOG_THROTTLE_PER_SECOND, hostname=self.instance_name
        )
        script += restart_syslogng_service()
        return script
