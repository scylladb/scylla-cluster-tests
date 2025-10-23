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

from sdcm.provision.aws.utils import (
    network_config_ipv6_workaround_script,
    enable_ssm_agent_script,
)
from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder


class AWSConfigurationScriptBuilder(ConfigurationScriptBuilder):
    """
    A class that builds instance initialization script from parameters
    """
    aws_additional_interface: bool = False
    aws_ipv6_workaround: bool = False

    def _wait_before_running_script(self) -> str:
        return 'while ! systemctl status cloud-init.service | grep "active (exited)"; do sleep 1; done\n'

    def _script_body(self) -> str:
        script = enable_ssm_agent_script()
        script += super()._script_body()
        if self.aws_ipv6_workaround:
            script += network_config_ipv6_workaround_script()
        return script
