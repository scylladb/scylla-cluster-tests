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
# Copyright (c) 2025 ScyllaDB

import logging
from dataclasses import dataclass

from sdcm.sct_provision.user_data_objects import SctUserDataObject
from sdcm.test_config import TestConfig
from sdcm.utils.sct_agent_installer import install_agent_script


logger = logging.getLogger(__name__)


@dataclass
class SctAgentUserDataObject(SctUserDataObject):

    @property
    def is_applicable(self) -> bool:
        agent_config = self.params.get('agent')
        return agent_config.get('enabled') and 'db' in self.node_type and agent_config.get('binary_url')

    @property
    def packages_to_install(self) -> set[str]:
        return {'curl', 'systemd'}

    @property
    def script_to_run(self) -> str:
        """
        Generate installation script for cloud-init.

        The script performs:
            - downloads agent binary from URL
            - creates configuration file with generated API key
            - sets up systemd service
            - starts agent and verifies it's running
        """
        agent_config = self.params.get('agent')
        api_key = TestConfig.agent_api_key()
        if not api_key:
            raise ValueError("Agent API key not available. Ensure it is generated before provisioning.")

        return install_agent_script(
            agent_binary_url=agent_config['binary_url'],
            api_keys=[api_key],
            port=agent_config['port'],
            max_concurrent_jobs=agent_config['max_concurrent_jobs'],
            log_level=agent_config.get('log_level'))
