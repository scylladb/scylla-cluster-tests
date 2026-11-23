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
# Copyright (c) 2026 ScyllaDB


"""The `agent` section: the SCT agent, a lightweight service on the test nodes for remote command execution.

YAML form::

    agent:
      enabled: true
      binary_url: "https://example.com/sct-agent"
      tls: true
"""

from pydantic import PositiveInt

from sdcm.sct_config.sections import ConfigSection
from sdcm.sct_config.types import Boolean, SctField, String


class AgentConfig(ConfigSection):
    """Settings of the SCT agent installed on DB, loader and monitor nodes."""

    enabled: Boolean | None = SctField(
        description="Install the SCT agent on the nodes and run commands through its REST API instead of SSH"
    )
    port: PositiveInt | None = SctField(description="Port of the agent's HTTP API")
    binary_url: String = SctField(description="URL the nodes download the agent binary from")
    max_concurrent_jobs: PositiveInt | None = SctField(description="Most jobs one agent runs at the same time")
    log_level: String = SctField(description="Log level of the agent service, e.g. `info` or `debug`")
    tls: Boolean | None = SctField(
        description="Serve the agent's API over TLS, with a certificate signed by the test CA"
    )

    @property
    def installed(self) -> bool:
        """True when the agent is enabled and has a binary to install."""
        return bool(self.enabled and self.binary_url)
