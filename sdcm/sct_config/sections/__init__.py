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

"""Section models: groups of options nested under one key of the SCT configuration.

A mixin in ``CONFIG_GROUPS`` declares a section as one of its fields (``agent: AgentConfig``), and
the section holds its options under prefix-free names (``config.agent.port``). Sections
are read and written through attributes only. Every layer (defaults, config files, env) merges
into a section field by field; see ``sdcm.sct_config.helpers.merge_dicts_append_strings``.
"""

from pydantic import BaseModel, ConfigDict


class ConfigSection(BaseModel):
    """Base of every section: unknown keys fail, and assignments are validated like root assignments."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
