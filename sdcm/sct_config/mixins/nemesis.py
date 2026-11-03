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

"""Nemesis (chaos testing) configuration options."""

from typing import Annotated, ClassVar

from pydantic import BaseModel, BeforeValidator, Field

from sdcm.sct_config.sections.nemesis import NemesisConfig
from sdcm.sct_config.types import yaml_from_str


class NemesisConfigMixin(BaseModel):
    """Nemesis (chaos testing).

    Which disruptions run, how often, and how targets are selected. Every option lives in the
    `nemesis` section; see ``sdcm.sct_config.sections.nemesis``.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Nemesis (chaos testing)"

    nemesis: Annotated[NemesisConfig, BeforeValidator(yaml_from_str)] = Field(
        default_factory=NemesisConfig,
        description="""
                Nemesis section: settings shared by all runners, defaults for options a runner leaves
                unset, and `runners`, the runners to start, one thread each. A runner entry is a
                single-key mapping of a runner class to its options, e.g.
                `{SisyphusMonkey: {selector: disruptive, interval: 15}}`; the options accepted depend
                on the runner class. A runner without `seed`, own or default, gets a random one.
        """,
    )
