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

"""Grow cluster tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField


class GrowClusterConfigMixin(BaseModel):
    """Grow cluster tests configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Grow cluster tests"

    cassandra_stress_population_size: int = SctField(
        description="The total population size over which the Cassandra stress tests are run.",
    )
    cassandra_stress_threads: int = SctField(
        description="The number of threads used by Cassandra stress tests.",
    )
    add_node_cnt: int = SctField(
        description="The number of nodes to add during the test.",
    )
