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

"""Vector Store configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField


class VectorStoreConfigMixin(BaseModel):
    """Vector Store configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Vector Store"

    n_vector_store_nodes: int = SctField(
        description="Number of vector store nodes (0 = VS is disabled)",
    )
    vector_store_port: int = SctField(
        description="Vector Store API port",
    )
    vector_store_scylla_port: int = SctField(
        description="ScyllaDB connection port for Vector Store",
    )
    vector_store_threads: int = SctField(
        description="Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)",
    )
