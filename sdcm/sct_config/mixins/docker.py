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

"""Docker backend configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField, String


class DockerConfigMixin(BaseModel):
    """Docker backend.

    Running the cluster as local Docker containers.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Docker backend"

    docker_image: String = SctField(
        description="Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version",
    )
    docker_network: String = SctField(
        description="Local docker network to use, if there's need to have db cluster connect to other services running in docker",
    )
