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

"""Kafka / CDC connectors configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.kafka.kafka_config import SctKafkaConfiguration
from sdcm.sct_config.types import SctField


class KafkaConfigMixin(BaseModel):
    """Kafka / CDC connectors.

    Kafka deployment and connector configuration for CDC testing.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Kafka / CDC connectors"

    kafka_backend: Literal["localstack", "vm", "msk"] | None = SctField(
        description="Type of Kafka backend to use",
    )
    kafka_connectors: list[SctKafkaConfiguration] = SctField(
        description="Kafka Connect connector definitions to deploy, as a list of config dicts -- typically the Scylla CDC source connector.",
    )
