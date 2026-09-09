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
        description="""
            Kafka Connect connectors to deploy, as a list of connector definitions.

            Each entry has a `source` (where to fetch the connector from -- a Confluent Hub
            coordinate or a release URL), a unique `name`, and a `config` block whose keys are the
            connector's own dotted options, passed through as-is.

            Example -- the Scylla CDC source connector:

                kafka_connectors:
                  - source: 'hub:scylladb/scylla-cdc-source-connector:1.1.2'
                    name: 'cdc-connector'
                    config:
                      connector.class: 'com.scylladb.cdc.debezium.connector.ScyllaConnector'
                      scylla.name: 'test-cluster'
                      scylla.table.names: 'keyspace1.table1'
                      scylla.user: 'cassandra'
                      scylla.password: 'cassandra'

            See `docs/kafka.md` for how SCT deploys Kafka, and the connectors' own documentation
            for the full option set:
            https://github.com/scylladb/scylla-cdc-source-connector#configuration (source
            connector) and
            https://github.com/scylladb/kafka-connect-scylladb/blob/master/documentation/CONFIG.md
            (sink connector). The accepted keys are modelled in
            `sdcm.kafka.kafka_config.ConnectorConfiguration`.
        """,
    )
