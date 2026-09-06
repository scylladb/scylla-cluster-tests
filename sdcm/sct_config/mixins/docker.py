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

from sdcm.sct_config.types import Boolean, SctField, String


class DockerConfigMixin(BaseModel):
    """Docker backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Docker backend"

    mgmt_docker_image: String = SctField(
        description="Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'",
    )
    docker_image: String = SctField(
        description="Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version",
    )
    docker_network: String = SctField(
        description="Local docker network to use, if there's need to have db cluster connect to other services running in docker",
    )
    vector_store_docker_image: String = SctField(
        description="Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version",
    )
    vector_store_version: String = SctField(
        description="Vector Store version / docker image tag",
    )
    docker_image_cassandra: String = SctField(
        description="Cassandra docker image repo, i.e. 'cassandra'. Used when db_type is 'cassandra'.",
    )
    cassandra_version: String = SctField(
        description="Cassandra version / docker image tag, i.e. '4.1' or '5.0'",
    )
    cassandra_num_tokens: int = SctField(
        description="num_tokens value to configure in cassandra.yaml.",
    )
    cassandra_oracle_version: String = SctField(
        description="Cassandra version for the oracle cluster, i.e. '4.1' or '5.0'",
    )
    install_cassandra_exporter: Boolean = SctField(
        description="Install Criteo cassandra_exporter on Cassandra nodes for Prometheus metrics collection. "
        "The exporter connects to JMX (port 7199) and exposes metrics on port 8080.",
    )
    cassandra_broadcast_rpc_public: Boolean = SctField(
        description="When True, set broadcast_rpc_address to the public IP of the node in cassandra.yaml, "
        "so clients outside the VPC (e.g. sct-runner driver connection that reads system.peers) "
        "can reach the nodes. Defaults to False (private IP, matches intra-VPC behavior).",
    )
