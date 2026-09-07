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

"""Auxiliary DB cluster (oracle / Cassandra) configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, IntOrList, SctField, String


class AuxDbConfigMixin(BaseModel):
    """Auxiliary DB cluster (oracle / Cassandra).

    A second database cluster used for comparison or migration testing -- the 'oracle' cluster in
    Gemini runs, or a Cassandra cluster in migration tests. Named for the role, not for Gemini,
    since other test types use it too.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Auxiliary DB cluster (oracle / Cassandra)"

    append_scylla_args_oracle: String = SctField(
        description="More arguments to append to oracle command line",
    )
    cassandra_broadcast_rpc_public: Boolean = SctField(
        description="When True, set broadcast_rpc_address to the public IP of the node in cassandra.yaml, "
        "so clients outside the VPC (e.g. sct-runner driver connection that reads system.peers) "
        "can reach the nodes. Defaults to False (private IP, matches intra-VPC behavior).",
    )
    cassandra_num_tokens: int = SctField(
        description="num_tokens value to configure in cassandra.yaml.",
    )
    cassandra_oracle_version: String = SctField(
        description="Cassandra version for the oracle cluster, i.e. '4.1' or '5.0'",
    )
    cassandra_version: String = SctField(
        description="Cassandra version / docker image tag, i.e. '4.1' or '5.0'",
    )
    docker_image_cassandra: String = SctField(
        description="Cassandra docker image repo, i.e. 'cassandra'. Used when db_type is 'cassandra'.",
    )
    install_cassandra_exporter: Boolean = SctField(
        description="Install Criteo cassandra_exporter on Cassandra nodes for Prometheus metrics collection. "
        "The exporter connects to JMX (port 7199) and exposes metrics on port 8080.",
    )
    n_test_oracle_db_nodes: IntOrList = SctField(
        description="Number list of oracle test nodes in multiple data centers.",
    )
    oracle_scylla_version: String = SctField(
        description="""Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'
                 Automatically looks up cloud images for formal versions.
                 WARNING: can't be used together with the backend's oracle image param
                 ('ami_id_db_oracle', 'gce_image_db_oracle', 'azure_image_db_oracle' or 'oci_image_db_oracle')""",
        appendable=False,
    )
    oracle_user_data_format_version: String = SctField(
        description="Same as 'user_data_format_version', but for the auxiliary oracle cluster's images.",
        appendable=False,
    )
