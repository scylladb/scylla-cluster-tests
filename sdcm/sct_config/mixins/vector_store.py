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

"""Vector Store configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField, String
from sdcm.utils.vector_store_utils import DEFAULT_VECTOR_STORE_SOURCE_REF, DEFAULT_VECTOR_STORE_SOURCE_REPO


class VectorStoreConfigMixin(BaseModel):
    """Vector Store.

    The Vector Store service under test alongside Scylla.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Vector Store"

    n_vector_store_nodes: int = SctField(
        description="Number of vector store nodes (0 = VS is disabled)",
    )
    vector_store_docker_image: String = SctField(
        description="Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version",
    )
    vector_store_port: int = SctField(
        description="TCP port the Vector Store service listens on for its API.",
    )
    vector_store_scylla_port: int = SctField(
        description="ScyllaDB connection port for Vector Store",
    )
    vector_store_source_build_timeout: int = SctField(
        description="Max seconds to wait for vector-store source build to complete",
    )
    vector_store_source_ref: String = SctField(
        description=f"""Git ref (branch, tag or full 40-character commit SHA) to build vector-store from source on the VS node.
        Setting this (or 'vector_store_source_repo') switches provisioning from a prebuilt AMI to a
        source build over the base AMI, and is mutually exclusive with 'vector_store_version'.
        If omitted while 'vector_store_source_repo' is set, defaults to '{DEFAULT_VECTOR_STORE_SOURCE_REF}'.""",
    )
    vector_store_source_repo: String = SctField(
        description=f"""Git repository URL to build vector-store from source on the VS node.
        Setting this (or 'vector_store_source_ref') switches provisioning from a prebuilt AMI to a
        source build over the base AMI, and is mutually exclusive with 'vector_store_version'.
        If omitted while 'vector_store_source_ref' is set, defaults to {DEFAULT_VECTOR_STORE_SOURCE_REPO}.
        Source builds are supported on the aws backend only.""",
    )
    vector_store_threads: int = SctField(
        description="Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)",
    )
    vector_store_scylla_username: String = SctField(
        description="Username for Vector Store to authenticate with ScyllaDB. "
        "When set, SCT creates this role and a service level on the ScyllaDB cluster, and attaches "
        "the service level to the role. With CassandraAuthorizer, SCT also grants VECTOR_SEARCH_INDEXING "
        "on all keyspaces to the role. Requires PasswordAuthenticator and the superuser in "
        "append_scylla_yaml (auth_superuser_name, auth_superuser_salted_password). "
        "configurations/auth_cassandra.yaml sets all of them.",
    )
    vector_store_scylla_password: String = SctField(
        description="Password for the Vector Store ScyllaDB user",
    )
    vector_store_service_level_shares: int = SctField(
        description="Shares for the Vector Store service level (default: 1000)",
    )
    vector_store_version: String = SctField(
        description="Vector Store version / docker image tag",
    )
