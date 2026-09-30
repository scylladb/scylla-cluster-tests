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

"""Alternator (DynamoDB API) configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String
from sdcm.utils import alternator


class AlternatorConfigMixin(BaseModel):
    """Alternator (DynamoDB API).

    Scylla's DynamoDB-compatible API: the endpoint, write isolation, load-balancing and the
    credentials the tests use against it.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Alternator (DynamoDB API)"

    alternator_access_key_id: String = SctField(description="the aws_access_key_id that would be used for alternator")
    alternator_enforce_authorization: Boolean = SctField(
        description="If true, enable the authorization check in dynamodb api (alternator)",
    )
    alternator_loadbalancing: Boolean = SctField(
        description="If true, enable native load balancing for alternator",
    )
    alternator_port: int = SctField(
        description="Port to configure for alternator in scylla.yaml",
    )
    alternator_secret_access_key: String = SctField(
        description="the aws_secret_access_key that would be used for alternator",
    )
    alternator_test_table: DictOrStr = SctField(
        description="""Dictionary of a test alternator table features:
                name: str - the name of the table
                lsi_name: str - the name of the local secondary index to create with a table
                gsi_name: str - the name of the global secondary index to create with a table
                tags: dict - the tags to apply to the created table
                items: int - expected number of items in the table after prepare""",
    )
    alternator_trust_all_certificates: Boolean = SctField(
        description="If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)",
    )
    alternator_use_dns_routing: Boolean = SctField(
        description="If true, spawn a docker with a dns server for the ycsb loader to point to",
    )
    alternator_write_isolation: String = SctField(
        description="Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details",
    )
    dynamodb_primarykey_type: Literal[tuple(x.value for x in alternator.enums.YCSBSchemaTypes.__members__.values())] = (
        SctField(
            description="Type of dynamodb table to create with range key or not",
        )
    )
