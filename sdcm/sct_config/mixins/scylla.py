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

"""Scylla installation and configuration configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, DictOrStr, DictOrStrOrPydantic, SctField, String, StringOrList
from sdcm.utils import alternator


class ScyllaConfigMixin(BaseModel):
    """Scylla installation and configuration configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Scylla installation and configuration"

    force_run_iotune: Boolean = SctField(
        description="Force running iotune on the DB nodes, regardless if image has predefined values",
    )
    db_type: String = SctField(
        description="Db type to install into db nodes, scylla/cassandra",
    )
    endpoint_snitch: String = SctField(
        description="""
            The snitch class scylla would use

            'GossipingPropertyFileSnitch' - default
            'Ec2MultiRegionSnitch' - default on aws backend
            'GoogleCloudSnitch'
         """,
    )
    scylla_repo: String = SctField(
        description="Url to the repo of scylla version to install scylla. Can provide specific version after a colon "
        "e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`",
    )
    scylla_apt_keys: StringOrList = SctField(
        description="APT keys for ScyllaDB repos",
    )
    unified_package: String = SctField(
        description="Url to the unified package of scylla version to install scylla",
    )
    nonroot_offline_install: Boolean = SctField(
        description="Install Scylla without required root privilege",
    )
    install_mode: String = SctField(
        description="Scylla install mode, repo/offline/web",
        appendable=False,
    )
    scylla_version: String = SctField(
        description="""Version of scylla to install, ex. '2.3.1'
                       Automatically lookup AMIs and repo links for formal versions.
                       WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'""",
        appendable=False,
    )
    user_data_format_version: String = SctField(
        description="""Format version of the user-data to use for scylla images,
                       default to what tagged on the image used""",
        appendable=False,
    )
    oracle_user_data_format_version: String = SctField(
        description="""Format version of the user-data to use for scylla images,
                       default to what tagged on the image used""",
        appendable=False,
    )
    oracle_scylla_version: String = SctField(
        description="""Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'
                 Automatically looks up cloud images for formal versions.
                 WARNING: can't be used together with the backend's oracle image param
                 ('ami_id_db_oracle', 'gce_image_db_oracle', 'azure_image_db_oracle' or 'oci_image_db_oracle')""",
        appendable=False,
    )
    scylla_linux_distro: String = SctField(
        description="""The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.""",
        appendable=False,
    )
    scylla_linux_distro_loader: String = SctField(
        description="""The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.""",
        appendable=False,
    )
    assert_linux_distro_features: StringOrList = SctField(
        description="""List of distro features relevant to SCT test. Example: 'fips'.
            This is used to assert that the distro features are supported by the scylla version being tested.
            If the feature is not supported, the test will fail.""",
        appendable=True,
    )
    scylla_repo_m: String = SctField(
        description="Url to the repo of scylla version to install scylla from for management tests",
    )
    update_db_packages: String = SctField(
        description="""A local directory of rpms to install a custom version on top of
                 the scylla installed (or from repo or from ami)""",
    )
    experimental_features: StringOrList = SctField(
        description="unlock specified experimental features",
    )
    server_encrypt: Boolean = SctField(
        description="when enable scylla will use encryption on the server side",
    )
    client_encrypt: Boolean = SctField(
        description="when enable scylla will use encryption on the client side",
    )
    hinted_handoff: String = SctField(
        description="when enable or disable scylla hinted handoff (enabled/disabled)",
    )
    nemesis_double_load_during_grow_shrink_duration: int = SctField(
        description="After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.",
    )
    authenticator: Literal[
        "PasswordAuthenticator", "AllowAllAuthenticator", "com.scylladb.auth.SaslauthdAuthenticator"
    ] = SctField(
        description="which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator",
    )
    authenticator_user: String = SctField(
        description="the username if PasswordAuthenticator is used",
    )
    authenticator_password: String = SctField(
        description="the password if PasswordAuthenticator is used",
    )
    authorizer: Literal["AllowAllAuthorizer", "CassandraAuthorizer"] = SctField(
        description="which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer",
    )
    # Temporary solution. We do not want to run SLA nemeses during not-SLA test until the feature is stable
    sla: Boolean = SctField(
        description="run SLA nemeses if the test is SLA only",
    )
    service_level_shares: list = SctField(
        description="List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]",
    )
    alternator_port: int = SctField(
        description="Port to configure for alternator in scylla.yaml",
    )
    dynamodb_primarykey_type: Literal[tuple(x.value for x in alternator.enums.YCSBSchemaTypes.__members__.values())] = (
        SctField(
            description="Type of dynamodb table to create with range key or not",
        )
    )
    alternator_write_isolation: String = SctField(
        description="Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details",
    )
    alternator_use_dns_routing: Boolean = SctField(
        description="If true, spawn a docker with a dns server for the ycsb loader to point to",
    )
    alternator_loadbalancing: Boolean = SctField(
        description="If true, enable native load balancing for alternator",
    )
    alternator_test_table: DictOrStr = SctField(
        description="""Dictionary of a test alternator table features:
                name: str - the name of the table
                lsi_name: str - the name of the local secondary index to create with a table
                gsi_name: str - the name of the global secondary index to create with a table
                tags: dict - the tags to apply to the created table
                items: int - expected number of items in the table after prepare""",
    )
    alternator_enforce_authorization: Boolean = SctField(
        description="If true, enable the authorization check in dynamodb api (alternator)",
    )
    alternator_access_key_id: String = SctField(description="the aws_access_key_id that would be used for alternator")
    alternator_secret_access_key: String = SctField(
        description="the aws_secret_access_key that would be used for alternator",
    )
    alternator_trust_all_certificates: Boolean = SctField(
        description="If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)",
    )
    region_aware_loader: Boolean = SctField(
        description="When in multi region mode, run stress on loader that is located in the same region as db node",
    )
    append_scylla_args: String = SctField(
        description="More arguments to append to scylla command line",
    )
    append_scylla_args_oracle: String = SctField(
        description="More arguments to append to oracle command line",
    )
    append_scylla_yaml: DictOrStrOrPydantic = SctField(
        description="More configuration to append to /etc/scylla/scylla.yaml",
    )
    append_scylla_node_exporter_args: String = SctField(
        description="More arguments to append to scylla-node-exporter command line",
    )
