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

"""Scylla installation and configuration."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, DictOrStrOrPydantic, SctField, String, StringOrList


class ScyllaConfigMixin(BaseModel):
    """Scylla installation and configuration.

    Which Scylla to install and how it is configured: repos, versions, distro,
    `scylla.yaml`/command-line options, experimental features, authentication and encryption.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Scylla installation and configuration"

    append_scylla_args: String = SctField(
        description="More arguments to append to scylla command line",
    )
    append_scylla_node_exporter_args: String = SctField(
        description="More arguments to append to scylla-node-exporter command line",
    )
    append_scylla_setup_args: String = SctField(
        description="More arguments to append to scylla_setup command line",
    )
    append_scylla_yaml: DictOrStrOrPydantic = SctField(
        description="More configuration to append to /etc/scylla/scylla.yaml",
    )
    assert_linux_distro_features: StringOrList = SctField(
        description="""List of distro features relevant to SCT test. Example: 'fips'.
            This is used to assert that the distro features are supported by the scylla version being tested.
            If the feature is not supported, the test will fail.""",
        appendable=True,
    )
    authenticator: Literal[
        "PasswordAuthenticator", "AllowAllAuthenticator", "com.scylladb.auth.SaslauthdAuthenticator"
    ] = SctField(
        description="which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator",
    )
    authenticator_password: String = SctField(
        description="the password if PasswordAuthenticator is used",
    )
    authenticator_user: String = SctField(
        description="the username if PasswordAuthenticator is used",
    )
    authorizer: Literal["AllowAllAuthorizer", "CassandraAuthorizer"] = SctField(
        description="which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer",
    )
    client_encrypt: Boolean = SctField(
        description="when enable scylla will use encryption on the client side",
    )
    client_encrypt_mtls: Boolean = SctField(
        description="when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled",
    )
    db_type: String = SctField(
        description="Db type to install into db nodes, scylla/cassandra",
    )
    enable_kms_key_rotation: Boolean = SctField(
        description="Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.",
    )
    endpoint_snitch: String = SctField(
        description="""
            The snitch class scylla would use

            'GossipingPropertyFileSnitch' - default
            'Ec2MultiRegionSnitch' - default on aws backend
            'GoogleCloudSnitch'
         """,
    )
    enterprise_disable_kms: Boolean = SctField(
        description="An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up",
    )
    experimental_features: StringOrList = SctField(
        description="Scylla experimental features to enable in scylla.yaml, as a list of feature names (e.g. 'udf', 'alternator-streams').",
    )
    hinted_handoff: String = SctField(
        description="when enable or disable scylla hinted handoff (enabled/disabled)",
    )
    install_mode: String = SctField(
        description="Scylla install mode, repo/offline/web",
        appendable=False,
    )
    internode_compression: String = SctField(
        description="Scylla `internode_compression` in scylla.yaml: which inter-node traffic to compress -- 'all', 'dc' (between datacenters only) or 'none'.",
    )
    internode_encryption: String = SctField(
        description="Scylla sub option of server_encryption_options: internode_encryption.",
    )
    jmx_heap_memory: int = SctField(
        description="The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).",
    )
    kms_key_rotation_interval: int = SctField(
        description="The time interval in minutes which gets waited before the KMS key rotation happens."
        " Applied when the AWS KMS service is configured to be used.",
    )
    ldap_server_type: String = SctField(
        description="This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]",
    )
    nonroot_offline_install: Boolean = SctField(
        description="Install Scylla without required root privilege",
    )
    peer_verification: Boolean = SctField(
        description="enable peer verification for encrypted communication",
    )
    prepare_saslauthd: Boolean = SctField(
        description="When defined true, will install and start saslauthd service",
    )
    scylla_apt_keys: StringOrList = SctField(
        description="APT keys for ScyllaDB repos",
    )
    scylla_d_overrides_files: StringOrList = SctField(
        description="list of files that should upload to /etc/scylla.d/ directory to override scylla config files",
        appendable=True,
    )
    scylla_encryption_options: String = SctField(
        description="options will be used for enable encryption at-rest for tables",
    )
    scylla_linux_distro: String = SctField(
        description="Distro and family for the DB node image, e.g. 'ubuntu-jammy' or 'debian-bookworm'.",
        appendable=False,
    )
    scylla_linux_distro_loader: String = SctField(
        description="Distro and family for the loader node image. Independent of the DB nodes, so loaders can run a different distro.",
        appendable=False,
    )
    scylla_network_config: list = SctField(
        description="""Configure Scylla networking with single or multiple NIC/IP combinations.
              It must be defined for listen_address and rpc_address. For each address mandatory parameters are:
              - address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication
              - ip_type: ipv4 or ipv6
              - public: false or true
              - nic: number of NIC. 0, 1
              Supported for AWS and GCE meanwhile""",
    )
    scylla_repo: String = SctField(
        description="Url to the repo of scylla version to install scylla. Can provide specific version after a colon "
        "e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`",
    )
    scylla_version: String = SctField(
        description="""Version of scylla to install, ex. '2.3.1'
                       Automatically lookup AMIs and repo links for formal versions.
                       WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'""",
        appendable=False,
    )
    server_encrypt: Boolean = SctField(
        description="when enable scylla will use encryption on the server side",
    )
    server_encrypt_mtls: Boolean = SctField(
        description="when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled",
    )
    service_level_shares: list = SctField(
        description="List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]",
    )
    unified_package: String = SctField(
        description="Url to the unified package of scylla version to install scylla",
    )
    update_db_packages: String = SctField(
        description="""A local directory of rpms to install a custom version on top of
                 the scylla installed (or from repo or from ami)""",
    )
    use_ldap: Boolean = SctField(
        description="When defined true, LDAP is going to be used.",
    )
    use_ldap_authentication: Boolean = SctField(
        description="Authenticate Scylla users against LDAP: starts an LDAP container and sets scylla.yaml to use it for authentication (who you are).",
    )
    use_ldap_authorization: Boolean = SctField(
        description="Authorize Scylla users through LDAP group membership: starts an LDAP container and sets scylla.yaml to use it for authorization (what you may do).",
    )
    use_preinstalled_scylla: Boolean = SctField(
        description="Don't install/update ScyllaDB on DB nodes",
    )
    user_data_format_version: String = SctField(
        description="user-data format version to send to the DB node images. Defaults to whatever the image is tagged with; set it only to override that.",
        appendable=False,
    )
