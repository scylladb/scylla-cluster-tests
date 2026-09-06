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

"""Authentication, encryption and credentials configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String


class SecurityConfigMixin(BaseModel):
    """Authentication, encryption and credentials configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Authentication, encryption and credentials"

    user_credentials_path: str = SctField(
        description="""Path to your user credentials. qa key are downloaded automatically from S3 bucket""",
    )
    use_ldap: Boolean = SctField(
        description="When defined true, LDAP is going to be used.",
    )
    use_ldap_authorization: Boolean = SctField(
        description="When defined true, will create a docker container with LDAP and configure scylla.yaml to use it",
    )
    use_ldap_authentication: Boolean = SctField(
        description="When defined true, will create a docker container with LDAP and configure scylla.yaml to use it",
    )
    prepare_saslauthd: Boolean = SctField(
        description="When defined true, will install and start saslauthd service",
    )
    ldap_server_type: String = SctField(
        description="This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]",
    )
    peer_verification: Boolean = SctField(
        description="enable peer verification for encrypted communication",
    )
    client_encrypt_mtls: Boolean = SctField(
        description="when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled",
    )
    server_encrypt_mtls: Boolean = SctField(
        description="when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled",
    )
    keystore_backend: Literal["s3", "secretsmanager"] = SctField(
        description="Credential storage backend for KeyStore: 'secretsmanager' (default) or 's3' (legacy)",
    )
    keystore_sm_prefix: String = SctField(
        description="AWS Secrets Manager secret name prefix when keystore_backend=secretsmanager (default: 'sct/')",
    )
    keystore_sm_region: String = SctField(
        description="AWS region holding the KeyStore secrets when keystore_backend=secretsmanager (default: 'us-east-1')",
    )
