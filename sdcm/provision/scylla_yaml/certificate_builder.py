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
# Copyright (c) 2021 ScyllaDB
from functools import cached_property
from pathlib import Path
from typing import Optional, Any

from pydantic import Field, computed_field

from sdcm.provision.helpers.certificate import (
    install_client_certificate, CLIENT_FACING_CERTFILE, CLIENT_FACING_KEYFILE, CA_CERT_FILE,
    SERVER_CERT_FILE, SERVER_KEY_FILE, SCYLLA_SSL_CONF_DIR)
from sdcm.provision.scylla_yaml.auxiliaries import ScyllaYamlAttrBuilderBase, ClientEncryptionOptions, \
    ServerEncryptionOptions


# Disabling no-member since can't import BaseNode from 'sdcm.cluster' due to a circular import

class ScyllaYamlCertificateAttrBuilder(ScyllaYamlAttrBuilderBase):
    """
    Builds scylla yaml attributes regarding encryption
    """
    node: Any = Field(exclude=True)

    @cached_property
    def _ssl_files_path(self) -> Path:
        install_client_certificate(self.node.remoter, self.node.ip_address)
        return SCYLLA_SSL_CONF_DIR

    @computed_field
    @property
    def client_encryption_options(self) -> Optional[ClientEncryptionOptions]:
        if not self.params.get('client_encrypt'):
            return None
        return ClientEncryptionOptions(
            enabled=True,
            certificate=str(self._ssl_files_path / CLIENT_FACING_CERTFILE.name),
            keyfile=str(self._ssl_files_path / CLIENT_FACING_KEYFILE.name),
            truststore=str(self._ssl_files_path / CA_CERT_FILE.name),
            require_client_auth=self.params.get('client_encrypt_mtls')
        )

    @computed_field
    @property
    def server_encryption_options(self) -> Optional[ServerEncryptionOptions]:
        if not self.params.get('internode_encryption') or not self.params.get('server_encrypt'):
            return None
        return ServerEncryptionOptions(
            internode_encryption=self.params.get('internode_encryption'),
            certificate=str(self._ssl_files_path / SERVER_CERT_FILE.name),
            keyfile=str(self._ssl_files_path / SERVER_KEY_FILE.name),
            truststore=str(self._ssl_files_path / CA_CERT_FILE.name),
            require_client_auth=self.params.get('server_encrypt_mtls')
        )
