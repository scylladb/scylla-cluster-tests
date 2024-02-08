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
import os
from functools import cached_property
from typing import Any

from pydantic import Field

from sdcm.provision.helpers.certificate import CLIENT_CERTFILE, CLIENT_KEYFILE, CLIENT_TRUSTSTORE, install_client_certificate
from sdcm.provision.scylla_yaml.auxiliaries import (
    ClientEncryptionOptions,
    ScyllaYamlAttrBuilderBase,
    ServerEncryptionOptions,
)

# Disabling no-member since can't import BaseNode from 'sdcm.cluster' due to a circular import


class ScyllaYamlCertificateAttrBuilder(ScyllaYamlAttrBuilderBase):
    """
    Builds scylla yaml attributes regarding encryption
    """
    node: Any = Field(as_dict=False)

    @cached_property
    def _ssl_files_path(self) -> str:
        install_client_certificate(self.node.remoter)
        return '/etc/scylla/ssl_conf'

    @property
    def client_encryption_options(self) -> ClientEncryptionOptions | None:
        if not self.params.get('client_encrypt'):
            return None
        return ClientEncryptionOptions(
            enabled=True,
            certificate=os.path.join(self._ssl_files_path, 'client', os.path.basename(CLIENT_CERTFILE)),
            keyfile=os.path.join(self._ssl_files_path, 'client', os.path.basename(CLIENT_KEYFILE)),
            truststore=os.path.join(self._ssl_files_path, 'client', os.path.basename(CLIENT_TRUSTSTORE)),
        )

    @property
    def server_encryption_options(self) -> ServerEncryptionOptions | None:
        if not self.params.get('internode_encryption') or not self.params.get('server_encrypt'):
            return None
        return ServerEncryptionOptions(
            internode_encryption=self.params.get('internode_encryption'),
            certificate=self._ssl_files_path + '/db.crt',
            keyfile=self._ssl_files_path + '/db.key',
            truststore=self._ssl_files_path + '/cadb.pem',
        )
