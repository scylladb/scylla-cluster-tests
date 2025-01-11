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
from typing import Optional, Any, List

from pydantic import Field, computed_field

from sdcm.provision.scylla_yaml.auxiliaries import ScyllaYamlAttrBuilderBase
from sdcm.utils.ldap import LDAP_SSH_TUNNEL_LOCAL_PORT, LdapServerType


class ScyllaYamlClusterAttrBuilder(ScyllaYamlAttrBuilderBase):
    """
    Builds scylla yaml attributes that stays persistent across the cluster
    """
    cluster_name: str
    test_config: Any = Field(exclude=True)
    msldap_server_info: dict = Field(exclude=True, default=None)

    @computed_field
    @property
    def hinted_handoff_enabled(self) -> Optional[str]:
        param_hinted_handoff = str(self.params.get('hinted_handoff')).lower()
        if param_hinted_handoff in ('enabled', 'true', '1'):
            return True
        if param_hinted_handoff in ('disabled', 'false', '0'):
            return False
        return None

    @computed_field
    @property
    def experimental_features(self) -> List[str]:
        features = self.params.get('experimental_features')
        if features is None:
            return []
        return features

    @computed_field
    @property
    def authenticator(self) -> Optional[str]:
        if self._is_authenticator_valid:
            return self._authenticator
        return None

    @computed_field
    @property
    def saslauthd_socket_path(self) -> Optional[str]:
        if self._is_authenticator_valid and self.params.get('prepare_saslauthd'):
            return '/run/saslauthd/mux'
        return None

    @computed_field
    @property
    def authorizer(self) -> Optional[str]:
        if self._authorizer in ['AllowAllAuthorizer', 'CassandraAuthorizer']:
            return self._authorizer
        return None

    @computed_field
    @property
    def alternator_port(self) -> Optional[str]:
        return self.params.get('alternator_port')

    @computed_field
    @property
    def alternator_write_isolation(self) -> Optional[str]:
        return "always_use_lwt" if self.params.get('alternator_port') else None

    @computed_field
    @property
    def alternator_enforce_authorization(self) -> bool:
        return bool(self.params.get('alternator_enforce_authorization'))

    @computed_field
    @property
    def internode_compression(self) -> Optional[str]:
        return self.params.get('internode_compression')

    @computed_field
    @property
    def endpoint_snitch(self) -> Optional[str]:
        """
        Comes from get_endpoint_snitch
        """
        if snitch := self.params.get('endpoint_snitch'):
            return snitch
        if self._multi_region:
            return self._default_endpoint_snitch
        return None

    @computed_field
    @property
    def ldap_attr_role(self) -> Optional[str]:
        return 'cn' if self._is_ldap_authorization else None

    @computed_field
    @property
    def ldap_bind_dn(self) -> Optional[str]:
        if self._is_msldap_authorization:
            return self._ms_ldap_bind_dn
        if self._is_openldap_authorization:
            return self._open_ldap_bind_dn
        return None

    @computed_field
    @property
    def role_manager(self) -> Optional[str]:
        return 'com.scylladb.auth.LDAPRoleManager' if self._is_ldap_authorization else None

    @computed_field
    @property
    def ldap_bind_passwd(self) -> Optional[str]:
        if self._is_msldap_authorization:
            return self._ms_ldap_bind_passwd
        if self._is_openldap_authorization:
            return self._open_ldap_bind_passwd
        return None

    @computed_field
    @property
    def ldap_url_template(self) -> Optional[str]:
        if self._is_msldap_authorization:
            server_port = self._ms_ldap_server_address_port
            ldap_filter = 'member=CN={USER}'
        elif self._is_openldap_authorization:
            server_port = self._open_ldap_server_address_port
            ldap_filter = 'uniqueMember=uid={USER},ou=Person'
        else:
            return None
        return f'ldap://{server_port}/{self._ldap_base_dn}?cn?sub?({ldap_filter},{self._ldap_base_dn})'

    @property
    def _is_msldap_authorization(self):
        return self._is_ldap_authorization and self.params.get('ldap_server_type') == LdapServerType.MS_AD

    @property
    def _is_openldap_authorization(self):
        return self._is_ldap_authorization and not self.params.get('ldap_server_type') == LdapServerType.MS_AD

    @property
    def _is_ldap_authorization(self):
        return self.params.get('use_ldap_authorization')

    @property
    def _ldap_base_dn(self) -> str:
        return 'dc=scylla-qa,dc=com'

    @property
    def _ms_ldap_base_dn(self) -> str:
        return 'DC=scylla-qa,DC=com'

    @property
    def _ms_ldap_bind_dn(self) -> str:
        return self._msldap_server_info['ldap_bind_dn']  # pylint: disable=unsubscriptable-object

    @property
    def _ms_ldap_bind_passwd(self) -> str:
        return self._msldap_server_info['admin_password']  # pylint: disable=unsubscriptable-object

    @property
    def _ms_ldap_server_address_port(self) -> str:
        return f'{self._msldap_server_info["server_address"]}:389'  # pylint: disable=unsubscriptable-object

    @property
    def _open_ldap_bind_dn(self) -> str:
        return f'cn=admin,{self._ldap_base_dn}'

    @property
    def _open_ldap_bind_passwd(self) -> str:
        return 'scylla-0'

    @property
    def _open_ldap_server_address_port(self) -> str:
        if self.test_config.IP_SSH_CONNECTIONS == 'public':
            # When connection goes public we run ssh tunnel on db-nodes side to access openldap server
            # that is why we pass address it in scylla config as '127.0.0.1:{LDAP_SSH_TUNNEL_LOCAL_PORT}'
            return '127.0.0.1:' + str(LDAP_SSH_TUNNEL_LOCAL_PORT)
        return self._openldap_server_address_port

    @cached_property
    def _openldap_server_address_port(self) -> str:
        ldap_address = self.test_config.LDAP_ADDRESS   # pylint: disable=no-member
        if not ldap_address or not ldap_address[0] or not ldap_address[1]:
            raise RuntimeError("OPENLDAP has not been started")
        return str(ldap_address[0]) + ':' + str(ldap_address[1])

    @cached_property
    def _msldap_server_info(self) -> dict:
        if not self.msldap_server_info:
            raise RuntimeError('MSLDAP is configured, but not `msldap_server_info` is provided')
        msldap_server_info_keys = set(self.msldap_server_info.keys())  # pylint: disable=no-member
        if missing_keys := {'ldap_bind_dn', 'admin_password', 'server_address'} - msldap_server_info_keys:
            raise RuntimeError("MSLDAP is configured, but `msldap_server_info` lack of following keys: "
                               f"{','.join(missing_keys)}")
        return self.msldap_server_info
