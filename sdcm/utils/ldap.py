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

from time import sleep
from enum import Enum

from cassandra import Unauthorized
from ldap3 import Server, Connection, ALL, ALL_ATTRIBUTES
from ldap3.core.exceptions import LDAPSocketOpenError

from sdcm.keystore import KeyStore
from sdcm.utils.decorators import retrying


LDAP_IMAGE = "osixia/openldap:1.4.0"
LDAP_PORT = 389
LDAP_SSL_PORT = 636
LDAP_SSH_TUNNEL_LOCAL_PORT = 5001
LDAP_SSH_TUNNEL_SSL_PORT = 5002
ORGANISATION = 'ScyllaDB'
LDAP_DOMAIN = 'scylla-qa.com'
# Ths suffix is added to default cassandra password to make it stronger, otherwise MS-AD doesn't allow it
DEFAULT_PWD_SUFFIX = '-0'
LDAP_PASSWORD = 'scylla-0'
LDAP_ROLE = 'scylla_ldap'
LDAP_USERS = ['scylla-qa', 'dummy-user']
LDAP_BASE_OBJECT = (lambda l: ','.join([f'dc={part}' for part in l.split('.')]))(LDAP_DOMAIN)  # noqa: PLC3002
SASLAUTHD_AUTHENTICATOR = 'com.scylladb.auth.SaslauthdAuthenticator'


class LdapServerNotReady(Exception):
    pass


class LdapConfigurationError(Exception):
    pass


class LdapServerType(str, Enum):
    MS_AD = "ms_ad"
    OPENLDAP = "openldap"


class LdapContainerMixin:
    ldap_server = None
    ldap_conn = None
    ldap_server_port = None
    ldap_server_ssl_port = None

    def ldap_container_run_args(self) -> dict:
        return dict(image=LDAP_IMAGE,
                    name=f"{self.name}-ldap-server",
                    detach=True,
                    ports={f"{LDAP_PORT}/tcp": None, f"{LDAP_SSL_PORT}/tcp": None, },
                    environment=[f'LDAP_ORGANISATION={ORGANISATION}', f'LDAP_DOMAIN={LDAP_DOMAIN}',
                                 f'LDAP_ADMIN_PASSWORD={LDAP_PASSWORD}'],
                    )

    @staticmethod
    @retrying(n=10, sleep_time=6, allowed_exceptions=(LdapServerNotReady, LDAPSocketOpenError))
    def create_ldap_connection(ip, ldap_port, user, password):
        LdapContainerMixin.ldap_server = Server(host=f'ldap://{ip}:{ldap_port}', get_info=ALL)
        LdapContainerMixin.ldap_conn = Connection(server=LdapContainerMixin.ldap_server, user=user, password=password)
        sleep(5)
        LdapContainerMixin.ldap_conn.open()
        LdapContainerMixin.ldap_conn.bind()

    @staticmethod
    def is_ldap_connection_bound():
        return LdapContainerMixin.ldap_conn.bound if LdapContainerMixin.ldap_conn else False

    @staticmethod
    def add_ldap_entry(ip, ldap_port, user, password, ldap_entry):
        if not LdapContainerMixin.is_ldap_connection_bound():
            LdapContainerMixin.create_ldap_connection(ip, ldap_port, user, password)
        LdapContainerMixin.ldap_conn.add(*ldap_entry)

    @staticmethod
    def search_ldap_entry(search_base, search_filter):
        LdapContainerMixin.ldap_conn.search(search_base=search_base, search_filter=search_filter,
                                            attributes=ALL_ATTRIBUTES)
        return LdapContainerMixin.ldap_conn.entries

    @staticmethod
    def modify_ldap_entry(*args, **kwargs):
        return LdapContainerMixin.ldap_conn.modify(*args, **kwargs)

    @staticmethod
    def delete_ldap_entry(*args, **kwargs):
        return LdapContainerMixin.ldap_conn.delete(*args, **kwargs)


class LdapUtilsMixin:
    """This mixin can be added to any class that inherits the 'ClusterTester' one and configures Ldap container"""

    @property
    def ldap_server_ip(self) -> str:
        if self.params.get('ldap_server_type') == LdapServerType.MS_AD:
            ldap_ms_ad_credentials = KeyStore().get_ldap_ms_ad_credentials()
            return ldap_ms_ad_credentials["server_address"]
        ldap_server_ip = '127.0.0.1' if self.test_config.IP_SSH_CONNECTIONS == 'public' \
            else self.test_config.LDAP_ADDRESS[0]
        return ldap_server_ip

    @property
    def ldap_port(self) -> str:
        if self.params.get('ldap_server_type') == LdapServerType.MS_AD:
            return LDAP_PORT
        ldap_port = LDAP_SSH_TUNNEL_LOCAL_PORT if self.test_config.IP_SSH_CONNECTIONS == 'public' \
            else self.test_config.LDAP_ADDRESS[1]
        return ldap_port

    def _add_ldap_entry(self, ldap_entry: list):
        self.log.debug("Adding an Ldap entry of: %s", ldap_entry)
        username = f'cn=admin,{LDAP_BASE_OBJECT}'
        self.localhost.add_ldap_entry(ip=self.ldap_server_ip, ldap_port=self.ldap_port,
                                      user=username, password=LDAP_PASSWORD, ldap_entry=ldap_entry)

    def create_role_in_ldap(self, ldap_role_name: str, unique_members: list):
        unique_members_list = [f'uid={user},ou=Person,{LDAP_BASE_OBJECT}' for user in unique_members]
        role_entry = [
            f'cn={ldap_role_name},{LDAP_BASE_OBJECT}',
            ['groupOfUniqueNames', 'simpleSecurityObject', 'top'],
            {
                'uniqueMember': unique_members_list,
                'userPassword': LDAP_PASSWORD
            }
        ]
        self._add_ldap_entry(ldap_entry=role_entry)

    def search_ldap_role(self, ldap_role_name: str, raise_error: bool = True) -> str:
        ldap_entry = f'(cn={ldap_role_name})'
        self.log.debug("Searching for Ldap entry: %s, %s", LDAP_BASE_OBJECT, ldap_entry)
        res = self.localhost.search_ldap_entry(LDAP_BASE_OBJECT, ldap_entry)
        self.log.debug("Ldap entry Search result: %s", res)
        if not res and raise_error:
            raise Exception(f'Failed to find {ldap_role_name} in Ldap.')
        distinguished_name = str(res).split()[1]
        return distinguished_name

    def search_ldap_user(self, ldap_user_name: str, raise_error: bool = True) -> str:
        ldap_entry = f'(uid={ldap_user_name})'
        self.log.debug("Searching for Ldap user: %s, %s", LDAP_BASE_OBJECT, ldap_entry)
        res = self.localhost.search_ldap_entry(search_base=LDAP_BASE_OBJECT, search_filter=ldap_entry)
        self.log.debug("Ldap user search result: %s", res)
        if not res and raise_error:
            raise Exception(f'Failed to find {ldap_user_name} in Ldap.')
        distinguished_name = str(res).split()[1]
        return distinguished_name

    def modify_ldap_role_delete_member(self, ldap_role_name: str, member_name: str, raise_error: bool = True):
        distinguished_name = self.search_ldap_role(ldap_role_name=ldap_role_name, raise_error=raise_error)
        self.log.debug("Deleting member %s from Ldap entry: %s", member_name, distinguished_name)
        unique_member_update = {'uniqueMember': [
            ('MODIFY_DELETE', [f'uid={member_name},ou=Person,{LDAP_BASE_OBJECT}'])]}
        res = self.localhost.modify_ldap_entry(distinguished_name, unique_member_update)
        if not res and raise_error:
            raise Exception(f'Failed to delete entry {distinguished_name} from Ldap role {ldap_role_name}')

    def delete_ldap_role(self, ldap_role_name: str, raise_error: bool = True):
        distinguished_name = self.search_ldap_role(ldap_role_name=ldap_role_name, raise_error=raise_error)
        self.log.debug("Deleting Ldap entry: %s", distinguished_name)
        res = self.localhost.delete_ldap_entry(distinguished_name)
        if not res and raise_error:
            raise Exception(f'Failed to delete entry {distinguished_name} from Ldap.')

    def delete_ldap_user(self, ldap_user_name: str, raise_error: bool = True):
        distinguished_name = self.search_ldap_user(ldap_user_name=ldap_user_name, raise_error=raise_error)
        self.log.debug("Deleting Ldap user entry: %s", distinguished_name)
        res = self.localhost.delete_ldap_entry(distinguished_name)
        if not res and raise_error:
            raise Exception(f'Failed to delete entry {distinguished_name} from Ldap.')

    @retrying(n=10, sleep_time=30, message='Waiting for no user permissions verification', allowed_exceptions=(ValueError,))
    def wait_verify_user_no_permissions(self, username: str):
        """
        Checks for unauthorized user permission following roles update.
        Creating a keyspace by the user and verify it fails as expected.
        """
        self.log.debug("Verifying user %s roles permission change in Scylla DB (following an LDAP Role update) ", username)

        try:
            with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0], user=username,
                                                        password=LDAP_PASSWORD) as session:
                session.execute(
                    """ CREATE KEYSPACE IF NOT EXISTS test_no_permission WITH replication = {'class': 'NetworkTopologyStrategy',
                    'replication_factor': 1} """)
                session.execute(""" DROP KEYSPACE IF EXISTS test_no_permission """)
            raise ValueError('DID NOT RAISE')
        except Unauthorized:
            return

    @retrying(n=10, sleep_time=30, message='Waiting for user permissions update', allowed_exceptions=Unauthorized)
    def wait_verify_user_permissions(self, username: str, keyspace: str = 'customer_ldap'):
        """
        Checks for authorized user permission following roles update.
        Creating a keyspace by the user and verify it doesn't fail.
        """
        self.log.debug("Verifying user %s roles permission change in Scylla DB (following an LDAP Role update) ", username)

        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0], user=username,
                                                    password=LDAP_PASSWORD) as session:
            session.execute(
                """ CREATE KEYSPACE IF NOT EXISTS test_permission_ks WITH replication = {'class': 'NetworkTopologyStrategy',
                'replication_factor': 1} """)
            session.execute(""" DROP KEYSPACE IF EXISTS test_permission_ks """)

            session.execute(
                f""" CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy',
                'replication_factor': 3}} """)
            session.execute(
                f""" CREATE TABLE IF NOT EXISTS {keyspace}.info (key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) """)
            session.execute('INSERT INTO customer_ldap.info (key, c, v) VALUES (\'key1\', \'c1\', \'v1\')')
            session.execute("SELECT * from customer_ldap.info LIMIT 1")

    @retrying(n=10, sleep_time=30, message='Waiting for user permissions update', allowed_exceptions=(AssertionError,))
    def wait_for_user_roles_update(self, username: str, are_roles_expected: bool):
        """
        Checks for updated output of user roles.
        Example command: LIST ROLES OF new_user;
        Example output:
        LIST ROLES OF new_user;
        ╭─────────┬─────────┬──────────────────────┬────────────╮
        │role     │ super   │ login                │ options    │
        ├─────────┼─────────┼──────────────────────┼────────────┤
        │customer │ False   │ False                │ {}         │
        ├─────────┼─────────┼──────────────────────┼────────────┤
        │trainer  │ False   │ False                │ {}         │
        ├─────────┼─────────┼──────────────────────┼────────────┤
        │new_user │ False   │ True                 │ {}         │
        ╰─────────┴─────────┴──────────────────────┴────────────╯
        """
        self.log.debug("Waiting user %s roles change in Scylla DB (following an LDAP Role update) ", username)
        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0]) as session:
            query = f"LIST ROLES OF '{username}';"
            result = session.execute(query)
            output = result.all()
            self.log.debug("LIST ROLES OF %s: %s", username, output)
            if are_roles_expected:
                assert len(output) > 1
            else:
                assert len(output) == 1

    def add_user_in_ldap(self, username: str):
        user_entry = [
            f'uid={username},ou=Person,{LDAP_BASE_OBJECT}',
            ['uidObject', 'organizationalPerson', 'top'],
            {
                'userPassword': LDAP_PASSWORD,
                'sn': 'PersonSn',
                'cn': 'PersonCn'
            }
        ]
        self._add_ldap_entry(ldap_entry=user_entry)

    def create_role_in_scylla(self, node, role_name: str, is_superuser: bool = True,
                              is_login: bool = True):
        self.log.debug("Configuring a Role %s in Scylla DB", role_name)
        create_role_cmd = f'CREATE ROLE IF NOT EXISTS \'{role_name}\''
        superuser_argument = ' WITH SUPERUSER=true' if is_superuser else ' WITH SUPERUSER=false'
        login_argument = ' AND login=true' if is_login else ' AND login=false'
        create_role_cmd += superuser_argument + login_argument
        if not self.params.get('prepare_saslauthd'):
            create_role_cmd += f' AND password=\'{LDAP_PASSWORD}\''
        node.run_cqlsh(create_role_cmd)
