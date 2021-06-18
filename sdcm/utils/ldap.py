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
from textwrap import dedent
from typing import List

from sdcm.keystore import KeyStore
from sdcm.test_config import TestConfig
from sdcm.utils.common import get_my_ip
from sdcm.utils.decorators import retrying

from time import sleep
from ldap3 import Server, Connection, ALL, ALL_ATTRIBUTES
from ldap3.core.exceptions import LDAPSocketOpenError

from sdcm.utils.docker_utils import ContainerManager

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
LDAP_BASE_OBJECT = (lambda l: ','.join([f'dc={part}' for part in l.split('.')]))(LDAP_DOMAIN)
SASLAUTHD_AUTHENTICATOR = 'com.scylladb.auth.SaslauthdAuthenticator'


class LdapServerNotReady(Exception):
    pass


class LdapContainerMixin:  # pylint: disable=too-few-public-methods
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

    def add_ldap_entry(self, user, password, ldap_entry, address=None, port=None, use_ssl=False):
        if address is None:
            address = self.get_ldap_address()
        if port is None:
            port = self.get_ldap_port(use_ssl)
        if not LdapContainerMixin.is_ldap_connection_bound():
            LdapContainerMixin.create_ldap_connection(address, port, user, password)
        LdapContainerMixin.ldap_conn.add(*ldap_entry)

    @staticmethod
    def search_ldap_entry(search_base, search_filter):
        LdapContainerMixin.ldap_conn.search(search_base=search_base, search_filter=search_filter,
                                            attributes=ALL_ATTRIBUTES)
        return LdapContainerMixin.ldap_conn.entries

    @staticmethod
    def modify_ldap_entry(*args, **kwargs):
        return LdapContainerMixin.ldap_conn.modify(*args, **kwargs)

    def populate_ldap_role(self, role_name: str, role_password: str, members: List[str], address: str = None,
                           port: str = None, use_ssl: bool = False):
        unique_members_list = [f'uid={user},ou=Person,{LDAP_BASE_OBJECT}' for user in members]
        ldap_entry = [
            f'cn={role_name},{LDAP_BASE_OBJECT}',
            ['groupOfUniqueNames', 'simpleSecurityObject', 'top'],
            {'uniqueMember': unique_members_list, 'userPassword': role_password},
        ]
        self.add_ldap_entry(
            user=self.get_ldap_admin_username(), password=self.get_ldap_admin_password(), ldap_entry=ldap_entry,
            address=address, port=port, use_ssl=use_ssl
        )

    def populate_ldap_builtin_persons(self):
        # Buildin user also need to be added in ldap server, otherwise it can't login to create LDAP_USERS
        for person_name, person_password in ((user, LDAP_PASSWORD) for user in LDAP_USERS):
            self.populate_ldap_person(person_name=person_name, person_password=person_password)

    def populate_ldap_person(self, person_name: str, person_password: str, address: str = None, port: str = None,
                             use_ssl: bool = False):
        ldap_entry = [
            f'uid={person_name},ou=Person,{LDAP_BASE_OBJECT}',
            ['uidObject', 'organizationalPerson', 'top'],
            {'userPassword': person_password, 'sn': 'PersonSn', 'cn': 'PersonCn'},
        ]
        self.add_ldap_entry(
            user=self.get_ldap_admin_username(), password=self.get_ldap_admin_password(), ldap_entry=ldap_entry,
            address=address, port=port, use_ssl=use_ssl
        )

    def populate_ldap_person_bucket(self, address: str = None, port: str = None, use_ssl: bool = False):
        ldap_entry = [
            f'ou=Person,{LDAP_BASE_OBJECT}',
            ['organizationalUnit', 'top'],
            {'ou': 'Person'},
        ]
        self.add_ldap_entry(
            user=self.get_ldap_admin_username(), password=self.get_ldap_admin_password(), ldap_entry=ldap_entry,
            address=address, port=port, use_ssl=use_ssl
        )

    @staticmethod
    def get_ldap_admin_username():
        return f'cn=admin,{LDAP_BASE_OBJECT}'

    @staticmethod
    def get_ldap_admin_password():
        return LDAP_PASSWORD  # not in use not for authorization, but must be in the config

    @staticmethod
    def get_ldap_address():
        return get_my_ip()

    def get_ldap_port(self, use_ssl: bool):
        if use_ssl:
            return ContainerManager.get_container_port(self, "ldap", LDAP_SSL_PORT)
        return ContainerManager.get_container_port(self, "ldap", LDAP_PORT)

    def start_and_configure_ldap(self, use_ssl=False):
        ldap_address, ldap_port = self._start_ldap_container(use_ssl=use_ssl)
        # Populate ldap with
        self.populate_ldap_role(role_name=LDAP_ROLE, role_password=LDAP_PASSWORD, members=LDAP_USERS,
                                address=ldap_address, port=ldap_port)
        return ldap_address, ldap_port

    @retrying(n=20, sleep_time=6, allowed_exceptions=LdapServerNotReady)
    def _start_ldap_container(self, use_ssl=False):
        ContainerManager.run_container(self, "ldap")
        if ContainerManager.get_container(self, 'ldap').exec_run("timeout 30s container/tool/wait-process")[0] != 0:
            raise LdapServerNotReady("LDAP server didn't finish its startup yet...")
        return get_my_ip(), self.get_ldap_port(use_ssl=use_ssl)


def get_openldap_config():
    if TestConfig.LDAP_ADDRESS is None:
        return {}
    if TestConfig.IP_SSH_CONNECTIONS == 'public' or TestConfig.MULTI_REGION:
        ldap_server_ip, ldap_port = '127.0.0.1', LDAP_SSH_TUNNEL_LOCAL_PORT
    else:
        ldap_server_ip, ldap_port = TestConfig.LDAP_ADDRESS, TestConfig.LDAP_PORT
    return {'role_manager': 'com.scylladb.auth.LDAPRoleManager',
            'ldap_url_template': f'ldap://{ldap_server_ip}:{ldap_port}/'
                                 f'{LDAP_BASE_OBJECT}?cn?sub?(uniqueMember='
                                 f'uid={{USER}},ou=Person,{LDAP_BASE_OBJECT})',
            'ldap_attr_role': 'cn',
            'ldap_bind_dn': f'cn=admin,{LDAP_BASE_OBJECT}',
            'ldap_bind_passwd': LDAP_PASSWORD}


def get_ldap_ms_ad_config():
    if TestConfig.LDAP_ADDRESS is None:
        return {}
    ldap_ms_ad_credentials = KeyStore().get_ldap_ms_ad_credentials()
    return {'ldap_attr_role': 'cn',
            'ldap_bind_dn': ldap_ms_ad_credentials['ldap_bind_dn'],
            'ldap_bind_passwd': ldap_ms_ad_credentials['admin_password'],
            'ldap_url_template':
                f'ldap://{ldap_ms_ad_credentials["server_address"]}:{LDAP_PORT}/{LDAP_BASE_OBJECT}?cn?sub?'
                f'(member=CN={{USER}},DC=scylla-qa,DC=com)',
            'role_manager': 'com.scylladb.auth.LDAPRoleManager'}


def get_saslauthd_config():
    if TestConfig.LDAP_ADDRESS is None:
        return {}
    if TestConfig.IP_SSH_CONNECTIONS == 'public' or TestConfig.MULTI_REGION:
        ldap_server_ip, ldap_port = '127.0.0.1', LDAP_SSH_TUNNEL_LOCAL_PORT
    else:
        ldap_server_ip, ldap_port = TestConfig.LDAP_ADDRESS, TestConfig.LDAP_PORT
    return {'ldap_servers': f'ldap://{ldap_server_ip}:{ldap_port}/',
            'ldap_search_base': f'ou=Person,{LDAP_BASE_OBJECT}',
            'ldap_bind_dn': f'cn=admin,{LDAP_BASE_OBJECT}',
            'ldap_bind_pw': LDAP_PASSWORD}


def get_saslauthd_ms_ad_config():
    ldap_ms_ad_credentials = KeyStore().get_ldap_ms_ad_credentials()
    ldap_server_ip = ldap_ms_ad_credentials["server_address"]
    ldap_port = LDAP_PORT
    ldap_search_base = f'OU=People,{LDAP_BASE_OBJECT}'
    ldap_bind_dn = ldap_ms_ad_credentials['ldap_bind_dn']
    ldap_bind_pw = ldap_ms_ad_credentials['admin_password']

    return {'ldap_servers': f'ldap://{ldap_server_ip}:{ldap_port}/',
            'ldap_search_base': ldap_search_base,
            'ldap_filter': '(cn=%u)',
            'ldap_bind_dn': ldap_bind_dn,
            'ldap_bind_pw': ldap_bind_pw}


def prepare_and_start_saslauthd_service(node):
    """
    Install and setup saslauthd service.
    """
    if node.is_rhel_like():
        setup_script = dedent(f"""
            sudo yum install -y cyrus-sasl
            sudo systemctl enable saslauthd
            echo 'MECH=ldap' | sudo tee -a /etc/sysconfig/saslauthd
            sudo touch /etc/saslauthd.conf
        """)
    else:
        setup_script = dedent(f"""
            sudo apt-get install -y sasl2-bin
            sudo systemctl enable saslauthd
            echo -e 'MECHANISMS=ldap\nSTART=yes\n' | sudo tee -a /etc/default/saslauthd
            sudo touch /etc/saslauthd.conf
            sudo adduser scylla sasl  # to avoid the permission issue of unit socket
        """)
    node.wait_apt_not_running()
    node.remoter.run('bash -cxe "%s"' % setup_script)
    if node.parent_cluster.params.get('use_ms_ad_ldap'):
        conf = get_saslauthd_ms_ad_config()
    else:
        conf = get_saslauthd_config()
    for key in conf.keys():
        node.remoter.run(f'echo "{key}: {conf[key]}" | sudo tee -a /etc/saslauthd.conf')
    with node.remote_scylla_yaml() as scylla_yml:
        scylla_yml['saslauthd_socket_path'] = '/run/saslauthd/mux'
    node.remoter.sudo('systemctl restart saslauthd')


def update_authenticator(nodes, authenticator='AllowAllAuthenticator', restart=True):
    """
    Update the authenticator of nodes, restart the nodes to make the change effective
    """
    for node in nodes:
        with node.remote_scylla_yaml() as scylla_yml:
            scylla_yml['authenticator'] = authenticator
        if restart:
            if authenticator == SASLAUTHD_AUTHENTICATOR:
                node.run_cqlsh(f'ALTER ROLE \'{LDAP_USERS[0]}\' with password=\'{LDAP_PASSWORD}\'')
                node.parent_cluster.use_saslauthd_authenticator = True
            else:
                node.parent_cluster.use_saslauthd_authenticator = False
            node.parent_cluster.params['are_ldap_users_on_scylla'] = node.parent_cluster.use_saslauthd_authenticator
            node.restart_scylla_server()
            node.wait_db_up()


def change_default_password(node, user='cassandra', password='cassandra'):
    """
    Default password of Role `cassandra` is same as username, MS-AD doesn't allow the weak password.
    Here we change password of `cassandra`, then the cassandra user can smoothly work in switching Authenticator.
    """
    node.run_cqlsh(f"ALTER ROLE '{user}' with password='{password}{DEFAULT_PWD_SUFFIX}'")
    node.parent_cluster.added_password_suffix = True
