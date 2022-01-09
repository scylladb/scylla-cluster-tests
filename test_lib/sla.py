#!/usr/bin/env python

import logging
import time

from cassandra import InvalidRequest


def sla_result_to_dict(sla_result):
    # Result example: <type 'list'>: [Row(service_level='sla1', shares=1)]
    sla_list = []
    for row in sla_result:
        current_sla = []
        for i, _ in enumerate(row._fields):
            current_sla.append(row[i])
        sla_list.append(current_sla)
    return sla_list


DEFAULT_SERVICE_LEVEL_SHARES = 1000


class ServiceLevel():  # pylint: disable=too-many-instance-attributes
    # The class provide interface to manage SERVICE LEVEL
    def __init__(self, session, name, service_shares=None, verbose=True):
        self.session = session
        self._name = name
        self._service_shares = service_shares
        self.verbose = verbose
        self._is_created = False
        self.log = logging.getLogger("test")

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def service_shares(self):
        return self._service_shares

    @service_shares.setter
    def service_shares(self, service_level_shares):
        self._service_shares = service_level_shares

    @property
    def is_created(self):
        return self._is_created

    @is_created.setter
    def is_created(self, is_created):
        self._is_created = is_created

    def create(self, if_not_exists=True):
        query = 'CREATE SERVICE_LEVEL{if_not_exists} {service_level_name}{shares}' \
            .format(if_not_exists=' IF NOT EXISTS' if if_not_exists else '',
                    service_level_name=self.name,
                    shares=' WITH SHARES = %d' % self.service_shares if self.service_shares is not None else '')
        if self.verbose:
            self.log.debug('Create service level query: {}'.format(query))
        self.session.execute(query)
        # It's take the about 10 sec to create service level. Waiting for the finish
        time.sleep(12)
        self.log.debug('Service level "{}" has been created'.format(self.name))
        self.is_created = True

    def alter(self, new_shares):
        query = 'ALTER SERVICE_LEVEL {service_level_name} WITH SHARES = {shares}' \
            .format(service_level_name=self.name,
                    shares=new_shares)
        if self.verbose:
            self.log.debug('Change service level query: {}'.format(query))
        self.session.execute(query)
        self.log.debug('Service level "{}" has been altered'.format(self.name))
        self.service_shares = new_shares

    def drop(self, if_exists=True):
        query = 'DROP SERVICE_LEVEL{if_exists} {service_level_name}' \
            .format(service_level_name=self.name,
                    if_exists=' IF EXISTS' if if_exists else '')
        if self.verbose:
            self.log.debug('Drop service level query: {}'.format(query))
        self.session.execute(query)
        self.log.debug('Service level "{}" has been dropped'.format(self.name))
        self.is_created = False

    def list_service_level(self):
        query = 'LIST SERVICE_LEVEL {}'.format(self.name)
        if self.verbose:
            self.log.debug('List service level query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_result_to_dict(res)

    def list_all_service_levels(self):
        query = 'LIST ALL SERVICE_LEVELS'
        if self.verbose:
            self.log.debug('List all service levels query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_result_to_dict(res)


class UserRoleBase():  # pylint: disable=too-many-instance-attributes
    # Base class for ROLES and USERS
    AUTHENTICATION_ENTITY = ''

    def __init__(self, session, name, password=None, superuser=None, verbose=True):  # pylint: disable=too-many-arguments
        self._name = name
        self.password = password
        self.session = session
        self.superuser = superuser
        self.verbose = verbose
        self._attached_service_level_name = ''
        self._attached_service_level_shares = None
        self.log = logging.getLogger("test")

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def attached_service_level_name(self):
        return self._attached_service_level_name

    @attached_service_level_name.setter
    def attached_service_level_name(self, service_level_name):
        self._attached_service_level_name = service_level_name

    @property
    def attached_service_level_shares(self):
        return self._attached_service_level_shares

    @attached_service_level_shares.setter
    def attached_service_level_shares(self, service_level_shares):
        self._attached_service_level_shares = service_level_shares

    def attach_service_level(self, service_level):
        query = 'ATTACH SERVICE_LEVEL {service_level_name} TO {role_name}' \
            .format(service_level_name=service_level.name,
                    role_name=self.name)
        if self.verbose:
            self.log.debug('Attach service level query: {}'.format(query))
        self.session.execute(query)
        self.log.debug('Service level "{}" has been attached to {} role'.format(service_level.name, self.name))
        self.attached_service_level_name = service_level.name
        self.attached_service_level_shares = service_level.service_shares \
            if service_level.service_shares \
            else DEFAULT_SERVICE_LEVEL_SHARES

    def detach_service_level(self):
        query = 'DETACH SERVICE_LEVEL FROM {role_name}' \
            .format(role_name=self.name)
        if self.verbose:
            self.log.debug('Detach service level query: {}'.format(query))
        self.session.execute(query)
        self.log.debug('The service level has been detached from {} role'.format(self.name))

        self.attached_service_level_name = ''
        self.attached_service_level_shares = None

    def grant_me_to(self, grant_to):
        role_be_granted = self.name
        grant_to = grant_to.name
        query = 'GRANT {role_be_granted} to {grant_to}'.format(role_be_granted=role_be_granted, grant_to=grant_to)
        if self.verbose:
            self.log.debug('GRANT role query: {}'.format(query))
        try:
            self.session.execute(query)
        except InvalidRequest as ex:
            assert ex.message.split('message=')[1].replace('"', '') == \
                '{grant_to} already includes role {role_be_granted}.'.format(role_be_granted=role_be_granted, grant_to=grant_to), \
                'Unexpected error during grant: {}'.format(ex.message)
        self.log.debug('Role "{role_be_granted}" has been granted to {grant_to}'.format(
            role_be_granted=role_be_granted, grant_to=grant_to))

    def revoke_me_from(self, revoke_from):
        role_be_revoked = self.name
        role_revokes_from = revoke_from.name
        query = 'REVOKE {role_be_revoked} FROM {role_revokes_from}'.format(role_be_revoked=role_be_revoked,
                                                                           role_revokes_from=role_revokes_from)
        if self.verbose:
            self.log.debug('REVOKE role query: {}'.format(query))
        try:
            self.session.execute(query)
        except InvalidRequest as ex:
            assert ex.message.split('message=')[1].replace('"', '') == \
                '{role_revokes_from} was not granted role {role_be_revoked}, so it cannot be revoked.' \
                .format(role_be_revoked=role_be_revoked, role_revokes_from=role_revokes_from), \
                'Unexpected error during revoke: {}'.format(ex.message)
        self.log.debug('Role "{role_be_revoked}" has been revocked from {role_revokes_from}'.format(
            role_be_revoked=role_be_revoked, role_revokes_from=role_revokes_from))

    def list_attached_sla_of_user_or_role(self):  # pylint: disable=invalid-name
        query = 'LIST ATTACHED SERVICE_LEVEL OF {}'.format(self.name)
        if self.verbose:
            self.log.debug('List attached service level(s) query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_result_to_dict(res)

    def list_all_attached_service_levels(self):   # pylint: disable=invalid-name
        query = 'LIST ATTACHED ALL SERVICE_LEVELS'
        if self.verbose:
            self.log.debug('List attached service level(s) query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_result_to_dict(res)

    def list_effective_service_levels(self, auth_obj):
        query = 'LIST SERVICE_LEVELS OF {}'.format(auth_obj.name)
        if self.verbose:
            self.log.debug('List effective service levels query: {}'.format(query))
        res = list(self.session.execute(query))
        return sla_result_to_dict(res)

    def drop(self, if_exists=True):
        query = 'DROP {entity}{if_exists} {name}'.format(entity=self.AUTHENTICATION_ENTITY,
                                                         name=self.name,
                                                         if_exists=' IF EXISTS' if if_exists else '')
        if self.verbose:
            self.log.debug('Drop {entity} query: {query}'.format(entity=self.AUTHENTICATION_ENTITY, query=query))

        self.session.execute(query)
        self.log.debug('{entity} "{name}" has been dropped'.format(entity=self.AUTHENTICATION_ENTITY, name=self.name))


class Role(UserRoleBase):
    # The class provide interface to manage ROLES
    AUTHENTICATION_ENTITY = 'ROLE'

    def __init__(self, session, name, password=None, login=False, superuser=False, options_dict=None, verbose=True):  # pylint: disable=too-many-arguments
        super().__init__(session, name, password, superuser, verbose)
        self.login = login
        self.options_dict = options_dict

    def create(self):
        # Example: CREATE ROLE bob WITH PASSWORD = 'password_b'AND LOGIN = true AND SUPERUSER = true;
        # Example: CREATE ROLE carlos WITH OPTIONS = {'custom_option1': 'option1_value', 'custom_option2': 99};
        role_options = {}
        for opt in ['password', 'login', 'superuser', 'options_dict']:
            if hasattr(self, opt):
                value = getattr(self, opt)
                if value:
                    role_options[opt.replace('_dict', '')] = '\'{}\''.format(value) \
                        if opt == 'password' else value
        role_options_str = ' AND '.join(['{} = {}'.format(opt, val) for opt, val in role_options.items()])
        if role_options_str:
            role_options_str = ' WITH {}'.format(role_options_str)

        query = 'CREATE ROLE IF NOT EXISTS {name}{role_options_str}'.format(name=self.name,
                                                                            role_options_str=role_options_str)
        if self.verbose:
            self.log.debug('CREATE role query: {}'.format(query))
        self.session.execute(query)

        # It's take the about 10 sec to create role. Waiting for the finish
        time.sleep(12)
        self.log.debug('Role "{}" has been created'.format(self.name))


class User(UserRoleBase):
    # The class provide interface to manage USERS
    AUTHENTICATION_ENTITY = 'USER'

    def create(self):
        user_options_str = '{password}{superuser}'.format(password=' PASSWORD \'{}\''
                                                          .format(self.password) if self.password else '',
                                                          superuser='' if self.superuser is None else ' SUPERUSER'
                                                          if self.superuser else ' NOSUPERUSER')
        if user_options_str:
            user_options_str = ' WITH {}'.format(user_options_str)

        query = 'CREATE USER IF NOT EXISTS {name}{user_options_str}'.format(name=self.name,
                                                                            user_options_str=user_options_str)
        if self.verbose:
            self.log.debug('Create user query: {}'.format(query))

        self.session.execute(query)

        # It's take the about 10 sec to create user. Waiting for the finish
        time.sleep(12)
        self.log.debug('User "{}" has been created'.format(self.name))
