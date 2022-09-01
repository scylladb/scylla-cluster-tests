#!/usr/bin/env python

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
# Copyright (c) 2022 ScyllaDB
import re

from longevity_test import LongevityTest
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from test_lib.sla import Role, ServiceLevel


class LongevitySlaTest(LongevityTest):
    DEFAULT_USER = "cassandra"
    DEFAULT_USER_PASSWORD = "cassandra"
    STRESS_ROLE_NAME_TEMPLATE = 'role%d_%d'
    STRESS_ROLE_PASSWORD_TEMPLATE = 'rolep%d'
    SERVICE_LEVEL_NAME_TEMPLATE = 'sl%d_%d'
    FULLSCAN_SERVICE_LEVEL_SHARES = 600

    def __init__(self, *args):
        super().__init__(*args)
        self.service_level_shares = self.params.get("service_level_shares")
        self.fullscan_role = None
        self.roles = []

    def test_custom_time(self):
        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0], user=self.DEFAULT_USER,
                                                    password=self.DEFAULT_USER_PASSWORD) as session:
            # Add index (shares position in the self.service_level_shares list) to role and service level names to do
            # it unique and prevent failure when try to create role/SL with same name
            for index, shares in enumerate(self.service_level_shares):
                self.roles.append(self.create_sla_auth(session=session, shares=shares, index=index))

            if self.params.get("run_fullscan"):
                self.fullscan_role = self.create_sla_auth(session=session, shares=self.FULLSCAN_SERVICE_LEVEL_SHARES)

        self.add_sla_credentials_to_stress_cmds()
        super().test_custom_time()

    def create_sla_auth(self, session, shares: int, index: int):
        role = Role(session=session, name=self.STRESS_ROLE_NAME_TEMPLATE % (shares, index),
                    password=self.STRESS_ROLE_PASSWORD_TEMPLATE % shares, login=True).create()
        role.attach_service_level(ServiceLevel(session=session, name=self.SERVICE_LEVEL_NAME_TEMPLATE % (shares, index),
                                               shares=shares).create())

        return role

    def add_sla_credentials_to_stress_cmds(self):
        def _set_credentials_to_cmd(cmd):
            if self.roles and "<sla credentials " in cmd:
                if 'user=' in cmd:
                    # if stress command is not defined as expected, stop the tests and fix it. Then re-run
                    TestFrameworkEvent(
                        source=self.__class__.__name__,
                        message="Stress command is defined wrong. Credentials already applied. Remove unnecessary "
                                f"and re-run the test. Command: {cmd}",
                        severity=Severity.CRITICAL
                    ).publish()

                index = re.search(r"<sla credentials (\d+)>", cmd)
                role_index = int(index.groups(0)[0]) if index else None
                if role_index is None:
                    # if stress command is not defined as expected, stop the tests and fix it. Then re-run
                    TestFrameworkEvent(
                        source=self.__class__.__name__,
                        message="Stress command is defined wrong. Expected pattern '<credentials \\d>' was not found. "
                                f"Fix the command and re-run the test. Command: {cmd}",
                        severity=Severity.CRITICAL
                    ).publish()
                sla_role_name = self.roles[role_index].name.replace('"', '')
                sla_role_password = self.roles[role_index].password
                return re.sub(r'<sla credentials \d+>', f'user={sla_role_name} password={sla_role_password}', cmd)
            return cmd

        for stress_op in ['prepare_write_cmd', 'stress_cmd', 'stress_read_cmd']:
            stress_cmds = []
            stress_params = self.params.get(stress_op)
            if isinstance(stress_params, str):
                stress_params = [stress_params]

            if not stress_params:
                continue

            for stress_cmd in stress_params:
                # cover multitenant test
                if isinstance(stress_cmd, list):
                    cmds = []
                    for current_cmd in stress_cmd:
                        cmds.append(_set_credentials_to_cmd(cmd=current_cmd))
                    stress_cmds.append(cmds)
                else:
                    stress_cmds.append(_set_credentials_to_cmd(cmd=stress_cmd))

            self.params[stress_op] = stress_cmds
