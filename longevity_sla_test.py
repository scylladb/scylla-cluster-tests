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
from longevity_test import LongevityTest
from sdcm.sla.libs.sla_utils import SlaUtils
from sdcm.utils import loader_utils
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from test_lib.sla import create_sla_auth


class LongevitySlaTest(LongevityTest, loader_utils.LoaderUtilsMixin):
    FULLSCAN_SERVICE_LEVEL_SHARES = 600

    def setUp(self):
        super().setUp()
        self.service_level_shares = self.params.get("service_level_shares")
        self.fullscan_role = None
        self.roles = []

    def test_custom_time(self):
        is_enterprise = self.db_cluster.nodes[0].is_enterprise
        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0], user=loader_utils.DEFAULT_USER,
                                                    password=loader_utils.DEFAULT_USER_PASSWORD) as session:
            # Add index (shares position in the self.service_level_shares list) to role and service level names to do
            # it unique and prevent failure when try to create role/SL with same name

            for index, shares in enumerate(self.service_level_shares):
                self.roles.append(create_sla_auth(session=session,
                                                  shares=shares,
                                                  index=str(index),
                                                  attach_service_level=is_enterprise))

            if self.params.get("run_fullscan"):
                self.fullscan_role = create_sla_auth(session=session, shares=self.FULLSCAN_SERVICE_LEVEL_SHARES,
                                                     index="0")

        # Wait for all SLs are propagated to all nodes
        for role in self.roles + [self.fullscan_role]:
            # self.fullscan_role may be None if "run_fullscan" is not defined
            if role and is_enterprise:
                with adaptive_timeout(Operations.SERVICE_LEVEL_PROPAGATION, node=self.db_cluster.data_nodes[0], timeout=15,
                                      service_level_for_test_step="MAIN_SERVICE_LEVEL"):
                    SlaUtils().wait_for_service_level_propagated(cluster=self.db_cluster, service_level=role.attached_service_level)

        self.add_sla_credentials_to_stress_cmds(workload_names=['prepare_write_cmd', 'stress_cmd', 'stress_read_cmd'],
                                                roles=self.roles, params=self.params,
                                                parent_class_name=self.__class__.__name__)

        super().test_custom_time()
