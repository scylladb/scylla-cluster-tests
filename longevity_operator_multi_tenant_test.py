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
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.operator.multitenant_common import MultiTenantTestMixin


class LongevityOperatorMultiTenantTest(MultiTenantTestMixin, LongevityTest):

    def test_custom_time(self):
        def _run_test_on_one_tenant(tenant):
            self.log.info("Longevity test for cluster %s with parameters: %s",
                          tenant.db_cluster, tenant.params)
            tenant.test_custom_time()

        self.log.info("Starting tests worker threads")

        tenants_number = self.k8s_clusters[0].tenants_number
        self.log.info("Clusters count: %s", tenants_number)
        object_set = ParallelObject(
            timeout=int(self.test_duration) * 60,
            objects=[[tenant] for tenant in self.tenants],
            num_workers=tenants_number,
        )
        object_set.run(func=_run_test_on_one_tenant, unpack_objects=True, ignore_exceptions=False)
