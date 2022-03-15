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

import logging

from performance_regression_test import PerformanceRegressionTest
from sdcm.db_stats import TestStatsMixin
from sdcm.cluster_k8s import eks, LOADER_CLUSTER_CONFIG, LoaderPodCluster


class TenantStats(TestStatsMixin):

    def __init__(self, db_cluster, loaders, monitors, params, _id):
        self.db_cluster = db_cluster
        self.loaders = loaders
        self.monitors = monitors
        self.params = params
        self.log = logging.getLogger(self.__class__.__name__)
        self.create_stats = True
        self._id = _id
        super().__init__()

    def id(self):  # pylint: disable=invalid-name
        return self._id


class PerformanceRegressionMultiTenantTest(PerformanceRegressionTest):  # pylint: disable=too-many-public-methods

    """
    Test Scylla performance regression with cassandra-stress.
    """

    def __init__(self, *args):
        self.tenants_stats: list[TenantStats] = []
        super().__init__(*args)

    def init_resources(self, *args, **kwargs):
        super().init_resources(*args, **kwargs)
        # add the first created Pods
        self.tenants_stats += [TenantStats(self.db_cluster, self.loaders, self.monitors, self.params, _id=self.id())]

        # create extra pods for dbs and loaders, on the same pools

        scylla_pool = self.k8s_cluster.pools[self.k8s_cluster.SCYLLA_POOL_NAME]
        loader_pool = self.k8s_cluster.pools[self.k8s_cluster.LOADER_POOL_NAME]

        for i in range(1, self.params.get('k8s_tenants_num')):
            db_cluster = eks.EksScyllaPodCluster(
                k8s_cluster=self.k8s_cluster,
                scylla_cluster_name=self.params.get("k8s_scylla_cluster_name"),
                user_prefix=self.params.get("user_prefix"),
                n_nodes=self.params.get("n_db_nodes"),
                params=self.params,
                node_pool=scylla_pool,
                namespace=f'scylla-{i}',
                skip_node_prepare=True,
            )
            loaders = LoaderPodCluster(k8s_cluster=self.k8s_cluster,
                                       loader_cluster_config=LOADER_CLUSTER_CONFIG,
                                       loader_cluster_name=self.params.get("k8s_loader_cluster_name"),
                                       user_prefix=self.params.get("user_prefix"),
                                       n_nodes=self.params.get("n_loaders"),
                                       params=self.params,
                                       node_pool=loader_pool,
                                       namespace=f"sct-loader-{i}")

            self.init_nodes(db_cluster=db_cluster)
            loaders.wait_for_init()
            self.tenants_stats += [TenantStats(db_cluster=db_cluster, loaders=loaders,
                                               monitors=self.monitors, params=self.params,
                                               _id=self.id())]

    # Override the stress related method so we can run the stress commands on each tenant
    # on it's own, (and not mix the stats between them, TODO: check if we don't need a separate monitor for each)

    def create_test_stats(self, *args, **kwargs):
        for tenant in self.tenants_stats:
            tenant.create_test_stats(*args, **kwargs)

    def check_regression(self, es_index=None, es_doc_type=None):
        for tenant in self.tenants_stats:
            super().check_regression(tenant._test_index, tenant._es_doc_type)  # pylint: disable=protected-access

    def get_stress_results(self, queue, store_results=True):
        for stress_queue, tenant in zip(queue, self.tenants_stats):
            results = stress_queue.get_results()
            tenant.update_stress_results(results)

    def update_stress_results(self, *args, **kwargs):
        pass

    def display_results(self, results, test_name=''):
        pass

    def update_test_details(self, *args, **kwargs):
        for tenant in self.tenants_stats:
            tenant.update_test_details(*args, **kwargs)

    def run_stress_thread(self, *args, **kwargs):
        queues = []
        for tenant in self.tenants_stats:
            queues += super().run_stress_thread(*args, loader_set=tenant.loaders, node_list=tenant.db_cluster.nodes, **kwargs)

        return queues
