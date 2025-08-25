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

from performance_regression_test import PerformanceRegressionTest
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.operator.multitenant_common import MultiTenantTestMixin


class PerformanceRegressionOperatorMultiTenantTest(MultiTenantTestMixin, PerformanceRegressionTest):
    load_iteration_timeout_sec = 7200

    def create_test_stats(self, *args, **kwargs):
        self.log.info(
            "Suppress the test class stats creation. "
            "Leave it for the per-DB classes.")
        self._stats = self._init_stats()

    def update_test_with_errors(self):
        self.log.info("update_test_with_errors: Suppress writing errors to ES")

    def run_fstrim_on_all_db_nodes(self):
        for tenant in self.tenants:
            self.log.info("Running fstrim command on the '%s' DB cluster", tenant.db_cluster.name)
            tenant.db_cluster.fstrim_scylla_disks_on_nodes()

    def wait_no_compactions_running(self, *args, **kwargs):
        for tenant in self.tenants:
            self.log.info("Waiting for compactions to finish on the '%s' DB cluster",
                          tenant.db_cluster.name)
            tenant.wait_no_compactions_running(*args, **kwargs)

    def preload_data(self, compaction_strategy=None):
        def _preload_data(tenant):
            prepare_write_cmd = tenant.params.get('prepare_write_cmd')
            db_cluster_name = tenant.db_cluster.name
            if not prepare_write_cmd:
                self.log.warning(
                    "No prepare command defined in YAML for the '%s' cluster",
                    db_cluster_name)
                return
            self.log.info("Running preload command for the '%s' cluster", db_cluster_name)
            tenant.create_test_stats(
                sub_type='write-prepare', doc_id_with_timestamp=True)
            stress_queue, params = [], {
                'prefix': 'preload-',
            }
            if self.params.get('round_robin'):
                self.log.debug(
                    "'%s' DB cluster: Populating data using round_robin", db_cluster_name)
                params.update({'stress_num': 1, 'round_robin': True})
            for stress_cmd in prepare_write_cmd:
                params.update({
                    'stress_cmd': stress_cmd,
                    'duration': self.params.get('prepare_stress_duration'),
                })
                # Run all stress commands
                params.update(dict(stats_aggregate_cmds=False))
                self.log.debug("'%s' DB cluster: RUNNING stress cmd: %s",
                               db_cluster_name, stress_cmd)
                stress_queue.append(tenant.run_stress_thread(**params))
            for stress in stress_queue:
                tenant.get_stress_results(queue=stress, store_results=False)

            tenant.update_test_details()

        self.log.info("Running preload operation in parallel on all the DB clusters")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[tenant] for tenant in self.tenants],
            num_workers=len(self.tenants),
        )
        object_set.run(func=_preload_data, unpack_objects=True, ignore_exceptions=False)

    def run_read_workload(self, nemesis=False):
        def _run_read_workload(tenant, nemesis):
            tenant.run_read_workload(nemesis=nemesis)

        self.log.info("Running 'read' workload operation in parallel")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[tenant, nemesis] for tenant in self.tenants],
            num_workers=len(self.tenants),
        )
        object_set.run(func=_run_read_workload, unpack_objects=True, ignore_exceptions=False)

    def run_write_workload(self, nemesis=False):
        def _run_write_workload(tenant, nemesis):
            tenant.run_write_workload(nemesis=nemesis)

        self.log.info("Running 'write' workload operation in parallel")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[tenant, nemesis] for tenant in self.tenants],
            num_workers=len(self.tenants),
        )
        object_set.run(func=_run_write_workload, unpack_objects=True, ignore_exceptions=False)

    def run_mixed_workload(self, nemesis: bool = False):
        def _run_mixed_workload(tenant, nemesis):
            tenant.run_mixed_workload(nemesis=nemesis)

        self.log.info("Running 'mixed' workload operation in parallel")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[tenant, nemesis] for tenant in self.tenants],
            num_workers=len(self.tenants),
        )
        object_set.run(func=_run_mixed_workload, unpack_objects=True, ignore_exceptions=False)

    def run_workload(self, stress_cmd, nemesis=False, sub_type=None):
        def _run_workload(tenant, stress_cmd, nemesis):
            tenant.run_workload(stress_cmd=stress_cmd, nemesis=nemesis)

        self.log.info("Running workload in parallel with following command:\n%s", stress_cmd)
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[tenant, stress_cmd, nemesis] for tenant in self.tenants],
            num_workers=len(self.tenants),
        )
        object_set.run(func=_run_workload, unpack_objects=True, ignore_exceptions=False)

    def test_write(self):
        raise NotImplementedError()

    def test_read(self):
        raise NotImplementedError()

    def test_mixed(self):
        raise NotImplementedError()

    def test_mv_write(self):
        raise NotImplementedError()

    def test_mv_write_populated(self):
        raise NotImplementedError()

    def test_mv_write_not_populated(self):
        raise NotImplementedError()

    def test_mv_read_populated(self):
        raise NotImplementedError()

    def test_mv_read_not_populated(self):
        raise NotImplementedError()

    def test_mv_mixed_populated(self):
        raise NotImplementedError()

    def test_mv_mixed_not_populated(self):
        raise NotImplementedError()

    def test_uniform_counter_update_bench(self):
        raise NotImplementedError()

    def test_timeseries_bench(self):
        raise NotImplementedError()
