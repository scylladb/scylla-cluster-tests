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

import copy
import logging

from performance_regression_test import PerformanceRegressionTest
from sdcm.utils.common import ParallelObject


# pylint: disable=super-init-not-called,consider-using-enumerate
# pylint: disable=too-many-instance-attributes,too-many-arguments
class ScyllaClusterStats(PerformanceRegressionTest):
    def __init__(self, db_cluster, loaders, monitors, prometheus_db, params, test_config,
                 cluster_index):
        self.db_cluster = db_cluster
        self.loaders = loaders
        self.monitors = monitors
        self.prometheus_db = prometheus_db
        self.params = copy.deepcopy(params)
        self.log = logging.getLogger(self.__class__.__name__)
        self._es_doc_type = "test_stats"
        self._stats = self._init_stats()
        self.test_config = test_config
        self._duration = self.params.get(key='test_duration')
        self.create_stats = self.params.get(key='store_perf_results')
        self.status = "RUNNING"
        self.cluster_index = str(cluster_index)
        self._test_id = self.test_config.test_id() + f"--{cluster_index}"
        self._test_index = self.get_str_index()
        self.create_test_stats()
        self.start_time = self.get_test_start_time()

    def get_str_index(self):
        return f"k8s-perf-{self.db_cluster.k8s_cluster.tenants_number}-tenants"

    def id(self):  # pylint: disable=invalid-name
        return self._test_index

    def __str__(self) -> str:
        return self._test_index + f"--{self.cluster_index}"

    def __repr__(self) -> str:
        return self.__str__()


# pylint: disable=too-many-public-methods
class PerformanceRegressionOperatorMultiTenantTest(PerformanceRegressionTest):
    scylla_clusters_stats = []
    load_iteration_timeout_sec = 7200

    def create_test_stats(self, *args, **kwargs):  # pylint: disable=unused-argument
        self.log.info(
            "Suppress the test class stats creation. "
            "Leave it for the per-DB classes.")
        self._stats = self._init_stats()

    def update_test_with_errors(self):
        self.log.info("update_test_with_errors: Suppress writing errors to ES")

    def setUp(self):
        super().setUp()
        for i in range(len(self.db_clusters_multitenant)):  # pylint: disable=no-member
            self.scylla_clusters_stats.append(ScyllaClusterStats(
                db_cluster=self.db_clusters_multitenant[i],  # pylint: disable=no-member
                loaders=self.loaders_multitenant[i],  # pylint: disable=no-member
                monitors=self.monitors_multitenant[i],  # pylint: disable=no-member
                prometheus_db=self.prometheus_db_multitenant[i],  # pylint: disable=no-member
                params=self.params,
                test_config=self.test_config,
                cluster_index=i + 1,
            ))
            for stress_cmd_param in self.params.stress_cmd_params:
                current_stress_cmd = self.params.get(stress_cmd_param)
                if not isinstance(current_stress_cmd, list):
                    continue
                # NOTE: 'prepare_write_cmd' is allowed to be list of strs
                if stress_cmd_param == 'prepare_write_cmd':
                    if not all((isinstance(current_stress_cmd_element, list)
                                for current_stress_cmd_element in current_stress_cmd)):
                        continue
                self.scylla_clusters_stats[i].params[stress_cmd_param] = current_stress_cmd[i]

    def run_fstrim_on_all_db_nodes(self):
        for db_cluster in self.db_clusters_multitenant:  # pylint: disable=no-member
            self.log.info("Running fstrim command on the '%s' DB cluster", db_cluster.name)
            db_cluster.fstrim_scylla_disks_on_nodes()

    def wait_no_compactions_running(self, *args, **kwargs):
        for scylla_cluster_stats in self.scylla_clusters_stats:
            self.log.info("Waiting for compactions to finish on the '%s' DB cluster",
                          scylla_cluster_stats.db_cluster.name)
            scylla_cluster_stats.wait_no_compactions_running(*args, **kwargs)

    def preload_data(self, compaction_strategy=None):
        def _preload_data(scylla_cluster_stats):
            prepare_write_cmd = scylla_cluster_stats.params.get('prepare_write_cmd')
            db_cluster_name = scylla_cluster_stats.db_cluster.name
            if not prepare_write_cmd:
                self.log.warning(
                    "No prepare command defined in YAML for the '%s' cluster",
                    db_cluster_name)
                return
            self.log.info("Running preload command for the '%s' cluster", db_cluster_name)
            scylla_cluster_stats.create_test_stats(
                sub_type='write-prepare', doc_id_with_timestamp=True)
            stress_queue, params = [], {
                'prefix': 'preload-',
            }
            if self.params.get('round_robin'):
                self.log.debug(
                    "'%s' DB cluster: Populating data using round_robin", db_cluster_name)
                params.update({'stress_num': 1, 'round_robin': True})
            for stress_cmd in prepare_write_cmd:
                params.update({'stress_cmd': stress_cmd})
                # Run all stress commands
                params.update(dict(stats_aggregate_cmds=False))
                self.log.debug("'%s' DB cluster: RUNNING stress cmd: %s",
                               db_cluster_name, stress_cmd)
                stress_queue.append(scylla_cluster_stats.run_stress_thread(**params))
            for stress in stress_queue:
                scylla_cluster_stats.get_stress_results(queue=stress, store_results=False)

            scylla_cluster_stats.update_test_details()

        self.log.info("Running preload operation in parallel on all the DB clusters")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[scs] for scs in self.scylla_clusters_stats],
            num_workers=len(self.scylla_clusters_stats),
        )
        object_set.run(func=_preload_data, unpack_objects=True, ignore_exceptions=False)

    def run_read_workload(self, nemesis=False):
        def _run_read_workload(scylla_cluster_stats, nemesis):
            scylla_cluster_stats.run_read_workload(nemesis=nemesis)

        self.log.info("Running 'read' workload operation in parallel")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[scs, nemesis] for scs in self.scylla_clusters_stats],
            num_workers=len(self.scylla_clusters_stats),
        )
        object_set.run(func=_run_read_workload, unpack_objects=True, ignore_exceptions=False)

    def run_write_workload(self, nemesis=False):
        def _run_write_workload(scylla_cluster_stats, nemesis):
            scylla_cluster_stats.run_write_workload(nemesis=nemesis)

        self.log.info("Running 'write' workload operation in parallel")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[scs, nemesis] for scs in self.scylla_clusters_stats],
            num_workers=len(self.scylla_clusters_stats),
        )
        object_set.run(func=_run_write_workload, unpack_objects=True, ignore_exceptions=False)

    def run_mixed_workload(self, nemesis: bool = False):
        def _run_mixed_workload(scylla_cluster_stats, nemesis):
            scylla_cluster_stats.run_mixed_workload(nemesis=nemesis)

        self.log.info("Running 'mixed' workload operation in parallel")
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[scs, nemesis] for scs in self.scylla_clusters_stats],
            num_workers=len(self.scylla_clusters_stats),
        )
        object_set.run(func=_run_mixed_workload, unpack_objects=True, ignore_exceptions=False)

    def run_workload(self, stress_cmd, nemesis=False, sub_type=None):
        def _run_workload(scylla_cluster_stats, stress_cmd, nemesis):
            scylla_cluster_stats.run_workload(stress_cmd=stress_cmd, nemesis=nemesis)

        self.log.info("Running workload in parallel with following command:\n%s", stress_cmd)
        object_set = ParallelObject(
            timeout=self.load_iteration_timeout_sec,
            objects=[[scs, stress_cmd, nemesis] for scs in self.scylla_clusters_stats],
            num_workers=len(self.scylla_clusters_stats),
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
