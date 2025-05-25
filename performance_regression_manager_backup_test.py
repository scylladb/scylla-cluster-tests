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
# Copyright (c) 2025 ScyllaDB


from performance_regression_test import PerformanceRegressionTest
from sdcm.mgmt.operations import ManagerTestFunctionsMixIn


class PerformanceRegressionManagerBackupTest(PerformanceRegressionTest, ManagerTestFunctionsMixIn):
    """
    Test Scylla Manager Backup performance regression.
    It assumes a relevant Scylla Manager configuration is set up in the YAML file.
    And specifically, a Scylla Manager backup Nemesis
    """

    def test_manager_backup(self):
        keyspace = 'keyspace1'
        table = 'standard1'
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()
        self.align_cluster_data_state(keyspace, table)
        self.run_workload(stress_cmd=self.params.get('stress_cmd_m'), nemesis=True, sub_type='mixed')
        self.align_cluster_data_state(keyspace, table, clear_snapshots=False)
        self.db_cluster.start_nemesis(interval=1, cycles_count=1)
        for nemesis_thread in self.db_cluster.nemesis_threads:
            nemesis_thread.join()
