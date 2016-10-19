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
# Copyright (c) 2016 ScyllaDB


from avocado import main

from sdcm.tester import ClusterTester

from sdcm.nemesis import UpgradeNemesis
from sdcm.nemesis import RollbackNemesis


class UpgradeTest(ClusterTester):

    """
    Test a Scylla cluster upgrade.

    :avocado: enable
    """

    default_params = {'timeout': 650000}

    def test_20_minutes(self):
        """
        Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
        """
        self.db_cluster.add_nemesis(nemesis=UpgradeNemesis,
                                    monitoring_set=self.monitors)
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(stress_cmd=self.params.get('stress_cmd'), duration=20)

    def test_20_minutes_rollback(self):
        """
        Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
        """
        self.db_cluster.add_nemesis(nemesis=UpgradeNemesis,
                                    monitoring_set=self.monitors)
        self.db_cluster.start_nemesis(interval=10)
        self.db_cluster.stop_nemesis(timeout=None)

        self.db_cluster.clean_nemesis()

        self.db_cluster.add_nemesis(nemesis=RollbackNemesis,
                                    monitoring_set=self.monitors)
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(stress_cmd=self.params.get('stress_cmd'),
                        duration=self.params.get('cassandra_stress_duration', 20))

if __name__ == '__main__':
    main()
