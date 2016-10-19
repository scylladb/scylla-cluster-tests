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


class LongevityTest(ClusterTester):

    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """

    default_params = {'timeout': 650000}

    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    monitoring_set=self.monitors)
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.verify_stress_thread(queue=stress_queue)


if __name__ == '__main__':
    main()
