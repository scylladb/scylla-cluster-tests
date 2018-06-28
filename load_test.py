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
# Copyright (c) 2017 ScyllaDB

from avocado import main
from sdcm.tester import ClusterTester


class LoadTest(ClusterTester):

    """
    Test a Scylla cluster load over a time period.

    :avocado: enable
    """

    def test_load(self):
        """
        Run cassandra-stress with params defined in tests/load_test.yaml
        """

        for stress_cmd in self.params.get('stress_cmd'):
            self.log.debug('stress cmd: {}'.format(stress_cmd))
            write_queue = self.run_stress_thread(stress_cmd=stress_cmd)
            self.verify_stress_thread(queue=write_queue)

        stress_queue = list()
        for stress_cmd in self.params.get('stress_read_cmd'):
            self.log.debug('stress read cmd: {}'.format(stress_cmd))
            stress_queue.append(self.run_stress_thread(stress_cmd=stress_cmd, stress_num=2))

        for stress in stress_queue:
            self.verify_stress_thread(queue=stress)


if __name__ == '__main__':
    main()
