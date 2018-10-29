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

import os
import re
import time
from avocado import main

from sdcm.nemesis import MgmtRepair
from sdcm.tester import ClusterTester


class MgmtCliTest(ClusterTester):
    """
    Test Scylla Manager operations on Scylla cluster.

    :avocado: enable
    """


    def test_mgmt_repair_nemesis(self):
        self.log.info('Starting c-s write workload for 1m')
        stress_cmd = self.params.get('stress_cmd')
        stress_cmd_queue = self.run_stress_thread(stress_cmd=stress_cmd)

        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)
        self.log.debug("test_mgmt_cli: initialize MgmtRepair nemesis")
        self.db_cluster.add_nemesis(nemesis=MgmtRepair,
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors,
                                    )
        self.db_cluster.start_nemesis()


if __name__ == '__main__':
    main()
