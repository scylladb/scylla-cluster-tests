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

import logging

from sdcm.tester import ClusterTester
from sdcm.utils import get_data_dir_path

logger = logging.getLogger(__name__)


class SstableCorruptTest(ClusterTester):

    """
    Testing that scylla cluster can get over an sstable corruption
    """

    def _run_stress(self, cmd, population_size):
        logger.debug('Runn stress %s' % cmd)
        ip = self.db_cluster.get_node_private_ips()[0]
        stress_cmd = "cassandra-stress {} n={} -pop seq=1..{} -node {} -port jmx=6868".format(cmd, population_size,
                                                                                              population_size, ip)
        self.run_stress(stress_cmd=stress_cmd)

    def corrupt_sstables(self):
        logger.debug('Corrupt sstables')
        src = get_data_dir_path('corrupt_sstables.sh')
        dst = '/tmp/corrupt_sstables.sh'
        node = self.db_cluster.nodes[0]
        node.remoter.send_files(src, dst)
        node.remoter.run('chmod +x {}'.format(dst))
        node.remoter.run('sudo {}'.format(dst))

    def test_sstable_corrupt(self):
        """
        1. Start a 1 node cluster
        2. Run cassandra-stress write on the loader node
        3. Flush sstables
        4. Corrupt sstables - flip some bits to number of sstable files
        5. Run cassandra-stress read
        6. Re-start the node
        7. Run cassandra-stress read - should not cause scylla crash
        """
        population_size = 1000000
        self._run_stress('write', population_size)

        logger.debug('Flush sstables')
        self.db_cluster.nodes[0].run('nodetool flush')

        self.corrupt_sstables()
        self._run_stress('read', population_size)

        logger.debug('Restart node')
        self.db_cluster.nodes[0].remoter.run('sudo systemctl restart scylla-server.service')
        self._run_stress('read', population_size)
