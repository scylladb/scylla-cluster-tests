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

import random

from avocado import main
from sdcm.tester import ClusterTester
from sdcm.utils import remote_get_file


class RefreshTest(ClusterTester):
    """
    Nodetool refresh after uploading lot of data to a cluster with running load in the background.
    :avocado: enable
    """
    def get_random_target(self):
        return random.choice(self.db_cluster.nodes)

    def prepare_sstable(self, node):
        self.run_stress_thread(stress_cmd=self.params.get('prepare_stress_cmd'),
                               stress_num=1,
                               keyspace_num=1)
        sstable_file = self.params.get('sstable_file')
        if self.params.get('skip_download', default='').lower() != 'true':
            remote_get_file(node.remoter,
                            self.params.get('sstable_url'),
                            self.params.get('sstable_file'),
                            hash_expected=self.params.get('sstable_md5'),
                            retries=2)
        result = node.remoter.run("sudo ls -t /var/lib/scylla/data/keyspace1/")
        upload_dir = result.stdout.split()[0]
        node.remoter.run('sudo tar xvfz {} -C /var/lib/scylla/data/keyspace1/{}/upload/'.format(sstable_file, upload_dir))

    def nodetool_refresh(self, node):

        self.log.debug('Make sure keyspace1.standard1 exists')
        result = node.remoter.run('nodetool --host localhost cfstats keyspace1.standard1')
        if result.exit_status == 0:
            node.remoter.run('nodetool --host localhost refresh -- keyspace1 standard1')
            cmd = "select * from keyspace1.standard1 where key=0x314e344b4d504d4b4b30"
            node.remoter.run('cqlsh -e "{}" {}'.format(cmd, node.private_ip_address), verbose=True)

    def test_refresh_node(self):
        node = self.get_random_target()
        self.prepare_sstable(node)
        # run a write workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        self.nodetool_refresh(node)

        self.get_stress_results(queue=stress_queue, stress_num=2, keyspace_num=1)


if __name__ == '__main__':
    main()
