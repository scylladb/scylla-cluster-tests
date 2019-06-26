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
import time
import json
import subprocess

from sdcm.tester import ClusterTester
from sdcm.utils import remote_get_file


class RefreshTest(ClusterTester):
    """
    Nodetool refresh after uploading lot of data to a cluster with running load in the background.
    :avocado: enable
    """
    # start timestamp to check read_timeout
    start = None
    # end timestamp to check read_timeout
    end = None

    def get_random_target(self):
        return random.choice(self.db_cluster.nodes)

    def get_current_timestamp(self):
        """
        Timestamp used for prometheus API
        """
        return str(time.time()).split('.')[0]

    def prepare_sstable(self, node):
        """
        Get backup of SSTables, uncompress it to upload directory
        """
        sstable_file = self.params.get('sstable_file')
        if self.params.get('skip_download', default='').lower() != 'true':
            remote_get_file(node.remoter,
                            self.params.get('sstable_url'),
                            self.params.get('sstable_file'),
                            hash_expected=self.params.get('sstable_md5'),
                            retries=2)

        # start to watch read_timeout
        self.start = self.get_current_timestamp()

        result = node.remoter.run("sudo ls -t /var/lib/scylla/data/keyspace1/")
        upload_dir = result.stdout.split()[0]
        # if tar takes too much system resource, we can limt by nice or cpulimit
        node.remoter.run('sudo tar xvfz {} -C /var/lib/scylla/data/keyspace1/{}/upload/'.format(sstable_file, upload_dir))

    def nodetool_refresh(self, node):
        """
        Load newly placed SSTables to the system
        """
        self.log.debug('Make sure keyspace1.standard1 exists')
        result = node.run_nodetool(sub_cmd="cfstats", args="keyspace1.standard1")
        assert result.exit_status == 0

        node.run_nodetool(sub_cmd="refresh", args="-- keyspace1 standard1")
        node.run_cqlsh("select * from keyspace1.standard1 where key=0x314e344b4d504d4b4b30")
        for i in range(int(self.params.get('flush_times', default=1))):
            time.sleep(int(self.params.get('flush_period', default=60)))
            node.run_nodetool("flush")

    def check_timeout(self):
        assert self.monitors.nodes, 'Monitor node should be set, we will try to get metrics from Prometheus server'
        cmd = 'curl http://%s:9090/api/v1/query_range?query=scylla_storage_proxy_coordinator_read_timeouts&start=%s&end=%s&step=60s' % (self.monitors.nodes[0].public_ip_address, self.start, self.end)
        self.log.debug('Get read timeout per minute by Prometheus API, cmd: %s', cmd)
        result = subprocess.check_output(cmd.split(), shell=True)

        orig_data = json.loads(result)
        read_timeout_msg = 'Read timeout of whole datacenter per minute should be less than 5000'
        self.log.debug('Check if we have significant read timeout, %s', read_timeout_msg)

        # parse prometheus response to generate a result matrix
        matrix = []
        for i in orig_data['data']['result']:
            shard_unit = []
            for j in i['values']:
                shard_unit.append(int(j[1]))
            matrix.append(shard_unit)

        # go through the matrix to check timeout per minute
        prev = None
        significant = []
        for time_idx in range(len(matrix[0])):
            all_timeout = 0
            for shard_unit in matrix:
                all_timeout += shard_unit[time_idx]
            if prev:
                timeout_per_min = all_timeout - prev
                self.log.debug('timeout_per_min: %s', timeout_per_min)
                if timeout_per_min > 5000:
                    significant.append(timeout_per_min)
            prev = all_timeout

        self.log.debug(significant)
        assert len(significant) == 0, read_timeout_msg

    def test_refresh_node(self):
        """
        Test steps:
        1. Prepare write workload
        2. Start mixed workload
        3. Get backup of SSTables, uncompress it to upload directory
        4. Load newly placed SSTables to the system
        5. Verify the SSTables are loaded by select
        6. Flush all SSTables
        7. Wait until the mixed workload finishes
        8. Check read_timeout during the refresh
        """
        node = self.get_random_target()
        # run a prepare write workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('prepare_stress_cmd'),
                                              stress_num=1,
                                              keyspace_num=1)
        self.get_stress_results(queue=stress_queue)
        # run a mixed workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        self.prepare_sstable(node)
        self.nodetool_refresh(node)
        self.get_stress_results(queue=stress_queue)

        # finish to watch read_timeout
        self.end = self.get_current_timestamp()

        self.check_timeout()
