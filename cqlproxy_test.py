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
# Copyright (c) 2018 ScyllaDB

import os
import re
import time
from avocado import main

from sdcm.tester import ClusterTester

#proxy listen port

"""
install proxy in Cassandra cluster's seed node
cql client needs to use non-default port: append `-port 3042` to cassandra-stress cmdline
switch Scylla to master

"""

class CqlProxyTest(ClusterTester):
    """
    Test CqlProxy

    :avocado: enable
    """

    def _build(self):
        # git clone request permission
        proxy.remoter.run('git clone https://github.com/scylladb/cqlproxy')
        proxy.remoter.run('cd cqlproxy/dist; make release/rpm')
        result = proxy.remoter.run('find ./release/rpm/|grep rpm$')
        return result.stdout.strip()

    def setUp(self):
        self.scylla_seed = self.db_cluster.nodes[0]
        self.cassandra_seed = self.cs_db_cluster.nodes[0]
        self.proxy = cassandra_seed

        #rpm_path = _build()
        rpm_path = 'https://s3.amazonaws.com/scylla-qa-team/cqlproxy-latest.x86_64.rpm'
        proxy.remoter.run('sudo yum install -y %s' % rpm_path)

        proxy.remoter.run("echo 'config_file: /etc/cassandra/conf/cassandra.yaml\nreplica_address: {}:9042\nlisten_address: {}:3042\n' > /tmp/cqlproxy.yaml ".format(
                          self.scylla_seed.private_ip_address, self.proxy.private_ip_address))
        proxy.remoter.run('sudo mv /tmp/cqlproxy.yaml /etc/cqlproxy/cqlproxy.yaml')
        proxy.remoter.run('sudo systemctl restart cqlproxy')
        proxy.remoter.run('sudo systemctl status cqlproxy')
        proxy.remoter.run('sudo systemctl status cqlproxy_shadow')

    def test_basic_cs(self):
        # -port native=3042 -node proxy.ip
        base_cmd = self.params.get('stress_cmd')
        base_cmd += ' -port native=9042 -node %s' % self.cassandra_seed.private_ip_address
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd, stress_num=2)
        self.verify_stress_thread(stress_queue)

        base_cmd = self.params.get('stress_cmd')
        base_cmd += ' -port native=3042 -node %s' % self.proxy.private_ip_address
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd, stress_num=2)
        self.verify_stress_thread(stress_queue)

    def test_stop_wait_start(self):
        base_cmd = self.params.get('stress_cmd')
        # run a workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd, stress_num=2)

        node = self.db_cluster.nodes[-1]
        node.remoter.run('sudo systemctl stop scylla-server.service')
        node.wait_db_down()
        time.sleep(30)
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.wait_db_up()

        self.verify_stress_thread(stress_queue)

    def test_restart(self):
        base_cmd = self.params.get('stress_cmd')
        last_node = self.db_cluster.nodes[-1]
        last_node.restart()
        # run a workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd, stress_num=2)
        self.verify_stress_thread(stress_queue)


if __name__ == '__main__':
    main()
