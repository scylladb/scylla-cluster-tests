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

import logging

from sdcm.tester import ClusterTester
from sdcm.tester import teardown_on_exception


class QueryLimitsTest(ClusterTester):

    """
    Test scylla cluster growth (adding nodes after an initial cluster size).
    """

    @teardown_on_exception
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None

        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)

        # we will give a very slow disk to the db node
        # so the loader node will easilly saturate it
        bdm = [{"DeviceName": "/dev/sda1",
                "Ebs": {"Iops": 100,
                        "VolumeType": "io1",
                        "DeleteOnTermination": True}}]
        loader_info = {'n_nodes': 1, 'device_mappings': None,
                       'type': 'm4.4xlarge'}
        db_info = {'n_nodes': 1, 'device_mappings': bdm,
                   'type': 'm4.4xlarge'}
        monitor_info = {'n_nodes': 1, 'device_mappings': None,
                        'type': 't3.small'}
        # Use big instance to be not throttled by the network
        self.init_resources(loader_info=loader_info, db_info=db_info,
                            monitor_info=monitor_info)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        nodes_monitored = [node.private_ip_address for node in self.db_cluster.nodes]
        nodes_monitored += [node.private_ip_address for node in self.loaders.nodes]
        self.monitors.wait_for_init(targets=nodes_monitored)
        self.stress_thread = None

        self.payload = "/tmp/payload"

        # Stop scylla-server
        self.db_cluster.run("sudo systemctl stop scylla-server.service")
        # Restrict the amount of memory available to scylla to 1024M
        self.db_cluster.run("grep -v SCYLLA_ARGS /etc/sysconfig/scylla-server > /tmp/l")
        self.db_cluster.run('echo \'SCYLLA_ARGS="-m 1024M"\' >> /tmp/l')
        self.db_cluster.run("sudo cp /tmp/l /etc/sysconfig/scylla-server")
        self.db_cluster.run("sudo chown root.root /etc/sysconfig/scylla-server")
        # Configure seastar IO to use the smallest acceptable values (slow disk)
        self.db_cluster.run('echo \'SEASTAR_IO="--max-io-requests 4"\' > /tmp/m')
        self.db_cluster.run("sudo mv /tmp/m /etc/scylla.d/io.conf")
        self.db_cluster.run("sudo chown root.root /etc/scylla.d/io.conf")
        # Start scylla-server
        self.db_cluster.run("sudo systemctl start scylla-server.service")
        # Install boost-program-options and libuv
        self.loaders.run("sudo yum install -y boost-program-options")
        self.loaders.run("sudo yum install -y libuv")
        # Copy payload to loader nodes
        self.loaders.send_file("queries-limits", self.payload)
        self.loaders.run("chmod +x " + self.payload)

    def test_connection_limits(self):
        ips = self.db_cluster.get_node_private_ips()
        params = " --servers %s --duration 600 --queries 1000000" % (ips[0])
        cmd = '%s %s' % (self.payload, params)
        result = self.loaders.nodes[0].remoter.run(cmd, ignore_status=True)
        if result.exit_status != 0:
            self.fail('Payload failed:\n%s' % result)
