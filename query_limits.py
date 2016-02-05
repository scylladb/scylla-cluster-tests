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
from sdcm.tester import clean_aws_resources


class QueryLimitsTest(ClusterTester):

    """
    Test scylla cluster growth (adding nodes after an initial cluster size).

    :avocado: enable
    """

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None

        # we will give a very slow disk to the db node
        # so the loader node will easilly saturate it
        bdm = [{"DeviceName": "/dev/sda1",
                "Ebs": {"Iops": 100,
                        "VolumeType": "io1",
                        "DeleteOnTermination": True}}]

        # Use big instance to be not throttled by the network
        self.init_resources(n_db_nodes=1, n_loader_nodes=1,
                            dbs_block_device_mappings=bdm,
                            dbs_type='m4.4xlarge',
                            loaders_type='m4.4xlarge')
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        self.stress_thread = None

        self.payload = "/tmp/payload"

        self.db_cluster.run("grep -v SCYLLA_ARGS /etc/sysconfig/scylla-server > /tmp/l")
        self.db_cluster.run("""echo "SCYLLA_ARGS=\"-m 128M -c 1\"" >> /tmp/l""")
        self.db_cluster.run("sudo cp /tmp/l /etc/sysconfig/scylla-server")
        self.db_cluster.run("sudo chown root.root /etc/sysconfig/scylla-server")
        self.db_cluster.run("sudo systemctl stop scylla-server.service")
        self.db_cluster.run("sudo systemctl start scylla-server.service")

        self.loaders.run("sudo dnf install -y boost-program-options")
        self.loaders.run("sudo dnf install -y libuv")
        self.loaders.send_file("queries-limits", self.payload)
        self.loaders.run("chmod +x " + self.payload)

    def test_connection_limits(self):
        ips = self.db_cluster.get_node_private_ips()
        params = " --servers %s --duration 600 --queries 1000000" % (ips[0])
        self.run_stress(stress_cmd=(self.payload + params), duration=10)


if __name__ == '__main__':
    main()
