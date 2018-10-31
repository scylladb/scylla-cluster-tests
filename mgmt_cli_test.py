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

from sdcm import mgmt
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

    def test_mgmt_cluster_crud(self):

        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.ScyllaManagerTool(manager_node=manager_node)

        cluster_name = 'mgr_cluster1'
        ip_addr_attr = 'public_ip_address' if self.params.get('cluster_backend') != 'gce' and \
                                              len(self.db_cluster.datacenter) > 1 else 'private_ip_address'
        hosts = [getattr(n, ip_addr_attr) for n in self.db_cluster.nodes]
        selected_host = hosts[0]

        mgr_cluster = manager_tool.add_cluster(name=cluster_name, host=selected_host)

        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name+"_renamed", "Cluster name wasn't changed after update command"

        # the below test currently fails and under clarification
        # new_ssh_user="super-scylla-manager"
        # mgr_cluster.update(ssh_user=new_ssh_user)
        # assert mgr_cluster.ssh_user == new_ssh_user, "Cluster ssh-user wasn't changed after update command"

        if len(hosts) > 1:
            mgr_cluster.update(host=hosts[1])
            assert mgr_cluster.host == hosts[1], "Cluster host wasn't changed after update command"
        mgr_cluster.delete()
        mgr_cluster2 = manager_tool.add_cluster(name=cluster_name, host=selected_host)



if __name__ == '__main__':
    main()
