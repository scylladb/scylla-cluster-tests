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
from sdcm.mgmt import HostStatus
from sdcm.nemesis import MgmtRepair
from sdcm.tester import ClusterTester


class MgmtCliTest(ClusterTester):
    """
    Test Scylla Manager operations on Scylla cluster.

    :avocado: enable
    """


    def test_mgmt_repair_nemesis(self):
        """

            Test steps:
            1) Run cassandra stress on cluster.
            2) Add cluster to Manager and run full repair via Nemesis
        """
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
        """

        Test steps:
        1) add a cluster to manager.
        2) update the cluster attributes in manager: name/host/ssh-user
        3) delete the cluster from manager and re-add again.
        """

        manager_tool = mgmt.ScyllaManagerTool(manager_node=self.monitors.nodes[0])
        hosts = self._get_cluster_hosts_ip()
        selected_host = hosts[0]
        cluster_name = 'mgr_cluster1'
        mgr_cluster = manager_tool.add_cluster(name=cluster_name, host=selected_host)

        # Test cluster attributes
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name+"_renamed", "Cluster name wasn't changed after update command"

        # the below test currently fails: https://github.com/scylladb/mermaid/issues/741
        # new_ssh_user="super-scylla-manager"
        # mgr_cluster.update(ssh_user=new_ssh_user)
        # assert mgr_cluster.ssh_user == new_ssh_user, "Cluster ssh-user wasn't changed after update command"

        if len(hosts) > 1:
            mgr_cluster.update(host=hosts[1])
            assert mgr_cluster.host == hosts[1], "Cluster host wasn't changed after update command"
        mgr_cluster.delete()
        mgr_cluster2 = manager_tool.add_cluster(name=cluster_name, host=selected_host)

    def _get_cluster_hosts_ip(self):
        return [node_data[1] for node_data in self._get_cluster_hosts_with_ips()]

    def _get_cluster_hosts_with_ips(self):
        ip_addr_attr = 'public_ip_address' if self.params.get('cluster_backend') != 'gce' and \
                                              len(self.db_cluster.datacenter) > 1 else 'private_ip_address'
        return [[n, getattr(n, ip_addr_attr)] for n in self.db_cluster.nodes]

    def test_manager_sanity(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run test_mgmt_cluster test.
        :return:
        """

        self.test_mgmt_repair_nemesis()
        self.test_mgmt_cluster_crud()

    def test_mgmt_cluster_healthcheck(self):

        manager_tool = mgmt.ScyllaManagerTool(manager_node=self.monitors.nodes[0])
        selected_host_ip = self._get_cluster_hosts_ip()[0]
        mgr_cluster = manager_tool.add_cluster(name='mgr_cluster1', host=selected_host_ip)
        other_host, other_host_ip = [host_data for host_data in self._get_cluster_hosts_with_ips() if host_data[1] != selected_host_ip][0]

        sleep = 40
        self.log.debug('Sleep {} seconds, waiting for health-check task to run by schedule on first time'.format(sleep))
        time.sleep(sleep)

        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.status == HostStatus.UP , "Not all hosts status is 'UP'"

        # Check for sctool status change after scylla-server down
        other_host.stop_scylla_server()
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check next run is: {}".format(healthcheck_task.next_run))
        self.log.debug('Sleep {} seconds, waiting for health-check task to run after node down'.format(sleep))
        time.sleep(sleep)

        dict_host_health = mgr_cluster.get_hosts_health()
        assert dict_host_health[other_host_ip].status == HostStatus.DOWN , "Host: {} status is not 'DOWN'".format(other_host_ip)



if __name__ == '__main__':
    main()
