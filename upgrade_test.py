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


import random
import time

from avocado import main

from sdcm.nemesis import RollbackNemesis
from sdcm.nemesis import UpgradeNemesis
from sdcm.data_path import get_data_path
from sdcm.fill_db_data import FillDatabaseData


class UpgradeTest(FillDatabaseData):
    """
    Test a Scylla cluster upgrade.

    :avocado: enable
    """

    def upgrade_node(self, node):
        repo_file = self.db_cluster.params.get('repo_file', None,  'scylla.repo.upgrade')
        new_version = self.db_cluster.params.get('new_version', None,  '')
        upgrade_node_packages = self.db_cluster.params.get('upgrade_node_packages')
        self.log.info('Upgrading a Node')

        # We assume that if update_db_packages is not empty we install packages from there.
        # In this case we don't use upgrade based on repo_file(ignored sudo yum update scylla...)
        orig_ver = node.remoter.run('rpm -qa scylla-server')
        if upgrade_node_packages:
            # update_scylla_packages
            node.remoter.send_files(upgrade_node_packages, '/tmp/scylla', verbose=True)
            # node.remoter.run('sudo yum update -y --skip-broken', connect_timeout=900)
            node.remoter.run('sudo yum install python34-PyYAML -y')
            # replace the packages
            node.remoter.run('rpm -qa scylla\*')
            node.remoter.run('sudo nodetool snapshot')
            # update *development* packages
            node.remoter.run('sudo rpm -UvhR --oldpackage /tmp/scylla/*development*', ignore_status=True)
            # and all the rest
            node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/* | true')
            node.remoter.run('rpm -qa scylla\*')
        elif repo_file:
            if repo_file.startswith('http'):
                node.remoter.run('sudo curl -L {} -o /etc/yum.repos.d/scylla.repo')
            else:
                scylla_repo = get_data_path(repo_file)
                node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
                node.remoter.run('sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup')
                node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
            # backup the data
            node.remoter.run('sudo cp /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml-backup')
            node.remoter.run('sudo nodetool snapshot')
            node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
            node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
            node.remoter.run('sudo yum clean all')
            ver_suffix = '-{}'.format(new_version) if new_version else ''
            node.remoter.run('sudo yum install scylla{0} scylla-server{0} scylla-jmx{0} scylla-tools{0}'
                             ' scylla-conf{0} scylla-kernel-conf{0} scylla-debuginfo{0} -y'.format(ver_suffix))
        # flush all memtables to SSTables
        node.remoter.run('sudo nodetool drain')
        node.remoter.run('sudo systemctl restart scylla-server.service')
        node.wait_db_up(verbose=True)
        new_ver = node.remoter.run('rpm -qa scylla-server')
        if orig_ver == new_ver:
            self.log.error('scylla-server version isn\'t changed')

    default_params = {'timeout': 650000}

    def test_upgrade_cql_queries(self):
        """
        Run a set of different cql queries against various types/tables before
        and after upgrade of every node to check the consistency of data
        """
        self.log.info('Populate DB with many types of tables and data')
        self.fill_db_data()
        self.log.info('Run some Queries to verify data BEFORE UPGRADE')
        self.verify_db_data()

        self.log.info('Starting c-s write workload to pupulate 10M paritions')
        # YAML: stress_cmd: cassandra-stress write cl=QUORUM n=10000000 -schema 'replication(factor=3)' -port jmx=6868
        # -mode cql3 native -rate threads=1000 -pop seq=1..10000000
        stress_cmd = self._cs_add_node_flag(self.params.get('stress_cmd'))
        self.run_stress_thread(stress_cmd=stress_cmd)

        self.log.info('Sleeping for 360s to let cassandra-stress populate some data before the mixed workload')
        time.sleep(600)

        self.log.info('Starting c-s read workload for 60m')
        # YAML: stress_cmd_1: cassandra-stress read cl=QUORUM duration=60m -schema 'replication(factor=3)'
        # -port jmx=6868 -mode cql3 native -rate threads=100 -pop seq=1..10000000
        stress_cmd_1 = self._cs_add_node_flag(self.params.get('stress_cmd_1'))
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd_1)

        self.log.info('Sleeping for 300s to let cassandra-stress start before the upgrade...')
        time.sleep(300)

        nodes_num = len(self.db_cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(nodes_num)]
        # shuffle it so we will upgrade the nodes in a random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            self.db_cluster.node_to_upgrade = self.db_cluster.nodes[i]
            self.log.info('Upgrade Node begin')
            self.upgrade_node(self.db_cluster.node_to_upgrade)
            time.sleep(300)
            self.log.info('Upgrade Node ended')

        self.log.info('Run some Queries to verify data AFTER UPGRADE')
        self.verify_db_data()
        self.verify_stress_thread(stress_queue)

    def test_20_minutes(self):
        """
        Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
        If upgrade_node_packages defined we specify duration 10 * len(nodes) minutes.
        """
        self.db_cluster.add_nemesis(nemesis=UpgradeNemesis,
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors)
        self.db_cluster.start_nemesis(interval=10)
        duration = 20
        if self.params.get('upgrade_node_packages'):
            duration = 30 * len(self.db_cluster.nodes)
        self.run_stress(stress_cmd=self.params.get('stress_cmd'), duration=duration)

    def test_20_minutes_rollback(self):
        """
        Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
        """
        self.db_cluster.add_nemesis(nemesis=UpgradeNemesis,
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors)
        self.db_cluster.start_nemesis(interval=10)
        self.db_cluster.stop_nemesis(timeout=None)

        self.db_cluster.clean_nemesis()

        self.db_cluster.add_nemesis(nemesis=RollbackNemesis,
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors)
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(stress_cmd=self.params.get('stress_cmd'),
                        duration=self.params.get('cassandra_stress_duration', 20))


if __name__ == '__main__':
    main()
