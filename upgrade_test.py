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
    orig_ver = None
    new_ver = None

    def upgrade_node(self, node):
        new_scylla_repo = self.db_cluster.params.get('new_scylla_repo', None,  None)
        new_version = self.db_cluster.params.get('new_version', None,  '')
        upgrade_node_packages = self.db_cluster.params.get('upgrade_node_packages')
        upgrade_rollback_mode = self.db_cluster.params.get('upgrade_rollback_mode', default='major_release')
        self.log.info('Upgrading a Node')

        # We assume that if update_db_packages is not empty we install packages from there.
        # In this case we don't use upgrade based on new_scylla_repo(ignored sudo yum update scylla...)
        result = node.remoter.run('scylla --version')
        self.orig_ver = result.stdout

        if upgrade_node_packages:
            # update_scylla_packages
            node.remoter.send_files(upgrade_node_packages, '/tmp/scylla', verbose=True)
            # node.remoter.run('sudo yum update -y --skip-broken', connect_timeout=900)
            node.remoter.run('sudo yum install python34-PyYAML -y')
            # replace the packages
            node.remoter.run('rpm -qa scylla\*')
            # flush all memtables to SSTables
            node.remoter.run('sudo nodetool drain')
            node.remoter.run('sudo nodetool snapshot')
            node.remoter.run('sudo systemctl stop scylla-server.service')
            # update *development* packages
            node.remoter.run('sudo rpm -UvhR --oldpackage /tmp/scylla/*development*', ignore_status=True)
            # and all the rest
            node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/* | true')
            node.remoter.run('rpm -qa scylla\*')
        elif new_scylla_repo:
            assert new_scylla_repo.startswith('http')
            node.remoter.run('sudo curl -L {} -o /etc/yum.repos.d/scylla.repo'.format(new_scylla_repo))
            # backup the data
            node.remoter.run('sudo cp /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml-backup')
            # flush all memtables to SSTables
            node.remoter.run('sudo nodetool drain')
            node.remoter.run('sudo nodetool snapshot')
            node.remoter.run('sudo systemctl stop scylla-server.service')
            node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
            node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
            node.remoter.run('sudo yum clean all')
            ver_suffix = '-{}'.format(new_version) if new_version else ''
            if upgrade_rollback_mode == 'reinstall':
                node.remoter.run('sudo yum remove scylla\* -y')
                node.remoter.run('sudo yum install scylla{0} -y'.format(ver_suffix))
            else:
                node.remoter.run('sudo yum update scylla{0}\* -y'.format(ver_suffix))
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.wait_db_up(verbose=True)
        result = node.remoter.run('rpm -qa scylla\*server')
        new_ver = result.stdout
        assert self.orig_ver != self.new_ver, "scylla-server version isn't changed"
        self.new_ver = new_ver

    def rollback_node(self, node):
        self.log.info('Rollbacking a Node')
        upgrade_rollback_mode = self.db_cluster.params.get('upgrade_rollback_mode', default='major_release')
        new_introduced_pkgs = self.db_cluster.params.get('new_introduced_pkgs', default=None)
        result = node.remoter.run('scylla --version')
        orig_ver = result.stdout
        node.remoter.run('sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo')
        # flush all memtables to SSTables
        node.remoter.run('nodetool drain')
        # backup the data
        node.remoter.run('nodetool snapshot')
        node.remoter.run('sudo systemctl stop scylla-server.service')
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo yum clean all')
        if upgrade_rollback_mode == 'reinstall':
            node.remoter.run('sudo yum remove scylla\* -y')
            node.remoter.run('sudo yum install {} -y' % node.scylla_pkg())
        elif upgrade_rollback_mode == 'minor_release':
            node.remoter.run('sudo yum downgrade scylla\*%s -y' % self.orig_ver.split('-')[0])
        else:
            if new_introduced_pkgs:
                node.remoter.run('sudo yum remove %s -y' % new_introduced_pkgs)
            node.remoter.run('sudo yum downgrade scylla\* -y')
            if new_introduced_pkgs:
                node.remoter.run('sudo yum install {} -y' % node.scylla_pkg())

        node.remoter.run('sudo cp /etc/scylla/scylla.yaml-backup /etc/scylla/scylla.yaml')
        result = node.remoter.run('sudo find /var/lib/scylla/data/system')
        snapshot_name = re.findall("system/peers-[a-z0-9]+/snapshots/(\d+)\n", result.stdout)
        cmd = "DIR='/var/lib/scylla/data/system'; for i in `sudo ls $DIR`;do sudo test -e $DIR/$i/snapshots/%s && sudo find $DIR/$i/snapshots/%s -type f -exec sudo /bin/cp {} $DIR/$i/ \;; done" % (snapshot_name[0], snapshot_name[0])
        node.remoter.run(cmd, verbose=True)
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.wait_db_up(verbose=True)
        result = node.remoter.run('scylla --version')
        new_ver = result.stdout
        self.log.debug('original scylla-server version is %s, latest: %s' % (orig_ver, new_ver))
        assert orig_ver != new_ver, "scylla-server version isn't changed"

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
            self.log.info('Upgrade Node %s begin', self.db_cluster.node_to_upgrade.name)
            self.upgrade_node(self.db_cluster.node_to_upgrade)
            time.sleep(300)
            self.log.info('Upgrade Node %s ended', self.db_cluster.node_to_upgrade.name)

        self.log.info('Run some Queries to verify data AFTER UPGRADE')
        self.verify_db_data()
        self.verify_stress_thread(stress_queue)

    def test_rolling_upgrade(self):
        """
        Upgrade half of nodes in the cluster, and start special read workload
        during the stage. Checksum method is changed to xxhash from Scylla 2.2,
        we want to use this case to verify the read (cl=ALL) workload works
        well, upgrade all nodes to new version in the end.
        """

        # generate random order to upgrade
        nodes_num = len(self.db_cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(nodes_num)]
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        ### prepare write workload
        self.log.info('Starting c-s prepare write workload (n=10000000)')
        prepare_write_stress = self.params.get('prepare_write_stress')
        prepare_write_stress_queue = self.run_stress_thread(stress_cmd=prepare_write_stress)

        self.log.info('Sleeping for 60s to let cassandra-stress start before the upgrade...')
        time.sleep(60)

        # upgrade first node
        self.db_cluster.node_to_upgrade = self.db_cluster.nodes[indexes[0]]
        self.log.info('Upgrade Node %s begin', self.db_cluster.node_to_upgrade.name)
        self.upgrade_node(self.db_cluster.node_to_upgrade)
        self.log.info('Upgrade Node %s ended', self.db_cluster.node_to_upgrade.name)
        self.db_cluster.node_to_upgrade.remoter.run("nodetool status")

        # wait for the prepare write workload to finish
        self.verify_stress_thread(prepare_write_stress_queue)

        ### read workload
        self.log.info('Starting c-s read workload for 10m')
        stress_cmd_read_10m = self.params.get('stress_cmd_read_10m')
        read_10m_stress_queue = self.run_stress_thread(stress_cmd=stress_cmd_read_10m)

        self.log.info('Sleeping for 60s to let cassandra-stress start before the upgrade...')
        time.sleep(60)

        # upgrade second node
        self.db_cluster.node_to_upgrade = self.db_cluster.nodes[indexes[1]]
        self.log.info('Upgrade Node %s begin', self.db_cluster.node_to_upgrade.name)
        self.upgrade_node(self.db_cluster.node_to_upgrade)
        self.log.info('Upgrade Node %s ended', self.db_cluster.node_to_upgrade.name)
        self.db_cluster.node_to_upgrade.remoter.run("nodetool status")

        # wiat for the 10m read workload to finish
        self.verify_stress_thread(read_10m_stress_queue)

        ### read workload (cl=ALL)
        self.log.info('Starting c-s read workload (cl=ALL n=10000000)')
        stress_cmd_read_clall = self.params.get('stress_cmd_read_clall')
        read_clall_stress_queue = self.run_stress_thread(stress_cmd=stress_cmd_read_clall)
        # wiat for the cl=ALL read workload to finish
        self.verify_stress_thread(read_clall_stress_queue)

        ### read workload (20m)
        self.log.info('Starting c-s read workload for 20m')
        stress_cmd_read_20m = self.params.get('stress_cmd_read_20m')
        read_20m_stress_queue = self.run_stress_thread(stress_cmd=stress_cmd_read_20m)

        # rollback second node
        self.log.info('Rollback Node %s begin', self.db_cluster.nodes[indexes[1]].name)
        self.rollback_node(self.db_cluster.nodes[indexes[1]])
        self.log.info('Rollback Node %s ended', self.db_cluster.nodes[indexes[1]].name)
        self.db_cluster.nodes[indexes[1]].remoter.run("nodetool status")

        for idx in indexs[1:]:
            self.db_cluster.node_to_upgrade = self.db_cluster.nodes[indexes[i]]
            self.log.info('Upgrade Node %s begin', self.db_cluster.node_to_upgrade.name)
            self.upgrade_node(self.db_cluster.node_to_upgrade)
            self.log.info('Upgrade Node %s ended', self.db_cluster.node_to_upgrade.name)
            self.db_cluster.node_to_upgrade.remoter.run("nodetool status")

        # wait for the 20m read workload to finish
        self.verify_stress_thread(read_20m_stress_queue)


if __name__ == '__main__':
    main()
