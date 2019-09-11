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


from sdcm.tester import ClusterTester
from sdcm.utils.common import get_data_dir_path
from sdcm.nemesis import UpgradeNemesis
from sdcm.nemesis import log_time_elapsed


class PartialUpgradeDowngradeNemesis(UpgradeNemesis):

    # downgrade a single node
    def downgrade_node(self, node):
        self.log.info('Downgrading a Node')
        scylla_repo = get_data_dir_path('scylla.repo.downgrade')
        node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
        node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.update_repo_cache()
        node.remoter.run('sudo yum downgrade scylla scylla-conf scylla-server scylla-jmx scylla-tools -y')
        node.remoter.run('sudo systemctl restart scylla-server.service')
        node.wait_db_up(verbose=True)

    @log_time_elapsed
    def disrupt(self):
        self.log.info('PartialUpgradeDowngrade Nemesis begin')

        # Upgrade first node
        node = self.cluster.nodes[0]
        self.upgrade_node(node)

        # then downgrade it
        self.downgrade_node(node)

        self.log.info('PartialUpgradeDowngrade Nemesis end')


class PartialUpgradeDowngradeTest(ClusterTester):

    """
    Test a Scylla cluster upgrade.

    :avocado: enable
    """

    default_params = {'timeout': 650000}

    def test_20_minutes(self):
        """
        Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
        """
        self.db_cluster.add_nemesis(nemesis=PartialUpgradeDowngradeNemesis,
                                    tester_obj=self)
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(duration=20)
