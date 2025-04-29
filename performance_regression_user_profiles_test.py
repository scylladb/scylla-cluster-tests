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

import os

from sdcm.tester import ClusterTester


class PerformanceRegressionUserProfilesTest(ClusterTester):
    """
    Test Scylla performance regression with cassandra-stress using custom user profiles.
    """

    def setUp(self):
        super().setUp()
        self.create_stats = False

    def _clean_keyspace(self, cs_profile):
        with open(cs_profile, encoding="utf-8") as fdr:
            key_space = [line.split(':')[-1].strip() for line in fdr.readlines() if line.startswith('keyspace:')]
        if key_space:
            self.log.debug('Drop keyspace {}'.format(key_space[0]))
            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:

                session.execute('DROP KEYSPACE IF EXISTS {};'.format(key_space[0]))

    def test_user_profiles(self):
        """
        Run workload using user profiles
        """
        duration = self.params.get('cs_duration')
        user_profiles = self.params.get('cs_user_profiles')
        assert user_profiles is not None, 'No user profiles defined!'
        for cs_profile in user_profiles:
            assert os.path.exists(cs_profile), 'File not found: {}'.format(cs_profile)
            self.log.debug('Run stress test with user profile {}, duration {}'.format(cs_profile, duration))
            profile_dst = os.path.join('/tmp', os.path.basename(cs_profile))
            with open(cs_profile, encoding="utf-8") as pconf:
                cont = pconf.readlines()
                for cmd in [line.lstrip('#').strip() for line in cont if line.find('cassandra-stress') > 0]:
                    stress_cmd = (cmd.format(profile_dst, duration))
                    self.log.debug('Stress cmd: {}'.format(stress_cmd))
                    self.create_test_stats()
                    stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=2, profile=cs_profile)
                    self.get_stress_results(queue=stress_queue)
                    self.update_test_details(scylla_conf=True)
                    self.check_regression()
                    self._clean_keyspace(cs_profile)
