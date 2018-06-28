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
from avocado import main
from sdcm.tester import ClusterTester


class PerformanceRegressionUserProfilesTest(ClusterTester):
    """
    Test Scylla performance regression with cassandra-stress using custom user profiles.

    :avocado: enable
    """
    def __init__(self, *args, **kwargs):
        super(PerformanceRegressionUserProfilesTest, self).__init__(*args, **kwargs)
        self.create_stats = False

    def _clean_keyspace(self, cs_profile):
        with open(cs_profile) as fdr:
            ks = [line.split(':')[-1].strip() for line in fdr.readlines() if line.startswith('keyspace:')]
        if ks:
            self.log.debug('Drop keyspace {}'.format(ks[0]))
            session = self.cql_connection_patient(self.db_cluster.nodes[0])
            session.execute('DROP KEYSPACE IF EXISTS {};'.format(ks[0]))

    def test_user_profiles(self):
        """
        Run workload using user profiles
        """
        duration = self.params.get('cs_duration', default='50m')
        user_profiles = self.params.get('cs_user_profiles')
        assert user_profiles is not None, 'No user profiles defined!'
        for cs_profile in user_profiles.split():
            assert os.path.exists(cs_profile), 'File not found: {}'.format(cs_profile)
            self.log.debug('Run stress test with user profile {}, duration {}'.format(cs_profile, duration))
            profile_dst = os.path.join('/tmp', os.path.basename(cs_profile))
            with open(cs_profile) as pconf:
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


if __name__ == '__main__':
    main()
