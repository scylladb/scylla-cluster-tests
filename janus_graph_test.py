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
from performance_regression_test import PerformanceRegressionTest


class JanusGraphTest(PerformanceRegressionTest):
    """
    Run Janus Graph tests against scylla as backend storage

    :avocado: enable
    """

    # https://github.com/scylladb/scylla/issues/2497
    def test_janus_graph_regression(self):
        """
        Test steps:

        1. git clone https://github.com/JanusGraph/janusgraph.git && cd janusgraph
        2. mvn clean install -DskipTests -pl janusgraph-cql -am
        3. mvn verify -pl janusgraph-cql -Dstorage.hostname=DB_IP -Dtest=CQLGraphTest
        """
        self.loaders.nodes[0].remoter.run('sudo yum install -y git maven')
        self.loaders.nodes[0].remoter.run('git clone https://github.com/JanusGraph/janusgraph.git')
        self.loaders.nodes[0].remoter.run('cd janusgraph && mvn clean install -DskipTests -pl janusgraph-cql -am')
        self.loaders.nodes[0].remoter.run(
            'cd janusgraph && mvn -X -e verify -pl janusgraph-cql -Dstorage.hostname={0} -Dtest=CQLGraphTest'.format(
                self.db_cluster.nodes[0].private_ip_address), ignore_status=True)
        dest = os.path.join(self.logdir, 'janusgraph-cql')
        os.mkdir(dest)
        self.loaders.nodes[0].remoter.receive_files(src='~/janusgraph/janusgraph-cql/target/',
                                                    dst=dest, ssh_timeout=300)
        self.loaders.nodes[0].remoter.run('cat janusgraph/janusgraph-cql/target/surefire-reports/*.txt')
        self.loaders.nodes[0].remoter.run_output_check('cat janusgraph/janusgraph-cql/target/surefire-reports/*.txt',300, False, 'Failures: 0, Errors: 0', 'FAILURE!')


if __name__ == '__main__':
    main()
