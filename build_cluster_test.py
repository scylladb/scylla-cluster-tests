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


class BuildClusterTest(ClusterTester):

    """
    Build a Scylla cluster with the appropriate parameters.

    :avocado: enable
    """

    default_params = {'timeout': 650000}

    def test_build(self):
        """
        Build a Scylla cluster with params defined in data_dir/scylla.yaml
        """
        self.log.info('DB cluster is: %s', self.db_cluster)
        for node in self.db_cluster.nodes:
            self.log.info(node.remoter.ssh_debug_cmd())
        self.log.info('Loader Nodes: %s', self.loaders)
        for node in self.loaders.nodes:
            self.log.info(node.remoter.ssh_debug_cmd())
        self.log.info('Monitor Nodes: %s', self.monitors)
        for node in self.monitors.nodes:
            self.log.info(node.remoter.ssh_debug_cmd())
        if self.monitors.nodes:
            self.log.info('Prometheus Web UI: http://%s:9090',
                          self.monitors.nodes[0].public_ip_address)
            self.log.info('Grafana Web UI: http://%s:3000',
                          self.monitors.nodes[0].public_ip_address)

    def test_use_public_dns_names(self):
        """
        Build a Scylla cluster with params defined in data_dir/scylla.yaml
        Stop cluster.
        Replace IPs for seeds, listen_address & broadcast_rpc_address on public dns names
        The cluster works properly.
        """
        if self.params.get('cluster_backend') != 'aws':
            self.log.error("Test supports only AWS instances")
            return

        self.log.info('DB cluster is: %s', self.db_cluster)
        for node in self.db_cluster.nodes:
            node.remoter.run('sudo systemctl stop scylla-server.service')
            node.remoter.run('sudo systemctl stop scylla-jmx.service')
            node.wait_db_down()

        addresses = {}
        seeds = []
        for node in self.db_cluster.nodes:
            if hasattr(node._instance, 'public_dns_name'):
                addresses[node.private_ip_address] = node._instance.public_dns_name
                if node.is_seed:
                    seeds.append(node.private_ip_address)
            else:
                self.log.error("Node instance doesn't have public dns name. "
                               "Please check AMI!")

        for node in self.db_cluster.nodes:
            node.remoter.run('sudo sed -i.bak s/{0}/{1}/g /etc/scylla/scylla.yaml'.format(node.private_ip_address,
                                                                                          addresses.get(
                                                                                              node.private_ip_address,
                                                                                              "")))
            for seed in seeds:
                node.remoter.run(
                    'sudo sed -i.bak2 s/{0}/{1}/g /etc/scylla/scylla.yaml'.format(seed, addresses.get(seed, "")))

        for node in self.db_cluster.nodes:
            if node.is_seed:
                node.remoter.run('sudo systemctl start scylla-server.service')
                node.remoter.run('sudo systemctl start scylla-jmx.service')
                node.wait_db_up(timeout=300)
                node.wait_jmx_up()

        for node in self.db_cluster.nodes:
            if not node.is_seed:
                node.remoter.run('sudo systemctl start scylla-server.service')
                node.remoter.run('sudo systemctl start scylla-jmx.service')
                node.wait_db_up(timeout=300)
                node.wait_jmx_up()

        base_cmd_w = ("cassandra-stress write no-warmup cl=QUORUM duration=5m "
                      "-schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads=10 -errors ignore "
                      "-pop seq=1..1000000")

        # run a workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=1, keyspace_num=10)
        self.get_stress_results(queue=stress_queue, stress_num=1, keyspace_num=10)


if __name__ == '__main__':
    main()
