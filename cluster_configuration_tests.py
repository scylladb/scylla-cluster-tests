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
from sdcm.utils.common import skip_optional_stage


class ClusterConfigurationTests(ClusterTester):
    """
    Build a Scylla cluster with the appropriate parameters.
    """

    default_params = {'timeout': 650000}

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = self._get_common_email_data()
        return email_data

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

    def test_change_seed_address_to_hostname(self):
        """
        Build a Scylla cluster with params defined in data_dir/scylla.yaml
        Stop cluster.
        Replace IPs for seeds, listen_address & broadcast_rpc_address to public dns names
        The cluster works properly.
        """
        if self.params.get('cluster_backend') != 'aws':
            self.log.error("Test supports only AWS instances")
            return

        self.log.info('DB cluster is: %s', self.db_cluster)
        for node in self.db_cluster.nodes:
            node.stop_scylla(verify_up=False, verify_down=True)

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
            with node.remote_scylla_yaml() as scylla_yml:
                scylla_yml.listen_address = addresses[node.private_ip_address]
                scylla_yml.rpc_address = addresses[node.private_ip_address]
                current_seed_provider = scylla_yml.seed_provider
                seeds_string = ",".join([addresses.get(seed) for seed in seeds])
                current_seed_provider[0].parameters = [{"seeds": seeds_string}]
                scylla_yml.seed_provider = current_seed_provider

        for node in self.db_cluster.nodes:
            if node.is_seed:
                node.start_scylla(verify_down=False, verify_up=True)
        for node in self.db_cluster.nodes:
            if not node.is_seed:
                node.start_scylla(verify_down=False, verify_up=True)
        base_cmd_w = self.params.get('stress_cmd')

        if not skip_optional_stage('main_load'):
            # run a workload
            cs_thread_pool = self.run_stress_thread(stress_cmd=base_cmd_w)
            self.verify_stress_thread(cs_thread_pool)
