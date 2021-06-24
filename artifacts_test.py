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
# Copyright (c) 2020 ScyllaDB

import datetime
import re

from sdcm.tester import ClusterTester


STRESS_CMD: str = "/usr/bin/cassandra-stress"


BACKENDS = {
    "aws": ["Ec2Snitch", "Ec2MultiRegionSnitch"],
    "gce": ["GoogleCloudSnitch"],
    "docker": ["GossipingPropertyFileSnitch", "SimpleSnitch"]
}


class ArtifactsTest(ClusterTester):
    @property
    def node(self):
        if self.db_cluster is None or not self.db_cluster.nodes:
            raise ValueError('DB cluster has not been initiated')
        return self.db_cluster.nodes[0]

    def check_cluster_name(self):
        with self.node.remote_scylla_yaml() as scylla_yaml:
            yaml_cluster_name = scylla_yaml.get('cluster_name', '')

        self.assertTrue(self.db_cluster.name == yaml_cluster_name,
                        f"Cluster name is not as expected. Cluster name in scylla.yaml: {yaml_cluster_name}. "
                        f"Cluster name: {self.db_cluster.name}")

    def run_cassandra_stress(self, args: str):
        result = self.node.remoter.run(
            f"{self.node.add_install_prefix(STRESS_CMD)} {args} -node {self.node.ip_address}")
        assert "java.io.IOException" not in result.stdout
        assert "java.io.IOException" not in result.stderr

    def check_scylla(self):
        self.node.run_nodetool("status")
        self.run_cassandra_stress("write n=10000 -mode cql3 native -pop seq=1..10000")
        self.run_cassandra_stress("mixed duration=1m -mode cql3 native -rate threads=10 -pop seq=1..10000")

    def verify_users(self):
        # We can't ship the image with Scylla internal users inside. So we
        # need to verify that mistakenly we didn't create all the users that we have project wide in the image
        self.log.info("Checking that all existent users except centos were created after boot")
        uptime = self.node.remoter.run(cmd="uptime -s").stdout.strip()
        datetime_format = "%Y-%m-%d %H:%M:%S"
        instance_start_time = datetime.datetime.strptime(uptime, datetime_format)
        self.log.info("Instance started at: %s", instance_start_time)
        out = self.node.remoter.run(cmd="ls -ltr --full-time /home", verbose=True).stdout.strip()
        for line in out.splitlines():
            splitted_line = line.split()
            if len(splitted_line) <= 2:
                continue
            user = splitted_line[-1]
            if user == "centos":
                self.log.info("Skipping user %s since it is a default image user.", user)
                continue
            self.log.info("Checking user: '%s'", user)
            if datetime_str := re.search(r"(\d+-\d+-\d+ \d+:\d+:\d+).", line):
                datetime_user_created = datetime.datetime.strptime(datetime_str.group(1), datetime_format)
                self.log.info("User '%s' created at '%s'", user, datetime_user_created)
                if datetime_user_created < instance_start_time and not user == "centos":
                    AssertionError("User %s was created in the image. Only user centos should exist in the image")
            else:
                raise AssertionError(f"Unable to parse/find timestamp of the user {user} creation in {line}")
        self.log.info("All users except image user 'centos' were created after the boot.")

    def verify_node_health(self):
        self.node.check_node_health()

    def verify_snitch(self, backend_name: str):
        """
        Verify that the snitch used in the cluster is appropriate for
        the backend used.
        """
        if not self.params["use_preinstalled_scylla"]:
            self.log.info("Skipping verifying the snitch due to the 'use_preinstalled_scylla' being set to False")
            return

        describecluster_snitch = self.get_describecluster_info().snitch
        with self.node.remote_scylla_yaml() as scylla_yaml:
            scylla_yaml_snitch = scylla_yaml['endpoint_snitch']
        expected_snitches = BACKENDS[backend_name]

        snitch_patterns = [re.compile(f"({snitch})") for snitch in expected_snitches]
        snitch_matches_describecluster = [pattern.search(describecluster_snitch) for pattern in snitch_patterns]
        snitch_matches_scylla_yaml = [pattern.search(scylla_yaml_snitch) for pattern in snitch_patterns]

        with self.subTest('verify snitch against describecluster output'):
            self.assertTrue(any(snitch_matches_describecluster),
                            msg=f"Expected snitch matches for describecluster to not be empty, but was. Snitch "
                                f"matches: {snitch_matches_describecluster}"
                            )

        with self.subTest('verify snitch against scylla.yaml configuration'):
            self.assertTrue(any(snitch_matches_scylla_yaml),
                            msg=f"Expected snitch matches for scylla yaml to not be empty, but was. Snitch "
                                f"matches: {snitch_matches_scylla_yaml}"
                            )

    def test_scylla_service(self):
        backend = self.params["cluster_backend"]

        if backend == "aws":
            with self.subTest("check ENA support"):
                assert self.node.ena_support, "ENA support is not enabled"

        if backend == "gce":
            with self.subTest("verify users"):
                self.verify_users()

        if self.params["use_preinstalled_scylla"] and backend != "docker":
            with self.subTest("check the cluster name"):
                self.check_cluster_name()

        with self.subTest('verify snitch'):
            self.verify_snitch(backend_name=backend)

        with self.subTest('verify node health'):
            self.verify_node_health()

        with self.subTest("check Scylla server after installation"):
            self.check_scylla()

        with self.subTest("check Scylla server after stop/start"):
            self.node.stop_scylla(verify_down=True)
            self.node.start_scylla(verify_up=True)
            self.check_scylla()

        with self.subTest("check Scylla server after restart"):
            self.node.restart_scylla(verify_up_after=True)
            self.check_scylla()

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = self._get_common_email_data()
        try:
            node = self.node
        except:  # pylint: disable=bare-except
            node = None
        if node:
            scylla_packages = node.scylla_packages_installed
        else:
            scylla_packages = None
        if not scylla_packages:
            scylla_packages = ['No scylla packages are installed. Please check log files.']
        email_data.update({"scylla_node_image": node.image if node else 'Node has not been initialized',
                           "scylla_packages_installed": scylla_packages,
                           "unified_package": self.params.get("unified_package"),
                           "nonroot_offline_install": self.params.get("nonroot_offline_install"),
                           "scylla_repo": self.params.get("scylla_repo"), })

        return email_data
