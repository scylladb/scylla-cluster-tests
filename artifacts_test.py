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

import re
import json
import datetime

import requests

from sdcm.tester import ClusterTester
from sdcm.cluster import SCYLLA_YAML_PATH
from sdcm.utils.housekeeping import HousekeepingDB

STRESS_CMD: str = "/usr/bin/cassandra-stress"


# class ArtifactTester(ClusterTester):
#     REPO_TABLE = "housekeeping.repo"
#
#     def get_last_id(self, table: str, uuid: str) -> int:
#         row = self.housekeeping.get_most_recent_record(f"SELECT * FROM {table} WHERE uuid = %s", (self.uuid, ))
#         return row[0] if row else 0
#
#     def setUp(self) -> None:
#         super().setUp()
#
#         self.housekeeping = HousekeepingDB.from_keystore_creds()
#         self.housekeeping.connect()
#
#     def tearDown(self) -> None:
#         self.housekeeping.close()
#
#         super().tearDown()


class ArtifactsTest(ClusterTester):
    @property
    def node(self):
        if self.db_cluster is None or not self.db_cluster.nodes:
            raise ValueError('DB cluster has not been initiated')
        return self.db_cluster.nodes[0]

    def check_cluster_name(self):
        with self.node.remote_scylla_yaml(SCYLLA_YAML_PATH) as scylla_yaml:
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

    def test_scylla_service(self):
        if self.params["cluster_backend"] == "aws":
            with self.subTest("check ENA support"):
                assert self.node.ena_support, "ENA support is not enabled"

        if self.params["cluster_backend"] == "gce":
            self.verify_users()

        if self.params["use_preinstalled_scylla"] and "docker" not in self.params["cluster_backend"]:
            with self.subTest("check the cluster name"):
                self.check_cluster_name()

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


class PrivateRepoTest(ClusterTester):
    REPO_TABLE = "housekeeping.repo"
    REPODOWNLOAD_TABLE = "housekeeping.repodownload"
    PRIVATE_REPO_BASEURL = "https://repositories.scylladb.com/scylla/"
    SW_REPO_RE = re.compile(PRIVATE_REPO_BASEURL + r"repo/(?P<uuid>[\w-]+)/(?P<ostype>[\w]+)")

    BODY_PREFIXES = {
        "centos": ("[scylla",
                   "name=",
                   "baseurl=",
                   "type=",
                   "skip_if_unavailable=",
                   "enabled=",
                   "enabled_metadata=",
                   "repo_gpgcheck=",
                   "gpgcheck=",
                   "gpgkey=",),
        "ubuntu": ("deb ",),
    }

    @staticmethod
    def extract_pkginfo_urls_from_yum_repo(repofile):
        urls = []
        for line in repofile.splitlines():
            if line.startswith("baseurl="):
                # Convert string like
                #   `baseurl=https://.../scylla/centos/scylladb-1.7/$releasever/$basearch/'
                # to
                #   `https://.../scylla/centos/scylladb-1.7/7Server/x86_64/repodata/repomd.xml'
                urls.append(
                    line[8:].replace("$releasever", "7Server").replace("$basearch", "x86_64") + "repodata/repomd.xml")
        return urls

    @staticmethod
    def extract_pkginfo_urls_from_apt_repo(repofile):
        urls = []
        for line in repofile.splitlines():
            if line.startswith("deb"):
                # Convert string like:
                #   `deb  [arch=amd64] https://.../deb/ubuntu xenial scylladb-1.7/multiverse'
                # to
                #   `https://.../deb/ubuntu/dists/xenial/scylladb-1.7/multiverse/binary-amd64/Packages.gz'
                _, arch, baseurl, distribution, component = line.split()
                arch = arch.strip("[]").split("=")[1]
                urls.append(f"{baseurl}/dists/{distribution}/{component}/binary-{arch}/Packages.gz")
        return urls

    PKGINFO_URL_PARSERS = {
        "centos": extract_pkginfo_urls_from_yum_repo,
        "ubuntu": extract_pkginfo_urls_from_apt_repo,
    }

    def get_last_id(self, table: str) -> int:
        row = self.housekeeping.get_most_recent_record(f"SELECT * FROM {table} WHERE uuid = %s", (self.uuid,))
        return row[0] if row else 0

    def setUp(self) -> None:
        super().setUp()

        self.sw_repo = self.params["scylla_repo"]

        match = self.SW_REPO_RE.match(self.sw_repo)
        self.assertTrue(match, f"Unable to parse URL: {self.sw_repo}")

        self.uuid, self.ostype = match.group("uuid"), match.group("ostype")
        self.body_prefixes = self.BODY_PREFIXES[self.ostype]
        self.pkginfo_url_parser = self.PKGINFO_URL_PARSERS[self.ostype]

        #  Strings `rpm/scylla'
        #          `deb/ubuntu'
        #          `deb/debian' have same length.
        self.pkginfo_url_prefix_len = len(f"{self.PRIVATE_REPO_BASEURL}/scylladb/{self.uuid}/rpm/scylla")

        self.housekeeping = HousekeepingDB.from_keystore_creds()
        self.housekeeping.connect()

    def tearDown(self) -> None:
        self.housekeeping.close()

        super().tearDown()

    def test_private_repo(self) -> None:
        with self.subTest("Verify repo file syntax and housekeeping stats."):
            last_id = self.get_last_id(self.REPO_TABLE)

            self.log.info("Get %s", self.sw_repo)
            repofile = requests.get(self.sw_repo).text
            self.log.info(">>> Downloaded repo file: >>>\n\n%s\n<<<", repofile)

            self.assertGreater(self.get_last_id(self.REPO_TABLE), last_id,
                               "Our download is not collected to repo table")

            for line in repofile.splitlines():
                if not line.strip():  # Skip empty lines.
                    continue
                if not any(line.startswith(prefix) for prefix in self.body_prefixes):
                    self.fail(f"Repo content has invalid line: {line}")

        with self.subTest("Verify that redirection works well."):
            last_id = self.get_last_id(self.REPODOWNLOAD_TABLE)

            pkginfo_url_list = self.pkginfo_url_parser(repofile)
            self.assertTrue(pkginfo_url_list, "There is should be at lease one pkginfo URL")
            self.log.info(">>> List of pkginfo URLs: >>>\n\t%s\n<<<", "\n\t".join(pkginfo_url_list))

            for pkginfo_url in pkginfo_url_list:
                self.assertTrue(pkginfo_url.startswith(self.PRIVATE_REPO_BASEURL),
                                f"pkginfo URL should start with {self.PRIVATE_REPO_BASEURL}")

                self.log.info("Get %s", pkginfo_url)
                response = requests.get(pkginfo_url)
                self.log.info("Landed at %s", response.url)
                self.assertIn("/downloads.scylladb.com/", response.url,
                              "We're not redirected to downloads.scylladb.com")
                self.assertTrue(response.url.endswith(pkginfo_url[self.pkginfo_url_prefix_len:]),
                                f"Looks like we're redirected to wrong resource: {response.url}")
                self.assertEqual(len(response.history), 1, "There is more than 1 redirection")

                first_response = response.history[-1].text
                self.log.info(first_response)
                try:
                    parsed_first_response = json.loads(first_response)
                except json.JSONDecodeError:
                    self.fail("Document downloaded is not a JSON")
                self.log.info(parsed_first_response)
                self.assertEqual(parsed_first_response["errorMessage"],
                                 "HandlerDemo.ResponseFound Redirection: Resource found elsewhere")
                self.assertEqual(parsed_first_response["errorType"], response.url)

                new_last_id = self.get_last_id(self.REPODOWNLOAD_TABLE)
                self.assertGreater(new_last_id, last_id, "Our download is not collected to repo table")

                last_id = new_last_id

        # with self.subTest("Test after restart."):
        #     self.db_cluster.nodes[1]

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()
        email_data.update({"repo_ostype": self.ostype,
                           "repo_uuid": self.uuid,
                           "scylla_repo": self.params.get("scylla_repo"), })

        return email_data
