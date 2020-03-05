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

import requests

from sdcm.tester import ClusterTester
from sdcm.utils.housekeeping import HousekeepingDB


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
               "gpgkey=", ),
    "ubuntu": ("deb ", ),
}


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


class PrivateRepoTest(ClusterTester):
    def get_last_id(self, table: str) -> int:
        row = self.housekeeping.get_most_recent_record(f"SELECT * FROM {table} WHERE uuid = %s", (self.uuid, ))
        return row[0] if row else 0

    def setUp(self) -> None:
        super().setUp()

        self.sw_repo = self.params["scylla_repo"]

        match = SW_REPO_RE.match(self.sw_repo)
        self.assertTrue(match, f"Unable to parse URL: {self.sw_repo}")

        self.uuid, self.ostype = match.group("uuid"), match.group("ostype")
        self.body_prefixes = BODY_PREFIXES[self.ostype]
        self.pkginfo_url_parser = PKGINFO_URL_PARSERS[self.ostype]

        #  Strings `rpm/scylla'
        #          `deb/ubuntu'
        #          `deb/debian' have same length.
        self.pkginfo_url_prefix_len = len(f"{PRIVATE_REPO_BASEURL}/scylladb/{self.uuid}/rpm/scylla")

        self.housekeeping = HousekeepingDB.from_keystore_creds()
        self.housekeeping.connect()

    def tearDown(self) -> None:
        self.housekeeping.close()

        super().tearDown()

    def test_private_repo(self) -> None:
        with self.subTest("Verify repo file syntax and housekeeping stats."):
            last_id = self.get_last_id(REPO_TABLE)

            self.log.info("Get %s", self.sw_repo)
            repofile = requests.get(self.sw_repo).text
            self.log.info(">>> Downloaded repo file: >>>\n\n%s\n<<<", repofile)

            self.assertGreater(self.get_last_id(REPO_TABLE), last_id,
                               "Our download is not collected to repo table")

            for line in repofile.splitlines():
                if not line.strip():  # Skip empty lines.
                    continue
                if not any(line.startswith(prefix) for prefix in self.body_prefixes):
                    self.fail(f"Repo content has invalid line: {line}")

        with self.subTest("Verify that redirection works well."):
            last_id = self.get_last_id(REPODOWNLOAD_TABLE)

            pkginfo_url_list = self.pkginfo_url_parser(repofile)
            self.assertTrue(pkginfo_url_list, "There is should be at lease one pkginfo URL")
            self.log.info(">>> List of pkginfo URLs: >>>\n\t%s\n<<<", "\n\t".join(pkginfo_url_list))

            for pkginfo_url in pkginfo_url_list:
                self.assertTrue(pkginfo_url.startswith(PRIVATE_REPO_BASEURL),
                                f"pkginfo URL should start with {PRIVATE_REPO_BASEURL}")

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

                new_last_id = self.get_last_id(REPODOWNLOAD_TABLE)
                self.assertGreater(new_last_id, last_id, "Our download is not collected to repo table")

                last_id = new_last_id

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()
        email_data.update({"repo_ostype": self.ostype,
                           "repo_uuid": self.uuid,
                           "scylla_repo": self.params.get("scylla_repo"), })

        return email_data
