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
# Copyright (c) 2021 ScyllaDB


import unittest

from utils.get_supported_scylla_base_versions import UpgradeBaseVersion  # pylint: disable=no-name-in-module, import-error


def general_test(scylla_repo='', linux_distro=''):
    scylla_version = None

    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, scylla_version)
    version_detector.set_start_support_version()
    _, version_list = version_detector.get_version_list()
    return version_list


class TestBaseVersion(unittest.TestCase):
    download_url_base = 'http://downloads.scylladb.com'
    url_base = f'{download_url_base}/unstable/scylla'

    def test_master(self):
        scylla_repo = self.url_base + '/master/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
        linux_distro = 'centos'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['5.2'])

    def test_4_5_with_centos8(self):
        scylla_repo = self.url_base + '/branch-4.5/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
        linux_distro = 'centos-8'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['4.4', '4.5'])

    def test_4_5(self):
        scylla_repo = self.url_base + '/4.5/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
        linux_distro = 'centos'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['4.4', '4.5'])

    def test_5_0(self):
        scylla_repo = self.url_base + '/branch-5.0/rpm/centos/2023-01-21T02:51:18Z/scylla.repo'
        linux_distro = 'centos-8'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['4.6', '5.0'])

    def test_5_1(self):
        scylla_repo = self.url_base + '/branch-5.1/deb/unified/2023-01-21T02:35:18Z/scylladb-5.1/scylla.list'
        linux_distro = 'ubuntu-focal'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['5.0', '5.1'])

    def test_enterprise(self):
        scylla_repo = self.url_base + '-enterprise/enterprise/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
        linux_distro = 'centos'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['2022.2'])

    def test_2021_1(self):
        scylla_repo = self.url_base + '-enterprise/branch-2021.1/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
        linux_distro = 'centos'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['4.3', '2020.1', '2021.1'])

    def test_2021_1_with_centos8(self):
        scylla_repo = self.url_base + '-enterprise/branch-2021.1/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
        linux_distro = 'centos-8'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(version_list, ['4.3', '2021.1'])

    def test_2022_1_with_centos8(self):
        scylla_repo = self.url_base + \
            '-enterprise/enterprise-2022.1/deb/unified/2022-06-03T00:22:55Z/scylladb-2022.1/scylla.list'
        linux_distro = 'centos-8'
        version_list = general_test(scylla_repo, linux_distro)
        self.assertEqual(['5.0', '2021.1', '2022.1'], version_list)


if __name__ == "__main__":
    unittest.main()
