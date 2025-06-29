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


from sdcm.utils.version_utils import ComparableScyllaVersion

from utils.get_supported_scylla_base_versions import UpgradeBaseVersion

"""
This module contains tests for the UpgradeBaseVersion class, which is used to determine
the supported Scylla versions for upgrades based on the provided repository, backend, and Linux distribution.
"""


def general_test(scylla_repo='', linux_distro='', cloud_provider=None):
    """
    General test function to retrieve the list of supported Scylla versions for upgrade.
    """
    scylla_version = None

    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, scylla_version)
    version_detector.set_start_support_version(cloud_provider)
    _, version_list = version_detector.get_version_list()
    return version_list


download_url_base = 'http://downloads.scylladb.com'
url_base = f'{download_url_base}/unstable/scylla'


def test_master_rpm():
    """
    Test that master branch select on specific version for upgrade.
    (not hardcoding the version, since it keep changing)
    """
    scylla_repo = url_base + '/master/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
    linux_distro = 'centos'
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= '2025.1'


def test_master_deb():
    """
    Test that master branch select on specific version for upgrade.
    (not hardcoding the version, since it keep changing)
    """
    scylla_repo = url_base + '/master/deb/unified/2025-02-16T22:46:42Z/scylladb-2025.1/scylla.list'
    linux_distro = 'ubuntu-jammy'
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= '2025.1'


def test_2024_1_with_centos9():
    """ Test that development branch of 2024.1 with centos9 is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.1/rpm/centos/latest/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'5.4', '2024.1'}


def test_2024_1():
    """ Test that development branch of 2024.1 is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.1/rpm/centos/latest/scylla.repo'
    linux_distro = 'centos'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'5.4', '2023.1', '2024.1'}


def test_2024_2():
    """ Test that development branch of 2024.2 is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.2/rpm/centos/latest/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'6.0', '2024.1', '2024.2'}


def test_2024_2_ubuntu():
    """ Test that development branch of 2024.2 on ubuntu is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.2/deb/unified/latest/scylladb-2024.2/scylla.list'
    linux_distro = 'ubuntu-focal'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'6.0', '2024.1', '2024.2'}


def test_2025_1_dev():
    """ Test that development branch of 2025.1 is returned correct versions """
    scylla_repo = url_base + '/master/rpm/centos/2025-01-15T09:25:01Z/scylla.repo'
    linux_distro = 'centos'
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= '2025.1'


def test_2025_1_release_ubuntu():
    """ Test that 2025.1 release on ubuntu is returned correct versions """
    scylla_repo = url_base + '/branch-2025.1/deb/unified/2025-02-16T22:46:42Z/scylladb-2025.1/scylla.list'
    linux_distro = 'ubuntu-focal'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'6.2', '2024.1', '2024.2'}.issubset(set(version_list))


def test_2025_1_release_centos():
    """ Test that 2025.1 release on centos is returned correct versions """
    scylla_repo = url_base + '/branch-2025.1/rpm/centos/2025-02-23T16:19:08Z/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'6.2', '2024.1', '2024.2'}.issubset(set(version_list))
