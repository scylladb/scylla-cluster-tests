from __future__ import absolute_import
import unittest

import pytest

import sdcm
from sdcm.utils.version_utils import (
    get_scylla_urls_from_repository,
    get_branch_version,
    get_branch_version_for_multiple_repositories,
    get_git_tag_from_helm_chart_version,
    is_enterprise,
    VERSION_NOT_FOUND_ERROR, RepositoryDetails, ScyllaFileType,
)

BASE_S3_DOWNLOAD_URL = 'https://s3.amazonaws.com/downloads.scylladb.com'
DEB_URL = \
    f'{BASE_S3_DOWNLOAD_URL}/enterprise/deb/unstable/stretch/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7e' \
    f'f592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d/68/scylladb-2019.1/' \
    f'scylla.list'
RPM_URL = \
    f'{BASE_S3_DOWNLOAD_URL}/enterprise/rpm/unstable/centos/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7e' \
    f'f592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da' \
    f'978aeec22/71/scylla.repo'

BROKEN_URL = 'https://www.google.com'


class TestVersionUtils(unittest.TestCase):

    def check_multiple_urls(self, urls):
        expected_versions, repo_urls = [], []
        for (url, expected_version) in urls:
            repo_urls.append(url)
            expected_versions.append(expected_version)
        for branch_version, expected_version in zip(get_branch_version_for_multiple_repositories(urls=repo_urls),
                                                    expected_versions):
            self.assertEqual(branch_version, expected_version)

    def test_01_get_branch_version_from_list(self):
        self.assertEqual(get_branch_version(DEB_URL), '2019.1.1')

    def test_02_get_branch_version_from_repo(self):
        self.check_multiple_urls(urls=[(RPM_URL, "2019.1.1")])

    def test_03_get_branch_version(self):
        self.check_multiple_urls(urls=[(RPM_URL, "2019.1.1"), (DEB_URL, "2019.1.1")])

    def test_04_get_branch_version_failed(self):
        msg_error = VERSION_NOT_FOUND_ERROR
        self.assertRaisesRegex(ValueError, msg_error, get_branch_version, BROKEN_URL)
        self.assertRaisesRegex(ValueError, msg_error, get_branch_version, BROKEN_URL)
        self.assertRaisesRegex(ValueError, msg_error, get_branch_version, BROKEN_URL)

    def test_05_is_enterprise(self):
        self.assertEqual(is_enterprise('2019.1.1'), True)
        self.assertEqual(is_enterprise('2018'), True)
        self.assertEqual(is_enterprise('3.1'), False)
        self.assertEqual(is_enterprise('2.2'), False)
        self.assertEqual(is_enterprise('666.development'), False)

    def test_06_get_scylla_urls_from_repository_rpm_one_arch(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(
                RepositoryDetails(
                    type=ScyllaFileType.YUM,
                    urls=[
                        'baseurl=http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/$basearch/',
                        '[scylla-generic]', 'enabled=1',
                        'name=Scylla for centos $releasever',
                        '[scylla]',
                        'name=Scylla for Centos $releasever - $basearch',
                        'baseurl=http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/noarch/',
                        'gpgcheck=0'
                    ]
                )
            )
        self.assertEqual(
            {
                'http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/noarch'
                '/repodata/repomd.xml',
                'http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/2021-06-29T00:59:00Z/scylla/x86_64'
                '/repodata/repomd.xml',
            },
            urls)

    def test_06_get_scylla_urls_from_repository_deb_one_arch(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=['deb [arch=amd64] http://downloads.scylladb.com/unstable/scylla/master/deb/unified/'
                      '2021-06-23T10:53:35Z/scylladb-master stable main'],
            ))
        self.assertEqual(
            {
                'http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/'
                'dists/stable/main/binary-amd64/Packages',
            },
            urls)

    def test_06_get_scylla_urls_from_repository_deb_two_archs(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=['deb [arch=amd64,arm64] http://downloads.scylladb.com/unstable/scylla/master/deb/unified/'
                      '2021-06-23T10:53:35Z/scylladb-master stable main'],
            ))
        self.assertEqual(
            {
                'http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/'
                'dists/stable/main/binary-amd64/Packages',
                'http://downloads.scylladb.com/unstable/scylla/master/deb/unified/2021-06-23T10:53:35Z/scylladb-master/'
                'dists/stable/main/binary-arm64/Packages',
            },
            urls)

    def test_06_get_scylla_urls_from_repository_no_urls(self):
        with unittest.mock.patch.object(sdcm.utils.version_utils, 'get_url_content', return_value='', clear=True):
            urls = get_scylla_urls_from_repository(RepositoryDetails(
                type=ScyllaFileType.DEBIAN,
                urls=[''],
            ))
        self.assertEqual(
            set(),
            urls)


@pytest.mark.parametrize("chart_version,git_tag", [
    ("v1.1.0-rc.2-0-gc86ad89", "v1.1.0-rc.2"),
    ("v1.1.0-rc.1-1-g6d35b37", "v1.1.0-rc.1"),
    ("v1.1.0-rc.1", "v1.1.0-rc.1"),
    ("v1.1.0-alpha.0-3-g6594091-nightly", "v1.1.0-alpha.0"),
    ("v1.1.0-alpha.0-3-g6594091", "v1.1.0-alpha.0"),
    ("v1.0.0", "v1.0.0"),
    ("v1.0.0-39-g5bc1839", "v1.0.0"),
    ("v1.0.0-rc0-53-g489398a-nightly", "v1.0.0-rc0"),
    ("v1.0.0-rc0-53-g489398a", "v1.0.0-rc0"),
    ("v1.0.0-rc0-51-ga52c206-latest", "v1.0.0-rc0"),
])
def test_06_get_git_tag_from_helm_chart_version(chart_version, git_tag):
    assert get_git_tag_from_helm_chart_version(chart_version) == git_tag


@pytest.mark.parametrize("chart_version", [
    "", "fake", "V1.0.0", "1.0.0", "1.1.0-rc.1", "1.1.0-rc.1-1-g6d35b37",
])
def test_07_get_git_tag_from_helm_chart_version__wrong_input(chart_version):
    try:
        git_tag = get_git_tag_from_helm_chart_version(chart_version)
    except ValueError:
        return
    assert False, f"'ValueError' was expected, but absent. Returned value: {git_tag}"
