from __future__ import absolute_import
import unittest

import pytest

from sdcm.utils.version_utils import (
    get_branch_version,
    get_branch_version_for_multiple_repositories,
    get_git_tag_from_helm_chart_version,
    is_enterprise,
    VERSION_NOT_FOUND_ERROR,
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
