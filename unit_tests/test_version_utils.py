import unittest

from sdcm.utils.version_utils import get_branch_version_from_list, get_branch_version_from_repo, get_branch_version, is_enterprise

# pylint: disable=invalid-name

DEB_URL = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/deb/unstable/stretch/'\
          '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1'\
          '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d/68/scylladb-2019.1/scylla.list'
RPM_URL = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/'\
          '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1'\
          '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla.repo'

ISSUE_5288_RPM_URL = 'http://scratch.scylladb.com/amos/yum_repos/master/piotrj_issue5288_v3/scylla.repo'

BROKEN_URL = 'https://www.google.com'


class TestVersionUtils(unittest.TestCase):
    def test_01_get_branch_version_from_list(self):
        self.assertEqual(get_branch_version_from_list(DEB_URL), '2019.1.1')

    def test_02_get_branch_version_from_repo(self):
        self.assertEqual(get_branch_version_from_repo(RPM_URL), '2019.1.1')
        self.assertEqual(get_branch_version_from_repo(ISSUE_5288_RPM_URL), '666.development')

    def test_03_get_branch_version(self):
        self.assertEqual(get_branch_version(RPM_URL), '2019.1.1')
        self.assertEqual(get_branch_version(DEB_URL), '2019.1.1')

    def test_04_get_branch_version_failed(self):
        self.assertRaisesRegexp(ValueError, "url isn't a correct", get_branch_version, BROKEN_URL)
        self.assertRaisesRegexp(ValueError, "url isn't a correct", get_branch_version_from_list, BROKEN_URL)
        self.assertRaisesRegexp(ValueError, "url isn't a correct", get_branch_version_from_repo, BROKEN_URL)

    def test_05_is_enterprise(self):
        self.assertEqual(is_enterprise('2019.1.1'), True)
        self.assertEqual(is_enterprise('2018'), True)
        self.assertEqual(is_enterprise('3.1'), False)
        self.assertEqual(is_enterprise('2.2'), False)
        self.assertEqual(is_enterprise('666.development'), False)
