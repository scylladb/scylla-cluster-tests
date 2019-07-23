import unittest

from sdcm.utils.version_utils import get_branch_version_from_list, get_branch_version_from_repo, get_branch_version, is_enterprise

deb_url = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/deb/unstable/stretch/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d/68/scylladb-2019.1/scylla.list'
rpm_url = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla.repo'

broken_url = 'https://www.google.com'


class TestVersionUtils(unittest.TestCase):
    def test_01_get_branch_version_from_list(self):
        self.assertEqual(get_branch_version_from_list(deb_url), '2019.1.1')

    def test_02_get_branch_version_from_repo(self):
        self.assertEqual(get_branch_version_from_repo(rpm_url), '2019.1.1')

    def test_03_get_branch_version(self):
        self.assertEqual(get_branch_version(rpm_url), '2019.1.1')
        self.assertEqual(get_branch_version(deb_url), '2019.1.1')

    def test_04_get_branch_version_failed(self):
        self.assertRaisesRegexp(ValueError, "url isn't a correct", get_branch_version, broken_url)
        self.assertRaisesRegexp(ValueError, "url isn't a correct", get_branch_version_from_list, broken_url)
        self.assertRaisesRegexp(ValueError, "url isn't a correct", get_branch_version_from_repo, broken_url)

    def test_05_is_enterprise(self):
        self.assertEqual(is_enterprise('2019.1.1'), True)
        self.assertEqual(is_enterprise('2018'), True)
        self.assertEqual(is_enterprise('3.1'), False)
        self.assertEqual(is_enterprise('2.2'), False)
