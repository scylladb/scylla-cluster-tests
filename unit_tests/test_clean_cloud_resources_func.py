import unittest
from sdcm.utils.common import clean_cloud_resources


class CleanCloudResourcesTest(unittest.TestCase):

    def test_no_tag_testid_and_runbyuser(self):
        params = {}
        res = clean_cloud_resources(params)
        self.assertFalse(res)

    def test_other_tags_and_no_testid_and_runbyuser(self):
        params = {"NodeType": "scylla-db"}
        res = clean_cloud_resources(params)
        self.assertFalse(res)

    def test_tag_testid_only(self):
        params = {"RunByUser": "test"}
        res = clean_cloud_resources(params)
        self.assertTrue(res)

    def test_tag_runbyuser_only(self):
        params = {"TestId": "1111"}
        res = clean_cloud_resources(params)
        self.assertTrue(res)

    def test_tags_testid_and_runbyuser(self):
        params = {"RunByUser": "test", "TestId": "1111"}
        res = clean_cloud_resources(params)
        self.assertTrue(res)

    def test_tags_testid_and_runbyuser_with_other(self):
        params = {"RunByUser": "test",
                  "TestId": "1111",
                  "NodeType": "monitor"}
        res = clean_cloud_resources(params)
        self.assertTrue(res)
