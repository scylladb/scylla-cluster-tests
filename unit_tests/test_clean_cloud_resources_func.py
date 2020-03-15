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
