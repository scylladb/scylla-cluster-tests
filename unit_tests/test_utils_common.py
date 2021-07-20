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

import os
import hashlib
import shutil
import logging
import unittest
import unittest.mock
from pathlib import Path

from sdcm.utils.common import tag_ami, convert_metric_to_ms
from sdcm.utils.common import download_dir_from_cloud

logging.basicConfig(level=logging.DEBUG)


class TestUtils(unittest.TestCase):
    def test_tag_ami_01(self):  # pylint: disable=no-self-use
        tag_ami(ami_id='ami-0876bfff890e17a06',
                tags_dict={'JOB_longevity-multi-keyspaces-60h': 'PASSED'}, region_name='eu-west-1')

    def test_scylla_bench_metrics_conversion(self):  # pylint: disable=no-self-use
        metrics = {"4ms": 4.0, "950µs": 0.95, "30ms": 30.0, "8.592961906s": 8592.961905999999,
                   "18.120703ms": 18.120703, "5.963775µs": 0.005963775, "9h0m0.024080491s": 32400024.080491,
                   "1m0.024080491s": 60024.080491, "546431": 546431.0}
        for metric, converted in metrics.items():
            actual = convert_metric_to_ms(metric)
            assert actual == converted, f"Expected {converted}, got {actual}"


class TestDownloadDir(unittest.TestCase):
    @staticmethod
    def clear_cloud_downloaded_path(url):
        md5 = hashlib.md5()
        md5.update(url.encode('utf-8'))
        tmp_dir = os.path.join('/tmp/download_from_cloud', md5.hexdigest())
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_update_db_packages_s3(self):
        sct_update_db_packages = 's3://downloads.scylladb.com/rpm/centos/scylladb-nightly/scylla/7/x86_64/repodata/'

        self.clear_cloud_downloaded_path(sct_update_db_packages)

        def touch_file(client, bucket, key, local_file_path):  # pylint: disable=unused-argument
            Path(local_file_path).touch()

        with unittest.mock.patch("sdcm.utils.common._s3_download_file", new=touch_file):
            update_db_packages = download_dir_from_cloud(sct_update_db_packages)

        assert os.path.exists(os.path.join(update_db_packages, "repomd.xml"))

    def test_update_db_packages_gce(self):
        sct_update_db_packages = 'gs://scratch.scylladb.com/sct_test/'

        class FakeObject:  # pylint: disable=too-few-public-methods
            def __init__(self, name):
                self.name = name

            @staticmethod
            def download(destination_path, overwrite_existing, *_, **__):
                Path(destination_path).touch(exist_ok=overwrite_existing)

        self.clear_cloud_downloaded_path(sct_update_db_packages)
        test_file_names = ["sct_test/bentsi.txt", "sct_test/charybdis.fs"]
        with unittest.mock.patch("libcloud.storage.drivers.google_storage.GoogleStorageDriver.list_container_objects",
                                 return_value=[FakeObject(name=fname) for fname in test_file_names]):
            update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        for fname in test_file_names:
            assert (Path(update_db_packages) / os.path.basename(fname)).exists()

    @staticmethod
    def test_update_db_packages_none():
        sct_update_db_packages = None
        update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        assert update_db_packages is None
