from __future__ import absolute_import
import os
import hashlib
import shutil
import logging
import unittest

from sdcm.utils.common import tag_ami
from sdcm.utils.common import download_dir_from_cloud

logging.basicConfig(level=logging.DEBUG)


class TestUtils(unittest.TestCase):
    def test_tag_ami_01(self):  # pylint: disable=no-self-use
        tag_ami(ami_id='ami-0876bfff890e17a06',
                tags_dict={'JOB_longevity-multi-keyspaces-60h': 'PASSED'}, region_name='eu-west-1')


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

        update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        assert os.path.exists(os.path.join(update_db_packages, "repomd.xml"))

    def test_update_db_packages_gce(self):
        sct_update_db_packages = 'gs://scratch.scylladb.com/sct_test/'

        self.clear_cloud_downloaded_path(sct_update_db_packages)

        update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        assert os.path.exists(os.path.join(update_db_packages, "text.txt"))

    @staticmethod
    def test_update_db_packages_none():
        sct_update_db_packages = None
        update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        assert update_db_packages is None
