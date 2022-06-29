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

from sdcm.cluster import BaseNode
from sdcm.utils.distro import Distro
from sdcm.utils.common import tag_ami, convert_metric_to_ms, download_dir_from_cloud
from sdcm.utils.sstable import load_inventory
from sdcm.utils.sstable.load_utils import SstableLoadUtils

logging.basicConfig(level=logging.DEBUG)


class TestUtils(unittest.TestCase):
    def test_tag_ami_01(self):  # pylint: disable=no-self-use
        tag_ami(ami_id='ami-076a213c791dc19cd',
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
        test_file_names = ["sct_test/", "sct_test/bentsi.txt", "sct_test/charybdis.fs"]
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


class DummyNode(BaseNode):  # pylint: disable=abstract-method
    _system_log = None
    is_enterprise = False
    distro = Distro.CENTOS7

    def _get_private_ip_address(self) -> str:
        return '127.0.0.1'

    def _get_public_ip_address(self) -> str:
        return '127.0.0.1'

    def start_task_threads(self) -> None:
        # disable all background threads
        pass

    @property
    def system_log(self) -> str:
        return self._system_log

    @system_log.setter
    def system_log(self, log: str):
        self._system_log = log

    def set_hostname(self) -> None:
        pass

    def configure_remote_logging(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500) -> None:
        pass

    @property
    def is_nonroot_install(self) -> bool:  # pylint: disable=invalid-overridden-method
        return False


class TestSstableLoadUtils(unittest.TestCase):
    node = None
    temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls.node = DummyNode(name='test_node', parent_cluster=None,
                             base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.init()

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'load_and_stream.log')

    @staticmethod
    def test_load_column_1_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=1, big_sstable=False,
                                                                  load_and_stream=False)
        assert test_data == load_inventory.COLUMN_1_DATA, \
            f"Expected {load_inventory.COLUMN_1_DATA}, got {test_data}"

        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=1, big_sstable=True,
                                                                  load_and_stream=False)
        assert test_data == load_inventory.BIG_SSTABLE_COLUMN_1_DATA, \
            f"Expected {load_inventory.BIG_SSTABLE_COLUMN_1_DATA}, got {test_data}"

    @staticmethod
    def test_load_multi_column_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=5, big_sstable=False,
                                                                  load_and_stream=False)
        assert test_data == load_inventory.MULTI_COLUMNS_DATA, \
            f"Expected {load_inventory.MULTI_COLUMNS_DATA}, got {test_data}"

        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=5, big_sstable=True,
                                                                  load_and_stream=False)
        assert test_data == load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA, \
            f"Expected {load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA}, got {test_data}"

        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=6, big_sstable=False,
                                                                  load_and_stream=False)
        assert test_data == load_inventory.MULTI_COLUMNS_DATA, \
            f"Expected {load_inventory.MULTI_COLUMNS_DATA}, got {test_data}"

        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=6, big_sstable=True,
                                                                  load_and_stream=False)
        assert test_data == load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA, \
            f"Expected {load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA}, got {test_data}"

    @staticmethod
    def test_load_not_supported_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=2, big_sstable=False,
                                                                  load_and_stream=False)
        assert not test_data, \
            f"Expected empty test data, got {test_data}"

    @staticmethod
    def test_load_load_and_stream_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(column_number=5, big_sstable=False,
                                                                  load_and_stream=True)
        assert test_data == load_inventory.MULTI_NODE_DATA, \
            f"Expected {load_inventory.MULTI_NODE_DATA}, got {test_data}"

    @staticmethod
    def test_distribute_test_files_to_cluster_nodes():
        test_cases = [{'nodes': ['node1', 'node2', 'node3'], 'expected_result': 5},
                      {'nodes': ['node1', 'node2'], 'expected_result': 5},
                      {'nodes': ['node1', 'node2', 'node3', 'node4'], 'expected_result': 5},
                      {'nodes': ['node1', 'node2', 'node3', 'node4', 'node5'], 'expected_result': 5},
                      {'nodes': ['node1', 'node2', 'node3', 'node5', 'node6'], 'expected_result': 5},
                      ]

        for case in test_cases:
            test_data = SstableLoadUtils.get_load_test_data_inventory(5, big_sstable=False, load_and_stream=True)
            map_files_to_node = SstableLoadUtils.distribute_test_files_to_cluster_nodes(nodes=case["nodes"],
                                                                                        test_data=test_data)
            assert len(map_files_to_node) == case["expected_result"], \
                f"Expected {case['expected_result']} elements, got {len(map_files_to_node)}"

    def test_load_and_stream_status(self):
        patterns = [SstableLoadUtils.LOAD_AND_STREAM_DONE_EXPR.format('keyspace1', 'standard1'),
                    SstableLoadUtils.LOAD_AND_STREAM_RUN_EXPR]
        system_log_follower = self.node.follow_system_log(start_from_beginning=True, patterns=patterns)
        SstableLoadUtils.validate_load_and_stream_status(self.node, system_log_follower)
