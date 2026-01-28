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
import time
import hashlib
import shutil
import logging
import unittest
import unittest.mock
from pathlib import Path

from sdcm import sct_config
from sdcm.cluster import BaseNode, BaseCluster, BaseScyllaCluster
from sdcm.utils.distro import Distro
from sdcm.utils.common import convert_metric_to_ms, download_dir_from_cloud, get_testrun_dir
from sdcm.utils.sstable import load_inventory
from sdcm.utils.sstable.load_utils import SstableLoadUtils

logging.basicConfig(level=logging.DEBUG)


class TestUtils(unittest.TestCase):
    def test_scylla_bench_metrics_conversion(self):
        metrics = {
            "4ms": 4.0,
            "950µs": 0.95,
            "30ms": 30.0,
            "8.592961906s": 8592.961905999999,
            "18.120703ms": 18.120703,
            "5.963775µs": 0.005963775,
            "9h0m0.024080491s": 32400024.080491,
            "1m0.024080491s": 60024.080491,
            "546431": 546431.0,
        }
        for metric, converted in metrics.items():
            actual = convert_metric_to_ms(metric)
            assert actual == converted, f"Expected {converted}, got {actual}"


class TestDownloadDir(unittest.TestCase):
    @staticmethod
    def clear_cloud_downloaded_path(url):
        md5 = hashlib.md5()
        md5.update(url.encode("utf-8"))
        tmp_dir = os.path.join("/tmp/download_from_cloud", md5.hexdigest())
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_update_db_packages_s3(self):
        sct_update_db_packages = "s3://downloads.scylladb.com/rpm/centos/scylladb-nightly/scylla/7/x86_64/repodata/"

        self.clear_cloud_downloaded_path(sct_update_db_packages)

        def touch_file(client, bucket, key, local_file_path):
            Path(local_file_path).touch()

        with unittest.mock.patch("sdcm.utils.common._s3_download_file", new=touch_file):
            update_db_packages = download_dir_from_cloud(sct_update_db_packages)

        assert os.path.exists(os.path.join(update_db_packages, "repomd.xml"))

    def test_update_db_packages_gce(self):
        sct_update_db_packages = "gs://scratch.scylladb.com/sct_test/"

        class FakeObject:
            def __init__(self, name):
                self.name = name

            @staticmethod
            def download_to_filename(filename, *_, **__):
                Path(filename).touch(exist_ok=True)

        self.clear_cloud_downloaded_path(sct_update_db_packages)
        test_file_names = ["sct_test/", "sct_test/bentsi.txt", "sct_test/charybdis.fs"]
        with unittest.mock.patch(
            "google.cloud.storage.Client.list_blobs", return_value=[FakeObject(name=fname) for fname in test_file_names]
        ):
            update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        for fname in test_file_names:
            assert (Path(update_db_packages) / os.path.basename(fname)).exists()

    @staticmethod
    def test_update_db_packages_none():
        sct_update_db_packages = None
        update_db_packages = download_dir_from_cloud(sct_update_db_packages)
        assert update_db_packages is None


class Remoter:
    def __init__(self, system_log):
        self.system_log = system_log

    def run(self, *args, **kwargs):
        lines = [
            "[shard 11] sstables_loader - load_and_stream: started ops_uuid=a2661989-6836-418f-aa67-2c5466499848, process [0-1] out",
            "[shard  2] sstables_loader - Done loading new SSTables for keyspace=keyspace1, table=standard1, load_and_stream=true, "
            "primary_replica_only=false, status=succeeded",
        ]
        for line in lines:
            with open(self.system_log, "a", encoding="utf-8") as file:
                file.write(f"{line}\n")


class DummyDbCluster(BaseCluster, BaseScyllaCluster):
    def __init__(self, nodes):
        self.nodes = nodes
        self.params = sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.racks_count = 0
        self.added_password_suffix = False
        self.log = logging.getLogger(__name__)
        self.node_type = "scylla-db"

    def start_nemesis(self):
        pass


class DummyNode(BaseNode):
    _system_log = None
    is_enterprise = False
    is_product_enterprise = False
    distro = Distro.CENTOS7

    def init(self):
        super().init()
        self.remoter.stop()
        self.remoter = Remoter(self.system_log)

    def do_default_installations(self):
        pass  # we don't need to install anything for this unittests

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

    def _get_private_ip_address(self) -> str:
        return "127.0.0.1"

    def _get_public_ip_address(self) -> str:
        return "127.0.0.1"

    @property
    def cql_address(self):
        return "127.0.0.1"

    def start_task_threads(self) -> None:
        # disable all background threads
        pass

    @property
    def system_log(self) -> str:
        return self._system_log

    @system_log.setter
    def system_log(self, log: str):
        self._system_log = log

    def wait_for_cloud_init(self):
        pass

    def set_hostname(self) -> None:
        pass

    def configure_remote_logging(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500) -> None:
        pass

    @property
    def is_nonroot_install(self) -> bool:
        return False

    @property
    def scylla_shards(self):
        return 0

    @property
    def cpu_cores(self) -> int:
        return 0


class TestSstableLoadUtils(unittest.TestCase):
    node = None
    temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls.node = DummyNode(
            name="test_node",
            parent_cluster=None,
            base_logdir=cls.temp_dir,
            ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
        )
        cls.node.parent_cluster = DummyDbCluster([cls.node])
        cls.node.init()

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), "test_data", "load_and_stream.log")

    @staticmethod
    def test_load_column_1_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=1, big_sstable=False, load_and_stream=False
        )
        assert test_data == load_inventory.COLUMN_1_DATA, f"Expected {load_inventory.COLUMN_1_DATA}, got {test_data}"

        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=1, big_sstable=True, load_and_stream=False
        )
        assert test_data == load_inventory.BIG_SSTABLE_COLUMN_1_DATA, (
            f"Expected {load_inventory.BIG_SSTABLE_COLUMN_1_DATA}, got {test_data}"
        )

    @staticmethod
    def test_load_multi_column_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=5, big_sstable=False, load_and_stream=False
        )
        assert test_data == load_inventory.MULTI_COLUMNS_DATA, (
            f"Expected {load_inventory.MULTI_COLUMNS_DATA}, got {test_data}"
        )

        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=5, big_sstable=True, load_and_stream=False
        )
        assert test_data == load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA, (
            f"Expected {load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA}, got {test_data}"
        )

        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=6, big_sstable=False, load_and_stream=False
        )
        assert test_data == load_inventory.MULTI_COLUMNS_DATA, (
            f"Expected {load_inventory.MULTI_COLUMNS_DATA}, got {test_data}"
        )

        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=6, big_sstable=True, load_and_stream=False
        )
        assert test_data == load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA, (
            f"Expected {load_inventory.BIG_SSTABLE_MULTI_COLUMNS_DATA}, got {test_data}"
        )

    @staticmethod
    def test_load_not_supported_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=2, big_sstable=False, load_and_stream=False
        )
        assert not test_data, f"Expected empty test data, got {test_data}"

    @staticmethod
    def test_load_load_and_stream_data_inventory():
        test_data = SstableLoadUtils.get_load_test_data_inventory(
            column_number=5, big_sstable=False, load_and_stream=True
        )
        assert test_data == load_inventory.MULTI_NODE_DATA, (
            f"Expected {load_inventory.MULTI_NODE_DATA}, got {test_data}"
        )

    @staticmethod
    def test_distribute_test_files_to_cluster_nodes():
        test_cases = [
            {"nodes": ["node1", "node2", "node3"], "expected_result": 5},
            {"nodes": ["node1", "node2"], "expected_result": 5},
            {"nodes": ["node1", "node2", "node3", "node4"], "expected_result": 5},
            {"nodes": ["node1", "node2", "node3", "node4", "node5"], "expected_result": 5},
            {"nodes": ["node1", "node2", "node3", "node5", "node6"], "expected_result": 5},
        ]

        for case in test_cases:
            test_data = SstableLoadUtils.get_load_test_data_inventory(5, big_sstable=False, load_and_stream=True)
            map_files_to_node = SstableLoadUtils.distribute_test_files_to_cluster_nodes(
                nodes=case["nodes"], test_data=test_data
            )
            assert len(map_files_to_node) == case["expected_result"], (
                f"Expected {case['expected_result']} elements, got {len(map_files_to_node)}"
            )

    def test_load_and_stream_waits_for_log_lines(self):
        self.node.remoter = Remoter(self.node.system_log)
        SstableLoadUtils.run_load_and_stream(self.node, start_timeout=1, end_timeout=2)


def test_get_testrun_dir_returns_newest_when_multiple_found(tmp_path):
    """Test that get_testrun_dir returns the newest directory when multiple directories contain the same test_id."""

    # Create a test_id
    test_id = "12345678-1234-1234-1234-123456789abc"

    # Create multiple test run directories with the same test_id
    # Older directory (created first)
    older_dir = tmp_path / "20240101-120000-older"
    older_dir.mkdir()
    older_test_id_file = older_dir / "test_id"
    older_test_id_file.write_text(test_id)

    # Small delay to ensure different modification times
    time.sleep(0.1)

    # Newer directory (created later)
    newer_dir = tmp_path / "20240101-130000-newer"
    newer_dir.mkdir()
    newer_test_id_file = newer_dir / "test_id"
    newer_test_id_file.write_text(test_id)

    # Call get_testrun_dir and verify it returns the newer directory
    result = get_testrun_dir(base_dir=str(tmp_path), test_id=test_id)

    # Should return the newer directory (most recently modified)
    assert result == str(newer_dir), f"Expected {newer_dir}, got {result}"


def test_get_testrun_dir_returns_none_when_not_found(tmp_path):
    """Test that get_testrun_dir returns None when test_id is not found."""

    # Create a directory without any test_id file
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    # Call get_testrun_dir with a test_id that doesn't exist
    result = get_testrun_dir(base_dir=str(tmp_path), test_id="nonexistent-test-id")

    assert result is None, f"Expected None, got {result}"


def test_get_testrun_dir_returns_single_match(tmp_path):
    """Test that get_testrun_dir returns the directory when only one match is found."""

    test_id = "single-match-test-id"

    # Create a single test run directory
    test_dir = tmp_path / "20240101-140000-single"
    test_dir.mkdir()
    test_id_file = test_dir / "test_id"
    test_id_file.write_text(test_id)

    # Call get_testrun_dir
    result = get_testrun_dir(base_dir=str(tmp_path), test_id=test_id)

    assert result == str(test_dir), f"Expected {test_dir}, got {result}"
