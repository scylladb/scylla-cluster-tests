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
# Copyright (c) 2022 ScyllaDB

import uuid

import pytest

from sdcm.logcollector import Collector
from sdcm.provision import provisioner_factory
from unit_tests.lib.fake_resources import prepare_fake_region


@pytest.fixture(scope='session')
def test_id():
    return f"{str(uuid.uuid4())}"


def test_create_collecting_nodes(test_id, tmp_path_factory):
    test_dir = tmp_path_factory.mktemp("log-collector")
    prepare_fake_region(test_id, "region_1", n_db_nodes=3, n_loaders=2, n_monitor_nodes=1)
    collector = Collector(test_id=test_id, test_dir=test_dir,
                          params={"cluster_backend": "fake", "use_cloud_manager": False})
    collector.create_collecting_nodes()
    provisioner = provisioner_factory.discover_provisioners(backend="fake", test_id=test_id)[0]

    db_nodes = [v_m for v_m in provisioner.list_instances() if v_m.tags.get("NodeType") == "scylla-db"]
    assert len(collector.db_cluster) == len(db_nodes)
    for collecting_node, v_m in zip(collector.db_cluster, db_nodes):
        assert collecting_node.name == v_m.name

    loader_nodes = [v_m for v_m in provisioner.list_instances() if v_m.tags.get("NodeType") == "loader"]
    assert len(collector.loader_set) == len(loader_nodes)
    for collecting_node, v_m in zip(collector.loader_set, loader_nodes):
        assert collecting_node.name == v_m.name

    monitor_nodes = [v_m for v_m in provisioner.list_instances() if v_m.tags.get("NodeType") == "monitor"]
    assert len(collector.monitor_set) == len(monitor_nodes)
    for collecting_node, v_m in zip(collector.monitor_set, monitor_nodes):
        assert collecting_node.name == v_m.name
