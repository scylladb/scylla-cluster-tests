#!/usr/bin/env python

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
# Copyright (c) 2021 ScyllaDB

import logging
import os
import traceback
import contextlib

from typing import Optional

import pytest

from functional_tests.scylla_operator.libs.auxiliary import ScyllaOperatorFunctionalClusterTester, sct_abs_path
from sdcm.cluster_k8s import ScyllaPodCluster


TESTER: Optional[ScyllaOperatorFunctionalClusterTester] = None
LOGGER = logging.getLogger(__name__)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):  # pylint: disable=unused-argument
    # Populate test result to test function instance
    outcome = yield
    rep = outcome.get_result()
    if rep.passed:
        item._test_result = ('SUCCESS', None)  # pylint: disable=protected-access
    else:
        item._test_result = ('FAILED', str(rep.longrepr))  # pylint: disable=protected-access


@pytest.fixture(autouse=True)
def harvest_test_results(
        request, tester: ScyllaOperatorFunctionalClusterTester):  # pylint: disable=redefined-outer-name
    # Pickup test results at the end of the test and submit it to the tester

    def publish_test_result():
        tester.update_test_status(
            request.node.nodeid,
            *request.node._test_result)  # pylint: disable=protected-access

    request.addfinalizer(publish_test_result)


@pytest.fixture(autouse=True, scope='package')
def tester() -> ScyllaOperatorFunctionalClusterTester:
    os.chdir(sct_abs_path())
    tester_inst = ScyllaOperatorFunctionalClusterTester()
    tester_inst.setUpClass()
    tester_inst.setUp()
    yield tester_inst
    with contextlib.suppress(Exception):
        tester_inst.tearDown()
    with contextlib.suppress(Exception):
        tester_inst.tearDownClass()


@pytest.fixture(scope="function")
def change_test_dir(request):
    os.chdir(sct_abs_path())
    yield
    os.chdir(request.config.invocation_dir)


# pylint: disable=redefined-outer-name
def skip_if_cluster_requirements_not_met(
        request,
        tester: ScyllaOperatorFunctionalClusterTester,
        db_cluster: ScyllaPodCluster):
    require_node_terminate = request.node.get_closest_marker('require_node_terminate')
    require_mgmt = request.node.get_closest_marker('require_mgmt')
    if require_node_terminate and require_node_terminate.args:
        supported_methods = getattr(db_cluster, 'node_terminate_methods', None) or []
        for terminate_method in require_node_terminate.args:
            if terminate_method not in supported_methods:
                pytest.skip(f'cluster {type(db_cluster).__name__} does not support {terminate_method} '
                            'node termination method')
    if require_mgmt and not tester.params.get('use_mgmt'):
        pytest.skip('test require scylla manager to be deployed')


@pytest.fixture()
def db_cluster(tester: ScyllaOperatorFunctionalClusterTester):  # pylint: disable=redefined-outer-name
    if not tester.healthy_flag:
        pytest.skip('cluster is not healthy, skipping rest of the tests')

    with tester.db_cluster.scylla_config_map as scylla_config_map:
        original_scylla_config_map = scylla_config_map
    original_scylla_cluster_spec = tester.db_cluster.get_scylla_cluster_plain_value('/spec')

    yield tester.db_cluster

    if tester.healthy_flag:
        _bring_cluster_back_to_original_state(
            tester.db_cluster,
            config_map=original_scylla_config_map,
            scylla_cluster_spec=original_scylla_cluster_spec
        )


# pylint: disable=redefined-outer-name
def _bring_cluster_back_to_original_state(
        db_cluster: ScyllaPodCluster,
        config_map: dict,
        scylla_cluster_spec: dict
):
    restart = False
    try:
        if scylla_cluster_spec != db_cluster.get_scylla_cluster_plain_value('/spec'):
            restart = True
        db_cluster.replace_scylla_cluster_value('/spec', scylla_cluster_spec)
        with db_cluster.scylla_config_map as recover_config_map:
            if recover_config_map != config_map:
                # if config map is changed scylla will be restarted and we don't have to explicitly restart it
                restart = False
            recover_config_map.clear()
            recover_config_map.update(config_map)
        if restart:
            db_cluster.restart_scylla()
    except Exception as exc:  # pylint: disable=broad-except
        tester.healthy_flag = False
        pytest.fail("Failed to bring cluster nodes back to original number due to :\n" +
                    "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))


@pytest.fixture()
def cassandra_rackdc_properties(db_cluster: ScyllaPodCluster):  # pylint: disable=redefined-outer-name
    return db_cluster.remote_cassandra_rackdc_properties
