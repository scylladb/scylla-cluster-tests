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
import contextlib
from typing import Optional

import pytest

from scylla_operator.libs.auxiliary import ScyllaOperatorFunctionalClusterTester
from sdcm import nemesis as nemesis_lib
from sdcm.cluster_k8s import ScyllaPodCluster


TESTER: Optional[ScyllaOperatorFunctionalClusterTester] = None


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Populate test result to test function instance
    outcome = yield
    rep = outcome.get_result()
    if rep.passed:
        item._test_result = ('SUCCESS', None)
    else:
        item._test_result = ('FAILED', str(rep.longrepr))


@pytest.fixture(autouse=True)
def harvest_test_results(request, tester):
    # Pickup test results at the end of the test and submit it to the tester

    def publish_test_result():
        tester.update_test_status(request.node.nodeid, *request.node._test_result)

    request.addfinalizer(publish_test_result)
    return None


@pytest.fixture(autouse=True, scope='package')
def tester():
    tester = ScyllaOperatorFunctionalClusterTester()
    tester.setUpClass()
    tester.setUp()
    yield tester
    with contextlib.suppress(Exception):
        tester.tearDown()
    with contextlib.suppress(Exception):
        tester.tearDownClass()


@pytest.fixture(autouse=True)
def nemesis(tester, db_cluster):
    nemesis = nemesis_lib.DecommissionMonkey(tester_obj=tester, termination_event=db_cluster.nemesis_termination_event)
    nemesis.set_target_node()
    return nemesis


@pytest.fixture()
def db_cluster(tester: ScyllaOperatorFunctionalClusterTester):
    tester.db_cluster.check_cluster_health()
    yield tester.db_cluster
    tester.db_cluster.check_cluster_health()


@pytest.fixture()
def cassandra_rackdc_properties(db_cluster: ScyllaPodCluster):
    return db_cluster.remote_cassandra_rackdc_properties
