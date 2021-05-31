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

import pytest

log = logging.getLogger()


def test_cassandra_rackdc(cassandra_rackdc_properties):
    """
    Test of applying cassandra-rackdc.properties via configmap
    """

    with cassandra_rackdc_properties() as props:
        original = props['prefer_local']

    log.info(f"cassandra-rackdc.properties.prefer_local = {original}")

    if original == 'false':
        changed = 'true'
    elif original == 'true':
        changed = 'false'
    else:
        assert False, f'cassandra-rackdc.properties have unexpected prefer_local value {original}'

    with cassandra_rackdc_properties() as props:
        assert original == props['prefer_local']
        props['prefer_local'] = changed

    with cassandra_rackdc_properties() as props:
        assert changed == props['prefer_local']
        props['prefer_local'] = original

    with cassandra_rackdc_properties() as props:
        assert original == props['prefer_local']


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_rolling_restart_cluster(tester, db_cluster, nemesis):
    nemesis.disrupt_rolling_restart_cluster(random=False)


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_stop_start_scylla_server(tester, db_cluster, nemesis):
    nemesis.disrupt_stop_start_scylla_server()


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_grow_shrink_cluster(tester, db_cluster, nemesis):
    nemesis.disrupt_grow_shrink_cluster()


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_terminate_and_replace_node_kubernetes(tester, db_cluster, nemesis):
    nemesis.disrupt_terminate_and_replace_node_kubernetes()


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_terminate_decommission_add_node_kubernetes(tester, db_cluster, nemesis):
    nemesis.disrupt_terminate_decommission_add_node_kubernetes()


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_mgmt_repair_cli(tester, db_cluster, nemesis):
    nemesis.disrupt_mgmt_repair_cli()


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_mgmt_backup_specific_keyspaces(tester, db_cluster, nemesis):
    nemesis.disrupt_mgmt_backup_specific_keyspaces()


@pytest.mark.skip("Does not work at the moment")
def test_disrupt_mgmt_backup(tester, db_cluster, nemesis):
    nemesis.disrupt_mgmt_backup()
