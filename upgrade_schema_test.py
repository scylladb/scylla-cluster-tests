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
# Copyright (c) 2016 ScyllaDB

# TODO: this test seem to be broken

import random
import struct
from unittest import TestCase

from cassandra.cluster import Cluster
from thrift.transport import TSocket

from sdcm.nemesis import UpgradeNemesisOneNode
from sdcm.tester import ClusterTester
from thrift_bindings.v22.Cassandra import *
from thrift_bindings.v22.ttypes import NotFoundException

tests = []
ks_name = 'test_upgrade_schema_ks'
thrift_client = None
cql_client = None


def test(test):
    tests.append(test())


def _i32(n):
    return struct.pack('>i', n)  # big endian = network order


def _i64(n):
    return struct.pack('>q', n)  # big endian = network order


def describe_table(ks, table):
    for cf in thrift_client.describe_keyspace(ks).cf_defs:
        if cf.name == table:
            return cf


class Test(TestCase):
    def setUp(self):
        pass

    # Can be called multiple times after setUp so must be idempotent.
    def runTest(self):
        pass


@test
class TestNonCompoundThriftDynamicTable(Test):
    keys = ['key%d' % i for i in range(10)]

    def setUp(self):
        cf = CfDef(ks_name, 'test_table0',
                   key_validation_class='UTF8Type',
                   comparator_type='LongType',
                   default_validation_class='Int32Type')
        thrift_client.system_add_column_family(cf)
        for key in self.keys:
            thrift_client.insert(key, ColumnParent('test_table0'), Column(_i64(1), _i32(1), 1), ConsistencyLevel.ONE)

    def runTest(self):
        for key in self.keys:
            self.assertEqual(
                thrift_client.get(key, ColumnPath('test_table0', column=_i64(1)), ConsistencyLevel.ONE).column,
                Column(_i64(1), _i32(1), 1))


@test
class TestCqlTableMigration(Test):
    def _insertData(self):
        cql_client.execute("INSERT INTO test1 (k, ck, value, value2) VALUES (0, 0, 1, 2)")
        cql_client.execute("INSERT INTO test1 (k, ck, value, value2) VALUES (1, 0, 1, 2)")
        cql_client.execute("INSERT INTO test1 (k, ck, value, value2) VALUES (2, 0, 1, 2)")

        cql_client.execute("INSERT INTO test2 (k, v1, v2) VALUES (0, 0, 1)")
        cql_client.execute("INSERT INTO test2 (k, v1, v2) VALUES (1, 0, 1)")
        cql_client.execute("INSERT INTO test2 (k, v1, v2) VALUES (2, 0, 1)")

        cql_client.execute("INSERT INTO test3 (k, v1, v2) VALUES (0, 0, 1)")
        cql_client.execute("INSERT INTO test3 (k, v1, v2) VALUES (1, 0, 1)")
        cql_client.execute("INSERT INTO test3 (k, v1, v2) VALUES (2, 0, 1)")

        cql_client.execute("INSERT INTO test4 (k, ck, ck1, v) VALUES (0, 0, 1, 2)")
        cql_client.execute("INSERT INTO test4 (k, ck, ck1, v) VALUES (1, 0, 1, 2)")
        cql_client.execute("INSERT INTO test4 (k, ck, ck1, v) VALUES (2, 0, 1, 2)")

        cql_client.execute("INSERT INTO test5 (k, ck, value) VALUES (0, 0, 1)")
        cql_client.execute("INSERT INTO test5 (k, ck, value) VALUES (1, 0, 1)")
        cql_client.execute("INSERT INTO test5 (k, ck, value) VALUES (2, 0, 1)")

        cql_client.execute("INSERT INTO test6 (k) VALUES (0)")
        cql_client.execute("INSERT INTO test6 (k) VALUES (1)")
        cql_client.execute("INSERT INTO test6 (k) VALUES (2)")

        cql_client.execute("INSERT INTO test7 (k, ck) VALUES (0, 0)")
        cql_client.execute("INSERT INTO test7 (k, ck) VALUES (1, 0)")
        cql_client.execute("INSERT INTO test7 (k, ck) VALUES (2, 0)")

    def _updateCounters(self, delta=1):
        cql_client.execute("UPDATE test8 set v = v + %d where k = 0 and ck = 0" % delta)
        cql_client.execute("UPDATE test8 set v = v + %d where k = 1 and ck = 0" % delta)
        cql_client.execute("UPDATE test8 set v = v + %d where k = 2 and ck = 0" % delta)

        cql_client.execute("UPDATE test9 set v = v + %d where k = 0 and ck = 0" % delta)
        cql_client.execute("UPDATE test9 set v = v + %d where k = 1 and ck = 0" % delta)
        cql_client.execute("UPDATE test9 set v = v + %d where k = 2 and ck = 0" % delta)

        cql_client.execute("UPDATE test10 set v = v + %d where k = 0" % delta)
        cql_client.execute("UPDATE test10 set v = v + %d where k = 1" % delta)
        cql_client.execute("UPDATE test10 set v = v + %d where k = 2" % delta)

        cql_client.execute("UPDATE test11 set v = v + %d where k = 0" % delta)
        cql_client.execute("UPDATE test11 set v = v + %d where k = 1" % delta)
        cql_client.execute("UPDATE test11 set v = v + %d where k = 2" % delta)

    def setUp(self):
        cql_client.execute("create table test1 (k int, ck int, value int, value2 int, primary key (k, ck))")
        cql_client.execute("create table test2 (k int, v1 int, v2 int, primary key (k))")
        cql_client.execute("create table test3 (k int, v1 int, v2 int, primary key (k)) with compact storage")
        cql_client.execute(
            "create table test4 (k int, ck int, ck1 int, v int, primary key (k, ck, ck1)) with compact storage")
        cql_client.execute("create table test5 (k int, ck int, value int, primary key (k, ck)) with compact storage")
        cql_client.execute("create table test6 (k int primary key)")
        cql_client.execute("create table test7 (k int, ck int, primary key (k, ck)) with compact storage")
        cql_client.execute("create table test8 (k int, ck int, v counter, primary key (k, ck))")
        cql_client.execute("create table test9 (k int, ck int, v counter, primary key (k, ck)) with compact storage")
        cql_client.execute("create table test10 (k int, v counter, primary key (k))")
        cql_client.execute("create table test11 (k int, v counter, primary key (k)) with compact storage")
        self._insertData()
        self._updateCounters()

    def runTest(self):
        self.assertEqual(set(cql_client.execute("SELECT * FROM test1")), {(1, 0, 1, 2), (0, 0, 1, 2), (2, 0, 1, 2)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test2")), {(1, 0, 1), (0, 0, 1), (2, 0, 1)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test3")), {(1, 0, 1), (0, 0, 1), (2, 0, 1)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test4")), {(1, 0, 1, 2), (0, 0, 1, 2), (2, 0, 1, 2)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test5")), {(1, 0, 1), (0, 0, 1), (2, 0, 1)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test6")), {(1,), (0,), (2,)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test7")), {(1, 0), (0, 0), (2, 0)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test8")), {(1, 0, 1), (0, 0, 1), (2, 0, 1)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test9")), {(1, 0, 1), (0, 0, 1), (2, 0, 1)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test10")), {(1, 1), (0, 1), (2, 1)})
        self.assertEqual(set(cql_client.execute("SELECT * FROM test11")), {(1, 1), (0, 1), (2, 1)})
        self._insertData()
        self._updateCounters()
        self._updateCounters(-1)


# Fails for Scylla 1.7 -> 2.0 migration due to https://github.com/scylladb/scylla/issues/2573
@test
class TestNonStandardComparator(Test):
    keys = ['key%d' % i for i in range(10)]

    def _insertData(self):
        for key in self.keys:
            thrift_client.insert(key, ColumnParent('test_table'), Column(_i64(4), _i32(1), 1), ConsistencyLevel.ONE)
            thrift_client.insert(key, ColumnParent('test_table'), Column(_i64(5), _i32(2), 2), ConsistencyLevel.ONE)

    def setUp(self):
        cf = CfDef(ks_name, 'test_table',
                   column_metadata=[
                       ColumnDef(_i64(4), 'Int32Type'),
                       ColumnDef(_i64(5), 'Int32Type'),
                       # Fails due to https://github.com/scylladb/scylla/issues/2573
                       # ColumnDef(_i64(-2), 'Int32Type')
                   ],
                   key_validation_class='UTF8Type',
                   comparator_type='LongType',
                   default_validation_class='UTF8Type')
        thrift_client.system_add_column_family(cf)
        self._insertData()

    def runTest(self):
        for key in self.keys:
            assert thrift_client.get(key, ColumnPath('test_table', column=_i64(4)),
                                     ConsistencyLevel.ONE).column == Column(_i64(4), _i32(1), 1)
            assert thrift_client.get(key, ColumnPath('test_table', column=_i64(5)),
                                     ConsistencyLevel.ONE).column == Column(_i64(5), _i32(2), 2)
        self._insertData()

        cdef = describe_table(ks_name, 'test_table')

        # Fails due to https://github.com/scylladb/scylla/issues/2573
        # assert cdef.comparator_type == "org.apache.cassandra.db.marshal.LongType"
        # Fails due to https://github.com/scylladb/scylla/issues/1474
        # assert cdef.default_validation_class == "org.apache.cassandra.db.marshal.UTF8Type"
        assert cdef.key_validation_class == "org.apache.cassandra.db.marshal.UTF8Type"


# Fails due to https://github.com/scylladb/scylla/issues/2588
# @test
class TestThriftTableConvertedFromDenseToSparse(Test):
    keys = ['key%d' % i for i in range(10)]

    def setUp(self):
        cf = CfDef(ks_name, 'test_table2',
                   key_validation_class='UTF8Type',
                   comparator_type='LongType',
                   default_validation_class='UTF8Type')
        thrift_client.system_add_column_family(cf)
        for key in self.keys:
            thrift_client.insert(key, ColumnParent('test_table2'), Column(_i64(1), _i32(1), 1), ConsistencyLevel.ONE)
            thrift_client.insert(key, ColumnParent('test_table2'), Column(_i64(2), _i32(1), 1), ConsistencyLevel.ONE)
            thrift_client.insert(key, ColumnParent('test_table2'), Column(_i64(3), _i32(1), 1), ConsistencyLevel.ONE)
            thrift_client.insert(key, ColumnParent('test_table2'), Column(_i64(4), _i32(1), 1), ConsistencyLevel.ONE)
        cf.column_metadata = [
            ColumnDef(_i64(4), 'Int32Type')
        ]
        thrift_client.system_update_column_family(cf)

    def runTest(self):
        for key in self.keys:
            self.assertEqual(
                thrift_client.get(key, ColumnPath('test_table2', column=_i64(1)), ConsistencyLevel.ONE).column,
                Column(_i64(1), _i32(1), 1))
            self.assertEqual(
                thrift_client.get(key, ColumnPath('test_table2', column=_i64(2)), ConsistencyLevel.ONE).column,
                Column(_i64(2), _i32(1), 1))
            self.assertEqual(
                thrift_client.get(key, ColumnPath('test_table2', column=_i64(3)), ConsistencyLevel.ONE).column,
                Column(_i64(3), _i32(1), 1))
            self.assertEqual(
                thrift_client.get(key, ColumnPath('test_table2', column=_i64(4)), ConsistencyLevel.ONE).column,
                Column(_i64(4), _i32(1), 1))


class UpgradeSchemaTest(ClusterTester):
    """
    Test a Scylla cluster upgrade.
    """

    default_params = {'timeout': 650000}

    def _get_thrift_client(self, host, port=9160):  # 9160
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        c = Client(protocol)
        c.transport = transport
        transport.open()
        return c

    def test_upgrade_schema(self):

        global thrift_client  # noqa: PLW0603
        global cql_client  # noqa: PLW0603
        ips = []
        for node in self.db_cluster.nodes:
            ips.append(node.public_ip_address)
        cluster = Cluster(contact_points=ips, executor_threads=1, max_schema_agreement_wait=30)
        cql_client = cluster.connect()

        thrift_client = self._get_thrift_client(self.db_cluster.nodes[0].public_ip_address)

        try:
            thrift_client.describe_keyspace(ks_name)
            cql_client.execute("drop keyspace \"%s\"" % ks_name)
        except NotFoundException:
            pass
        thrift_client.system_add_keyspace(
            KsDef(ks_name, 'org.apache.cassandra.locator.NetworkTopologyStrategy', {'replication_factor': '1'}, cf_defs=[]))
        thrift_client.set_keyspace(ks_name)
        cql_client.set_keyspace(ks_name)

        for test in tests:
            test.setUp()
        for test in tests:
            test.runTest()
        thrift_client.transport.flush()
        thrift_client.transport.close()
        cql_client.shutdown()
        cql_client = None

        duration = 15 * len(self.db_cluster.nodes)

        stress_cmd = self._cs_add_node_flag(self.params.get('stress_cmd'))
        cs_thread_pool = self.run_stress_thread(stress_cmd=stress_cmd,
                                                duration=duration)

        l = len(self.db_cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(l)]
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            self.db_cluster.node_to_upgrade = self.db_cluster.nodes[i]
            self.log.info("started upgrade node {0}".format(self.db_cluster.node_to_upgrade))
            self.db_cluster.add_nemesis(nemesis=UpgradeNemesisOneNode,
                                        tester_obj=self)
            self.db_cluster.start_nemesis(interval=15)
            # 15 minutes to upgrade 1 node
            self.db_cluster.stop_nemesis(timeout=15 * 60)

            thrift_client = self._get_thrift_client(self.db_cluster.node_to_upgrade.public_ip_address)
            cluster = Cluster(contact_points=ips, executor_threads=1)
            cql_client = cluster.connect()
            thrift_client.set_keyspace(ks_name)
            cql_client.set_keyspace(ks_name)
            for test in tests:
                test.runTest()
            thrift_client.transport.flush()
            thrift_client.transport.close()
            cql_client.shutdown()
            cql_client = None
        self.log.info("test_upgrade_schema completed without errors")
        self.verify_stress_thread(cs_thread_pool)
