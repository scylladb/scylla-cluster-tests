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

from __future__ import annotations

import time
import unittest
import tempfile
import logging
from typing import TYPE_CHECKING

import boto3
import requests

from sdcm.prometheus import start_metrics_server, nemesis_metrics_obj
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.setup import start_events_device, stop_events_device

from sdcm.stress_thread import CassandraStressThread, CassandraStressEventsPublisher
from sdcm.loader import CassandraStressExporter
from sdcm.ycsb_thread import YcsbStressThread

if TYPE_CHECKING:
    from mypy_boto3_ec2 import EC2Client


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)

# pylint: disable=line-too-long


class Node:  # pylint: disable=no-init,too-few-public-methods
    def __init__(self):
        self.ssh_login_info = {'hostname': '34.253.205.91',
                               'user': 'centos',
                               'key_file': '~/.ssh/scylla-qa-ec2'}

        self.remoter = RemoteCmdRunnerBase.create_remoter(**self.ssh_login_info)
        self.ip_address = '34.253.205.91'


class DbNode:  # pylint: disable=no-init,too-few-public-methods
    ip_address = "34.244.157.61"
    dc_idx = 1


class LoaderSetDummy:  # pylint: disable=no-init,too-few-public-methods
    def __init__(self):
        self.nodes = [Node()]

    @staticmethod
    def get_db_auth():
        return None

    name = 'LoaderSetDummy'


@unittest.skip("manual tests")
class EC2ClientTests(unittest.TestCase):
    @staticmethod
    def test_01():
        network_if = [{'DeviceIndex': 0,
                       'SubnetId': 'subnet-ad3ce9f4',
                       'AssociatePublicIpAddress': True,
                       'Groups': ['sg-5e79983a']}]
        user_data = '--clustername cluster-scale-test-xxx --bootstrap true --totalnodes 3'

        ec2: EC2Client = boto3.client("ec2", region_name="us-east1")
        instance_type = 'm3.medium'
        image_id = 'ami-56373b2d'
        avail_zone = ec2.get_subnet_info(network_if[0]['SubnetId'])['AvailabilityZone']

        inst = ec2.create_spot_instances(instance_type, image_id, avail_zone,
                                         network_if, user_data=user_data, count=2)
        print(inst)
        inst = ec2.create_spot_instances(instance_type, image_id, avail_zone, network_if, count=1, duration=60)
        print(inst)
        inst = ec2.create_spot_fleet(instance_type, image_id, avail_zone, network_if, user_data=user_data, count=2)
        print(inst)


@unittest.skip("manual tests")
class TestCassandraStressExporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prom_address = start_metrics_server()
        cls.metrics = nemesis_metrics_obj()

    def test_01(self):
        tmp_file = tempfile.NamedTemporaryFile(mode='w+')  # pylint: disable=consider-using-with
        cs_exporter = CassandraStressExporter("127.0.0.1", self.metrics, 'write',
                                              tmp_file.name, loader_idx=1, cpu_idx=0)

        res = cs_exporter.start()

        line = '[34.241.184.166] [stdout] total,      83086089,   70178,   70178,   70178,    14.2,    11.9,    33.2,    53.6,    77.7,   105.4, 1220.0,  0.00868,      0,      0,       0,       0,       0,       0'

        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()

        tmp_file.file.write(line[:30])
        tmp_file.file.flush()

        time.sleep(2)

        tmp_file.file.write(line[30:] + '\n')
        tmp_file.file.flush()

        tmp_file.file.write(line[30:])
        tmp_file.file.flush()

        time.sleep(2)
        output = requests.get("http://{}/metrics".format(self.prom_address)).text
        assert 'collectd_cassandra_stress_write_gauge{cassandra_stress_write="0",cpu_idx="0",instance="127.0.0.1",loader_idx="1",type="ops"} 70178.0' in output

        time.sleep(1)
        cs_exporter.stop()

        res.result()


class BaseSCTEventsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir)
        time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        stop_events_device()


@unittest.skip("manual tests")
class TestStressThread(BaseSCTEventsTest):
    def test_01(self):  # pylint: disable=no-self-use
        start_metrics_server()
        cstress = CassandraStressThread(LoaderSetDummy(), timeout=60, node_list=[DbNode()], stress_num=1,
                                        stress_cmd="cassandra-stress write cl=ONE duration=3m -schema 'replication(factor=3) compaction(strategy=SizeTieredCompactionStrategy)'"
                                        " -mode cql3 native -rate threads=1000 -pop seq=1..10000000 -log interval=5")

        cstress1 = CassandraStressThread(LoaderSetDummy(), timeout=60, node_list=[DbNode()],
                                         stress_num=1,
                                         stress_cmd="cassandra-stress write cl=ONE duration=3m -schema 'replication(factor=3) compaction(strategy=SizeTieredCompactionStrategy)'"
                                         " -mode cql3 native -rate threads=1000 -pop seq=1..10000000 -log interval=5")

        cstress.run()
        time.sleep(5)
        cstress1.run()
        time.sleep(60)
        logging.info("killing")
        Node().remoter.run(cmd='pgrep -f cassandra-stress | xargs -I{}  kill -TERM -{}', ignore_status=True)

        logging.info(cstress.verify_results())
        logging.info(cstress1.verify_results())

    @staticmethod
    def test_02():

        tmp_file = tempfile.NamedTemporaryFile(mode='w+')  # pylint: disable=consider-using-with
        tailer = CassandraStressEventsPublisher(node=Node(), cs_log_filename=tmp_file.name)

        res = tailer.start()
        bad_line = "Cannot achieve consistency level"
        line = '[34.241.184.166] [stdout] total,      83086089,   70178,   70178,   70178,    14.2,    11.9,    33.2,    53.6,    77.7,   105.4, 1220.0,  0.00868,      0,      0,       0,       0,       0,       0'

        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()

        tmp_file.file.write(bad_line + '\n')
        tmp_file.file.flush()
        time.sleep(2)
        tailer.stop()

        res.result()


@unittest.skip("manual tests")
class TestYcsbStressThread(BaseSCTEventsTest):
    def test_01(self):  # pylint: disable=no-self-use
        params = dict(alternator_port=8080)  # , dynamodb_primarykey_type='HASH_AND_RANGE')
        loader_set = LoaderSetDummy()
        thread1 = YcsbStressThread(loader_set,
                                   'bin/ycsb load dynamodb -P workloads/workloada -threads 250 -p recordcount=50000000 -p fieldcount=10 -p fieldlength=1024 -s', 70, 'None', node_list=[DbNode()],
                                   params=params)

        thread2 = YcsbStressThread(loader_set,
                                   'bin/ycsb run dynamodb -P workloads/workloada -threads 100 -p recordcount=50000000 -p fieldcount=10 -p fieldlength=1024 -p operationcount=10000 -s',
                                   70, 'None', node_list=[DbNode()],
                                   params=params)

        try:
            thread1.run()
            time.sleep(10)
            thread2.run()
            thread1.get_results()
        except Exception:
            logging.exception("failed")
            raise

        finally:
            loader_set.nodes[0].remoter.run('pgrep -f ycsb | xargs -I{}  kill -TERM -{}', ignore_status=True)
