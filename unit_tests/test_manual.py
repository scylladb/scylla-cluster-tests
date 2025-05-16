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
from sdcm.loader import CassandraStressExporter, CassandraStressHDRExporter
from sdcm.ycsb_thread import YcsbStressThread

if TYPE_CHECKING:
    from mypy_boto3_ec2 import EC2Client


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class Node:
    def __init__(self):
        self.ssh_login_info = {'hostname': '34.253.205.91',
                               'user': 'centos',
                               'key_file': '~/.ssh/scylla_test_id_ed25519'}

        self.remoter = RemoteCmdRunnerBase.create_remoter(**self.ssh_login_info)
        self.ip_address = '34.253.205.91'


class DbNode:
    ip_address = "34.244.157.61"
    dc_idx = 1


class LoaderSetDummy:
    def __init__(self):
        self.nodes = [Node()]
        self.params = {}

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
        tmp_file = tempfile.NamedTemporaryFile(mode='w+')
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
        assert 'sct_cassandra_stress_write_gauge{cassandra_stress_write="0",cpu_idx="0",instance="127.0.0.1",loader_idx="1",type="ops"} 70178.0' in output

        time.sleep(1)
        cs_exporter.stop()

        res.result()


@unittest.skip("manual tests")
class TestCassandraStressHDRExporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prom_address = start_metrics_server()
        cls.metrics = nemesis_metrics_obj()

    def test_01_mixed(self):
        tmp_file = tempfile.NamedTemporaryFile(mode='w+')
        cs_exporter = CassandraStressHDRExporter("127.0.0.1", self.metrics, 'mixed',
                                                 tmp_file.name, loader_idx=1, cpu_idx=0)

        res = cs_exporter.start()

        line = 'Tag=WRITE-rt,0.000,5.000,54.329,HISTFAAAA/R42i1Uy47cRBQt3y5XV3vcxt1d8Xgcxxi3KSxjHMfpWKYz5IHCRDwiFCGEUIQQiwghlogVCzZILFhGWUQIIRaIBSvEGvEBfAHiC9jxB8O57qnuqvKtc++59ThV6TdPNkK4f4hDmV30zmScn5/f/u8w8Of3Dr1Iz9NlWpEgukRHJKRckKuWbHOd4S9RXFIkGRXkYnxGSpIUeiaVJi0UQPZA4XEltPBIS/LAgAghtULDdIC1FAouJJSHnkDucxiK4hgNZ6lRfYal70tfo/hKyUCSUeSHWksPnlIjiR8oFQZrE5o4TAr0mVmnUZyHUZZ4iQmjdZrEUZQkVZ0nWRLnUVEWWWIzk6RFV/hZWfVZbm1W5WlWpmOeFF2V2bztG5t3XdPuh6be19lQlk03jvumbevdYIey6++09TgU49i31b3dnbo53VVNP/Q72/Xj6/as3I/1kDVddy/t+tPWjsPOlkN51vZd079TvNXtz/bjrb447fouL+q+3dd9nFR9miPpbqh23dg0dqiqqi6qelc2Rd12XTe0tr1VDG1dNk3RtGVTd4VNq7qubNumpi7zOikb25exraosK6o8K9IsK/ukjps8TlvbpI2NsjRKbWWiPonTNAlzs8YuRUlkbBgGEay0MPE6x/7GmYnjOAUaRGEYB8ZIs7aeWftxGEUmWvsogR8avfaNVkZFYRx6XiADE4TaD1kha208H0encOYhTk/hwPlbBZ72ICqFA/fAAgRHq8mXk5K08iEM6A3DxNEwoCjNKAHV0Ch4lIZ2pD6IkiQLVrAipceqUyy6mRLggQonmEUnOAPLULI96Zwm9pkE7UKyXmfKBcIiRoSYvPEtCR9sHiyFG4FY+PEF4VvjsuqnhIdLxB2JCUAel6/EarJBu+SEh6ALFNdvMV28ORqe1MWixBQy42hA8KINLiw7cm54YvBoItqgfQ49aFBn0+KmUIHAE9oCml/MZzGFIA8teRKImPMmXJlI53RZrkhusZwFjM005OKdwMcxfux/A0tZ0EYe0yVM+wryIhs3N9HOAW8BC8S6yLRE3RBdBXQNwBF9DvCJgyfoCn0C1KX7iLtBH9BjzPMxHidaSn6WXqProDwB9INDD+glMC9RjxFxFRHyZRhfor5L79FnSLGa0t0GEX1NK7mhR/Stg4kvpwU8gN9DJN0iwZaeOfQRfUW/O8jwJr0AwqcOKH5yMKU5/eJg6IT+cjCHh7ybSM6btaWnh127DqebBLbfHDykM7hdw/IQdER3iT7G9r+P0TfwW2H69+lHXu6/zvTaHuNzDtolpo0M/zj0zMU6fnXob6K36VP62aFXCBkfYS30BbzoVZB9R/Qh/Q8V5nMQ'
        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()
        time.sleep(2)

        line = 'Tag=READ-rt,0.999,5.000,60.588,HISTFAAAA+F42j1Uv6/dNBROTnz9/Pz8/IxfmmfS4KapSfNCuETR7e2lPGiXqkOFEKoqBoaKATEyMSE2mBDq0AExVLDwJyCEmJgQM2LkL2DhP3h89gUSxfb59X3H9jlpPn92mmWr37L9U/w750m4vLy8+/de8c13Od2kjM4ZnRDLqKCCMWKENct4RhAzwgwxaohTJmDGxBjjTHCecYGXC8YEE0ozLjWXVkmurBQq+khjpTFKV5aXUMrWl1IbVUJRVsYbZ6wvrdGybLRxTeM629TK9GNfu7r1g61bVXelbHvnXNWEpvbW14Nvu+D7yU5+68ZpO3bDehknvyy7dRO6sGznobnTh6mf1hf9rl2H9bgN5Th1y2bqNsvFPG/DsN3O07DsuuXezl8MgNkt3bxxfp4298I4D35zcWcZ5k3ft+6tadzs2nmYwtCG4P3U1d3Ydt3omqHrwlSFcWyX0fUuTLWvwjCt2z4gsJ+W0COs8eu57Ta9vwhNMzRLOZk1OFzfYddtMGNbd86FfvZBNjgUX7eTqofKh6oOlW7LWmscgK+crqquKvHaujJlg7PR8WyMhUpZHGZTaVPaSpeuspW02jpnSl1r19cNPHRpjDYkJS7GOqWlkKKqDNdcU62qUimppIyRRljQylKT0CXHLXLcrJGVdfCVioTQSijDpORCGM1LziXuXSj4cq40FpI0k0oJlIYlJoTgoONgFODjkUiksjGWRyMTGtWkhWVcodTgAggUmERVcgZkjKi2UgmMMDCRwQVr1K2ATTKNUkViMMUHE4lYrLDKvRsKmHFBBLKMJAEAHjKqY5nzIjJQDBUZ4xRrPZY6TOgLQqoYZXRMnZBaIYurgmKLFDw2DACTJaOjSIInef3XXGi5Fb4DigDRP7Za6jrEs9iDMQDNB2bwRICo4hGbyWREuyXYLB5MkULRp+CM7JErpoYj+b+BozfoDiNaauaMFSkgou8/JLiKCr6XYpvHOM5XdJjgDvCu0kYhnBEdQU67eSHuBYYTfHsXmAvY6RgDS9h0yKI+/mXwsf0BILUixV2hqxgT8TGShCnq6RSaKwn0Vayvw3KXMDyE6pweI+IA+Kf0Gt1PhK/Qx3SLvs+B8QhpHAL0FPNVdgbzEb2EROlN+K3oNr1HH0F5Tr/m6Treh+Ea7H9F8RZCz6C6SV8QvUtPCaEP2NOcbhC9DpYb9AbelwG/onfoy5wuGeB/yukPQsy3RD/n9Al9ABQQvogcjul3AvwBfZ3TA8h/FsC5hg09z9kjbAQh18F6GzxvA+kk7uFZDsN9+gzSY/olpyegfJ4T/bCCzxP6Kqcfc/qQPgXOP5zIbjo='
        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()
        time.sleep(2)

        output = requests.get("http://{}/metrics".format(self.prom_address)).text
        expected_lines = output.split("\n")[-12:]

        assert 'collectd_cassandra_stress_hdr_mixed_gauge{cassandra_stress_hdr_mixed="WRITE",cpu_idx="0",instance="127.0.0.1",keyspace="",loader_idx="1",type="lat_perc_50"} 0.62' in expected_lines
        assert 'collectd_cassandra_stress_hdr_mixed_gauge{cassandra_stress_hdr_mixed="READ",cpu_idx="0",instance="127.0.0.1",keyspace="",loader_idx="1",type="lat_perc_999"} 36.54' in expected_lines
        time.sleep(1)
        cs_exporter.stop()

        res.result(10)

    def test_02_write(self):
        tmp_file = tempfile.NamedTemporaryFile(mode='w+')
        cs_exporter = CassandraStressHDRExporter("127.0.0.1", self.metrics, 'write',
                                                 tmp_file.name, loader_idx=1, cpu_idx=0)

        res = cs_exporter.start()

        line = 'Tag=WRITE-rt,0.000,5.000,54.329,HISTFAAAA/R42i1Uy47cRBQt3y5XV3vcxt1d8Xgcxxi3KSxjHMfpWKYz5IHCRDwiFCGEUIQQiwghlogVCzZILFhGWUQIIRaIBSvEGvEBfAHiC9jxB8O57qnuqvKtc++59ThV6TdPNkK4f4hDmV30zmScn5/f/u8w8Of3Dr1Iz9NlWpEgukRHJKRckKuWbHOd4S9RXFIkGRXkYnxGSpIUeiaVJi0UQPZA4XEltPBIS/LAgAghtULDdIC1FAouJJSHnkDucxiK4hgNZ6lRfYal70tfo/hKyUCSUeSHWksPnlIjiR8oFQZrE5o4TAr0mVmnUZyHUZZ4iQmjdZrEUZQkVZ0nWRLnUVEWWWIzk6RFV/hZWfVZbm1W5WlWpmOeFF2V2bztG5t3XdPuh6be19lQlk03jvumbevdYIey6++09TgU49i31b3dnbo53VVNP/Q72/Xj6/as3I/1kDVddy/t+tPWjsPOlkN51vZd079TvNXtz/bjrb447fouL+q+3dd9nFR9miPpbqh23dg0dqiqqi6qelc2Rd12XTe0tr1VDG1dNk3RtGVTd4VNq7qubNumpi7zOikb25exraosK6o8K9IsK/ukjps8TlvbpI2NsjRKbWWiPonTNAlzs8YuRUlkbBgGEay0MPE6x/7GmYnjOAUaRGEYB8ZIs7aeWftxGEUmWvsogR8avfaNVkZFYRx6XiADE4TaD1kha208H0encOYhTk/hwPlbBZ72ICqFA/fAAgRHq8mXk5K08iEM6A3DxNEwoCjNKAHV0Ch4lIZ2pD6IkiQLVrAipceqUyy6mRLggQonmEUnOAPLULI96Zwm9pkE7UKyXmfKBcIiRoSYvPEtCR9sHiyFG4FY+PEF4VvjsuqnhIdLxB2JCUAel6/EarJBu+SEh6ALFNdvMV28ORqe1MWixBQy42hA8KINLiw7cm54YvBoItqgfQ49aFBn0+KmUIHAE9oCml/MZzGFIA8teRKImPMmXJlI53RZrkhusZwFjM005OKdwMcxfux/A0tZ0EYe0yVM+wryIhs3N9HOAW8BC8S6yLRE3RBdBXQNwBF9DvCJgyfoCn0C1KX7iLtBH9BjzPMxHidaSn6WXqProDwB9INDD+glMC9RjxFxFRHyZRhfor5L79FnSLGa0t0GEX1NK7mhR/Stg4kvpwU8gN9DJN0iwZaeOfQRfUW/O8jwJr0AwqcOKH5yMKU5/eJg6IT+cjCHh7ybSM6btaWnh127DqebBLbfHDykM7hdw/IQdER3iT7G9r+P0TfwW2H69+lHXu6/zvTaHuNzDtolpo0M/zj0zMU6fnXob6K36VP62aFXCBkfYS30BbzoVZB9R/Qh/Q8V5nMQ'
        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()
        time.sleep(2)

        output = requests.get("http://{}/metrics".format(self.prom_address)).text
        expected_lines = output.split("\n")[-6:]
        assert 'collectd_cassandra_stress_hdr_write_gauge{cassandra_stress_hdr_write="WRITE",cpu_idx="0",instance="127.0.0.1",keyspace="",loader_idx="1",type="lat_perc_50"} 0.62' in expected_lines
        time.sleep(1)
        cs_exporter.stop()

        res.result(10)

    def test_03_read(self):
        tmp_file = tempfile.NamedTemporaryFile(mode='w+')
        cs_exporter = CassandraStressHDRExporter("127.0.0.1", self.metrics, 'read',
                                                 tmp_file.name, loader_idx=1, cpu_idx=0)

        res = cs_exporter.start()

        line = 'Tag=READ-rt,0.999,5.000,60.588,HISTFAAAA+F42j1Uv6/dNBROTnz9/Pz8/IxfmmfS4KapSfNCuETR7e2lPGiXqkOFEKoqBoaKATEyMSE2mBDq0AExVLDwJyCEmJgQM2LkL2DhP3h89gUSxfb59X3H9jlpPn92mmWr37L9U/w750m4vLy8+/de8c13Od2kjM4ZnRDLqKCCMWKENct4RhAzwgwxaohTJmDGxBjjTHCecYGXC8YEE0ozLjWXVkmurBQq+khjpTFKV5aXUMrWl1IbVUJRVsYbZ6wvrdGybLRxTeM629TK9GNfu7r1g61bVXelbHvnXNWEpvbW14Nvu+D7yU5+68ZpO3bDehknvyy7dRO6sGznobnTh6mf1hf9rl2H9bgN5Th1y2bqNsvFPG/DsN3O07DsuuXezl8MgNkt3bxxfp4298I4D35zcWcZ5k3ft+6tadzs2nmYwtCG4P3U1d3Ydt3omqHrwlSFcWyX0fUuTLWvwjCt2z4gsJ+W0COs8eu57Ta9vwhNMzRLOZk1OFzfYddtMGNbd86FfvZBNjgUX7eTqofKh6oOlW7LWmscgK+crqquKvHaujJlg7PR8WyMhUpZHGZTaVPaSpeuspW02jpnSl1r19cNPHRpjDYkJS7GOqWlkKKqDNdcU62qUimppIyRRljQylKT0CXHLXLcrJGVdfCVioTQSijDpORCGM1LziXuXSj4cq40FpI0k0oJlIYlJoTgoONgFODjkUiksjGWRyMTGtWkhWVcodTgAggUmERVcgZkjKi2UgmMMDCRwQVr1K2ATTKNUkViMMUHE4lYrLDKvRsKmHFBBLKMJAEAHjKqY5nzIjJQDBUZ4xRrPZY6TOgLQqoYZXRMnZBaIYurgmKLFDw2DACTJaOjSIInef3XXGi5Fb4DigDRP7Za6jrEs9iDMQDNB2bwRICo4hGbyWREuyXYLB5MkULRp+CM7JErpoYj+b+BozfoDiNaauaMFSkgou8/JLiKCr6XYpvHOM5XdJjgDvCu0kYhnBEdQU67eSHuBYYTfHsXmAvY6RgDS9h0yKI+/mXwsf0BILUixV2hqxgT8TGShCnq6RSaKwn0Vayvw3KXMDyE6pweI+IA+Kf0Gt1PhK/Qx3SLvs+B8QhpHAL0FPNVdgbzEb2EROlN+K3oNr1HH0F5Tr/m6Treh+Ea7H9F8RZCz6C6SV8QvUtPCaEP2NOcbhC9DpYb9AbelwG/onfoy5wuGeB/yukPQsy3RD/n9Al9ABQQvogcjul3AvwBfZ3TA8h/FsC5hg09z9kjbAQh18F6GzxvA+kk7uFZDsN9+gzSY/olpyegfJ4T/bCCzxP6Kqcfc/qQPgXOP5zIbjo='
        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()
        time.sleep(2)

        output = requests.get("http://{}/metrics".format(self.prom_address)).text
        expected_lines = output.split("\n")[-6:]
        assert 'collectd_cassandra_stress_hdr_read_gauge{cassandra_stress_hdr_read="READ",cpu_idx="0",instance="127.0.0.1",keyspace="",loader_idx="1",type="lat_perc_999"} 36.54' in expected_lines
        time.sleep(1)
        cs_exporter.stop()

        res.result(10)

    def test_04_mixed_only_write(self):
        tmp_file = tempfile.NamedTemporaryFile(mode='w+')
        cs_exporter = CassandraStressHDRExporter("127.0.0.1", self.metrics, 'mixed',
                                                 tmp_file.name, loader_idx=1, cpu_idx=0)

        res = cs_exporter.start()

        line = 'Tag=WRITE-rt,0.000,5.000,54.329,HISTFAAAA/R42i1Uy47cRBQt3y5XV3vcxt1d8Xgcxxi3KSxjHMfpWKYz5IHCRDwiFCGEUIQQiwghlogVCzZILFhGWUQIIRaIBSvEGvEBfAHiC9jxB8O57qnuqvKtc++59ThV6TdPNkK4f4hDmV30zmScn5/f/u8w8Of3Dr1Iz9NlWpEgukRHJKRckKuWbHOd4S9RXFIkGRXkYnxGSpIUeiaVJi0UQPZA4XEltPBIS/LAgAghtULDdIC1FAouJJSHnkDucxiK4hgNZ6lRfYal70tfo/hKyUCSUeSHWksPnlIjiR8oFQZrE5o4TAr0mVmnUZyHUZZ4iQmjdZrEUZQkVZ0nWRLnUVEWWWIzk6RFV/hZWfVZbm1W5WlWpmOeFF2V2bztG5t3XdPuh6be19lQlk03jvumbevdYIey6++09TgU49i31b3dnbo53VVNP/Q72/Xj6/as3I/1kDVddy/t+tPWjsPOlkN51vZd079TvNXtz/bjrb447fouL+q+3dd9nFR9miPpbqh23dg0dqiqqi6qelc2Rd12XTe0tr1VDG1dNk3RtGVTd4VNq7qubNumpi7zOikb25exraosK6o8K9IsK/ukjps8TlvbpI2NsjRKbWWiPonTNAlzs8YuRUlkbBgGEay0MPE6x/7GmYnjOAUaRGEYB8ZIs7aeWftxGEUmWvsogR8avfaNVkZFYRx6XiADE4TaD1kha208H0encOYhTk/hwPlbBZ72ICqFA/fAAgRHq8mXk5K08iEM6A3DxNEwoCjNKAHV0Ch4lIZ2pD6IkiQLVrAipceqUyy6mRLggQonmEUnOAPLULI96Zwm9pkE7UKyXmfKBcIiRoSYvPEtCR9sHiyFG4FY+PEF4VvjsuqnhIdLxB2JCUAel6/EarJBu+SEh6ALFNdvMV28ORqe1MWixBQy42hA8KINLiw7cm54YvBoItqgfQ49aFBn0+KmUIHAE9oCml/MZzGFIA8teRKImPMmXJlI53RZrkhusZwFjM005OKdwMcxfux/A0tZ0EYe0yVM+wryIhs3N9HOAW8BC8S6yLRE3RBdBXQNwBF9DvCJgyfoCn0C1KX7iLtBH9BjzPMxHidaSn6WXqProDwB9INDD+glMC9RjxFxFRHyZRhfor5L79FnSLGa0t0GEX1NK7mhR/Stg4kvpwU8gN9DJN0iwZaeOfQRfUW/O8jwJr0AwqcOKH5yMKU5/eJg6IT+cjCHh7ybSM6btaWnh127DqebBLbfHDykM7hdw/IQdER3iT7G9r+P0TfwW2H69+lHXu6/zvTaHuNzDtolpo0M/zj0zMU6fnXob6K36VP62aFXCBkfYS30BbzoVZB9R/Qh/Q8V5nMQ'
        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()
        time.sleep(2)

        line = 'Tag=READ-st,0.999,5.000,60.588,HISTFAAAA+F42j1Uv6/dNBROTnz9/Pz8/IxfmmfS4KapSfNCuETR7e2lPGiXqkOFEKoqBoaKATEyMSE2mBDq0AExVLDwJyCEmJgQM2LkL2DhP3h89gUSxfb59X3H9jlpPn92mmWr37L9U/w750m4vLy8+/de8c13Od2kjM4ZnRDLqKCCMWKENct4RhAzwgwxaohTJmDGxBjjTHCecYGXC8YEE0ozLjWXVkmurBQq+khjpTFKV5aXUMrWl1IbVUJRVsYbZ6wvrdGybLRxTeM629TK9GNfu7r1g61bVXelbHvnXNWEpvbW14Nvu+D7yU5+68ZpO3bDehknvyy7dRO6sGznobnTh6mf1hf9rl2H9bgN5Th1y2bqNsvFPG/DsN3O07DsuuXezl8MgNkt3bxxfp4298I4D35zcWcZ5k3ft+6tadzs2nmYwtCG4P3U1d3Ydt3omqHrwlSFcWyX0fUuTLWvwjCt2z4gsJ+W0COs8eu57Ta9vwhNMzRLOZk1OFzfYddtMGNbd86FfvZBNjgUX7eTqofKh6oOlW7LWmscgK+crqquKvHaujJlg7PR8WyMhUpZHGZTaVPaSpeuspW02jpnSl1r19cNPHRpjDYkJS7GOqWlkKKqDNdcU62qUimppIyRRljQylKT0CXHLXLcrJGVdfCVioTQSijDpORCGM1LziXuXSj4cq40FpI0k0oJlIYlJoTgoONgFODjkUiksjGWRyMTGtWkhWVcodTgAggUmERVcgZkjKi2UgmMMDCRwQVr1K2ATTKNUkViMMUHE4lYrLDKvRsKmHFBBLKMJAEAHjKqY5nzIjJQDBUZ4xRrPZY6TOgLQqoYZXRMnZBaIYurgmKLFDw2DACTJaOjSIInef3XXGi5Fb4DigDRP7Za6jrEs9iDMQDNB2bwRICo4hGbyWREuyXYLB5MkULRp+CM7JErpoYj+b+BozfoDiNaauaMFSkgou8/JLiKCr6XYpvHOM5XdJjgDvCu0kYhnBEdQU67eSHuBYYTfHsXmAvY6RgDS9h0yKI+/mXwsf0BILUixV2hqxgT8TGShCnq6RSaKwn0Vayvw3KXMDyE6pweI+IA+Kf0Gt1PhK/Qx3SLvs+B8QhpHAL0FPNVdgbzEb2EROlN+K3oNr1HH0F5Tr/m6Treh+Ea7H9F8RZCz6C6SV8QvUtPCaEP2NOcbhC9DpYb9AbelwG/onfoy5wuGeB/yukPQsy3RD/n9Al9ABQQvogcjul3AvwBfZ3TA8h/FsC5hg09z9kjbAQh18F6GzxvA+kk7uFZDsN9+gzSY/olpyegfJ4T/bCCzxP6Kqcfc/qQPgXOP5zIbjo='
        tmp_file.file.write(line + '\n')
        tmp_file.file.flush()
        time.sleep(2)

        output = requests.get("http://{}/metrics".format(self.prom_address)).text
        expected_lines = output.split("\n")[-6:]
        logging.getLogger(__file__).info(output)
        logging.getLogger(__file__).info(expected_lines)
        assert 'collectd_cassandra_stress_hdr_mixed_gauge{cassandra_stress_hdr_mixed="WRITE",cpu_idx="0",instance="127.0.0.1",keyspace="",loader_idx="1",type="lat_perc_50"} 0.62' in expected_lines, "Expected log message was not found"
        assert 'collectd_cassandra_stress_hdr_mixed_gauge{cassandra_stress_hdr_mixed="READ",cpu_idx="0",instance="127.0.0.1",keyspace="",loader_idx="1",type="lat_perc_999"} 36.54' not in expected_lines, "Not expected log message was found"
        time.sleep(1)
        cs_exporter.stop()

        res.result(10)


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
    def test_01(self):
        start_metrics_server()
        cstress = CassandraStressThread(LoaderSetDummy(), timeout=60, node_list=[DbNode()], stress_num=1,
                                        stress_cmd="cassandra-stress write cl=ONE duration=3m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3) compaction(strategy=SizeTieredCompactionStrategy)'"
                                        " -mode cql3 native -rate threads=1000 -pop seq=1..10000000 -log interval=5")

        cstress1 = CassandraStressThread(LoaderSetDummy(), timeout=60, node_list=[DbNode()],
                                         stress_num=1,
                                         stress_cmd="cassandra-stress write cl=ONE duration=3m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3) compaction(strategy=SizeTieredCompactionStrategy)'"
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

        tmp_file = tempfile.NamedTemporaryFile(mode='w+')
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
    def test_01(self):
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
