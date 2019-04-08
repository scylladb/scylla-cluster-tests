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

import os
import tempfile
import shutil

from .collectd import ScyllaCollectdSetup


class CassandraStressExporterSetup(object):

    def exporter_install(self):
        cassandra_stress_exporter = """#!/usr/bin/env python
#
# Run cassandra-stress like this:
#
#    cassandra-stress (...) -log file=cs.log
#
# Run this exporter like this:
#
#    tail -f cs.log | sudo ./cassandra_stress_exporter &
#
# You don't need to restart exporter across c-s restarts.
#
# Exports the following metrics:
#
#    cassandra_stress-0/gauge-ops
#    cassandra_stress-0/gauge-lat_mean
#    cassandra_stress-0/gauge-lat_perc_50
#    cassandra_stress-0/gauge-lat_perc_95
#    cassandra_stress-0/gauge-lat_perc_99
#    cassandra_stress-0/gauge-lat_perc_99
#    cassandra_stress-0/gauge-lat_perc_max
#    cassandra_stress-0/gauge-errors
#

import sys
import socket
import os
import time

cs_opt = ''
if len(sys.argv) > 1:
    cs_opt = '_' + sys.argv[1]

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

hostname = get_local_ip()

server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.connect('/var/run/collectd-unixsock')

def readlines(sock, recv_buffer=4096, delim=os.linesep):
    buffer = ''
    data = True
    while data:
        data = sock.recv(recv_buffer)
        buffer += data
        while buffer.find(delim) != -1:
            line, buffer = buffer.split(os.linesep, 1)
            yield line
    return

class ValuePack:
    def __init__(self):
        self.messages = []
        self.time = time.time()
    def add(self, plugin, plugin_instance, type, type_instance, val):
        self.messages.append('PUTVAL "%s/%s-%s/%s-%s" %f:%f%s' % (hostname, plugin, plugin_instance, type, type_instance, self.time, val, os.linesep))
    def send(self):
        for msg in self.messages:
            server.send(msg)
        lines = readlines(server)
        for msg in self.messages:
            reply = next(lines)
            if not reply.startswith('0 Success'):
                print('collectd error: ' + reply)

start_time = time.time()
plugin = 'cassandra_stress' + cs_opt
plugin_instance = '0'

with os.fdopen(sys.stdin.fileno(), 'r', 1) as input:
    while True:
        line = sys.stdin.readline()

        if not line.startswith('total,'):
            continue

        if time.time() - start_time < 1.0: # Skip existing lines
            continue

        cols = [element.strip() for element in line.split(',')]

        ops = float(cols[2])
        lat_mean = float(cols[5])
        lat_med = float(cols[6])
        lat_perc_95 = float(cols[7])
        lat_perc_99 = float(cols[8])
        lat_perc_999 = float(cols[9])
        lat_max = float(cols[10])
        errors = int(cols[13])

        pack = ValuePack()
        pack.add(plugin, plugin_instance, 'gauge', 'ops', ops)
        pack.add(plugin, plugin_instance, 'gauge', 'lat_mean', lat_mean)
        pack.add(plugin, plugin_instance, 'gauge', 'lat_perc_50', lat_med)
        pack.add(plugin, plugin_instance, 'gauge', 'lat_perc_95', lat_perc_95)
        pack.add(plugin, plugin_instance, 'gauge', 'lat_perc_99', lat_perc_99)
        pack.add(plugin, plugin_instance, 'gauge', 'lat_perc_99.9', lat_perc_999)
        pack.add(plugin, plugin_instance, 'gauge', 'lat_perc_max', lat_max)
        pack.add(plugin, plugin_instance, 'gauge', 'errors', errors)
        pack.send()

    server.close()
"""

        tmp_dir_exporter = tempfile.mkdtemp(prefix='cassandra-stress-tools')
        tmp_path_exporter = os.path.join(tmp_dir_exporter, 'cassandra_stress_exporter')
        tmp_path_remote = '/tmp/cassandra_stress_exporter'
        system_path_remote = '/usr/bin/cassandra_stress_exporter'
        with open(tmp_path_exporter, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(cassandra_stress_exporter)
        try:
            self.node.remoter.send_files(src=tmp_path_exporter, dst=tmp_path_remote)
            self.node.remoter.run('sudo mv %s %s' %
                                  (tmp_path_remote, system_path_remote))
            self.node.remoter.run('sudo chmod +x %s' % system_path_remote)
        finally:
            shutil.rmtree(tmp_dir_exporter)

    def install(self, node):
        self.node = node
        self.exporter_install()

        collectd_setup = ScyllaCollectdSetup()
        collectd_setup.install(node)
