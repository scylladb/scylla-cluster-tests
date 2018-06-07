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


class CollectdSetup(object):

    def start_collectd_service(self):
        raise NotImplementedError

    def collectd_exporter_setup(self):
        raise NotImplementedError

    def collectd_config(self):
        raise NotImplementedError

    def _setup_collectd(self):
        system_path_remote = self.collectd_config()
        tmp_dir_exporter = tempfile.mkdtemp(prefix='collectd')
        tmp_path_exporter = os.path.join(tmp_dir_exporter, 'scylla.conf')
        tmp_path_remote = "/tmp/scylla-collectd.conf"

        with open(tmp_path_exporter, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(self._collectd_cfg)
        try:
            self.node.remoter.send_files(src=tmp_path_exporter, dst=tmp_path_remote)
            command = "sudo mv %s %s" % (tmp_path_remote, system_path_remote)
            self.node.remoter.run(command)
            self.node.remoter.run('sudo sh -c "echo FQDNLookup   false >> /etc/collectd.conf"')
            self.start_collectd_service()
        finally:
            pass
            shutil.rmtree(tmp_dir_exporter)

    def _set_exporter_path(self):
        self.collectd_exporter_system_base_dir = '/var/tmp'
        self.collectd_exporter_base_dir = 'collectd_exporter-0.3.1.linux-amd64'
        self.collectd_exporter_tarball = '%s.tar.gz' % self.collectd_exporter_base_dir
        self.collectd_exporter_base_url = 'https://github.com/prometheus/collectd_exporter/releases/download/0.3.1'
        self.collectd_exporter_system_dir = os.path.join(self.collectd_exporter_system_base_dir,
                                                         self.collectd_exporter_base_dir)
        self.collectd_exporter_path = os.path.join(self.collectd_exporter_system_dir, 'collectd_exporter')

    def install(self, node):
        self.node = node

        self._setup_collectd()
        self._set_exporter_path()
        self.node.remoter.run('curl --insecure %s/%s -o %s/%s -L' %
                              (self.collectd_exporter_base_url, self.collectd_exporter_tarball,
                               self.collectd_exporter_system_base_dir, self.collectd_exporter_tarball))
        self.node.remoter.run('tar -xzvf %s/%s -C %s' %
                              (self.collectd_exporter_system_base_dir,
                               self.collectd_exporter_tarball,
                               self.collectd_exporter_system_base_dir),
                              verbose=False)
        self.collectd_exporter_setup()


class CassandraCollectdSetup(CollectdSetup):

    def collectd_config(self):
        # The following configuration file is liberaly copy pasted from
        # https://raw.githubusercontent.com/scylladb/cassandra-test-and-deploy/master/roles/collectd-client/templates/collectd-client.conf
        self._collectd_cfg = """
LoadPlugin syslog

LoadPlugin aggregation
LoadPlugin cpu
LoadPlugin disk
LoadPlugin interface
LoadPlugin java
LoadPlugin load
LoadPlugin memory
LoadPlugin network

<Plugin "aggregation">
 <Aggregation>
   Plugin "cpu"
   Type "cpu"

   GroupBy "Host"
   GroupBy "TypeInstance"

   CalculateNum false
   CalculateSum false
   CalculateAverage true
   CalculateMinimum false
   CalculateMaximum false
   CalculateStddev false
 </Aggregation>
</Plugin>

<Plugin disk>
    Disk "/^[hs]d[a-f][0-9]?$/"
    IgnoreSelected false
</Plugin>

<Plugin interface>
    Interface "eth0"
    IgnoreSelected false
</Plugin>

<Plugin "java">
  JVMArg "-verbose:jni"
  JVMArg "-Djava.class.path=/usr/share/collectd/java/collectd-api.jar:/usr/share/collectd/java/generic-jmx.jar"
  LoadPlugin "org.collectd.java.GenericJMX"

  <Plugin "GenericJMX">
    <MBean "cassandra/classes">
      ObjectName "java.lang:type=ClassLoading"
      InstancePrefix "cassandra_java"

      <Value>
        Type "gauge"
        InstancePrefix "loaded_classes"
        Table false
        Attribute "LoadedClassCount"
      </Value>
    </MBean>

    <MBean "cassandra/compilation">
      ObjectName "java.lang:type=Compilation"
      InstancePrefix "cassandra_java"

      <Value>
        Type "total_time_in_ms"
        InstancePrefix "compilation_time"
        Table false
        Attribute "TotalCompilationTime"
      </Value>
    </MBean>

    <MBean "cassandra/storage_proxy">
      ObjectName "org.apache.cassandra.db:type=StorageProxy"
      InstancePrefix "cassandra_activity_storage_proxy"

      <Value>
        Type "counter"
        InstancePrefix "read"
        Table false
        Attribute "ReadOperations"
      </Value>
      <Value>
        Type "counter"
        InstancePrefix "write"
        Table false
        Attribute "WriteOperations"
      </Value>

    </MBean>

    <MBean "cassandra/memory">
      ObjectName "java.lang:type=Memory,*"
      InstancePrefix "cassandra_java_memory"
      <Value>
        Type "memory"
        InstancePrefix "heap-"
        Table true
        Attribute "HeapMemoryUsage"
      </Value>

      <Value>
        Type "memory"
        InstancePrefix "nonheap-"
        Table true
        Attribute "NonHeapMemoryUsage"
      </Value>
    </MBean>

    <MBean "cassandra/memory_pool">
      ObjectName "java.lang:type=MemoryPool,*"
      InstancePrefix "cassandra_java_memory_pool-"
      InstanceFrom "name"
      <Value>
        Type "memory"
        Table true
        Attribute "Usage"
      </Value>
    </MBean>

    <MBean "cassandra/garbage_collector">
      ObjectName "java.lang:type=GarbageCollector,*"
      InstancePrefix "cassandra_java_gc-"
      InstanceFrom "name"

      <Value>
        Type "invocations"
        Table false
        Attribute "CollectionCount"
      </Value>

      <Value>
        Type "total_time_in_ms"
        InstancePrefix "collection_time"
        Table false
        Attribute "CollectionTime"
      </Value>
    </MBean>

    <MBean "cassandra/concurrent">
      ObjectName "org.apache.cassandra.internal:type=*"
      InstancePrefix "cassandra_activity_internal"
      <Value>
        Attribute "CompletedTasks"
        InstanceFrom "type"
        Type "counter"
        InstancePrefix "tasks-"
      </Value>
    </MBean>

    <MBean "cassandra/request">
      ObjectName "org.apache.cassandra.request:type=*"
      InstancePrefix "cassandra_activity_request-"
      <Value>
        Attribute "CompletedTasks"
        InstanceFrom "type"
        Type "counter"
        InstancePrefix "tasks-"
      </Value>
    </MBean>

    <MBean "cassandra/cfstats">
      ObjectName "org.apache.cassandra.db:type=ColumnFamilies,*"
      InstanceFrom "keyspace"
      InstancePrefix "cassandra_columnfamilies_stats-"
      <Value>
        Attribute "ReadCount"
        InstancePrefix "livereadcount-"
        Type "counter"
        InstanceFrom "columnfamily"
      </Value>
      <Value>
        Attribute "TotalReadLatencyMicros"
        InstancePrefix "livereadlatency-"
        Type "counter"
        InstanceFrom "columnfamily"
      </Value>
      <Value>
        Attribute "WriteCount"
        InstancePrefix "livewritecount-"
        Type "counter"
        InstanceFrom "columnfamily"
      </Value>
      <Value>
        Attribute "TotalWriteLatencyMicros"
        InstancePrefix "livewritelatency-"
        Type "counter"
        InstanceFrom "columnfamily"
      </Value>
      <Value>
        Attribute "LiveSSTableCount"
        InstancePrefix "live_sstable_count-"
        Type "gauge"
        InstanceFrom "columnfamily"
      </Value>
      <Value>
        Attribute "TotalDiskSpaceUsed"
        InstancePrefix "total_disk_space_used-"
        Type "gauge"
        InstanceFrom "columnfamily"
      </Value>

    </MBean>

    <MBean "cassandra/compaction">
      ObjectName "org.apache.cassandra.db:type=CompactionManager"
      InstancePrefix "cassandra_compaction"
      <Value>
        Attribute "PendingTasks"
        InstancePrefix "pending"
        Type "gauge"
      </Value>
    </MBean>

    <MBean "cassandra/storage_service">
      ObjectName "org.apache.cassandra.db:type=StorageService"
      InstancePrefix "cassandra_storage_service"
      <Value>
        Attribute "StreamThroughputMbPerSec"
        InstancePrefix "stream"
        Type "gauge"
      </Value>
    </MBean>

    <Connection>
      Host "{{collectd_node_name}}"
      ServiceURL "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi"
      Collect "cassandra/storage_proxy"
      Collect "cassandra/classes"
      Collect "cassandra/compilation"
      Collect "cassandra/memory"
      Collect "cassandra/memory_pool"
      Collect "cassandra/garbage_collector"
      Collect "cassandra/concurrent"
      Collect "cassandra/cfstats"
      Collect "cassandra/request"
      Collect "cassandra/compaction"
      Collect "cassandra/storage_service"
    </Connection>
  </Plugin>
</Plugin>

<Plugin "network">
        Listen "127.0.0.1" "25826"
        Server "127.0.0.1" "65534"
        Forward true
</Plugin>

<Plugin unixsock>
    SocketFile "/var/run/collectd-unixsock"
    SocketPerms "0666"
</Plugin>
"""
        return "/etc/collectd/collectd.conf"

    def start_collectd_service(self):
        # no need to enable anything since it's upstart, just start it.
        self.node.remoter.run('sudo service collectd restart')

    def collectd_exporter_setup(self):
        service_file = """# Run node_exporter

start on startup

script
   %s -collectd.listen-address=:65534
end script
""" % self.collectd_exporter_path

        tmp_dir_exporter = tempfile.mkdtemp(prefix='collectd-upstart-service')
        tmp_path_exporter = os.path.join(tmp_dir_exporter, 'collectd_exporter.conf')
        tmp_path_remote = '/tmp/collectd_exporter.conf'
        system_path_remote = '/etc/init/collectd_exporter.conf'
        with open(tmp_path_exporter, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(service_file)
        try:
            self.node.remoter.send_files(src=tmp_path_exporter, dst=tmp_path_remote)
            self.node.remoter.run('sudo mv %s %s' %
                                  (tmp_path_remote, system_path_remote))
            self.node.remoter.run('sudo service collectd_exporter restart')
        finally:
            shutil.rmtree(tmp_dir_exporter)


class ScyllaCollectdSetup(CollectdSetup):

    def collectd_config(self):
        self._collectd_cfg = """
LoadPlugin network
LoadPlugin disk
LoadPlugin interface
LoadPlugin unixsock
LoadPlugin df
LoadPlugin processes
<Plugin network>
        Listen "127.0.0.1" "25826"
        Server "127.0.0.1" "65534"
        Forward true
</Plugin>
<Plugin disk>
</Plugin>
<Plugin interface>
</Plugin>
<Plugin "df">
  FSType "xfs"
  IgnoreSelected false
</Plugin>
<Plugin unixsock>
    SocketFile "/var/run/collectd-unixsock"
    SocketPerms "0666"
</Plugin>
<Plugin processes>
    Process "scylla"
</Plugin>
"""
        return "/etc/collectd.d/scylla.conf"

    def start_collectd_service(self):
        # Disable SELinux to allow the unix socket plugin to work
        self.node.remoter.run('sudo setenforce 0', ignore_status=True)
        self.node.remoter.run('sudo systemctl enable collectd.service')
        if self.node.is_docker():
            self.node.remoter.run('sudo /usr/sbin/collectd')
        else:
            self.node.remoter.run('sudo systemctl restart collectd.service')

    def collectd_exporter_setup(self):
        systemd_unit = """[Unit]
Description=Collectd Exporter

[Service]
Type=simple
User=root
Group=root
ExecStart=%s -collectd.listen-address=:65534

[Install]
WantedBy=multi-user.target
""" % self.collectd_exporter_path

        tmp_dir_exporter = tempfile.mkdtemp(prefix='collectd-systemd-service')
        tmp_path_exporter = os.path.join(tmp_dir_exporter, 'collectd-exporter.service')
        tmp_path_remote = '/tmp/collectd-exporter.service'
        system_path_remote = '/etc/systemd/system/collectd-exporter.service'
        with open(tmp_path_exporter, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(systemd_unit)
        try:
            self.node.remoter.send_files(src=tmp_path_exporter, dst=tmp_path_remote)
            self.node.remoter.run('sudo mv %s %s' %
                                  (tmp_path_remote, system_path_remote))

            if self.node.is_docker():
                self.node.remoter.run('sudo {} -collectd.listen-address=:65534 &'.format(self.collectd_exporter_path))
            else:
                self.node.remoter.run('sudo systemctl start collectd-exporter.service')
        finally:
            shutil.rmtree(tmp_dir_exporter)

    def install(self, node):
        self.node = node

        if self.node.is_rhel_like():
            self.node.remoter.run('sudo yum install -y epel-release', retry=3)
            self.node.remoter.run('sudo yum upgrade ca-certificates -y '
                                  '--disablerepo=epel',
                                  ignore_status=True,
                                  verbose=True, retry=3)
            self.node.remoter.run('sudo yum install -y collectd')
        else:
            self.node.remoter.run('sudo apt-get install -y collectd')
        self._setup_collectd()
        self._set_exporter_path()
        self.node.remoter.run('curl --insecure %s/%s -o %s/%s -L' %
                              (self.collectd_exporter_base_url, self.collectd_exporter_tarball,
                               self.collectd_exporter_system_base_dir, self.collectd_exporter_tarball))
        self.node.remoter.run('tar -xzvf %s/%s -C %s' %
                              (self.collectd_exporter_system_base_dir,
                               self.collectd_exporter_tarball,
                               self.collectd_exporter_system_base_dir),
                              verbose=False)
        self.collectd_exporter_setup()
