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

"""
Classes that introduce disruption in clusters.
"""

import inspect
import logging
import random
import time
import datetime
import string
import threading

from avocado.utils import process

from sdcm.cluster import SCYLLA_YAML_PATH
from .utils import get_data_dir_path
from .log import SDCMAdapter
from . import prometheus
from . import mgmt
from avocado.utils import wait

from sdcm.utils import remote_get_file


class Nemesis(object):

    def __init__(self, cluster, loaders, monitoring_set, termination_event, **kwargs):
        self.cluster = cluster
        self.loaders = loaders
        self.monitoring_set = monitoring_set
        self.target_node = None
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.set_target_node()
        result = self.target_node.remoter.run('rpm -qa | grep scylla | sort', verbose=False,
                                              ignore_status=True)
        self.db_software_version = ['not available (using cassandra)']
        if result.stdout:
            self.db_software_version = result.stdout.splitlines()
        self.termination_event = termination_event
        self.operation_log = []
        self.current_disruption = None
        self.duration_list = []
        self.error_list = []
        self.interval = 0
        self.start_time = time.time()
        self.stats = {}
        self.db_stats = kwargs.get('db_stats', None)
        self.metrics_srv = prometheus.nemesis_metrics_obj()
        self._random_sequence = None

    def update_stats(self, disrupt, status=True, data={}):
        key = {True: 'runs', False: 'failures'}
        if disrupt not in self.stats:
            self.stats[disrupt] = {'runs': [], 'failures': [], 'cnt': 0}
        self.stats[disrupt][key[status]].append(data)
        self.stats[disrupt]['cnt'] += 1
        self.log.info('Update nemesis info: %s', self.stats)
        if self.db_stats:
            self.db_stats.update({'nemesis': self.stats})

    def set_target_node(self):
        non_seed_nodes = [node for node in self.cluster.nodes if not node.is_seed]
        self.target_node = random.choice(non_seed_nodes)
        self.log.info('Current Target: %s', self.target_node)

    def set_termination_event(self, termination_event):
        self.termination_event = termination_event

    def run(self, interval=30):
        interval *= 60
        self.log.info('Interval: %s s', interval)
        self.interval = interval
        while True:
            self._set_current_disruption()
            self.disrupt()
            if self.termination_event is not None:
                if self.termination_event.isSet():
                    self.termination_event = None
                    self.log.info('Asked to stop running nemesis')
                    break
            time.sleep(interval)
            self.set_target_node()

    def report(self):
        if len(self.duration_list) > 0:
            avg_duration = sum(self.duration_list) / len(self.duration_list)
        else:
            avg_duration = 0

        self.log.info('Report')
        self.log.info('DB Version:')
        for line in self.db_software_version:
            self.log.info(line)
        self.log.info('Interval: %s s', self.interval)
        self.log.info('Average duration: %s s', avg_duration)
        self.log.info('Total execution time: %s s', int(time.time() - self.start_time))
        self.log.info('Times executed: %s', len(self.duration_list))
        self.log.info('Unexpected errors: %s', len(self.error_list))
        self.log.info('Operation log:')
        for operation in self.operation_log:
            self.log.info(operation)

    def __str__(self):
        try:
            return str(self.__class__).split("'")[1]
        except:
            return str(self.__class__)

    def _run_nodetool(self, cmd, node):
        try:
            result = node.remoter.run(cmd)
            self.log.debug("Command '%s' duration -> %s s", result.command,
                           result.duration)
            return result
        except process.CmdError, details:
            err = ("nodetool command '%s' failed on node %s: %s" %
                   (cmd, self.target_node, details.result))
            self.error_list.append(err)
            self.log.error(err)
            return None
        except Exception:
            err = 'Unexpected exception running nodetool'
            self.error_list.append(err)
            self.log.error(err, exc_info=True)
            return None

    def _kill_scylla_daemon(self):
        self.log.info('Kill all scylla processes in %s', self.target_node)
        kill_cmd = "sudo pkill -9 scylla"
        self.target_node.remoter.run(kill_cmd, ignore_status=True)

        # Let's wait for the target Node to have their services re-started
        self.log.info('Waiting scylla services to be restarted after we killed them...')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to be restarted after we killed them...')
        self.target_node.wait_jmx_up()

    def disrupt_stop_wait_start_scylla_server(self, sleep_time=300):
        self._set_current_disruption('StopWaitStartService %s' % self.target_node)
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
        self.log.info("Sleep for %s seconds", sleep_time)
        time.sleep(sleep_time)
        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt_stop_start_scylla_server(self):
        self._set_current_disruption('StopStartService %s' % self.target_node)
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt_restart_then_repair_node(self):
        self._set_current_disruption('RestartThenRepairNode %s' % self.target_node)
        self.target_node.restart()
        self.log.info('Waiting scylla services to start after node restart')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node restart')
        self.target_node.wait_jmx_up()
        self.repair_nodetool_repair()

    def disrupt_hard_reboot_node(self):
        self._set_current_disruption('HardRebootNode %s' % self.target_node)
        self.target_node.reboot(hard=True)
        self.log.info('Waiting scylla services to start after node reboot')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node reboot')
        self.target_node.wait_jmx_up()

    def disrupt_soft_reboot_node(self):
        self._set_current_disruption('SoftRebootNode %s' % self.target_node)
        self.target_node.reboot(hard=False)
        self.log.info('Waiting scylla services to start after node reboot')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node reboot')
        self.target_node.wait_jmx_up()

    def _destroy_data(self):
        # Send the script used to corrupt the DB
        break_scylla = get_data_dir_path('break_scylla.sh')
        self.target_node.remoter.send_files(break_scylla,
                                            "/tmp/break_scylla.sh")

        # Stop scylla service before deleting sstables to avoid partial deletion of files that are under compaction
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)

        # corrupt the DB
        self.target_node.remoter.run('chmod +x /tmp/break_scylla.sh')
        self.target_node.remoter.run('sudo /tmp/break_scylla.sh')  # corrupt the DB

        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt(self):
        raise NotImplementedError('Derived classes must implement disrupt()')

    def _set_current_disruption(self, label=None):
        if not label:
            label = "%s on target node %s" % (self.__class__.__name__, self.target_node)
        self.log.debug('Set current_disruption -> %s', label)
        self.current_disruption = label
        self.log.info(label)

    def disrupt_destroy_data_then_repair(self):
        self._set_current_disruption('CorruptThenRepair %s' % self.target_node)
        self._destroy_data()
        # try to save the node
        self.repair_nodetool_repair()

    def disrupt_destroy_data_then_rebuild(self):
        self._set_current_disruption('CorruptThenRebuild %s' % self.target_node)
        self._destroy_data()
        # try to save the node
        self.repair_nodetool_rebuild()

    def disrupt_nodetool_drain(self):
        self._set_current_disruption('Drainer %s' % self.target_node)
        drain_cmd = 'nodetool -h localhost drain'
        result = self._run_nodetool(drain_cmd, self.target_node)
        for node in self.cluster.nodes:
            if node == self.target_node:
                self.log.info('Status for target %s: %s', node,
                              self._run_nodetool('nodetool status', node))
            else:
                self.log.info('Status for regular %s: %s', node,
                              self._run_nodetool('nodetool status', node))
        if result is not None:
            self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
            self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def _add_and_init_new_cluster_node(self, old_node_private_ip=None, timeout=10800):
        """When old_node_private_ip is not None replacement node procedure is initiated"""
        self.log.info("Adding new node to cluster...")
        new_node = self.cluster.add_nodes(count=1, dc_idx=self.target_node.dc_idx, enable_auto_bootstrap=True)[0]
        new_node.replacement_node_ip = old_node_private_ip
        self.cluster.wait_for_init(node_list=[new_node], timeout=timeout)
        self.cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
        self.monitoring_set.reconfigure_scylla_monitoring()
        return new_node

    def _terminate_cluster_node(self, node):
        self.cluster.terminate_node(node)
        self.monitoring_set.reconfigure_scylla_monitoring()

    def disrupt_nodetool_decommission(self, add_node=True):
        def get_node_info_list(verification_node):
            try:
                return self.cluster.get_node_info_list(verification_node)
            except Exception as details:
                self.log.error(str(details))
                return None
        self._set_current_disruption('Decommission %s' % self.target_node)
        target_node_ip = self.target_node.private_ip_address
        decommission_cmd = 'nodetool --host localhost decommission'
        result = self._run_nodetool(decommission_cmd, self.target_node)
        if result is not None:
            verification_node = random.choice(self.cluster.nodes)
            node_info_list = get_node_info_list(verification_node)
            while verification_node == self.target_node or node_info_list is None:
                verification_node = random.choice(self.cluster.nodes)
                node_info_list = get_node_info_list(verification_node)

            private_ips = [node_info['ip'] for node_info in node_info_list]
            error_msg = ('Node that was decommissioned %s still in the cluster. '
                         'Cluster status info: %s' % (self.target_node,
                                                      node_info_list))
            if target_node_ip in private_ips:
                self.log.info('Decommission %s FAIL', self.target_node)
                self.log.error(error_msg)
            else:
                self.log.info('Decommission %s PASS', self.target_node)
                self._terminate_cluster_node(self.target_node)
                # Replace the node that was terminated.
                if add_node:
                    # When adding node after decommission the node is declared as up only after it completed bootstrapping,
                    # increasing the timeout for now
                    self._add_and_init_new_cluster_node()
                # after decomission and add_node, the left nodes have data that isn't part of their tokens anymore.
                # In order to eliminate cases that we miss a "data loss" bug because of it, we cleanup this data.
                for node in self.cluster.nodes:
                        # TODO: wherever we have keyspace1 hardcoded, to replace it with a list of keyspaces.
                        node.remoter.run('nodetool --host localhost cleanup keyspace1', verbose=True)

    def disrupt_terminate_and_replace_node(self):
        # using "Replace a Dead Node" procedure from http://docs.scylladb.com/procedures/replace_dead_node/
        self._set_current_disruption('TerminateAndReplaceNode %s' % self.target_node)
        old_node_private_ip = self.target_node.private_ip_address
        self._terminate_cluster_node(self.target_node)
        new_node = self._add_and_init_new_cluster_node(old_node_private_ip)
        self.repair_nodetool_repair(new_node)

    def disrupt_no_corrupt_repair(self):
        self._set_current_disruption('NoCorruptRepair %s' % self.target_node)
        self.repair_nodetool_repair()

    def disrupt_major_compaction(self):
        self._set_current_disruption('MajorCompaction %s' % self.target_node)
        cmd = 'nodetool -h localhost compact'
        self._run_nodetool(cmd, self.target_node)

    def disrupt_nodetool_refresh(self, big_sstable=True, skip_download=False):
        node = self.target_node
        self._set_current_disruption('Refresh keyspace1.standard1 on {}'.format(node.name))
        if big_sstable:
            # 100G, the big file will be saved to GCE image
            sstable_url = 'https://s3.amazonaws.com/scylla-qa-team/keyspace1.standard1.tar.gz'
            sstable_file = "/tmp/keyspace1.standard1.tar.gz"
            sstable_md5 = 'f64ab85111e817f22f93653a4a791b1f'
        else:
            # 100M (300000 rows)
            sstable_url = 'https://s3.amazonaws.com/scylla-qa-team/keyspace1.standard1.100M.tar.gz'
            sstable_file = '/tmp/keyspace1.standard1.100M.tar.gz'
            sstable_md5 = 'f641f561067dd612ff95f2b89bd12530'
        if not skip_download:
            remote_get_file(node.remoter, sstable_url, sstable_file,
                            hash_expected=sstable_md5, retries=2)

        self.log.debug('Make sure keyspace1.standard1 exists')
        result = self._run_nodetool('nodetool --host localhost cfstats keyspace1.standard1',
                                    node)
        if result is not None and result.exit_status == 0:
            result = node.remoter.run("sudo ls -t /var/lib/scylla/data/keyspace1/")
            upload_dir = result.stdout.split()[0]
            node.remoter.run('sudo tar xvfz {} -C /var/lib/scylla/data/keyspace1/{}/upload/'.format(sstable_file, upload_dir))

            refresh_cmd = 'nodetool --host localhost refresh -- keyspace1 standard1'
            self._run_nodetool(refresh_cmd, node)
            cmd = "select * from keyspace1.standard1 where key=0x314e344b4d504d4b4b30"
            node.remoter.run('cqlsh -e "{}" {}'.format(cmd, node.private_ip_address), verbose=True)

    def disrupt_nodetool_enospc(self, sleep_time=30, all_nodes=False):
        if all_nodes:
            nodes = self.cluster.nodes
        else:
            nodes = [self.target_node]
        self._set_current_disruption('Enospc test on {}'.format([n.name for n in nodes]))

        def search_database_enospc(node):
            """
            Search database log by executing cmd inside node, use shell tool to
            avoid return and process huge data.
            """
            cmd = "journalctl --no-tail --no-pager -u scylla-server.service|grep 'No space left on device'|wc -l"
            result = node.remoter.run(cmd, verbose=True)
            return int(result.stdout)

        def approach_enospc():
            # get the size of free space (default unit: KB)
            result = node.remoter.run("df -l|grep '/var/lib/scylla'")
            free_space_size = result.stdout.split()[3]

            occupy_space_size = int(int(free_space_size) * 90 / 100)
            occupy_space_cmd = 'sudo fallocate -l {}K /var/lib/scylla/occupy_90percent.{}'.format(occupy_space_size, datetime.datetime.now().strftime('%s'))
            self.log.debug('Cost 90% free space on /var/lib/scylla/ by {}'.format(occupy_space_cmd))
            try:
                node.remoter.run(occupy_space_cmd, verbose=True)
            except Exception as details:
                self.log.error(str(details))
            return search_database_enospc(node) > orig_errors

        for node in nodes:
            result = node.remoter.run('cat /proc/mounts')
            if '/var/lib/scylla' not in result.stdout:
                self.log.error("Scylla doesn't use an individual storage, skip enospc test")
                continue

            # check original ENOSPC error
            orig_errors = search_database_enospc(node)
            wait.wait_for(func=approach_enospc,
                          timeout=300,
                          step=5,
                          text='Wait for new ENOSPC error occurs in database')

            self.log.debug('Sleep {} seconds before releasing space to scylla'.format(sleep_time))
            time.sleep(sleep_time)

            self.log.debug('Delete occupy_90percent file to release space to scylla-server')
            node.remoter.run('sudo rm -rf /var/lib/scylla/occupy_90percent.*')

            self.log.debug('Sleep a while before restart scylla-server')
            time.sleep(sleep_time / 2)
            node.remoter.run('sudo systemctl restart scylla-server.service')
            node.wait_db_up()

    def _deprecated_disrupt_stop_start(self):
        # TODO: We don't support fully stopping the AMI instance anymore
        # TODO: This nemesis has to be rewritten to just stop/start scylla server
        self.log.info('StopStart %s', self.target_node)
        self.target_node.restart()

    def call_random_disrupt_method(self, disrupt_methods=None, predefined_sequence=False):
        if not disrupt_methods:
            disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                               attr[0].startswith('disrupt_') and
                               callable(attr[1])]
        else:
            disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                               attr[0] in disrupt_methods and
                               callable(attr[1])]
        if not predefined_sequence:
            disrupt_method = random.choice(disrupt_methods)
        else:
            if not self._random_sequence:
                # Generate random sequence, every method has same chance to be called.
                # Here we use multiple original methods list, it will increase the chance
                # to call same method continuously.
                #
                # Adjust the rate according to the test duration. Try to call more unique
                # methods and don't wait to long time to meet the balance if the test
                # duration is short.
                test_duration = self.cluster.params.get('test_duration', default=180)
                if test_duration < 600:  # less than 10 hours
                    rate = 1
                elif test_duration < 4320:  # less than 3 days
                    rate = 2
                else:
                    rate = 3
                multiple_disrupt_methods = disrupt_methods * rate
                random.shuffle(multiple_disrupt_methods)
                self._random_sequence = multiple_disrupt_methods
            # consume the random sequence
            disrupt_method = self._random_sequence.pop()

        disrupt_method_name = disrupt_method.__name__.replace('disrupt_', '')
        self.log.info(">>>>>>>>>>>>>Started random_disrupt_method %s" % disrupt_method_name)
        self.metrics_srv.event_start(disrupt_method_name)
        exc = None
        try:
            disrupt_method()
        except Exception as exc:
            self.log.error("Exception in random_disrupt_method %s: %s", disrupt_method_name, exc)
        else:
            self.log.info("<<<<<<<<<<<<<Finished random_disrupt_method %s" % disrupt_method_name)
        finally:
            self.metrics_srv.event_stop(disrupt_method_name)
            if exc:
                # propagate exception to the wrapper - to have the same handling as via-class call
                raise

    def repair_nodetool_repair(self, node=None):
        node = node if node else self.target_node
        repair_cmd = 'nodetool -h localhost repair'
        self._run_nodetool(repair_cmd, node)

    def repair_nodetool_rebuild(self):
        rebuild_cmd = 'nodetool -h localhost rebuild'
        self._run_nodetool(rebuild_cmd, self.target_node)

    def disrupt_nodetool_cleanup(self):
        self._set_current_disruption('NodetoolCleanupMonkey %s' % self.target_node)
        cmd = 'nodetool -h localhost cleanup keyspace1'
        self._run_nodetool(cmd, self.target_node)

    def _run_in_cqlsh(self, cmd, node=None):
        if not node:
            node = self.target_node
        node.remoter.run('cqlsh -e "{}" {}'.format(cmd, node.private_ip_address), verbose=True)

    def _modify_table_property(self, name, val, keyspace="keyspace1", table="standard1"):
        disruption_name = "".join([p.strip().capitalize() for p in name.split("_")])
        self._set_current_disruption('ModifyTableProperties%s %s' % (disruption_name, self.target_node))
        cmd = "ALTER TABLE {keyspace}.{table} WITH {name} = {val};".format(**locals())
        self._run_in_cqlsh(cmd)

    def modify_table_comment(self):
        # default: comment = ''
        prop_val = ''.join(random.choice(string.ascii_letters) for _ in xrange(24))
        self._modify_table_property(name="comment", val="'%s'" % prop_val)

    def modify_table_gc_grace_time(self):
        """
            The number of seconds after data is marked with a tombstone (deletion marker)
            before it is eligible for garbage-collection.
            default: gc_grace_seconds = 864000
        """
        self._modify_table_property(name="gc_grace_seconds", val=random.randint(216000, 864000))

    def modify_table_caching(self):
        """
           Caching optimizes the use of cache memory by a table without manual tuning.
           Cassandra weighs the cached data by size and access frequency.
           default: caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
        """
        prop_val = dict(
            keys=random.choice(["NONE", "ALL"]),
            rows_per_partition=random.choice(["NONE", "ALL", random.randint(1, 10000)])
        )
        self._modify_table_property(name="caching", val=str(prop_val))

    def modify_table_bloom_filter_fp_chance(self):
        """
            The Bloom filter sets the false-positive probability for SSTable Bloom filters.
            When a client requests data, Cassandra uses the Bloom filter to check if the row
            exists before doing disk I/O. Bloom filter property value ranges from 0 to 1.0.
            Lower Bloom filter property probabilities result in larger Bloom filters that use more memory.
            The effects of the minimum and maximum values:
                0: Enables the unmodified, effectively the largest possible, Bloom filter.
                1.0: Disables the Bloom filter.
            default: bloom_filter_fp_chance = 0.01
        """
        self._modify_table_property(name="bloom_filter_fp_chance", val=random.random()/2)

    def modify_table_compaction(self):
        """
            The compaction property defines the compaction strategy class for this table.
            default: compaction = {'class': 'SizeTieredCompactionStrategy'}
        """
        # TODO: Sub-properties for each of compaction strategies should also be tested
        strategies = ("SizeTieredCompactionStrategy", "DateTieredCompactionStrategy",
                      "TimeWindowCompactionStrategy", "LeveledCompactionStrategy")
        prop_val = {"class": random.choice(strategies)}
        self._modify_table_property(name="compaction", val=str(prop_val))

    def modify_table_compression(self):
        """
            The compression algorithm. Valid values are LZ4Compressor), SnappyCompressor, and DeflateCompressor
            default: compression = {}
        """
        algos = ("",  # no compression
                 "LZ4Compressor",
                 "SnappyCompressor",
                 "DeflateCompressor")
        algo = random.choice(algos)
        prop_val = {"sstable_compression": algo}
        if algo:
            prop_val["chunk_length_kb"] = random.choice(["4K", "64KB", "1M"])
            prop_val["crc_check_chance"] = random.random()
        self._modify_table_property(name="compression", val=str(prop_val))

    def modify_table_crc_check_chance(self):
        """
            default: crc_check_chance = 1.0
        """
        self._modify_table_property(name="crc_check_chance", val=random.random())

    def modify_table_dclocal_read_repair_chance(self):
        """
            The probability that a successful read operation triggers a read repair.
            Unlike the repair controlled by read_repair_chance, this repair is limited to
            replicas in the same DC as the coordinator. The value must be between 0 and 1
            default: dclocal_read_repair_chance = 0.1
        """
        self._modify_table_property(name="dclocal_read_repair_chance", val=random.random())

    def modify_table_default_time_to_live(self):
        """
            The value of this property is a number of seconds. If it is set, Cassandra applies a
            default TTL marker to each column in the table, set to this value. When the table TTL
            is exceeded, Cassandra tombstones the table.
            default: default_time_to_live = 0
        """
        self._modify_table_property(name="default_time_to_live", val=random.randint(60, 600))  # max allowed TTL - 20 years (630720000)

    def modify_table_max_index_interval(self):
        """
            If the total memory usage of all index summaries reaches this value, Cassandra decreases
            the index summaries for the coldest SSTables to the maximum set by max_index_interval.
            The max_index_interval is the sparsest possible sampling in relation to memory pressure.
            default: max_index_interval = 2048
        """
        self._modify_table_property(name="max_index_interval", val=random.choice([1024, 4096, 8192]))

    def modify_table_min_index_interval(self):
        """
            The minimum gap between index entries in the index summary. A lower min_index_interval
            means the index summary contains more entries from the index, which allows Cassandra
            to search fewer index entries to execute a read. A larger index summary may also use
            more memory. The value for min_index_interval is the densest possible sampling of the index.
            default: min_index_interval = 128
        """
        self._modify_table_property(name="min_index_interval", val=random.choice([128, 256, 512]))

    def modify_table_memtable_flush_period_in_ms(self):
        """
            The number of milliseconds before Cassandra flushes memtables associated with this table.
            default: memtable_flush_period_in_ms = 0
        """
        self._modify_table_property(name="memtable_flush_period_in_ms", val=random.randint(1, 5000))

    def modify_table_read_repair_chance(self):
        """
            The probability that a successful read operation will trigger a read repair.of read repairs
            being invoked. Unlike the repair controlled by dc_local_read_repair_chance, this repair is
            not limited to replicas in the same DC as the coordinator. The value must be between 0 and 1
            default: read_repair_chance = 0.0
        """
        self._modify_table_property(name="read_repair_chance", val=random.random())

    def modify_table_speculative_retry(self):
        """
            Use the speculative retry property to configure rapid read protection. In a normal read,
            Cassandra sends data requests to just enough replica nodes to satisfy the consistency
            level. In rapid read protection, Cassandra sends out extra read requests to other replicas,
            even after the consistency level has been met. The speculative retry property specifies
            the trigger for these extra read requests.
                ALWAYS: Send extra read requests to all other replicas after every read.
                Xpercentile: Cassandra constantly tracks each table's typical read latency (in milliseconds).
                             If you set speculative retry to Xpercentile, Cassandra sends redundant read
                             requests if the coordinator has not received a response after X percent of the
                             table's typical latency time.
                Nms: Send extra read requests to all other replicas if the coordinator node has not received
                     any responses within N milliseconds.
                NONE: Do not send extra read requests after any read.
            default: speculative_retry = '99.0PERCENTILE';
        """
        options = ("'ALWAYS'",
                   "'%spercentile'" % random.randint(1, 99),
                   "'%sms'" % random.randint(1, 1000))
        self._modify_table_property(name="speculative_retry", val=random.choice(options))

    def disrupt_modify_table(self):
        # randomly select and run one of disrupt_modify_table* methods
        disrupt_func_name = random.choice([dm for dm in dir(self) if dm.startswith("modify_table")])
        disrupt_func = getattr(self, disrupt_func_name)
        disrupt_func()

    def disrupt_mgmt_repair(self):
        self._set_current_disruption('ManagementRepair')
        if not self.cluster.params.get('use_mgmt', default=None):
            self.log.warning('Scylla-manager configuration is not defined!')
            return
        server = self.monitoring_set.nodes[0].public_ip_address
        port = self.cluster.params.get('mgmt_port', default=10090)
        try:
            mgmt_client = mgmt.ScyllaMgmt(server=server, port=port)
            cluster_name = self.cluster.name
            cluster_id = mgmt_client.get_cluster(cluster_name)
            if not cluster_id:
                ip_addr_attr = 'public_ip_address' if self.cluster.params.get('cluster_backend') != 'gce' and \
                                                      len(self.cluster.datacenter) > 1 else 'private_ip_address'
                targets = [getattr(n, ip_addr_attr) for n in self.cluster.nodes]
                cluster_id = mgmt_client.add_cluster(cluster_name=cluster_name, hosts=targets)
            repair_timeout = 36 * 60 * 60  # 36 hours
            mgmt_client.run_repair(cluster_id, timeout=repair_timeout)
        except Exception as ex:
            self.log.error('Failed to execute scylla-manager repair, error: %s', ex)

    def disrupt_abort_repair(self):
        """
        Start repair target_node in background, then try to abort the repair streaming.
        """
        self._set_current_disruption('AbortRepairMonkey')
        self.log.debug("Start repair target_node in background")
        thread1 = threading.Thread(target=self.repair_nodetool_repair)
        thread1.start()

        def repair_streaming_exists():
            result = self.target_node.remoter.run('curl -X GET --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/stream_manager/stream_manager/"')
            return 'repair-' in result.stdout

        wait.wait_for(func=repair_streaming_exists,
                      timeout=10,
                      step=0.01,
                      text='Wait for repair starts')

        self.log.debug("Abort repair streaming by storage_service/force_terminate_repair API")
        self.target_node.remoter.run('curl -X GET --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/storage_service/force_terminate_repair"')
        thread1.join(timeout=120)

        self.log.debug("Execute a complete repair for target node")
        self.repair_nodetool_repair()


def log_time_elapsed_and_status(method):
    """
    Log time elapsed for method to run

    :param method: Remote method to wrap.
    :return: Wrapped method.
    """

    def print_nodetool_status(self):
        for node in self.cluster.nodes:
            try:
                if node == self.target_node:
                    self.log.info('Status for target %s: %s', node,
                                  self._run_nodetool('nodetool status', node))
                else:
                    self.log.info('Status for regular %s: %s', node,
                                  self._run_nodetool('nodetool status', node))
            except:
                self.log.info('unable to get nodetool status from: %s' % node)

    def wrapper(*args, **kwargs):
        print_nodetool_status(args[0])
        num_nodes_before = len(args[0].cluster.nodes)
        start_time = time.time()
        args[0].log.debug('Start disruption at `%s`', datetime.datetime.fromtimestamp(start_time))
        class_name = args[0].__class__.__name__.replace('Monkey', '')
        if class_name.find('Chaos') < 0:
            args[0].metrics_srv.event_start(class_name)
        result = None
        error = None
        status = True
        try:
            result = method(*args, **kwargs)
        except Exception, details:
            details = str(details)
            args[0].error_list.append(details)
            args[0].log.error('Unhandled exception in method %s', method, exc_info=True)
            error = details
        finally:
            end_time = time.time()
            time_elapsed = int(end_time - start_time)
            args[0].duration_list.append(time_elapsed)
            log_info = {'operation': args[0].current_disruption,
                        'start': int(start_time),
                        'end': int(end_time),
                        'duration': time_elapsed}
            args[0].operation_log.append(log_info)
            args[0].log.debug('%s duration -> %s s', args[0].current_disruption, time_elapsed)
            if error:
                log_info.update({'error': error})
                status = False

            if class_name.find('Chaos') < 0:
                args[0].metrics_srv.event_stop(class_name)
            disrupt = args[0].current_disruption.split()[0]
            log_info['node'] = args[0].current_disruption.replace(disrupt, '').strip()
            del log_info['operation']
            args[0].update_stats(disrupt, status, log_info)
            print_nodetool_status(args[0])
            num_nodes_after = len(args[0].cluster.nodes)
            if num_nodes_before != num_nodes_after:
                args[0].log.error('num nodes before %s and nodes after %s does not match' %
                                  (num_nodes_before, num_nodes_after))
            return result
    return wrapper


class NoOpMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        time.sleep(300)


class StopWaitStartMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_stop_wait_start_scylla_server(600)


class StopStartMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_stop_start_scylla_server()

class RestartThenRepairNodeMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_restart_then_repair_node()

class HardRebootNodeMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_hard_reboot_node()

class SoftRebootNodeMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_soft_reboot_node()

class DrainerMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_drain()


class CorruptThenRepairMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_destroy_data_then_repair()


class CorruptThenRebuildMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_destroy_data_then_rebuild()


class DecommissionMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self, add_node=True):
        self.disrupt_nodetool_decommission(add_node=add_node)


class NoCorruptRepairMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_no_corrupt_repair()


class MajorCompactionMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_major_compaction()


class RefreshMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_refresh()


class RefreshBigMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_refresh(big_sstable=True, skip_download=True)


class EnospcMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_enospc()


class EnospcAllNodesMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_enospc(all_nodes=True)


class NodeToolCleanupMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_cleanup()


class ChaosMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method()


class LimitedChaosMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        # Limit the nemesis scope:
        #  - NodeToolCleanupMonkey
        #  - DecommissionMonkey
        #  - DrainerMonkey
        #  - RefreshMonkey
        #  - StopStartMonkey
        #  - MajorCompactionMonkey
        #  - ModifyTableMonkey
        #  - EnospcMonkey
        #  - StopWaitStartMonkey
        #  - RestartNodeMonkey
        self.call_random_disrupt_method(disrupt_methods=['disrupt_nodetool_cleanup', 'disrupt_nodetool_decommission',
                                                         'disrupt_nodetool_drain', 'disrupt_nodetool_refresh',
                                                         'disrupt_stop_start_scylla_server', 'disrupt_major_compaction',
                                                         'disrupt_modify_table', 'disrupt_nodetool_enospc',
                                                         'disrupt_stop_wait_start_scylla_server',
                                                         'disrupt_restart_node'])


class AllMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(predefined_sequence=True)


class MdcChaosMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=['disrupt_destroy_data_then_repair', 'disrupt_no_corrupt_repair', 'disrupt_nodetool_decommission'])


class UpgradeNemesis(Nemesis):

    # # upgrade a single node
    # def upgrade_node(self, node):
    #     repo_file = self.cluster.params.get('repo_file', None,  'scylla.repo.upgrade')
    #     new_version = self.cluster.params.get('new_version', None,  '')
    #     upgrade_node_packages = self.cluster.params.get('upgrade_node_packages')
    #     self.log.info('Upgrading a Node')
    #
    #     # We assume that if update_db_packages is not empty we install packages from there.
    #     # In this case we don't use upgrade based on repo_file(ignored sudo yum update scylla...)
    #     orig_ver = node.remoter.run('rpm -qa scylla-server')
    #     if upgrade_node_packages:
    #         # update_scylla_packages
    #         node.remoter.send_files(upgrade_node_packages, '/tmp/scylla', verbose=True)
    #         # node.remoter.run('sudo yum update -y --skip-broken', connect_timeout=900)
    #         node.remoter.run('sudo yum install python34-PyYAML -y')
    #         # replace the packages
    #         node.remoter.run('rpm -qa scylla\*')
    #         node.remoter.run('sudo nodetool snapshot')
    #         # update *development* packages
    #         node.remoter.run('sudo rpm -UvhR --oldpackage /tmp/scylla/*development*', ignore_status=True)
    #         # and all the rest
    #         node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/* | true')
    #         node.remoter.run('rpm -qa scylla\*')
    #     elif repo_file:
    #         scylla_repo = get_data_dir_path(repo_file)
    #         node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
    #         node.remoter.run('sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup')
    #         node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
    #         # backup the data
    #         node.remoter.run('sudo cp /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml-backup')
    #         node.remoter.run('sudo nodetool snapshot')
    #         node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
    #         node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
    #         node.remoter.run('sudo yum clean all')
    #         ver_suffix = '-{}'.format(new_version) if new_version else ''
    #         node.remoter.run('sudo yum install scylla{0} scylla-server{0} scylla-jmx{0} scylla-tools{0}'
    #                          ' scylla-conf{0} scylla-kernel-conf{0} scylla-debuginfo{0} -y'.format(ver_suffix))
    #     # flush all memtables to SSTables
    #     node.remoter.run('sudo nodetool drain')
    #     node.remoter.run('sudo systemctl restart scylla-server.service')
    #     node.wait_db_up(verbose=True)
    #     new_ver = node.remoter.run('rpm -qa scylla-server')
    #     if orig_ver == new_ver:
    #         self.log.error('scylla-server version isn\'t changed')

    @log_time_elapsed_and_status
    def disrupt(self):
        self.log.info('Upgrade Nemesis begin')
        # get the number of nodes
        nodes_num = len(self.cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(nodes_num)]
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.upgrade_node(node)

        self.log.info('Upgrade Nemesis end')


class UpgradeNemesisOneNode(UpgradeNemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.log.info('UpgradeNemesisOneNode begin')
        self.upgrade_node(self.cluster.node_to_upgrade)

        self.log.info('UpgradeNemesisOneNode end')


class RollbackNemesis(Nemesis):

    # rollback a single node
    def rollback_node(self, node):
        self.log.info('Rollbacking a Node')
        orig_ver = node.remoter.run('rpm -qa scylla-server')
        node.remoter.run('sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo')
        # backup the data
        node.remoter.run('nodetool snapshot')
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum downgrade scylla scylla-server scylla-jmx scylla-tools scylla-conf scylla-kernel-conf scylla-debuginfo -y')
        # flush all memtables to SSTables
        node.remoter.run('nodetool drain')
        node.remoter.run('sudo cp {0}-backup {0}'.format(SCYLLA_YAML_PATH))
        node.remoter.run('sudo systemctl restart scylla-server.service')
        node.wait_db_up(verbose=True)
        new_ver = node.remoter.run('rpm -qa scylla-server')
        self.log.debug('original scylla-server version is %s, latest: %s' % (orig_ver, new_ver))
        if orig_ver == new_ver:
            raise ValueError('scylla-server version isn\'t changed')

    @log_time_elapsed_and_status
    def disrupt(self):
        self.log.info('Rollback Nemesis begin')
        # get the number of nodes
        nodes_num = len(self.cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(nodes_num)]
        # shuffle it so we will rollback the nodes in a
        # random order
        random.shuffle(indexes)

        # rollback all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.rollback_node(node)

        self.log.info('Rollback Nemesis end')


class ModifyTableMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_modify_table()

class MgmtRepair(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_mgmt_repair()


class AbortRepairMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_abort_repair()


class NodeTerminateAndReplace(Nemesis):
    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_terminate_and_replace_node()
