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

# pylint: disable=too-many-lines

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
import os
import re
import traceback
from collections import OrderedDict

from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster import SCYLLA_YAML_PATH, NodeSetupTimeout, NodeSetupFailed, Setup
from sdcm.mgmt import TaskStatus
from sdcm.utils.common import retrying, remote_get_file, get_non_system_ks_cf_list
from sdcm.log import SDCMAdapter
from sdcm.keystore import KeyStore
from sdcm.prometheus import nemesis_metrics_obj
from sdcm import mgmt
from sdcm import wait
from sdcm.sct_events import DisruptionEvent, DbEventsFilter
from sdcm.db_stats import PrometheusDBStats


class NoFilesFoundToDestroy(Exception):
    pass


class NoKeyspaceFound(Exception):
    pass


class FilesNotCorrupted(Exception):
    pass


class UnsupportedNemesis(Exception):
    """ raised from within a nemesis execution to skip this nemesis"""


class Nemesis():  # pylint: disable=too-many-instance-attributes,too-many-public-methods

    disruptive = False
    run_with_gemini = True
    networking = False

    def __init__(self, tester_obj, termination_event):
        self.tester = tester_obj  # ClusterTester object
        self.cluster = tester_obj.db_cluster
        self.loaders = tester_obj.loaders
        self.monitoring_set = tester_obj.monitors
        self.target_node = None
        logger = logging.getLogger(__name__)
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.termination_event = termination_event
        self.operation_log = []
        self.current_disruption = None
        self.duration_list = []
        self.error_list = []
        self.interval = 0
        self.start_time = time.time()
        self.stats = {}
        self.metrics_srv = nemesis_metrics_obj()
        self._random_sequence = None

    def update_stats(self, disrupt, status=True, data=None):
        if not data:
            data = {}
        key = {True: 'runs', False: 'failures'}
        if disrupt not in self.stats:
            self.stats[disrupt] = {'runs': [], 'failures': [], 'cnt': 0}
        self.stats[disrupt][key[status]].append(data)
        self.stats[disrupt]['cnt'] += 1
        self.log.info('Update nemesis info: %s', self.stats)
        if self.tester.create_stats:
            self.tester.update({'nemesis': self.stats})

    def publish_event(self, disrupt, status=True, data=None):
        if not data:
            data = {}
        data['node'] = self.target_node
        DisruptionEvent(name=disrupt, status=status, **data)

    def set_current_running_nemesis(self, node):
        node.running_nemesis = self.__class__.__name__

    def set_target_node(self):
        """Set node to run nemesis on

        Keyword Arguments:
            is_running {bool} -- if True, method called upon nemesis start/running (default: {False})
        """
        filter_seed = self.cluster.params.get('nemesis_filter_seeds', default=True)

        if filter_seed:
            non_seed_nodes = [node for node in self.cluster.nodes if not node.is_seed and not node.running_nemesis]
            # if non_seed_nodes is empty, nemesis would failed.
            self.log.debug("List of NonSeed nodes: {}".format([node.name for node in non_seed_nodes]))
            if not non_seed_nodes:
                self.log.warning("Cluster doesn't contain free nonseeds nodes. Test will failed")
            self.target_node = random.choice(non_seed_nodes)
        else:
            all_nodes = [node for node in self.cluster.nodes if not node.running_nemesis]
            self.target_node = random.choice(all_nodes)

        # Set name of nemesis, which is going to run on target node
        self.set_current_running_nemesis(node=self.target_node)

        self.log.info('Current Target: %s with running nemesis: %s', self.target_node, self.target_node.running_nemesis)

    def run(self, interval=30):
        interval *= 60
        self.log.info('Interval: %s s', interval)
        self.interval = interval
        while not self.termination_event.isSet():
            cur_interval = interval
            self.set_target_node()
            self._set_current_disruption(report=False)
            try:
                self.disrupt()
            except UnsupportedNemesis:
                cur_interval = 0
            finally:
                self.target_node.running_nemesis = None
                self.termination_event.wait(timeout=cur_interval)

    def report(self):
        if self.duration_list:
            avg_duration = sum(self.duration_list) / len(self.duration_list)
        else:
            avg_duration = 0

        self.log.info('Report')
        self.log.info('DB Version: %s', getattr(self.cluster.nodes[0], "scylla_version", "n/a"))
        self.log.info('Interval: %s s', self.interval)
        self.log.info('Average duration: %s s', avg_duration)
        self.log.info('Total execution time: %s s', int(time.time() - self.start_time))
        self.log.info('Times executed: %s', len(self.duration_list))
        self.log.info('Unexpected errors: %s', len(self.error_list))
        self.log.info('Operation log:')
        for operation in self.operation_log:
            self.log.info(operation)

    def get_list_of_disrupt_methods_for_nemesis_subclasses(self, disruptive=None, run_with_gemini=None, networking=None):  # pylint: disable=invalid-name
        if disruptive is not None:
            return self._get_subclasses_disrupt_methods(disruptive=disruptive)
        if run_with_gemini is not None:
            return self._get_subclasses_disrupt_methods(run_with_gemini=run_with_gemini)
        if networking is not None:
            return self._get_subclasses_disrupt_methods(networking=networking)
        return None

    def _get_subclasses_disrupt_methods(self, **kwargs):
        subclasses_list = self._get_subclasses(**kwargs)
        disrupt_methods_list = []
        for subclass in subclasses_list:
            method_name = re.search(
                r'self\.(?P<method_name>disrupt_[A-Za-z_]+?)\(.*\)', inspect.getsource(subclass), flags=re.MULTILINE)
            if method_name:
                disrupt_methods_list.append(method_name.group('method_name'))
        self.log.debug("Gathered subclass methods: {}".format(disrupt_methods_list))
        return disrupt_methods_list

    @staticmethod
    def _get_subclasses(**kwargs):
        filter_by_attribute, value = list(kwargs.items())[0]

        nemesis_subclasses = [nemesis for nemesis in Nemesis.__subclasses__()
                              if getattr(nemesis, filter_by_attribute) == value and
                              (nemesis not in COMPLEX_NEMESIS or nemesis not in DEPRECATED_LIST_OF_NEMESISES)]
        for inherit_nemesis_class in RELATIVE_NEMESIS_SUBCLASS_LIST:
            nemesis_subclasses.extend([nemesis for nemesis in inherit_nemesis_class.__subclasses__()
                                       if getattr(nemesis, filter_by_attribute) == value and
                                       (nemesis not in COMPLEX_NEMESIS or nemesis not in DEPRECATED_LIST_OF_NEMESISES)])
        return nemesis_subclasses

    def __str__(self):
        try:
            return str(self.__class__).split("'")[1]
        except Exception:  # pylint: disable=broad-except
            return str(self.__class__)

    def _kill_scylla_daemon(self):
        self.log.info('Kill all scylla processes in %s', self.target_node)
        kill_cmd = "sudo pkill -9 scylla"
        self.target_node.remoter.run(kill_cmd, ignore_status=True)

        # Let's wait for the target Node to have their services re-started
        self.log.info('Waiting scylla services to be restarted after we killed them...')
        self.target_node.wait_db_up(timeout=14400)
        self.log.info('Waiting JMX services to be restarted after we killed them...')
        self.target_node.wait_jmx_up()

    def disrupt_stop_wait_start_scylla_server(self, sleep_time=300):  # pylint: disable=invalid-name
        self._set_current_disruption('StopWaitStartService %s' % self.target_node)
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
        self.log.info("Sleep for %s seconds", sleep_time)
        time.sleep(sleep_time)
        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt_stop_start_scylla_server(self):  # pylint: disable=invalid-name
        self._set_current_disruption('StopStartService %s' % self.target_node)
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
        self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    # This nemesis should be run with "private" ip_ssh_connections till the issue #665 is not fixed
    def disrupt_restart_then_repair_node(self):  # pylint: disable=invalid-name
        self._set_current_disruption('RestartThenRepairNode %s' % self.target_node)
        self.target_node.restart()
        self.log.info('Waiting scylla services to start after node restart')
        self.target_node.wait_db_up(timeout=28800)  # 8 hours
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

    def disrupt_multiple_hard_reboot_node(self):  # pylint: disable=invalid-name
        num_of_reboots = random.randint(2, 10)
        for i in range(num_of_reboots):
            self._set_current_disruption('MultipleHardRebootNode %s' % self.target_node)
            self.log.debug("Rebooting {} out of {} times".format(i+1, num_of_reboots))
            self.target_node.reboot(hard=True)
            if random.choice([True, False]):
                self.log.info('Waiting scylla services to start after node reboot')
                self.target_node.wait_db_up()
            else:
                self.log.info('Waiting JMX services to start after node reboot')
                self.target_node.wait_jmx_up()
            sleep_time = random.randint(0, 100)
            self.log.info('Sleep {} seconds after hard reboot and service-up for node {}'.format(sleep_time, self.target_node))
            time.sleep(sleep_time)

    def disrupt_soft_reboot_node(self):
        self._set_current_disruption('SoftRebootNode %s' % self.target_node)
        self.target_node.reboot(hard=False)
        self.log.info('Waiting scylla services to start after node reboot')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node reboot')
        self.target_node.wait_jmx_up()

    def disrupt_restart_with_resharding(self):
        self._set_current_disruption('RestartNode with resharding %s' % self.target_node)
        murmur3_partitioner_ignore_msb_bits = 15  # pylint: disable=invalid-name
        self.log.info(f'Restart node with resharding. New murmur3_partitioner_ignore_msb_bits value: '
                      f'{murmur3_partitioner_ignore_msb_bits}')
        self.target_node.restart_node_with_resharding(
            murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits)
        self.log.info('Waiting scylla services to start after node restart')
        self.target_node.wait_db_up()
        self.log.info('Waiting JMX services to start after node restart')
        self.target_node.wait_jmx_up()

        # Wait 5 minutes our before return back the default value
        self.log.debug('Wait 5 minutes our before return murmur3_partitioner_ignore_msb_bits back the default value (12)')
        time.sleep(360)
        self.log.info('Set back murmur3_partitioner_ignore_msb_bits value to 12')
        self.target_node.restart_node_with_resharding()

    @retrying(n=10, allowed_exceptions=(NoKeyspaceFound, NoFilesFoundToDestroy))
    def _choose_file_for_destroy(self, ks_cfs):
        file_for_destroy = ''

        ks_cf_for_destroy = random.choice(ks_cfs)  # expected value as: 'keyspace1.standard1'

        ks_cf_for_destroy = ks_cf_for_destroy.replace('.', '/')
        files = self.target_node.remoter.run("sudo sh -c 'find /var/lib/scylla/data/%s-* -type f'" % ks_cf_for_destroy,
                                             verbose=False)
        if files.stderr:
            raise NoFilesFoundToDestroy('Failed to get data files for destroy in {}. Error: {}'.format(ks_cf_for_destroy,
                                                                                                       files.stderr))

        for one_file in files.stdout.split():
            if not one_file or '/' not in one_file:
                continue
            file_name = os.path.basename(one_file)
            # The file name like: /var/lib/scylla/data/scylla_bench/test-f60e4f30c98f11e98d46000000000002/mc-220-big-Data.db
            # For corruption we need to remove all files that their names are started from "mc-220-" (MC format)
            # Old format: "system-truncated-ka-" (system-truncated-ka-7-Data.db)
            # Search for "<digit>-" substring
            file_name_template = re.search(r"(.*-\d+)-", file_name).group(1)
            file_for_destroy = one_file.replace(file_name, file_name_template + '-*')
            self.log.debug('Selected files for destroy: {}'.format(file_for_destroy))
            if file_for_destroy:
                break

        if not file_for_destroy:
            raise NoFilesFoundToDestroy('Data file for destroy is not found in {}'.format(ks_cf_for_destroy))

        return file_for_destroy

    def _destroy_data_and_restart_scylla(self):

        ks_cfs = get_non_system_ks_cf_list(loader_node=random.choice(self.loaders.nodes),
                                           db_node=self.target_node)
        if not ks_cfs:
            raise NoKeyspaceFound(
                'Non-system keyspace and table are not found. CorruptThenRepair nemesis can\'t be run')

        # Stop scylla service before deleting sstables to avoid partial deletion of files that are under compaction
        self.target_node.stop_scylla_server(verify_up=False, verify_down=True)

        try:
            # Remove 5 data files
            for _ in range(5):
                file_for_destroy = self._choose_file_for_destroy(ks_cfs)

                result = self.target_node.remoter.run('sudo rm -f %s' % file_for_destroy)
                if result.stderr:
                    raise FilesNotCorrupted('Files were not corrupted. CorruptThenRepair nemesis can\'t be run. '
                                            'Error: {}'.format(result))
                self.log.debug('Files {} were destroyed'.format(file_for_destroy))

        finally:
            self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    def disrupt(self):
        raise NotImplementedError('Derived classes must implement disrupt()')

    def get_disrupt_name(self):
        return self.current_disruption.split()[0]

    def get_class_name(self):
        return self.__class__.__name__.replace('Monkey', '')

    def _set_current_disruption(self, label=None, report=True, node=None):
        current_node = node if node else self.target_node

        if not label:
            label = "%s on target node %s" % (self.__class__.__name__, current_node)
        self.log.debug('Set current_disruption -> %s', label)
        self.current_disruption = label
        self.log.info(label)
        if report:
            DisruptionEvent(type='start', name=self.get_disrupt_name(), status=True, node=str(current_node))

    def disrupt_destroy_data_then_repair(self):  # pylint: disable=invalid-name
        self._set_current_disruption('CorruptThenRepair %s' % self.target_node)
        self._destroy_data_and_restart_scylla()
        # try to save the node
        self.repair_nodetool_repair()

    def disrupt_destroy_data_then_rebuild(self):  # pylint: disable=invalid-name
        self._set_current_disruption('CorruptThenRebuild %s' % self.target_node)
        self._destroy_data_and_restart_scylla()
        # try to save the node
        self.repair_nodetool_rebuild()

    def disrupt_nodetool_drain(self):
        self._set_current_disruption('Drainer %s' % self.target_node)
        result = self.target_node.run_nodetool("drain")
        self.cluster.check_cluster_health()
        if result is not None:
            self.target_node.stop_scylla_server(verify_up=False, verify_down=True)
            self.target_node.start_scylla_server(verify_up=True, verify_down=False)

    @retrying(n=3, sleep_time=60, allowed_exceptions=(NodeSetupFailed, NodeSetupTimeout))
    def _add_and_init_new_cluster_node(self, old_node_ip=None, timeout=10800):
        """When old_node_private_ip is not None replacement node procedure is initiated"""
        self.log.info("Adding new node to cluster...")
        new_node = self.cluster.add_nodes(count=1, dc_idx=self.target_node.dc_idx, enable_auto_bootstrap=True)[0]
        self.set_current_running_nemesis(node=new_node)  # prevent to run nemesis on new node when running in parallel
        new_node.replacement_node_ip = old_node_ip
        try:
            self.cluster.wait_for_init(node_list=[new_node], timeout=timeout)
        except (NodeSetupFailed, NodeSetupTimeout):
            self.log.warning("Setup of the '%s' failed, removing it from list of nodes" % new_node)
            self.cluster.nodes.remove(new_node)
            self.log.warning("Node will not be terminated. Please terminate manually!!!")
            raise
        self.cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
        self.monitoring_set.reconfigure_scylla_monitoring()
        return new_node

    def _terminate_cluster_node(self, node):
        self.cluster.terminate_node(node)
        self.monitoring_set.reconfigure_scylla_monitoring()

    def disrupt_nodetool_decommission(self, add_node=True):
        # pylint: disable=too-many-locals

        def get_node_info_list(verification_node):
            try:
                return self.cluster.get_node_info_list(verification_node)
            except Exception as details:  # pylint: disable=broad-except
                self.log.error(str(details))
                return None
        self._set_current_disruption('Decommission %s' % self.target_node)
        target_node_ip = self.target_node.ip_address
        self.target_node.run_nodetool("decommission")
        verification_node = random.choice(self.cluster.nodes)
        node_info_list = get_node_info_list(verification_node)
        while verification_node == self.target_node or node_info_list is None:
            verification_node = random.choice(self.cluster.nodes)
            node_info_list = get_node_info_list(verification_node)

        nodetool_ips = [node_info['ip'] for node_info in node_info_list]
        error_msg = ('Node that was decommissioned %s still in the cluster. '
                     'Cluster status info: %s' % (self.target_node,
                                                  node_info_list))
        if target_node_ip in nodetool_ips:
            self.log.info('Decommission %s FAIL', self.target_node)
            self.log.error(error_msg)
        else:
            self.log.info('Decommission %s PASS', self.target_node)
            self._terminate_cluster_node(self.target_node)
            # Replace the node that was terminated.
            new_node = None
            if add_node:
                # When adding node after decommission the node is declared as up only after it completed bootstrapping,
                # increasing the timeout for now
                new_node = self._add_and_init_new_cluster_node()
            # after decomission and add_node, the left nodes have data that isn't part of their tokens anymore.
            # In order to eliminate cases that we miss a "data loss" bug because of it, we cleanup this data.
            # This fix important when just user profile is run in the test and "keyspace1" doesn't exist.
            try:
                test_keyspaces = self.cluster.get_test_keyspaces()
                for node in self.cluster.nodes:
                    for keyspace in test_keyspaces:
                        node.run_nodetool(sub_cmd='cleanup', args=keyspace)
            finally:
                if new_node:
                    new_node.running_nemesis = None

    def disrupt_terminate_and_replace_node(self):  # pylint: disable=invalid-name
        # using "Replace a Dead Node" procedure from http://docs.scylladb.com/procedures/replace_dead_node/
        self._set_current_disruption('TerminateAndReplaceNode %s' % self.target_node)
        old_node_ip = self.target_node.ip_address
        self._terminate_cluster_node(self.target_node)
        new_node = self._add_and_init_new_cluster_node(old_node_ip)
        try:
            self.repair_nodetool_repair(new_node)
        finally:
            new_node.running_nemesis = None

    def disrupt_no_corrupt_repair(self):
        self._set_current_disruption('NoCorruptRepair %s' % self.target_node)
        self.repair_nodetool_repair()

    def disrupt_major_compaction(self):
        self._set_current_disruption('MajorCompaction %s' % self.target_node)
        self.target_node.run_nodetool("compact")

    def disrupt_nodetool_refresh(self, big_sstable=True, skip_download=False):
        self._set_current_disruption('Refresh keyspace1.standard1 on {}'.format(self.target_node.name))
        if big_sstable:
            # 100G, the big file will be saved to GCE image
            # Fixme: It's very slow and unstable to download 100G files from S3 to GCE instances,
            #        currently we actually uploaded a small file (3.6 K) to S3.
            #        We had a solution to save the file in GCE image, it requires bigger boot disk.
            #        In my old test, the instance init is easy to fail. We can try to use a
            #        split shared disk to save the 100GB file.
            sstable_url = 'https://s3.amazonaws.com/scylla-qa-team/keyspace1.standard1.tar.gz'
            sstable_file = "/tmp/keyspace1.standard1.tar.gz"
            sstable_md5 = '76cca3135e175d859c0efb67c6a7b233'
        else:
            # 100M (300000 rows)
            sstable_url = 'https://s3.amazonaws.com/scylla-qa-team/keyspace1.standard1.100M.tar.gz'
            sstable_file = '/tmp/keyspace1.standard1.100M.tar.gz'
            sstable_md5 = 'f641f561067dd612ff95f2b89bd12530'
        if not skip_download:
            key_store = KeyStore()
            creds = key_store.get_scylladb_upload_credentials()
            remote_get_file(self.target_node.remoter, sstable_url, sstable_file,
                            hash_expected=sstable_md5, retries=2,
                            user_agent=creds['user_agent'])

        self.log.debug('Prepare keyspace1.standard1 if it does not exist')
        self._prepare_test_table(ks='keyspace1')
        result = self.target_node.run_nodetool(sub_cmd="cfstats", args="keyspace1.standard1")
        if result is not None and result.exit_status == 0:
            result = self.target_node.remoter.run("sudo ls -t /var/lib/scylla/data/keyspace1/")
            upload_dir = result.stdout.split()[0]
            self.target_node.remoter.run('sudo tar xvfz {} -C /var/lib/scylla/data/keyspace1/{}/upload/'.format(
                sstable_file, upload_dir))
            self.target_node.run_nodetool(sub_cmd="refresh", args="-- keyspace1 standard1")
            cmd = "select * from keyspace1.standard1 where key=0x314e344b4d504d4b4b30"
            self.target_node.run_cqlsh(cmd)

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
            cmd = "sudo journalctl --no-tail --no-pager -u scylla-server.service|grep 'No space left on device'|wc -l"
            result = node.remoter.run(cmd, verbose=True)
            return int(result.stdout)

        def approach_enospc(node):
            # get the size of free space (default unit: KB)
            result = node.remoter.run("df -l|grep '/var/lib/scylla'")
            free_space_size = result.stdout.split()[3]

            occupy_space_size = int(int(free_space_size) * 90 / 100)
            occupy_space_cmd = 'sudo fallocate -l {}K /var/lib/scylla/occupy_90percent.{}'.format(
                occupy_space_size, datetime.datetime.now().strftime('%s'))
            self.log.debug('Cost 90% free space on /var/lib/scylla/ by {}'.format(occupy_space_cmd))
            try:
                node.remoter.run(occupy_space_cmd, verbose=True)
            except Exception as details:  # pylint: disable=broad-except
                self.log.error(str(details))
            return search_database_enospc(node) > orig_errors

        for node in nodes:
            with DbEventsFilter(type='NO_SPACE_ERROR', node=node), \
                    DbEventsFilter(type='BACKTRACE', line='No space left on device', node=node), \
                    DbEventsFilter(type='DATABASE_ERROR', line='No space left on device', node=node), \
                    DbEventsFilter(type='FILESYSTEM_ERROR', line='No space left on device', node=node):

                result = node.remoter.run('cat /proc/mounts')
                if '/var/lib/scylla' not in result.stdout:
                    self.log.error("Scylla doesn't use an individual storage, skip enospc test")
                    continue

                # check original ENOSPC error
                orig_errors = search_database_enospc(node)
                wait.wait_for(func=approach_enospc,
                              timeout=300,
                              step=5,
                              text='Wait for new ENOSPC error occurs in database',
                              node=node)

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
        # pylint: disable=too-many-branches

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
        try:
            disrupt_method()
        except Exception as exc:  # pylint: disable=broad-except
            error_msg = "Exception in random_disrupt_method %s: %s", disrupt_method_name, exc
            self.log.error(error_msg)
            self.error_list.append(error_msg)
            raise
        else:
            self.log.info("<<<<<<<<<<<<<Finished random_disrupt_method %s" % disrupt_method_name)
        finally:
            self.metrics_srv.event_stop(disrupt_method_name)

    def repair_nodetool_repair(self, node=None):
        node = node if node else self.target_node
        node.run_nodetool("repair")

    def repair_nodetool_rebuild(self):
        self.target_node.run_nodetool('rebuild')

    def disrupt_nodetool_cleanup(self):
        # This fix important when just user profile is run in the test and "keyspace1" doesn't exist.
        test_keyspaces = self.cluster.get_test_keyspaces()
        for node in self.cluster.nodes:
            self._set_current_disruption('NodetoolCleanupMonkey %s' % node, node=node)
            for keyspace in test_keyspaces:
                node.run_nodetool(sub_cmd="cleanup", args=keyspace)

    def _prepare_test_table(self, ks='keyspace1'):
        # get the count of the truncate table
        test_keyspaces = self.cluster.get_test_keyspaces()

        # if keyspace doesn't exist, create it by cassandra-stress
        if ks not in test_keyspaces:
            stress_cmd = "cassandra-stress write n=400000 cl=QUORUM -port jmx=6868 -mode native cql3 -schema 'replication(factor=3)' -log interval=5"
            cs_thread = self.tester.run_stress_thread(stress_cmd=stress_cmd, keyspace_name=ks)
            cs_thread.verify_results()

    def disrupt_truncate(self):
        self._set_current_disruption('TruncateMonkey {}'.format(self.target_node))

        keyspace_truncate = 'ks_truncate'
        table = 'standard1'

        self._prepare_test_table(ks=keyspace_truncate)

        # do the actual truncation
        self.target_node.run_cqlsh(cmd='TRUNCATE {}.{}'.format(keyspace_truncate, table), timeout=120)

    def _modify_table_property(self, name, val, filter_out_table_with_counter=False):
        disruption_name = "".join([p.strip().capitalize() for p in name.split("_")])
        self._set_current_disruption('ModifyTableProperties%s %s' % (disruption_name, self.target_node))

        ks_cfs = get_non_system_ks_cf_list(loader_node=random.choice(self.loaders.nodes),
                                           db_node=self.target_node,
                                           filter_out_table_with_counter=filter_out_table_with_counter,
                                           filter_out_mv=True)  # not allowed to modify MV

        keyspace_table = random.choice(ks_cfs) if ks_cfs else ks_cfs
        if not keyspace_table:
            raise ValueError('Non-system keyspace and table are not found. ModifyTableProperties nemesis can\'t be run')

        cmd = "ALTER TABLE {keyspace_table} WITH {name} = {val};".format(
            keyspace_table=keyspace_table, name=name, val=val)
        self.log.info('_modify_table_property: {}'.format(cmd))
        with self.tester.cql_connection_patient(self.target_node) as session:
            session.execute(cmd)

    def modify_table_comment(self):
        # default: comment = ''
        prop_val = ''.join(random.choice(string.ascii_letters) for _ in range(24))
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

    def modify_table_bloom_filter_fp_chance(self):  # pylint: disable=invalid-name
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
        self._modify_table_property(name="bloom_filter_fp_chance", val=random.random() / 2)

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

    def modify_table_dclocal_read_repair_chance(self):  # pylint: disable=invalid-name
        """
            The probability that a successful read operation triggers a read repair.
            Unlike the repair controlled by read_repair_chance, this repair is limited to
            replicas in the same DC as the coordinator. The value must be between 0 and 1
            default: dclocal_read_repair_chance = 0.1
        """
        self._modify_table_property(name="dclocal_read_repair_chance", val=random.choice([0, 0.2, 0.5, 0.9]))

    def modify_table_default_time_to_live(self):  # pylint: disable=invalid-name
        """
            The value of this property is a number of seconds. If it is set, Cassandra applies a
            default TTL marker to each column in the table, set to this value. When the table TTL
            is exceeded, Cassandra tombstones the table.
            default: default_time_to_live = 0
        """
        # Select table without columns with "counter" type for this nemesis - issue #1037:
        #    Modify_table nemesis chooses first non-system table, and modify default_time_to_live of it.
        #    But table with counters doesn't support this
        self._modify_table_property(name="default_time_to_live", val=random.randint(864000, 630720000),
                                    filter_out_table_with_counter=True)  # max allowed TTL - 20 years (630720000)

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

    def modify_table_memtable_flush_period_in_ms(self):  # pylint: disable=invalid-name
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
        self._modify_table_property(name="read_repair_chance", val=random.choice([0, 0.2, 0.5, 0.9]))

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

    def disrupt_mgmt_repair_cli(self):
        self._set_current_disruption('ManagementRepair')
        if not self.cluster.params.get('use_mgmt', default=None):
            raise UnsupportedNemesis('Scylla-manager configuration is not defined!')

        manager_node = self.monitoring_set.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)

        cluster_name = self.cluster.name
        mgr_cluster = manager_tool.get_cluster(cluster_name)
        if not mgr_cluster:
            self.log.debug("Could not find cluster : {} on Manager. Adding it to Manager".format(cluster_name))
            ip_addr_attr = 'public_ip_address' if self.cluster.params.get('cluster_backend') != 'gce' and \
                Setup.INTRA_NODE_COMM_PUBLIC else 'private_ip_address'
            targets = [getattr(n, ip_addr_attr) for n in self.cluster.nodes]
            mgr_cluster = manager_tool.add_cluster(name=cluster_name, host=targets[0], disable_automatic_repair=True)
        mgr_task = mgr_cluster.create_repair_task()
        task_final_status = mgr_task.wait_and_get_final_status(timeout=86400)  # timeout is 24 hours
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(
            mgr_task.id, str(mgr_task.status))
        self.log.info('Task: {} is done.'.format(mgr_task.id))

        self.log.debug("sctool version is : {}".format(manager_tool.version))

    def disrupt_abort_repair(self):
        """
        Start repair target_node in background, then try to abort the repair streaming.
        """
        self._set_current_disruption('AbortRepairMonkey')
        self.log.debug("Start repair target_node in background")
        thread1 = threading.Thread(target=self.repair_nodetool_repair)
        thread1.start()

        def repair_streaming_exists():
            active_repair_cmd = 'curl -s -X GET --header "Content-Type: application/json" --header ' \
                                '"Accept: application/json" "http://127.0.0.1:10000/storage_service/active_repair/"'
            result = self.target_node.remoter.run(active_repair_cmd)
            active_repairs = re.match(r".*\[(\d)\].*", result.stdout)
            if active_repairs:
                self.log.debug("Found '%s' active repairs", active_repairs.group(1))
                return True
            return False

        wait.wait_for(func=repair_streaming_exists,
                      timeout=300,
                      step=1,
                      throw_exc=True,
                      text='Wait for repair starts')

        self.log.debug("Abort repair streaming by storage_service/force_terminate_repair API")
        with DbEventsFilter(type='DATABASE_ERROR', line="repair's stream failed: streaming::stream_exception", node=self.target_node), \
                DbEventsFilter(type='RUNTIME_ERROR', line='Can not find stream_manager', node=self.target_node), \
                DbEventsFilter(type='RUNTIME_ERROR', line='is aborted', node=self.target_node), \
                DbEventsFilter(type='RUNTIME_ERROR', line='Failed to repair', node=self.target_node):

            self.target_node.remoter.run(
                'curl -X POST --header "Content-Type: application/json" --header "Accept: application/json" "http://127.0.0.1:10000/storage_service/force_terminate_repair"')
            thread1.join(timeout=120)
            time.sleep(10)  # to make sure all failed logs/events, are ignored correctly

        self.log.debug("Execute a complete repair for target node")
        self.repair_nodetool_repair()

    def disrupt_validate_hh_short_downtime(self):  # pylint: disable=invalid-name
        """
            Validates that hinted handoff mechanism works: there were no drops and errors
            during short stop of one of the nodes in cluster
        """
        self._set_current_disruption("ValidateHintedHandoffShortDowntime")
        start_time = time.time()
        self.target_node.stop_scylla()
        time.sleep(10)
        self.target_node.start_scylla()
        time.sleep(120)  # Wait to complete hints sending
        assert self.tester.hints_sending_in_progress() is False, "Hints are sent too slow"
        self.tester.verify_no_drops_and_errors(starting_from=start_time)

    def disrupt_snapshot_operations(self):
        self._set_current_disruption('SnapshotOperations')
        result = self.target_node.run_nodetool('snapshot')
        self.log.debug(result)
        if result.stderr:
            self.tester.fail(result.stderr)
        snapshot_name = re.findall(r'(\d+)', result.stdout, re.MULTILINE)[0]
        result = self.target_node.run_nodetool('listsnapshots')
        self.log.debug(result)
        if snapshot_name in result.stdout and not result.stderr:
            self.log.info('Snapshot %s created' % snapshot_name)
        else:
            self.tester.fail('Snapshot %s creating failed %s' % (snapshot_name, result.stderr))

        result = self.target_node.run_nodetool('clearsnapshot')
        self.log.debug(result)
        if result.stderr:
            self.tester.fail(result.stderr)

    def disrupt_show_toppartitions(self):
        def _parse_toppartitions_output(output):
            """parsing output of toppartitions

            input format stored in output parameter:
            WRITES Sampler:
              Cardinality: ~10 (15 capacity)
              Top 10 partitions:
                Partition     Count       +/-
                9        11         0
                0         1         0
                1         1         0

            READS Sampler:
              Cardinality: ~10 (256 capacity)
              Top 3 partitions:
                Partition     Count       +/-
                0         3         0
                1         3         0
                2         3         0
                3         2         0

            return Dict:
            {
                'READS': {
                    'toppartitions': '10',
                    'partitions': OrderedDict('0': {'count': '1', 'margin': '0'},
                                              '1': {'count': '1', 'margin': '0'},
                                              '2': {'count': '1', 'margin': '0'}),
                    'cardinality': '10',
                    'capacity': '256',
                },
                'WRITES': {
                    'toppartitions': '10',
                    'partitions': OrderedDict('10': {'count': '1', 'margin': '0'},
                                              '11': {'count': '1', 'margin': '0'},
                                              '21': {'count': '1', 'margin': '0'}),
                    'cardinality': '10',
                    'capacity': '256',
                    'sampler': 'WRITES'
                }
            }


            Arguments:
                output {str} -- stdout of nodetool topparitions command

            Returns:
                dict -- result of parsing
            """

            pattern1 = r"(?P<sampler>[A-Z]+)\sSampler:\W+Cardinality:\s~(?P<cardinality>[0-9]+)\s\((?P<capacity>[0-9]+)\scapacity\)\W+Top\s(?P<toppartitions>[0-9]+)\spartitions:"
            pattern2 = r"(?P<partition>[\w:]+)\s+(?P<count>[\d]+)\s+(?P<margin>[\d]+)"
            toppartitions = {}
            for out in output.split('\n\n'):
                partition = OrderedDict()
                sampler_data = re.match(pattern1, out, re.MULTILINE)
                sampler_data = sampler_data.groupdict()
                partitions = re.findall(pattern2, out, re.MULTILINE)
                for val in partitions:
                    partition.update({val[0]: {'count': val[1], 'margin': val[2]}})
                sampler_data.update({'partitions': partition})
                toppartitions[sampler_data.pop('sampler')] = sampler_data
            return toppartitions

        def generate_random_parameters_values():  # pylint: disable=invalid-name
            ks_cf_list = get_non_system_ks_cf_list(self.loaders.nodes[0], self.cluster.nodes[0])
            try:
                ks, cf = random.choice(ks_cf_list).split('.')
            except IndexError as details:
                self.log.error('User-defined Keyspace and ColumnFamily are not found %s.', ks_cf_list)
                self.log.debug('Error during choosing keyspace and columnfamily %s', details)
                raise Exception('User-defined Keyspace and ColumnFamily are not found. \n{}'.format(details))
            return {
                'toppartition': str(random.randint(5, 20)),
                'samplers': random.choice(['writes', 'reads', 'writes,reads']),
                'capacity': str(random.randint(100, 1024)),
                'ks': ks,
                'cf': cf,
                'duration': str(random.randint(1000, 10000))
            }

        self._set_current_disruption("ShowTopPartitions")
        # workaround for issue #4519
        self.target_node.run_nodetool('cfstats')

        args = generate_random_parameters_values()
        sub_cmd_args = "{ks} {cf} {duration} -s {capacity} -k {toppartition} -a {samplers}".format(**args)

        result = self.target_node.run_nodetool(sub_cmd='toppartitions', args=sub_cmd_args)

        toppartition_result = _parse_toppartitions_output(result.stdout)
        for sampler in args['samplers'].split(','):
            sampler = sampler.upper()
            self.tester.assertIn(sampler, toppartition_result,
                                 msg="{} sampler not found in result".format(sampler))
            self.tester.assertTrue(toppartition_result[sampler]['toppartitions'] == args['toppartition'],
                                   msg="Wrong expected and actual top partitions number for {} sampler".format(sampler))
            self.tester.assertTrue(toppartition_result[sampler]['capacity'] == args['capacity'],
                                   msg="Wrong expected and actual capacity number for {} sampler".format(sampler))
            self.tester.assertLessEqual(len(toppartition_result[sampler]['partitions'].keys()), int(args['toppartition']),
                                        msg="Wrong number of requested and expected toppartitions for {} sampler".format(sampler))

    def disrupt_network_random_interruptions(self):  # pylint: disable=invalid-name
        # pylint: disable=too-many-locals
        self._set_current_disruption('NetworkRandomInterruption')
        if not self.cluster.aws_extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `aws_extra_network_interface: True`")

        # get the last 10min avg network bandwidth used, and limit  30% to 70% of it
        prometheus_stats = PrometheusDBStats(host=self.monitoring_set.nodes[0].external_address)
        query = 'avg(irate(node_network_receive_bytes_total{instance=~"%s", device="eth0"}[30s]))' % self.target_node.ip_address
        now = time.time()
        results = prometheus_stats.query(query=query, start=now - 600, end=now)
        avg_bitrate_per_node = max([float(avg_rate) for _, avg_rate in results[0]["values"]])
        avg_mpbs_per_node = avg_bitrate_per_node / 1024 / 1024

        if avg_mpbs_per_node > 10:
            min_limit = int(round(avg_mpbs_per_node * 0.30))
            max_limit = int(round(avg_mpbs_per_node * 0.70))
            rate_limit_suffix = "mbps"
        else:
            avg_kbps_per_node = avg_bitrate_per_node / 1024
            min_limit = int(round(avg_kbps_per_node * 0.30))
            max_limit = int(round(avg_kbps_per_node * 0.70))
            rate_limit_suffix = "kbps"

        rate_limit = "{}{}".format(random.randrange(min_limit, max_limit), rate_limit_suffix)

        # random packet loss - between 1% - 15%
        loss_percentage = random.randrange(1, 15)

        # random packet corruption - between 1% - 15%
        corrupt_percentage = random.randrange(1, 15)

        # random packet delay - between 1s - 30s
        delay_in_secs = random.randrange(1, 30)

        list_of_tc_options = [("NetworkRandomInterruption_{}%-loss".format(loss_percentage), "--loss {}%".format(loss_percentage)),
                              ("NetworkRandomInterruption_{}%-corrupt".format(corrupt_percentage),
                               "--corrupt {}%".format(corrupt_percentage)),
                              ("NetworkRandomInterruption_{}sec-delay".format(delay_in_secs),
                               "--delay {}s --delay-distro 500ms".format(delay_in_secs)),
                              ("NetworkRandomInterruption_{}-limit".format(rate_limit), "--rate {}".format(rate_limit))]

        list_of_timeout_options = [10, 60, 120, 300, 500]
        option_name, selected_option = random.choice(list_of_tc_options)
        wait_time = random.choice(list_of_timeout_options)

        self._set_current_disruption(option_name)
        self.log.debug("NetworkRandomInterruption: [%s] for %dsec", selected_option, wait_time)
        self.target_node.traffic_control(None)
        try:
            self.target_node.traffic_control(selected_option)
            time.sleep(wait_time)
        finally:
            self.target_node.traffic_control(None)

    def disrupt_network_block(self):
        self._set_current_disruption('BlockNetwork')
        if not self.cluster.aws_extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `aws_extra_network_interface: True`")

        selected_option = "--loss 100%"
        list_of_timeout_options = [10, 60, 120, 300, 500]
        wait_time = random.choice(list_of_timeout_options)
        self.log.debug("BlockNetwork: [%s] for %dsec", selected_option, wait_time)
        self.target_node.traffic_control(None)
        try:
            self.target_node.traffic_control(selected_option)
            time.sleep(wait_time)
        finally:
            self.target_node.traffic_control(None)

    def disrupt_network_start_stop_interface(self):  # pylint: disable=invalid-name
        self._set_current_disruption('StopStartNetworkInterfaces')
        if not self.cluster.aws_extra_network_interface:
            raise UnsupportedNemesis("for this nemesis to work, you need to set `aws_extra_network_interface: True`")

        list_of_timeout_options = [10, 60, 120, 300, 500]
        wait_time = random.choice(list_of_timeout_options)
        self.log.debug("Taking down eth1 for %dsec", wait_time)

        try:
            self.target_node.remoter.run("sudo /sbin/ifdown eth1")
            time.sleep(wait_time)
        finally:
            self.target_node.remoter.run("sudo /sbin/ifup eth1")


class NotSpotNemesis(Nemesis):
    def set_target_node(self):

        if isinstance(self.cluster, ScyllaAWSCluster):
            non_seed_nodes = [node for node in self.cluster.nodes
                              if not node.is_seed and
                              not node.is_spot and
                              not node.running_nemesis]
            if non_seed_nodes:
                self.target_node = random.choice(non_seed_nodes)

        else:
            super(NotSpotNemesis, self).set_target_node()

        self.log.info('Current Target: %s', self.target_node)

    def disrupt(self):
        raise NotImplementedError()


def log_time_elapsed_and_status(method):
    """
    Log time elapsed for method to run

    :param method: Remote method to wrap.
    :return: Wrapped method.
    """

    def wrapper(*args, **kwargs):
        # pylint: disable=too-many-locals
        args[0].cluster.check_cluster_health()
        num_nodes_before = len(args[0].cluster.nodes)
        start_time = time.time()
        args[0].log.debug('Start disruption at `%s`', datetime.datetime.fromtimestamp(start_time))
        class_name = args[0].get_class_name()
        if class_name.find('Chaos') < 0:
            args[0].metrics_srv.event_start(class_name)
        result = None
        status = True
        log_info = {
            'operation': args[0].current_disruption,
            'start': int(start_time),
            'end': 0,
            'duration': 0,
            'node': str(args[0].target_node),
            'type': 'end',
        }
        try:
            result = method(*args, **kwargs)
        except UnsupportedNemesis as exp:
            log_info.update({'type': 'skipped', 'skip_reason': str(exp)})
            logging.info("skipping %s", args[0].get_disrupt_name())
            logging.info("cause of: %s", str(exp))
            raise

        except Exception as details:  # pylint: disable=broad-except
            details = str(details)
            args[0].error_list.append(details)
            args[0].log.error('Unhandled exception in method %s', method, exc_info=True)
            log_info.update({'error': details, 'full_traceback': traceback.format_exc()})
            status = False
        finally:
            end_time = time.time()
            time_elapsed = int(end_time - start_time)
            log_info.update({
                'end': int(end_time),
                'duration': time_elapsed,
            })
            args[0].duration_list.append(time_elapsed)
            args[0].operation_log.append(log_info)
            args[0].log.debug('%s duration -> %s s', args[0].current_disruption, time_elapsed)

            if class_name.find('Chaos') < 0:
                args[0].metrics_srv.event_stop(class_name)
            disrupt = args[0].get_disrupt_name()
            del log_info['operation']

            args[0].update_stats(disrupt, status, log_info)
            DisruptionEvent(name=disrupt, status=status, **log_info)
            args[0].cluster.check_cluster_health()
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

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_stop_wait_start_scylla_server(600)


class StopStartMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_stop_start_scylla_server()


class RestartThenRepairNodeMonkey(NotSpotNemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_restart_then_repair_node()


class MultipleHardRebootNodeMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_multiple_hard_reboot_node()


class HardRebootNodeMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_hard_reboot_node()


class SoftRebootNodeMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_soft_reboot_node()


class DrainerMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_drain()


class CorruptThenRepairMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_destroy_data_then_repair()


class CorruptThenRebuildMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_destroy_data_then_rebuild()


class DecommissionMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_decommission()


class NoCorruptRepairMonkey(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_no_corrupt_repair()


class MajorCompactionMonkey(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_major_compaction()


class RefreshMonkey(Nemesis):

    disruptive = False
    run_with_gemini = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_refresh()


class RefreshBigMonkey(Nemesis):

    disruptive = False
    run_with_gemini = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_refresh(big_sstable=True, skip_download=True)


class EnospcMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_enospc()


class EnospcAllNodesMonkey(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_enospc(all_nodes=True)


class NodeToolCleanupMonkey(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_nodetool_cleanup()


class TruncateMonkey(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_truncate()


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
        #  - HardRebootMonkey
        #  - SoftRebootMonkey
        #  - TruncateMonkey
        #  - ToppartitionsMonkey
        self.call_random_disrupt_method(disrupt_methods=['disrupt_nodetool_cleanup', 'disrupt_nodetool_decommission',
                                                         'disrupt_nodetool_drain', 'disrupt_nodetool_refresh',
                                                         'disrupt_stop_start_scylla_server', 'disrupt_major_compaction',
                                                         'disrupt_modify_table', 'disrupt_nodetool_enospc',
                                                         'disrupt_stop_wait_start_scylla_server',
                                                         'disrupt_hard_reboot_node', 'disrupt_soft_reboot_node',
                                                         'disrupt_truncate', 'disrupt_show_toppartitions'])


class ScyllaCloudLimitedChaosMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        # Limit the nemesis scope to only one relevant to scylla cloud, where we defined we don't have AWS api access:
        self.call_random_disrupt_method(disrupt_methods=['disrupt_nodetool_cleanup',
                                                         'disrupt_nodetool_drain', 'disrupt_nodetool_refresh',
                                                         'disrupt_stop_start_scylla_server', 'disrupt_major_compaction',
                                                         'disrupt_modify_table', 'disrupt_nodetool_enospc',
                                                         'disrupt_stop_wait_start_scylla_server',
                                                         'disrupt_soft_reboot_node',
                                                         'disrupt_truncate'])


class AllMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(predefined_sequence=True)


class MdcChaosMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(
            disrupt_methods=['disrupt_destroy_data_then_repair', 'disrupt_no_corrupt_repair', 'disrupt_nodetool_decommission'])


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
    #         node.run_nodetool("snapshot")
    #         # update *development* packages
    #         node.remoter.run('sudo rpm -UvhR --oldpackage /tmp/scylla/*development*', ignore_status=True)
    #         # and all the rest
    #         node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/*.rpm | true')
    #         node.remoter.run('rpm -qa scylla\*')
    #     elif repo_file:
    #         scylla_repo = get_data_dir_path(repo_file)
    #         node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
    #         node.remoter.run('sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup')
    #         node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
    #         # backup the data
    #         node.remoter.run('sudo cp /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml-backup')
    #         node.run_nodetool("snapshot")
    #         node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
    #         node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
    #         node.remoter.run('sudo yum clean all')
    #         ver_suffix = '-{}'.format(new_version) if new_version else ''
    #         node.remoter.run('sudo yum install scylla{0} scylla-server{0} scylla-jmx{0} scylla-tools{0}'
    #                          ' scylla-conf{0} scylla-kernel-conf{0} scylla-debuginfo{0} -y'.format(ver_suffix))
    #     # flush all memtables to SSTables
    #     node.run_nodetool("drain")
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
        indexes = list(range(nodes_num))
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.upgrade_node(node)  # pylint: disable=no-member

        self.log.info('Upgrade Nemesis end')


class UpgradeNemesisOneNode(UpgradeNemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.log.info('UpgradeNemesisOneNode begin')
        self.upgrade_node(self.cluster.node_to_upgrade)  # pylint: disable=no-member

        self.log.info('UpgradeNemesisOneNode end')


class RollbackNemesis(Nemesis):

    # rollback a single node
    def rollback_node(self, node):
        self.log.info('Rollbacking a Node')
        orig_ver = node.remoter.run('rpm -qa scylla-server')
        node.remoter.run('sudo cp ~/scylla.repo-backup /etc/yum.repos.d/scylla.repo')
        # backup the data
        node.run_nodetool("snapshot")
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo yum clean all')
        node.remoter.run(
            'sudo yum downgrade scylla scylla-server scylla-jmx scylla-tools scylla-conf scylla-kernel-conf scylla-debuginfo -y')
        # flush all memtables to SSTables
        node.run_nodetool("drain")
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
        indexes = list(range(nodes_num))
        # shuffle it so we will rollback the nodes in a
        # random order
        random.shuffle(indexes)

        # rollback all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.rollback_node(node)

        self.log.info('Rollback Nemesis end')


class ModifyTableMonkey(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_modify_table()


class MgmtRepair(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.log.info('disrupt_mgmt_repair_cli Nemesis begin')
        self.disrupt_mgmt_repair_cli()
        self.log.info('disrupt_mgmt_repair_cli Nemesis end')
        # For Manager APIs test, use: self.disrupt_mgmt_repair_api()


class AbortRepairMonkey(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_abort_repair()


class NodeTerminateAndReplace(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_terminate_and_replace_node()


class ValidateHintedHandoffShortDowntime(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_validate_hh_short_downtime()


class SnapshotOperations(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_snapshot_operations()


class NodeRestartWithResharding(Nemesis):

    disruptive = True

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_restart_with_resharding()


class TopPartitions(Nemesis):

    disruptive = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_show_toppartitions()


class RandomInterruptionNetworkMonkey(Nemesis):
    disruptive = False
    networking = True
    run_with_gemini = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_network_random_interruptions()


class BlockNetworkMonkey(Nemesis):
    disruptive = False
    networking = True
    run_with_gemini = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_network_block()


class StopStartInterfacesNetworkMonkey(Nemesis):
    disruptive = False
    networking = True
    run_with_gemini = False

    @log_time_elapsed_and_status
    def disrupt(self):
        self.disrupt_network_start_stop_interface()


class DisruptiveMonkey(Nemesis):
    # Limit the nemesis scope:
        #  - ValidateHintedHandoffShortDowntime
        #  - CorruptThenRepairMonkey
        #  - CorruptThenRebuildMonkey
        #  - RestartThenRepairNodeMonkey
        #  - StopStartMonkey
        #  - MultipleHardRebootNodeMonkey
        #  - HardRebootNodeMonkey
        #  - SoftRebootNodeMonkey
        #  - StopWaitStartMonkey
        #  - NodeTerminateAndReplace
        #  - EnospcMonkey
        #  - DecommissionMonkey
        #  - NodeRestartWithResharding
        #  - DrainerMonkey

    def __init__(self, *args, **kwargs):
        super(DisruptiveMonkey, self).__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_disrupt_methods_for_nemesis_subclasses(disruptive=True)

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class NonDisruptiveMonkey(Nemesis):
        # Limit the nemesis scope:
        #  - NodeToolCleanupMonkey
        #  - SnapshotOperations
        #  - RefreshMonkey
        #  - RefreshBigMonkey -
        #  - NoCorruptRepairMonkey
        #  - MgmtRepair
        #  - AbortRepairMonkey

    def __init__(self, *args, **kwargs):
        super(NonDisruptiveMonkey, self).__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_disrupt_methods_for_nemesis_subclasses(disruptive=False)

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class NetworkMonkey(Nemesis):
    # Limit the nemesis scope:
    #  - RandomInterruptionNetworkMonkey
    #  - StopStartInterfacesNetworkMonkey
    #  - BlockNetworkMonkey
    def __init__(self, *args, **kwargs):
        super(NetworkMonkey, self).__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_disrupt_methods_for_nemesis_subclasses(networking=True)

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class GeminiChaosMonkey(Nemesis):
    # Limit the nemesis scope to use with gemini
        # - StopStartMonkey
        # - RestartThenRepairNodeMonkey
    def __init__(self, *args, **kwargs):
        super(GeminiChaosMonkey, self).__init__(*args, **kwargs)
        self.disrupt_methods_list = self.get_list_of_disrupt_methods_for_nemesis_subclasses(run_with_gemini=True)

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


class GeminiNonDisruptiveChaosMonkey(Nemesis):
    def __init__(self, *args, **kwargs):
        super(GeminiNonDisruptiveChaosMonkey, self).__init__(*args, **kwargs)
        run_with_gemini = set(self.get_list_of_disrupt_methods_for_nemesis_subclasses(run_with_gemini=True))
        non_disruptive = set(self.get_list_of_disrupt_methods_for_nemesis_subclasses(disruptive=False))
        self.disrupt_methods_list = run_with_gemini.intersection(non_disruptive)

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method(disrupt_methods=self.disrupt_methods_list)


RELATIVE_NEMESIS_SUBCLASS_LIST = [NotSpotNemesis]

DEPRECATED_LIST_OF_NEMESISES = [UpgradeNemesis, UpgradeNemesisOneNode, RollbackNemesis]

COMPLEX_NEMESIS = [NoOpMonkey, ChaosMonkey,
                   LimitedChaosMonkey,
                   ScyllaCloudLimitedChaosMonkey,
                   AllMonkey, MdcChaosMonkey,
                   DisruptiveMonkey, NonDisruptiveMonkey, GeminiNonDisruptiveChaosMonkey,
                   GeminiChaosMonkey, NetworkMonkey]
