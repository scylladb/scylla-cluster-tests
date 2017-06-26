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

from avocado.utils import process

from .data_path import get_data_path
from .log import SDCMAdapter

from sdcm.utils import remote_get_file


class Nemesis(object):

    def __init__(self, cluster, loaders, monitoring_set, termination_event):
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
        self.target_node.remoter.run('sudo systemctl stop scylla-server.service')
        self.target_node.wait_db_down()
        self.log.info("Sleep for %s seconds", sleep_time)
        time.sleep(sleep_time)
        self.target_node.remoter.run('sudo systemctl start scylla-server.service')
        self.target_node.wait_db_up()

    def disrupt_stop_start_scylla_server(self):
        self._set_current_disruption('StopStartService %s' % self.target_node)
        self.target_node.remoter.run('sudo systemctl stop scylla-server.service')
        self.target_node.wait_db_down()
        self.target_node.remoter.run('sudo systemctl start scylla-server.service')
        self.target_node.wait_db_up()

    def _destroy_data(self):
        # Send the script used to corrupt the DB
        break_scylla = get_data_path('break_scylla.sh')
        self.target_node.remoter.send_files(break_scylla,
                                            "/tmp/break_scylla.sh")

        # corrupt the DB
        self.target_node.remoter.run('chmod +x /tmp/break_scylla.sh')
        self.target_node.remoter.run('sudo /tmp/break_scylla.sh')

        self._kill_scylla_daemon()

    def disrupt(self):
        raise NotImplementedError('Derived classes must implement disrupt()')

    def _set_current_disruption(self, label):
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
            self.target_node.remoter.run('sudo systemctl stop scylla-server.service')
            self.target_node.wait_db_down()
            self.target_node.remoter.run('sudo systemctl start scylla-server.service')
            self.target_node.wait_db_up()

    def reconfigure_monitoring(self):
        for monitoring_node in self.monitoring_set.nodes:
            self.log.info('Monitoring node: %s', str(monitoring_node))
            targets = [n.private_ip_address for n in self.cluster.nodes]
            targets += [n.private_ip_address for n in self.loaders.nodes]
            monitoring_node.reconfigure_prometheus(targets=targets)

    def disrupt_nodetool_decommission(self, add_node=True):
        self._set_current_disruption('Decommission %s' % self.target_node)
        target_node_ip = self.target_node.private_ip_address
        decommission_cmd = 'nodetool --host localhost decommission'
        result = self._run_nodetool(decommission_cmd, self.target_node)
        if result is not None:
            verification_node = random.choice(self.cluster.nodes)
            while verification_node == self.target_node:
                verification_node = random.choice(self.cluster.nodes)

            node_info_list = self.cluster.get_node_info_list(verification_node)
            private_ips = [node_info['ip'] for node_info in node_info_list]
            error_msg = ('Node that was decommissioned %s still in the cluster. '
                         'Cluster status info: %s' % (self.target_node,
                                                      node_info_list))
            if target_node_ip in private_ips:
                self.log.info('Decommission %s FAIL', self.target_node)
                self.log.error(error_msg)
            else:
                self.log.info('Decommission %s PASS', self.target_node)
                self.cluster.nodes.remove(self.target_node)
                self.target_node.destroy()
                self.reconfigure_monitoring()
                # Replace the node that was terminated.
                if add_node:
                    new_nodes = self.cluster.add_nodes(count=1)
                    self.cluster.wait_for_init(node_list=new_nodes)
                    self.reconfigure_monitoring()

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
            # 3.5K (10 rows)
            sstable_url = 'https://s3.amazonaws.com/scylla-qa-team/keyspace1.standard1.small.tar.gz'
            sstable_file = "/tmp/keyspace1.standard1.small.tar.gz"
            sstable_md5 = '76cca3135e175d859c0efb67c6a7b233'
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

    def _deprecated_disrupt_stop_start(self):
        # TODO: We don't support fully stopping the AMI instance anymore
        # TODO: This nemesis has to be rewritten to just stop/start scylla server
        self.log.info('StopStart %s', self.target_node)
        self.target_node.restart()

    def call_random_disrupt_method(self):
        disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                           attr[0].startswith('disrupt_') and
                           callable(attr[1])]
        disrupt_method = random.choice(disrupt_methods)
        disrupt_method()

    def repair_nodetool_repair(self):
        repair_cmd = 'nodetool -h localhost repair'
        self._run_nodetool(repair_cmd, self.target_node)

    def repair_nodetool_rebuild(self):
        rebuild_cmd = 'nodetool -h localhost rebuild'
        self._run_nodetool(rebuild_cmd, self.target_node)


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
        result = None
        try:
            result = method(*args, **kwargs)
        except Exception, details:
            args[0].error_list.append(details)
            args[0].log.error('Unhandled exception in method %s', method, exc_info=True)
        finally:
            end_time = time.time()
            time_elapsed = int(end_time - start_time)
            args[0].duration_list.append(time_elapsed)
            args[0].operation_log.append({'operation': args[0].current_disruption,
                                          'start': int(start_time),
                                          'end': int(end_time), 'duration': time_elapsed})
            args[0].log.debug('%s duration -> %s s', args[0].current_disruption, time_elapsed)
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
    def disrupt(self):
        self.disrupt_nodetool_decommission(add_node=True)


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


class ChaosMonkey(Nemesis):

    @log_time_elapsed_and_status
    def disrupt(self):
        self.call_random_disrupt_method()


class UpgradeNemesis(Nemesis):

    # upgrade a single node
    def upgrade_node(self, node):
        self.log.info('Upgrading a Node')
        orig_ver = node.remoter.run('rpm -qa scylla-server')
        scylla_repo = get_data_path('scylla.repo.upgrade')
        node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
        node.remoter.run('sudo cp /etc/yum.repos.d/scylla.repo ~/scylla.repo-backup')
        node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
        # backup the data
        node.remoter.run('sudo cp /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml-backup')
        node.remoter.run('sudo nodetool snapshot')
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update scylla scylla-server scylla-jmx scylla-tools scylla-conf scylla-kernel-conf -y')
        # flush all memtables to SSTables
        node.remoter.run('sudo nodetool drain')
        node.remoter.run('sudo systemctl restart scylla-server.service')
        node.wait_db_up(verbose=True)
        new_ver = node.remoter.run('rpm -qa scylla-server')
        if orig_ver == new_ver:
            self.log.error('scylla-server version isn\'t changed')

    @log_time_elapsed_and_status
    def disrupt(self):
        self.log.info('Upgrade Nemesis begin')
        # get the number of nodes
        l = len(self.cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(l)]
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.upgrade_node(node)

        self.log.info('Upgrade Nemesis end')


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
        node.remoter.run('sudo yum downgrade scylla scylla-server scylla-jmx scylla-tools scylla-conf scylla-kernel-conf -y')
        # flush all memtables to SSTables
        node.remoter.run('nodetool drain')
        node.remoter.run('sudo cp /etc/scylla/scylla.yaml-backup /etc/scylla/scylla.yaml')
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
        l = len(self.cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(l)]
        # shuffle it so we will rollback the nodes in a
        # random order
        random.shuffle(indexes)

        # rollback all the nodes in random order
        for i in indexes:
            node = self.cluster.nodes[i]
            self.rollback_node(node)

        self.log.info('Rollback Nemesis end')
