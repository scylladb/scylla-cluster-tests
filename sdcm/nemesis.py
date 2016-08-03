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

from avocado.utils import process

from .data_path import get_data_path
from .log import SDCMAdapter


class Nemesis(object):

    def __init__(self, cluster, termination_event):
        self.cluster = cluster
        self.target_node = None
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.set_target_node()
        result = self.target_node.remoter.run('rpm -qa | grep scylla | sort', verbose=False,
                                              ignore_status=True)
        self.db_software_version = 'not available'
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
        self.log.info('Unhandled exceptions: %s', len(self.error_list))
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
            self.log.error("nodetool command '%s' failed on node %s: %s",
                           cmd, self.target_node, details.result)
            return None
        except Exception:
            self.log.error('Unexpected exception running nodetool',
                           exc_info=True)
            return None

    def _kill_scylla_daemon(self):
        self.log.info('Kill all scylla processes in %s', self.target_node)
        kill_cmd = "sudo pkill -9 scylla"
        self.target_node.remoter.run(kill_cmd, ignore_status=True)

        # TODO: Due to scylla-server.service changing behavior
        # now we don't wait the DB to be down
        # self.target_node.wait_db_down()

        # TODO: Remove scylla-server restart upon systemd service is fixed
        # https://github.com/scylladb/scylla/issues/904
        restart_cmd = 'sudo systemctl restart scylla-server.service'
        self.target_node.remoter.run(restart_cmd)

        # Let's wait for the target Node to have their services re-started
        self.target_node.wait_db_up()

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
        self.target_node.remoter.run('/tmp/break_scylla.sh')

        self._kill_scylla_daemon()

    def disrupt(self):
        raise NotImplementedError('Derived classes must implement disrupt()')

    def _set_current_disruption(self, label):
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
        if result is not None:
            self.target_node.restart()

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
                # Replace the node that was terminated.
                if add_node:
                    new_nodes = self.cluster.add_nodes(count=1)
                    self.cluster.wait_for_init(node_list=new_nodes)

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


def log_time_elapsed(method):
    """
    Log time elapsed for method to run

    :param method: Remote method to wrap.
    :return: Wrapped method.
    """
    def wrapper(*args, **kwargs):
        args[0].log.debug('Start -> %s', args[0].current_disruption)
        start_time = time.time()
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
            return result
    return wrapper


class NoOpMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        time.sleep(300)


class StopWaitStartMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_stop_wait_start_scylla_server(600)


class StopStartMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_stop_start_scylla_server()


class DrainerMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_nodetool_drain()


class CorruptThenRepairMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_destroy_data_then_repair()


class CorruptThenRebuildMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_destroy_data_then_rebuild()


class DecommissionMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_nodetool_decommission(add_node=True)


class ChaosMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.call_random_disrupt_method()


class UpgradeNemesis(Nemesis):

    # upgrade a single node
    def upgrade_node(self, node):
        self.log.info('Upgrading a Node')
        scylla_repo = get_data_path('scylla.repo.upgrade')
        node.remoter.send_files(scylla_repo, '/tmp/scylla.repo', verbose=True)
        node.remoter.run('sudo cp /tmp/scylla.repo /etc/yum.repos.d/scylla.repo')
        # backup the data
        node.remoter.run('sudo nodetool snapshot')
        node.remoter.run('sudo chown root.root /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo chmod 644 /etc/yum.repos.d/scylla.repo')
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update scylla scylla-server scylla-jmx scylla-tools scylla-conf scylla-kernel-conf -y')
        # flush all memtables to SSTables
        node.remoter.run('sudo nodetool drain')
        node.remoter.run('sudo systemctl restart scylla-server.service')
        node.wait_db_up(verbose=True)

    @log_time_elapsed
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
