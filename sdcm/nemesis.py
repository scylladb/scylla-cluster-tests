"""
Classes that introduce disruption in clusters.
"""

import random
import time

from .data_path import get_data_path

NODETOOL_CMD_TIMEOUT = 240


class Nemesis(object):

    def __init__(self, cluster, termination_event):
        self.cluster = cluster
        self.node_to_operate = None
        self.set_node_to_operate()
        self.termination_event = termination_event

    def set_node_to_operate(self):
        self.node_to_operate = random.choice(self.cluster.nodes)
        print('Node to operate: {}'.format(self.node_to_operate))

    def run(self, interval=30, termination_event=None):
        interval *= 60
        while True:
            time.sleep(interval)
            self.disrupt()
            if termination_event is not None:
                if self.termination_event.isSet():
                    self.termination_event = None
                    break
            self.set_node_to_operate()

    def disrupt(self):
        return NotImplementedError('Derived Nemesis classes must '
                                   'implement the method disrupt')

    def kill_scylla_daemon(self):
        self.node_to_operate.remoter.run('sudo killall -9 scylla')


class ChaosMonkey(Nemesis):

    def disrupt(self):
        self.node_to_operate.instance.stop()
        time.sleep(60)
        self.node_to_operate.instance.start()


class DrainerMonkey(Nemesis):

    def run(self, interval=30, termination_event=None):
        interval *= 60
        time.sleep(interval)
        self.disrupt()

    def disrupt(self):
        self.node_to_operate.remoter.run('nodetool -h localhost drain',
                                         timeout=NODETOOL_CMD_TIMEOUT)
        self.node_to_operate.instance.stop()
        time.sleep(60)
        self.node_to_operate.instance.start()
        self.node_to_operate.wait_for_init()


class CorruptorMonkey(Nemesis):

    def run(self, interval=30, termination_event=None):
        interval *= 60
        time.sleep(interval)
        self.disrupt()

    def disrupt(self):
        # Send the script used to corrupt the DB
        break_scylla = get_data_path('break_scylla.sh')
        self.node_to_operate.remoter.send_files(break_scylla,
                                                "/tmp/break_scylla.sh")

        # corrupt the DB
        self.node_to_operate.remoter.run('chmod +x /tmp/break_scylla.sh')
        self.node_to_operate.remoter.run('/tmp/break_scylla.sh')

        # lennart's systemd will restart scylla let him a bit of time
        self.kill_scylla_daemon()
        time.sleep(60)

        # try to save the node
        self.repair()
        time.sleep(60)

    def repair(self):
        return NotImplementedError('Derived CorruptorMonkey classes must '
                                   'implement the method repair')


class RepairMonkey(CorruptorMonkey):

    def repair(self):
        self.node_to_operate.remoter.run('nodetool -h localhost repair',
                                         timeout=NODETOOL_CMD_TIMEOUT)


class RebuildMonkey(CorruptorMonkey):

    def repair(self):
        for node in self.cluster.nodes:
            node.remoter.run_parallel('nodetool -h localhost rebuild',
                                      timeout=NODETOOL_CMD_TIMEOUT)


class DecommissionMonkey(Nemesis):

    def disrupt(self):
        node_to_operate_ip = self.node_to_operate.instance.private_ip_address
        self.node_to_operate.remoter.run('nodetool --host localhost '
                                         'decommission',
                                         timeout=NODETOOL_CMD_TIMEOUT)
        verification_node = random.choice(self.cluster.nodes)
        while verification_node == self.node_to_operate:
            verification_node = random.choice(self.cluster.nodes)

        node_info_list = self.cluster.get_node_info_list(verification_node)
        private_ips = [node_info['ip'] for node_info in node_info_list]
        error_msg = ('Node that was decommissioned {} still in the cluster. '
                     'Cluster status info: {}'.format(self.node_to_operate,
                                                      node_info_list))
        assert node_to_operate_ip not in private_ips, error_msg
        self.cluster.nodes.remove(self.node_to_operate)
        self.node_to_operate.instance.terminate()
