"""
Classes that introduce disruption in clusters.
"""

import random
import time


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
            self.break_it()
            if termination_event is not None:
                if self.termination_event.isSet():
                    self.termination_event = None
                    break
            self.set_node_to_operate()

    def break_it(self):
        return NotImplementedError('Derived Nemesis classes must '
                                   'implement the method break_it')

    def kill_scylla_daemon(self):
        self.node_to_operate.remoter.run('sudo killall -9 scylla')


class ChaosMonkey(Nemesis):

    def break_it(self):
        self.node_to_operate.instance.stop()
        time.sleep(60)
        self.node_to_operate.instance.start()

class DrainerMonkey(Nemesis):

    def run(self, interval=30, termination_event=None):
        interval *= 60
        time.sleep(interval)
        self.break_it()

    def break_it(self):
        self.node_to_operate.remoter.run('nodetool -h localhost drain')
        self.node_to_operate.instance.stop()
        time.sleep(60)
        self.node_to_operate.instance.start()

class CorruptorMonkey(Nemesis):

    def run(self, interval=30, termination_event=None):
        interval *= 60
        time.sleep(interval)
        self.break_it()

    def break_it(self):
        # Send the script used to corrupt the DB
        break_scylla = self.get_data_path('break_scylla.sh')
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
        self.node_to_operate.remoter.run('nodetool -h localhost repair')

class RebuildMonkey(CorruptorMonkey):

    def repair(self):
        for node in self.cluster.nodes:
            node.remoter.run_parallel('nodetool -h localhost rebuild')

