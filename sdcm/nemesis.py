"""
Classes that introduce disruption in clusters.
"""

import inspect
import random
import time

from .data_path import get_data_path


class Nemesis(object):

    def __init__(self, cluster, termination_event):
        self.cluster = cluster
        self.target_node = None
        self.set_target_node()
        self.termination_event = termination_event

    def set_target_node(self):
        non_seed_nodes = [node for node in self.cluster.nodes if not node.is_seed]
        self.target_node = random.choice(non_seed_nodes)
        print('{}: Current Target: {}'.format(self, self.target_node))

    def run(self, interval=30, termination_event=None):
        interval *= 60
        while True:
            time.sleep(interval)
            self.disrupt()
            if termination_event is not None:
                if self.termination_event.isSet():
                    self.termination_event = None
                    break
            self.set_target_node()

    def __str__(self):
        return str(self.__class__)

    def disrupt(self):
        raise NotImplementedError('Derived classes must implement disrupt()')

    def disrupt_nodetool_drain(self):
        print('{}: Drain {} then restart it'.format(self, self.target_node))
        self.target_node.remoter.run('nodetool -h localhost drain')
        self.target_node.restart()

    def disrupt_nodetool_decommission(self):
        print('{}: Decomission {}'.format(self, self.target_node))
        target_node_ip = self.target_node.instance.private_ip_address
        result = self.target_node.remoter.run('nodetool --host localhost '
                                              'decommission')
        print('{}: {} took {} s to finish'.format(self, result.command,
                                                  result.duration))
        verification_node = random.choice(self.cluster.nodes)
        while verification_node == self.target_node:
            verification_node = random.choice(self.cluster.nodes)

        node_info_list = self.cluster.get_node_info_list(verification_node)
        private_ips = [node_info['ip'] for node_info in node_info_list]
        error_msg = ('Node that was decommissioned {} still in the cluster. '
                     'Cluster status info: {}'.format(self.target_node,
                                                      node_info_list))
        assert target_node_ip not in private_ips, error_msg
        self.cluster.nodes.remove(self.target_node)
        self.target_node.destroy()
        # Replace the node that was terminated.
        new_nodes = self.cluster.add_nodes(count=1)
        self.cluster.wait_for_init(node_list=new_nodes)

    def disrupt_stop_start(self):
        print('{}: Stop {} then restart it'.format(self, self.target_node))
        self.target_node.restart()

    def disrupt_kill_scylla_daemon(self):
        print('{}: Kill all scylla processes in {}'.format(self,
                                                           self.target_node))
        kill_cmd = "sudo pkill -9 scylla"
        self.target_node.remoter.run(kill_cmd, ignore_status=True)

        self.target_node.wait_db_down()
        # Let's wait for the target Node to have their services re-started
        self.target_node.wait_db_up()

    def _destroy_data(self):
        # Send the script used to corrupt the DB
        break_scylla = get_data_path('break_scylla.sh')
        self.target_node.remoter.send_files(break_scylla,
                                            "/tmp/break_scylla.sh")

        # corrupt the DB
        self.target_node.remoter.run('chmod +x /tmp/break_scylla.sh')
        self.target_node.remoter.run('/tmp/break_scylla.sh')

        # lennart's systemd will restart scylla let him a bit of time
        self.disrupt_kill_scylla_daemon()

    def disrupt_destroy_data_then_repair(self):
        print('{}: Destroy user data in {}, then run nodetool '
              'repair'.format(self, self.target_node))
        self._destroy_data()
        # try to save the node
        self.repair_nodetool_repair()

    def disrupt_destroy_data_then_rebuild(self):
        print('{}: Destroy user data in {}, then run nodetool '
              'rebuild'.format(self, self.target_node))
        self._destroy_data()
        # try to save the node
        self.repair_nodetool_rebuild()

    def call_random_disrupt_method(self):
        disrupt_methods = [attr[1] for attr in inspect.getmembers(self) if
                           attr[0].startswith('disrupt_') and
                           callable(attr[1])]
        disrupt_method = random.choice(disrupt_methods)
        try:
            disrupt_method()
        except Exception, details:
            print('Disrupt method {} failed: {}'.format(disrupt_method,
                                                        details))

    def repair_nodetool_repair(self):
        result = self.target_node.remoter.run('nodetool -h localhost repair')
        print('{}: {} duration -> {} s'.format(self, result.command,
                                               result.duration))

    def repair_nodetool_rebuild(self):
        for node in self.cluster.nodes:
            result = node.remoter.run('nodetool -h localhost rebuild')
            print('{}: {} duration -> {} s'.format(self, result.command,
                                                   result.duration))


def log_time_elapsed(method):
    """
    Log time elapsed for method to run

    :param method: Remote method to wrap.
    :return: Wrapped method.
    """
    def wrapper(*args, **kwargs):
        print('{}: {} start'.format(args[0], method))
        start_time = time.time()
        try:
            result = method(*args, **kwargs)
        finally:
            elapsed_time = int(time.time() - start_time)
            print('{}: {} duration -> {} s'.format(args[0], method,
                                                   elapsed_time))
        return result
    return wrapper


class StopStartMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.disrupt_stop_start()


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
        self.disrupt_nodetool_decommission()


class ChaosMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self.call_random_disrupt_method()
