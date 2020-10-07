import queue
import os

from sdcm.services.base import NodeThreadService


class DecodeBacktraceThread(NodeThreadService):
    _interval = 0

    def __init__(self, node, decoding_queue: queue.Queue):
        self._decoding_queue = decoding_queue
        super().__init__(node)

    def _service_body(self):
        scylla_debug_file = None
        event = None
        try:
            obj = self._decoding_queue.get(timeout=5)
            if obj is None:
                # self._decoding_queue.task_done()
                return
            event = obj["event"]
            if not scylla_debug_file:
                scylla_debug_file = self.copy_scylla_debug_info(obj["node"], obj["debug_file"])
            output = self.decode_raw_backtrace(scylla_debug_file, " ".join(event.raw_backtrace.split('\n')))
            event.add_backtrace_info(backtrace=output.stdout)
            self._decoding_queue.task_done()
        except queue.Empty:
            pass
        finally:
            if event:
                event.publish()

    def _service_can_iterate(self, delay):
        if not self._decoding_queue.empty():
            return False
        return self._termination_event.wait(delay)

    def copy_scylla_debug_info(self, node, debug_file):
        """Copy scylla debug file from db-node to monitor-node

        Copy via builder
        :param node: db node
        :type node: BaseNode
        :param debug_file: path to scylla_debug_file on db-node
        :type debug_file: str
        :returns: path on monitor node
        :rtype: {str}
        """
        base_scylla_debug_file = os.path.basename(debug_file)
        transit_scylla_debug_file = os.path.join(node.parent_cluster.logdir, base_scylla_debug_file)
        final_scylla_debug_file = os.path.join("/tmp", base_scylla_debug_file)
        if not os.path.exists(transit_scylla_debug_file):
            node.remoter.receive_files(debug_file, transit_scylla_debug_file)
        res = self._node.remoter.run(
            "test -f {}".format(final_scylla_debug_file), ignore_status=True, verbose=False)
        if res.exited != 0:
            self._node.remoter.send_files(  # pylint: disable=not-callable
                transit_scylla_debug_file,
                final_scylla_debug_file)
        self._log.info(f"File on the node: {final_scylla_debug_file}")
        self._log.info(f"Remove transit file: {transit_scylla_debug_file}")
        os.remove(transit_scylla_debug_file)
        return final_scylla_debug_file

    def decode_raw_backtrace(self, scylla_debug_file, raw_backtrace):
        """run decode backtrace on monitor node

        Decode backtrace on monitor node
        :param scylla_debug_file: file path on db-node
        :type scylla_debug_file: str
        :param raw_backtrace: string with backtrace data
        :type raw_backtrace: str
        :returns: result of bactrace
        :rtype: {str}
        """
        return self._node.remoter.run(f'addr2line -Cpife {scylla_debug_file} {raw_backtrace}', verbose=True)
