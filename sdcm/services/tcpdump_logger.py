import os

from sdcm.services.base import NodeThreadService
from sdcm.utils.decorators import retrying


class TcpdumpLogger(NodeThreadService):
    def __init__(self, node, tcpdump_id: str, port: int = 10000):
        self._tcpdump_id = tcpdump_id
        pcap_name = f'tcpdump-{self._tcpdump_id}.pcap'
        self._pcap_remote_file_path = os.path.join('/tmp', pcap_name)
        self._pcap_local_file_path = os.path.join(node.logdir, pcap_name)
        self._port = port
        super().__init__(node)

    def _service_body(self):
        try:
            self._node.remoter.run(
                f'sudo tcpdump -vv -i lo port {self._port} -w {self._pcap_remote_file_path} > /dev/null 2>&1',
                ignore_status=True)
            self._node.remoter.receive_files(
                src=self._pcap_remote_file_path,
                dst=self._pcap_local_file_path)  # pylint: disable=not-callable
        except Exception as details:  # pylint: disable=broad-except
            self._log.error(f'Error running tcpdump on lo, tcp port {self._port}: {str(details)}')

    def stop(self):
        super().stop()
        self._kill_tcpdump()

    @retrying(n=5)
    def _kill_tcpdump(self):
        self._node.remoter.run('sudo killall tcpdump', ignore_status=True)
