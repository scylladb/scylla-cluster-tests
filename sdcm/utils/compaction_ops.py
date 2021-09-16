import time
from typing import Callable, Optional, NamedTuple, Union

from fabric.runners import Result

from sct import LOGGER
from sdcm.cluster import BaseNode, BaseCluster, BaseScyllaCluster
from sdcm.rest.storage_service_client import StorageServiceClient


class ScrubModes(NamedTuple):
    ABORT: str = "ABORT"
    SKIP: str = "SKIP"
    SEGREGATE: str = "SEGREGATE"
    VALIDATE: str = "VALIDATE"


class NodetoolCommands(NamedTuple):
    flush: str = "flush"
    stop_major_compaction: str = "stop COMPACTION"
    stop_scrub_compaction: str = "stop SCRUB"
    stop_cleanup_compaction: str = "stop CLEANUP"
    stop_upgrade_compaction: str = "stop UPGRADE"
    stop_reshape_compaction: str = "stop RESHAPE"


class CompactionOps:
    NODETOOL_CMD = NodetoolCommands()
    SCRUB_MODES = ScrubModes()

    def __init__(self, cluster: Union[BaseCluster, BaseScyllaCluster], node: Optional[BaseNode] = None):
        self.cluster = cluster
        self.node = node if node else self.cluster.nodes[0]
        self.storage_service_client = StorageServiceClient(node=self.node)

    def trigger_major_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
        return self.storage_service_client.compact_ks_cf(keyspace=keyspace, cf=cf)

    def trigger_scrub_compaction(self,
                                 keyspace: str = "keyspace1",
                                 cf: str = "standard1",
                                 scrub_mode: Optional[str] = None) -> Result:
        params = {"keyspace": keyspace, "cf": cf, "scrub_mode": scrub_mode}

        return self.storage_service_client.scrub_ks_cf(**params)

    def trigger_cleanup_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
        return self.storage_service_client.cleanup_ks_cf(keyspace=keyspace, cf=cf)

    def trigger_validation_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
        return self.storage_service_client.scrub_ks_cf(keyspace=keyspace,
                                                       cf=cf,
                                                       scrub_mode=self.SCRUB_MODES.VALIDATE)

    def trigger_upgrade_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
        return self.storage_service_client.upgrade_sstables(keyspace=keyspace, cf=cf)

    def trigger_flush(self):
        self.node.run_nodetool(self.NODETOOL_CMD.flush)

    def stop_major_compaction(self):
        self._stop_compaction(self.NODETOOL_CMD.stop_major_compaction)

    def stop_scrub_compaction(self):
        self._stop_compaction(self.NODETOOL_CMD.stop_scrub_compaction)

    def stop_cleanup_compaction(self):
        self._stop_compaction(self.NODETOOL_CMD.stop_cleanup_compaction)

    def stop_upgrade_compaction(self):
        self._stop_compaction(self.NODETOOL_CMD.stop_upgrade_compaction)

    def stop_reshape_compaction(self):
        self._stop_compaction(self.NODETOOL_CMD.stop_reshape_compaction)

    def stop_validation_compaction(self):
        self.stop_scrub_compaction()

    def disable_autocompaction_on_ks_cf(self, node: BaseNode,  keyspace: str = "", cf: Optional[str] = ""):
        node = node if node else self.node
        node.run_nodetool(f'disableautocompaction {keyspace} {cf}')

    def _stop_compaction(self, nodetool_cmd: str):
        LOGGER.info("Stopping compaction with nodetool %s", nodetool_cmd)
        self.node.run_nodetool(nodetool_cmd)

    @staticmethod
    def stop_on_user_compaction_logged(node: BaseNode, watch_for: str, timeout: int,
                                       stop_func: Callable, mark: Optional[int] = None):
        start_time = time.time()
        with open(node.system_log, "r") as log_file:
            if mark:
                log_file.seek(mark)

            while time.time() - start_time < timeout:
                line = log_file.readline()
                if watch_for in line:
                    stop_func()
                    LOGGER.info("Grepped expression found: %s in log line %s", watch_for, line)
                    break
