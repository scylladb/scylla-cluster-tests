import logging
from functools import partial

from sdcm import wait
from sdcm.cluster import BaseNode
from sdcm.exceptions import WaitForTimeoutError
from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent, ScrubValidationErrorEvent
from sdcm.teardown_validators.base import TeardownValidator
from sdcm.utils.common import S3Storage, ParallelObject
from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3

LOGGER = logging.getLogger(__name__)


class SstablesValidator(TeardownValidator):
    validator_name = 'scrub'

    @staticmethod
    def _upload_corrupted_files(node: BaseNode, quarantine_log_lines):
        # get quarantine dir from lines:
        # INFO  2024-04-02 12:40:24,787 [shard 0:stre] sstable - Moving sstable /var/lib/scylla/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/me-3gey_0z3c_2h5vl2ogebmg26ku9t-big-Data.db to "/var/lib/scylla/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/quarantine"
        # print(log_lines)
        quarantine_dirs = {line.split(' to "')[1][:-2]
                           for line in quarantine_log_lines
                           if 'sstable - Moving sstable' in line and 'quarantine' in line}
        if not quarantine_dirs:
            LOGGER.error('No quarantine directories found in db logs on node: %s', node.name)
            return "<No quarantine directories found>"
        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info, list(quarantine_dirs), s3_bucket=S3Storage.bucket_name,
            s3_key=f"{node.parent_cluster.uuid}/{node.name}-corrupted-sstables.tar.gz",
            max_size_gb=100, public_read_acl=True)
        return s3_link

    def _run_nodetool_scrub(self, node: BaseNode, keyspace: str, table: str, timeout=1200):
        try:
            node.wait_db_up(timeout=300)
        except WaitForTimeoutError as ex:
            # sometimes node can boot very long after last nemesis (e.g. bootstrap new node).
            LOGGER.error("Error waiting for node %s to be up in sstable validator: %s\nskipping validation", node.name, ex)
            return
        finish_scrub_follower = node.follow_system_log(patterns=['Finished scrubbing in validate mode'])
        quarantine_lines = node.follow_system_log(patterns=['sstable - Moving sstable'], start_from_beginning=True)
        result = node.run_nodetool(sub_cmd='scrub', args=f"--mode VALIDATE --no-snapshot {keyspace} {table}".strip(),
                                   timeout=timeout, coredump_on_timeout=False, ignore_status=True)
        if not result.ok:
            ValidatorEvent(
                message=f'Error running nodetool scrub on node {node.name}: {result.stdout}\n{result.stderr}',
                severity=Severity.ERROR).publish()
        # sometimes logs might be delayed, so we need to wait for them
        scrub_finish_lines = wait.wait_for(func=lambda: list(finish_scrub_follower), step=10,
                                           text="Waiting for 'Finished scrubbing in validate mode' logs",
                                           timeout=300, throw_exc=False)
        if not scrub_finish_lines:
            ValidatorEvent(
                message=f'No scrubbing validation message found in db logs on node: {node.name}', severity=Severity.ERROR).publish()
            return
        invalid_sstables = [line for line in scrub_finish_lines if 'sstable is invalid' in line]
        if invalid_sstables:
            LOGGER.error("Invalid sstables found: %s.", invalid_sstables)
            s3_link = self._upload_corrupted_files(node, list(quarantine_lines))
            ScrubValidationErrorEvent(node.name, s3_link).publish()

    def validate(self):
        for cluster in self.tester.db_clusters_multitenant:
            keyspace = self.configuration.get("keyspace")
            table = self.configuration.get("table")
            timeout = self.configuration.get("timeout", 1200)
            run_scrub = partial(self._run_nodetool_scrub, keyspace=keyspace, table=table, timeout=timeout)
            run_scrub.__name__ = run_scrub.func.__name__
            try:
                LOGGER.info("Running nodetool scrub on all nodes in validation mode")
                parallel_obj = ParallelObject(objects=cluster.nodes, timeout=timeout)
                parallel_obj.run(run_scrub, ignore_exceptions=False, unpack_objects=True)
                LOGGER.info("Nodetool scrub validation finished")
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Error during nodetool scrub validation: %s", exc)
                ValidatorEvent(
                    message=f'Error during nodetool scrub validation: {exc}', severity=Severity.ERROR).publish()
