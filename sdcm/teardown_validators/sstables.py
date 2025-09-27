from contextlib import ExitStack, contextmanager
import logging
import re
from functools import partial

from sdcm import wait
from sdcm.cluster import BaseNode
from sdcm.exceptions import WaitForTimeoutError
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.teardown_validators import ValidatorEvent, ScrubValidationErrorEvent
from sdcm.teardown_validators.base import TeardownValidator
from sdcm.utils.common import S3Storage
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3

LOGGER = logging.getLogger(__name__)


@contextmanager
def severity_change_context():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.ERROR,  # killing stress creates Critical error
            event_class=DatabaseLogEvent.CORRUPTED_SSTABLE,
            extra_time_to_expiration=60))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,  # if a node is shutting down, errors are logged
            event_class=DatabaseLogEvent.DATABASE_ERROR,
            regex=r"api - scrub .* failed: seastar::abort_requested_exception \(abort requested\)",
            extra_time_to_expiration=60))
        yield


class SstablesValidator(TeardownValidator):
    validator_name = 'scrub'

    @staticmethod
    def _upload_corrupted_files(node: BaseNode, invalid_sstables_lines):
        # get corrupted sstables from lines:
        # INFO  2024-04-02 12:40:24,787 [shard 0:stre] sstable - Moving sstable /var/lib/scylla/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/me-3gey_0z3c_2h5vl2ogebmg26ku9t-big-Data.db to "/var/lib/scylla/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/quarantine"
        # scylla[5976]:  [shard 6:strm] compaction - Finished scrubbing in validate mode /var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db - sstable is invalid"
        corrupted_sstables = []
        sstable_path_regexp = re.compile(r'[./\w\-]+(\.db|quarantine)')
        for line in invalid_sstables_lines:
            if matches := sstable_path_regexp.finditer(line):
                corrupted_sstables.append(list(matches)[-1].group(0))
        # remove duplicates from corrupted_sstables
        corrupted_sstables = list(set(corrupted_sstables))
        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info, list(corrupted_sstables), s3_bucket=S3Storage.bucket_name,
            s3_key=f"{node.parent_cluster.uuid}/{node.name}-corrupted-sstables.tar.gz",
            max_size_gb=30, public_read_acl=True)
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
        invalid_sstables_lines = [line for line in scrub_finish_lines if 'sstable is invalid' in line]
        if invalid_sstables_lines:
            invalid_sstables_lines += list(quarantine_lines)
            LOGGER.error("Invalid sstables found: %s.", invalid_sstables_lines)
            s3_link = self._upload_corrupted_files(node, list(invalid_sstables_lines))
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
                with severity_change_context():
                    parallel_obj.run(run_scrub, ignore_exceptions=False, unpack_objects=True)
                LOGGER.info("Nodetool scrub validation finished")
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Error during nodetool scrub validation: %s", exc)
                ValidatorEvent(
                    message=f'Error during nodetool scrub validation: {exc}', severity=Severity.ERROR).publish()
