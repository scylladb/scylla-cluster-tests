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
from sdcm.utils.sstable.sstable_utils import decrypt_sstable_files_on_node, strip_encryption_options_from_schema

LOGGER = logging.getLogger(__name__)


@contextmanager
def severity_change_context():
    with ExitStack() as stack:
        stack.enter_context(
            EventsSeverityChangerFilter(
                new_severity=Severity.ERROR,  # killing stress creates Critical error
                event_class=DatabaseLogEvent.CORRUPTED_SSTABLE,
                extra_time_to_expiration=60,
            )
        )
        stack.enter_context(
            EventsSeverityChangerFilter(
                new_severity=Severity.NORMAL,  # if a node is shutting down, errors are logged
                event_class=DatabaseLogEvent.DATABASE_ERROR,
                regex=r"api - scrub .* failed: seastar::abort_requested_exception \(abort requested\)",
                extra_time_to_expiration=60,
            )
        )
        yield


def table_dir_from_sstable_path(sstable: str) -> str | None:
    """Extract the table directory (``/var/lib/scylla/data/<ks>/<table-dir>``) from an sstable path."""
    clean = re.sub(r"/quarantine(/.*)?$", "", sstable)
    parts = clean.rstrip("/").split("/")
    # Expected structure: ['', 'var', 'lib', 'scylla', 'data', <ks>, <table-dir>, ...]
    return "/".join(parts[:7]) if len(parts) >= 7 else None


def decrypt_corrupted_sstables(node: BaseNode, corrupted_sstables: list[str]) -> None:
    """Take per-table snapshots to get schema.cql, then decrypt the corrupted Data.db files."""
    snapshot_tag = "sct-scrub-decrypt"
    # Map table_dir -> (keyspace, table_name) for each unique table touched
    table_dir_info: dict[str, tuple[str, str]] = {}
    for sstable in corrupted_sstables:
        table_dir = table_dir_from_sstable_path(sstable)
        if table_dir:
            parts = table_dir.rstrip("/").split("/")
            table_dir_info[table_dir] = (parts[5], parts[6].split("-")[0])

    # Collect schema text per table dir, taking snapshots as needed
    schema_by_table_dir: dict[str, str] = {}
    for table_dir, (keyspace, table_name) in table_dir_info.items():
        snap_result = node.remoter.run(
            f"nodetool snapshot -t {snapshot_tag} {keyspace} -cf {table_name}",
            ignore_status=True,
        )
        if snap_result.ok:
            schema_path = f"{table_dir}/snapshots/{snapshot_tag}/schema.cql"
            schema_result = node.remoter.run(f"cat {schema_path}", ignore_status=True, verbose=False)
            if schema_result.ok and schema_result.stdout.strip():
                schema_by_table_dir[table_dir] = strip_encryption_options_from_schema(schema_result.stdout)
            node.remoter.run(f"nodetool clearsnapshot -t {snapshot_tag} {keyspace}", ignore_status=True)

    # Group Data.db files by table_dir and decrypt each group
    for table_dir, schema_text in schema_by_table_dir.items():
        keyspace, table_name = table_dir_info[table_dir]
        data_files = [
            s for s in corrupted_sstables if s.endswith(".db") and table_dir_from_sstable_path(s) == table_dir
        ]
        if data_files:
            decrypt_sstable_files_on_node(
                node=node,
                data_files=data_files,
                schema_text=schema_text,
            )


class SstablesValidator(TeardownValidator):
    validator_name = "scrub"

    def _upload_corrupted_files(self, node: BaseNode, invalid_sstables_lines):
        # get corrupted sstables from lines:
        # INFO  2024-04-02 12:40:24,787 [shard 0:stre] sstable - Moving sstable /var/lib/scylla/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/me-3gey_0z3c_2h5vl2ogebmg26ku9t-big-Data.db to "/var/lib/scylla/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/quarantine"
        # scylla[5976]:  [shard 6:strm] compaction - Finished scrubbing in validate mode /var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db - sstable is invalid"
        corrupted_sstables = []
        sstable_path_regexp = re.compile(r"[./\w\-]+(\.db|quarantine)")
        for line in invalid_sstables_lines:
            if matches := sstable_path_regexp.finditer(line):
                corrupted_sstables.append(list(matches)[-1].group(0))
        # remove duplicates from corrupted_sstables
        corrupted_sstables = list(set(corrupted_sstables))

        decrypt_corrupted_sstables(node, corrupted_sstables)

        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info,
            list(corrupted_sstables),
            s3_bucket=S3Storage.bucket_name,
            s3_key=f"{node.parent_cluster.uuid}/{node.name}-corrupted-sstables.tar.gz",
            max_size_gb=30,
            public_read_acl=True,
        )
        return s3_link

    def _run_nodetool_scrub(self, node: BaseNode, keyspace: str, table: str, timeout=1200):
        try:
            node.wait_db_up(timeout=300)
        except WaitForTimeoutError as ex:
            # sometimes node can boot very long after last nemesis (e.g. bootstrap new node).
            LOGGER.error(
                "Error waiting for node %s to be up in sstable validator: %s\nskipping validation", node.name, ex
            )
            return
        finish_scrub_follower = node.follow_system_log(patterns=["Finished scrubbing in validate mode"])
        quarantine_lines = node.follow_system_log(patterns=["sstable - Moving sstable"], start_from_beginning=True)
        result = node.run_nodetool(
            sub_cmd="scrub",
            args=f"--mode VALIDATE --no-snapshot {keyspace} {table}".strip(),
            timeout=timeout,
            coredump_on_timeout=False,
            ignore_status=True,
        )
        if not result.ok:
            ValidatorEvent(
                message=f"Error running nodetool scrub on node {node.name}: {result.stdout}\n{result.stderr}",
                severity=Severity.ERROR,
            ).publish()
        # sometimes logs might be delayed, so we need to wait for them
        scrub_finish_lines = wait.wait_for(
            func=lambda: list(finish_scrub_follower),
            step=10,
            text="Waiting for 'Finished scrubbing in validate mode' logs",
            timeout=300,
            throw_exc=False,
        )
        if not scrub_finish_lines:
            ValidatorEvent(
                message=f"No scrubbing validation message found in db logs on node: {node.name}",
                severity=Severity.ERROR,
            ).publish()
            return
        invalid_sstables_lines = [line for line in scrub_finish_lines if "sstable is invalid" in line]
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
                    message=f"Error during nodetool scrub validation: {exc}", severity=Severity.ERROR
                ).publish()
