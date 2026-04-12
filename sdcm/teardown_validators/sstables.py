from contextlib import ExitStack, contextmanager
import json
import logging
import re
from functools import partial
from pathlib import Path

from sdcm import wait
from sdcm.cluster import BaseNode
from sdcm.exceptions import WaitForTimeoutError
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.events_device import EVENTS_LOG_DIR, RAW_EVENTS_LOG
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.teardown_validators import ValidatorEvent, ScrubValidationErrorEvent
from sdcm.teardown_validators.base import TeardownValidator
from sdcm.utils.common import S3Storage
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3
from sdcm.utils.sstable.sstable_utils import (
    CORRUPTED_SSTABLES_STATE_FILENAME,
    decrypt_sstables_on_node,
    decrypt_sstable_files_on_node,
    strip_encryption_options_from_schema,
)

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


def decrypt_corrupted_sstables(node: BaseNode, corrupted_sstables: list[str]) -> list[str]:
    """Take per-table snapshots to get schema.cql, then decrypt the corrupted Data.db files.

    Returns a list of decrypted directory paths that should be included in the upload alongside
    the original (encrypted) files.
    """
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
            f"nodetool snapshot -t {snapshot_tag} -cf {table_name} -- {keyspace}",
            ignore_status=True,
        )
        if snap_result.ok:
            schema_path = f"{table_dir}/snapshots/{snapshot_tag}/schema.cql"
            schema_result = node.remoter.run(f"cat {schema_path}", ignore_status=True, verbose=False)
            if schema_result.ok and schema_result.stdout.strip():
                schema_by_table_dir[table_dir] = strip_encryption_options_from_schema(schema_result.stdout)
            node.remoter.run(f"nodetool clearsnapshot -t {snapshot_tag} {keyspace}", ignore_status=True)

    # Group Data.db files by table_dir and decrypt each group
    decrypted_dirs = []
    for table_dir, schema_text in schema_by_table_dir.items():
        keyspace, table_name = table_dir_info[table_dir]
        data_files = [
            s for s in corrupted_sstables if s.endswith("-Data.db") and table_dir_from_sstable_path(s) == table_dir
        ]
        if data_files:
            decrypted_dir = decrypt_sstable_files_on_node(
                node=node,
                data_files=data_files,
                schema_text=schema_text,
                keyspace=keyspace,
                table=table_name,
            )
            if decrypted_dir:
                decrypted_dirs.append(decrypted_dir)
    return decrypted_dirs


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

        decrypted_dirs = decrypt_corrupted_sstables(node, corrupted_sstables)

        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info,
            list(corrupted_sstables) + decrypted_dirs,
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


CORRUPTED_SSTABLE_PATH_RE = re.compile(r"[./\w\-]+\.db")


def _parse_sstable_path(sstable_path: str):
    """Parse a ``.db`` sstable path into ``(sstable_dir, keyspace, table_name, sstable_name)``.

    Returns ``None`` when the path does not have the expected Scylla data layout
    (``/var/lib/scylla/data/<ks>/<table-dir>/<component>.db``).
    """
    parts = sstable_path.rsplit("/", 3)
    if len(parts) != 4:
        return None
    data_path, ks, table_dir, sst_name = parts
    # Sanity-check: ks and table_dir must be non-empty and table_dir must contain a UUID
    # separator (e.g. "standard1-01b9f260d55311ef8caf5992914eabbe").
    if not ks or "-" not in table_dir:
        return None
    return f"{data_path}/{ks}/{table_dir}", ks, table_dir.split("-")[0], sst_name


def find_corrupted_sstable_in_events(raw_events_file: Path):
    """Scan the raw events log and return (sstable_dir, keyspace, table_name, sstable_name, node_name).

    Iterates all ``.db`` path matches in the event line (last match first) so that the correct
    Scylla data path is selected even when the line contains multiple ``.db`` references.

    Only the **first** ``CORRUPTED_SSTABLE`` event is processed.  If multiple tables or nodes
    have corrupted sstables, only the first discovered event is prepared for upload.  This matches
    the behavior of ``SSTablesCollector`` which also handles a single corrupted-sstables state file.

    Returns a tuple of all-None values when no matching event is found.
    """
    with open(raw_events_file, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "CORRUPTED_SSTABLE" and event.get("severity") == "CRITICAL":
                matches = CORRUPTED_SSTABLE_PATH_RE.findall(event.get("line", ""))
                # Try matches in reverse order: the actual sstable path is typically the
                # last .db reference in the log line (after error-description text).
                for candidate in reversed(matches):
                    parsed = _parse_sstable_path(candidate)
                    if parsed is not None:
                        sstable_dir, ks, table_name, sst_name = parsed
                        return sstable_dir, ks, table_name, sst_name, event.get("node")
                if matches:
                    LOGGER.warning(
                        "CorruptedSstablesPreparator: found .db paths but none matched Scylla data layout: %s",
                        matches,
                    )
                else:
                    LOGGER.warning(
                        "CorruptedSstablesPreparator: no .db path found in event line: %s",
                        event.get("line", "")[:200],
                    )
    return None, None, None, None, None


def find_node_by_name(db_clusters_multitenant, node_name: str):
    """Return the first node whose ``name`` matches *node_name*, or ``None``."""
    for cluster in db_clusters_multitenant:
        for node in cluster.nodes:
            if node.name == node_name:
                return node
    return None


def take_snapshot_and_decrypt(node, sstable_dir: str, keyspace: str, table_name: str):
    """Run ``nodetool snapshot``, then decrypt the snapshot.  Return ``(snapshot_path, decrypted_path)``.

    Raises on unexpected errors so the caller can catch and log.
    """
    result = node.remoter.run(
        f"nodetool snapshot {keyspace} -cf {table_name}",
        ignore_status=True,
    )
    if not result.ok:
        LOGGER.warning("CorruptedSstablesPreparator: nodetool snapshot failed: %s — skipping", result.stderr)
        return None, None
    try:
        snapshot_dir = result.stdout.split("Snapshot directory: ")[1].strip()
    except IndexError:
        LOGGER.error("CorruptedSstablesPreparator: cannot parse snapshot dir from stdout: %s", result.stdout)
        return None, None
    snapshot_path = f"{sstable_dir}/snapshots/{snapshot_dir}"
    decrypted_path = decrypt_sstables_on_node(node, snapshot_path, keyspace=keyspace, table=table_name)
    return snapshot_path, decrypted_path


class CorruptedSstablesPreparator(TeardownValidator):
    """Pre-snapshot and pre-decrypt corrupted sstables while the node is still alive.

    Reads CORRUPTED_SSTABLE events from the raw events log, takes a ``nodetool snapshot`` of the
    affected table on the relevant node, decrypts the sstables, and writes a JSON state file to
    ``{logdir}/corrupted_sstables_snapshot.json``.

    ``SSTablesCollector.collect_logs()`` reads that state file and only performs the upload,
    avoiding any SSH calls after ``stop_resources()`` when the node may no longer be reachable.
    """

    validator_name = "corrupted_sstables_preparator"

    def validate(self):
        logdir = Path(self.tester.logdir)
        state_file = logdir / CORRUPTED_SSTABLES_STATE_FILENAME
        raw_events_file = logdir / EVENTS_LOG_DIR / RAW_EVENTS_LOG

        if not raw_events_file.exists():
            LOGGER.debug("CorruptedSstablesPreparator: raw events log not found at %s — skipping", raw_events_file)
            return

        sstable_dir, keyspace, table_name, sstable_name, node_name = find_corrupted_sstable_in_events(raw_events_file)
        if not node_name:
            LOGGER.info("CorruptedSstablesPreparator: no CORRUPTED_SSTABLE event found — nothing to prepare")
            return

        node = find_node_by_name(self.tester.db_clusters_multitenant, node_name)
        if node is None:
            LOGGER.warning("CorruptedSstablesPreparator: node %s not found in any cluster — skipping", node_name)
            return

        LOGGER.info(
            "CorruptedSstablesPreparator: taking snapshot for %s.%s on node %s", keyspace, table_name, node_name
        )
        try:
            snapshot_path, decrypted_path = take_snapshot_and_decrypt(node, sstable_dir, keyspace, table_name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CorruptedSstablesPreparator: unexpected error — skipping. Error: %s", exc)
            return
        if snapshot_path is None:
            return

        state = {
            "snapshot_path": snapshot_path,
            "decrypted_path": decrypted_path,
            "keyspace": keyspace,
            "table_name": table_name,
            "sstable_name": sstable_name,
            "node_name": node_name,
            "ssh_login_info": node.ssh_login_info,
        }
        state_file.write_text(json.dumps(state), encoding="utf-8")
        LOGGER.info(
            "CorruptedSstablesPreparator: state written to %s (snapshot=%s, decrypted=%s)",
            state_file,
            snapshot_path,
            decrypted_path,
        )
