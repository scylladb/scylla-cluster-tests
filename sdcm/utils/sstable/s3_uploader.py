import logging
from datetime import datetime
from pathlib import Path

from sdcm.cluster import BaseNode
from sdcm.logcollector import CollectingNode
from sdcm.utils.common import S3Storage
from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3

LOGGER = logging.getLogger(__name__)


def upload_system_table_to_s3(
    node: CollectingNode | BaseNode, table_name: str, test_id: str, public: bool = True
) -> str:
    """Uploads system table data to S3 by streaming directly from the node.

    This function:
    1. Runs CQL query on the node to export table data
    2. Compresses with zstd on the node
    3. Streams directly to S3 without loading into memory

    Args:
        node: The node to collect data from
        table_name: Full table name (e.g., 'system.compaction_history')
        test_id: Test ID for S3 path organization
        public: Whether to make the S3 object publicly readable

    Returns:
        S3 download link or empty string on failure
    """
    LOGGER.info("Collecting %s table from node %s...", table_name, node.name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    remote_file = f"/tmp/sct_{table_name.replace('.', '_')}_{timestamp}.json"
    compressed_file = f"{remote_file}.zst"

    try:
        # Export table to JSON file on the node
        export_cmd = f"cqlsh -e \"SELECT JSON * FROM {table_name}\" > {remote_file}"
        LOGGER.info("Exporting %s to %s on node %s", table_name, remote_file, node.name)
        result = node.remoter.run(export_cmd, ignore_status=True)

        if not result.ok:
            LOGGER.warning("Failed to export %s on node %s: %s", table_name, node.name, result.stderr)
            return ""

        # Check if file exists and get its size
        check_result = node.remoter.run(f"test -f {remote_file} && stat -f%z {remote_file} || stat -c%s {remote_file}", ignore_status=True)
        if not check_result.ok:
            LOGGER.warning("File %s not found on node %s", remote_file, node.name)
            return ""

        file_size_bytes = int(check_result.stdout.strip())
        file_size_mb = file_size_bytes / (1024 * 1024)
        LOGGER.info("Exported file size: %.2f MB", file_size_mb)

        # Skip if empty or too small (likely no data)
        if file_size_bytes < 100:
            LOGGER.info("Table %s appears to be empty, skipping upload", table_name)
            node.remoter.run(f"rm -f {remote_file}", ignore_status=True)
            return ""

        # Compress with zstd
        LOGGER.info("Compressing %s with zstd on node %s", remote_file, node.name)
        node.remoter.run(f"zstd -f {remote_file} -o {compressed_file}")

        # Upload to S3
        s3_key = f"{test_id}/{timestamp}/{table_name.replace('.', '_')}-{timestamp}-{node.name}.json.zst"
        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info,
            [compressed_file],
            s3_bucket=S3Storage.bucket_name,
            s3_key=s3_key,
            max_size_gb=80,
            public_read_acl=public,
        )

        if s3_link:
            LOGGER.info("Successfully uploaded %s from node %s to %s", table_name, node.name, s3_link)

        # Cleanup
        node.remoter.run(f"rm -f {remote_file} {compressed_file}", ignore_status=True)

    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Error while collecting and uploading %s: %s", table_name, exc, exc_info=exc)
        # Attempt cleanup on error
        node.remoter.run(f"rm -f {remote_file} {compressed_file}", ignore_status=True)
        s3_link = ""

    return s3_link


def upload_sstables_to_s3(
    node: CollectingNode | BaseNode, keyspace: str, test_id: str, tables: list | None = None, public: bool = True
):
    """Uploads given keyspace/tables sstables snapshot to s3.

    Uploaded snapshots will be visible in show-logs command for given test_id."""
    LOGGER.info("Collecting sstables for node %s...", node.name)
    data_directory = "/var/lib/scylla/data"
    snapshot_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_tag = f"sct-{snapshot_date}"
    nodetool_snapshot_cmd = f"nodetool snapshot -t {snapshot_tag}"
    if tables:
        nodetool_snapshot_cmd += " -cf " + ",".join(tables)
    nodetool_snapshot_cmd += f" -- {keyspace}"
    try:
        node.remoter.run(nodetool_snapshot_cmd)
        snapshot_paths = node.remoter.run(f"find {data_directory} -type d -name {snapshot_tag}").stdout.split()
        s3_link = upload_remote_files_directly_to_s3(
            node.ssh_login_info,
            snapshot_paths,
            s3_bucket=S3Storage.bucket_name,
            s3_key=f"{test_id}/{snapshot_date}/sstables-{snapshot_date}-{node.name}-{keyspace}.tar.gz",
            max_size_gb=400,
            public_read_acl=public,
        )
        if s3_link:
            LOGGER.info("Successfully uploaded sstables on node %s for keyspace %s", node.name, keyspace)
        node.remoter.run(f"nodetool clearsnapshot -t {snapshot_tag} {keyspace}")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Error while getting and uploading sstables: %s", exc, exc_info=exc)
        s3_link = ""
    return s3_link
