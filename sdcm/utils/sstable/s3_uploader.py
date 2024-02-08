import logging
from datetime import datetime

from sdcm.cluster import BaseNode
from sdcm.logcollector import CollectingNode
from sdcm.utils.common import S3Storage
from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3

LOGGER = logging.getLogger(__name__)


def upload_sstables_to_s3(node: CollectingNode | BaseNode, keyspace: str, test_id: str, tables: list | None = None):
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
            node.ssh_login_info, snapshot_paths, s3_bucket=S3Storage.bucket_name,
            s3_key=f"{test_id}/{snapshot_date}/sstables-{snapshot_date}-{node.name}-{keyspace}.tar.gz",
            max_size_gb=400, public_read_acl=True)
        if s3_link:
            LOGGER.info("Successfully uploaded sstables on node %s for keyspace %s", node.name, keyspace)
        node.remoter.run(f"nodetool clearsnapshot -t {snapshot_tag} {keyspace}")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Error while getting and uploading sstables: %s", exc, exc_info=exc)
        s3_link = ""
    return s3_link
