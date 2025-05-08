import gzip
import json
import os
import re
from pathlib import Path

from sdcm.utils.common import download_dir_from_cloud


CREATE_KS_REPLICATION_BLOCK_PATTERN = re.compile(r"replication\s*=\s*({.*?})")
REPLICATION_DICT_KEYS_PATTERN = re.compile(r"'([^']+)':")


def get_schema_create_statements_from_snapshot(
        bucket: str,
        mgr_cluster_id: str,
        snapshot_tag: str,
        exclude_system_ks: bool = True,
        exclude_roles: bool = True,
) -> tuple[list[str], list[str]]:
    """Parses schema information from a snapshot file and extracts CQL statements for keyspaces, tables, views and other
    entities. Excludes entries related to system keyspaces and roles if the corresponding flags are set to True.

    Important note: only AWS is supported for now.

    Returns:
        tuple[list[str], list[str]]: A tuple containing two lists:
            - List of CQL statements for creating keyspaces.
            - List of CQL statements for other entities (tables, views and others except excluded ones).
    """
    s3_dir_with_schema_json = f"s3://{bucket}/backup/schema/cluster/{mgr_cluster_id}"
    temp_dir_with_schema_files = download_dir_from_cloud(url=s3_dir_with_schema_json)

    # If several backups have been created for the same cluster, the downloaded dir will contain several files.
    # Thus, the matching schema file should be found by the snapshot tag.
    for filename in os.listdir(temp_dir_with_schema_files):
        if snapshot_tag in filename:
            schema_filename = Path(temp_dir_with_schema_files, filename)
            break
    else:
        raise FileNotFoundError(f"Schema file for snapshot {snapshot_tag} not found in {s3_dir_with_schema_json}")

    with gzip.open(schema_filename, 'rt', encoding='utf-8') as gz_file:
        schema = json.load(gz_file)

    keyspace_statements = []
    other_entities_statements = []

    for entry in schema:
        if exclude_system_ks:
            if entry["keyspace"].startswith("system"):
                continue

        if exclude_roles:
            if entry["type"] == "role":
                continue

        if entry["type"] == "keyspace":
            keyspace_statements.append(entry["cql_stmt"])
        else:
            other_entities_statements.append(entry["cql_stmt"])

    return keyspace_statements, other_entities_statements


def get_dc_name_from_ks_statement(ks_statement: str) -> list[str]:
    """Extracts the name of a datacenter from a CREATE KEYSPACE statement.

    For example, for the statement:

        "CREATE KEYSPACE \"5gb_stcs_quorum_64_16_2024_2_4\"
            WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'us-east': '3'}
            AND durable_writes = true
            AND tablets = {'enabled': false};"

    the function will return ["us-east"].
    """
    # Firstly, the code extracts the whole replication block from the statement.
    # Then, it finds all keys in the replication dictionary and filters out the 'class' key.
    # It doesn't search for dc_name in the replication block directly because the order of keys might be different.
    replication_block = CREATE_KS_REPLICATION_BLOCK_PATTERN.search(ks_statement).group(1)
    _keys = REPLICATION_DICT_KEYS_PATTERN.findall(replication_block)

    return [key for key in _keys if key != 'class']
