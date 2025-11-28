import gzip
import json
import logging
import os
import re
from pathlib import Path

from sdcm.utils.common import download_dir_from_cloud


LOGGER = logging.getLogger(__name__)

CREATE_KS_REPLICATION_BLOCK_PATTERN = re.compile(r"replication\s*=\s*({.*?})")
REPLICATION_DICT_KEYS_PATTERN = re.compile(r"'([^']+)':")


def _parse_schema_entries(
        schema: list,
        exclude_system_ks: bool = True,
        exclude_roles: bool = True,
) -> tuple[list[str], list[str]]:
    """Helper function to parse schema entries and extract CQL statements

    Args:
        schema: List of schema entries from JSON
        exclude_system_ks: Whether to exclude system keyspaces from the results
        exclude_roles: Whether to exclude role-related statements from the results

    Returns:
        tuple[list[str], list[str]]: A tuple containing two lists:
            - List of CQL statements for creating keyspaces.
            - List of CQL statements for other entities (tables, views and others except excluded ones).
    """
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


def get_schema_create_statements_from_snapshot(
        bucket: str,
        mgr_cluster_id: str,
        snapshot_tag: str,
        exclude_system_ks: bool = True,
        exclude_roles: bool = True,
) -> tuple[list[str], list[str]]:
    """Parses schema information from a snapshot file downloaded from S3.

    Important note: only AWS is supported for now.

    Args:
        bucket: S3 bucket name containing the schema snapshots
        mgr_cluster_id: Management cluster ID to locate the schema files
        snapshot_tag: Tag to identify the specific snapshot file
        exclude_system_ks: Whether to exclude system keyspaces from the results
        exclude_roles: Whether to exclude role-related statements from the results
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

    return _parse_schema_entries(schema, exclude_system_ks, exclude_roles)


def get_schema_create_statements_from_file(
        schema_file_path: str,
        exclude_system_ks: bool = True,
        exclude_roles: bool = True,
) -> tuple[list[str], list[str]]:
    """Parses schema information from a local schema.json file.

    Args:
        schema_file_path: Path to the local schema.json file
        exclude_system_ks: Whether to exclude system keyspaces from the results
        exclude_roles: Whether to exclude role-related statements from the results
    """
    with open(schema_file_path, encoding='utf-8') as file:
        schema = json.load(file)

    return _parse_schema_entries(schema, exclude_system_ks, exclude_roles)


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


def substitute_dc_name_in_ks_statements_if_different(ks_statements: list[str], current_dc_name: str) -> list[str]:
    """Substitute datacenter name in keyspace statements to match the current cluster's DC name

    Args:
        ks_statements: List of CREATE KEYSPACE CQL statements
        current_dc_name: The current datacenter name that should be used

    Returns:
        list[str]: Modified keyspace statements with updated DC names
    """
    if not ks_statements:
        raise ValueError("ks_statements list is empty. Cannot substitute datacenter name")
    dc_names = get_dc_name_from_ks_statement(ks_statements[0])

    if not dc_names:
        raise ValueError("No datacenter name found in the first keyspace statement. Cannot substitute datacenter name")
    dc_from_backup_name = dc_names[0]

    LOGGER.debug("DC name from backup: %s, DC name under test: %s", dc_from_backup_name, current_dc_name)

    if current_dc_name != dc_from_backup_name:
        LOGGER.debug("DC names mismatch - restoring the schema manually altering cql statements and "
                     "applying them one by one")
        # Alter the dc_name in keyspace cql statements to match the current cluster's dc_name
        # Include quotes and colon to avoid unintended replacements
        old_dc_block = f"'{dc_from_backup_name}':"
        new_dc_block = f"'{current_dc_name}':"
        ks_statements = [stmt.replace(old_dc_block, new_dc_block) for stmt in ks_statements]

    return ks_statements
