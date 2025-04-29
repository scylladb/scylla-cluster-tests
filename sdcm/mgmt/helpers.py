import ast
import gzip
import json
import os
import re
from pathlib import Path

from sdcm.utils.common import download_dir_from_cloud


def get_schema_create_statements_from_snapshot(bucket: str, mgr_cluster_id: str) -> tuple[list[str], list[str]]:
    """Parses schema information from a snapshot file and extracts CQL statements for keyspaces and tables,
    excluding entries related to system keyspaces, roles.

    Important note: only AWS is supported for now.

    Returns:
        tuple[list[str], list[str]]: A tuple containing two lists:
            - List of CQL statements for creating keyspaces.
            - List of CQL statements for creating tables.
    """
    s3_dir_with_schema_json = f"s3://{bucket}/backup/schema/cluster/{mgr_cluster_id}"
    temp_dir_with_schema_file = download_dir_from_cloud(url=s3_dir_with_schema_json)
    schema_file = Path(temp_dir_with_schema_file, os.listdir(temp_dir_with_schema_file)[0])

    with gzip.open(schema_file, 'rt', encoding='utf-8') as gz_file:
        schema = json.load(gz_file)

    keyspace_statements = []
    table_statements = []

    for entry in schema:
        if entry["type"] == "keyspace" and not entry["keyspace"].startswith("system"):
            keyspace_statements.append(entry["cql_stmt"])
        elif entry["type"] == "table" and not entry["keyspace"].startswith("system"):
            table_statements.append(entry["cql_stmt"])

    return keyspace_statements, table_statements


def get_dc_name_from_ks_statement(ks_statement: str) -> str:
    """Extracts the name of a datacenter from a CREATE KEYSPACE statement.

    For example, for the statement:

        "CREATE KEYSPACE \"5gb_stcs_quorum_64_16_2024_2_4\"
            WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'us-east': '3'}
            AND durable_writes = true
            AND tablets = {'enabled': false};"

    the function will return "us-east".
    """
    pattern = re.compile(r"replication\s*=\s*({.*?})")
    replication_block = pattern.search(ks_statement).group(1)
    replication_dict = ast.literal_eval(replication_block)

    dc_keys = [key for key in replication_dict.keys() if key != 'class']
    return dc_keys[0]
