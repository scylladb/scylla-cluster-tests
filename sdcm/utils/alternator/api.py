from __future__ import annotations

import logging
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import chain
from pprint import pformat
import time
from typing import NamedTuple, TYPE_CHECKING

import boto3
from cassandra import InvalidRequest
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from mypy_boto3_dynamodb.service_resource import Table

from sdcm.utils.alternator import schemas, enums, consts
from sdcm.utils.alternator.consts import TABLE_NAME, NO_LWT_TABLE_NAME
from sdcm.utils.common import normalize_ipv6_url
from sdcm.utils.context_managers import environment

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class AlternatorApi(NamedTuple):
    resource: DynamoDBServiceResource
    client: DynamoDBClient


TTL_ENABLED_SPECIFICATION = dict(AttributeName='ttl', Enabled=True)
TTL_DISABLED_SPECIFICATION = dict(AttributeName='ttl', Enabled=False)


class Alternator:
    def __init__(self, sct_params):
        self.params = sct_params
        self.alternator_apis = {}

    def create_endpoint_url(self, node):
        if node.is_kubernetes():
            web_protocol = "http" + ("s" if self.params.get("alternator_port") == 8043 else "")
            address = node.external_address
        else:
            web_protocol = "http"
            address = normalize_ipv6_url(node.external_address)
        return "{}://{}:{}".format(web_protocol, address, self.params.get("alternator_port"))

    @staticmethod
    def get_salted_hash(node: BaseNode, username: str) -> str:
        with node.parent_cluster.cql_connection_patient_exclusive(node) as session:
            for ks in ("system", "system_auth_v2", "system_auth"):
                try:
                    response = session.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role=%s;", (username,)).one()
                except InvalidRequest:
                    continue
                if response:
                    return response.salted_hash
            raise ValueError("couldn't find the salted_hash")

    def get_dynamodb_api(self, node) -> AlternatorApi:
        endpoint_url = self.create_endpoint_url(node=node)
        if endpoint_url not in self.alternator_apis:
            aws_params = dict(endpoint_url=endpoint_url,
                              region_name="None")
            if self.params.get('alternator_enforce_authorization'):
                aws_access_key_id = self.params.get("alternator_access_key_id")
                aws_params.update(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=self.get_salted_hash(node, username=aws_access_key_id)
                )
            # NOTE: add CA bundle info for HTTPS case
            env_vars = {}
            if "https" in endpoint_url:
                if ca_bundle_path := getattr(node, "alternator_ca_bundle_path", None):
                    env_vars["AWS_CA_BUNDLE"] = ca_bundle_path
                else:
                    LOGGER.warning("Alternator CA was not provided to the 'alternator' boto3 client.")
            with environment(**env_vars):
                resource: DynamoDBServiceResource = boto3.resource('dynamodb', **aws_params)
                client: DynamoDBClient = boto3.client('dynamodb', **aws_params)
                self.alternator_apis[endpoint_url] = AlternatorApi(resource=resource, client=client)
        return self.alternator_apis[endpoint_url]

    def set_credentials(self, node):
        if self.params.get('alternator_enforce_authorization'):
            with node.parent_cluster.cql_connection_patient(node) as session:
                session.execute("CREATE ROLE %s WITH PASSWORD = %s AND login = true AND superuser = true",
                                (self.params.get('alternator_access_key_id'),
                                    self.params.get('alternator_secret_access_key')))

    def get_credentials(self, node):
        access_key_id = self.params.get('alternator_access_key_id')
        if self.params.get('alternator_enforce_authorization'):
            return (access_key_id, self.get_salted_hash(node=node, username=access_key_id))
        else:
            access_key = self.params.get('alternator_secret_access_key')
            return (access_key_id, access_key) if access_key_id and access_key else None

    def set_write_isolation(self, node, isolation, table_name=consts.TABLE_NAME):
        dynamodb_api = self.get_dynamodb_api(node=node)
        isolation = isolation if not isinstance(isolation, enums.WriteIsolation) else isolation.value
        got = dynamodb_api.client.describe_table(TableName=table_name)['Table']
        arn = got['TableArn']
        tags = [
            {
                'Key': 'system:write_isolation',
                'Value': isolation
            }
        ]
        dynamodb_api.client.tag_resource(ResourceArn=arn, Tags=tags)

    def create_table(self, node,
                     schema=enums.YCSBSchemaTypes.HASH_AND_RANGE, isolation=None, table_name=consts.TABLE_NAME,
                     wait_until_table_exists=True, tablets_enabled: bool = False, lsi: str = None, gsi: str = None,
                     tags: dict[str, str] = None, **kwargs) -> Table:
        if isinstance(schema, enums.YCSBSchemaTypes):
            schema = schema.value
        schema = schemas.ALTERNATOR_SCHEMAS[schema]
        if lsi:
            schema['LocalSecondaryIndexes'] = [
                {
                    'IndexName': lsi,
                    'KeySchema': schema['KeySchema'],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]
        if gsi:
            schema['GlobalSecondaryIndexes'] = [
                {
                    'IndexName': gsi,
                    'KeySchema': schema['KeySchema'],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]
        tags_list = []
        if tags:
            tags_list.extend({'Key': k, 'Value': v} for k, v in tags.items())

        dynamodb_api = self.get_dynamodb_api(node=node)
        # Tablets feature is currently supported by Alternator, but disabled by default (since LWT is not supported).
        # It should be explicitly requested by the specified tag.
        # TODO: the 'tablets_enabled' parameter might become un-needed once Alternator tablets default is switched to be enabled.
        # This might be dependant on tablets LWT support issue (scylladb/scylladb#18068)
        if tablets_enabled:
            tags_list.append({'Key': 'experimental:initial_tablets', 'Value': '0'})

        if tags_list:
            kwargs['Tags'] = tags_list

        LOGGER.debug("Creating a new table '{}' using node '{}'".format(table_name, node.name))
        table = dynamodb_api.resource.create_table(
            TableName=table_name, BillingMode="PAY_PER_REQUEST", **schema, **kwargs)
        if wait_until_table_exists:
            waiter = dynamodb_api.client.get_waiter('table_exists')
            waiter.wait(TableName=table_name, WaiterConfig=dict(Delay=1, MaxAttempts=100))

        LOGGER.info("The table '{}' successfully created..".format(table_name))
        response = dynamodb_api.client.describe_table(TableName=table_name)

        if isolation:
            self.set_write_isolation(node=node, isolation=isolation, table_name=table_name)
        LOGGER.debug("Table's schema and configuration are: {}".format(response))
        return table

    def verify_tables_features(self, node, tables: dict = None, **kwargs):
        if tables:
            for table_name, schema in tables.items():
                self.verify_table_features(node, table_name, schema=schema, **kwargs)

    def verify_table_features(self, node, table_name=consts.TABLE_NAME, schema=enums.YCSBSchemaTypes.HASH_AND_RANGE,
                              lsi: str = None, gsi: str = None, tags: dict[str, str] = None,
                              wait_for_item_count: int = -1):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.client.describe_table(TableName=table_name)["Table"]

        if isinstance(schema, enums.YCSBSchemaTypes):
            schema = schema.value
        schema = schemas.ALTERNATOR_SCHEMAS[schema]
        assert table['KeySchema'] == schema['KeySchema'], "Table KeySchema does not match expected"

        table['Tags'] = dynamodb_api.client.list_tags_of_resource(ResourceArn=table['TableArn'])['Tags']
        for key, value in (tags or {}).items():
            assert {'Key': key, 'Value': value} in table['Tags'], f"Expected tag {key}:{value} to be present"

        if gsi is not None:
            gsis = {idx['IndexName']: idx for idx in table.get('GlobalSecondaryIndexes', [])}
            assert gsi in gsis.keys(), f"Expected GSI {gsi} to be present"
            assert gsis[gsi]['KeySchema'] == schema['KeySchema'], f"GSI {gsi} KeySchema does not match expected"

        if lsi is not None:
            lsis = {idx['IndexName']: idx for idx in table.get('LocalSecondaryIndexes', [])}
            assert lsi in lsis.keys(), f"Expected LSI {lsi} to be present"
            assert lsis[lsi]['KeySchema'] == schema['KeySchema'], f"LSI {lsi} KeySchema does not match expected"

        if wait_for_item_count >= 0:
            def repeat_scan_until_count(count, index=None, attempts=40):
                for attempt in range(attempts):
                    if attempt > 0:
                        time.sleep(attempt)
                    c = 0
                    start_key = {}
                    while start_key is not None:
                        response = dynamodb_api.client.scan(TableName=table_name, IndexName=index, Select='COUNT', **start_key) \
                            if index else dynamodb_api.client.scan(TableName=table_name, Select='COUNT', **start_key)
                        start_key = {'ExclusiveStartKey': response.get(
                            'LastEvaluatedKey')} if 'LastEvaluatedKey' in response else None
                        c += response['Count']
                    if c == count:
                        return True
                return False
            assert repeat_scan_until_count(count=wait_for_item_count, attempts=1), \
                f"Table {table_name} did not reach {wait_for_item_count} items within the expected time"
            if gsi is not None:
                assert repeat_scan_until_count(count=wait_for_item_count, index=gsi), \
                    f"GSI {gsi} did not reach {wait_for_item_count} items within the expected time"
            if lsi is not None:
                assert repeat_scan_until_count(count=wait_for_item_count, index=lsi), \
                    f"LSI {lsi} did not reach {wait_for_item_count} items within the expected time"

    def update_table_ttl(self, node, table_name, enabled: bool = True):
        dynamodb_api = self.get_dynamodb_api(node=node)
        ttl_specification = TTL_ENABLED_SPECIFICATION if enabled else TTL_DISABLED_SPECIFICATION
        dynamodb_api.client.update_time_to_live(TableName=table_name, TimeToLiveSpecification=ttl_specification)

    def modify_alternator_ttl_spec(self, node, enabled: bool):
        self.update_table_ttl(node=node, table_name=TABLE_NAME, enabled=enabled)
        self.update_table_ttl(node=node, table_name=NO_LWT_TABLE_NAME, enabled=enabled)

    def scan_table(self, node, table_name=consts.TABLE_NAME, threads_num=None, **kwargs):
        is_parallel_scan = threads_num and threads_num > 0
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)

        def _scan_table(part_scan_idx=None):
            parallel_params, result, still_running_while = {}, [], True
            if is_parallel_scan:
                parallel_params = {"TotalSegments": threads_num, "Segment": part_scan_idx}
                LOGGER.debug("Starting parallel scan part '{}' on table '{}'".format(part_scan_idx + 1, table_name))
            else:
                LOGGER.debug("Starting full scan on table '{}'".format(table_name))
            while still_running_while:
                response = table.scan(**parallel_params, **kwargs)
                result.extend(response["Items"])
                still_running_while = 'LastEvaluatedKey' in response

            LOGGER.debug("Founding the following items:\n{}".format(pformat(result)))
            return result

        if is_parallel_scan:
            with ThreadPoolExecutor(max_workers=threads_num) as executor:
                threads = [executor.submit(_scan_table, part_idx) for part_idx in range(threads_num)]
                scan_result = [thread.result() for thread in threads]
            return list(chain(*scan_result)) if len(scan_result) > 1 else scan_result
        return _scan_table()

    def batch_write_actions(self, node,
                            table_name=consts.TABLE_NAME, new_items=None, delete_items=None,
                            schema=schemas.HASH_SCHEMA):
        dynamodb_api = self.get_dynamodb_api(node=node)
        assert new_items or delete_items, "should pass new_items or delete_items, other it's a no-op"
        new_items, delete_items = new_items or [], delete_items or []
        if new_items:
            LOGGER.debug("Adding new {} items to table '{}'.\n{}..".format(
                len(new_items), table_name, pformat(new_items)))
        if delete_items:
            LOGGER.debug("Deleting %s items from table '%s'.\nDeleted: %s..",
                         len(delete_items), table_name, pformat(delete_items))

        table = dynamodb_api.resource.Table(name=table_name)
        with table.batch_writer() as batch:
            for item in new_items:
                batch.put_item(item)
            if delete_items:
                table_keys = [key["AttributeName"] for key in schema["KeySchema"]]
                for item in delete_items:
                    batch.delete_item({key: item[key] for key in table_keys})
        return table

    def is_table_exists(self, node, table_name: consts.TABLE_NAME):
        dynamodb_api = self.get_dynamodb_api(node=node)
        is_table_exists = True

        try:
            dynamodb_api.client.describe_table(TableName=table_name)
        except dynamodb_api.client.exceptions.ResourceNotFoundException:
            is_table_exists = False
        LOGGER.info("The table '{}'{} exists in endpoint {}..".format(
            table_name, '' if is_table_exists else 'not', node.name))
        return is_table_exists

    def delete_table(self, node, table_name: consts.TABLE_NAME, wait_until_table_removed=True):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        table.delete()
        if wait_until_table_removed:
            waiter = dynamodb_api.client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
            LOGGER.info("The '{}' table successfully removed".format(table_name))
        else:
            LOGGER.info("Send request to removed '{}' table".format(table_name))
