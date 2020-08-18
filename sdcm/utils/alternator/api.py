import logging
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import chain
from typing import NamedTuple

import boto3
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from mypy_boto3_dynamodb.service_resource import Table

from sdcm.utils.alternator import schemas, enums, consts
from sdcm.utils.common import normalize_ipv6_url

LOGGER = logging.getLogger(__name__)


class AlternatorApi(NamedTuple):
    resource: DynamoDBServiceResource
    client: DynamoDBClient


class Alternator:
    def __init__(self, sct_params):
        self.params = sct_params
        self.alternator_apis = {}

    def create_endpoint_url(self, node):
        return 'http://{}:{}'.format(normalize_ipv6_url(node.external_address), self.params.get("alternator_port"))

    def get_dynamodb_api(self, node) -> AlternatorApi:
        endpoint_url = self.create_endpoint_url(node=node)
        if endpoint_url not in self.alternator_apis:
            aws_params = dict(endpoint_url=endpoint_url, aws_access_key_id=self.params.get("alternator_access_key_id"),
                              aws_secret_access_key=self.params.get("alternator_secret_access_key"),
                              region_name=self.params.get("region_name") and self.params.get("region_name").split()[0])
            resource: DynamoDBServiceResource = boto3.resource('dynamodb', **aws_params)
            client: DynamoDBClient = boto3.client('dynamodb', **aws_params)
            self.alternator_apis[endpoint_url] = AlternatorApi(resource=resource, client=client)
        return self.alternator_apis[endpoint_url]

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

    def create_table(self, node,  # pylint: disable=too-many-arguments
                     schema=enums.YCSVSchemaTypes.HASH_AND_RANGE, isolation=None, table_name=consts.TABLE_NAME,
                     wait_until_table_exists=True, **kwargs) -> Table:
        if isinstance(schema, enums.YCSVSchemaTypes):
            schema = schema.value
        schema = schemas.ALTERNATOR_SCHEMAS[schema]
        dynamodb_api = self.get_dynamodb_api(node=node)
        LOGGER.debug(f"Creating a new table '{table_name}' using node '{node.name}'")
        table = dynamodb_api.resource.create_table(
            TableName=table_name, BillingMode="PAY_PER_REQUEST", **schema, **kwargs)
        if wait_until_table_exists:
            waiter = dynamodb_api.client.get_waiter('table_exists')
            waiter.wait(TableName=table_name, WaiterConfig=dict(Delay=1, MaxAttempts=100))

        LOGGER.info(f"The table '{table_name}' successfully created..")
        response = dynamodb_api.client.describe_table(TableName=table_name)

        if isolation:
            self.set_write_isolation(node=node, isolation=isolation, table_name=table_name)
        LOGGER.debug(f"Table's schema and configuration are: {response}")
        return table

    def scan_table(self, node, table_name=consts.TABLE_NAME, threads_num=None, **kwargs):
        is_parallel_scan = threads_num and threads_num > 0
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)

        def _scan_table(part_scan_idx=None):
            parallel_params, result, still_running_while = {}, [], True
            if is_parallel_scan:
                parallel_params = {"TotalSegments": threads_num, "Segment": part_scan_idx}
                LOGGER.debug(f"Starting parallel scan part '{part_scan_idx + 1}' on table '{table_name}'")
            else:
                LOGGER.debug(f"Starting full scan on table '{table_name}'")
            while still_running_while:
                response = table.scan(**parallel_params, **kwargs)
                result.extend(response["Items"])
                still_running_while = 'LastEvaluatedKey' in response

            return result

        if is_parallel_scan:
            with ThreadPoolExecutor(max_workers=threads_num) as executor:
                threads = [executor.submit(_scan_table, part_idx) for part_idx in range(threads_num)]
                scan_result = [thread.result() for thread in threads]
            return list(chain(*scan_result)) if len(scan_result) > 1 else scan_result
        return _scan_table()

    def batch_write_actions(self, node,  # pylint:disable=too-many-arguments,dangerous-default-value
                            table_name=consts.TABLE_NAME, new_items=None, delete_items=None,
                            schema=schemas.HASH_SCHEMA):
        dynamodb_api = self.get_dynamodb_api(node=node)
        assert new_items or delete_items, "should pass new_items or delete_items, other it's a no-op"
        new_items, delete_items = new_items or [], delete_items or []
        if new_items:
            LOGGER.debug(f"Adding new {len(new_items)} items to table '{table_name}'..")
        if delete_items:
            LOGGER.debug(f"Deleting {len(delete_items)} items from table '{table_name}'..")

        table = dynamodb_api.resource.Table(name=table_name)
        with table.batch_writer() as batch:
            for item in new_items:
                batch.put_item(item)
            if delete_items:
                table_keys = [key["AttributeName"] for key in schema["KeySchema"]]
                for item in delete_items:
                    batch.delete_item({key: item[key] for key in table_keys})
        return table

    def compare_table_data(self, node, table_data, table_name=consts.TABLE_NAME) -> bool:
        data = self.scan_table(node=node, table_name=table_name)
        return set(hash(tuple(str(xxx))) for xxx in table_data) == set(hash(tuple(str(xxx))) for xxx in data)

    def is_table_exists(self, node, table_name: consts.TABLE_NAME, endpoint_url=None):
        dynamodb_api = self.get_dynamodb_api(node=node)
        is_table_exists = True

        try:
            dynamodb_api.client.describe_table(TableName=table_name)
        except dynamodb_api.client.exceptions.ResourceNotFoundException:
            is_table_exists = False
        LOGGER.info(f"The table '{table_name}'{'' if is_table_exists else 'not'} exists in endpoint {endpoint_url}..")
        return is_table_exists

    def delete_table(self, node, table_name: consts.TABLE_NAME, wait_until_table_removed=True):
        dynamodb_api = self.get_dynamodb_api(node=node)
        table = dynamodb_api.resource.Table(name=table_name)
        table.delete()
        if wait_until_table_removed:
            waiter = dynamodb_api.client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
            LOGGER.info(f"The '{table_name}' table successfully removed")
        else:
            LOGGER.info(f"Send request to removed '{table_name}' table")
