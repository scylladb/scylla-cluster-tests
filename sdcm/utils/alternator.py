import logging
from contextlib import contextmanager
from enum import Enum

import boto3
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from botocore.errorfactory import ClientError

from sdcm.sct_events import EventsSeverityChangerFilter, YcsbStressEvent, PrometheusAlertManagerEvent, Severity


LOGGER = logging.getLogger(__name__)


class WriteIsolation(Enum):
    ALWAYS_USE_LWT = "always_use_lwt"
    FORBID_RMW = "forbid_rmw"
    ONLY_RMW_USES_LWT = "only_rmw_uses_lwt"
    UNSAFE_RMW = "unsafe_rmw"


def create_table(endpoint_url, test_params, table_name=None):
    dynamodb_primarykey_type = test_params.get('dynamodb_primarykey_type')
    write_isolation = test_params.get('alternator_write_isolation')

    try:
        aws_params = dict(endpoint_url=endpoint_url,
                          aws_access_key_id=test_params.get('alternator_access_key_id'),
                          aws_secret_access_key=test_params.get('alternator_secret_access_key')
                          )
        dynamodb_resource: DynamoDBServiceResource = boto3.resource('dynamodb', **aws_params)
        dynamodb_client: DynamoDBClient = boto3.client('dynamodb', **aws_params)

        name = table_name or 'usertable'
        params = dict()

        if dynamodb_primarykey_type == 'HASH_AND_RANGE':
            params = dict(TableName=name,
                          BillingMode='PAY_PER_REQUEST',
                          KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'},
                                     {'AttributeName': 'c', 'KeyType': 'RANGE'}
                                     ],
                          AttributeDefinitions=[
                              {'AttributeName': 'p', 'AttributeType': 'S'},
                              {'AttributeName': 'c', 'AttributeType': 'S'},
                          ])

        elif dynamodb_primarykey_type == 'HASH':
            params = dict(TableName=name,
                          BillingMode='PAY_PER_REQUEST',
                          KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                          AttributeDefinitions=[
                              {'AttributeName': 'p', 'AttributeType': 'S'}])

        table = dynamodb_resource.create_table(**params)

        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.config.delay = 1
        waiter.config.max_attempts = 100
        waiter.wait(TableName=name)

        if write_isolation:
            set_write_isolation(table, write_isolation)

    except ClientError as ex:
        LOGGER.warning(str(ex))
        assert 'already exists' in str(ex)


def set_table_write_isolation(table_name='usertable', isolation=None, endpoint_url=None):
    dynamodb_resource: DynamoDBServiceResource = boto3.resource('dynamodb', endpoint_url=endpoint_url)
    table = dynamodb_resource.Table(table_name)
    set_write_isolation(table, isolation)


def set_write_isolation(table: DynamoDBServiceResource.Table, isolation):
    isolation = isolation if not isinstance(isolation, WriteIsolation) else isolation.value
    got = table.meta.client.describe_table(TableName=table.name)['Table']
    arn = got['TableArn']
    tags = [
        {
            'Key': 'system:write_isolation',
            'Value': isolation
        }
    ]
    table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)


@contextmanager
def ignore_alternator_client_errors():
    """
    Suppress errors and alerts related to alternator YCSB client errors

    Ref: https://github.com/scylladb/scylla/issues/5802, since we don't control which client connected to each
    node (using DNS now), e might have client connect to a non working node in some cases (like when internal
    port 7000 is disconnected, which make this not to not be able to do LWT ops)

    :return: context manager of all those filter
    """
    with EventsSeverityChangerFilter(event_class=PrometheusAlertManagerEvent, regex=".*YCSBTooManyErrors.*",
                                     severity=Severity.WARNING, extra_time_to_expiration=60), \
        EventsSeverityChangerFilter(event_class=PrometheusAlertManagerEvent,
                                    regex=".*YCSBTooManyVerifyErrors.*",
                                    severity=Severity.WARNING, extra_time_to_expiration=60), \
        EventsSeverityChangerFilter(event_class=YcsbStressEvent, regex=r".*Cannot achieve consistency level.*",
                                    severity=Severity.WARNING, extra_time_to_expiration=30), \
        EventsSeverityChangerFilter(event_class=YcsbStressEvent, regex=r".*Operation timed out.*",
                                    severity=Severity.WARNING, extra_time_to_expiration=30):
        yield
