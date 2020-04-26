import logging
from contextlib import contextmanager

import boto3
from botocore.errorfactory import ClientError

from sdcm.sct_events import EventsSeverityChangerFilter, YcsbStressEvent, PrometheusAlertManagerEvent, Severity


LOGGER = logging.getLogger(__name__)


def create_table(endpoint_url, test_params, table_name=None):
    dynamodb_primarykey_type = test_params.get('dynamodb_primarykey_type', 'HASH')
    write_isolation = test_params.get('alternator_write_isolation')

    try:
        dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)

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

        table = dynamodb.create_table(**params)

        waiter = table.meta.client.get_waiter('table_exists')
        waiter.config.delay = 1
        waiter.config.max_attempts = 100
        waiter.wait(TableName=name)

        if write_isolation:
            set_write_isolation(table, write_isolation)

    except ClientError as ex:
        LOGGER.warning(str(ex))
        assert 'already exists' in str(ex)


def set_write_isolation(table, isolation):
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
                                    severity=Severity.WARNING, extra_time_to_expiration=30):
        yield
