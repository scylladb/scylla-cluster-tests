import logging

import boto3
from botocore.errorfactory import ClientError

LOGGER = logging.getLogger(__name__)


def create_table(endpoint_url, dynamodb_primarykey_type):
    try:
        dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url)

        name = 'usertable'
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

    except ClientError as ex:
        LOGGER.warning(str(ex))
        assert 'already exists' in str(ex)
