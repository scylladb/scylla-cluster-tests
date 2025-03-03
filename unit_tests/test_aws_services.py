from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest
import boto3
# we need to set the environment variable before importing moto
# otherwise it won't pick it up
os.environ['MOTO_AMIS_PATH'] = 'test_data/mocked_ami_data.json'
from moto.server import ThreadedMotoServer

from sdcm.keystore import KeyStore
from sdcm.utils.aws_region import AwsRegion
from sdcm.sct_provision.common.layout import SCTProvisionLayout, create_sct_configuration
from sdcm.utils.common import get_scylla_ami_versions


AWS_REGION = "us-east-1"


@pytest.fixture(scope="session", autouse=True)
def fixture_get_real_keys():
    KeyStore().sync(keys=['scylla-qa-ec2', 'scylla-test', 'scylla_test_id_ed25519', 'scylla_test_id_ed25519.pub'],
                    local_path=Path('~/.ssh/').expanduser(), permissions=0o0600)


@pytest.fixture(scope="module", autouse=True)
def moto_server():
    """Fixture to run a mocked AWS server for testing."""
    # Note: pass `port=0` to get a random free port.
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    aws_endpoint_url = f"http://{host}:{port}"

    # this is a bit tricky with other unittest (if we run in parallel, which we currently don't)
    # it might break some actually tests, we should consider moving as session based fixture just for
    # blocking unittests from touching actual AWS service
    os.environ['AWS_ENDPOINT_URL'] = aws_endpoint_url
    yield aws_endpoint_url
    del os.environ['AWS_ENDPOINT_URL']
    server.stop()


@pytest.fixture(scope="module", autouse=True)
def keystore_configure(moto_server):
    s3 = boto3.resource(
        service_name='s3',
        region_name=AWS_REGION,
        endpoint_url=moto_server
    )

    bucket = s3.Bucket('scylla-qa-keystore')
    bucket.create()
    bucket.put_object(Key='gcp-sct-project-1.json', Body=b'{}')
    bucket.put_object(Key='aws_images_role.json', Body=b'{"role_arn": "arn:aws:iam::123456789012:role/role-name", '
                                                       b'"role_session_name": "role-session-name"}')
    for file in ('scylla-qa-ec2', 'scylla-test', 'scylla_test_id_ed25519', 'scylla_test_id_ed25519.pub'):
        bucket.upload_file(Filename=str(Path('~/.ssh').expanduser() / file), Key=file)

    # TODO: check if we can pass tag straight to json in MOTO_AMIS_PATH
    # TODO: implement https://github.com/getmoto/moto/issues/8517 for that
    ami_id = 'ami-760aaa0f'
    tags = [
        {'Key': 'scylla_version', 'Value': '1.2.3'},
        {'Key': 'user_data_format_version', 'Value': '3'},
    ]
    ec2 = boto3.client('ec2', region_name=AWS_REGION, endpoint_url=moto_server)

    ec2.create_tags(
        Resources=[ami_id],
        Tags=tags
    )

    # we need to set the environment variable before importing moto
    SCYLLA_AMI_OWNER_ID_LIST = ("797456418907", "158855661827")
    amis = ec2.describe_images(
        Owners=SCYLLA_AMI_OWNER_ID_LIST,
        Filters=[{"Name": "is-public", "Values": ["true"]},
                 {"Name": "name", "Values": ["ScyllaDB*2024.2*"]}]
    )
    for ami in amis["Images"]:
        tags = [
            {'Key': 'scylla_version', 'Value': '2024.2.0'},
            {'Key': 'user_data_format_version', 'Value': '3'},
            {'Key': 'environment', 'Value': 'production'},
        ]
        ec2.create_tags(
            Resources=[ami["ImageId"]],
            Tags=tags
        )


@pytest.fixture(scope="module")
def aws_region(keystore_configure) -> AwsRegion:
    # we need all of SCT info configured in moto
    # hence we configure the region first
    _aws_region = AwsRegion(region_name=AWS_REGION)
    _aws_region.configure()
    return _aws_region


def test_01_keystore() -> None:
    """Test the s3 keystore functionality."""
    k = KeyStore()
    assert k.get_gcp_credentials() == {}


def test_02_keystore_sync(tmp_path) -> None:
    """
    Validate the sync is working and setting right permissions.
    """
    k = KeyStore()
    k.sync(keys=['scylla-qa-ec2', 'scylla-test', 'scylla_test_id_ed25519'],
           local_path=tmp_path, permissions=0o0600)

    for file in tmp_path.iterdir():
        assert stat.S_IMODE(file.stat().st_mode) == 0o600


def test_03_provision(aws_region: AwsRegion) -> None:

    # test AWS provision flow

    # TODO: switch all this to configuration yaml, it would be clear and easier to maintain
    os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
    os.environ['SCT_REGION_NAME'] = 'us-east-1'
    os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-760aaa0f'
    os.environ['SCT_INSTANCE_TYPE_DB'] = 'm5.xlarge'
    os.environ['SCT_N_DB_NODES'] = '3'
    os.environ['SCT_N_MONITORS_NODES'] = '1'
    os.environ['SCT_N_LOADERS'] = '1'
    os.environ['SCT_LOGS_TRANSPORT'] = 'ssh'
    params = create_sct_configuration('test_04_provision')

    layout = SCTProvisionLayout(params=params)
    layout.provision()

    # TODO: add some checks here, like checking if the instances are existing running in moto


def test_04_get_scylla_ami_versions() -> None:
    amis = get_scylla_ami_versions(region_name=AWS_REGION, version="all")

    assert {ami.id for ami in amis} == {
        'ami-0e5174dc58971bf48',
        'ami-0fc20c0763e9dd04d',
        'ami-01b7d077069a32b52',
        'ami-019677598edc8d6ad',
        'ami-070ddae2f97d07e70',
        'ami-050a1c9be69353d38',
        'ami-0c4b2f47376303de9',
        'ami-0087d94f8bcf03c47'
    }
