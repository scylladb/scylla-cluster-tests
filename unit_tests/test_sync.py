import socket
from pathlib import Path

import pytest

from sdcm.keystore import KeyStore


@pytest.mark.integration
def test_multiple_sync_on_lots_of_files():
    """
    this start multiple client of boto3 in multiple thread,
    opening boto3 session isn't thread safe, so this test is to make sure our lock is working
    """
    def is_using_aws_mock():
        try:
            socket.gethostbyname("aws-mock.itself")
            return True
        except socket.gaierror:
            return False

    if not is_using_aws_mock():
        key_store = KeyStore()
        key_store.sync(keys=['scylla-qa-ec2', 'scylla-test', 'scylla_test_id_ed25519'] * 20,
                       local_path=Path('~/.ssh/').expanduser(), permissions=0o0600)
