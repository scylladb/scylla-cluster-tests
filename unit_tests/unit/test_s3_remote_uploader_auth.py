"""SSH auth of upload_remote_files_directly_to_s3: key auth, or "none" auth for keyless lab access hosts."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils import s3_remote_uploader

SSH_INFO = {"hostname": "172.17.0.8", "port": 10192, "user": "jenkins"}


@pytest.fixture
def session():
    session = MagicMock()
    with (
        patch.object(s3_remote_uploader, "socket"),
        patch.object(s3_remote_uploader, "Session", return_value=session),
        patch.object(s3_remote_uploader.boto3, "client"),
        patch.object(s3_remote_uploader, "KeyStore"),
        patch.object(s3_remote_uploader, "SshOutAsFile"),
    ):
        session.open_session.return_value.read.side_effect = [(9, b"1G\ttotal\n"), (0, b"")]
        yield session


def upload(ssh_info):
    return s3_remote_uploader.upload_remote_files_directly_to_s3(ssh_info, ["/tmp/f"], "bucket", "key")


def test_keyless_host_uses_none_auth(session):
    session.userauth_authenticated.return_value = True

    assert upload(SSH_INFO) == "https://bucket.s3.amazonaws.com/key"
    session.userauth_list.assert_called_once_with("jenkins")
    session.userauth_publickey_fromfile.assert_not_called()


def test_keyless_host_refusing_none_auth_raises(session):
    session.userauth_authenticated.return_value = False

    with pytest.raises(ConnectionError, match="none' auth as jenkins"):
        upload(SSH_INFO)


def test_host_with_key_uses_the_key(session):
    upload(SSH_INFO | {"key_file": "/keys/id"})

    session.userauth_publickey_fromfile.assert_called_once_with(username="jenkins", privatekey="/keys/id")
    session.userauth_list.assert_not_called()
