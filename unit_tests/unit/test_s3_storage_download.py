# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2025 ScyllaDB

import os
from unittest.mock import Mock, patch

import pytest
import requests
from botocore.exceptions import ClientError

from sdcm.keystore import KeyStore
from sdcm.utils.common import S3Storage
from sdcm.utils.decorators import retrying


@pytest.fixture
def mock_keystore():
    """Mock KeyStore to return test credentials."""
    with patch.object(KeyStore, "get_argus_rest_credentials_per_provider") as mock_creds:
        mock_creds.return_value = {"token": "test-token", "extra_headers": {}}
        yield mock_creds


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch):
    """Override S3Storage.download_file to use minimal retry times for tests."""

    # Get the original undecorated method
    original_download = (
        S3Storage.download_file.__wrapped__
        if hasattr(S3Storage.download_file, "__wrapped__")
        else S3Storage.download_file
    )

    # Apply fast retry decorator (3 retries with 0.01s delay to support tests with 2 failures + 1 success)
    @retrying(n=3, sleep_time=0.01, message="Downloading file from S3")
    def fast_download_file(self, link, dst_dir):
        # Call the original function logic by getting it from the class
        return original_download(self, link, dst_dir)

    # Monkey patch the method on the class
    monkeypatch.setattr(S3Storage, "download_file", fast_download_file)
    yield


def mock_redirect_response(location=None, status_code=302, url=None):
    """Build a mocked non-followed response, as returned by an un-redirected GET."""
    response = Mock()
    response.headers = {"location": location} if location else {}
    response.status_code = status_code
    response.url = url or "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    return response


def test_download_direct_s3_link_success(tmp_path):
    """Test successful download with direct S3 link."""
    s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/test-file.tar.zst"

    mock_bucket = Mock()
    mock_bucket.download_file = Mock()

    with patch("boto3.resource") as mock_boto3:
        mock_boto3.return_value.Bucket.return_value = mock_bucket

        # Create a test file to simulate successful download
        test_file = tmp_path / "test-file.tar.zst"
        test_file.write_text("test content")

        storage = S3Storage()
        result = storage.download_file(s3_link, str(tmp_path))

        assert result == os.path.join(os.path.abspath(str(tmp_path)), "test-file.tar.zst")
        mock_bucket.download_file.assert_called_once()


def test_download_argus_link_success(tmp_path, mock_keystore):
    """Test successful download with Argus proxy link that redirects to S3."""
    argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"
    s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/file.tar.zst?signature=xyz"

    mock_response = mock_redirect_response(location=s3_link)

    mock_bucket = Mock()
    mock_bucket.download_file = Mock()

    with patch("requests.get", return_value=mock_response):
        with patch("boto3.resource") as mock_boto3:
            mock_boto3.return_value.Bucket.return_value = mock_bucket

            # Create a test file to simulate successful download
            test_file = tmp_path / "file.tar.zst"
            test_file.write_text("test content")

            storage = S3Storage()
            result = storage.download_file(argus_link, str(tmp_path))

            assert result == os.path.join(os.path.abspath(str(tmp_path)), "file.tar.zst")
            mock_bucket.download_file.assert_called_once()


def test_download_argus_link_no_redirect_fails(tmp_path, mock_keystore):
    """Test failure when Argus answers a page instead of a redirect (e.g. Cloudflare login)."""
    argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

    mock_response = mock_redirect_response(
        status_code=200,
        url="https://scylladb.cloudflareaccess.com/cdn-cgi/access/login/argus.scylladb.com",
    )

    with patch("requests.get", return_value=mock_response):
        with patch("boto3.resource"):
            storage = S3Storage()

            with pytest.raises(RuntimeError) as exc_info:
                storage.download_file(argus_link, str(tmp_path))

            assert "Argus communication failed" in str(exc_info.value)
            assert "no redirect returned" in str(exc_info.value)


def test_download_argus_link_uses_get_not_head(tmp_path, mock_keystore):
    """Argus answers 405 to HEAD since the FastAPI migration, so the link must be resolved with GET."""
    argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"
    s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/file.tar.zst?signature=xyz"

    mock_bucket = Mock()
    mock_bucket.download_file = Mock()

    with patch("requests.get", return_value=mock_redirect_response(location=s3_link)) as mock_get:
        with patch("requests.head", side_effect=AssertionError("HEAD is answered with 405 by Argus")):
            with patch("boto3.resource") as mock_boto3:
                mock_boto3.return_value.Bucket.return_value = mock_bucket

                (tmp_path / "file.tar.zst").write_text("test content")

                storage = S3Storage()
                storage.download_file(argus_link, str(tmp_path))

    assert mock_get.call_args.kwargs["allow_redirects"] is False
    assert mock_get.call_args.kwargs["stream"] is True


def test_download_argus_request_exception_fails(tmp_path, mock_keystore):
    """Test failure when request to Argus raises an exception."""
    argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

    with patch("requests.get", side_effect=requests.exceptions.ConnectionError("Connection failed")):
        with patch("boto3.resource"):
            storage = S3Storage()

            with pytest.raises(RuntimeError) as exc_info:
                storage.download_file(argus_link, str(tmp_path))

            assert "Failed to communicate with Argus" in str(exc_info.value)


def test_download_s3_client_error_fails(tmp_path):
    """Test failure when S3 download raises ClientError (e.g., 404)."""
    s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/missing-file.tar.zst"

    mock_bucket = Mock()
    error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
    mock_bucket.download_file.side_effect = ClientError(error_response, "HeadObject")

    with patch("boto3.resource") as mock_boto3:
        mock_boto3.return_value.Bucket.return_value = mock_bucket

        storage = S3Storage()

        with pytest.raises(RuntimeError) as exc_info:
            storage.download_file(s3_link, str(tmp_path))

        assert "Failed to download file" in str(exc_info.value)
        assert "from S3 bucket" in str(exc_info.value)


def test_download_invalid_link_no_filename_fails(tmp_path):
    """Test failure when S3 link doesn't contain a valid filename."""
    s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/"

    with patch("boto3.resource"):
        storage = S3Storage()

        with pytest.raises(RuntimeError) as exc_info:
            storage.download_file(s3_link, str(tmp_path))

        assert "Invalid S3 link" in str(exc_info.value)
        assert "could not extract file name" in str(exc_info.value)


def test_download_with_retry_on_argus_failure(tmp_path, mock_keystore):
    """Test that download retries on Argus failures."""
    argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

    # First two attempts fail with no redirect, third succeeds
    mock_response_fail = mock_redirect_response(
        status_code=200,
        url="https://scylladb.cloudflareaccess.com/cdn-cgi/access/login/argus.scylladb.com",
    )
    mock_response_success = mock_redirect_response(
        location="https://cloudius-jenkins-test.s3.amazonaws.com/test-path/file.tar.zst"
    )

    mock_bucket = Mock()
    mock_bucket.download_file = Mock()

    with patch(
        "requests.get",
        side_effect=[
            mock_response_fail,  # First attempt fails
            mock_response_fail,  # Second attempt fails
            mock_response_success,  # Third attempt succeeds
        ],
    ):
        with patch("boto3.resource") as mock_boto3:
            mock_boto3.return_value.Bucket.return_value = mock_bucket

            # Create a test file to simulate successful download
            test_file = tmp_path / "file.tar.zst"
            test_file.write_text("test content")

            storage = S3Storage()
            result = storage.download_file(argus_link, str(tmp_path))

            assert result == os.path.join(os.path.abspath(str(tmp_path)), "file.tar.zst")
            mock_bucket.download_file.assert_called_once()


def test_download_retries_exhausted_fails(tmp_path, mock_keystore):
    """Test that download fails after all retries are exhausted."""
    argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

    # All attempts fail with no redirect
    mock_response = mock_redirect_response(
        status_code=200,
        url="https://scylladb.cloudflareaccess.com/cdn-cgi/access/login/argus.scylladb.com",
    )

    with patch("requests.get", return_value=mock_response):
        with patch("boto3.resource"):
            storage = S3Storage()

            with pytest.raises(RuntimeError) as exc_info:
                storage.download_file(argus_link, str(tmp_path))

            assert "Argus communication failed" in str(exc_info.value)
