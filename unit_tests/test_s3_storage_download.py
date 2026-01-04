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
import tempfile
from unittest.mock import Mock, patch

import pytest
import requests
from botocore.exceptions import ClientError

from sdcm.utils.common import S3Storage
from sdcm.keystore import KeyStore


class TestS3StorageDownloadFile:
    """Tests for S3Storage.download_file method with retry and error handling."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def mock_keystore(self):
        """Mock KeyStore to return test credentials."""
        with patch.object(KeyStore, "get_argus_rest_credentials_per_provider") as mock_creds:
            mock_creds.return_value = {"token": "test-token", "extra_headers": {}}
            yield mock_creds

    def test_download_direct_s3_link_success(self, temp_dir):
        """Test successful download with direct S3 link."""
        s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/test-file.tar.zst"

        mock_bucket = Mock()
        mock_bucket.download_file = Mock()

        with patch("boto3.resource") as mock_boto3:
            mock_boto3.return_value.Bucket.return_value = mock_bucket

            # Create a test file to simulate successful download
            test_file = os.path.join(temp_dir, "test-file.tar.zst")
            with open(test_file, "w") as f:
                f.write("test content")

            storage = S3Storage()
            result = storage.download_file(s3_link, temp_dir)

            assert result == os.path.join(os.path.abspath(temp_dir), "test-file.tar.zst")
            mock_bucket.download_file.assert_called_once()

    def test_download_argus_link_success(self, temp_dir, mock_keystore):
        """Test successful download with Argus proxy link that redirects to S3."""
        argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"
        s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/file.tar.zst?signature=xyz"

        # Mock requests.head to simulate successful Argus redirect
        mock_response = Mock()
        mock_response.history = [Mock()]
        mock_response.history[0].headers = {"location": s3_link}
        mock_response.status_code = 200

        mock_bucket = Mock()
        mock_bucket.download_file = Mock()

        with patch("requests.head", return_value=mock_response):
            with patch("boto3.resource") as mock_boto3:
                mock_boto3.return_value.Bucket.return_value = mock_bucket

                # Create a test file to simulate successful download
                test_file = os.path.join(temp_dir, "file.tar.zst")
                with open(test_file, "w") as f:
                    f.write("test content")

                storage = S3Storage()
                result = storage.download_file(argus_link, temp_dir)

                assert result == os.path.join(os.path.abspath(temp_dir), "file.tar.zst")
                mock_bucket.download_file.assert_called_once()

    def test_download_argus_link_no_redirect_fails(self, temp_dir, mock_keystore):
        """Test failure when Argus doesn't redirect (e.g., Cloudflare login page)."""
        argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

        # Mock requests.head to simulate no redirect (Cloudflare login page)
        mock_response = Mock()
        mock_response.history = []  # No redirect occurred
        mock_response.status_code = 200
        mock_response.url = "https://scylladb.cloudflareaccess.com/cdn-cgi/access/login/argus.scylladb.com"

        with patch("requests.head", return_value=mock_response):
            with patch("boto3.resource"):
                storage = S3Storage()

                with pytest.raises(RuntimeError) as exc_info:
                    storage.download_file(argus_link, temp_dir)

                assert "Argus communication failed" in str(exc_info.value)
                assert "no redirect occurred" in str(exc_info.value)

    def test_download_argus_link_no_location_header_fails(self, temp_dir, mock_keystore):
        """Test failure when Argus redirect has no location header."""
        argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

        # Mock requests.head to simulate redirect without location header
        mock_response = Mock()
        mock_response.history = [Mock()]
        mock_response.history[0].headers = {}  # No location header
        mock_response.status_code = 200

        with patch("requests.head", return_value=mock_response):
            with patch("boto3.resource"):
                storage = S3Storage()

                with pytest.raises(RuntimeError) as exc_info:
                    storage.download_file(argus_link, temp_dir)

                assert "Argus redirect failed" in str(exc_info.value)
                assert "no location header found" in str(exc_info.value)

    def test_download_argus_request_exception_fails(self, temp_dir, mock_keystore):
        """Test failure when request to Argus raises an exception."""
        argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

        with patch("requests.head", side_effect=requests.exceptions.ConnectionError("Connection failed")):
            with patch("boto3.resource"):
                storage = S3Storage()

                with pytest.raises(RuntimeError) as exc_info:
                    storage.download_file(argus_link, temp_dir)

                assert "Failed to communicate with Argus" in str(exc_info.value)

    def test_download_s3_client_error_fails(self, temp_dir):
        """Test failure when S3 download raises ClientError (e.g., 404)."""
        s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/missing-file.tar.zst"

        mock_bucket = Mock()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_bucket.download_file.side_effect = ClientError(error_response, "HeadObject")

        with patch("boto3.resource") as mock_boto3:
            mock_boto3.return_value.Bucket.return_value = mock_bucket

            storage = S3Storage()

            with pytest.raises(RuntimeError) as exc_info:
                storage.download_file(s3_link, temp_dir)

            assert "Failed to download file" in str(exc_info.value)
            assert "from S3 bucket" in str(exc_info.value)

    def test_download_invalid_link_no_filename_fails(self, temp_dir):
        """Test failure when S3 link doesn't contain a valid filename."""
        s3_link = "https://cloudius-jenkins-test.s3.amazonaws.com/"

        with patch("boto3.resource"):
            storage = S3Storage()

            with pytest.raises(RuntimeError) as exc_info:
                storage.download_file(s3_link, temp_dir)

            assert "Invalid S3 link" in str(exc_info.value)
            assert "could not extract file name" in str(exc_info.value)

    def test_download_with_retry_on_argus_failure(self, temp_dir, mock_keystore):
        """Test that download retries on Argus failures."""
        argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

        # First two attempts fail with no redirect, third succeeds
        mock_response_fail = Mock()
        mock_response_fail.history = []
        mock_response_fail.status_code = 200
        mock_response_fail.url = "https://scylladb.cloudflareaccess.com/cdn-cgi/access/login/argus.scylladb.com"

        mock_response_success = Mock()
        mock_response_success.history = [Mock()]
        mock_response_success.history[0].headers = {
            "location": "https://cloudius-jenkins-test.s3.amazonaws.com/test-path/file.tar.zst"
        }
        mock_response_success.status_code = 200

        mock_bucket = Mock()
        mock_bucket.download_file = Mock()

        with patch(
            "requests.head",
            side_effect=[
                mock_response_fail,  # First attempt fails
                mock_response_fail,  # Second attempt fails
                mock_response_success,  # Third attempt succeeds
            ],
        ):
            with patch("boto3.resource") as mock_boto3:
                mock_boto3.return_value.Bucket.return_value = mock_bucket

                # Create a test file to simulate successful download
                test_file = os.path.join(temp_dir, "file.tar.zst")
                with open(test_file, "w") as f:
                    f.write("test content")

                storage = S3Storage()
                result = storage.download_file(argus_link, temp_dir)

                assert result == os.path.join(os.path.abspath(temp_dir), "file.tar.zst")
                mock_bucket.download_file.assert_called_once()

    def test_download_retries_exhausted_fails(self, temp_dir, mock_keystore):
        """Test that download fails after all retries are exhausted."""
        argus_link = "https://argus.scylladb.com/api/v1/tests/test-id/log/file.tar.zst/download"

        # All attempts fail with no redirect
        mock_response = Mock()
        mock_response.history = []
        mock_response.status_code = 200
        mock_response.url = "https://scylladb.cloudflareaccess.com/cdn-cgi/access/login/argus.scylladb.com"

        with patch("requests.head", return_value=mock_response):
            with patch("boto3.resource"):
                storage = S3Storage()

                with pytest.raises(RuntimeError) as exc_info:
                    storage.download_file(argus_link, temp_dir)

                assert "Argus communication failed" in str(exc_info.value)
