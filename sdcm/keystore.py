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
# Copyright (c) 2020 ScyllaDB

import os
import json
import time
import hashlib
import logging
import tempfile
import threading
import configparser
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Iterator
from concurrent.futures.thread import ThreadPoolExecutor
from collections import namedtuple

import boto3
import paramiko
import tenacity
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import S3ServiceResource
from botocore.exceptions import ClientError
from cloud_detect import provider

LOGGER = logging.getLogger(__name__)

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"

SSHKey = namedtuple("SSHKey", ["name", "public_key", "private_key"])

BOTO3_CLIENT_CREATION_LOCK = threading.Lock()

# S3 error codes that indicate transient failures worth retrying.
_TRANSIENT_S3_ERROR_CODES = frozenset({"SlowDown", "InternalError", "ServiceUnavailable"})

# Secrets Manager error codes that indicate transient failures worth retrying.
_TRANSIENT_SM_ERROR_CODES = frozenset({"ThrottlingException", "InternalServiceError"})


def _is_transient_error(exception):
    """Return True if the exception is a transient AWS error worth retrying."""
    if not isinstance(exception, ClientError):
        return False
    code = exception.response.get("Error", {}).get("Code")
    return code in _TRANSIENT_S3_ERROR_CODES or code in _TRANSIENT_SM_ERROR_CODES


class KeyStore:
    """Credential store backed by S3 or AWS Secrets Manager.

    Fetches credentials (JSON configs, SSH keys, API tokens) from a
    configurable backend.  Results are cached in a thread-safe dict for
    the lifetime of the instance.

    Backend selection is controlled by the ``SCT_KEYSTORE_BACKEND``
    environment variable (``s3`` by default, ``secretsmanager`` for
    AWS Secrets Manager).

    Use :func:`get_keystore` for a shared singleton instance that
    maximises cache hits across call sites.
    """

    def __init__(self):
        self._cache: dict[str, bytes] = {}
        self._cache_lock = threading.Lock()
        self._backend = os.environ.get("SCT_KEYSTORE_BACKEND", "s3")
        self._sm_prefix = os.environ.get("SCT_KEYSTORE_SM_PREFIX", "sct/")

    @property
    def s3(self) -> S3ServiceResource:
        return boto3.resource("s3")

    @property
    def s3_client(self) -> S3Client:
        with BOTO3_CLIENT_CREATION_LOCK:
            return boto3.client("s3")

    @tenacity.retry(
        retry=tenacity.retry_if_exception(_is_transient_error),
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=4),
        reraise=True,
    )
    def _fetch_from_s3(self, file_name):
        """Fetch a file from S3 with automatic retry for transient errors."""
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, file_name)
        return obj.get()["Body"].read()

    @tenacity.retry(
        retry=tenacity.retry_if_exception(_is_transient_error),
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=4),
        reraise=True,
    )
    def _fetch_from_secrets_manager(self, file_name):
        """Fetch a secret from AWS Secrets Manager with automatic retry."""
        sm = boto3.client("secretsmanager")
        secret_name = f"{self._sm_prefix}{file_name}"
        resp = sm.get_secret_value(SecretId=secret_name)
        if "SecretBinary" in resp:
            return resp["SecretBinary"]
        return resp["SecretString"].encode("utf-8")

    def _fetch_content(self, file_name):
        """Fetch content from the configured backend."""
        if self._backend == "secretsmanager":
            return self._fetch_from_secrets_manager(file_name)
        return self._fetch_from_s3(file_name)

    def get_file_contents(self, file_name, *, bypass_cache=False):
        """Retrieve a file's raw bytes from the configured backend.

        Args:
            file_name: Key name in the keystore (e.g. ``email_config.json``).
            bypass_cache: When True, skip the in-memory cache and fetch
                fresh content from the backend.  The result still updates
                the cache for subsequent calls.

        Returns:
            Raw bytes of the file content.
        """
        if not bypass_cache:
            with self._cache_lock:
                if file_name in self._cache:
                    LOGGER.debug("cache hit for %s", file_name)
                    return self._cache[file_name]

        start = time.monotonic()
        result = self._fetch_content(file_name)
        elapsed = time.monotonic() - start

        if elapsed > 2:
            LOGGER.warning("slow fetch for %s (backend=%s): %.2fs", file_name, self._backend, elapsed)
        else:
            LOGGER.info("fetched %s (backend=%s) in %.2fs", file_name, self._backend, elapsed)

        with self._cache_lock:
            self._cache[file_name] = result

        return result

    def clear_cache(self):
        """Remove all cached credentials from the in-memory cache."""
        with self._cache_lock:
            self._cache.clear()

    def get_json(self, json_file):
        """Retrieve a JSON file from the keystore and return the parsed object."""
        # deepcode ignore replace~read~decode~json.loads: is done automatically
        return json.loads(self.get_file_contents(json_file))

    def download_file(self, filename, dest_filename):
        """Download a keystore file directly to disk, bypassing the cache.

        .. warning::
            This writes decrypted secret bytes to an arbitrary filesystem
            path. Callers are responsible for the destination's security:
            prefer :func:`materialize_ssh_key` for SSH keys (RAM-backed
            tmpfs + 0600 perms + cleanup); other callers must set 0600
            permissions and place the file on non-persistent storage
            whenever possible.

            CodeQL flags the ``write()`` below as
            ``py/clear-text-storage-sensitive-data``. This is intentional
            — external consumers (libssh2, ssh/scp/rsync, Ansible, Docker
            volume mounts, Jenkins builders) require keys on a
            filesystem. The finding must be dismissed as a false-positive
            in the GitHub Security tab by a maintainer.
        """
        with open(dest_filename, "wb") as file_obj:
            file_obj.write(self._fetch_content(filename))

    def get_email_credentials(self):
        return self.get_json("email_config.json")

    def get_gcp_credentials(self):
        project = os.environ.get("SCT_GCE_PROJECT") or "gcp-sct-project-1"
        return self.get_json(f"{project}.json")

    def get_dbaaslab_gcp_credentials(self):
        return self.get_json("gcp-scylladbaaslab.json")

    def get_gcp_service_accounts(self):
        project = os.environ.get("SCT_GCE_PROJECT") or "gcp-sct-project-1"
        service_accounts = self.get_json(f"{project}_service_accounts.json")
        for sa in service_accounts:
            if "https://www.googleapis.com/auth/cloud-platform" not in sa["scopes"]:
                sa["scopes"].append("https://www.googleapis.com/auth/cloud-platform")
        return service_accounts

    def get_scylladb_upload_credentials(self):
        return self.get_json("scylladb_upload.json")

    def get_qa_users(self):
        return self.get_json("qa_users.json")

    def get_acl_grantees(self):
        return self.get_json("bucket-users.json")

    def get_ssh_key_pair(self, name):
        """Retrieve an SSH key pair (public + private) from the keystore.

        Args:
            name: Base name of the key (e.g. ``scylla_test_id_ed25519``).
                The public key is fetched as ``{name}.pub``.

        Returns:
            An :class:`SSHKey` namedtuple with *name*, *public_key*, and
            *private_key* fields (bytes).
        """
        return SSHKey(
            name=name,
            public_key=self.get_file_contents(file_name=f"{name}.pub"),
            private_key=self.get_file_contents(file_name=name),
        )

    def get_qa_ssh_keys(self):
        return [self.get_ssh_key_pair(name="scylla_test_id_ed25519")]

    def get_housekeeping_db_credentials(self):
        return self.get_json("housekeeping-db.json")

    def get_ldap_ms_ad_credentials(self):
        return self.get_json("ldap_ms_ad.json")

    def get_backup_azure_blob_credentials(self):
        return self.get_json("backup_azure_blob.json")

    def get_docker_hub_credentials(self):
        return self.get_json("docker.json")

    def get_azure_credentials(self):
        return self.get_json("azure.json")

    def get_oci_credentials(self) -> dict:
        """Get OCI credentials from S3 keystore or local ~/.oci/config file.

        Returns a dict with keys: tenancy, user, fingerprint, key_content, region, compartment_id.
        The key_content contains the PEM-encoded private key content.

        Priority:
        1. If SCT_OCI_CONFIG_PATH env var is set, use that file
        2. If ~/.oci/config exists, parse it and read the key file
        3. Fall back to S3 keystore oci.json
        """
        # Check for explicit config path override
        local_config_path = os.environ.get("SCT_OCI_CONFIG_PATH")

        if local_config_path and os.path.exists(local_config_path):
            return self._parse_local_oci_config(local_config_path)

        # Fall back to S3 keystore
        return self.get_json("oci.json")

    @staticmethod
    def _parse_local_oci_config(config_path: str, profile: str = "DEFAULT") -> dict:
        """Parse local OCI config file and return credentials dict.

        Args:
            config_path: Path to the OCI config file (typically ~/.oci/config)
            profile: The profile section to read from the config file

        Returns:
            Dict with tenancy, user, fingerprint, key_content, region, compartment_id
        """
        config = configparser.ConfigParser()
        config.read(config_path)

        if profile not in config:
            raise ValueError(f"Profile '{profile}' not found in OCI config file: {config_path}")

        section = config[profile]

        # Read the private key file content
        key_file_path = os.path.expanduser(section.get("key_file", ""))
        if not key_file_path or not os.path.exists(key_file_path):
            raise ValueError(f"OCI key file not found: {key_file_path}")

        with open(key_file_path, "r", encoding="utf-8") as key_file:
            key_content = key_file.read()

        return {
            "tenancy": section.get("tenancy"),
            "user": section.get("user"),
            "fingerprint": section.get("fingerprint"),
            "key_content": key_content,
            "region": section.get("region"),
            "compartment_id": section.get("compartment_id", section.get("tenancy")),  # default to tenancy if not set
        }

    def get_azure_kms_config(self):
        return self.get_json("azure_kms_config.json")

    def get_gcp_kms_config(self):
        return self.get_json("gcp_kms_config.json")

    def get_argus_rest_credentials_per_provider(self, cloud_provider: str | None = None):
        cloud_provider = cloud_provider or provider(timeout=0.5)

        if os.environ.get("JOB_NAME"):  # we are in Jenkins
            try:
                return self.get_json(f"argus_rest_credentials_sct_{cloud_provider}.json")
            except ClientError as e:
                if not e.response["Error"]["Code"] == "NoSuchKey":
                    raise

        return self.get_json("argus_rest_credentials.json")

    def get_baremetal_config(self, config_name: str):
        return self.get_json(f"{config_name}.json")

    def get_cloud_rest_credentials(self, environment: str = "lab"):
        return self.get_json(f"scylla_cloud_sct_api_creds_{environment}.json")

    def get_jira_credentials(self):
        return self.get_json("scylladb_jira.json")

    @staticmethod
    def calculate_s3_etag(file: BinaryIO, chunk_size=8 * 1024 * 1024):
        """Calculates the S3 custom e-tag (a specially formatted MD5 hash)"""
        md5s = []

        while True:
            data = file.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

        if len(md5s) == 1:
            return '"{}"'.format(md5s[0].hexdigest())

        digests = b"".join(m.digest() for m in md5s)
        digests_md5 = hashlib.md5(digests)
        return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

    def get_obj_if_needed(self, key, local_path, permissions):
        """Download an object to disk if the local copy is stale.

        For the S3 backend, staleness is detected via ETag.  For the Secrets
        Manager backend, a sidecar ``<file>.version`` file stores the remote
        VersionId so repeated ``sct.py`` / ``hydra`` invocations can skip
        the download when the secret has not been rotated.
        """
        path = os.path.join(local_path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        dl_flag = True
        if self._backend == "s3":
            tag = self.get_object_etag(key)
            try:
                with open(path, "rb") as file_obj:
                    if tag == self.calculate_s3_etag(file_obj):
                        dl_flag = False
            except FileNotFoundError:
                # Local cache file does not exist yet; keep dl_flag=True so we download it.
                pass
        else:  # secretsmanager
            remote_version = self.get_sm_version_id(key)
            version_path = f"{path}.version"
            if os.path.exists(path) and os.path.exists(version_path):
                try:
                    with open(version_path, "r", encoding="utf-8") as vf:
                        if vf.read().strip() == remote_version:
                            dl_flag = False
                except OSError:
                    # Sidecar version file is stale or unreadable; force a re-download.
                    pass

        if dl_flag:
            self.download_file(filename=key, dest_filename=path)
            os.chmod(path=path, mode=permissions)
            if self._backend == "secretsmanager":
                with open(f"{path}.version", "w", encoding="utf-8") as vf:
                    vf.write(self.get_sm_version_id(key))

    def sync(self, keys, local_path, permissions=0o777):
        """Syncs the local and remote copies from the configured backend."""
        with ThreadPoolExecutor(max_workers=len(keys)) as executor:
            args = [(key, local_path, permissions) for key in keys]
            list(executor.map(lambda p: self.get_obj_if_needed(*p), args))

    def get_object_etag(self, key):
        obj = self.s3_client.head_object(Bucket=KEYSTORE_S3_BUCKET, Key=key)
        return obj.get("ETag")

    def get_sm_version_id(self, key):
        """Return the current VersionId of a Secrets Manager secret."""
        sm = boto3.client("secretsmanager")
        secret_name = f"{self._sm_prefix}{key}"
        resp = sm.describe_secret(SecretId=secret_name)
        # VersionIdsToStages maps VersionId -> [stage names]; AWSCURRENT marks the live version
        for version_id, stages in resp.get("VersionIdsToStages", {}).items():
            if "AWSCURRENT" in stages:
                return version_id
        return ""


# Linux tmpfs mount; RAM-backed so secrets never hit persistent storage.
_TMPFS_PATH = Path("/dev/shm")


@contextmanager
def materialize_ssh_key(name: str) -> Iterator[Path]:
    """Write an SSH private key from the keystore to RAM-backed tmpfs.

    Yields a :class:`Path` to a temp file containing the private key bytes
    with ``0600`` permissions. The file is placed on ``/dev/shm`` (Linux
    tmpfs, RAM-only) when available, so the key never touches persistent
    storage; it falls back to the system temp dir otherwise. The file is
    removed on context exit.

    Args:
        name: Key name in the keystore (e.g. ``scylla-qa-ec2``). Resolved
            via :meth:`KeyStore.get_ssh_key_pair`.

    Yields:
        Path to the materialised key file. The path is valid only inside
        the ``with`` block — consumers that need the key for longer must
        manage lifetime themselves (e.g. via :class:`contextlib.ExitStack`).

    Example:
        >>> with materialize_ssh_key("scylla-qa-ec2") as key_path:
        ...     subprocess.run(["ssh", "-i", str(key_path), "user@host"])
    """
    tmpfs_dir = str(_TMPFS_PATH) if _TMPFS_PATH.is_dir() else None
    fh = tempfile.NamedTemporaryFile(dir=tmpfs_dir, prefix=f"sct-ssh-{name}-", delete=False)
    try:
        os.fchmod(fh.fileno(), 0o600)
        fh.write(KeyStore().get_ssh_key_pair(name).private_key)
        fh.flush()
        path = Path(fh.name)
    finally:
        fh.close()
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


def pub_key_from_private_key_file(key_file):
    try:
        return paramiko.rsakey.RSAKey.from_private_key_file(os.path.expanduser(key_file)).get_base64(), "ssh-rsa"
    except paramiko.ssh_exception.SSHException:
        return paramiko.ed25519key.Ed25519Key.from_private_key_file(
            os.path.expanduser(key_file)
        ).get_base64(), "ssh-ed25519"
