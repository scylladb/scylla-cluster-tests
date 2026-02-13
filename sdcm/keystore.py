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

import configparser
import hashlib
import json
import os
import threading
from collections import namedtuple
from concurrent.futures.thread import ThreadPoolExecutor
from typing import BinaryIO

import boto3
import paramiko
from botocore.exceptions import ClientError
from cloud_detect import provider
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.service_resource import S3ServiceResource

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"

SSHKey = namedtuple("SSHKey", ["name", "public_key", "private_key"])

BOTO3_CLIENT_CREATION_LOCK = threading.Lock()


class KeyStore:
    @property
    def s3(self) -> S3ServiceResource:
        return boto3.resource("s3")

    @property
    def s3_client(self) -> S3Client:
        with BOTO3_CLIENT_CREATION_LOCK:
            return boto3.client("s3")

    def get_file_contents(self, file_name):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, file_name)
        return obj.get()["Body"].read()

    def get_json(self, json_file):
        # deepcode ignore replace~read~decode~json.loads: is done automatically
        return json.loads(self.get_file_contents(json_file))

    def download_file(self, filename, dest_filename):
        with open(dest_filename, "wb") as file_obj:
            file_obj.write(self.get_file_contents(filename))

    def get_email_credentials(self):
        return self.get_json("email_config.json")

    def get_elasticsearch_token(self) -> dict[str, str]:
        conf_dict = self.get_json("es_token.json")
        conf_dict.pop("kibana_url", None)
        conf_dict.pop("es_url", None)
        return conf_dict

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
        return SSHKey(
            name=name,
            public_key=self.get_file_contents(file_name=f"{name}.pub"),
            private_key=self.get_file_contents(file_name=name),
        )

    def get_ec2_ssh_key_pair(self):
        return self.get_ssh_key_pair(name="scylla_test_id_ed25519")

    def get_gce_ssh_key_pair(self):
        return self.get_ssh_key_pair(name="scylla_test_id_ed25519")

    def get_azure_ssh_key_pair(self):
        return self.get_ssh_key_pair(name="scylla_test_id_ed25519")

    def get_oci_ssh_key_pair(self):
        return self.get_ssh_key_pair(name="scylla_test_id_ed25519")

    def get_qa_ssh_keys(self):
        return [
            self.get_ec2_ssh_key_pair(),
            self.get_gce_ssh_key_pair(),
        ]

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
        """Downloads an object at key to file path, checking to see if an existing file matches the current hash"""
        tag = self.get_object_etag(key)
        path = os.path.join(local_path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        dl_flag = True
        try:
            with open(path, "rb") as file_obj:
                if tag == self.calculate_s3_etag(file_obj):
                    dl_flag = False
        except FileNotFoundError:
            pass

        if dl_flag:
            self.download_file(filename=key, dest_filename=path)
            os.chmod(path=path, mode=permissions)

    def sync(self, keys, local_path, permissions=0o777):
        """Syncs the local and remote S3 copies"""
        with ThreadPoolExecutor(max_workers=len(keys)) as executor:
            args = [(key, local_path, permissions) for key in keys]
            list(executor.map(lambda p: self.get_obj_if_needed(*p), args))

    def get_object_etag(self, key):
        obj = self.s3_client.head_object(Bucket=KEYSTORE_S3_BUCKET, Key=key)
        return obj.get("ETag")


def pub_key_from_private_key_file(key_file):
    try:
        return paramiko.rsakey.RSAKey.from_private_key_file(os.path.expanduser(key_file)).get_base64(), "ssh-rsa"
    except paramiko.ssh_exception.SSHException:
        return paramiko.ed25519key.Ed25519Key.from_private_key_file(
            os.path.expanduser(key_file)
        ).get_base64(), "ssh-ed25519"
