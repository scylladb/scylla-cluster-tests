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
import hashlib
import threading
from typing import BinaryIO
from concurrent.futures.thread import ThreadPoolExecutor
from collections import namedtuple

import boto3
import paramiko
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
        with open(dest_filename, 'wb') as file_obj:
            file_obj.write(self.get_file_contents(filename))

    def get_email_credentials(self):
        return self.get_json("email_config.json")

    def get_elasticsearch_token(self) -> dict[str, str]:
        conf_dict = self.get_json("es_token.json")
        conf_dict.pop('kibana_url', None)
        conf_dict.pop('es_url', None)
        return conf_dict

    def get_gcp_credentials(self):
        project = os.environ.get('SCT_GCE_PROJECT') or 'gcp-sct-project-1'
        return self.get_json(f"{project}.json")

    def get_dbaaslab_gcp_credentials(self):
        return self.get_json("gcp-scylladbaaslab.json")

    def get_gcp_service_accounts(self):
        project = os.environ.get('SCT_GCE_PROJECT') or 'gcp-sct-project-1'
        service_accounts = self.get_json(f"{project}_service_accounts.json")
        for sa in service_accounts:
            if 'https://www.googleapis.com/auth/cloud-platform' not in sa['scopes']:
                sa['scopes'].append('https://www.googleapis.com/auth/cloud-platform')
        return service_accounts

    def get_scylladb_upload_credentials(self):
        return self.get_json("scylladb_upload.json")

    def get_qa_users(self):
        return self.get_json("qa_users.json")

    def get_acl_grantees(self):
        return self.get_json("bucket-users.json")

    def get_ssh_key_pair(self, name):
        return SSHKey(name=name,
                      public_key=self.get_file_contents(file_name=f"{name}.pub"),
                      private_key=self.get_file_contents(file_name=name))

    def get_ec2_ssh_key_pair(self):
        return self.get_ssh_key_pair(name="scylla_test_id_ed25519")

    def get_gce_ssh_key_pair(self):
        return self.get_ssh_key_pair(name="scylla_test_id_ed25519")

    def get_azure_ssh_key_pair(self):
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

    def get_azure_kms_config(self):
        return self.get_json("azure_kms_config.json")

    def get_gcp_kms_config(self):
        return self.get_json("gcp_kms_config.json")

    def get_argusdb_credentials(self):
        return self.get_json("argusdb_config_v2.json")

    def get_argus_rest_credentials(self):
        return self.get_json("argus_rest_credentials.json")

    def get_baremetal_config(self, config_name: str):
        return self.get_json(f"{config_name}.json")

    def get_cloud_rest_credentials(self, environment: str = "lab"):
        return self.get_json(f"scylla_cloud_sct_api_creds_{environment}.json")

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

        digests = b''.join(m.digest() for m in md5s)
        digests_md5 = hashlib.md5(digests)
        return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

    def get_obj_if_needed(self, key, local_path, permissions):
        """Downloads an object at key to file path, checking to see if an existing file matches the current hash"""
        tag = self.get_object_etag(key)
        path = os.path.join(local_path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        dl_flag = True
        try:
            with open(path, 'rb') as file_obj:
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
        return obj.get('ETag')


def pub_key_from_private_key_file(key_file):
    try:
        return paramiko.rsakey.RSAKey.from_private_key_file(os.path.expanduser(key_file)).get_base64(), "ssh-rsa"
    except paramiko.ssh_exception.SSHException:
        return paramiko.ed25519key.Ed25519Key.from_private_key_file(os.path.expanduser(key_file)).get_base64(), "ssh-ed25519"
