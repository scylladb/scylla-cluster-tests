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
from collections import namedtuple

import boto3
import paramiko
from mypy_boto3_s3.service_resource import S3ServiceResource

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"

SSHKey = namedtuple("SSHKey", ["name", "public_key", "private_key"])


class KeyStore:  # pylint: disable=too-many-public-methods
    def __init__(self):
        self.s3: S3ServiceResource = boto3.resource("s3")

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

    def get_elasticsearch_token(self):
        return self.get_json("es_token.json")

    def get_gcp_credentials(self):
        project = os.environ.get("SCT_GCE_PROJECT") or "gcp-sct-project-1"
        return self.get_json(f"{project}.json")

    def get_dbaaslab_gcp_credentials(self):
        return self.get_json("gcp-scylladbaaslab.json")

    def get_gcp_service_accounts(self):
        project = os.environ.get("SCT_GCE_PROJECT") or "gcp-sct-project-1"
        return self.get_json(f"{project}_service_accounts.json")

    def get_scylladb_upload_credentials(self):
        return self.get_json("scylladb_upload.json")

    def get_qa_users(self):
        return self.get_json("qa_users.json")

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

    def get_argusdb_credentials(self):
        return self.get_json("argusdb_config_v2.json")

    def get_argus_rest_credentials(self):
        return self.get_json("argus_rest_credentials.json")

    def get_baremetal_config(self, config_name: str):
        return self.get_json(f"{config_name}.json")


def pub_key_from_private_key_file(key_file):
    try:
        return paramiko.rsakey.RSAKey.from_private_key_file(os.path.expanduser(key_file)).get_base64(), "ssh-rsa"
    except paramiko.ssh_exception.SSHException:
        return paramiko.ed25519key.Ed25519Key.from_private_key_file(
            os.path.expanduser(key_file)
        ).get_base64(), "ssh-ed25519"
