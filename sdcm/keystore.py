import json

import boto3
from mypy_boto3_s3.service_resource import S3ServiceResource

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"


class KeyStore:
    def __init__(self):
        self.s3: S3ServiceResource = boto3.resource("s3")

    def get_json(self, json_file):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, json_file)
        return json.loads(obj.get()["Body"].read())

    def download_file(self, filename, dest_filename):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, filename)
        with open(dest_filename, 'w') as file_obj:
            file_obj.write(obj.get()["Body"].read().decode())

    def get_email_credentials(self):
        return self.get_json("email_config.json")

    def get_elasticsearch_credentials(self):
        return self.get_json("es.json")

    def get_gcp_credentials(self):
        return self.get_json("gcp.json")

    def get_scylladb_upload_credentials(self):
        return self.get_json("scylladb_upload.json")

    def get_qa_users(self):
        return self.get_json("qa_users.json")

    def get_qa_ssh_keys(self):
        # TODO
        pass

    def get_housekeeping_db_credentials(self):
        return self.get_json("housekeeping-db.json")
