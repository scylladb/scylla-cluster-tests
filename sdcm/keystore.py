import json

import boto3

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"


class KeyStore(object):
    def __init__(self):
        self.s3 = boto3.resource("s3")

    def _get_json(self, json_file):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, json_file)
        return json.loads(obj.get()["Body"].read())

    def download_file(self, filename, dest_filename):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, filename)
        with open(dest_filename, 'w') as file_obj:
            file_obj.write(obj.get()["Body"].read())

    def get_email_credentials(self):
        return self._get_json("email_config.json")

    def get_elasticsearch_credentials(self):
        return self._get_json("es.json")

    def get_gcp_credentials(self):
        return self._get_json("gcp.json")

    def get_scylladb_upload_credentials(self):
        return self._get_json("scylladb_upload.json")

    def get_qa_ssh_keys(self):
        # TODO
        pass
