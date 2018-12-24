import boto3
import json

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"


class KeyStore(object):
    def __init__(self):
        self.s3 = boto3.resource("s3")

    def _get_json(self, json_file):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, json_file)
        return json.loads(obj.get()["Body"].read())

    def get_scylladb_upload_credentials(self):
        return self._get_json("scylladb_upload.json")
