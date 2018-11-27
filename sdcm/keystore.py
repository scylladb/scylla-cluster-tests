import boto3
import json

KEYSTORE_S3_BUCKET = "scylla-qa-keystore"


class KeyStore(object):
    def __init__(self):
        self.s3 = boto3.resource("s3")

    def _get_json(self, json_file):
        obj = self.s3.Object(KEYSTORE_S3_BUCKET, json_file)
        return json.loads(obj.get()["Body"].read())

    def get_email_credentials(self):
        return self._get_json("email_config.json")

    def get_elasticsearch_credentials(self):
        return self._get_json("es.json")

    def get_qa_ssh_keys(self):
        # TODO
        pass
