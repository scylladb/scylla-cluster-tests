import json

from moto.ec2.utils import gen_moto_amis

from sdcm.utils.aws_utils import get_scylla_images_ec2_client
# generate moto AMIs file, base on actual Scylla AMIs
# used to generate `unit_tests/test_data/mocked_ami_data.json`

# client = boto3.client("ec2", region_name="us-east-1")
client = get_scylla_images_ec2_client("us-east-1")
SCYLLA_AMI_OWNER_ID_LIST = ("797456418907", "158855661827")
test = client.describe_images(
    Owners=SCYLLA_AMI_OWNER_ID_LIST,
    Filters=[{"Name": "is-public", "Values": ["true"]},
             {"Name": "name", "Values": ["ScyllaDB*2024.2*"]}]
)

result = gen_moto_amis(test["Images"])

print(json.dumps(result, indent=2))
