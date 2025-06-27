from textwrap import dedent

import boto3

region_names = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "af-south-1": "Africa (Cape Town)",
    "ap-east-1": "Asia Pacific (Hong Kong)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "ap-northeast-3": "Asia Pacific (Osaka)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ca-central-1": "Canada (Central)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "Europe (London)",
    "eu-south-1": "Europe (Milan)",
    "eu-west-3": "Europe (Paris)",
    "eu-north-1": "Europe (Stockholm)",
    "me-south-1": "Middle East (Bahrain)",
    "sa-east-1": "South America (São Paulo)",
}


def main():
    for region in ["us-east-1", "us-west-2", "eu-west-1", "eu-west-2", "eu-north-1", "eu-central-1"]:
        # Create an EC2 client
        ec2 = boto3.client("ec2", region_name=region)

        # Set the name and owner for the AMI that we want to find
        name = "ubuntu/images/hvm-ssd/ubuntu-*-22.04-amd64-server-*"
        owners = ["099720109477"]  # Canonical's account ID

        # Use the describe_images method to retrieve a list of AMIs that match the specified criteria
        response = ec2.describe_images(Owners=owners, Filters=[{"Name": "name", "Values": [name]}])

        # Get a list of the AMIs
        amis = response["Images"]
        amis.sort(key=lambda x: x["CreationDate"])
        loader_image_name = "*scylla-qa-loader-ami-v19"
        owners = ["self"]
        response = ec2.describe_images(Owners=owners, Filters=[{"Name": "name", "Values": [loader_image_name]}])
        loader_ami = response["Images"][-1]

        # Print the AMI IDs
        print(
            dedent(f"""
            {region}: # {region_names[region]}
              ami_id_loader: '{loader_ami["ImageId"]}' # Loader dedicated AMI {loader_image_name[1:]}
              ami_id_monitor: '{amis[-1]["ImageId"]}' # {amis[-1]["Name"]} {amis[-1]["Description"]}
        """).strip()
        )


if __name__ == "__main__":
    main()
