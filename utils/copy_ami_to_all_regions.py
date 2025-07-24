import os
import sys
import click
import boto3
import json

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from sdcm.sct_config import SCTConfiguration


@click.command()
@click.option('--ami-id', required=False, help='Source AMI ID')
@click.option('--ami-name', required=False, help='Name for the copied AMI')
@click.option('--source-region', required=True, help='Source AWS region', default='us-east-1')
@click.option('--target-regions', required=True, help='Space-separated list of target regions', default=" ".join(SCTConfiguration.aws_supported_regions))
@click.option('--dry-run', is_flag=True, default=False, help='Show what would be done without making changes')
@click.option('--copy-permissions', is_flag=True, default=False, help='Copy launch permissions from source AMI to target AMIs')
def copy_ami_to_all_regions(ami_id, ami_name, source_region, target_regions, dry_run, copy_permissions):  # noqa: PLR0914
    """Copy an AMI to multiple regions and apply its tags, and wait for it to become available."""
    target_regions_list = target_regions.split()
    ec2_source = boto3.client('ec2', region_name=source_region)
    if not ami_id:
        # find the ami_id by name tag in the source region
        list_response = ec2_source.describe_images(
            Filters=[{'Name': 'name', 'Values': [ami_name]}],
        )
        images = list_response.get('Images', [])
        ami_id = next(iter([img['ImageId'] for img in images if img['Name'] == ami_name]), None)

        # search by tag if ami_id is still not found
        if not ami_id:
            list_response = ec2_source.describe_images(
                Filters=[{'Name': 'tag:Name', 'Values': [ami_name]}],
            )
            images = list_response.get('Images', [])
            ami_id = next(iter([img["ImageId"] for img in images if img["Name"] == ami_name]), None)
    # Retrieve tags of the source AMI
    tags_response = ec2_source.describe_tags(
        Filters=[{'Name': 'resource-id', 'Values': [ami_id]}]
    )
    tags = tags_response.get('Tags', [])
    tags_for_create = [{'Key': t['Key'], 'Value': t['Value']} for t in tags]
    tagged_name = next(iter([t['Value'] for t in tags if t['Key'] == 'Name']), None)
    ami_name = ami_name or tagged_name

    for target_region in target_regions_list:
        if dry_run:
            click.echo(
                f"[Dry Run] Would copy AMI '{ami_id}' from {source_region} to {target_region} with name '{ami_name}'")
            click.echo(f"[Dry Run] Would apply tags: {json.dumps(tags_for_create)} to the new AMI in {target_region}")
            if copy_permissions:
                click.echo(f"[Dry Run] Would copy launch permissions from {ami_id} to new AMI in {target_region}")
            continue

        ec2_target = boto3.client("ec2", region_name=target_region)

        # add a check to see if the AMI already exists in the target region
        # and continue if it does
        check_response = ec2_target.describe_images(
            Filters=[{'Name': 'name', 'Values': [ami_name]}],
            Owners=['self']
        )
        if check_response.get('Images'):
            click.echo(f"AMI exists already in {target_region}")
            new_ami_id = next(iter([img['ImageId']
                              for img in check_response['Images'] if img['Name'] == ami_name]), None)
        else:
            click.echo(f"Copying AMI to {target_region}")
            copy_response = ec2_target.copy_image(
                Name=ami_name,
                SourceRegion=source_region,
                SourceImageId=ami_id
            )
            new_ami_id = copy_response["ImageId"]

            click.echo("Waiting for the new AMI to become available...")
            waiter = ec2_target.get_waiter("image_available")
            waiter.wait(ImageIds=[new_ami_id])

        if tags_for_create:
            click.echo(f"Applying tags to the new AMI {new_ami_id} in {target_region}")

            ec2_target.create_tags(Resources=[new_ami_id], Tags=tags_for_create)

        if copy_permissions:
            click.echo(f"Copying launch permissions from {ami_id} to {new_ami_id} in {target_region}")
            # Get launch permissions from source AMI
            launch_permissions = ec2_source.describe_image_attribute(
                ImageId=ami_id,
                Attribute='launchPermission'
            ).get('LaunchPermissions', [])
            if launch_permissions:
                ec2_target.modify_image_attribute(
                    ImageId=new_ami_id,
                    LaunchPermission={'Add': launch_permissions}
                )


if __name__ == '__main__':
    copy_ami_to_all_regions()

# Example usage:
# python copy_ami_to_all_regions.py --ami-id ami-0d93dc84c27b5864c --ami-name scylla-qa-loader-ami-v22-ubuntu22 --source-region us-east-1 --target-regions "us-west-1 us-west-2 eu-west-1" --dry-run
#
#  To run with a specific AWS profile, you can set the AWS_PROFILE environment variable:
#  gimme-aws-creds
#  ... select your profile ...
#  AWS_PROFILE="xxxxx-/DeveloperAccessRole"  python copy_ami_to_all_regions.py ...
