AMI_ID=ami-0c4e5a2331f97fd0d
AMI_NAME='scylladb-monitor-4-6-2-2024-02-13t08-06-04z'
SOURCE_REGION=us-east-1
TARGET_REGIONS='us-west-2 eu-west-1 eu-west-2 eu-north-1 eu-central-1'

for target_region in $TARGET_REGIONS
do
	echo "copy AMI to $target_region"
	aws ec2 copy-image --region $target_region  --name "$AMI_NAME" --source-region $SOURCE_REGION --source-image-id $AMI_ID
done
