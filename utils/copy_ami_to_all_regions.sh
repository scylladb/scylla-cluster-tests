AMI_ID=ami-03bcb976479f42c61
AMI_NAME='scylla_monitoring_2023-01-16T11-43-28Z'
SOURCE_REGION=us-east-1
TARGET_REGIONS='us-west-2 eu-west-1 eu-west-2 eu-north-1 eu-central-1'

for target_region in $TARGET_REGIONS
do
	echo "copy AMI to $target_region"
	aws ec2 copy-image --region $target_region  --name "$AMI_NAME" --source-region $SOURCE_REGION --source-image-id $AMI_ID --copy-image-tags
done
