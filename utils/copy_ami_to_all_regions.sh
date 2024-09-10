AMI_ID="ami-0d93dc84c27b5864c"
AMI_NAME="scylla-qa-loader-ami-v22-ubuntu22"
SOURCE_REGION="us-east-1"
TARGET_REGIONS="us-west-2 eu-west-1 eu-west-2 eu-north-1 eu-central-1"

# Retrieve the tags of the source AMI
TAGS=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$AMI_ID" --region $SOURCE_REGION --query "Tags[*].{Key:Key,Value:Value}")

for target_region in $TARGET_REGIONS; do
    echo "Copying AMI to $target_region"
    NEW_AMI_ID=$(aws ec2 copy-image --region "${target_region}" --name "$AMI_NAME" --source-region $SOURCE_REGION --source-image-id $AMI_ID --query 'ImageId' --output text)

    echo "Applying tags to the new AMI in $target_region"
    aws ec2 create-tags --resources "${NEW_AMI_ID}" --tags "$TAGS" --region "${target_region}"
done
