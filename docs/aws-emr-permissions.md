# AWS EMR permissions for SCT

SCT provisions Amazon EMR clusters for spark-migrator testing. The IAM user or role running SCT needs
the permissions below in addition to the standard SCT AWS permissions documented in
[aws_configuration.md](aws_configuration.md).

## IAM policy

The policy uses four SID blocks scoped to the minimum actions required:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SCTEmrClusterManagement",
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:RunJobFlow",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:ListClusters",
        "elasticmapreduce:TerminateJobFlows",
        "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeStep",
        "elasticmapreduce:ListSteps"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SCTEmrRoleSetup",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:CreateInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:GetRole",
        "iam:GetInstanceProfile",
        "iam:ListAttachedRolePolicies"
      ],
      "Resource": [
        "arn:aws:iam::*:role/EMR_DefaultRole",
        "arn:aws:iam::*:role/EMR_EC2_DefaultRole",
        "arn:aws:iam::*:instance-profile/EMR_EC2_DefaultRole"
      ]
    },
    {
      "Sid": "SCTEmrPassRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::*:role/EMR_DefaultRole",
        "arn:aws:iam::*:role/EMR_EC2_DefaultRole"
      ]
    },
    {
      "Sid": "SCTEmrS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:HeadBucket",
        "s3:PutObject",
        "s3:GetObject",
        "s3:HeadObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::sct-emr-spark-migrator-*",
        "arn:aws:s3:::sct-emr-spark-migrator-*/*"
      ]
    }
  ]
}
```

## Setup commands

### Create the policy

```shell
aws iam create-policy \
  --policy-name SCT-EMR \
  --policy-document file://docs/aws-emr-permissions.json \
  --description "SCT permissions for Amazon EMR cluster provisioning"
```

Or inline from this document — save the JSON block above to `docs/aws-emr-permissions.json` first.

### Attach the policy

```shell
# attach to an IAM user
aws iam attach-user-policy \
  --user-name <SCT_USER> \
  --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/SCT-EMR

# or attach to an IAM role
aws iam attach-role-policy \
  --role-name <SCT_ROLE> \
  --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/SCT-EMR
```

### Verify the policy

```shell
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::<ACCOUNT_ID>:user/<SCT_USER> \
  --action-names elasticmapreduce:RunJobFlow elasticmapreduce:TerminateJobFlows \
  --output table
```

All actions should show `allowed` in the `EvalDecision` column.

## EMR default roles

EMR requires two service-linked roles (`EMR_DefaultRole` and `EMR_EC2_DefaultRole`). If they
don't exist in your account yet, create them once:

```shell
aws emr create-default-roles
```

This is a one-time setup per AWS account.
