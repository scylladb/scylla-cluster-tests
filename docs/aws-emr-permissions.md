# AWS EMR Permissions for SCT spark-migrator Tests

This document describes the IAM permissions required to run SCT spark-migrator tests with Amazon EMR,
and provides AWS CLI commands to create and attach a single consolidated policy.

## Required Permissions

SCT's EMR integration uses three AWS service namespaces:

| Service | Actions | Purpose |
|---------|---------|---------|
| `elasticmapreduce` | `RunJobFlow`, `DescribeCluster`, `ListClusters`, `TerminateJobFlows`, `AddJobFlowSteps`, `DescribeStep` | Cluster lifecycle and job submission |
| `iam` | `GetRole`, `CreateRole`, `AttachRolePolicy`, `GetInstanceProfile`, `CreateInstanceProfile`, `AddRoleToInstanceProfile`, `PassRole` | One-time setup of `EMR_DefaultRole` and `EMR_EC2_DefaultRole` |
| `s3` | `HeadObject`, `PutObject`, `GetObject` | Upload spark-migrator JAR and write EMR logs |

## Creating the Consolidated Policy

All permissions are combined into a single policy to stay well within the AWS limit of 10 managed
policies per group.

### Step 1 — Write the policy document

```bash
cat > /tmp/sct-emr-policy.json <<'EOF'
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
        "elasticmapreduce:DescribeStep"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SCTEmrRoleSetup",
      "Effect": "Allow",
      "Action": [
        "iam:GetRole",
        "iam:CreateRole",
        "iam:AttachRolePolicy",
        "iam:GetInstanceProfile",
        "iam:CreateInstanceProfile",
        "iam:AddRoleToInstanceProfile"
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
        "s3:HeadObject",
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::sct-emr-spark-migrator-*/*"
      ]
    }
  ]
}
EOF
```

### Step 2 — Create the policy

Replace `ACCOUNT_ID` with your AWS account ID (find it with `aws sts get-caller-identity --query Account --output text`).

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws iam create-policy \
  --policy-name SCT-EMR \
  --description "SCT permissions for EMR cluster management, IAM role setup, and S3 JAR storage" \
  --policy-document file:///tmp/sct-emr-policy.json
```

### Step 3 — Attach the policy to the `qa` group

```bash
aws iam attach-group-policy \
  --group-name qa \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/SCT-EMR
```

### Step 4 — Verify the policy is attached

```bash
aws iam list-attached-group-policies --group-name qa \
  --query 'AttachedPolicies[?PolicyName==`SCT-EMR`].{Name:PolicyName,Arn:PolicyArn}' \
  --output table
```

## Updating an Existing Policy

If the policy already exists and needs to be updated (e.g., new permissions were added), create a
new policy version:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws iam create-policy-version \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/SCT-EMR \
  --policy-document file:///tmp/sct-emr-policy.json \
  --set-as-default
```

> **Note:** AWS allows at most 5 versions per policy. Remove old versions when needed:
> ```bash
> aws iam delete-policy-version \
>   --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/SCT-EMR \
>   --version-id v1
> ```

## One-Time EMR Role Setup

The first time EMR is used in a region, the service roles `EMR_DefaultRole` and `EMR_EC2_DefaultRole`
must exist. SCT creates them automatically via `AwsRegion.ensure_emr_roles()` if the caller has the
`iam:GetRole` / `iam:CreateRole` permissions above.

To set them up manually instead:

```bash
# Create default EMR roles using the AWS CLI helper
aws emr create-default-roles
```

This single command creates both roles and attaches the standard AWS-managed policies, which is
equivalent to what `ensure_emr_roles()` does.
