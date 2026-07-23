# KeyStore: Credential Management

SCT credentials (SSH keys, API tokens, cloud configs) are stored in
both the `scylla-qa-keystore` S3 bucket and AWS Secrets Manager.
When adding or rotating credentials, **update both backends** so the
S3 fallback stays in sync.

## Backend selection

| Source | Key | Value |
|---|---|---|
| Environment variable | `SCT_KEYSTORE_BACKEND` | `s3` (default) or `secretsmanager` |
| SCT config file | `keystore_backend` | `s3` or `secretsmanager` |
| Environment variable | `SCT_KEYSTORE_SM_PREFIX` | default: `sct/` |
| SCT config file | `keystore_sm_prefix` | default: `sct/` |

The environment variable takes precedence.

## Managed credentials

| Key | Type | Notes |
|---|---|---|
| `scylla_test_id_ed25519` | binary | SSH private key |
| `scylla_test_id_ed25519.pub` | binary | SSH public key |
| `gcp-sct-project-1.json` | json | GCP service account |
| `gcp-scylladbaaslab.json` | json | GCP service account (DBaaS lab) |
| `azure.json` | json | Azure credentials |
| `oci.json` | json | OCI credentials |
| `docker.json` | json | Docker Hub credentials |
| `email_config.json` | json | SMTP config |
| `ldap_ms_ad.json` | json | LDAP / AD config |
| `argus_rest_credentials.json` | json | Argus API token |
| `scylladb_jira.json` | json | Jira API token |
| `housekeeping-db.json` | json | Internal DB credentials |
| `backup_azure_blob.json` | json | Azure Blob backup credentials |
| `azure_kms_config.json` | json | Azure KMS config |
| `gcp_kms_config.json` | json | GCP KMS config |
| `scylladb_upload.json` | json | Upload API token |
| `qa_users.json` | json | QA user roster |
| `bucket-users.json` | json | ACL grantees |

## Adding or updating credentials

Always update **both** S3 and Secrets Manager.

### JSON credential

```bash
# 1. Upload to S3
aws s3 cp /path/to/new_cred.json s3://scylla-qa-keystore/new_cred.json

# 2. Upload to Secrets Manager (create or update)
#    First time:
aws secretsmanager create-secret \
    --name sct/new_cred.json \
    --secret-string file:///path/to/new_cred.json \
    --tags Key=team,Value=sct
#    Update existing:
aws secretsmanager put-secret-value \
    --secret-id sct/new_cred.json \
    --secret-string file:///path/to/new_cred.json

# 3. Clean up local copy
shred -u /path/to/new_cred.json
```

### Binary credential (SSH key)

```bash
# 1. Upload to S3
aws s3 cp /path/to/key s3://scylla-qa-keystore/scylla_test_id_ed25519

# 2. Upload to Secrets Manager
aws secretsmanager put-secret-value \
    --secret-id sct/scylla_test_id_ed25519 \
    --secret-binary fileb:///path/to/key

# 3. Clean up
shred -u /path/to/key
```

SCT detects rotation via ETag (S3) or VersionId sidecar (SM) and
re-downloads automatically on the next run.

## Extracting a credential for debugging

```bash
# From Secrets Manager (JSON)
aws secretsmanager get-secret-value \
    --secret-id sct/email_config.json \
    --query SecretString --output text | jq .

# From Secrets Manager (binary)
aws secretsmanager get-secret-value \
    --secret-id sct/scylla_test_id_ed25519 \
    --query SecretBinary --output text | base64 -d > /tmp/key
chmod 600 /tmp/key

# From S3
aws s3 cp s3://scylla-qa-keystore/email_config.json -
```

## Validation

Quick check that all entries are readable in Secrets Manager:

```bash
for name in scylla_test_id_ed25519 scylla_test_id_ed25519.pub \
            gcp-sct-project-1.json azure.json oci.json docker.json \
            email_config.json ldap_ms_ad.json \
            argus_rest_credentials.json scylladb_jira.json \
            housekeeping-db.json backup_azure_blob.json \
            azure_kms_config.json gcp_kms_config.json \
            scylladb_upload.json qa_users.json bucket-users.json \
            gcp-scylladbaaslab.json; do
    if aws secretsmanager get-secret-value --secret-id "sct/$name" \
         --output text --query Name >/dev/null 2>&1; then
        echo "OK   sct/$name"
    else
        echo "MISS sct/$name"
    fi
done
```

## Access policy

IAM policy for SCT runtime (read-only on `sct/*`):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SCTSecretsManagerReadOnly",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:sct/*"
        }
    ]
}
```
