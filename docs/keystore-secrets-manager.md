# KeyStore: S3 ↔ Secrets Manager Migration Guide

This guide explains how to migrate SCT's credential storage from the
legacy `scylla-qa-keystore` S3 bucket to AWS Secrets Manager, and how
to debug / maintain individual credentials during and after the
migration.

## Backend selection

SCT reads the active backend from one of:

| Source | Key | Value |
|---|---|---|
| Environment variable | `SCT_KEYSTORE_BACKEND` | `secretsmanager` (default) or `s3` |
| SCT config file | `keystore_backend` | `s3` or `secretsmanager` |
| Environment variable | `SCT_KEYSTORE_SM_PREFIX` | default: `sct/` |
| SCT config file | `keystore_sm_prefix` | default: `sct/` |

The environment variable takes precedence.

## One-time migration from S3 to Secrets Manager

The migration script is maintained in a separate follow-up PR. In the
meantime, the same operations can be performed with the AWS CLI.

**Prerequisites:**

- AWS credentials with read access to the `scylla-qa-keystore` S3 bucket
  **and** write access to Secrets Manager (`secretsmanager:CreateSecret`,
  `secretsmanager:TagResource`).
- IAM policy granting SCT read-only access to `sct/*` secrets (see
  [Access policy](#access-policy) below).

### Manifest of entries to migrate

The authoritative list is also duplicated in `scripts/validate-keystore-sm.sh`
(the `MANIFEST_KEYS` array), which can validate and migrate missing entries
in one pass — see [Validation after migration](#validation-after-migration).

| S3 key | Type | Notes |
|---|---|---|
| `scylla_test_id_ed25519` | binary | SSH private key (only one SCT synchronises at boot) |
| `scylla_test_id_ed25519.pub` | binary | SSH public key |
| `gcp-sct-project-1.json` | json | GCP service account (main project) |
| `gcp-sct-project-1_service_accounts.json` | json | Per-SA credentials fetched by `get_gcp_service_accounts()` |
| `gcp-scylladbaaslab.json` | json | GCP service account (DBaaS lab) |
| `azure.json` | json | Azure credentials |
| `oci.json` | json | OCI credentials |
| `aws_images_role.json` | json | AWS role for image lookup (`sdcm/utils/aws_utils.py`) |
| `jenkins.json` | json | Jenkins API credentials used by runner setup |
| `github_access.json` | json | GitHub API token used by `sdcm/utils/issues.py` |
| `docker.json` | json | Docker Hub credentials |
| `email_config.json` | json | SMTP config |
| `ldap_ms_ad.json` | json | LDAP / AD config |
| `argus_rest_credentials.json` | json | Argus API token (default) |
| `argus_rest_credentials_sct_<cloud>.json` | json | Optional per-cloud override, only read under Jenkins (falls back to the default above when absent) |
| `scylladb_jira.json` | json | Jira API token |
| `housekeeping-db.json` | json | Internal DB credentials |
| `backup_azure_blob.json` | json | Azure Blob backup credentials |
| `azure_kms_config.json` | json | Azure KMS config |
| `gcp_kms_config.json` | json | GCP KMS config |
| `scylladb_upload.json` | json | Upload API token |
| `scylla_doctor_full.json` | json | S3 bucket config for scylla-doctor full edition |
| `qa_users.json` | json | QA user roster |
| `bucket-users.json` | json | ACL grantees |
| `scylla_cloud_sct_api_creds_lab.json` | json | Scylla Cloud API creds for the lab environment |
| `CA.pem` | binary | Encryption-at-rest certificate |
| `SCYLLADB.pem` | binary | Encryption-at-rest certificate |
| `hytrust-kmip-cacert.pem` | binary | HyTrust KMIP CA certificate |
| `hytrust-kmip-scylla.pem` | binary | HyTrust KMIP client certificate |

### Migrating a single JSON entry with AWS CLI

```bash
# Download from S3
aws s3 cp s3://scylla-qa-keystore/email_config.json /tmp/email_config.json

# Upload to Secrets Manager under the sct/ prefix
aws secretsmanager create-secret \
    --name sct/email_config.json \
    --secret-string file:///tmp/email_config.json \
    --description "SCT keystore entry migrated from S3" \
    --tags Key=team,Value=sct Key=secret_type,Value=config Key=rotation_tier,Value=tier3

# Clean up local copy
shred -u /tmp/email_config.json
```

### Migrating a binary entry (SSH key)

```bash
aws s3 cp s3://scylla-qa-keystore/scylla_test_id_ed25519 /tmp/key
aws secretsmanager create-secret \
    --name sct/scylla_test_id_ed25519 \
    --secret-binary fileb:///tmp/key \
    --description "SCT SSH private key (migrated from S3)" \
    --tags Key=team,Value=sct Key=secret_type,Value=ssh_key Key=rotation_tier,Value=tier1
shred -u /tmp/key
```

## Debugging: extract a single credential

### Via AWS CLI

```bash
# JSON entry → stdout (piped through jq for readability)
aws secretsmanager get-secret-value \
    --secret-id sct/email_config.json \
    --query SecretString --output text | jq .

# Binary entry → file on disk
aws secretsmanager get-secret-value \
    --secret-id sct/scylla_test_id_ed25519 \
    --query SecretBinary --output text | base64 -d > ~/.ssh/scylla_test_id_ed25519
chmod 600 ~/.ssh/scylla_test_id_ed25519
```

### Via Python / SCT

```python
from sdcm.keystore import KeyStore

# Default backend is S3 — set the env var before creating the instance:
#   export SCT_KEYSTORE_BACKEND=secretsmanager

ks = KeyStore()
data = ks.get_file_contents("email_config.json")
print(data.decode())
```

## Rotating / uploading a new value

### Update an existing entry

```bash
# JSON
aws secretsmanager put-secret-value \
    --secret-id sct/email_config.json \
    --secret-string file:///path/to/new-email-config.json

# Binary
aws secretsmanager put-secret-value \
    --secret-id sct/scylla_test_id_ed25519 \
    --secret-binary fileb:///path/to/new-key
```

AWS assigns a new `VersionId` on each update. SCT's
`get_obj_if_needed()` reads this via `describe_secret` and only
re-downloads the file if it differs from the locally cached
`<file>.version` sidecar, so rotation is picked up on the next
`sct.py run-test` or `hydra` invocation without extra API calls
between rotations.

### Create a new entry

```bash
aws secretsmanager create-secret \
    --name sct/new_entry.json \
    --secret-string file:///path/to/value.json \
    --tags Key=team,Value=sct Key=secret_type,Value=config
```

## Access policy

The recommended IAM policy for SCT's AWS role, scoped to read-only
access on `sct/*` secrets:

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

`DescribeSecret` is required so SCT can read `VersionId` for the
sidecar caching described above.

## Validation after migration

Use `scripts/validate-keystore-sm.sh` — it encapsulates the sanity check against
the full manifest above, can optionally drive a live `KeyStore()` round-trip,
invoke `./sct.py list-images` (the smallest command that exercises
`key_store.sync()`), and even migrate any missing entries from S3 in a single
pass.

```bash
# Read-only sanity check
./scripts/validate-keystore-sm.sh

# Also do a live fetch through sdcm.keystore for every entry
./scripts/validate-keystore-sm.sh --python

# Reproduce the full Jenkins boot sequence locally
./scripts/validate-keystore-sm.sh --sct

# Copy any missing entries from s3://scylla-qa-keystore into Secrets Manager
./scripts/validate-keystore-sm.sh --migrate-missing
```

The script exits non-zero if any entry is unresolved, so it can be wired into
a pre-cutover CI check.

## Cutover runbook

1. **Pre-cutover**: Run the validation loop above — confirm every
   `sct/*` entry is reachable.
2. **Cutover**: Set `SCT_KEYSTORE_BACKEND=secretsmanager` in the SCT
   runner environment (Jenkins credentials, developer shells).
3. **Smoke test**: Run a docker-backend test, verify `INFO fetched …
   (backend=secretsmanager) in …s` entries appear in the logs.
4. **Rollback**: Unset `SCT_KEYSTORE_BACKEND` (or set it to `s3`).
   The S3 bucket is kept in sync by the Hub during the grace period,
   so callers see no disruption.
