# KeyStore: Credential Management

SCT credentials (SSH keys, API tokens, cloud configs) are stored in
both the `scylla-qa-keystore` S3 bucket and AWS Secrets Manager.
When adding or rotating credentials, **update both backends** so the
S3 fallback stays in sync.

## Backend selection

| Source | Key | Value |
|---|---|---|
| Environment variable | `SCT_KEYSTORE_BACKEND` | `secretsmanager` (default) or `s3` |
| SCT config file | `keystore_backend` | `secretsmanager` (default) or `s3` |
| Environment variable | `SCT_KEYSTORE_SM_PREFIX` | default: `sct/` |
| SCT config file | `keystore_sm_prefix` | default: `sct/` |
| Environment variable | `SCT_KEYSTORE_SM_REGION` | default: `us-east-1` |
| SCT config file | `keystore_sm_region` | default: `us-east-1` |

The environment variable takes precedence.

The region must be pinned explicitly: S3 has a global endpoint so boto3 falls
back to `us-east-1` on its own, but Secrets Manager is strictly regional and
raises `NoRegionError` when neither `AWS_DEFAULT_REGION` nor an explicit region
is set. `KeyStore` therefore always passes `region_name`, defaulting to
`us-east-1` where the `sct/*` secrets live.

## Managed credentials

| Key | Type | Notes |
|---|---|---|
| `scylla_test_id_ed25519` | binary | SSH private key |
| `scylla_test_id_ed25519.pub` | binary | SSH public key |
| `gcp-sct-project-1.json` | json | GCP service account (see [GCP projects](#gcp-projects)) |
| `gcp-sct-project-1_service_accounts.json` | json | GCP instance service accounts |
| `gcp-local-ssd-latency.json` | json | GCP service account (see [GCP projects](#gcp-projects)) |
| `gcp-local-ssd-latency_service_accounts.json` | json | GCP instance service accounts |
| `gcp-scylladbaaslab.json` | json | GCP service account (DBaaS lab) |
| `azure.json` | json | Azure credentials |
| `oci.json` | json | OCI credentials |
| `docker.json` | json | Docker Hub credentials |
| `email_config.json` | json | SMTP config |
| `ldap_ms_ad.json` | json | LDAP / AD config |
| `argus_rest_credentials.json` | json | Argus API token (shared fallback) |
| `argus_rest_credentials_sct_{provider}.json` | json | Argus API token per cloud provider; optional, falls back to the shared one (see [optional entries](#optional-entries)) |
| `scylla_cloud_sct_api_creds_{env}.json` | json | Scylla Cloud REST API creds per `xcloud_env` (see [optional entries](#optional-entries)) |
| `scylladb_jira.json` | json | Jira API token |
| `CA.pem` | binary | Encryption-at-rest CA cert |
| `SCYLLADB.pem` | binary | Encryption-at-rest cert |
| `hytrust-kmip-cacert.pem` | binary | HyTrust KMIP CA cert |
| `hytrust-kmip-scylla.pem` | binary | HyTrust KMIP client cert |
| `housekeeping-db.json` | json | Internal DB credentials |
| `backup_azure_blob.json` | json | Azure Blob backup credentials |
| `azure_kms_config.json` | json | Azure KMS config |
| `gcp_kms_config.json` | json | GCP KMS config |
| `scylladb_upload.json` | json | Upload API token |
| `qa_users.json` | json | QA user roster |
| `bucket-users.json` | json | ACL grantees |
| `aws_images_role.json` | json | AWS role for image copying |
| `github_access.json` | json | GitHub API token |
| `jenkins.json` | json | Jenkins API credentials |
| `scylla_doctor_full.json` | json | scylla-doctor credentials |

## Optional entries

Some keys are resolved from a runtime value, so which ones are needed depends on
what a job is pointed at. A missing entry here does **not** fail loudly, which
makes it the easiest kind of gap to miss:

| Key pattern | Resolved from | Consumers | On a miss |
|---|---|---|---|
| `argus_rest_credentials_sct_{provider}.json` | detected cloud provider | `KeyStore.get_argus_rest_credentials_per_provider()` | falls back to `argus_rest_credentials.json`, logged at debug. Only `aws` is mirrored today. |
| `scylla_cloud_sct_api_creds_{env}.json` | `xcloud_env` | `xcloud` runs, `utils/cloud_cleanup/xcloud/clean_xcloud.py`, `sdcm/utils/cloud_monitor/resources/xcloud.py` | both cleanup and cloud-monitor catch it, log a warning and **skip that environment** — leaked clusters, no hard failure. `lab` is mirrored; `staging` and `prod` are **not**. |
| `{s3_baremetal_config}.json` | `s3_baremetal_config` | baremetal provisioning + log collection | hard failure at provisioning. Not mirrored (`baremetal_config_example.json`, `baremetal_credentials.json`, `oci_baremetal_config.json` exist in S3 only) — baremetal jobs must set `keystore_backend: 's3'` until they are. |

Mirror the entry for any environment/provider/config a job actually uses before
pointing it at the `secretsmanager` backend.

## Not mirrored: bulk data caches

The `issues/` prefix in `scylla-qa-keystore` holds the Jira/GitHub issue caches
that `sdcm/utils/issues.py` reads. These are **not** credentials and are
deliberately **not** mirrored:

- they exceed the 64 KB Secrets Manager secret limit (largest is >1 MB);
- they are rewritten every 6 hours by `.github/workflows/cache-issues.yaml` and
  `cache-jira-issues.yaml`, which upload to S3 only.

`CachedJiraIssues` and `CachedGitHubIssues` therefore construct
`KeyStore(backend="s3")` explicitly and ignore `keystore_backend`. Without that
pin, every issue lookup misses the cache and falls back to the live API — the
GitHub rate limit the cache exists to avoid — while only emitting a warning.

Use `KeyStore(backend="s3")` for any future non-credential bulk data; do not try
to mirror it.

## GCP projects

GCP credentials are keyed by project id, not by a fixed name.
`KeyStore.get_gcp_credentials()` and `KeyStore.get_gcp_service_accounts()`
resolve the project from `SCT_GCE_PROJECT` (defaulting to
`gcp-sct-project-1`) and fetch:

- `{project}.json` — the service-account key used to authenticate to GCP
- `{project}_service_accounts.json` — the service accounts attached to
  provisioned instances

The set of project ids SCT may be pointed at is
`SUPPORTED_PROJECTS` in `sdcm/utils/gce_utils.py`. **Every project in
`SUPPORTED_PROJECTS` needs both entries present in Secrets Manager**, or
any run / cleanup / cloud-monitor pass that iterates the project set
fails with `ResourceNotFoundException` for the missing one. Current set:

| Project id | Required secrets |
|---|---|
| `gcp-sct-project-1` | `sct/gcp-sct-project-1.json`, `sct/gcp-sct-project-1_service_accounts.json` |
| `gcp-local-ssd-latency` | `sct/gcp-local-ssd-latency.json`, `sct/gcp-local-ssd-latency_service_accounts.json` |

When adding a project (see [gcp_create_new_project.md](gcp_create_new_project.md)),
mirror both entries into Secrets Manager in the same change that adds the
id to `SUPPORTED_PROJECTS`.

> **Backporting note.** The Secrets Manager mirror only holds the
> projects listed above. Release branches that still carry extra project
> ids in `SUPPORTED_PROJECTS` — the legacy `gcp` project is present on
> `branch-2022.2` through `branch-2024.2` — must have those ids dropped
> as part of backporting the `secretsmanager` default, otherwise
> `sct.py`/cleanup will look up `sct/gcp.json`, which does not exist.
> Branches `branch-2025.1` and newer, plus `branch-perf-v17`, already
> carry exactly the two projects above and need no adjustment.

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

Quick check that all entries are readable in Secrets Manager. `describe-secret`
is enough to prove existence and — unlike `get-secret-value` — never pulls the
secret material onto the machine running the check:

```bash
# Required for every run.
required="scylla_test_id_ed25519 scylla_test_id_ed25519.pub
          gcp-sct-project-1.json gcp-sct-project-1_service_accounts.json
          gcp-local-ssd-latency.json gcp-local-ssd-latency_service_accounts.json
          azure.json oci.json docker.json
          email_config.json ldap_ms_ad.json
          argus_rest_credentials.json scylladb_jira.json
          housekeeping-db.json backup_azure_blob.json
          azure_kms_config.json gcp_kms_config.json
          scylladb_upload.json qa_users.json bucket-users.json
          gcp-scylladbaaslab.json aws_images_role.json
          github_access.json jenkins.json scylla_doctor_full.json
          CA.pem SCYLLADB.pem hytrust-kmip-cacert.pem hytrust-kmip-scylla.pem"

# Needed only by the jobs that use them -- see "Optional entries" above.
optional="argus_rest_credentials_sct_aws.json
          argus_rest_credentials_sct_gce.json
          argus_rest_credentials_sct_azure.json
          scylla_cloud_sct_api_creds_lab.json
          scylla_cloud_sct_api_creds_staging.json
          scylla_cloud_sct_api_creds_prod.json"

check() {
    for name in $2; do
        if aws secretsmanager describe-secret --secret-id "sct/$name" \
             --region us-east-1 --output text --query Name >/dev/null 2>&1; then
            echo "OK   sct/$name"
        else
            echo "$1 sct/$name"
        fi
    done
}
check MISS "$required"   # any MISS breaks runs on the secretsmanager backend
check GAP  "$optional"   # a GAP only breaks the jobs that need that entry
```

The `issues/` cache is intentionally absent from both lists — see
[Not mirrored: bulk data caches](#not-mirrored-bulk-data-caches).

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
