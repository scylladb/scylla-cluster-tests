# GCP Project Setup for SCT

This document describes how to set up a new GCP project for running SCT (Scylla Cluster Tests),
including required APIs, service account creation, IAM permissions, network configuration, and
storage setup.

## Reference: Current `sct-project-1` Configuration

The existing `sct-project-1` project (project number: `1070746702980`) serves as the reference
implementation for this guide.

---

## Prerequisites

- `gcloud` CLI installed and authenticated with an account that has `roles/owner` or
  `roles/resourcemanager.projectCreator` on the target organization
- Access to the `scylla-qa-keystore` S3 bucket (for storing service account credentials)
- Organization ID: `273512867970` (ScyllaDB)

---

## Step 1: Create the GCP Project

```bash
export NEW_PROJECT_ID="gcp-sct-project-2"  # Choose your project ID
export ORG_ID="273512867970"
export BILLING_ACCOUNT_ID="<your-billing-account-id>"

# Create the project
gcloud projects create "$NEW_PROJECT_ID" \
    --organization="$ORG_ID" \
    --name="$NEW_PROJECT_ID"

# Link billing account
gcloud billing projects link "$NEW_PROJECT_ID" \
    --billing-account="$BILLING_ACCOUNT_ID"
```

---

## Step 2: Enable Required APIs

The following APIs must be enabled for SCT to function:

```bash
gcloud services enable \
    compute.googleapis.com \
    container.googleapis.com \
    containerregistry.googleapis.com \
    storage.googleapis.com \
    storage-api.googleapis.com \
    iam.googleapis.com \
    iamcredentials.googleapis.com \
    cloudkms.googleapis.com \
    cloudresourcemanager.googleapis.com \
    dns.googleapis.com \
    logging.googleapis.com \
    monitoring.googleapis.com \
    oslogin.googleapis.com \
    secretmanager.googleapis.com \
    artifactregistry.googleapis.com \
    gkebackup.googleapis.com \
    networkconnectivity.googleapis.com \
    --project="$NEW_PROJECT_ID"
```

### API Purpose Reference

| API | Purpose in SCT |
|-----|---------------|
| `compute.googleapis.com` | VM instances, disks, images, networks, firewalls |
| `container.googleapis.com` | GKE clusters for k8s-gke backend |
| `containerregistry.googleapis.com` | Container image storage |
| `storage.googleapis.com` | GCS buckets for backups, logs, artifacts |
| `iam.googleapis.com` | Service account management |
| `cloudkms.googleapis.com` | KMS key management for encryption tests |
| `dns.googleapis.com` | DNS management |
| `logging.googleapis.com` | GCE instance log monitoring (host errors, preemptions) |
| `monitoring.googleapis.com` | Instance monitoring |
| `oslogin.googleapis.com` | OS Login for SSH access |
| `gkebackup.googleapis.com` | GKE backup operations |
| `networkconnectivity.googleapis.com` | VPC peering for xcloud tests |

---

## Step 3: Create the Main Service Account

SCT uses a service account (stored as `<project-id>.json` in the S3 keystore) for all operations.
In `sct-project-1`, this is the Compute Engine default service account
(`<project-number>-compute@developer.gserviceaccount.com`).

For a new project, create a dedicated service account:

```bash
export SA_NAME="sct-automation"
export SA_EMAIL="${SA_NAME}@${NEW_PROJECT_ID}.iam.gserviceaccount.com"

# Create the service account
gcloud iam service-accounts create "$SA_NAME" \
    --display-name="SCT Automation Service Account" \
    --project="$NEW_PROJECT_ID"

# Create and download the key
gcloud iam service-accounts keys create "/tmp/${NEW_PROJECT_ID}.json" \
    --iam-account="$SA_EMAIL" \
    --project="$NEW_PROJECT_ID"
```

### Upload Credentials to S3 Keystore

```bash
# Upload the service account key to the SCT keystore
aws s3 cp "/tmp/${NEW_PROJECT_ID}.json" "s3://scylla-qa-keystore/${NEW_PROJECT_ID}.json"

# Clean up local key
rm "/tmp/${NEW_PROJECT_ID}.json"
```

---

## Step 4: Assign IAM Roles to Service Account

The service account needs the following project-level IAM roles:

```bash
# Core compute permissions (instances, disks, images, networks, firewalls)
gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/editor"

# GKE cluster management
gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/container.admin"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/container.clusterAdmin"

# Storage admin (buckets, objects, lifecycle, object retention)
gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.admin"

# KMS admin (for encryption testing - Scylla EaR, Manager backup encryption)
gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/cloudkms.admin"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/cloudkms.cryptoKeyEncrypter"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/cloudkms.cryptoKeyDecrypter"
```

### IAM Roles Summary

| Role | Purpose |
|------|---------|
| `roles/editor` | General compute, networking, logging, monitoring access |
| `roles/container.admin` | Full GKE cluster lifecycle management |
| `roles/container.clusterAdmin` | GKE RBAC administration |
| `roles/storage.admin` | Full GCS access including bucket creation, object retention, lifecycle |
| `roles/cloudkms.admin` | Create/manage KMS keyrings and keys |
| `roles/cloudkms.cryptoKeyEncrypter` | Encrypt data with KMS keys |
| `roles/cloudkms.cryptoKeyDecrypter` | Decrypt data with KMS keys |

### Note on `storage.buckets.enableObjectRetention`

The `roles/storage.admin` role includes the `storage.buckets.enableObjectRetention` permission
required for WORM backup testing. The basic `roles/editor` role does NOT include this permission.
This was the root cause of [SCT-378](https://scylladb.atlassian.net/browse/SCT-378).

---

## Step 5: Create the Backup Service Account

SCT creates a separate service account for Scylla Manager backup tests:

```bash
export BACKUP_SA_NAME="sct-manager-backup"
export BACKUP_SA_EMAIL="${BACKUP_SA_NAME}@${NEW_PROJECT_ID}.iam.gserviceaccount.com"

# Create backup service account
gcloud iam service-accounts create "$BACKUP_SA_NAME" \
    --display-name="Account for having access to the gcs bucket for SCT backup tests" \
    --project="$NEW_PROJECT_ID"

# Grant KMS permissions for backup encryption tests
gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${BACKUP_SA_EMAIL}" \
    --role="roles/cloudkms.admin"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${BACKUP_SA_EMAIL}" \
    --role="roles/cloudkms.cryptoOperator"
```

> **Note:** The backup service account also gets `roles/storage.objectAdmin` on individual
> backup buckets. This is handled automatically by `GceRegion.configure_backup_storage()` in
> `sdcm/utils/gce_region.py`.

---

## Step 6: Create Service Accounts JSON for Instance Scopes

SCT assigns service account scopes to GCE instances. Create the scopes configuration:

```bash
# Create the service accounts scopes file
cat > "/tmp/${NEW_PROJECT_ID}_service_accounts.json" << 'EOF'
[
  {
    "email": "<project-number>-compute@developer.gserviceaccount.com",
    "scopes": [
      "https://www.googleapis.com/auth/devstorage.read_write",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
]
EOF

# Upload to keystore
aws s3 cp "/tmp/${NEW_PROJECT_ID}_service_accounts.json" \
    "s3://scylla-qa-keystore/${NEW_PROJECT_ID}_service_accounts.json"
```

> **Note:** The `https://www.googleapis.com/auth/cloud-platform` scope is automatically appended
> by `KeyStore.get_gcp_service_accounts()` if not present (see `sdcm/keystore.py`).

---

## Step 7: Network Configuration

SCT uses a VPC network named `qa-vpc` with auto-created subnets:

```bash
# Create the qa-vpc network (auto-subnet mode)
gcloud compute networks create qa-vpc \
    --subnet-mode=auto \
    --project="$NEW_PROJECT_ID"
```

### Firewall Rules

SCT automatically configures firewall rules via `GceRegion.configure_firewall()` in
`sdcm/utils/gce_region.py`. The following rules are created:

```bash
# Allow SSH from anywhere
gcloud compute firewall-rules create qa-vpc-allow-ssh \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:22 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow all internal traffic (10.0.0.0/8)
gcloud compute firewall-rules create qa-vpc-allow-all-internal \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=all \
    --source-ranges=10.0.0.0/8 \
    --project="$NEW_PROJECT_ID"

# Allow ICMP
gcloud compute firewall-rules create qa-vpc-allow-icmp \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=icmp \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow Grafana (monitoring)
gcloud compute firewall-rules create qa-vpc-allow-grafana \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:3000 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow Scylla gossip ports
gcloud compute firewall-rules create qa-vpc-allow-node-gossip \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:7000,tcp:7001 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow Scylla REST API
gcloud compute firewall-rules create qa-vpc-allow-node-api \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:10000 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow CQL native transport
gcloud compute firewall-rules create qa-vpc-allow-node-cql \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:9042,tcp:9142,tcp:19042,tcp:19142 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow Prometheus metrics
gcloud compute firewall-rules create qa-vpc-allow-prometheus \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:9090,tcp:9100,tcp:9103,tcp:9180 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow HTTP
gcloud compute firewall-rules create qa-vpc-allow-http \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:80 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow Alternator (DynamoDB-compatible API)
gcloud compute firewall-rules create qa-vpc-allow-alternator \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:8080 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Allow Scylla Manager ports
gcloud compute firewall-rules create qa-vpc-allow-manager \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:10001,tcp:5080,tcp:5443,tcp:5090,tcp:5112 \
    --source-ranges=0.0.0.0/0 \
    --project="$NEW_PROJECT_ID"

# Deny all (for sct-network-only tagged instances) - priority 200
gcloud compute firewall-rules create qa-vpc-deny-all \
    --network=qa-vpc --direction=INGRESS \
    --action=DENY --rules=all \
    --source-ranges=0.0.0.0/0 \
    --target-tags=sct-network-only \
    --priority=200 \
    --project="$NEW_PROJECT_ID"

# Allow internal only for sct-network-only tagged instances - priority 100
gcloud compute firewall-rules create qa-vpc-allow-only-sct-network \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=all \
    --source-ranges=10.0.0.0/8 \
    --target-tags=sct-network-only \
    --priority=100 \
    --project="$NEW_PROJECT_ID"

# Allow Grafana public for sct-network-only tagged instances - priority 100
gcloud compute firewall-rules create qa-vpc-allow-grafana-public \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=tcp:3000 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=sct-network-only \
    --priority=100 \
    --project="$NEW_PROJECT_ID"

# Allow all public for sct-allow-public tagged instances - priority 100
gcloud compute firewall-rules create qa-vpc-allow-public \
    --network=qa-vpc --direction=INGRESS \
    --action=ALLOW --rules=all \
    --source-ranges=0.0.0.0/0 \
    --target-tags=sct-allow-public \
    --priority=100 \
    --project="$NEW_PROJECT_ID"
```

> **Note:** Most of these rules are automatically created by `GceRegion.configure()` when SCT
> runs for the first time against a new region. The manual commands above are for reference
> or if you need to pre-configure before the first run.

---

## Step 8: KMS Setup

SCT tests encryption features using Cloud KMS:

```bash
# Create a keyring (one per region where tests run)
gcloud kms keyrings create demo-keyring \
    --location=us-east1 \
    --project="$NEW_PROJECT_ID"
```

> **Note:** Individual crypto keys are created dynamically by `GcpKms.get_or_create_key()` in
> `sdcm/utils/gcp_kms.py` during test execution.

---

## Step 9: Register Project in SCT Code

To use the new project with SCT, update `sdcm/utils/gce_utils.py`:

```python
SUPPORTED_PROJECTS = {"gcp-sct-project-1", "gcp-local-ssd-latency", "<new-project-id>"} | {
    os.environ.get("SCT_GCE_PROJECT", "gcp-sct-project-1")
}
```

---

## Step 10: Configure Supported Regions

Update `SUPPORTED_REGIONS` in `sdcm/utils/gce_utils.py` if the new project uses different regions:

```python
SUPPORTED_REGIONS = {
    "us-east1": "cd",
    "us-east4": "abc",
    "us-west1": "abc",
    "us-central1": "abcf",
}
```

---

## Step 11: Run Region Configuration

After all manual setup, run the SCT region configuration to create backup buckets and
service accounts automatically:

```bash
export SCT_GCE_PROJECT="<new-project-id>"
python -c "
from sdcm.utils.gce_region import GceRegion
for r in ['us-east1', 'us-east4', 'us-west1', 'us-central1']:
    GceRegion(r).configure()
"
```

Or equivalently via the SCT CLI:
```bash
export SCT_GCE_PROJECT="<new-project-id>"
./sct.py prepare-regions -c gce -r us-east1
```

---

## Step 12: Create Runner Image and Jenkins Worker

```bash
export SCT_GCE_PROJECT="<new-project-id>"

# Create the SCT runner image
./sct.py create-runner-image --cloud-provider gce --region us-east1

# Create a Jenkins worker instance
./sct.py create-runner-instance -c gce -r us-east1 -d 9999 --test-id 1234

# Rename worker to a permanent name (replace ZONE with actual zone, e.g., us-east1-c)
export ZONE="us-east1-c"
gcloud beta compute instances stop sct-runner-1-5-instance-1234 --project="$NEW_PROJECT_ID" --zone="$ZONE"
gcloud beta compute instances set-name sct-runner-1-5-instance-1234 \
    --project="$NEW_PROJECT_ID" --zone="$ZONE" \
    --new-name="${NEW_PROJECT_ID}-builder1-us-east1"
gcloud beta compute instances start "${NEW_PROJECT_ID}-builder1-us-east1" --project="$NEW_PROJECT_ID" --zone="$ZONE"

# Reserve a static IP for the builder
gcloud compute addresses create "${NEW_PROJECT_ID}-builder1-us-east1" \
    --region=us-east1 \
    --project="$NEW_PROJECT_ID"
```

Then configure the instance in Jenkins with labels:
- `<project-id>-builders-us-east1`
- `<project-id>-builders`
- `sct-<short-name>-us-east1`

---

## Step 13: Grant QA Team Access

```bash
export QA_GROUP="qa@scylladb.com"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="group:${QA_GROUP}" \
    --role="roles/editor"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="group:${QA_GROUP}" \
    --role="roles/container.admin"

gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="group:${QA_GROUP}" \
    --role="roles/container.clusterAdmin"
```

---

## Step 14: Upload SSH Public Key

Upload the SCT SSH public key to the project metadata:

```bash
# Get the public key from the keystore
aws s3 cp s3://scylla-qa-keystore/scylla_test_id_ed25519.pub /tmp/sct_key.pub

# Add to project metadata
gcloud compute project-info add-metadata \
    --metadata-from-file=ssh-keys=/tmp/sct_key.pub \
    --project="$NEW_PROJECT_ID"
```

Or via the console: https://console.cloud.google.com/compute/metadata?tab=sshkeys&project=<project-id>

---

## Usage

Once set up, use the new project:

```bash
export SCT_GCE_PROJECT="<new-project-id>"
hydra run-test longevity_test.LongevityTest.test_custom_time --backend gce --config test-cases/PR-provision-test.yaml
```

---

## Verification Checklist

- [ ] Project created and billing linked
- [ ] All required APIs enabled
- [ ] Main service account created with key uploaded to S3 keystore
- [ ] Service accounts scopes JSON uploaded to S3 keystore
- [ ] IAM roles assigned (especially `roles/storage.admin` for object retention)
- [ ] `qa-vpc` network created
- [ ] Firewall rules configured (or let SCT auto-create on first run)
- [ ] KMS keyring created in primary region
- [ ] Project registered in `SUPPORTED_PROJECTS`
- [ ] Region configuration run (`GceRegion.configure()`)
- [ ] Runner image created
- [ ] Jenkins worker configured
- [ ] SSH public key uploaded to project metadata
- [ ] QA team access granted
- [ ] Test run successful with `--backend gce`

---

## Troubleshooting

### `storage.buckets.enableObjectRetention` Permission Denied

The `roles/editor` role does NOT include this permission. You must grant `roles/storage.admin`
to the service account:

```bash
gcloud projects add-iam-policy-binding "$NEW_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.admin"
```

### Credential File Not Found

Ensure the service account key is uploaded to S3 with the correct filename:
- Main credentials: `s3://scylla-qa-keystore/<project-id>.json`
- Service account scopes: `s3://scylla-qa-keystore/<project-id>_service_accounts.json`

The `KeyStore.get_gcp_credentials()` method in `sdcm/keystore.py` resolves the filename from
`SCT_GCE_PROJECT` environment variable (defaults to `gcp-sct-project-1`).

### Quota Issues

Check and request quota increases for:
- CPUs per region (for large clusters)
- Persistent disk SSD (for local SSD tests)
- In-use IP addresses
- GKE clusters per project

```bash
gcloud compute project-info describe --project="$NEW_PROJECT_ID" \
    --format="table(quotas[].metric,quotas[].limit,quotas[].usage)"
```

---

## Automation Script

A fully automated setup script is available at [`docs/setup_gcp_project.sh`](setup_gcp_project.sh).

```bash
./docs/setup_gcp_project.sh <project-id> <billing-account-id>
```
