#!/usr/bin/env bash
# Setup script for a new GCP project for SCT (Scylla Cluster Tests)
#
# Prerequisites:
#   - gcloud CLI authenticated with organization admin permissions
#   - aws CLI configured (for uploading credentials to S3 keystore)
#
# Usage:
#   ./docs/setup_gcp_project.sh <project-id> <billing-account-id>
#
# Example:
#   ./docs/setup_gcp_project.sh gcp-sct-project-2 01ABCD-234567-FEDCBA

set -euo pipefail

ORG_ID="273512867970"
KEYSTORE_BUCKET="scylla-qa-keystore"
QA_GROUP="qa@scylladb.com"
NETWORK_NAME="qa-vpc"
KMS_KEYRING="demo-keyring"
KMS_LOCATION="us-east1"

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <project-id> <billing-account-id>"
    echo ""
    echo "Arguments:"
    echo "  project-id         The GCP project ID to create (e.g., gcp-sct-project-2)"
    echo "  billing-account-id The billing account to link (required)"
    exit 1
fi

PROJECT_ID="$1"
BILLING_ACCOUNT="$2"
SA_NAME="sct-automation"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
BACKUP_SA_NAME="sct-manager-backup"
BACKUP_SA_EMAIL="${BACKUP_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE="/tmp/${PROJECT_ID}.json"
SCOPES_FILE="/tmp/${PROJECT_ID}_service_accounts.json"

trap 'rm -f "$KEY_FILE" "$SCOPES_FILE"' EXIT

echo "=== Setting up GCP project: ${PROJECT_ID} ==="
echo ""

echo "[1/12] Creating project..."
if gcloud projects describe "$PROJECT_ID" &>/dev/null; then
    echo "  Project already exists, skipping creation."
else
    gcloud projects create "$PROJECT_ID" \
        --organization="$ORG_ID" \
        --name="$PROJECT_ID"
    echo "  Project created."
fi

echo "[2/12] Linking billing account..."
gcloud billing projects link "$PROJECT_ID" \
    --billing-account="$BILLING_ACCOUNT"
echo "  Billing linked."

echo "[3/12] Enabling required APIs..."
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
    --project="$PROJECT_ID"
echo "  APIs enabled."

echo "[4/12] Creating main service account..."
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" &>/dev/null; then
    echo "  Service account already exists."
else
    gcloud iam service-accounts create "$SA_NAME" \
        --display-name="SCT Automation Service Account" \
        --project="$PROJECT_ID"
    echo "  Service account created: ${SA_EMAIL}"
fi

echo "[5/12] Creating service account key..."
if aws s3 ls "s3://${KEYSTORE_BUCKET}/${PROJECT_ID}.json" &>/dev/null; then
    echo "  Key already exists in keystore, skipping creation."
else
    gcloud iam service-accounts keys create "$KEY_FILE" \
        --iam-account="$SA_EMAIL" \
        --project="$PROJECT_ID"
    echo "  Uploading key to S3 keystore..."
    aws s3 cp "$KEY_FILE" "s3://${KEYSTORE_BUCKET}/${PROJECT_ID}.json"
    rm -f "$KEY_FILE"
    echo "  Key uploaded."
fi
echo "  Key uploaded and local copy removed."

echo "[6/12] Assigning IAM roles to service account..."
declare -a ROLES=(
    "roles/editor"
    "roles/container.admin"
    "roles/container.clusterAdmin"
    "roles/storage.admin"
    "roles/cloudkms.admin"
    "roles/cloudkms.cryptoKeyEncrypter"
    "roles/cloudkms.cryptoKeyDecrypter"
)

for role in "${ROLES[@]}"; do
    echo "  Granting ${role}..."
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="$role" \
        --quiet
done
echo "  All roles assigned."

echo "[7/12] Creating backup service account..."
if gcloud iam service-accounts describe "$BACKUP_SA_EMAIL" --project="$PROJECT_ID" &>/dev/null; then
    echo "  Backup service account already exists."
else
    gcloud iam service-accounts create "$BACKUP_SA_NAME" \
        --display-name="Account for having access to the gcs bucket for SCT backup tests" \
        --project="$PROJECT_ID"
    echo "  Backup service account created: ${BACKUP_SA_EMAIL}"
fi

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${BACKUP_SA_EMAIL}" \
    --role="roles/cloudkms.admin" \
    --quiet

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${BACKUP_SA_EMAIL}" \
    --role="roles/cloudkms.cryptoOperator" \
    --quiet
echo "  Backup SA roles assigned."

echo "[8/12] Creating service account scopes configuration..."
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")

cat > "$SCOPES_FILE" << EOF
[
  {
    "email": "${PROJECT_NUMBER}-compute@developer.gserviceaccount.com",
    "scopes": [
      "https://www.googleapis.com/auth/devstorage.read_write",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
]
EOF

aws s3 cp "$SCOPES_FILE" "s3://${KEYSTORE_BUCKET}/${PROJECT_ID}_service_accounts.json"
rm -f "$SCOPES_FILE"
echo "  Scopes config uploaded."

echo "[9/12] Creating VPC network '${NETWORK_NAME}'..."
if gcloud compute networks describe "$NETWORK_NAME" --project="$PROJECT_ID" &>/dev/null; then
    echo "  Network already exists."
else
    gcloud compute networks create "$NETWORK_NAME" \
        --subnet-mode=auto \
        --project="$PROJECT_ID"
    echo "  Network created."
fi

echo "[10/12] Creating KMS keyring..."
if gcloud kms keyrings describe "$KMS_KEYRING" --location="$KMS_LOCATION" --project="$PROJECT_ID" &>/dev/null; then
    echo "  Keyring already exists."
else
    gcloud kms keyrings create "$KMS_KEYRING" \
        --location="$KMS_LOCATION" \
        --project="$PROJECT_ID"
    echo "  Keyring created."
fi

echo "[11/12] Granting QA team access..."
declare -a QA_ROLES=(
    "roles/editor"
    "roles/container.admin"
    "roles/container.clusterAdmin"
)

for role in "${QA_ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="group:${QA_GROUP}" \
        --role="$role" \
        --quiet
done
echo "  QA team access granted."

echo "[12/12] Setup complete!"
echo ""
echo "=== Summary ==="
echo "  Project ID:            ${PROJECT_ID}"
echo "  Project Number:        ${PROJECT_NUMBER}"
echo "  Main SA:               ${SA_EMAIL}"
echo "  Backup SA:             ${BACKUP_SA_EMAIL}"
echo "  Network:               ${NETWORK_NAME} (auto-subnet)"
echo "  KMS Keyring:           ${KMS_KEYRING} (${KMS_LOCATION})"
echo "  Credentials in S3:     s3://${KEYSTORE_BUCKET}/${PROJECT_ID}.json"
echo "  Scopes in S3:          s3://${KEYSTORE_BUCKET}/${PROJECT_ID}_service_accounts.json"
echo ""
echo "=== Next Steps ==="
echo "  1. Add '${PROJECT_ID}' to SUPPORTED_PROJECTS in sdcm/utils/gce_utils.py"
echo "  2. Run region configuration:"
echo "     export SCT_GCE_PROJECT=${PROJECT_ID}"
echo "     ./sct.py prepare-regions -c gce -r us-east1"
echo "  3. Create runner image: ./sct.py create-runner-image --cloud-provider gce --region us-east1"
echo "  4. Upload SSH public key to project metadata"
echo "  5. Test with: SCT_GCE_PROJECT=${PROJECT_ID} hydra run-test ... --backend gce"
