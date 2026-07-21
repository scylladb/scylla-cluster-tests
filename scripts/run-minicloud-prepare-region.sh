#!/bin/bash
set -euo pipefail

# Run AwsRegion.configure() against minicloud to set up VPC, subnets, security
# groups, route tables, and key pairs in minicloud's mock EC2.
#
# This MUST run before the provision test so that instances launch into
# minicloud-managed networking instead of referencing real AWS subnets.
#
# Usage:
#   ./scripts/run-minicloud-prepare-region.sh
#
# Expects minicloud to already be running (or set MINICLOUD_START=1 to auto-start via Docker).

MINICLOUD_DOCKER="${MINICLOUD_DOCKER:-ghcr.io/scylladb/minicloud:0.1.0}"
MINICLOUD_PORT="${MINICLOUD_PORT:-5000}"

S3_PASSTHROUGH_BUCKETS="${S3_PASSTHROUGH_BUCKETS:-scylla-qa-keystore}"
AWS_REGION="${SCT_REGION_NAME:-eu-west-1}"
export AWS_REGION

MINICLOUD_START="${MINICLOUD_START:-1}"

cleanup() {
    if [[ "$MINICLOUD_START" == "1" ]]; then
        echo ">>> Stopping minicloud container"
        docker stop minicloud 2>/dev/null || true
        docker rm minicloud 2>/dev/null || true
    fi
}

if [[ "$MINICLOUD_START" == "1" ]]; then
    trap cleanup EXIT

    if [[ ! -e /dev/kvm ]]; then
        echo "ERROR: /dev/kvm not available."
        exit 1
    fi
    if ! aws sts get-caller-identity &>/dev/null; then
        echo "ERROR: AWS credentials not configured."
        exit 1
    fi

    echo ">>> Starting minicloud container..."
    export MINICLOUD_DOCKER
    export MINICLOUD_PORT
    export MINICLOUD_AWS_REGION="$AWS_REGION"
    export S3_PASSTHROUGH_BUCKETS
    bash scripts/start-minicloud.sh
fi

export AWS_ENDPOINT_URL="http://localhost:$MINICLOUD_PORT"

echo ">>> Running AwsRegion.configure() for region '$AWS_REGION' via minicloud..."
echo "    AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL"
echo ""

uv run sct.py prepare-regions --cloud-provider aws --regions "$AWS_REGION"

echo ">>> Region '$AWS_REGION' configured in minicloud successfully!"
