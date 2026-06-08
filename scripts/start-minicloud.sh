#!/bin/bash
# Start minicloud as a Docker container with all required configuration.
# Uses the same env vars as MinicloudConfig.from_env() in sdcm/utils/minicloud.py.
#
# The container image includes minicloud-setup.sh which is run automatically by
# the entrypoint to configure networking (TUN/bridges) before starting minicloud.
#
# Required:
#   - Docker with --device /dev/kvm support (KVM-capable host)
#   - AWS credentials: either ~/.aws/ mount or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars
#
# Environment variables (all optional, defaults match MinicloudConfig):
#   MINICLOUD_DOCKER          - Docker image (default: scylladb/minicloud:dev)
#   MINICLOUD_PORT            - API port (default: 5000)
#   MINICLOUD_AWS_REGION      - AWS region to emulate (default: us-east-1)
#   S3_PASSTHROUGH_BUCKETS    - Comma-separated S3 buckets to forward to real AWS
#                               (default: scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com)
#   MINICLOUD_GCS_BUCKET      - GCS bucket for image staging (optional)
#   MINICLOUD_LIGHTWEIGHT     - Run in lightweight mode (default: true)
#   MINICLOUD_LIGHTWEIGHT_MEMORY - Memory per VM in lightweight mode (default: 1.5GiB)
#   GCS_KEY_FILE              - Path to GCS service account JSON key (optional)
#   MINICLOUD_CONTAINER_NAME  - Container name (default: minicloud)

set -euo pipefail

MINICLOUD_DOCKER="${MINICLOUD_DOCKER:-scylladb/minicloud:dev}"
MINICLOUD_PORT="${MINICLOUD_PORT:-5000}"
MINICLOUD_AWS_REGION="${MINICLOUD_AWS_REGION:-us-east-1}"
S3_PASSTHROUGH_BUCKETS="${S3_PASSTHROUGH_BUCKETS:-scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com}"
MINICLOUD_GCS_BUCKET="${MINICLOUD_GCS_BUCKET:-}"
MINICLOUD_LIGHTWEIGHT="${MINICLOUD_LIGHTWEIGHT:-true}"
MINICLOUD_LIGHTWEIGHT_MEMORY="${MINICLOUD_LIGHTWEIGHT_MEMORY:-1.5GiB}"
GCS_KEY_FILE="${GCS_KEY_FILE:-}"
MINICLOUD_CONTAINER_NAME="${MINICLOUD_CONTAINER_NAME:-minicloud}"
MINICLOUD_CACHE_DIR="${MINICLOUD_CACHE_DIR:-${HOME}/.cache/minicloud}"

# Stop existing container if running
docker stop "${MINICLOUD_CONTAINER_NAME}" 2>/dev/null && docker rm "${MINICLOUD_CONTAINER_NAME}" 2>/dev/null || true

mkdir -p "${MINICLOUD_CACHE_DIR}"

# Build docker run arguments
DOCKER_ARGS=(
    run -d
    --name "${MINICLOUD_CONTAINER_NAME}"
    --device /dev/kvm
    --device /dev/net/tun
    --network host
    --cap-add NET_ADMIN
    -v "${MINICLOUD_CACHE_DIR}:/root/.cache/minicloud"
)

# AWS credentials: prefer env vars, fall back to ~/.aws mount
if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    DOCKER_ARGS+=(-e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}")
    DOCKER_ARGS+=(-e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}")
    if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
        DOCKER_ARGS+=(-e "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}")
    fi
elif [[ -d "${HOME}/.aws" ]]; then
    DOCKER_ARGS+=(-v "${HOME}/.aws:/root/.aws:ro")
fi

DOCKER_ARGS+=(-e "AWS_REGION=${MINICLOUD_AWS_REGION}")

# GCS credentials mount
if [[ -n "${GCS_KEY_FILE}" && -f "${GCS_KEY_FILE}" ]]; then
    DOCKER_ARGS+=(-v "${GCS_KEY_FILE}:/etc/minicloud/gcs-key.json:ro")
    DOCKER_ARGS+=(-e "GOOGLE_APPLICATION_CREDENTIALS=/etc/minicloud/gcs-key.json")
fi

DOCKER_ARGS+=("${MINICLOUD_DOCKER}")

# Minicloud CLI arguments
MINICLOUD_ARGS=(
    --port "${MINICLOUD_PORT}"
    --aws-region "${MINICLOUD_AWS_REGION}"
    --s3-passthrough-buckets "${S3_PASSTHROUGH_BUCKETS}"
)

if [[ -n "${MINICLOUD_GCS_BUCKET}" ]]; then
    MINICLOUD_ARGS+=(--gcs-bucket "${MINICLOUD_GCS_BUCKET}")
fi

if [[ "${MINICLOUD_LIGHTWEIGHT}" == "true" ]]; then
    MINICLOUD_ARGS+=(--lightweight --lightweight-memory "${MINICLOUD_LIGHTWEIGHT_MEMORY}")
fi

echo "Starting minicloud container (image: ${MINICLOUD_DOCKER}, port: ${MINICLOUD_PORT})..."
docker "${DOCKER_ARGS[@]}" "${MINICLOUD_ARGS[@]}"

# Wait for health
echo "Waiting for minicloud to become healthy..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${MINICLOUD_PORT}" \
        -d "Action=DescribeRegions&Version=2016-11-15" >/dev/null 2>&1; then
        echo "minicloud is healthy on port ${MINICLOUD_PORT}"
        echo ""
        echo "Set these environment variables to use minicloud:"
        echo "  export AWS_ENDPOINT_URL=http://localhost:${MINICLOUD_PORT}"
        echo "  export SCT_MINICLOUD_ENDPOINT_URL=http://localhost:${MINICLOUD_PORT}"
        exit 0
    fi
    sleep 1
done

echo "ERROR: minicloud failed to become healthy within 30s" >&2
echo "Container logs:" >&2
docker logs "${MINICLOUD_CONTAINER_NAME}" 2>&1 | tail -20 >&2
exit 1
