#!/usr/bin/env bash
# Upload PR review analysis cache to S3 for archival/sharing.
#
# Usage:
#   ./upload_cache.sh s3://my-bucket/review-analysis
#   ./upload_cache.sh s3://my-bucket/review-analysis --dry-run
#
# The cache directory structure uploaded:
#   cache/raw/pr_list.json       - PR index (136K lines)
#   cache/raw/*.json             - Per-PR raw data (11,313 files)
#   cache/classified/*.json      - Per-PR classified data (11,312 files)
#   cache/report_data.json       - Aggregated report data (100KB)
#
# Total size: ~118MB
#
# Prerequisites:
#   - AWS CLI installed and configured (aws configure / AWS_PROFILE)
#   - S3 bucket must exist

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CACHE_DIR="${SCRIPT_DIR}/cache"

usage() {
    echo "Usage: $0 <s3-destination> [--dry-run] [--download]"
    echo ""
    echo "Examples:"
    echo "  $0 s3://my-bucket/review-analysis           # Upload cache"
    echo "  $0 s3://my-bucket/review-analysis --dry-run  # Preview upload"
    echo "  $0 s3://my-bucket/review-analysis --download # Download cache"
    echo ""
    echo "Options:"
    echo "  --dry-run   Show what would be uploaded without doing it"
    echo "  --download  Download from S3 to local cache instead of uploading"
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

S3_DEST="${1}"
DRY_RUN=""
DOWNLOAD=false

shift
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN="--dryrun"
            ;;
        --download)
            DOWNLOAD=true
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
    shift
done

# Validate S3 destination format
if [[ ! "${S3_DEST}" =~ ^s3:// ]]; then
    echo "Error: Destination must start with s3://"
    usage
fi

# Strip trailing slash
S3_DEST="${S3_DEST%/}"

if [[ "${DOWNLOAD}" == true ]]; then
    echo "=== Downloading cache from ${S3_DEST}/cache/ ==="
    mkdir -p "${CACHE_DIR}/raw" "${CACHE_DIR}/classified"

    aws s3 sync "${S3_DEST}/cache/" "${CACHE_DIR}/" ${DRY_RUN} \
        --no-progress \
        --exclude "__pycache__/*" \
        --exclude "*.pyc"

    echo ""
    echo "Download complete."
    echo "Files: $(find "${CACHE_DIR}" -type f | wc -l)"
    echo "Size:  $(du -sh "${CACHE_DIR}" | cut -f1)"
else
    # Validate cache exists
    if [[ ! -d "${CACHE_DIR}" ]]; then
        echo "Error: Cache directory not found at ${CACHE_DIR}"
        echo "Run the collection pipeline first: ./run.sh --since 2019-01-01"
        exit 1
    fi

    RAW_COUNT=$(find "${CACHE_DIR}/raw" -name "*.json" 2>/dev/null | wc -l)
    CLASSIFIED_COUNT=$(find "${CACHE_DIR}/classified" -name "*.json" 2>/dev/null | wc -l)
    TOTAL_SIZE=$(du -sh "${CACHE_DIR}" | cut -f1)

    echo "=== PR Review Analysis Cache Upload ==="
    echo "Source:     ${CACHE_DIR}"
    echo "Dest:       ${S3_DEST}/cache/"
    echo "Raw PRs:    ${RAW_COUNT} files"
    echo "Classified: ${CLASSIFIED_COUNT} files"
    echo "Total size: ${TOTAL_SIZE}"
    if [[ -n "${DRY_RUN}" ]]; then
        echo "Mode:       DRY RUN (no changes)"
    fi
    echo ""

    # Upload with sync (only uploads changed/new files)
    aws s3 sync "${CACHE_DIR}/" "${S3_DEST}/cache/" ${DRY_RUN} \
        --no-progress \
        --exclude "__pycache__/*" \
        --exclude "*.pyc"

    echo ""
    if [[ -n "${DRY_RUN}" ]]; then
        echo "Dry run complete. Remove --dry-run to upload."
    else
        echo "Upload complete."
        echo "To download on another machine:"
        echo "  ./upload_cache.sh ${S3_DEST} --download"
    fi
fi
