#!/bin/bash
set -euo pipefail

# Package minicloud binary and helpers into a tarball, then upload to S3.
#
# Usage:
#   bash scripts/minicloud-package.sh /path/to/minicloud/repo
#
# Requires:
#   - minicloud repo with target/release/minicloud built
#   - AWS credentials configured (for S3 upload)
#
# The tarball contains:
#   minicloud              - the binary
#   minicloud-setup.sh     - network setup script (if present)
#
# Uploads to:
#   s3://scylla-qa-minicloud/minicloud-<git-short-sha>.tar.gz
#   s3://scylla-qa-minicloud/minicloud-latest.tar.gz  (overwritten each time)

MINICLOUD_DIR="${1:?Usage: $0 /path/to/minicloud/repo}"
S3_BUCKET="s3://scylla-qa-minicloud"

if [[ ! -f "$MINICLOUD_DIR/target/release/minicloud" ]]; then
    echo "ERROR: $MINICLOUD_DIR/target/release/minicloud not found."
    echo "Build minicloud first: cd $MINICLOUD_DIR && cargo build --release"
    exit 1
fi

VERSION=$(git -C "$MINICLOUD_DIR" rev-parse --short HEAD)
TARBALL="minicloud-${VERSION}.tar.gz"
WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

echo "Packaging minicloud version: $VERSION"

mkdir -p "$WORKDIR/minicloud"
cp "$MINICLOUD_DIR/target/release/minicloud" "$WORKDIR/minicloud/"

if [[ -f "$MINICLOUD_DIR/minicloud-setup.sh" ]]; then
    cp "$MINICLOUD_DIR/minicloud-setup.sh" "$WORKDIR/minicloud/"
fi

tar -czf "$WORKDIR/$TARBALL" -C "$WORKDIR" minicloud/
echo "Created: $WORKDIR/$TARBALL ($(du -h "$WORKDIR/$TARBALL" | cut -f1))"

echo "Uploading to $S3_BUCKET/$TARBALL ..."
aws s3 cp "$WORKDIR/$TARBALL" "$S3_BUCKET/$TARBALL"

echo "Uploading to $S3_BUCKET/minicloud-latest.tar.gz ..."
aws s3 cp "$WORKDIR/$TARBALL" "$S3_BUCKET/minicloud-latest.tar.gz"

echo "Done. Published minicloud $VERSION to $S3_BUCKET"
