#!/bin/bash
set -euo pipefail

# Install system dependencies and download the minicloud binary from S3.
#
# Usage:
#   bash scripts/minicloud-runner-setup.sh [version]
#
# Arguments:
#   version  - git short SHA to download (default: "latest")
#
# Requires:
#   - Ubuntu/Debian host with /dev/kvm available
#   - AWS credentials configured (for S3 download)
#
# Installs to: /opt/minicloud/
# Symlinks:    /usr/local/bin/minicloud

S3_BUCKET="s3://scylla-qa-minicloud"
INSTALL_DIR="/opt/minicloud"
VERSION="${1:-latest}"
TARBALL_NAME="minicloud-${VERSION}.tar.gz"

if [[ ! -e /dev/kvm ]]; then
    echo "ERROR: /dev/kvm not available. This host does not support KVM virtualization."
    echo "Ensure you are running on a bare-metal or nested-virt-capable instance."
    exit 1
fi

echo "Installing system dependencies..."
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    iproute2 \
    iptables

echo "Downloading minicloud ($VERSION) from $S3_BUCKET..."
sudo mkdir -p "$INSTALL_DIR"
aws s3 cp "$S3_BUCKET/$TARBALL_NAME" /tmp/minicloud.tar.gz
sudo tar -xzf /tmp/minicloud.tar.gz -C "$INSTALL_DIR" --strip-components=1
rm -f /tmp/minicloud.tar.gz

sudo chmod +x "$INSTALL_DIR/minicloud"
sudo ln -sf "$INSTALL_DIR/minicloud" /usr/local/bin/minicloud

if [[ -f "$INSTALL_DIR/minicloud-setup.sh" ]]; then
    sudo chmod +x "$INSTALL_DIR/minicloud-setup.sh"
fi

echo "Verifying installation..."
minicloud --version

echo "Done. minicloud installed to $INSTALL_DIR"
