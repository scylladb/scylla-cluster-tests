#!/usr/bin/env bash
set -e

SSH_KEYS_BUCKET=scylla-qa-keystore
SSH_KEYS_DIR=~/.ssh

echo "Obtaining QA SSH keys..."
umask 077
aws s3 sync s3://${SSH_KEYS_BUCKET} ${SSH_KEYS_DIR} --exact-timestamps --exclude '*' \
  --include scylla-qa-ec2 \
  --include scylla-test \
  --include support \
  --include scylla_test_id_ed25519 \
|| (echo "Please refer to https://github.com/scylladb/scylla-cluster-tests#setting-up-sct-environment to configure your aws-cli" && exit 1)
echo "QA SSH keys obtained."
