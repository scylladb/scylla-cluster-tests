#!/usr/bin/env bash
set -e
SSH_KEYS_DIR=~/.ssh
QA_PRIVATE_KEY_NAME=scylla-qa-ec2
QA_PRIVATE_KEY_PATH=${SSH_KEYS_DIR}/${QA_PRIVATE_KEY_NAME}

if [ -e ${QA_PRIVATE_KEY_PATH} ]; then
    echo "QA private key ${QA_PRIVATE_KEY_PATH} exists. Nothing to update."
else
    echo "QA private key ${QA_PRIVATE_KEY_PATH} doesn't exist. Retrieving..."
    mkdir -p ${SSH_KEYS_DIR}
    aws s3 cp s3://scylla-qa-keystore/${QA_PRIVATE_KEY_NAME} ${QA_PRIVATE_KEY_PATH}
    chmod 600 ${QA_PRIVATE_KEY_PATH}
    echo "Done."
fi