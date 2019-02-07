#!/usr/bin/env bash
set -e
SSH_KEYS_DIR=~/.ssh
QA_PRIVATE_KEY_NAMES="scylla-qa-ec2 scylla-test"
echo "Obtaining QA SSH keys..."
for pkey_name in ${QA_PRIVATE_KEY_NAMES}
do
    QA_PRIVATE_KEY_PATH=${SSH_KEYS_DIR}/${pkey_name}
    if [ -e ${QA_PRIVATE_KEY_PATH} ]; then
        echo "QA private key '${QA_PRIVATE_KEY_PATH}' exists. Nothing to update."
    else
        echo "QA private key '${QA_PRIVATE_KEY_PATH}' doesn't exist. Retrieving..."
        mkdir -p ${SSH_KEYS_DIR}
        aws s3 cp s3://scylla-qa-keystore/${pkey_name} ${QA_PRIVATE_KEY_PATH} || (echo "Please refer to https://github.com/scylladb/scylla-cluster-tests#setting-up-sct-environment to configure your aws-cli" && exit 1)
        chmod 600 ${QA_PRIVATE_KEY_PATH}
        echo "Done."
    fi
done
echo "QA SSH keys obtained."