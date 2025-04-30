#!/bin/bash

set -xe

COREDUMP_DIR="/var/lib/systemd/coredump"

SHORT_SCT_TEST_ID="$(echo $SCT_TEST_ID | cut -c1-8)"

# putting on /tmp since it's mount into host, and won't be lost between hydra executions
COREDUMP_TARBALL="/tmp/sct-coredumps-$SHORT_SCT_TEST_ID.tar.zst"

RUNNER_IP=$(cat sct_runner_ip||echo "")

if [[ -n "${RUNNER_IP}" ]] ; then
    EXTRA_HYDRA_ARGS="--execute-on-runner ${RUNNER_IP}"
fi

# Check if the coredumps exists in directory
if ./docker/env/hydra.sh $EXTRA_HYDRA_ARGS "bash -c \"[[ -n \\\"\$( ls $COREDUMP_DIR )\\\" ]]\"" ; then

    # Compress the coredumps into a tar.gz file
    ./docker/env/hydra.sh $EXTRA_HYDRA_ARGS "bash -c \"tar --zstd -cf $COREDUMP_TARBALL -C $COREDUMP_DIR .\""

    # Upload the tar.gz file
    ./docker/env/hydra.sh $EXTRA_HYDRA_ARGS upload --test-id $SCT_TEST_ID $COREDUMP_TARBALL
fi
