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

# Only coredumps from this build. On an ephemeral builder or runner the directory is empty at
# start, so mtime filtering changes nothing there - but on a long-lived agent it holds every dump
# the host ever produced (other jobs' included), and unbounded this used to tar and upload all of
# it. collectTestCoredumps passes the build start as COREDUMPS_SINCE_EPOCH; standalone runs
# fall back to the last 24h.
SINCE_EPOCH="${COREDUMPS_SINCE_EPOCH:-$(( $(date +%s) - 86400 ))}"

# List this build's coredumps. Keep hydra's exit code out of the pipeline: piping straight into
# grep would report a failed listing as an empty one, and the "nothing to upload" branch below
# would then state as fact something we never managed to check.
set +e
COREDUMP_LISTING=$(./docker/env/hydra.sh $EXTRA_HYDRA_ARGS "bash -c \"find $COREDUMP_DIR -maxdepth 1 -type f -newermt @$SINCE_EPOCH\"")
HYDRA_STATUS=$?
set -e

if [[ ${HYDRA_STATUS} -ne 0 ]] ; then
    echo "WARNING: listing $COREDUMP_DIR failed (hydra exited ${HYDRA_STATUS}) - cannot tell whether this build produced coredumps, skipping upload"
    exit 0
fi

# grep drops hydra's own preamble; exit 1 here genuinely means the directory held nothing new
NEW_COREDUMPS=$(echo "${COREDUMP_LISTING}" | grep "^$COREDUMP_DIR/" || true)

if [[ -n "${NEW_COREDUMPS}" ]] ; then

    # Compress only the new coredumps into a tarball (relative paths, like the old -C invocation)
    ./docker/env/hydra.sh $EXTRA_HYDRA_ARGS "bash -c \"cd $COREDUMP_DIR && find . -maxdepth 1 -type f -newermt @$SINCE_EPOCH -print0 | sudo tar --zstd -cf $COREDUMP_TARBALL --null -T -\""

    # Upload the tarball
    ./docker/env/hydra.sh $EXTRA_HYDRA_ARGS upload --test-id $SCT_TEST_ID $COREDUMP_TARBALL
else
    echo "no coredumps newer than @$SINCE_EPOCH in $COREDUMP_DIR - nothing to upload"
fi
