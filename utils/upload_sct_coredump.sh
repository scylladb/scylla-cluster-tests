#!/bin/bash

set -xe

# Overridable so unit tests can point this at a throwaway directory instead of the real
# systemd coredump store.
COREDUMP_DIR="${COREDUMPS_DIR:-/var/lib/systemd/coredump}"

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

# Allow-list of comms (the systemd-coredump filename is core.<comm>.<uid>.<bootid>.<pid>.<ts>[.zst])
# we upload; anything else is noise (sshd, agents, ...) that still ends up in the coredump
# directory but isn't ours to investigate. Comma-separated glob entries, '*' is the only wildcard;
# an entry with no '*' matches that comm exactly (e.g. "java" excludes "javascript").
COREDUMPS_INCLUDE_COMM="${COREDUMPS_INCLUDE_COMM:-python*,scylla*,java}"
# Single regex shared by the builder-side split below and the container-side tar filter: it must
# contain no single quotes and no '$' because hydra.sh re-evaluates the command through
# `eval '<cmd>'` and either would break that quoting. Anchored on "core.<comm>." so a dotted comm
# like python3.14 still matches, while a foreign comm that merely starts with an allowed one
# (mypython, javascript) does not.
CORE_NAME_ALTERNATIVES="${COREDUMPS_INCLUDE_COMM//,/|}"
CORE_NAME_ALTERNATIVES="${CORE_NAME_ALTERNATIVES//\*/.*}"
CORE_NAME_RE=".*/core[.](${CORE_NAME_ALTERNATIVES})[.].*"

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
    # -x anchors each match to the whole line (a full path), -E enables the (a|b|c) alternation above.
    KEPT_COREDUMPS=$(echo "${NEW_COREDUMPS}" | grep -Ex "$CORE_NAME_RE" || true)
    SKIPPED_COREDUMPS=$(echo "${NEW_COREDUMPS}" | grep -Evx "$CORE_NAME_RE" || true)

    if [[ -n "${SKIPPED_COREDUMPS}" ]] ; then
        SKIPPED_COUNT=$(echo "${SKIPPED_COREDUMPS}" | wc -l)
        echo "skipping ${SKIPPED_COUNT} coredump(s) whose comm is not in COREDUMPS_INCLUDE_COMM (${COREDUMPS_INCLUDE_COMM}):"
        echo "${SKIPPED_COREDUMPS}"
    fi

    if [[ -n "${KEPT_COREDUMPS}" ]] ; then
        # Run the upload helper inside hydra, so the archive is created (allow-listed coredumps
        # only, same CORE_NAME_RE as the split above), uploaded, and removed in one go.
        ./docker/env/hydra.sh $EXTRA_HYDRA_ARGS "bash ./utils/upload_sct_coredump_inside_hydra.sh \"$COREDUMP_DIR\" $SINCE_EPOCH \"$CORE_NAME_RE\""
    else
        echo "all coredumps newer than @$SINCE_EPOCH in $COREDUMP_DIR were filtered out by COREDUMPS_INCLUDE_COMM (${COREDUMPS_INCLUDE_COMM}) - nothing to upload"
    fi
else
    echo "no coredumps newer than @$SINCE_EPOCH in $COREDUMP_DIR - nothing to upload"
fi
