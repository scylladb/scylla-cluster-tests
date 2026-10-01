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
# directory but isn't ours to investigate. Comma-separated glob entries restricted to
# [A-Za-z0-9_+-] plus '*' as the only wildcard; an entry with no '*' matches that comm exactly
# (e.g. "java" excludes "javascript"). systemd-coredump escapes '.', ' ' and '/' out of comm as
# \x2e, \x20, \x2f (xescape() in systemd's coredump-vacuum/coredump.c), so the comm field of a
# real filename never contains a literal dot - a python3.14 process shows up as
# "python3\x2e14". Write "python3*" rather than "python3.14".
COREDUMPS_INCLUDE_COMM="${COREDUMPS_INCLUDE_COMM:-python*,scylla*,java}"

# Validated and escaped below into a single regex shared by the builder-side split and the
# container-side tar filter: it must contain no single quotes and no '$' because hydra.sh
# re-evaluates the command through `eval '<cmd>'` and either would break that quoting - hence the
# strict whitelist instead of trusting the entries as-is. '*' becomes '[^.]*', not '.*', so a
# wildcard widens only within the comm field and can never run past the following dot into the
# uid/bootid fields. Anchored on "core.<comm>." below.
if [[ "${COREDUMPS_INCLUDE_COMM}" == *, ]] ; then
    echo "ERROR: COREDUMPS_INCLUDE_COMM has a trailing comma (empty entry): '${COREDUMPS_INCLUDE_COMM}'" >&2
    exit 1
fi

IFS=',' read -ra COREDUMPS_INCLUDE_COMM_ENTRIES <<< "${COREDUMPS_INCLUDE_COMM}"
CORE_NAME_ALTERNATIVES=""
for ENTRY in "${COREDUMPS_INCLUDE_COMM_ENTRIES[@]}" ; do
    if [[ -z "${ENTRY}" ]] ; then
        echo "ERROR: COREDUMPS_INCLUDE_COMM has an empty entry (check for a stray comma): '${COREDUMPS_INCLUDE_COMM}'" >&2
        exit 1
    fi
    if [[ "${ENTRY}" == *.* ]] ; then
        echo "ERROR: COREDUMPS_INCLUDE_COMM entry '${ENTRY}' contains a literal '.': systemd-coredump always escapes a dot out of comm (as \\x2e), so a dot here can never match a real coredump - use '*' instead, e.g. 'python3*' instead of 'python3.14': '${COREDUMPS_INCLUDE_COMM}'" >&2
        exit 1
    fi
    if [[ ! "${ENTRY}" =~ ^[A-Za-z0-9_+*-]+$ ]] ; then
        echo "ERROR: COREDUMPS_INCLUDE_COMM entry '${ENTRY}' contains characters outside the allowed [A-Za-z0-9_+-] (plus '*' as wildcard): '${COREDUMPS_INCLUDE_COMM}'" >&2
        exit 1
    fi
    # Escape ERE metacharacters in the entry itself (bracket form survives the nested
    # eval/find/grep quoting without needing backslashes) before translating '*' into a wildcard
    # confined to the comm field.
    ESCAPED_ENTRY="${ENTRY//+/[+]}"
    ESCAPED_ENTRY="${ESCAPED_ENTRY//\*/[^.]*}"
    if [[ -n "${CORE_NAME_ALTERNATIVES}" ]] ; then
        CORE_NAME_ALTERNATIVES="${CORE_NAME_ALTERNATIVES}|${ESCAPED_ENTRY}"
    else
        CORE_NAME_ALTERNATIVES="${ESCAPED_ENTRY}"
    fi
done
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
