#!groovy

// Tear the minicloud container down at the end of a build, and keep its log.
//
// ORDERING IS LOAD-BEARING: this must be the *last* teardown step, after log collection. The
// container runs with --network host but not --pid host, so QEMU shares its PID namespace and
// `docker rm -f` kills every guest with it - collect_minicloud_logs() needs them alive.
//
// Always safe to call: it is a no-op when there is no container, so callers can put it in a
// `finally` without guarding.
def call() {
    sh """#!/bin/bash
# Teardown must never fail a build that has already produced its result.
set +e
set -x

if ! docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx minicloud ; then
    echo "no minicloud container to stop"
    exit 0
fi

# Capture the log before removing the container - this is the only place the emulator's own view of
# the run survives, and it is what tells you whether guests failed to boot or the API rejected a
# call. Bounded: a long run can produce a lot.
mkdir -p ./minicloud-logs
docker logs --tail 5000 minicloud > ./minicloud-logs/minicloud-container.log 2>&1
echo "--- last 50 lines of the minicloud container log ---"
tail -50 ./minicloud-logs/minicloud-container.log

# Kills the emulator and every guest inside it.
docker rm -f minicloud

# Leave ~/.cache/minicloud/amis alone: it is the expensive, reusable part. See minicloudReclaim.
exit 0
"""
    archiveArtifacts artifacts: 'minicloud-logs/**', allowEmptyArchive: true
}
