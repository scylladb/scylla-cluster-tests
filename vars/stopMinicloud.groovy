#!groovy

// Tear the minicloud container down at the end of a build, and keep its log.
//
// LOCAL-AGENT TOPOLOGY ONLY, by design: in runner topology the container lives on the
// sct-runner and dies with it when 'Clean SCT Runners' terminates the instance - its logs are
// collected earlier by the regular collect-logs stage, which executes on the runner. This
// helper inspects the local docker daemon and is a deliberate no-op there (guarded on
// ./sct_runner_ip below), so callers can keep it unconditional in their `finally`.
//
// ORDERING IS LOAD-BEARING: this must be the *last* teardown step, after log collection. The
// container runs with --network host but not --pid host, so QEMU shares its PID namespace and
// `docker rm -f` kills every guest with it - collect_minicloud_logs() needs them alive.
//
// Honours the post_behavior_* keep modes, because on minicloud the container IS the cluster:
// `docker rm -f` takes every guest with it, so removing it while 'Clean resources' has just
// reported the nodes preserved would make keep/keep-on-failure silently untrue. Same rule as
// cleanSctRunners (all three post_behavior_* set to keep), plus keep-on-failure on a build that
// did not succeed - the case those modes exist for.
//
// Always safe to call: it is a no-op when there is no container, so callers can put it in a
// `finally` without guarding.
def call(Map params = [:], Object build = null) {
    def keepAlways = ['db_nodes', 'loader_nodes', 'monitor_nodes'].every {
        params."post_behavior_${it}" == 'keep'
    }
    def buildFailed = build != null && build.currentResult != 'SUCCESS'
    def keepOnFailure = buildFailed && ['db_nodes', 'loader_nodes', 'monitor_nodes'].any {
        params."post_behavior_${it}" == 'keep-on-failure'
    }
    def keepGuests = keepAlways || keepOnFailure

    sh """#!/bin/bash
# Teardown must never fail a build that has already produced its result.
set +e
set -x

# Runner topology: the container is on the runner, not on this builder - nothing to do here.
if [[ -n "\$(cat sct_runner_ip 2>/dev/null)" ]] ; then
    echo "runner topology: the minicloud container lives on the sct-runner and is torn down with it"
    exit 0
fi

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

if [[ "${keepGuests}" == "true" ]] ; then
    # The container is the cluster here: removing it would destroy the very nodes the
    # post_behavior_* settings asked to keep. Left running, and left for the next build's
    # minicloudReclaim to collect - so "keep" on a shared agent means "until the next build".
    echo "post_behavior_* asks to keep the nodes: leaving the minicloud container and its guests running"
    echo "inspect with: docker logs minicloud ; ssh into the guests from this agent"
    exit 0
fi

# Kills the emulator and every guest inside it.
docker rm -f minicloud

# Leave ~/.cache/minicloud/amis alone: it is the expensive, reusable part. See minicloudReclaim.
exit 0
"""
    archiveArtifacts artifacts: 'minicloud-logs/**', allowEmptyArchive: true
}
