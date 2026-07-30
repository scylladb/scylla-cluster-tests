#!groovy

// Reclaim disk on a long-lived Jenkins agent before a minicloud build starts.
//
// Cloud builders die after their build, so nothing in SCT cleans a workspace up: `--logdir $(pwd)`
// leaves a <test-id>/ tree behind, plus ./latest and ~/.cache/minicloud/instances/*. On an agent
// that lives for months that accumulates until the disk fills.
//
// Deliberately at build START rather than end: the previous failure's logs stay on the box for
// post-mortem, which is one of the few genuine advantages a long-lived agent has over an ephemeral
// one. The cost is that a build inherits whatever the last one left, hence the age-based sweep.
//
// What is deliberately KEPT:
//   ~/.cache/minicloud/amis   downloading and converting a Scylla AMI is tens of minutes and tens
//                             of GiB. This cache is the entire economic case for a long-lived
//                             agent - never sweep it.
//   minicloud0                the host TUN device; recreating it needs sudo we would rather not have
//   docker images             the host also serves dtest, so `docker image prune -a` would evict
//                             images that are not ours and are expensive to refetch
def call(Map args = [:]) {
    def keepDays = args.get('keepDays', 3)

    sh """#!/bin/bash
# Reclaiming is best-effort: a build must never fail because an old log tree could not be removed.
set +e
set -x

# Old per-test log trees in the persistent workspace, and the symlink into the newest one.
find . -maxdepth 1 -type d -name '????????-????-????-????-????????????' -mtime +${keepDays} -print -exec rm -rf {} + 2>/dev/null
find . -maxdepth 1 -name 'latest' -type l -delete 2>/dev/null

# Per-instance guest state. NOT ~/.cache/minicloud/amis - see the comment above.
if [[ -d "\${HOME}/.cache/minicloud/instances" ]] ; then
    find "\${HOME}/.cache/minicloud/instances" -maxdepth 1 -mindepth 1 -mtime +${keepDays} -print -exec rm -rf {} + 2>/dev/null
fi

# A stale sct_runner_ip is actively dangerous here: on a persistent workspace it would send every
# stage down the --execute-on-runner branch and SSH to an IP that belongs to a long-dead runner.
rm -fv ./sct_runner_ip

# Containers left by an aborted build. Untagged image layers only - never a broad prune, the host
# is shared with dtest.
docker ps -a --filter 'name=minicloud' --format '{{.Names}} {{.Status}}' 2>/dev/null
docker container prune -f --filter 'until=${keepDays * 24}h' 2>/dev/null
docker image prune -f 2>/dev/null

echo "--- free space after reclaim ---"
df -h "\${HOME}" . 2>/dev/null
exit 0
"""
}
