#!groovy

// Reclaim disk on a long-lived Jenkins agent running minicloud builds.
//
// Cloud builders die after their build, so nothing in SCT cleans a workspace up: `--logdir $(pwd)`
// leaves a <test-id>/ tree behind, plus ./latest and ~/.cache/minicloud/{instances,amis}. On an
// agent that lives for months that accumulates until the disk fills.
//
// Called TWICE per build, deliberately:
//
//   minicloudReclaim()             at build start - sweeps what earlier builds left, and clears a
//                                  stale ./sct_runner_ip before any stage can act on it
//   minicloudReclaim(atEnd: true)  at build end - this build's own guest state and container, so
//                                  the agent is left clean for whoever runs next. A shared agent
//                                  also serves scylla builds and dtest, and those jobs know
//                                  nothing about minicloud, so they will never clean up after it.
//
// The end-of-build pass deliberately keeps the log tree: collect-logs has already uploaded it to
// S3/Argus, but leaving it on the box for a few days is the one real advantage a static agent has
// for post-mortem. The start-of-build sweep ages it out.
//
// What is deliberately NOT touched:
//   docker images   the host is shared with scylla builds and dtest and the RelEng team manage
//                   image retention their own way; not even a dangling-only prune here, since
//                   "dangling" includes layers those jobs are mid-way through building
//   minicloud0      the host TUN device; recreating it needs sudo we would rather not have
def call(Map args = [:]) {
    // Log trees and guest state age out after a few days; the AMI/image cache gets a much longer
    // TTL because rebuilding one entry is tens of minutes and tens of GiB - it is the entire
    // economic case for a static agent. It still needs a TTL: master images are rebuilt daily, so
    // a cache that only ever grows fills the disk on its own.
    def keepDays = args.get('keepDays', 3)
    def keepImageDays = args.get('keepImageDays', 30)
    def atEnd = args.get('atEnd', false)

    sh """#!/bin/bash
# Reclaiming is best-effort: a build must never fail because an old file could not be removed.
set +e
set -x

if [[ "${atEnd}" == "true" ]] ; then
    # This build's own leftovers. The container first: it pins the qcow2 overlays under
    # instances/, so removing it before them is what actually frees the disk.
    docker ps -a --filter 'name=minicloud' --format '{{.Names}} {{.Status}}' 2>/dev/null
    docker rm -f minicloud 2>/dev/null
    rm -rf "\${HOME}/.cache/minicloud/instances"/* 2>/dev/null
    rm -fv ./sct_runner_ip
else
    # Old per-test log trees in the persistent workspace, and the symlink into the newest one.
    find . -maxdepth 1 -type d -name '????????-????-????-????-????????????' -mtime +${keepDays} -print -exec rm -rf {} + 2>/dev/null
    find . -maxdepth 1 -name 'latest' -type l -delete 2>/dev/null

    # Guest state an aborted build never got to clean.
    if [[ -d "\${HOME}/.cache/minicloud/instances" ]] ; then
        find "\${HOME}/.cache/minicloud/instances" -maxdepth 1 -mindepth 1 -mtime +${keepDays} -print -exec rm -rf {} + 2>/dev/null
    fi

    # The image cache, on its own long TTL - see keepImageDays. Entries are per Scylla
    # image/AMI, so a daily master build leaves one behind every day.
    if [[ -d "\${HOME}/.cache/minicloud/amis" ]] ; then
        find "\${HOME}/.cache/minicloud/amis" -maxdepth 1 -mindepth 1 -mtime +${keepImageDays} -print -exec rm -rf {} + 2>/dev/null
    fi

    # A stale sct_runner_ip is actively dangerous here: on a persistent workspace it would send
    # every stage down the --execute-on-runner branch and SSH to an IP that belongs to a
    # long-dead runner.
    rm -fv ./sct_runner_ip

    # A container left by an aborted build. Removed by NAME, never via `docker container prune`:
    # the host is explicitly shared, and a host-wide prune would take other jobs' stopped
    # containers with it.
    docker ps -a --filter 'name=minicloud' --format '{{.Names}} {{.Status}}' 2>/dev/null
    docker rm -f minicloud 2>/dev/null
fi

echo "--- free space after reclaim ---"
df -h "\${HOME}" . 2>/dev/null
exit 0
"""
}
