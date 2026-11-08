#!groovy

// Sweep a persistent workspace on a long-lived local agent, at build start, from the directory the
// test runs in.
//
// Cloud builders die after their build; a local agent (minicloud or baremetal) keeps its workspace,
// and `--logdir $(pwd)` leaves a <test-id>/ tree behind every run, plus ./latest and any coredump
// archive. Nothing here touches docker or other jobs' files, so it is safe on an agent shared with
// minicloud, scylla builds and dtest; minicloudReclaim() does the minicloud-only part.
def call(Map args = [:]) {
    // Log trees stay a few days for post-mortem: collect-logs already uploaded them
    def keepDays = args.get('keepDays', 3)

    sh """#!/bin/bash
# Best-effort: a build must never fail because an old file could not be removed.
set +e
set -x

# Old per-test log trees, and the symlink into the newest one.
find . -maxdepth 1 -type d -name '????????-????-????-????-????????????' -mtime +${keepDays} -print -exec rm -rf {} + 2>/dev/null
find . -maxdepth 1 -name 'latest' -type l -delete 2>/dev/null

# Coredump archives left by aborted hydra builds, owned by the build user.
find . -maxdepth 1 -type f -name 'sct-coredumps-*.tar.zst' -print -delete 2>/dev/null

# A stale sct_runner_ip is actively dangerous: with no runner created on a local agent, it would send
# every stage down the --execute-on-runner branch and SSH to an IP of a long-dead runner.
rm -fv ./sct_runner_ip

df -h . 2>/dev/null
exit 0
"""
}
