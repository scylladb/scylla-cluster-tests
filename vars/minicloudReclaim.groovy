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
// Everything under the minicloud state dir (instances/, amis/) is written by the minicloud
// container, which runs as root, so the agent user gets "Permission denied" on every file in it.
// Those entries are deleted as root through a throwaway container running the minicloud image
// (always already on the agent: a build pulled it to run the emulator), with only the state dir
// mounted. Anything that still could not be removed is reported with a WARNING rather than
// swallowed - a silent failure here is how one agent grew 1.7 TB of guest disks (SCT-1116).
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

STATE_DIR="\${SCT_MINICLOUD_STATE_DIR:-\${HOME}/.cache/minicloud}"
STATE_DIR="\${STATE_DIR/#\\~/\${HOME}}"

# An image to delete root-owned state with: the one the running container uses, else the build's
# override, else the pinned default, else any minicloud image already on the agent. Restricted to
# local images (--pull never below) - reclaim must not start a multi-GiB pull.
RECLAIM_IMAGE=""
for candidate in \
        "\$(docker inspect minicloud --format '{{.Config.Image}}' 2>/dev/null)" \
        "\${SCT_MINICLOUD_DOCKER_IMAGE}" \
        "\$(awk '\$1 == "image:" {print \$2; exit}' defaults/docker_images/minicloud/values_minicloud.yaml 2>/dev/null)" \
        \$(docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null | grep -E '^(ghcr\\.io/scylladb|scylladb)/minicloud:') ; do
    if [[ -n "\${candidate}" ]] && docker image inspect "\${candidate}" >/dev/null 2>&1 ; then
        RECLAIM_IMAGE="\${candidate}"
        break
    fi
done
echo "deleting minicloud state as root via: \${RECLAIM_IMAGE:-<no local minicloud image - as the agent user>}"

RECLAIM_FAILED=0

# reclaim <subdir of the state dir> [find predicates...]: delete its top-level entries that match.
reclaim() {
    local dir="\${STATE_DIR}/\$1"
    shift
    [[ -d "\${dir}" ]] || return 0
    local targets
    mapfile -t targets < <(find "\${dir}" -mindepth 1 -maxdepth 1 "\$@" 2>/dev/null)
    [[ \${#targets[@]} -gt 0 ]] || return 0
    printf 'reclaiming %s\\n' "\${targets[@]}"

    # Mounted at its own path, so the targets mean the same thing inside the container.
    if [[ -n "\${RECLAIM_IMAGE}" ]] ; then
        docker run --rm --pull never --network none --user root --entrypoint rm \
            -v "\${STATE_DIR}:\${STATE_DIR}" "\${RECLAIM_IMAGE}" -rf -- "\${targets[@]}"
    fi
    # Also the fallback when there is no image, and harmless after a successful root pass.
    rm -rf -- "\${targets[@]}" 2>/dev/null

    # Checked against the selected entries, not by re-running find: a partial delete bumps a
    # directory's mtime, so an age-based re-scan would stop seeing exactly the entries that failed.
    local leftovers=()
    for target in "\${targets[@]}" ; do
        [[ -e "\${target}" || -L "\${target}" ]] && leftovers+=("\${target}")
    done
    if [[ \${#leftovers[@]} -gt 0 ]] ; then
        RECLAIM_FAILED=1
        set +x
        echo "WARNING: minicloudReclaim could not remove \${#leftovers[@]} entr(y/ies) under \${dir} - clear them as root:"
        du -sh "\${leftovers[@]}" 2>/dev/null || printf '  %s\\n' "\${leftovers[@]}"
        set -x
    fi
}

if [[ "${atEnd}" == "true" ]] ; then
    # This build's own leftovers. The container first: it pins the qcow2 overlays under
    # instances/, so removing it before them is what actually frees the disk.
    docker ps -a --filter 'name=minicloud' --format '{{.Names}} {{.Status}}' 2>/dev/null
    docker rm -f minicloud 2>/dev/null
    reclaim instances
    rm -fv ./sct_runner_ip
else
    # A container left by an aborted build, first for the same reason as above: its guests hold
    # their disks open. Removed by NAME, never via `docker container prune`: the host is
    # explicitly shared, and a host-wide prune would take other jobs' stopped containers with it.
    docker ps -a --filter 'name=minicloud' --format '{{.Names}} {{.Status}}' 2>/dev/null
    docker rm -f minicloud 2>/dev/null

    # Old per-test log trees in the persistent workspace, and the symlink into the newest one.
    find . -maxdepth 1 -type d -name '????????-????-????-????-????????????' -mtime +${keepDays} -print -exec rm -rf {} + 2>/dev/null
    find . -maxdepth 1 -name 'latest' -type l -delete 2>/dev/null

    # Delete stale coredump archives left by aborted hydra builds.
    # They are owned by the build user, so no sudo is needed.
    find . -maxdepth 1 -type f -name 'sct-coredumps-*.tar.zst' -print -delete 2>/dev/null

    # Guest state an aborted build never got to clean.
    reclaim instances -mtime +${keepDays}

    # The image cache, on its own long TTL - see keepImageDays. Entries are per Scylla
    # image/AMI, so a daily master build leaves one behind every day.
    reclaim amis -mtime +${keepImageDays}

    # A stale sct_runner_ip is actively dangerous here: on a persistent workspace it would send
    # every stage down the --execute-on-runner branch and SSH to an IP that belongs to a
    # long-dead runner.
    rm -fv ./sct_runner_ip
fi

if [[ "\${RECLAIM_FAILED}" == "1" ]] ; then
    echo "WARNING: minicloudReclaim left minicloud state behind - see the WARNING lines above"
fi

echo "--- free space after reclaim ---"
df -h "\${HOME}" . 2>/dev/null
exit 0
"""
}
