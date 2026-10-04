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
    // Log trees and guest state age out after a few days. The AMI/image cache is aged by LAST USE
    // (atime) instead, because only some of it is ever reused: a Scylla image is a daily master
    // build or a base release, and a weekly job rarely meets the same one twice, while the stock
    // distro images (Ubuntu and friends) are what the next week's runs boot again - and rebuilding
    // one is tens of minutes. Two weeks of no use lets a weekly job miss one Friday on this agent
    // (they float across the minipcs) and still find its image.
    def keepDays = args.get('keepDays', 3)
    def keepImageDays = args.get('keepImageDays', 14)
    // ...and a size cap on top, least recently used first: the TTL alone lets a burst of daily
    // master images fill the disk well inside the TTL.
    def maxImageCacheGiB = args.get('maxImageCacheGiB', 10)
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

# remove <dir> <entries of dir...>: delete them as root, and report any that survive.
remove() {
    local dir="\$1"
    shift
    local targets=("\$@")
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

# reclaim <subdir of the state dir> [find predicates...]: delete its top-level entries that match.
reclaim() {
    local dir="\${STATE_DIR}/\$1"
    shift
    [[ -d "\${dir}" ]] || return 0
    local targets
    mapfile -t targets < <(find "\${dir}" -mindepth 1 -maxdepth 1 "\$@" 2>/dev/null)
    remove "\${dir}" "\${targets[@]}"
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

    # The image cache: by last use, then by size - see keepImageDays and maxImageCacheGiB.
    AMI_CACHE="\${STATE_DIR}/amis"
    if [[ -d "\${AMI_CACHE}" ]] ; then
        # Aged by last READ (-atime), not by download (-mtime): every boot reads the qcow2 as a
        # backing file, so an image in weekly use stays, where -mtime evicted it a fixed time after
        # its download however often it was used (SCT-1145). relatime still refreshes atime once a
        # day on read. noatime freezes atime at download time, which quietly turns all of this back
        # into download-age eviction - say so where it will be seen rather than guess around it.
        if findmnt -no OPTIONS -T "\${AMI_CACHE}" 2>/dev/null | tr ',' '\\n' | grep -qx noatime ; then
            echo "WARNING: \${AMI_CACHE} is on a noatime mount - image cache eviction falls back to download age"
        fi
        reclaim amis -atime +${keepImageDays}

        # Then hold it under maxImageCacheGiB, least recently read first. The victims are picked up
        # front and removed in one pass: re-measuring after each delete would spin forever on an
        # entry that cannot be removed.
        max_kib=\$(( ${maxImageCacheGiB} * 1024 * 1024 ))
        used_kib=\$(du -sk "\${AMI_CACHE}" 2>/dev/null | cut -f1)
        evict=()
        while read -r _ entry ; do
            (( \${used_kib:-0} > max_kib )) || break
            evict+=("\${entry}")
            entry_kib=\$(du -sk "\${entry}" 2>/dev/null | cut -f1)
            used_kib=\$(( used_kib - \${entry_kib:-0} ))
        done < <(find "\${AMI_CACHE}" -maxdepth 1 -mindepth 1 -printf '%A@ %p\\n' 2>/dev/null | sort -n)
        if [[ \${#evict[@]} -gt 0 ]] ; then
            echo "image cache over ${maxImageCacheGiB}GiB, evicting \${#evict[@]} least recently used entr(y/ies)"
            remove "\${AMI_CACHE}" "\${evict[@]}"
        fi
    fi

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
