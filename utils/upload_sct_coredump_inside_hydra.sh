#!/bin/bash
#
# Archive this build coredumps, upload the archive, and delete it.
#
# Runs inside the hydra container. `upload_sct_coredump.sh` starts it through `hydra.sh` after
# it finds coredumps to collect. All three steps must run in the same container. With
# `--execute-on-runner`, each hydra call re-syncs the checkout to the runner using `rsync --delete`,
# so any archive left by one call would be gone before the next call could upload it.
#
# Usage: upload_sct_coredump_inside_hydra.sh <coredump-dir> <since-epoch>

set -xe

COREDUMP_DIR="$1"
SINCE_EPOCH="$2"

# keep the archive in the checkout on disk
COREDUMP_TARBALL="$(pwd)/sct-coredumps-${SCT_TEST_ID:0:8}.tar.zst"

# remove the archive on every exit, including failed uploads; coredumps stay on the host, so the local archive is only temporary.
trap 'rm -f "${COREDUMP_TARBALL}"' EXIT

# `sudo` reads only the root-owned coredumps; the redirect stays in this unprivileged shell,
# so the archive is owned by the build user and can be removed without root.
(cd "${COREDUMP_DIR}" && find . -maxdepth 1 -type f -newermt "@${SINCE_EPOCH}" -print0 | sudo tar --zstd -cf - --null -T -) > "${COREDUMP_TARBALL}"

./sct.py upload --test-id "${SCT_TEST_ID}" "${COREDUMP_TARBALL}"
