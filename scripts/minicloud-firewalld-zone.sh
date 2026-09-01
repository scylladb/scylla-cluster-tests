#!/bin/bash
# Put minicloud TUN device in firewalld trusted zone (host-side script).
#
# Why this is a host script:
# Minicloud0 can be created from the privileged hydra container because it shares the host
# network namespace, but `firewall-cmd` is not in that image. So CI/containerized SCT runs use
# this script, while direct host runs (`sct.py start-minicloud`) do the same assignment in
# `sdcm/utils/minicloud/networking.py`.
#
# Why this matters:
# If minicloud0 stays in the default zone, firewalld blocks guest IMDS traffic DNATed into the
# host INPUT chain. DHCP and NTP still work, so guests boot and get addresses, but SSH key fetch
# fails and logins end with `AuthenticationError` after about 1500 seconds.
#
# This runs on every start because the assignment is runtime-only, and
# `firewall-cmd --reload` clears runtime interface-zone bindings.
#
# State and zone checks run unprivileged first, then retry with `sudo -n` when needed.
# On Fedora jenkins agents, polkit can block unprivileged reads. Hosts without passwordless
# sudo can still pass if a boot-time unit already set the zone. Only zone writes need root.
#
# Exit behavior:
# - 0: no firewalld, firewalld stopped, no TUN yet, or already trusted
# - non-zero: zone assignment was required but failed (never silent)
set -uo pipefail

TUN=minicloud0
ZONE=trusted

no_op() {
    echo "$1"
    exit 0
}

# run a read-only firewall-cmd query, retrying with sudo -n when polkit denies the caller
fw_read() {
    local out rc
    out="$(firewall-cmd "$@" 2>&1)"
    rc=$?

    if [[ ${rc} -ne 0 && "${out}" != *"not running"* ]]; then
        out="$(sudo -n firewall-cmd "$@" 2>&1)"
        rc=$?
    fi

    echo "${out}"
    return ${rc}
}

if ! command -v firewall-cmd >/dev/null 2>&1; then
    no_op "firewalld is not installed - no zone assignment needed for ${TUN}"
fi

if ! state="$(fw_read --state)"; then
    if [[ "${state}" == *"not running"* ]]; then
        no_op "firewalld is not running - no zone assignment needed for ${TUN}"
    fi
    echo "ERROR: firewalld is installed but its state could not be queried, unprivileged or" \
         "via passwordless sudo (${state})" >&2
    exit 1
fi

if ! ip link show "${TUN}" >/dev/null 2>&1; then
    no_op "${TUN} does not exist on this host - nothing to zone"
fi

if [[ "$(fw_read --get-zone-of-interface="${TUN}")" == "${ZONE}" ]]; then
    no_op "${TUN} is already in firewalld's ${ZONE} zone"
fi

# the write needs root: -n fails fast if passwordless sudo is unavailable
if ! output="$(sudo -n firewall-cmd --zone="${ZONE}" --change-interface="${TUN}" 2>&1)"; then
    echo "ERROR: could not move ${TUN} into firewalld's ${ZONE} zone (${output}) - the change" \
         "needs passwordless sudo, unless a boot-time unit pre-assigns the zone; without it" \
         "guests boot and get DHCP but never fetch their SSH key, failing every login with" \
         "AuthenticationError" >&2
    exit 1
fi

echo "moved ${TUN} into firewalld's ${ZONE} zone (runtime-only, re-applied every run)"
