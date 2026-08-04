#!groovy

// Validate that this Jenkins agent can actually host minicloud, before anything expensive runs.
//
// Every probe is bounded and nothing throws mid-way: failures are collected and reported once at
// the end, so a misconfigured agent costs one build to diagnose rather than one build per problem.
// Same shape as tagBuilder.groovy's identifyCloud().
//
// Everything probed here is a property of the AGENT, not of the run, which is why it is not simply
// a call into MinicloudManager.preflight_check(): that one runs inside the test process, ~20 minutes
// in, after Argus registration, the hydra pull and the checkout, from inside a container - so it
// cannot answer any of the questions below. Where the two do overlap the Python side is the one
// kept (see the RAM note further down); nothing here re-implements a check Python can make.
//
// What Python cannot see from where it runs:
//
//   * it does Path("/dev/kvm").exists(), which says nothing about whether the agent user can
//     *open* it. That is the single most likely misconfiguration on a shared lab box.
//   * it does shutil.which("docker"), which says nothing about the daemon being reachable or the
//     agent user being in the docker group.
//
// Exact RAM sizing is deliberately NOT here - it belongs in Python, where SCTConfiguration knows
// n_db_nodes + n_loaders + n_monitor_nodes after the yaml+env merge and can parse IntOrList
// ("3 3" for multi-DC). See MinicloudManager._check_host_memory().
//
// Nested virtualization is not checked: these agents are bare metal, so KVM is native.

def call(Map args = [:]) {
    def minHomeGib = args.get('minHomeGib', 80)
    def minWorkspaceGib = args.get('minWorkspaceGib', 20)

    // One shell, one report. `set +e` throughout: a probe that cannot run is a finding, not a
    // reason to abort before the other probes have said their piece.
    def report = sh(returnStdout: true, script: """#!/bin/bash
set +e

fail=()
warn=()

# --- KVM -----------------------------------------------------------------------------------
# modprobe first: after a reboot without module auto-load /dev/kvm does not exist yet, and checking
# the device before creating it would record a failure the successful modprobe can never clear.
if ! grep -qE '^(kvm_intel|kvm_amd) ' /proc/modules ; then
    # -n so this can never sit waiting for a password prompt
    if ! sudo -n modprobe kvm_intel 2>/dev/null && ! sudo -n modprobe kvm_amd 2>/dev/null ; then
        fail+=("no kvm_intel/kvm_amd module loaded and it could not be modprobe'd without a password")
    fi
fi

if [[ ! -e /dev/kvm ]] ; then
    fail+=("/dev/kvm is missing - this agent cannot run minicloud guests")
elif [[ ! -w /dev/kvm ]] ; then
    fail+=("/dev/kvm exists but is not writable by \$(id -un) (groups: \$(id -Gn)) - add the agent user to the 'kvm' group and RESTART the agent process, reconnecting is not enough")
fi

# --- docker --------------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1 ; then
    fail+=("docker is not on PATH")
elif ! timeout 30 docker info >/dev/null 2>&1 ; then
    fail+=("docker is on PATH but 'docker info' failed - daemon down, or \$(id -un) is not in the 'docker' group")
fi

# --- host networking ------------------------------------------------------------------------
# _setup_host_networking() only LOGGER.warning's when it cannot create minicloud0, so without one
# of these two the build proceeds with every guest silently unreachable.
# Full `inet <addr>/<prefix>` token, not a substring: '10.127.0.1' also matches 10.127.0.10/24,
# which would pass here and then leave every guest unreachable. Mirrors _host_networking_matches().
if ! ip addr show minicloud0 2>/dev/null | grep -qE 'inet 10\\.127\\.0\\.1/24( |\$)' ; then
    if ! sudo -n true 2>/dev/null ; then
        fail+=("minicloud0 does not carry 10.127.0.1/24 and there is no passwordless sudo to create it - either pre-create it with a boot-time unit (preferred) or grant sudo -n")
    fi
else
    # The device exists, so nothing later will reconfigure it: the routes have to be right now.
    # A leftover blanket 10.0.0.0/8 from an older image black-holes Argus and the QA infra.
    for want in 10.160.0.0/11 172.31.0.0/16 ; do
        if ! ip route show "\${want}" 2>/dev/null | grep -q 'dev minicloud0' ; then
            fail+=("minicloud0 exists but \${want} is not routed through it - re-run minicloud-setup.sh with MINICLOUD_VPC_ROUTES, or grant sudo -n so the run can")
        fi
    done
    if ip route show 10.0.0.0/8 2>/dev/null | grep -q 'dev minicloud0' ; then
        fail+=("minicloud0 still carries the legacy 10.0.0.0/8 route, which black-holes Argus and the QA infra from this agent - re-run minicloud-setup.sh to replace it")
    fi
fi

# --- port 5000 -----------------------------------------------------------------------------
if ss -ltn 2>/dev/null | grep -q ':5000 ' ; then
    # -f and a body match, both needed: bare `curl -s -o /dev/null` exits 0 on a 404, so any random
    # listener on 5000 would pass for a minicloud. DescribeVpcs is also the health check
    # MinicloudManager uses - DescribeRegions answers before the API is really ready.
    if ! docker ps --filter 'name=^minicloud\$' --format '{{.Names}}' 2>/dev/null | grep -qx minicloud ; then
        # Answering the probe is not enough: it has to be OUR container. Otherwise
        # MinicloudManager.start() finds a healthy endpoint it does not own and reuses it.
        fail+=("port 5000 is in use but no container named 'minicloud' is running - something else owns it")
    elif timeout 10 curl -fs "http://localhost:5000/?Action=DescribeVpcs&Version=2016-11-15" 2>/dev/null | grep -q 'DescribeVpcsResponse' ; then
        warn+=("port 5000 is already held by a responding minicloud container - it will be reused or replaced")
    else
        fail+=("a container named 'minicloud' holds port 5000 but does not answer DescribeVpcs - it is wedged; remove it with 'docker rm -f minicloud'")
    fi
fi

# --- environment ---------------------------------------------------------------------------
# hydra.sh does `id -u "\${USER}"` under `set -eo pipefail`, so an unset USER on a
# systemd-launched JNLP agent kills hydra with a bare "id: '': no such user".
[[ -z "\${USER}" ]] && fail+=("USER is not set - hydra.sh resolves the agent user under 'set -eo pipefail' and dies when it is empty. Common on systemd-launched JNLP agents.")
[[ -z "\${HOME}" ]] && fail+=("HOME is not set")
if [[ -n "\${HOME}" && ! -w "\${HOME}" ]] ; then
    fail+=("HOME (\${HOME}) is not writable - the ~/.cache/minicloud AMI cache lives there")
fi

# --- disk ----------------------------------------------------------------------------------
# The AMI cache is tens of GiB per image and is the whole economic case for a long-lived agent.
home_free=\$(df --output=avail -BG "\${HOME:-/}" 2>/dev/null | tail -1 | tr -dc '0-9')
if [[ -n "\${home_free}" && "\${home_free}" -lt ${minHomeGib} ]] ; then
    fail+=("only \${home_free}GiB free in \${HOME} - need ${minHomeGib}GiB for the minicloud image cache")
fi
ws_free=\$(df --output=avail -BG . 2>/dev/null | tail -1 | tr -dc '0-9')
if [[ -n "\${ws_free}" && "\${ws_free}" -lt ${minWorkspaceGib} ]] ; then
    fail+=("only \${ws_free}GiB free in the workspace - need ${minWorkspaceGib}GiB for logs and guest images")
fi

# --- warn-only -----------------------------------------------------------------------------
if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx minicloud ; then
    warn+=("a container named 'minicloud' already exists - it will be replaced")
fi
stale_qemu=\$(pgrep -c qemu-system-x86_64 2>/dev/null)
if [[ -n "\${stale_qemu}" && "\${stale_qemu}" -gt 0 ]] ; then
    warn+=("\${stale_qemu} qemu-system-x86_64 process(es) already running - they eat the RAM this build needs, and on a shared box may not be ours")
fi
mem_avail=\$(awk '/MemAvailable/ {print int(\$2/1048576)}' /proc/meminfo 2>/dev/null)
if [[ -n "\${mem_avail}" && "\${mem_avail}" -lt 8 ]] ; then
    warn+=("only \${mem_avail}GiB MemAvailable - exact sizing is checked later against the test shape")
fi

for w in "\${warn[@]}" ; do echo "PREFLIGHT-WARN \${w}" ; done
for f in "\${fail[@]}" ; do echo "PREFLIGHT-FAIL \${f}" ; done
echo "PREFLIGHT-DONE \${#fail[@]}"
""")

    def failures = []
    report.readLines().each { line ->
        line = line.trim()
        if (line.startsWith('PREFLIGHT-WARN ')) {
            println("minicloud preflight WARNING: " + line.substring('PREFLIGHT-WARN '.length()))
        } else if (line.startsWith('PREFLIGHT-FAIL ')) {
            failures << line.substring('PREFLIGHT-FAIL '.length())
        }
    }

    if (failures) {
        def message = "=================== This agent cannot host minicloud (${failures.size()} problem(s)) " +
                      "===================\n" + failures.collect { " * ${it}" }.join('\n') +
                      "\nSee docs/minicloud.md for the agent prerequisites."
        println(message)
        throw new Exception(message)
    }

    println("minicloud preflight passed on ${env.NODE_NAME}")
    return true
}
