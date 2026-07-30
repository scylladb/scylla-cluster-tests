#!groovy

// Validate that this Jenkins agent can actually host minicloud, before anything expensive runs.
//
// Every probe is bounded and nothing throws mid-way: failures are collected and reported once at
// the end, so a misconfigured agent costs one build to diagnose rather than one build per problem.
// Same shape as tagBuilder.groovy's identifyCloud().
//
// This duplicates a little of MinicloudManager.preflight_check() on purpose. That one runs inside
// the test process, ~20 minutes in, after Argus registration, the hydra pull and the checkout - and
// it cannot see everything that matters here:
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
if [[ ! -e /dev/kvm ]] ; then
    fail+=("/dev/kvm is missing - this agent cannot run minicloud guests")
elif [[ ! -w /dev/kvm ]] ; then
    fail+=("/dev/kvm exists but is not writable by \$(id -un) (groups: \$(id -Gn)) - add the agent user to the 'kvm' group and RESTART the agent process, reconnecting is not enough")
fi

if ! grep -qE '^(kvm_intel|kvm_amd) ' /proc/modules ; then
    # -n so this can never sit waiting for a password prompt
    if ! sudo -n modprobe kvm_intel 2>/dev/null && ! sudo -n modprobe kvm_amd 2>/dev/null ; then
        fail+=("no kvm_intel/kvm_amd module loaded and it could not be modprobe'd without a password")
    fi
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
if ! ip addr show minicloud0 2>/dev/null | grep -q '10.127.0.1' ; then
    if ! sudo -n true 2>/dev/null ; then
        fail+=("minicloud0 does not carry 10.127.0.1 and there is no passwordless sudo to create it - either pre-create it with a boot-time unit (preferred) or grant sudo -n")
    fi
fi

# --- port 5000 -----------------------------------------------------------------------------
if ss -ltn 2>/dev/null | grep -q ':5000 ' ; then
    # -f and a body match, both needed: bare `curl -s -o /dev/null` exits 0 on a 404, so any random
    # listener on 5000 would pass for a minicloud. DescribeVpcs is also the health check
    # MinicloudManager uses - DescribeRegions answers before the API is really ready.
    if timeout 10 curl -fs "http://localhost:5000/?Action=DescribeVpcs&Version=2016-11-15" 2>/dev/null | grep -q 'DescribeVpcsResponse' ; then
        warn+=("port 5000 is already held by a responding minicloud - it will be reused or replaced")
    else
        fail+=("port 5000 is in use by something that is not minicloud")
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
