#!/usr/bin/env bash

set -eo pipefail

# make sure the call to get_username.py doesn't import any local modules
export PYTHONSAFEPATH=true

CMD=$@
DOCKER_ENV_DIR=$(readlink -f "$0")
DOCKER_ENV_DIR=$(dirname "${DOCKER_ENV_DIR}")
DOCKER_REPO=scylladb/hydra
DOCKER_REGISTRY=docker.io
SCT_DIR=$(dirname "${DOCKER_ENV_DIR}")
SCT_DIR=$(dirname "${SCT_DIR}")
VERSION=v$(cat "${DOCKER_ENV_DIR}/version")
HOST_NAME=SCT-CONTAINER
USER_ID=$(id -u "${USER}"):$(id -g "${USER}")
HOME_DIR=${HOME}

CREATE_RUNNER_INSTANCE=""
RUNNER_IP_FILE="${SCT_DIR}/sct_runner_ip"
RUNNER_IP=""
RUNNER_CMD=""

HYDRA_DRY_RUN=""
HYDRA_HELP=""

function die () {
    msg="$1"
    if [[ -n "$msg" ]]; then
        echo "$(basename $0): $msg." 1>&2
    fi
    cat <<EOF 1>&2

Run \`$0 --help' to print the full help message.
EOF
    exit 1
}

export SCT_TEST_ID=${SCT_TEST_ID:-$(uuidgen)}
export GIT_USER_EMAIL=$(git config --get user.email)

# Hydra arguments parsing

SCT_ARGUMENTS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --execute-on-new-runner)
            CREATE_RUNNER_INSTANCE="1"
            shift
            ;;
        --execute-on-runner)
            RUNNER_IP="$2"
            shift 2
            ;;
        --dry-run-hydra)
            HYDRA_DRY_RUN="1"
            shift
            ;;
        --install-package-from-directory)
            SCT_ARGUMENTS+=("$1" "$2")
            shift 2
            ;;
        --install-bash-completion)
            SCT_ARGUMENTS+=("$1")
            shift
            ;;
        --help)
            HYDRA_HELP="1"
            SCT_ARGUMENTS+=("$1")
            shift
            ;;
        -*)
            echo "Unknown argument '$1'"
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

# Hydra command arguments line parsing

HYDRA_COMMAND=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--backend)
            SCT_CLUSTER_BACKEND="$2"
            HYDRA_COMMAND+=("$1" "$2")
            shift 2
            ;;
        --help)
            HYDRA_HELP="1"
            HYDRA_COMMAND+=("$1")
            shift
            ;;
        *)
            HYDRA_COMMAND+=("$1")
            shift
            ;;
    esac
done

if [[ -n "${CREATE_RUNNER_INSTANCE}" ]]; then
    if [[ -n "${RUNNER_IP}" ]]; then
        echo "Can't use '--execute-on-new-runner' and '--execute-on-runner IP' options simultaneously"
        exit 1
    fi
    if [[ -f "${RUNNER_IP_FILE}" ]]; then
        RUNNER_IP=$(<"${RUNNER_IP_FILE}")
        echo "Looks like there is another SCT runner launched already (Public IP: ${RUNNER_IP})"
        echo "Please, delete '${RUNNER_IP_FILE}' file first and try again."
        echo "Or use 'hydra --execute-on-runner ${RUNNER_IP} ...' to run command on existing runner"
        exit 1
    fi
    echo ">>> Create a new SCT runner instance"
    echo
    if [[ -z "${HYDRA_DRY_RUN}" ]]; then
        HYDRA=$0
    else
        HYDRA="echo $0"
    fi

    if [[ -n "${RESTORED_TEST_ID}" ]]; then
        RESTORED_TEST_ID="--restored-test-id ${RESTORED_TEST_ID}"
    else
        RESTORED_TEST_ID=""
    fi

    ${HYDRA} create-runner-instance \
      --cloud-provider aws \
      --region "${RUNNER_REGION:-us-east-1}" \
      --availability-zone "${RUNNER_AZ:-a}" \
      --test-id "${SCT_TEST_ID}" \
      --duration "${RUNNER_DURATION:-1440}" \
      --restore-monitor "${RESTORE_MONITOR_RUNNER:-False}" \
      ${RESTORED_TEST_ID}

    if [[ -z "${HYDRA_DRY_RUN}" ]]; then
        RUNNER_IP=$(<"${RUNNER_IP_FILE}")
    else
        RUNNER_IP="127.0.0.1"  # set it for testing purpose.
    fi
    echo
    echo ">>> Run hydra command on the new SCT runner w/ public IP: ${RUNNER_IP}"
    echo
fi

# if running on Build server
if [[ -n "$JENKINS_URL" || -n "$BUILD_TAG" || -n "$GITHUB_ACTIONS" ]]; then
    echo "Running on Build Server..."
    HOST_NAME=`hostname`
else
    TTY_STDIN="-it"
    TPUT_OPTIONS=""
    [[ -z "$TERM" || "$TERM" == 'dumb' ]] && TPUT_OPTIONS="-T xterm-256color"
    TERM_SET_SIZE="export COLUMNS=`tput $TPUT_OPTIONS cols`; export LINES=`tput $TPUT_OPTIONS lines`;"
fi

if which docker >/dev/null 2>&1 ; then
  tool=${HYDRA_TOOL-docker}
elif which podman >/dev/null 2>&1 ; then
  tool=${HYDRA_TOOL-podman}
else
  die "Please make sure you install either podman or docker on this machine to run hydra"
fi

if [[  -n "$JENKINS_URL" || -n "$BUILD_TAG" || -n "$GITHUB_ACTIONS" || -z "`$tool images ${DOCKER_REGISTRY}/${DOCKER_REPO}:${VERSION} -q`" ]]; then
    echo "Pull version $VERSION from Docker Hub..."
    $tool pull ${DOCKER_REGISTRY}/${DOCKER_REPO}:${VERSION}
else
    echo "There is ${DOCKER_REGISTRY}/${DOCKER_REPO}:${VERSION} in local cache, using it."
fi

if [ -z "$HYDRA_DRY_RUN" ]; then
    DOCKER_GROUP_ARGS=()
else
    # Setting it for testing purpose
    DOCKER_GROUP_ARGS='--group-add 1 --group-add 2 --group-add 3'
fi

DOCKER_ADD_HOST_ARGS=()

# export all SCT_* env vars into the docker run
SCT_OPTIONS=$(env | sed -n 's/^\(SCT_[^=]*\)=.*/--env \1/p')

# export all PYTEST_* env vars into the docker run
PYTEST_OPTIONS=$(env | sed -n 's/^\(PYTEST_[^=]*\)=.*/--env \1/p')

# export all BUILD_* env vars into the docker run
BUILD_OPTIONS=$(env | sed -n 's/^\(BUILD_[^=]*\)=.*/--env \1/p')

# export all AWS_* env vars into the docker run
AWS_OPTIONS=$(env | sed -n 's/^\(AWS_[^=]*\)=.*/--env \1/p')

# export all JENKINS_* env vars into the docker run
JENKINS_OPTIONS=$(env | sed -n 's/^\(JENKINS_[^=]*\)=.*/--env \1/p')

is_podman="$($tool --help | { grep -o podman || :; })"
docker_common_args=()

function EPHEMERAL_PORT() {
    LOW_BOUND=49152
    RANGE=16384
    while true; do
        CANDIDATE=$[$LOW_BOUND + ($RANDOM % $RANGE)]
        (echo "" >/dev/tcp/127.0.0.1/${CANDIDATE}) >/dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo $CANDIDATE
            break
        fi
    done
}

# One line per established TCP socket matching the ss(8) filter given as arguments:
# queue sizes, owning pid, kernel timer (keepalive countdown, or 'on' = retransmitting)
# and the tcp_info fields that tell a stalled reader from a lost path.
function hydra_ss_summary () {
    ss -Htnipo state established "$@" 2>/dev/null | awk '
        /^[0-9]/ { extra = ""
                   if (match($0, /pid=[0-9]+/)) extra = extra " " substr($0, RSTART, RLENGTH)
                   if (match($0, /timer:\([^)]*\)/)) extra = extra " " substr($0, RSTART, RLENGTH)
                   head = "local=" $3 " peer=" $4 " recvq=" $1 " sendq=" $2 extra; next }
        { info = ""
          for (i = 1; i <= NF; i++) if ($i ~ /^(rtt|retrans|unacked|lastsnd|lastrcv|lastack|notsent|rwnd_limited|sndbuf_limited):/) info = info " " $i
          print "  " head info }'
}

# The watchdog reads the builder's own sockets and processes with Linux tools (ss, ps -C,
# etimes); print why it can't run here, or nothing when it can.
function hydra_watchdog_unsupported_reason () {
    local tool missing=()
    if [[ "$(uname -s)" != "Linux" ]]; then
        echo "not a Linux host"
        return
    fi
    for tool in ss timeout awk ps ssh mktemp; do
        command -v "${tool}" >/dev/null 2>&1 || missing+=("${tool}")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "missing ${missing[*]}"
    fi
}

# SCT-1044: the docker CLI reaches the runner through one `ssh ... docker system dial-stdio`
# connection per API stream (attach, wait, ...). When one of them stalls nothing is printed
# until it times out, sometimes hours later. Every HYDRA_WATCHDOG_INTERVAL seconds, sample
# both ends' view of those connections through a fresh SSH probe, which also shows whether
# the runner itself is reachable and what the container's processes are blocked on.
# Every sample is appended to hydra-watchdog.log in the test's result dir on the runner
# (collected with the sct-runner-events logs). The console gets the full sample right away when
# something looks wrong, and otherwise every HYDRA_WATCHDOG_CONSOLE_INTERVAL seconds (default 30m),
# so the evidence survives even when log collection doesn't. HYDRA_WATCHDOG_VERBOSE=true prints all.
function hydra_watchdog () {
    set +e
    local interval=$1 now started rc out builder_view anomalies last_console=-1 child=""
    out=$(mktemp) || return
    # Children run in the background and are waited on, so TERM is handled right away and
    # nothing outlives the watchdog holding hydra's stdout open.
    trap '[[ -n "${child}" ]] && kill "${child}" 2>/dev/null; rm -f "${out}"; exit 0' TERM
    # Runs on the runner; stdin is the builder's view. The result dir is the newest one holding
    # this test's id, the same one collect-logs picks.
    local probe="runner_view=\$(echo \"uptime: \$(uptime)\"
        echo 'runner -> builder sockets:'
        hydra_ss_summary '( sport = :22 and dst '\${SSH_CLIENT%% *}' )'
        docker ps --all --filter name=${SCT_TEST_ID:-hydra} --format '{{.Names}} {{.Status}}'
        ps -eo pid,stat,wchan:24,etimes,args --sort=pid | awk '/[s]ct\\.py/ {print substr(\$0, 1, 200)}')
        logdir=\$(find ~/sct-results -mindepth 2 -maxdepth 2 -name test_id 2>/dev/null \\
                 | xargs -r grep -lx '${SCT_TEST_ID}' 2>/dev/null | xargs -r -n1 dirname | xargs -r ls -td | head -1)
        log=\${logdir:+\${logdir}/hydra-watchdog.log}
        { echo \"=== \${WATCHDOG_NOW}\"; cat; echo 'runner view:'; echo \"\${runner_view}\"; } >> \"\${log:-/dev/null}\"
        echo \"log: \${log:-<no result dir for ${SCT_TEST_ID} yet>}\"
        echo \"\${runner_view}\""
    while true; do
        sleep "${interval}" &
        child=$!
        wait "${child}"
        now=$(date -u +%FT%TZ)
        started=${SECONDS}
        # taken before the probe connects, so the probe's own socket isn't part of it
        builder_view=$(
            echo "builder -> runner ${RUNNER_IP} sockets:"
            hydra_ss_summary "( dst ${RUNNER_IP} and dport = :22 )"
            ps -o pid=,etimes=,args= -C ssh | grep -F -- "${RUNNER_IP}" | sed 's/^/  ssh pid,age_s,args: /')
        # No multiplexing: the probe must open its own connection, not ride a stalled master.
        timeout 60 ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=15 \
            -o ControlMaster=no -o ControlPath=none \
            -o ServerAliveInterval=5 -o ServerAliveCountMax=3 "ubuntu@${RUNNER_IP}" \
            "WATCHDOG_NOW=${now}; $(declare -f hydra_ss_summary); ${probe}" \
            <<< "${builder_view}" >"${out}" 2>&1 &
        child=$!
        wait "${child}"
        rc=$?
        child=""
        anomalies=$(hydra_watchdog_anomalies "${rc}" "${builder_view}" "${out}")
        # the first sample always prints, so the console says early where the samples go
        if [[ -n "${anomalies}" || "${HYDRA_WATCHDOG_VERBOSE:-false}" == "true" || "${last_console}" -lt 0 ]] \
                || (( SECONDS - last_console >= ${HYDRA_WATCHDOG_CONSOLE_INTERVAL:-1800} )); then
            {
                [[ -n "${anomalies}" ]] && echo "ANOMALY: ${anomalies}"
                echo "${builder_view}"
                echo "runner probe rc=${rc} took $((SECONDS - started))s:"
                sed 's/^/  /' "${out}"
            } | sed "s/^/[hydra-watchdog ${now}] /"
            last_console=${SECONDS}
        fi
    done
}

# What looks wrong in one watchdog sample, as a one-line summary (empty = healthy): the probe
# failed, a dial-stdio socket keeps retransmitting or has unanswered keepalive or zero-window probes
# on either end, or sct.py is blocked writing to its stdout. The runner also lists the probe's own
# connection; only sockets whose port the builder holds count.
function hydra_watchdog_anomalies () {
    local rc=$1 builder_view=$2 probe_out=$3 ports stuck='timer:\((on|keepalive|persist),[^,]*,[1-9]'
    [[ "${rc}" != 0 ]] && echo -n "runner probe failed rc=${rc}; "
    grep -Eq "${stuck}" <<< "${builder_view}" && echo -n "builder socket retrying; "
    ports=$(grep -o 'local=[^ ]*' <<< "${builder_view}" | sed 's/.*://' | paste -sd'|')
    [[ -n "${ports}" ]] && grep -E "peer=[^ ]*:(${ports}) " "${probe_out}" | grep -Eq "${stuck}" \
        && echo -n "runner socket retrying; "
    grep -q ' pipe_write ' "${probe_out}" && echo -n "sct.py blocked on stdout; "
    true
}

function stop_hydra_watchdog () {
    if [[ -n "${HYDRA_WATCHDOG_PID}" ]]; then
        kill "${HYDRA_WATCHDOG_PID}" 2>/dev/null || true
        wait "${HYDRA_WATCHDOG_PID}" 2>/dev/null || true
        HYDRA_WATCHDOG_PID=""
    fi
}

function run_in_docker () {
    CMD_TO_RUN=$1
    REMOTE_DOCKER_HOST=$2

    # If running on macOS, we need to mount /var/run/docker.sock to communicate with Docker daemon
    if [[ $OSTYPE == 'darwin'* ]]; then
        docker_common_args+=(
         -v /var/run/docker.sock:/var/run/docker.sock
         -v /dev:/dev:rw
         --tmpfs "${HOME_DIR}/.docker:exec,uid=$(id -u ${USER}),gid=$(id -g ${USER})"
         --tmpfs "${HOME_DIR}/.local:exec,uid=$(id -u ${USER}),gid=$(id -g ${USER}),size=256m"
         -e HOME="${HOME_DIR}"
       )
    elif [ -z "$is_podman" ]; then
        docker_common_args+=(
           -v /var/run:/run
           -v /dev:/dev:rw
           --tmpfs "${HOME_DIR}/.local:exec,mode=1777"
           -u ${USER_ID}
           )
    else
        PODMAN_PORT=$(EPHEMERAL_PORT)
        podman system --log-level=error service -t 0 tcp:localhost:${PODMAN_PORT} &
        trap "exit" INT TERM
        trap "kill 0" EXIT
        docker_common_args+=(
          -v $SCT_DIR/docker/docker_mocked_as_podman:/usr/local/bin/docker
          --userns=keep-id
          -e DOCKER_HOST=tcp://localhost:$PODMAN_PORT
          -u ${USER_ID}
        )
    fi

    echo "Going to run '${CMD_TO_RUN}'..."
    $([[ -n "$HYDRA_DRY_RUN" ]] && echo echo) \
    $tool ${REMOTE_DOCKER_HOST} run --rm ${TTY_STDIN} --privileged \
        -h "${HOST_NAME:0:64}" \
        -v "${SCT_DIR}:${SCT_DIR}" \
        -v /tmp:/tmp \
        -v /var/tmp:/var/tmp \
        -v "${HOME_DIR}:${HOME_DIR}" \
        -w "${SCT_DIR}" \
        -e JOB_NAME="${JOB_NAME}" \
        -e BUILD_URL="${BUILD_URL}" \
        -e BUILD_NUMBER="${BUILD_NUMBER}" \
        -e _SCT_BASE_DIR="${SCT_DIR}" \
        -e GIT_USER_EMAIL \
        -e PYTHONUNBUFFERED=1 \
        -e RUNNER_IP \
        -v /sys/fs/cgroup:/sys/fs/cgroup:ro \
        -v /etc/passwd:/etc/passwd:ro \
        -v /etc/group:/etc/group:ro \
        -v /etc/sudoers:/etc/sudoers:ro \
        -v /etc/sudoers.d/:/etc/sudoers.d:ro \
        -v /etc/shadow:/etc/shadow:ro \
        ${DOCKER_GROUP_ARGS[@]} \
        ${DOCKER_ADD_HOST_ARGS[@]} \
        ${docker_common_args[@]} \
        ${SCT_OPTIONS} \
        ${PYTEST_OPTIONS} \
        ${BUILD_OPTIONS} \
        ${JENKINS_OPTIONS} \
        ${AWS_OPTIONS} \
        --env GIT_BRANCH \
        --env CHANGE_TARGET \
        --env PYTHONFAULTHANDLER=yes \
        --env TERM \
        --net=host \
        --ulimit core=-1 \
        --ulimit nofile=65536:65536 \
        -v /var/lib/systemd/coredump:/var/lib/systemd/coredump \
        --name="${SCT_TEST_ID}_$(date +%s)" \
        ${DOCKER_REGISTRY}/${DOCKER_REPO}:${VERSION} \
        /bin/bash -c "${PREPARE_CMD}; ${TERM_SET_SIZE} eval '${CMD_TO_RUN}'"
}

if [[ -n "$RUNNER_IP" ]]; then
    export RUNNER_IP  # make it available inside SCT code.

    if [[ ! "$RUNNER_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "=========================================================================================================="
        echo "Invalid IP provided for '--execute-on-runner'. Run 'hydra create-runner-instance' or check ./sct_runner_ip"
        echo "=========================================================================================================="
        exit 2
    fi
    echo "SCT Runner IP: $RUNNER_IP"

    if [ -z "$HYDRA_DRY_RUN" ]; then
        eval $(ssh-agent)
    else
        echo 'eval $(ssh-agent)'
    fi

    function clean_ssh_agent {
        echo "Cleaning SSH agent"
        if [ -z "$HYDRA_DRY_RUN" ]; then
            eval $(ssh-agent -k)
        else
            echo 'eval $(ssh-agent -k)'
        fi
    }

    trap clean_ssh_agent EXIT

    if [ -z "$HYDRA_DRY_RUN" ]; then
        ssh-add ~/.ssh/scylla_test_id_ed25519
    else
        echo ssh-add ~/.ssh/scylla_test_id_ed25519
    fi

    echo "Going to run a Hydra commands on SCT runner '$RUNNER_IP'..."
    HOME_DIR="/home/ubuntu"

    echo "Syncing ${SCT_DIR} to the SCT runner instance..."
    if [ -z "$HYDRA_DRY_RUN" ]; then
        ssh-keygen -R "$RUNNER_IP" || true
        rsync -ar -e 'ssh -o StrictHostKeyChecking=no' --delete ${SCT_DIR} ubuntu@${RUNNER_IP}:/home/ubuntu/
    else
        echo "ssh-keygen -R \"$RUNNER_IP\" || true"
        echo "rsync -ar -e 'ssh -o StrictHostKeyChecking=no' --delete ${SCT_DIR} ubuntu@${RUNNER_IP}:/home/ubuntu/"
    fi
    if [[ -z "$AWS_OPTIONS" ]]; then
        echo "AWS credentials were not passed using AWS_* environment variables!"
        echo "Checking if ~/.aws/credentials exists..."
        if [ ! -f ~/.aws/credentials ]; then
            echo "Can't run SCT without AWS credentials!"
            exit 1
        fi
        echo "AWS credentials file found. Syncing to SCT Runner..."
        if [ -z "$HYDRA_DRY_RUN" ]; then
            rsync -ar -e 'ssh -o StrictHostKeyChecking=no' --delete ~/.aws ubuntu@${RUNNER_IP}:/home/ubuntu/
        else
            echo "rsync -ar -e 'ssh -o StrictHostKeyChecking=no' --delete ~/.aws ubuntu@${RUNNER_IP}:/home/ubuntu/"
        fi
    else
        echo "AWS_* environment variables found and will passed to Hydra container."
    fi

    SCT_DIR="/home/ubuntu/scylla-cluster-tests"
    HOST_NAME="ip-${RUNNER_IP//./-}"
    USER_ID=1000:1000
    RUNNER_CMD="ssh -o StrictHostKeyChecking=no ubuntu@${RUNNER_IP}"
    DOCKER_HOST="-H ssh://ubuntu@${RUNNER_IP}"
fi

if [ -z "${DOCKER_GROUP_ARGS[@]}" ]; then
    for gid in $(${RUNNER_CMD} id -G); do
        DOCKER_GROUP_ARGS+=(--group-add "$gid")
    done
fi

PREPARE_CMD="test"

COMMAND=${HYDRA_COMMAND[0]}

if [[ "$COMMAND" == *'bash'* ]] || [[ "$COMMAND" == *'python'* ]]; then
    CMD=${HYDRA_COMMAND[@]}
else
    CMD="./sct.py ${SCT_ARGUMENTS[@]} ${HYDRA_COMMAND[@]}"
fi

if [[ -z "$RUNNER_IP" || -n "$HYDRA_DRY_RUN" ]]; then
    run_in_docker "${CMD}" "${DOCKER_HOST}"
    exit
fi

HYDRA_WATCHDOG_PID=""
if [[ "${HYDRA_WATCHDOG_INTERVAL:-300}" -gt 0 ]]; then
    HYDRA_WATCHDOG_UNSUPPORTED=$(hydra_watchdog_unsupported_reason)
    if [[ -n "${HYDRA_WATCHDOG_UNSUPPORTED}" ]]; then
        echo "[hydra-watchdog] disabled: ${HYDRA_WATCHDOG_UNSUPPORTED}"
    else
        hydra_watchdog "${HYDRA_WATCHDOG_INTERVAL:-300}" &
        HYDRA_WATCHDOG_PID=$!
        trap 'stop_hydra_watchdog; clean_ssh_agent' EXIT
    fi
fi
HYDRA_DOCKER_STARTED=${SECONDS}
HYDRA_DOCKER_RC=0
run_in_docker "${CMD}" "${DOCKER_HOST}" || HYDRA_DOCKER_RC=$?
echo "[hydra $(date -u +%FT%TZ)] docker run on runner ${RUNNER_IP} exited rc=${HYDRA_DOCKER_RC} after $((SECONDS - HYDRA_DOCKER_STARTED))s"
exit ${HYDRA_DOCKER_RC}
